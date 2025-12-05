import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from games.igdb_api import make_igdb_request
from games.models import (
    Game, Genre, Keyword, Platform, Series,
    Company, Theme, PlayerPerspective, GameMode, Screenshot, Country
)


class DataLoader:
    """Класс для загрузки данных из IGDB"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr
        self._db_lock = threading.Lock()
        self._api_lock = threading.Lock()  # Блокировка для контроля rate limiting
        self._last_request_time = 0
        self._min_request_interval = 0.25  # Минимальный интервал между запросами: 4 запроса в секунду
        self._retry_delay = 2.0  # Задержка при 429 ошибке

    def _rate_limited_request(self, endpoint, query, debug=False, max_retries=3):
        """Выполняет запрос к API с rate limiting и retry логикой"""
        for attempt in range(max_retries):
            try:
                # Rate limiting: ждем между запросами
                with self._api_lock:
                    current_time = time.time()
                    time_since_last = current_time - self._last_request_time

                    if time_since_last < self._min_request_interval:
                        sleep_time = self._min_request_interval - time_since_last
                        time.sleep(sleep_time)

                    self._last_request_time = time.time()

                # Выполняем запрос
                response = make_igdb_request(endpoint, query, debug=debug)
                return response

            except Exception as e:
                error_msg = str(e)

                # Проверяем, является ли ошибка 429 (Too Many Requests)
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    wait_time = self._retry_delay * (attempt + 1)  # Экспоненциальная задержка
                    time.sleep(wait_time)
                    continue
                else:
                    # Для других ошибок не пытаемся повторно
                    raise

        # Если все попытки исчерпаны
        raise Exception(f"API request failed after {max_retries} retries")

    def _batch_processor(self, ids_list, process_batch_func, emoji, name, debug=False):
        """Универсальный метод для обработки данных пачками"""
        if not ids_list:
            return {}

        result_map = {}
        lock = threading.Lock()

        batches = [ids_list[i:i + 10] for i in range(0, len(ids_list), 10)]
        total_batches = len(batches)

        if debug:
            self.stdout.write(f'      {emoji} Загрузка {name}: {len(ids_list)} объектов, {total_batches} пачек')

        max_workers = min(total_batches, 5)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for batch_num, batch_ids in enumerate(batches, 1):
                future = executor.submit(
                    process_batch_func, batch_num, batch_ids, result_map, lock, total_batches, name, debug
                )
                futures.append(future)

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    if debug:
                        with lock:
                            self.stderr.write(f'      ❌ Ошибка в потоке: {e}')

        if debug:
            loaded = len(result_map)
            self.stdout.write(
                f'      {emoji} Всего загружено {name}: {loaded}/{len(ids_list)} (из {total_batches} пачек)')

        return result_map

    def _generic_process_single(self, obj_id, data_by_id, name, model_class):
        """Универсальная обработка одного объекта"""
        if obj_id not in data_by_id:
            return None

        item_data = data_by_id[obj_id]
        item_name = item_data.get('name', f'{name} {obj_id}')

        with self._db_lock:
            existing = model_class.objects.filter(igdb_id=obj_id).first()

            if existing:
                needs_update = False
                if model_class == Series and existing.name.startswith('Series ') and not item_name.startswith(
                        'Series '):
                    existing.name = item_name
                    needs_update = True
                elif not existing.name.strip() and item_name.strip():
                    existing.name = item_name
                    needs_update = True

                if needs_update:
                    existing.save()
                return (obj_id, existing)
            else:
                obj = model_class.objects.create(igdb_id=obj_id, name=item_name)
                return (obj_id, obj)

    def _process_batch_template(self, batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                                endpoint, model_class, get_data_func=None, process_single_func=None):
        """Шаблон для обработки пачки данных"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} объектов')

            # Загружаем данные
            if get_data_func:
                batch_data = get_data_func(batch_ids)
            else:
                id_list = ','.join(map(str, batch_ids))
                query = f'fields id,name; where id = ({id_list});'
                batch_data = self._rate_limited_request(endpoint, query, debug=False)

            data_by_id = {item['id']: item for item in batch_data if 'id' in item}

            # Обрабатываем объекты параллельно
            batch_map = {}
            with ThreadPoolExecutor(max_workers=min(len(batch_ids), 10)) as executor:
                futures = {executor.submit(
                    process_single_func if process_single_func else
                    lambda obj_id: self._generic_process_single(obj_id, data_by_id, name, model_class),
                    obj_id
                ): obj_id for obj_id in batch_ids}

                for future in as_completed(futures):
                    obj_id = futures[future]
                    try:
                        result = future.result()
                        if result:
                            obj_id, obj = result
                            batch_map[obj_id] = obj
                    except Exception as e:
                        if debug:
                            with lock:
                                self.stderr.write(f'               ❌ Ошибка future для {obj_id}: {e}')

            with lock:
                result_map.update(batch_map)

            if debug:
                with lock:
                    self.stdout.write(f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_map)} объектов')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_data_parallel_generic(self, ids_list, endpoint, model_class, emoji, name, debug=False):
        """Универсальный метод для параллельной загрузки данных"""

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_batch_template(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                endpoint, model_class
            )

        return self._batch_processor(ids_list, process_batch, emoji, name, debug)

    def _process_covers_batch(self, batch_num, batch_ids, cover_map, lock, total_batches, name, debug):
        """Обрабатывает пачку обложек"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} объектов')

            id_list = ','.join(map(str, batch_ids))
            query = f'fields id,url,image_id; where id = ({id_list});'
            batch_data = self._rate_limited_request('covers', query, debug=False)

            data_by_id = {item['id']: item for item in batch_data if 'id' in item}

            def process_single_cover(cover_id):
                if cover_id not in data_by_id:
                    return None

                cover_data = data_by_id[cover_id]
                if cover_data.get('image_id'):
                    return (cover_id,
                            f"https://images.igdb.com/igdb/image/upload/t_cover_big/{cover_data['image_id']}.jpg")
                elif cover_data.get('url'):
                    url = cover_data['url']
                    return (cover_id, f"https:{url.replace('thumb', 'cover_big')}")
                return None

            # Параллельная обработка
            batch_map = {}
            with ThreadPoolExecutor(max_workers=min(len(batch_ids), 10)) as executor:
                futures = {executor.submit(process_single_cover, cover_id): cover_id for cover_id in batch_ids}

                for future in as_completed(futures):
                    cover_id = futures[future]
                    try:
                        result = future.result()
                        if result:
                            cover_id, url = result
                            batch_map[cover_id] = url
                    except Exception as e:
                        if debug:
                            with lock:
                                self.stderr.write(f'               ❌ Ошибка future для обложки {cover_id}: {e}')

            with lock:
                cover_map.update(batch_map)

            if debug:
                with lock:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} объектов')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_covers_parallel(self, cover_ids, debug=False):
        """Параллельная загрузка обложек"""
        return self._batch_processor(cover_ids, self._process_covers_batch, '🖼️', 'обложек', debug)

    def _process_screenshots_batch(self, batch_num, batch_game_ids, result_map, lock,
                                   total_batches, name, debug, screenshots_info):
        """Обрабатывает пачку скриншотов"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_game_ids)} игр')

            def process_single_game(game_id):
                try:
                    game_screenshots_count = screenshots_info.get(game_id, 0)
                    if game_screenshots_count <= 0:
                        return (game_id, 0)

                    query = f'fields id,url,image_id,width,height; where game = {game_id}; limit {game_screenshots_count};'
                    screenshots_data = self._rate_limited_request('screenshots', query, debug=False)

                    if not screenshots_data:
                        return (game_id, 0)

                    with self._db_lock:
                        game_obj = Game.objects.filter(igdb_id=game_id).first()

                    if not game_obj:
                        return (game_id, 0)

                    with self._db_lock:
                        existing_ids = set(Screenshot.objects.filter(
                            game=game_obj,
                            igdb_id__in=[s.get('id') for s in screenshots_data if s.get('id')]
                        ).values_list('igdb_id', flat=True))

                    screenshots_to_create = [
                        Screenshot(
                            game=game_obj,
                            igdb_id=s.get('id'),
                            image_url=f"https://images.igdb.com/igdb/image/upload/t_original/{s.get('image_id')}.jpg",
                            width=s.get('width') or 0,
                            height=s.get('height') or 0
                        ) for s in screenshots_data
                        if s.get('id') and s.get('image_id') and s.get('id') not in existing_ids
                    ]

                    if screenshots_to_create:
                        with self._db_lock:
                            Screenshot.objects.bulk_create(screenshots_to_create, batch_size=10)
                        return (game_id, len(screenshots_to_create))
                    else:
                        return (game_id, 0)

                except Exception as e:
                    if debug:
                        with lock:
                            self.stderr.write(f'            ❌ Ошибка загрузки скриншотов для игры {game_id}: {e}')
                    return (game_id, 0)

            # Параллельная обработка игр
            batch_map = {}
            with ThreadPoolExecutor(max_workers=min(len(batch_game_ids), 10)) as executor:
                futures = {executor.submit(process_single_game, game_id): game_id for game_id in batch_game_ids}

                for future in as_completed(futures):
                    game_id = futures[future]
                    try:
                        game_id, count = future.result()
                        if count > 0:
                            batch_map[game_id] = count
                    except Exception as e:
                        if debug:
                            with lock:
                                self.stderr.write(f'            ❌ Ошибка future для игры {game_id}: {e}')

            with lock:
                result_map.update(batch_map)

            if debug:
                total_screenshots = sum(batch_map.values())
                with lock:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {total_screenshots} скриншотов из {len(batch_map)} игр'
                    )

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_screenshots_parallel(self, game_ids, screenshots_info, debug=False):
        """Параллельная загрузка скриншотов"""
        if not game_ids:
            return 0

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_screenshots_batch(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug, screenshots_info
            )

        result_map = self._batch_processor(game_ids, process_batch, '📸', 'скриншотов', debug)

        total_screenshots = sum(result_map.values()) if result_map else 0
        if debug:
            games_with_screenshots = len([v for v in result_map.values() if v > 0])
            self.stdout.write(
                f'      📸 Всего загружено {total_screenshots} скриншотов для {games_with_screenshots}/{len(game_ids)} игр')

        return total_screenshots

    def _process_additional_data_batch(self, batch_num, batch_game_ids, result_map, lock, total_batches, name, debug):
        """Обрабатывает пачку дополнительных данных"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_game_ids)} игр')

            id_list = ','.join(map(str, batch_game_ids))
            query = f'''
                fields id,name,collections,franchises,involved_companies.company,
                       involved_companies.developer,involved_companies.publisher,
                       themes,player_perspectives,game_modes;
                where id = ({id_list});
            '''
            batch_data = self._rate_limited_request('games', query, debug=False)

            data_by_id = {item['id']: item for item in batch_data if 'id' in item}

            def process_single_game(game_id):
                if game_id not in data_by_id:
                    return None
                return (game_id, data_by_id[game_id])

            # Параллельная обработка
            batch_map = {}
            with ThreadPoolExecutor(max_workers=min(len(batch_game_ids), 10)) as executor:
                futures = {executor.submit(process_single_game, game_id): game_id for game_id in batch_game_ids}

                for future in as_completed(futures):
                    game_id = futures[future]
                    try:
                        result = future.result()
                        if result:
                            game_id, game_data = result
                            batch_map[game_id] = game_data
                    except Exception as e:
                        if debug:
                            with lock:
                                self.stderr.write(f'               ❌ Ошибка future для игры {game_id}: {e}')

            with lock:
                result_map.update(batch_map)

            if debug:
                with lock:
                    self.stdout.write(f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} игр')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_additional_data_parallel(self, game_ids, debug=False):
        """Параллельная загрузка дополнительных данных"""
        return self._batch_processor(game_ids, self._process_additional_data_batch, '📚', 'доп. данных', debug)

    def load_and_process_additional_data(self, game_ids, debug=False):
        """Загружает и обрабатывает дополнительные данные"""
        additional_data_map = self.load_additional_data_parallel(game_ids, debug)

        all_series_ids = set()
        all_company_ids = set()
        all_theme_ids = set()
        all_perspective_ids = set()
        all_mode_ids = set()

        for data in additional_data_map.values():
            if data.get('collections'):
                all_series_ids.update(data['collections'])
            if data.get('themes'):
                all_theme_ids.update(data['themes'])
            if data.get('player_perspectives'):
                all_perspective_ids.update(data['player_perspectives'])
            if data.get('game_modes'):
                all_mode_ids.update(data['game_modes'])
            if data.get('involved_companies'):
                all_company_ids.update(c.get('company') for c in data['involved_companies'] if c.get('company'))

        return additional_data_map, {
            'all_series_ids': list(all_series_ids),
            'all_company_ids': list(all_company_ids),
            'all_theme_ids': list(all_theme_ids),
            'all_perspective_ids': list(all_perspective_ids),
            'all_mode_ids': list(all_mode_ids)
        }

    def _process_companies_with_country_batch(self, batch_num, batch_ids, company_map, lock,
                                              total_batches, name, debug):
        """Обрабатывает пачку компаний с информацией о стране"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} компаний')

            id_list = ','.join(map(str, batch_ids))
            query = f'fields id,name,country; where id = ({id_list});'
            batch_data = self._rate_limited_request('companies', query, debug=False)

            data_by_id = {item['id']: item for item in batch_data if 'id' in item}

            def process_single_company(company_id):
                if company_id not in data_by_id:
                    return None

                company_data = data_by_id[company_id]
                company_name = company_data.get('name', f'Company {company_id}')
                country_code = company_data.get('country')

                with self._db_lock:
                    existing = Company.objects.filter(igdb_id=company_id).first()

                    if existing:
                        needs_update = False
                        if not existing.name.strip() and company_name.strip():
                            existing.name = company_name
                            needs_update = True

                        if country_code and not existing.country:
                            country_obj, _ = Country.objects.get_or_create(
                                code=country_code,
                                defaults={'name': f'Country {country_code}'}
                            )
                            existing.country = country_obj
                            needs_update = True

                        if needs_update:
                            existing.save()
                        return (company_id, existing)
                    else:
                        company = Company.objects.create(igdb_id=company_id, name=company_name)
                        if country_code:
                            country_obj, _ = Country.objects.get_or_create(
                                code=country_code,
                                defaults={'name': f'Country {country_code}'}
                            )
                            company.country = country_obj
                            company.save()
                        return (company_id, company)

            # Параллельная обработка
            batch_map = {}
            with ThreadPoolExecutor(max_workers=min(len(batch_ids), 10)) as executor:
                futures = {executor.submit(process_single_company, company_id): company_id for company_id in batch_ids}

                for future in as_completed(futures):
                    company_id = futures[future]
                    try:
                        result = future.result()
                        if result:
                            company_id, company = result
                            batch_map[company_id] = company
                    except Exception as e:
                        if debug:
                            with lock:
                                self.stderr.write(f'               ❌ Ошибка future для компании {company_id}: {e}')

            with lock:
                company_map.update(batch_map)

            if debug:
                with lock:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} компаний')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_companies_with_country_parallel(self, company_ids, debug=False):
        """Параллельная загрузка компаний с информацией о стране"""
        if not company_ids:
            if debug:
                self.stdout.write('   ⚠️  Нет ID компаний для загрузки')
            return {}

        if debug:
            self.stdout.write(f'   🏢 Загрузка {len(company_ids)} компаний с информацией о стране...')

        return self._batch_processor(company_ids, self._process_companies_with_country_batch, '🏢',
                                     'компаний со странами', debug)

    def create_basic_games(self, games_data_list, debug=False):
        """Создает игры с основными данными"""
        from .base_command import BaseIgdbCommand
        base_command = BaseIgdbCommand()

        with self._db_lock:
            existing_game_ids = set(Game.objects.filter(
                igdb_id__in=[g.get('id') for g in games_data_list if g.get('id')]
            ).values_list('igdb_id', flat=True))

        def process_single_game(game_data):
            game_id = game_data.get('id')
            if not game_id or game_id in existing_game_ids:
                return None
            return base_command.create_game_object(game_data, {})

        # Параллельная обработка
        games_to_create = []
        with ThreadPoolExecutor(max_workers=min(len(games_data_list), 10)) as executor:
            futures = [executor.submit(process_single_game, game_data) for game_data in games_data_list]

            for future in as_completed(futures):
                try:
                    game = future.result()
                    if game:
                        games_to_create.append(game)
                except Exception as e:
                    if debug:
                        self.stderr.write(f'   ❌ Ошибка future создания игры: {e}')

        if games_to_create:
            with self._db_lock:
                Game.objects.bulk_create(games_to_create, batch_size=50)

        return len(games_to_create), {game.igdb_id: game for game in games_to_create}

    def update_games_with_covers(self, game_basic_map, cover_map, game_data_map, debug=False):
        """Обновляет игры обложками"""
        with self._db_lock:
            games = list(Game.objects.filter(igdb_id__in=game_basic_map.keys()))

        def process_single_game(game):
            game_data = game_data_map.get(game.igdb_id)
            if game_data and game_data.get('cover'):
                cover_id = game_data['cover']
                if cover_id in cover_map and cover_map[cover_id] != game.cover_url:
                    game.cover_url = cover_map[cover_id]
                    return game
            return None

        # Параллельная обработка
        games_to_update = []
        with ThreadPoolExecutor(max_workers=min(len(games), 10)) as executor:
            futures = [executor.submit(process_single_game, game) for game in games]

            for future in as_completed(futures):
                try:
                    game = future.result()
                    if game:
                        games_to_update.append(game)
                except Exception as e:
                    if debug:
                        self.stderr.write(f'   ❌ Ошибка future обновления игры: {e}')

        if games_to_update:
            with self._db_lock:
                Game.objects.bulk_update(games_to_update, ['cover_url'], batch_size=50)

        return len(games_to_update)

    def load_all_data_types_sequentially(self, collected_data, debug=False):
        """Последовательно загружает все типы данных"""
        step_times = {}
        data_maps = {}

        steps = [
            ('🖼️  Обложки', 'covers', lambda: self.load_covers_parallel(collected_data['all_cover_ids'], debug),
             'cover_map'),
            ('🎭 Жанры', 'genres', lambda: self.load_data_parallel_generic(
                collected_data['all_genre_ids'], 'genres', Genre, '🎭', 'жанров', debug), 'genre_map'),
            ('🖥️  Платформы', 'platforms', lambda: self.load_data_parallel_generic(
                collected_data['all_platform_ids'], 'platforms', Platform, '🖥️', 'платформ', debug), 'platform_map'),
            ('🔑 Ключевые слова', 'keywords', lambda: self.load_data_parallel_generic(
                collected_data['all_keyword_ids'], 'keywords', Keyword, '🔑', 'ключевых слов', debug), 'keyword_map'),
            ('📚 Серии', 'series', lambda: self.load_data_parallel_generic(
                collected_data['all_series_ids'], 'collections', Series, '📚', 'серий', debug), 'series_map'),
            ('🏢 Компании', 'companies', lambda: self.load_companies_with_country_parallel(
                collected_data['all_company_ids'], debug), 'company_map'),
            ('🎨 Темы', 'themes', lambda: self.load_data_parallel_generic(
                collected_data['all_theme_ids'], 'themes', Theme, '🎨', 'тем', debug), 'theme_map'),
            ('👁️  Перспективы', 'perspectives', lambda: self.load_data_parallel_generic(
                collected_data['all_perspective_ids'], 'player_perspectives', PlayerPerspective, '👁️', 'перспектив',
                debug), 'perspective_map'),
            ('🎮 Режимы', 'modes', lambda: self.load_data_parallel_generic(
                collected_data['all_mode_ids'], 'game_modes', GameMode, '🎮', 'режимов', debug), 'mode_map'),
        ]

        for i, (display_name, key, load_func, map_key) in enumerate(steps, 1):
            if debug:
                self.stdout.write(
                    f'\n{i}️⃣  {display_name.split()[0]} {display_name.split()[1] if len(display_name.split()) > 1 else ""}...')

            start_step = time.time()
            data_maps[map_key] = load_func()
            step_times[key] = time.time() - start_step

        return data_maps, step_times