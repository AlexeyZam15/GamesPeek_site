import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from games.igdb_api import make_igdb_request
from games.models import (
    Game, Genre, Keyword, Platform, Series,
    Company, Theme, PlayerPerspective, GameMode, Screenshot
)


class DataLoader:
    """Класс для загрузки данных из IGDB"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr

    def _batch_processor(self, ids_list, process_batch_func, emoji, name, debug=False):
        """Универсальный метод для обработки данных пачками"""
        if not ids_list:
            return {}

        result_map = {}
        lock = threading.Lock()

        # Разбиваем на пачки по 10
        batches = [ids_list[i:i + 10] for i in range(0, len(ids_list), 10)]
        total_batches = len(batches)

        if debug:
            self.stdout.write(f'      {emoji} Загрузка {name}: {len(ids_list)} объектов, {total_batches} пачек')

        # Запускаем параллельную обработку
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
            total = len(ids_list)
            self.stdout.write(f'      {emoji} Всего загружено {name}: {loaded}/{total} (из {total_batches} пачек)')

        return result_map

    def _process_generic_batch(self, batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                               endpoint, create_func):
        """Обрабатывает пачку данных для универсальной загрузки"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} объектов')

            id_list = ','.join(map(str, batch_ids))
            query = f'fields id,name; where id = ({id_list});'

            batch_data = make_igdb_request(endpoint, query, debug=False)

            batch_map = {}
            processed_ids = set()
            for item_data in batch_data:
                item_id = item_data.get('id')
                if not item_id:
                    continue

                processed_ids.add(item_id)
                item_name = item_data.get('name', f'{name} {item_id}')
                item = create_func(item_id, item_name)
                batch_map[item_id] = item

            # Отладка: проверяем какие ID не были обработаны
            if debug:
                with lock:
                    all_batch_ids = set(batch_ids)
                    missing_ids = all_batch_ids - processed_ids
                    if missing_ids:
                        self.stdout.write(
                            f'         ⚠️  {name} пачка {batch_num}: {len(missing_ids)} ID не получены от API')

            with lock:
                result_map.update(batch_map)

            if debug:
                with lock:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} объектов')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_data_parallel_generic(self, ids_list, endpoint, model_class, create_func, emoji, name, debug=False):
        """Универсальный метод для параллельной загрузки данных"""

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_generic_batch(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug, endpoint, create_func
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

            batch_data = make_igdb_request('covers', query, debug=False)

            batch_map = {}
            for cover_data in batch_data:
                cover_id = cover_data.get('id')
                if not cover_id:
                    continue

                if cover_data.get('image_id'):
                    high_res_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{cover_data['image_id']}.jpg"
                    batch_map[cover_id] = high_res_url
                elif cover_data.get('url'):
                    url = cover_data['url']
                    high_res_url = f"https:{url.replace('thumb', 'cover_big')}"
                    batch_map[cover_id] = high_res_url

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

    def _load_game_screenshots(self, game_id, screenshots_count, debug=False):
        """Загружает ВСЕ скриншоты для одной игры, но пачками"""
        try:
            # Если скриншотов нет, пропускаем
            if not screenshots_count or screenshots_count == 0:
                return 0

            # Загружаем ВСЕ скриншоты для игры
            query = f'fields id,url,image_id,width,height; where game = {game_id}; limit {screenshots_count};'
            screenshots_data = make_igdb_request('screenshots', query, debug=False)

            if not screenshots_data:
                return 0

            screenshots_to_create = []
            game_obj = Game.objects.filter(igdb_id=game_id).first()
            if not game_obj:
                return 0

            for screenshot_data in screenshots_data:
                image_id = screenshot_data.get('image_id')
                if image_id:
                    width = screenshot_data.get('width') or 0
                    height = screenshot_data.get('height') or 0

                    screenshot_obj = Screenshot(
                        game=game_obj,
                        igdb_id=screenshot_data.get('id'),
                        image_url=f"https://images.igdb.com/igdb/image/upload/t_original/{image_id}.jpg",
                        width=width,
                        height=height
                    )
                    screenshots_to_create.append(screenshot_obj)

            if screenshots_to_create:
                # Сохраняем пачками по 10 (как вам нужно)
                for i in range(0, len(screenshots_to_create), 10):
                    batch = screenshots_to_create[i:i + 10]
                    Screenshot.objects.bulk_create(batch, ignore_conflicts=True)

            return len(screenshots_to_create)

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка загрузки скриншотов для игры {game_id}: {e}')
            return 0

    def _process_screenshots_batch(self, batch_num, batch_game_ids, result_map, lock,
                                   total_batches, name, debug, screenshots_info):
        """Обрабатывает пачку скриншотов, зная сколько их у каждой игры"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_game_ids)} игр')

            batch_screenshots = 0

            for game_id in batch_game_ids:
                try:
                    # Получаем количество скриншотов для этой игры
                    game_screenshots_count = screenshots_info.get(game_id, 0)

                    if game_screenshots_count > 0:
                        screenshots = self._load_game_screenshots(
                            game_id, game_screenshots_count, debug=debug
                        )
                        with lock:
                            result_map[game_id] = screenshots
                            batch_screenshots += screenshots

                        if debug and screenshots != game_screenshots_count:
                            with lock:
                                self.stdout.write(
                                    f'         ⚠️  Игра {game_id}: загружено {screenshots}/{game_screenshots_count} скриншотов'
                                )
                    else:
                        if debug:
                            with lock:
                                self.stdout.write(f'         ℹ️  Игра {game_id}: нет скриншотов')

                except Exception as e:
                    if debug:
                        with lock:
                            self.stderr.write(f'         ❌ Ошибка скриншотов для игры {game_id}: {e}')

            if debug:
                with lock:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {batch_screenshots} скриншотов'
                    )

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_screenshots_parallel(self, game_ids, screenshots_info, debug=False):
        """Параллельная загрузка скриншотов с учетом информации о количестве"""
        if not game_ids:
            return 0

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_screenshots_batch(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug, screenshots_info
            )

        result_map = self._batch_processor(game_ids, process_batch, '📸', 'скриншотов', debug)

        total_screenshots = sum(result_map.values()) if result_map else 0
        return total_screenshots

    def _process_additional_data_batch(self, batch_num, batch_game_ids, result_map, lock, total_batches, name, debug):
        """Обрабатывает пачку дополнительных данных"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_game_ids)} игр')

            id_list = ','.join(map(str, batch_game_ids))
            query = f'''
                fields name,collections,franchises,involved_companies.company,
                       involved_companies.developer,involved_companies.publisher,
                       themes,player_perspectives,game_modes;
                where id = ({id_list});
            '''

            batch_data = make_igdb_request('games', query, debug=False)

            with lock:
                for game_data in batch_data:
                    game_id = game_data.get('id')
                    if game_id:
                        result_map[game_id] = game_data

                if debug:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} игр')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_additional_data_parallel(self, game_ids, debug=False):
        """Параллельная загрузка дополнительных данных пачками по 10"""
        return self._batch_processor(game_ids, self._process_additional_data_batch, '📚', 'доп. данных', debug)

    def load_and_process_additional_data(self, game_ids, debug=False):
        """Загружает и обрабатывает дополнительные данные"""
        additional_data_map = self.load_additional_data_parallel(game_ids, debug)

        # Собираем ID дополнительных данных
        all_series_ids = set()
        all_company_ids = set()
        all_theme_ids = set()
        all_perspective_ids = set()
        all_mode_ids = set()

        for additional_data in additional_data_map.values():
            if additional_data.get('collections'):
                all_series_ids.update(additional_data['collections'])

            if additional_data.get('themes'):
                all_theme_ids.update(additional_data['themes'])

            if additional_data.get('player_perspectives'):
                all_perspective_ids.update(additional_data['player_perspectives'])

            if additional_data.get('game_modes'):
                all_mode_ids.update(additional_data['game_modes'])

            if additional_data.get('involved_companies'):
                for company_data in additional_data['involved_companies']:
                    if company_data.get('company'):
                        all_company_ids.add(company_data['company'])

        return additional_data_map, {
            'all_series_ids': list(all_series_ids),
            'all_company_ids': list(all_company_ids),
            'all_theme_ids': list(all_theme_ids),
            'all_perspective_ids': list(all_perspective_ids),
            'all_mode_ids': list(all_mode_ids)
        }

    def create_model_func(self, model_class, debug=False):
        """Универсальная функция создания моделей с отладкой"""
        model_name = model_class.__name__

        def create_func(item_id, item_name):
            try:
                # Пытаемся найти существующий объект
                existing = model_class.objects.filter(igdb_id=item_id).first()

                if existing:
                    # Проверяем, нужно ли обновить имя
                    needs_update = False

                    # Если имя по умолчанию ("Series XXXX"), обновляем
                    if model_name == 'Series' and existing.name.startswith(f'{model_name} '):
                        needs_update = True
                    # Если текущее имя пустое, а новое не пустое
                    elif not existing.name.strip() and item_name.strip():
                        needs_update = True

                    if needs_update:
                        if debug:
                            self.stdout.write(
                                f'      🔄 Обновление {model_name} {item_id}: "{existing.name}" → "{item_name}"')
                        existing.name = item_name
                        existing.save()

                    return existing
                else:
                    # Создаем новый объект
                    if debug and model_name == 'Series':
                        self.stdout.write(f'      ✨ Создание новой серии {item_id}: "{item_name}"')

                    obj = model_class.objects.create(
                        igdb_id=item_id,
                        name=item_name
                    )
                    return obj

            except Exception as e:
                if debug:
                    self.stderr.write(f'      ❌ Ошибка создания {model_name} {item_id}: {e}')
                # В случае ошибки, пытаемся получить или создать с минимальными данными
                obj, created = model_class.objects.get_or_create(
                    igdb_id=item_id,
                    defaults={'name': item_name}
                )
                return obj

        return create_func

    def create_series_func(self, debug=False):
        """Специальная функция для создания серий с детальной отладкой"""

        def create_series(item_id, item_name):
            # Сначала проверяем, есть ли уже такая серия
            existing = Series.objects.filter(igdb_id=item_id).first()

            if existing:
                # Если серия уже существует, проверяем нужно ли обновить имя
                if existing.name != item_name:
                    # Проверяем, не является ли текущее имя именем по умолчанию
                    if existing.name.startswith('Series ') and not item_name.startswith('Series '):
                        # Обновляем имя
                        existing.name = item_name
                        existing.save()
                        if debug:
                            self.stdout.write(
                                f'         🔄 Обновлена серия {item_id}: "{existing.name}" → "{item_name}"')
                return existing
            else:
                # Создаем новую серию
                try:
                    series = Series.objects.create(
                        igdb_id=item_id,
                        name=item_name
                    )
                    if debug:
                        self.stdout.write(f'         ✨ Создана серия {item_id}: "{item_name}"')
                    return series
                except Exception as e:
                    if debug:
                        self.stderr.write(f'         ❌ Ошибка создания серии {item_id}: {e}')
                    # Пробуем создать с минимальными данными
                    series, _ = Series.objects.get_or_create(
                        igdb_id=item_id,
                        defaults={'name': item_name}
                    )
                    return series

        return create_series

    def create_basic_games(self, games_data_list, debug=False):
        """Создает игры с основными данными"""
        from .base_command import BaseIgdbCommand

        base_command = BaseIgdbCommand()
        games_basic_to_create = []
        game_basic_map = {}

        for game_data in games_data_list:
            game_id = game_data.get('id')
            if not game_id:
                continue

            if Game.objects.filter(igdb_id=game_id).exists():
                continue

            try:
                game = base_command.create_game_object(game_data, {})
                games_basic_to_create.append(game)
                game_basic_map[game_id] = game

            except Exception as e:
                if debug:
                    self.stderr.write(f'   ❌ Ошибка создания игры {game_id}: {e}')

        # Сохраняем игры в базу
        if games_basic_to_create:
            Game.objects.bulk_create(games_basic_to_create)

        return len(games_basic_to_create), game_basic_map

    def update_games_with_covers(self, game_basic_map, cover_map, game_data_map, debug=False):
        """Обновляет игры обложками"""
        games_to_update = []

        for game in Game.objects.filter(igdb_id__in=game_basic_map.keys()):
            game_data = game_data_map.get(game.igdb_id)
            if game_data and game_data.get('cover'):
                cover_id = game_data['cover']
                if cover_id in cover_map:
                    game.cover_url = cover_map[cover_id]
                    games_to_update.append(game)

        if games_to_update:
            Game.objects.bulk_update(games_to_update, ['cover_url'])

        return len(games_to_update)

    def load_all_data_types_sequentially(self, collected_data, debug=False):
        """Последовательно загружает все типы данных"""
        step_times = {}
        data_maps = {}

        # 1️⃣ ПАРАЛЛЕЛЬНАЯ загрузка обложек
        if debug:
            self.stdout.write('\n1️⃣  🖼️  ЗАГРУЗКА ОБЛОЖЕК...')
        start_step = time.time()
        data_maps['cover_map'] = self.load_covers_parallel(collected_data['all_cover_ids'], debug)
        step_times['covers'] = time.time() - start_step

        # 2️⃣ ПАРАЛЛЕЛЬНАЯ загрузка жанров
        if debug:
            self.stdout.write('\n2️⃣  🎭 ЗАГРУЗКА ЖАНРОВ...')
        start_step = time.time()
        data_maps['genre_map'] = self.load_data_parallel_generic(
            collected_data['all_genre_ids'], 'genres', Genre,
            self.create_model_func(Genre, debug), '🎭', 'жанров', debug
        )
        step_times['genres'] = time.time() - start_step

        # 3️⃣ ПАРАЛЛЕЛЬНАЯ загрузка платформ
        if debug:
            self.stdout.write('\n3️⃣  🖥️  ЗАГРУЗКА ПЛАТФОРМ...')
        start_step = time.time()
        data_maps['platform_map'] = self.load_data_parallel_generic(
            collected_data['all_platform_ids'], 'platforms', Platform,
            self.create_model_func(Platform, debug), '🖥️', 'платформ', debug
        )
        step_times['platforms'] = time.time() - start_step

        # 4️⃣ ПАРАЛЛЕЛЬНАЯ загрузка ключевых слов
        if debug:
            self.stdout.write('\n4️⃣  🔑 ЗАГРУЗКА КЛЮЧЕВЫХ СЛОВ...')
        start_step = time.time()
        data_maps['keyword_map'] = self.load_data_parallel_generic(
            collected_data['all_keyword_ids'], 'keywords', Keyword,
            self.create_model_func(Keyword, debug), '🔑', 'ключевых слов', debug
        )
        step_times['keywords'] = time.time() - start_step

        # 5️⃣ ПАРАЛЛЕЛЬНАЯ загрузка серий
        if debug:
            self.stdout.write('\n5️⃣  📚 ЗАГРУЗКА СЕРИЙ...')

        start_step = time.time()

        # Проверяем данные перед загрузкой
        series_ids = collected_data['all_series_ids']

        # Загружаем серии с отладкой
        data_maps['series_map'] = self.load_data_parallel_generic(
            series_ids, 'collections', Series,
            self.create_series_func(debug), '📚', 'серий', debug
        )

        step_times['series'] = time.time() - start_step

        # 6️⃣ ПАРАЛЛЕЛЬНАЯ загрузка компаний
        if debug:
            self.stdout.write('\n6️⃣  🏢 ЗАГРУЗКА КОМПАНИЙ...')
        start_step = time.time()
        data_maps['company_map'] = self.load_data_parallel_generic(
            collected_data['all_company_ids'], 'companies', Company,
            self.create_model_func(Company, debug), '🏢', 'компаний', debug
        )
        step_times['companies'] = time.time() - start_step

        # 7️⃣ ПАРАЛЛЕЛЬНАЯ загрузка тем
        if debug:
            self.stdout.write('\n7️⃣  🎨 ЗАГРУЗКА ТЕМ...')
        start_step = time.time()
        data_maps['theme_map'] = self.load_data_parallel_generic(
            collected_data['all_theme_ids'], 'themes', Theme,
            self.create_model_func(Theme, debug), '🎨', 'тем', debug
        )
        step_times['themes'] = time.time() - start_step

        # 8️⃣ ПАРАЛЛЕЛЬНАЯ загрузка перспектив
        if debug:
            self.stdout.write('\n8️⃣  👁️  ЗАГРУЗКА ПЕРСПЕКТИВ...')
        start_step = time.time()
        data_maps['perspective_map'] = self.load_data_parallel_generic(
            collected_data['all_perspective_ids'], 'player_perspectives', PlayerPerspective,
            self.create_model_func(PlayerPerspective, debug), '👁️', 'перспектив', debug
        )
        step_times['perspectives'] = time.time() - start_step

        # 9️⃣ ПАРАЛЛЕЛЬНАЯ загрузка режимов
        if debug:
            self.stdout.write('\n9️⃣  🎮 ЗАГРУЗКА РЕЖИМОВ...')
        start_step = time.time()
        data_maps['mode_map'] = self.load_data_parallel_generic(
            collected_data['all_mode_ids'], 'game_modes', GameMode,
            self.create_model_func(GameMode, debug), '🎮', 'режимов', debug
        )
        step_times['modes'] = time.time() - start_step

        # 🔟 Выводим общую статистику загрузки
        if debug:
            self.stdout.write('\n📊 ОБЩАЯ СТАТИСТИКА ЗАГРУЗКИ ДАННЫХ:')
            self.stdout.write('   ────────────────────────────────')

            data_types = [
                ('🖼️  Обложки', 'covers', len(data_maps.get('cover_map', {})),
                 len(collected_data.get('all_cover_ids', []))),
                ('🎭 Жанры', 'genres', len(data_maps.get('genre_map', {})),
                 len(collected_data.get('all_genre_ids', []))),
                ('🖥️  Платформы', 'platforms', len(data_maps.get('platform_map', {})),
                 len(collected_data.get('all_platform_ids', []))),
                ('🔑 Ключевые слова', 'keywords', len(data_maps.get('keyword_map', {})),
                 len(collected_data.get('all_keyword_ids', []))),
                ('📚 Серии', 'series', len(data_maps.get('series_map', {})),
                 len(collected_data.get('all_series_ids', []))),
                ('🏢 Компании', 'companies', len(data_maps.get('company_map', {})),
                 len(collected_data.get('all_company_ids', []))),
                ('🎨 Темы', 'themes', len(data_maps.get('theme_map', {})), len(collected_data.get('all_theme_ids', []))),
                ('👁️  Перспективы', 'perspectives', len(data_maps.get('perspective_map', {})),
                 len(collected_data.get('all_perspective_ids', []))),
                ('🎮 Режимы', 'modes', len(data_maps.get('mode_map', {})), len(collected_data.get('all_mode_ids', []))),
            ]

            for display_name, key, loaded, total in data_types:
                if total > 0:
                    success_rate = (loaded / total) * 100
                    time_val = step_times.get(key, 0)
                    self.stdout.write(f'   • {display_name}: {loaded}/{total} ({success_rate:.1f}%) [{time_val:.2f}с]')

            # Суммарное время
            total_load_time = sum(step_times.values())
            self.stdout.write(f'   ⏱️  Общее время загрузки: {total_load_time:.2f}с')

        return data_maps, step_times