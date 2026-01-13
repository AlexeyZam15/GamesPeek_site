# games/management/commands/load_igdb/data_loader.py
import time
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from games.igdb_api import make_igdb_request
from games.models import (
    Game, Genre, Keyword, Platform, Series,
    Company, Theme, PlayerPerspective, GameMode, Screenshot
)


# data_loader.py - исправленные методы

class DataLoader:
    """Класс для загрузки данных из IGDB"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr
        self._db_lock = threading.Lock()
        self._api_lock = threading.Lock()
        self._last_request_time = 0
        self._min_request_interval = 0.25
        self._retry_delay = 2.0
        self._interrupted = threading.Event()

    def set_interrupted(self):
        """Устанавливает флаг прерывания"""
        self._interrupted.set()

    def is_interrupted(self):
        """Проверяет прерывание"""
        return self._interrupted.is_set()

    def _batch_processor_weighted(self, items_list, process_batch_func, emoji, name,
                                  weight_calculator_func, extra_data=None, debug=False):
        """Универсальный метод для обработки данных с учетом веса"""
        if not items_list or self.is_interrupted():
            if debug and self.is_interrupted():
                self.stdout.write(f'      ⏹️  Прерывание: пропускаем загрузку {name}')
            return {}

        # Создаем пачки с учетом веса
        batches = self._create_weighted_batches(
            items_list, weight_calculator_func, extra_data, debug=debug
        )
        total_batches = len(batches)

        if debug:
            weights = []
            for item in items_list:
                if extra_data:
                    weight = weight_calculator_func(item, extra_data)
                else:
                    weight = weight_calculator_func(item)
                weights.append(weight)

            total_weight = sum(weights)
            avg_weight = total_weight / len(items_list) if items_list else 0

            batch_sizes = [len(batch) for batch in batches]
            avg_batch_size = sum(batch_sizes) / total_batches if total_batches > 0 else 0

            single_batches = sum(1 for size in batch_sizes if size == 1)
            multi_batches = total_batches - single_batches

            self.stdout.write(f'      {emoji} Загрузка {name}: {len(items_list)} объектов, {total_batches} пачек')
            self.stdout.write(f'      📊 Средний вес: {avg_weight:.1f}')
            self.stdout.write(f'      📦 Средний размер пачки: {avg_batch_size:.1f}')
            self.stdout.write(f'      🔢 Пачек по 1 элементу: {single_batches}, пачек > 1: {multi_batches}')

        result_map = {}
        lock = threading.Lock()

        # Адаптивное количество воркеров
        if total_batches <= 3:
            max_workers = total_batches
        elif any(len(batch) == 1 for batch in batches):
            max_workers = min(total_batches, 8)
        else:
            max_workers = min(total_batches, 6)

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for batch_num, batch_items in enumerate(batches, 1):
                    # Проверка прерывания перед созданием задачи
                    if self.is_interrupted():
                        if debug:
                            self.stdout.write(f'      ⏹️  Прерывание: отменяем создание пачек {name}')
                        break

                    future = executor.submit(
                        process_batch_func,
                        batch_num, batch_items, result_map,
                        lock, total_batches, name, debug
                    )
                    futures.append(future)

                # Обработка с проверкой прерывания и таймаутом
                for future in as_completed(futures):
                    if self.is_interrupted():
                        if debug:
                            self.stdout.write(f'      ⏹️  Прерывание: прерываем выполнение {name}')
                        # Отменяем оставшиеся фьючерсы
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break

                    try:
                        future.result(timeout=60)  # Таймаут 60 секунд
                    except TimeoutError:
                        if debug:
                            with lock:
                                self.stderr.write(f'      ⏱️  Таймаут выполнения пачки {name}')
                    except Exception as e:
                        if debug:
                            with lock:
                                self.stderr.write(f'      ❌ Ошибка в потоке: {e}')

        except RuntimeError as e:
            if "cannot schedule new futures after interpreter shutdown" in str(e):
                if debug:
                    self.stderr.write(f'      ⏹️  Рантайм ошибка: интерпретатор завершает работу')
            else:
                raise

        if debug:
            loaded = len(result_map)
            self.stdout.write(
                f'      {emoji} Всего загружено {name}: {loaded}/{len(items_list)} (из {total_batches} пачек)')

        return result_map

    def _batch_processor_regular(self, ids_list, process_batch_func, emoji, name, debug=False):
        """Универсальный метод для обработки данных без учета веса"""
        if not ids_list or self.is_interrupted():
            if debug and self.is_interrupted():
                self.stdout.write(f'      ⏹️  Прерывание: пропускаем загрузку {name}')
            return {}

        result_map = {}
        lock = threading.Lock()
        batches = [ids_list[i:i + 10] for i in range(0, len(ids_list), 10)]
        total_batches = len(batches)

        if debug:
            self.stdout.write(f'      {emoji} Загрузка {name}: {len(ids_list)} объектов, {total_batches} пачек')

        max_workers = min(total_batches, 5)

        try:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for batch_num, batch_ids in enumerate(batches, 1):
                    # Проверка прерывания перед созданием задачи
                    if self.is_interrupted():
                        if debug:
                            self.stdout.write(f'      ⏹️  Прерывание: отменяем создание пачек {name}')
                        break

                    future = executor.submit(
                        process_batch_func, batch_num, batch_ids, result_map, lock, total_batches, name, debug
                    )
                    futures.append(future)

                for future in as_completed(futures):
                    if self.is_interrupted():
                        if debug:
                            self.stdout.write(f'      ⏹️  Прерывание: прерываем выполнение {name}')
                        # Отменяем оставшиеся фьючерсы
                        for f in futures:
                            if not f.done():
                                f.cancel()
                        break

                    try:
                        future.result(timeout=60)  # Таймаут 60 секунд
                    except TimeoutError:
                        if debug:
                            with lock:
                                self.stderr.write(f'      ⏱️  Таймаут выполнения пачки {name}')
                    except Exception as e:
                        if debug:
                            with lock:
                                self.stderr.write(f'      ❌ Ошибка в потоке: {e}')

        except RuntimeError as e:
            if "cannot schedule new futures after interpreter shutdown" in str(e):
                if debug:
                    self.stderr.write(f'      ⏹️  Рантайм ошибка: интерпретатор завершает работу')
            else:
                raise

        if debug:
            loaded = len(result_map)
            self.stdout.write(
                f'      {emoji} Всего загружено {name}: {loaded}/{len(ids_list)} (из {total_batches} пачек)')

        return result_map

    def load_all_data_types_sequentially(self, collected_data, debug=False):
        """Последовательно загружает все типы данных"""
        if self.is_interrupted():
            if debug:
                self.stdout.write('⏹️  Прерывание: пропускаем загрузку всех типов данных')
            return {}, {}

        step_times = {}
        data_maps = {}

        steps = [
            ('🖼️  Обложки', 'covers', lambda: self.load_covers_parallel(collected_data['all_cover_ids'], debug),
             'cover_map'),
            ('🎭 Жанры', 'genres', lambda: self.load_data_parallel_generic(
                collected_data['all_genre_ids'], 'genres', Genre, '🎭', 'жанров', debug), 'genre_map'),
            ('🖥️  Платформы', 'platforms', lambda: self.load_data_parallel_generic(
                collected_data['all_platform_ids'], 'platforms', Platform, '🖥️', 'платформ', debug), 'platform_map'),
            ('🔑 Ключевые слова', 'keywords', lambda: self.load_keywords_parallel_with_weights(
                collected_data['all_keyword_ids'], debug), 'keyword_map'),
        ]

        # Загружаем только основные данные
        for i, (display_name, key, load_func, map_key) in enumerate(steps, 1):
            if self.is_interrupted():
                if debug:
                    self.stdout.write(f'⏹️  Прерывание: пропускаем {display_name}')
                break

            if debug:
                self.stdout.write(f'\n{i}️⃣  {display_name}...')

            start_step = time.time()
            data_maps[map_key] = load_func()
            step_times[key] = time.time() - start_step

        # Загружаем дополнительные данные только если не было прерывания
        if not self.is_interrupted():
            additional_steps = [
                ('📚 Серии', 'series', lambda: self.load_data_parallel_generic(
                    collected_data.get('all_series_ids', []), 'collections', Series, '📚', 'серий', debug),
                 'series_map'),
                ('🏢 Компании', 'companies', lambda: self.load_companies_parallel(
                    collected_data.get('all_company_ids', []), debug), 'company_map'),
                ('🎨 Темы', 'themes', lambda: self.load_data_parallel_generic(
                    collected_data.get('all_theme_ids', []), 'themes', Theme, '🎨', 'тем', debug), 'theme_map'),
                ('👁️  Перспективы', 'perspectives', lambda: self.load_data_parallel_generic(
                    collected_data.get('all_perspective_ids', []), 'player_perspectives', PlayerPerspective, '👁️',
                    'перспектив',
                    debug), 'perspective_map'),
                ('🎮 Режимы', 'modes', lambda: self.load_data_parallel_generic(
                    collected_data.get('all_mode_ids', []), 'game_modes', GameMode, '🎮', 'режимов', debug), 'mode_map'),
            ]

            start_idx = len(steps) + 1
            for i, (display_name, key, load_func, map_key) in enumerate(additional_steps, start_idx):
                if self.is_interrupted():
                    if debug:
                        self.stdout.write(f'⏹️  Прерывание: пропускаем {display_name}')
                    break

                if debug:
                    self.stdout.write(f'\n{i}️⃣  {display_name}...')

                start_step = time.time()
                data_maps[map_key] = load_func()
                step_times[key] = time.time() - start_step

        return data_maps, step_times

    def _rate_limited_request(self, endpoint, query, debug=False, max_retries=3):
        """Выполняет запрос к API с rate limiting и retry логикой"""
        # Проверяем и корректируем лимит в запросе
        limit_match = re.search(r'limit\s+(\d+)', query, re.IGNORECASE)
        if limit_match:
            limit_value = int(limit_match.group(1))
            if limit_value > 500:
                if debug:
                    self.stdout.write(f'   ⚠️  Ограничиваю лимит запроса с {limit_value} до 500')
                query = re.sub(r'limit\s+\d+', f'limit 500', query, flags=re.IGNORECASE)

        for attempt in range(max_retries):
            try:
                with self._api_lock:
                    current_time = time.time()
                    time_since_last = current_time - self._last_request_time
                    if time_since_last < self._min_request_interval:
                        sleep_time = self._min_request_interval - time_since_last
                        time.sleep(sleep_time)
                    self._last_request_time = time.time()
                response = make_igdb_request(endpoint, query, debug=debug)
                return response
            except Exception as e:
                error_msg = str(e)
                if "429" in error_msg or "Too Many Requests" in error_msg:
                    wait_time = self._retry_delay * (attempt + 1)
                    time.sleep(wait_time)
                    continue
                else:
                    raise
        raise Exception(f"API request failed after {max_retries} retries")

    def _calculate_data_weight(self, game_data, screenshots_info):
        """Рассчитывает 'вес' данных для игры"""
        if not game_data:
            return 1.0

        weight = 1.0  # Базовая стоимость игры

        # Упрощенный расчет
        if game_data.get('genres'):
            weight += min(len(game_data['genres']) * 0.1, 1.0)

        if game_data.get('platforms'):
            weight += min(len(game_data['platforms']) * 0.1, 1.0)

        if game_data.get('keywords'):
            weight += min(len(game_data['keywords']) * 0.15, 2.0)

        # СКРИНШОТЫ - уменьшено влияние
        game_id = game_data.get('id')
        if game_id and screenshots_info.get(game_id, 0) > 0:
            screenshot_count = screenshots_info[game_id]
            if screenshot_count <= 5:
                weight += screenshot_count * 0.3
            elif screenshot_count <= 15:
                weight += 1.5 + (screenshot_count - 5) * 0.2
            elif screenshot_count <= 30:
                weight += 3.5 + (screenshot_count - 15) * 0.1
            else:
                weight += 6.0 + (screenshot_count - 30) * 0.05

        # Ограничиваем максимальный вес
        return min(weight, 15.0)

    def _create_weighted_batches(self, items_list, weight_calculator_func, extra_data=None,
                                 max_batch_weight=15, min_batch_size=3, max_batch_size=10, debug=False):
        """Создает пачки с учетом веса для ЛЮБОГО типа данных"""
        if not items_list:
            return []

        # Рассчитываем вес для всех элементов
        items_with_weight = []
        for item in items_list:
            if extra_data:
                weight = weight_calculator_func(item, extra_data)
            else:
                weight = weight_calculator_func(item)
            items_with_weight.append((item, weight))

        # Сортируем по весу (от большего к меньшему)
        items_with_weight.sort(key=lambda x: x[1], reverse=True)

        all_batches = []
        current_batch = []
        current_weight = 0

        for item, weight in items_with_weight:
            # Пытаемся добавить в текущую пачку
            can_add = (
                    len(current_batch) < max_batch_size and
                    current_weight + weight <= max_batch_weight
            )

            if can_add:
                current_batch.append(item)
                current_weight += weight
            else:
                # Если пачка не пустая, сохраняем её
                if current_batch:
                    all_batches.append(current_batch)
                # Начинаем новую пачку
                current_batch = [item]
                current_weight = weight

        # Добавляем последнюю пачку
        if current_batch:
            all_batches.append(current_batch)

        # Объединяем очень маленькие пачки
        if len(all_batches) > 1:
            optimized_batches = []
            small_batches = []

            for batch in all_batches:
                if len(batch) < min_batch_size:
                    small_batches.extend(batch)
                else:
                    optimized_batches.append(batch)

            # Создаем нормальные пачки из маленьких
            if small_batches:
                for i in range(0, len(small_batches), min_batch_size):
                    chunk = small_batches[i:i + min_batch_size]
                    if chunk:
                        optimized_batches.append(chunk)

            all_batches = optimized_batches

        if debug and all_batches:
            batch_sizes = [len(batch) for batch in all_batches]
            avg_size = sum(batch_sizes) / len(batch_sizes)
            single_batches = sum(1 for size in batch_sizes if size == 1)

            self.stdout.write(f'      📊 Создано {len(all_batches)} пачек, средний размер: {avg_size:.1f}')
            self.stdout.write(f'      🔢 Пачек по 1 элементу: {single_batches}')

        return all_batches

    def _calculate_weight_for_game(self, game_id, game_data_map_and_screenshots):
        """Расчет веса для игры"""
        if not game_id:
            return 1.0

        # Извлекаем данные из кортежа
        game_data_map, screenshots_info = game_data_map_and_screenshots

        # Получаем game_data по game_id
        game_data = game_data_map.get(game_id, {})

        # Используем существующий метод расчета веса
        return self._calculate_data_weight(game_data, screenshots_info)

    def _calculate_weight_for_simple(self, item_id, extra_data=None):
        """Простой расчет веса для элементов без сложных данных"""
        return 1.0  # Все элементы имеют одинаковый вес

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
                                endpoint, model_class):
        """Шаблон для обработки пачки данных"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} объектов')

            id_list = ','.join(map(str, batch_ids))
            query = f'fields id,name; where id = ({id_list});'
            batch_data = self._rate_limited_request(endpoint, query, debug=False)

            data_by_id = {item['id']: item for item in batch_data if 'id' in item}

            batch_map = {}
            with ThreadPoolExecutor(max_workers=min(len(batch_ids), 10)) as executor:
                futures = {executor.submit(
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

        return self._batch_processor_regular(ids_list, process_batch, emoji, name, debug)

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
        return self._batch_processor_regular(cover_ids, self._process_covers_batch, '🖼️', 'обложек', debug)

    def _process_screenshots_batch(self, batch_num, batch_game_ids, result_map, lock,
                                   total_batches, name, debug, game_data_map, screenshots_info):
        """Обрабатывает пачку скриншотов"""
        try:
            batch_weight = 0
            if debug:
                batch_weight = sum(self._calculate_data_weight(game_data_map.get(gid, {}),
                                                               screenshots_info) for gid in batch_game_ids)
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: '
                                      f'{len(batch_game_ids)} игр, вес: {batch_weight:.1f}')

            def process_single_game(game_id):
                try:
                    game_screenshots_count = screenshots_info.get(game_id, 0)
                    if game_screenshots_count <= 0:
                        return (game_id, 0)

                    game_weight = self._calculate_data_weight(game_data_map.get(game_id, {}),
                                                              screenshots_info)
                    limit = game_screenshots_count
                    if game_weight > 5:
                        limit = min(game_screenshots_count * 2, 50)
                    elif game_weight > 3:
                        limit = min(game_screenshots_count + 5, 30)

                    query = f'fields id,url,image_id,width,height; where game = {game_id}; limit {limit};'
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

            if len(batch_game_ids) == 1 or batch_weight > 15:
                max_workers = min(len(batch_game_ids), 2)
            else:
                max_workers = min(len(batch_game_ids), 4)

            batch_map = {}
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
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

    def load_screenshots_parallel(self, game_ids, game_data_map, screenshots_info, debug=False):
        """Параллельная загрузка скриншотов с учетом веса игр"""
        if not game_ids:
            return 0

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_screenshots_batch(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                game_data_map, screenshots_info
            )

        # Подготавливаем данные для функции расчета веса
        game_data_and_screenshots = (game_data_map, screenshots_info)

        # Используем взвешенную систему для скриншотов
        result_map = self._batch_processor_weighted(
            game_ids, process_batch, '📸', 'скриншотов',
            self._calculate_weight_for_game, game_data_and_screenshots, debug
        )

        total_screenshots = sum(result_map.values()) if result_map else 0
        if debug:
            games_with_screenshots = len([v for v in result_map.values() if v > 0])
            self.stdout.write(
                f'      📸 Всего загружено {total_screenshots} скриншотов для {games_with_screenshots}/{len(game_ids)} игр')

        return total_screenshots

    def _process_additional_data_batch(self, batch_num, batch_game_ids, result_map, lock,
                                       total_batches, name, debug, game_data_map, screenshots_info):
        """Обрабатывает пачку дополнительных данных"""
        try:
            batch_weight = 0
            if debug:
                batch_weight = sum(self._calculate_data_weight(game_data_map.get(gid, {}),
                                                               screenshots_info) for gid in batch_game_ids)
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: '
                                      f'{len(batch_game_ids)} игр, вес: {batch_weight:.1f}')

            avg_weight = batch_weight / len(batch_game_ids) if batch_game_ids else 0

            # ИСПРАВЛЕНИЕ: лимит не должен превышать 500
            query_limit = 500  # МАКСИМУМ для IGDB API

            # Можно уменьшить лимит если вес большой, но не превышать 500
            if avg_weight > 6:
                query_limit = min(500, 300)  # 300 максимум для тяжелых игр
            elif avg_weight < 2:
                query_limit = 500  # 500 максимум для легких игр

            id_list = ','.join(map(str, batch_game_ids))
            query = f'''
                fields id,name,collections,franchises,involved_companies.company,
                       involved_companies.developer,involved_companies.publisher,
                       themes,player_perspectives,game_modes;
                where id = ({id_list});
                limit {query_limit};
            '''
            batch_data = self._rate_limited_request('games', query, debug=False)

            data_by_id = {item['id']: item for item in batch_data if 'id' in item}

            def process_single_game(game_id):
                if game_id not in data_by_id:
                    return None
                return (game_id, data_by_id[game_id])

            if len(batch_game_ids) == 1 or avg_weight > 6:
                max_workers = min(len(batch_game_ids), 2)
            else:
                max_workers = min(len(batch_game_ids), 5)

            batch_map = {}
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {executor.submit(process_single_game, game_id): game_id
                           for game_id in batch_game_ids}

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

    def load_additional_data_parallel(self, game_ids, game_data_map, screenshots_info, debug=False):
        """Параллельная загрузка дополнительных данных"""

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_additional_data_batch(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                game_data_map, screenshots_info
            )

        # Подготавливаем данные для функции расчета веса
        game_data_and_screenshots = (game_data_map, screenshots_info)

        # Используем взвешенную систему для доп. данных
        return self._batch_processor_weighted(
            game_ids, process_batch, '📚', 'доп. данных',
            self._calculate_weight_for_game, game_data_and_screenshots, debug
        )

    def load_and_process_additional_data(self, game_ids, game_data_map, screenshots_info, debug=False):
        """Загружает и обрабатывает дополнительные данные"""
        additional_data_map = self.load_additional_data_parallel(
            game_ids, game_data_map, screenshots_info, debug
        )

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

    def _process_companies_batch(self, batch_num, batch_ids, company_map, lock, total_batches, name, debug):
        """Обрабатывает пачку компаний"""
        try:
            if debug:
                with lock:
                    self.stdout.write(
                        f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} компаний')

            id_list = ','.join(map(str, batch_ids))
            # УДАЛЕНО поле logo из запроса
            query = f'fields id,name; where id = ({id_list});'
            batch_data = self._rate_limited_request('companies', query, debug=False)

            data_by_id = {item['id']: item for item in batch_data if 'id' in item}

            def process_single_company(company_id):
                if company_id not in data_by_id:
                    return None

                company_data = data_by_id[company_id]
                company_name = company_data.get('name', f'Company {company_id}')
                # УДАЛЕНО: logo_igdb_id = company_data.get('logo')

                with self._db_lock:
                    existing = Company.objects.filter(igdb_id=company_id).first()

                    if existing:
                        needs_update = False
                        if not existing.name.strip() and company_name.strip():
                            existing.name = company_name
                            needs_update = True

                        # УДАЛЕН весь блок кода, связанный с логотипами

                        if needs_update:
                            existing.save()
                        return (company_id, existing)
                    else:
                        # УДАЛЕН код для создания логотипов
                        company = Company(igdb_id=company_id, name=company_name)
                        company.save()
                        return (company_id, company)

            batch_map = {}
            with ThreadPoolExecutor(max_workers=min(len(batch_ids), 10)) as executor:
                futures = {executor.submit(process_single_company, company_id): company_id for company_id in
                           batch_ids}

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
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_map)} компаний')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_companies_parallel(self, company_ids, debug=False):
        """Параллельная загрузка компаний с логотипами"""
        return self._batch_processor_regular(company_ids, self._process_companies_batch, '🏢', 'компаний', debug)

    def create_basic_games(self, games_data_list, debug=False):
        """Создает игры с основными данными"""
        from django.utils import timezone
        from datetime import datetime

        with self._db_lock:
            existing_game_ids = set(Game.objects.filter(
                igdb_id__in=[g.get('id') for g in games_data_list if g.get('id')]
            ).values_list('igdb_id', flat=True))

        def process_single_game(game_data):
            game_id = game_data.get('id')
            if not game_id or game_id in existing_game_ids:
                return None

            # Создаем объект игры напрямую
            game = Game(
                igdb_id=game_id,
                name=game_data.get('name', ''),
                summary=game_data.get('summary', ''),
                storyline=game_data.get('storyline', ''),
                rating=game_data.get('rating'),
                rating_count=game_data.get('rating_count', 0)
            )

            # Сохраняем game_type из данных игры
            game_type = game_data.get('game_type')
            if game_type is not None:
                game.game_type = game_type

            if game_data.get('first_release_date'):
                naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                game.first_release_date = timezone.make_aware(naive_datetime)

            return game

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

    def load_keywords_parallel_with_weights(self, keyword_ids, debug=False):
        """Параллельная загрузка ключевых слов с учетом веса"""

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_batch_template(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                'keywords', Keyword
            )

        # Используем взвешенную систему для ключевых слов
        return self._batch_processor_weighted(
            keyword_ids, process_batch, '🔑', 'ключевых слов',
            self._calculate_weight_for_simple, None, debug
        )
