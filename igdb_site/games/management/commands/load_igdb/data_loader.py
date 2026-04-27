# games/management/commands/load_igdb/data_loader.py
import time
import re
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from games.igdb_api import make_igdb_request
from games.models import (
    Game, Genre, Keyword, Platform, Series,
    Company, Theme, PlayerPerspective, GameMode, Screenshot,
    GameEngine
)
import requests
from urllib.parse import urlparse


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
        self.debug_mode = False

    def set_interrupted(self):
        """Устанавливает флаг прерывания"""
        self._interrupted.set()

    def is_interrupted(self):
        """Проверяет прерывание"""
        return self._interrupted.is_set()

    def load_engines_parallel(self, engine_ids, debug=False):
        """Параллельная загрузка игровых движков"""
        if not engine_ids:
            if debug:
                self.stdout.write('   ⚙️ Нет ID движков для загрузки')
            return {}

        if debug:
            self.stdout.write(f'   ⚙️ Загрузка {len(engine_ids)} движков...')
            self.stdout.write(f'   ⚙️ ID движков: {engine_ids}')

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            if debug:
                with lock:
                    self.stdout.write(f'      🔄 Обработка пачки {batch_num}: {batch_ids}')

            return self._process_batch_template(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                'game_engines', GameEngine
            )

        result = self._batch_processor_regular(engine_ids, process_batch, '⚙️', 'движков', debug)

        if debug:
            self.stdout.write(f'   ⚙️ Результат загрузки движков: {len(result)} из {len(engine_ids)}')
            for engine_id, engine in result.items():
                self.stdout.write(f'      ✅ {engine.name} (ID: {engine.igdb_id})')

        return result

    def debug_cover_format(self, cover_id, debug=False):
        """Отладочный метод для проверки формата обложки"""
        query = f'fields id,url,image_id; where id = {cover_id};'

        try:
            cover_data = self._rate_limited_request('covers', query, debug=debug)

            if not cover_data:
                if debug:
                    self.stdout.write(f'   ❌ Обложка {cover_id} не найдена')
                return None

            cover = cover_data[0]

            if debug:
                self.stdout.write(f'\n🔍 ДЕБАГ ОБЛОЖКИ ID: {cover_id}')
                self.stdout.write(f'   • Полные данные: {cover}')
                self.stdout.write(f'   • image_id: {cover.get("image_id")}')
                self.stdout.write(f'   • url: {cover.get("url")}')

                if cover.get('image_id'):
                    image_id = cover['image_id']
                    self.stdout.write(f'\n   🔗 Варианты URL через image_id:')
                    self.stdout.write(
                        f'      • JPG: https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg')
                    self.stdout.write(
                        f'      • WebP: https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.webp')

                if cover.get('url'):
                    url = cover['url']
                    self.stdout.write(f'\n   🔗 Варианты URL через url:')
                    self.stdout.write(f'      • Исходный: {url}')
                    if url.startswith('//'):
                        url = f"https:{url}"
                        self.stdout.write(f'      • С https: {url}')

                    if 't_thumb' in url:
                        self.stdout.write(f'      • Cover Big JPG: {url.replace("t_thumb", "t_cover_big")}')
                        self.stdout.write(
                            f'      • Cover Big WebP: {url.replace("t_thumb", "t_cover_big").replace(".jpg", ".webp")}')

            return cover

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка запроса обложки {cover_id}: {e}')
            return None

    def create_basic_games(self, games_data_list, debug=False):
        """Создает игры с основными данными, избегая дубликатов"""
        from django.utils import timezone
        from datetime import datetime

        igdb_ids = [g.get('id') for g in games_data_list if g.get('id')]
        existing_game_ids = set()
        skipped_games = 0

        if igdb_ids:
            with self._db_lock:
                existing_game_ids = set(Game.objects.filter(
                    igdb_id__in=igdb_ids
                ).values_list('igdb_id', flat=True))
                skipped_games = len([gid for gid in igdb_ids if gid in existing_game_ids])

        def process_single_game(game_data):
            game_id = game_data.get('id')
            if not game_id or game_id in existing_game_ids:
                if debug and game_id in existing_game_ids:
                    self.stdout.write(f'   ⏭️  Игра уже существует: {game_id}')
                return None

            game = Game(
                igdb_id=game_id,
                name=game_data.get('name', ''),
                summary=game_data.get('summary', ''),
                storyline=game_data.get('storyline', ''),
                rating=game_data.get('rating'),
                rating_count=game_data.get('rating_count', 0)
            )

            game_type = game_data.get('game_type')
            if game_type is not None:
                game.game_type = game_type

            if game_data.get('first_release_date'):
                naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                game.first_release_date = timezone.make_aware(naive_datetime)

            return game

        games_to_create = []
        for game_data in games_data_list:
            try:
                game = process_single_game(game_data)
                if game:
                    games_to_create.append(game)
            except Exception as e:
                if debug:
                    self.stderr.write(f'   ❌ Ошибка создания игры: {e}')

        if games_to_create:
            try:
                with self._db_lock:
                    Game.objects.bulk_create(games_to_create, batch_size=50, ignore_conflicts=True)
                if debug:
                    self.stdout.write(f'   ✅ Создано игр: {len(games_to_create)}')
            except Exception as e:
                if debug:
                    self.stderr.write(f'   ❌ Ошибка bulk_create игр: {e}')
                created_count = 0
                for game in games_to_create:
                    try:
                        game.save()
                        created_count += 1
                    except:
                        pass
                return created_count, {}, skipped_games

        return len(games_to_create), {game.igdb_id: game for game in games_to_create}, skipped_games

    def _bulk_check_existing_objects(self, model_class, igdb_ids):
        """Массовая проверка существующих объектов по igdb_id"""
        with self._db_lock:
            existing_objects = model_class.objects.filter(igdb_id__in=igdb_ids)
            return {obj.igdb_id: obj for obj in existing_objects}

    def _process_item_simple(self, obj_id, item_name, name, model_class, debug=False):
        """Простая обработка объекта без сложной логики кодировки"""
        with self._db_lock:
            try:
                if debug and hasattr(self, 'stdout'):
                    self.stdout.write(f'      🔍 Поиск существующего объекта {obj_id} ({model_class.__name__})...')

                existing = model_class.objects.filter(igdb_id=obj_id).first()

                if existing:
                    if debug and hasattr(self, 'stdout'):
                        self.stdout.write(f'      ✅ Объект уже существует: {existing.name}')

                    needs_update = False

                    if model_class == Series and existing.name.startswith('Series ') and not item_name.startswith(
                            'Series '):
                        existing.name = item_name
                        needs_update = True
                    elif not existing.name.strip() and item_name.strip():
                        existing.name = item_name
                        needs_update = True
                    elif existing.name != item_name and item_name.strip():
                        existing.name = item_name
                        needs_update = True

                    if needs_update:
                        if debug and hasattr(self, 'stdout'):
                            self.stdout.write(f'      🔄 Обновляем имя с "{existing.name}" на "{item_name}"')
                        existing.save()
                    return (obj_id, existing)
                else:
                    if debug and hasattr(self, 'stdout'):
                        self.stdout.write(f'      🆕 Создаем новый объект: {item_name}')

                    # СОХРАНЯЕМ СРАЗУ, чтобы получить primary key
                    obj = model_class(igdb_id=obj_id, name=item_name)
                    obj.save()  # ← Сохраняем сразу!

                    if debug and hasattr(self, 'stdout'):
                        self.stdout.write(f'      🔍 Проверка объекта перед сохранением:')
                        self.stdout.write(f'         • ID в БД: {obj.id}')
                        self.stdout.write(f'         • IGDB ID: {obj.igdb_id}')
                        self.stdout.write(f'         • Имя: {repr(obj.name)}')

                    return (obj_id, obj)

            except Exception as e:
                error_msg = f"""
          ❌ КРИТИЧЕСКАЯ ОШИБКА создания объекта {obj_id} ({model_class.__name__}):
             • Ошибка: {type(e).__name__}: {e}
             • Имя для сохранения: {repr(item_name) if item_name else 'None'}
             • Длина имени: {len(item_name) if item_name else 0}
             • Тип имени: {type(item_name)}
          """

                if hasattr(self, 'stderr'):
                    self.stderr.write(error_msg)

                    import traceback
                    self.stderr.write(f"""
          📋 ПОЛНАЯ ТРАССИРОВКА ОШИБКИ:""")

                    tb_lines = traceback.format_exc().split('\n')
                    for line in tb_lines:
                        if line.strip():
                            self.stderr.write(f'         {line}')

                # Пробуем восстановиться
                try:
                    if debug and hasattr(self, 'stdout'):
                        self.stdout.write(f'      🛠️  Попытка восстановления...')

                    fallback_name = f"Object {obj_id}"
                    if model_class == Series:
                        fallback_name = f"Series {obj_id}"
                    elif model_class == GameMode:
                        fallback_name = f"Game Mode {obj_id}"
                    elif model_class == PlayerPerspective:
                        fallback_name = f"Perspective {obj_id}"
                    elif model_class == Theme:
                        fallback_name = f"Theme {obj_id}"
                    elif model_class == Keyword:
                        fallback_name = f"Keyword {obj_id}"
                    elif model_class == Platform:
                        fallback_name = f"Platform {obj_id}"
                    elif model_class == Genre:
                        fallback_name = f"Genre {obj_id}"
                    elif model_class == Company:
                        fallback_name = f"Company {obj_id}"

                    # Еще раз проверяем существование
                    existing = model_class.objects.filter(igdb_id=obj_id).first()
                    if existing:
                        if debug and hasattr(self, 'stdout'):
                            self.stdout.write(f"      ✅ Объект уже существует, возвращаем его")
                        return (obj_id, existing)

                    # СОХРАНЯЕМ СРАЗУ
                    obj = model_class(igdb_id=obj_id, name=fallback_name)
                    obj.save()  # ← Сохраняем сразу!

                    if debug and hasattr(self, 'stdout'):
                        self.stdout.write(f"      ✅ Объект создан с именем по умолчанию (ID в БД: {obj.id})")
                    return (obj_id, obj)

                except Exception as e2:
                    if hasattr(self, 'stderr'):
                        self.stderr.write(f"""
          💥 ДВОЙНАЯ ОШИБКА:
             • Первая ошибка: {type(e).__name__}: {e}
             • Вторая ошибка: {type(e2).__name__}: {e2}
          """)
                    return None

    def _batch_processor_weighted(self, items_list, process_batch_func, emoji, name,
                                  weight_calculator_func, extra_data=None, debug=False):
        """Универсальный метод для обработки данных с учетом веса"""
        if not items_list or self.is_interrupted():
            if debug and self.is_interrupted():
                self.stdout.write(f'      ⏹️  Прерывание: пропускаем загрузку {name}')
            return {}

        batches = self._create_weighted_batches(
            items_list, weight_calculator_func, extra_data, debug=debug
        )
        total_batches = len(batches)

        result_map = {}
        lock = threading.Lock()

        if total_batches <= 3:
            max_workers = total_batches
        elif any(len(batch) == 1 for batch in batches):
            max_workers = min(total_batches, 8)
        else:
            max_workers = min(total_batches, 6)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for batch_num, batch_items in enumerate(batches, 1):
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

            for future in as_completed(futures):
                if self.is_interrupted():
                    if debug:
                        self.stdout.write(f'      ⏹️  Прерывание: прерываем выполнение {name}')
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break

                try:
                    future.result(timeout=60)
                except Exception as e:
                    if debug:
                        with lock:
                            self.stderr.write(f'      ❌ Ошибка в future для {name}: {e}')

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

        if debug:
            self.stdout.write(f'      {emoji} Начало загрузки {name}: {len(ids_list)} объектов')

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
                    for f in futures:
                        if not f.done():
                            f.cancel()
                    break

                try:
                    future.result(timeout=60)
                except Exception as e:
                    if debug:
                        with lock:
                            self.stderr.write(f'      ❌ Ошибка в пачке {name}: {e}')

        if debug:
            loaded = len(result_map)
            self.stdout.write(
                f'      {emoji} Всего загружено {name}: {loaded}/{len(ids_list)} (из {total_batches} пачек)')

        return result_map

    def load_all_data_types_sequentially(self, collected_data, debug=False):
        """Последовательно загружает все типы данных (keywords отключены)"""
        if self.is_interrupted():
            if debug:
                self.stdout.write('⏹️  Прерывание: пропускаем загрузку всех типов данных')
            return {}, {}

        if debug:
            self.stdout.write('\n🔍 ДИАГНОСТИКА СОБРАННЫХ ДАННЫХ:')
            for key, value in collected_data.items():
                if isinstance(value, list):
                    self.stdout.write(f'   • {key}: {len(value)} элементов')
                elif isinstance(value, dict):
                    self.stdout.write(f'   • {key}: {len(value)} записей')
                else:
                    self.stdout.write(f'   • {key}: {type(value)}')

        step_times = {}
        data_maps = {}

        steps = [
            ('🖼️  Обложки', 'covers', lambda: self.load_covers_parallel(collected_data['all_cover_ids'], debug),
             'cover_map'),
            ('🎭 Жанры', 'genres', lambda: self.load_data_parallel_generic(
                collected_data['all_genre_ids'], 'genres', Genre, '🎭', 'жанров', debug), 'genre_map'),
            ('🖥️  Платформы', 'platforms', lambda: self.load_data_parallel_generic(
                collected_data['all_platform_ids'], 'platforms', Platform, '🖥️', 'платформ', debug), 'platform_map'),
            # КЛЮЧЕВЫЕ СЛОВА ПОЛНОСТЬЮ ОТКЛЮЧЕНЫ
            # ('🔑 Ключевые слова', 'keywords', lambda: self.load_keywords_parallel_with_weights(
            #     collected_data['all_keyword_ids'], debug), 'keyword_map'),
            ('⚙️ Движки', 'engines', lambda: self.load_engines_parallel(
                collected_data.get('all_engine_ids', []), debug), 'engine_map'),
        ]

        for i, (display_name, key, load_func, map_key) in enumerate(steps, 1):
            if self.is_interrupted():
                if debug:
                    self.stdout.write(f'⏹️  Прерывание: пропускаем {display_name}')
                break

            if debug:
                self.stdout.write(f'\n{i}️⃣  {display_name}...')

            start_step = time.time()

            # Для ключевых слов возвращаем пустой словарь
            if map_key == 'keyword_map':
                data_maps[map_key] = {}
            else:
                data_maps[map_key] = load_func()

            step_times[key] = time.time() - start_step

            if debug and map_key in data_maps:
                self.stdout.write(f'   📊 Результат: {len(data_maps[map_key])} объектов')

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
                    'перспектив', debug), 'perspective_map'),
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

                if debug and map_key in data_maps:
                    self.stdout.write(f'   📊 Результат: {len(data_maps[map_key])} объектов')

        return data_maps, step_times

    def _rate_limited_request(self, endpoint, query, debug=False, max_retries=3):
        """Выполняет запрос к API с rate limiting и retry логикой"""
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

        weight = 1.0

        if game_data.get('genres'):
            weight += min(len(game_data['genres']) * 0.1, 1.0)

        if game_data.get('platforms'):
            weight += min(len(game_data['platforms']) * 0.1, 1.0)

        if game_data.get('keywords'):
            weight += min(len(game_data['keywords']) * 0.15, 2.0)

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

        return min(weight, 15.0)

    def _create_weighted_batches(self, items_list, weight_calculator_func, extra_data=None,
                                 max_batch_weight=15, min_batch_size=3, max_batch_size=10, debug=False):
        """Создает пачки с учетом веса для ЛЮБОГО типа данных"""
        if not items_list:
            return []

        items_with_weight = []
        for item in items_list:
            if extra_data:
                weight = weight_calculator_func(item, extra_data)
            else:
                weight = weight_calculator_func(item)
            items_with_weight.append((item, weight))

        items_with_weight.sort(key=lambda x: x[1], reverse=True)

        all_batches = []
        current_batch = []
        current_weight = 0

        for item, weight in items_with_weight:
            can_add = (
                    len(current_batch) < max_batch_size and
                    current_weight + weight <= max_batch_weight
            )

            if can_add:
                current_batch.append(item)
                current_weight += weight
            else:
                if current_batch:
                    all_batches.append(current_batch)
                current_batch = [item]
                current_weight = weight

        if current_batch:
            all_batches.append(current_batch)

        if len(all_batches) > 1:
            optimized_batches = []
            small_batches = []

            for batch in all_batches:
                if len(batch) < min_batch_size:
                    small_batches.extend(batch)
                else:
                    optimized_batches.append(batch)

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

        game_data_map, screenshots_info = game_data_map_and_screenshots
        game_data = game_data_map.get(game_id, {})

        return self._calculate_data_weight(game_data, screenshots_info)

    def _calculate_weight_for_simple(self, item_id, extra_data=None):
        """Простой расчет веса для элементов без сложных данных"""
        return 1.0

    def _generic_process_single(self, obj_id, data_by_id, name, model_class, debug=False):
        """Универсальная обработка одного объекта"""
        # Сначала проверяем, не появился ли объект в базе за время обработки
        with self._db_lock:
            existing = model_class.objects.filter(igdb_id=obj_id).first()
            if existing:
                if debug:
                    self.stdout.write(f'      ✅ Объект уже существует в базе: {obj_id} ({model_class.__name__})')
                return (obj_id, existing)

        if obj_id not in data_by_id:
            if debug:
                self.stdout.write(f'      ⚠️  Объект {obj_id} не найден в данных API')
            return None

        item_data = data_by_id[obj_id]

        try:
            item_name = item_data.get('name', f'{name} {obj_id}')

            if debug:
                self.stdout.write(f'      🆕 Создаем новый объект: {item_name}')

            obj = model_class(igdb_id=obj_id, name=item_name)
            obj.save()  # Сразу сохраняем, чтобы получить primary key

            if debug:
                self.stdout.write(f'      ✅ Объект успешно создан (ID в БД: {obj.id})')

            return (obj_id, obj)

        except Exception as e:
            error_msg = f"""
            ❌ ОШИБКА создания объекта {obj_id} ({model_class.__name__}):
            • Ошибка: {type(e).__name__}: {e}
            • Имя для сохранения: {repr(item_name) if 'item_name' in locals() else 'None'}
            """

            if hasattr(self, 'stderr'):
                self.stderr.write(error_msg)

            # Пробуем восстановиться с именем по умолчанию
            try:
                if debug:
                    self.stdout.write(f'      🛠️  Попытка восстановления с именем по умолчанию...')

                fallback_name = f"{model_class.__name__} {obj_id}"

                # Еще раз проверяем существование (на случай гонки)
                with self._db_lock:
                    existing = model_class.objects.filter(igdb_id=obj_id).first()
                    if existing:
                        return (obj_id, existing)

                obj = model_class(igdb_id=obj_id, name=fallback_name)
                obj.save()

                if debug:
                    self.stdout.write(f'      ✅ Объект создан с именем по умолчанию')
                return (obj_id, obj)

            except Exception as e2:
                if hasattr(self, 'stderr'):
                    self.stderr.write(f"""
                    💥 ДВОЙНАЯ ОШИБКА:
                    • Первая ошибка: {type(e).__name__}: {e}
                    • Вторая ошибка: {type(e2).__name__}: {e2}
                    """)
                return None

    def _process_item_with_encoding(self, obj_id, item_name, name, model_class, debug=False):
        """Обработка с попыткой разных кодировок"""
        encoding_attempts = [
            ('utf-8', 'strict'),
            ('utf-8', 'ignore'),
            ('utf-8', 'replace'),
            ('latin1', 'strict'),
            ('cp1252', 'strict'),
            ('iso-8859-1', 'strict'),
        ]

        last_exception = None

        for encoding, errors in encoding_attempts:
            try:
                if isinstance(item_name, bytes):
                    decoded_name = item_name.decode(encoding, errors)
                else:
                    decoded_name = item_name

                if debug:
                    with open(f'encoding_success_{obj_id}.txt', 'a', encoding='utf-8') as f:
                        f.write(f"Success with {encoding}/{errors}: {decoded_name[:50]}\n")

                item_name = decoded_name
                break

            except Exception as e:
                last_exception = e
                if debug:
                    with open(f'encoding_fail_{obj_id}.txt', 'a', encoding='utf-8') as f:
                        f.write(f"Failed with {encoding}/{errors}: {str(e)}\n")

        if last_exception:
            item_name = f'{name} {obj_id}'
            if debug:
                with open(f'encoding_fallback_{obj_id}.txt', 'w', encoding='utf-8') as f:
                    f.write(f"Using fallback name for {obj_id}\n")

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
        if debug:
            with lock:
                self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} объектов')
                if len(batch_ids) <= 10:
                    self.stdout.write(f'         🔍 ID для загрузки: {batch_ids}')

        existing_in_db = {}
        with self._db_lock:
            existing_objects = model_class.objects.filter(igdb_id__in=batch_ids)
            existing_in_db = {obj.igdb_id: obj for obj in existing_objects}

        for obj_id, obj in existing_in_db.items():
            with lock:
                result_map[obj_id] = obj
                if debug:
                    self.stdout.write(f'         ✅ Объект уже существует: {obj.name} (ID: {obj_id})')

        ids_to_load = [obj_id for obj_id in batch_ids if obj_id not in existing_in_db]

        if not ids_to_load:
            if debug:
                with lock:
                    self.stdout.write(f'         ✅ Все объекты уже в базе')
            return

        if debug:
            with lock:
                self.stdout.write(f'         🔄 Загружаем из API: {ids_to_load}')

        id_list = ','.join(map(str, ids_to_load))
        query = f'fields id,name; where id = ({id_list});'
        try:
            batch_data = self._rate_limited_request(endpoint, query, debug=debug)
            if debug:
                with lock:
                    self.stdout.write(f'         📥 Получено из API: {len(batch_data)} объектов')
                    for item in batch_data:
                        self.stdout.write(f'            • {item.get("name")} (ID: {item.get("id")})')
        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка запроса для пачки {name} {batch_num}: {e}')
            return

        data_by_id = {item['id']: item for item in batch_data if 'id' in item}

        batch_map = {}

        # Обрабатываем каждый объект последовательно
        for obj_id in ids_to_load:
            try:
                if obj_id not in data_by_id:
                    if debug:
                        with lock:
                            self.stdout.write(f'         ⚠️ Объект {obj_id} не найден в данных API')
                    continue

                item_data = data_by_id[obj_id]
                item_name = item_data.get('name', f'{name} {obj_id}')

                # Проверяем еще раз, не появился ли объект в базе
                with self._db_lock:
                    existing = model_class.objects.filter(igdb_id=obj_id).first()
                    if existing:
                        with lock:
                            result_map[obj_id] = existing
                            if debug:
                                self.stdout.write(
                                    f'         ✅ Объект уже существует (появился во время загрузки): {existing.name}')
                        continue

                # СОЗДАЕМ И СРАЗУ СОХРАНЯЕМ объект
                if debug:
                    with lock:
                        self.stdout.write(f'         🆕 Создаем новый объект: {item_name} (ID: {obj_id})')

                # Создаем и сразу сохраняем, чтобы получить primary key
                obj = model_class(igdb_id=obj_id, name=item_name)
                obj.save()  # ← КРИТИЧЕСКИ ВАЖНО: сохраняем СРАЗУ!

                if debug:
                    with lock:
                        self.stdout.write(f'         ✅ Объект сохранен: {obj.name} (ID в БД: {obj.id})')

                # Добавляем в результат УЖЕ СОХРАНЕННЫЙ объект
                with lock:
                    batch_map[obj_id] = obj

            except Exception as e:
                if debug:
                    with lock:
                        self.stderr.write(f'         ❌ Ошибка для объекта {obj_id}: {e}')
                        import traceback
                        self.stderr.write(f'         📋 Трассировка: {traceback.format_exc()[:200]}')

                # Пробуем восстановиться с именем по умолчанию
                try:
                    fallback_name = f"{model_class.__name__} {obj_id}"

                    # Проверяем еще раз
                    with self._db_lock:
                        existing = model_class.objects.filter(igdb_id=obj_id).first()
                        if existing:
                            with lock:
                                batch_map[obj_id] = existing
                            if debug:
                                with lock:
                                    self.stdout.write(f'         ✅ Найден существующий объект при восстановлении')
                            continue

                    # Создаем с запасным именем и сразу сохраняем
                    obj = model_class(igdb_id=obj_id, name=fallback_name)
                    obj.save()  # ← Сохраняем сразу!

                    with lock:
                        batch_map[obj_id] = obj

                    if debug:
                        with lock:
                            self.stdout.write(f'         ✅ Объект создан с именем по умолчанию: {fallback_name}')
                except Exception as e2:
                    if debug:
                        with lock:
                            self.stderr.write(f'         💥 Двойная ошибка для объекта {obj_id}: {e2}')

        # Добавляем все обработанные объекты в result_map
        with lock:
            result_map.update(batch_map)

        if debug:
            with lock:
                loaded_new = len(batch_map)
                loaded_existing = len(existing_in_db)
                self.stdout.write(
                    f'         ✅ Пачка {name} {batch_num}/{total_batches}: {loaded_new} новых, {loaded_existing} существующих')

    def load_data_parallel_generic(self, ids_list, endpoint, model_class, emoji, name, debug=False):
        """Универсальный метод для параллельной загрузки данных"""

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_batch_template(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                endpoint, model_class
            )

        return self._batch_processor_regular(ids_list, process_batch, emoji, name, debug)

    def _process_covers_batch(self, batch_num, batch_ids, cover_map, lock, total_batches, name, debug):
        """Обрабатывает пачку обложек - упрощенная версия"""
        if debug:
            with lock:
                self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} объектов')

        id_list = ','.join(map(str, batch_ids))
        query = f'fields id,url,image_id; where id = ({id_list});'
        try:
            batch_data = self._rate_limited_request('covers', query, debug=debug)
        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка запроса для пачки обложек {batch_num}: {e}')
            return

        data_by_id = {item['id']: item for item in batch_data if 'id' in item}

        def process_single_cover(cover_id):
            if cover_id not in data_by_id:
                return None

            cover_data = data_by_id[cover_id]
            url = cover_data.get('url', '')

            if url:
                if 't_thumb' in url:
                    url = url.replace('t_thumb', 't_cover_big')

                if url.endswith('.webp'):
                    url = url.replace('.webp', '.jpg')
                elif not url.endswith('.jpg'):
                    url += '.jpg'

                if url.startswith('//'):
                    url = f"https:{url}"

                return (cover_id, url)

            elif cover_data.get('image_id'):
                image_id = cover_data['image_id']
                return (cover_id, f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg")

            else:
                return (cover_id, f"https://images.igdb.com/igdb/image/upload/t_cover_big/{cover_id}.jpg")

        batch_map = {}
        for cover_id in batch_ids:
            result = process_single_cover(cover_id)
            if result:
                cover_id, url = result
                batch_map[cover_id] = url

        with lock:
            cover_map.update(batch_map)

        if debug:
            with lock:
                self.stdout.write(f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} объектов')
                if batch_map:
                    self.stdout.write(f'            📸 Пример URL: {list(batch_map.values())[0][:50]}...')

        return batch_map

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

                    # Получаем существующие URL скриншотов из БД для этой игры
                    with self._db_lock:
                        existing_urls = set(
                            Screenshot.objects.filter(
                                game=game_obj
                            ).values_list('url', flat=True)
                        )

                    screenshots_to_create = []
                    for s in screenshots_data:
                        screenshot_id = s.get('id')
                        image_id = s.get('image_id')

                        if not screenshot_id or not image_id:
                            continue

                        image_url = f"https://images.igdb.com/igdb/image/upload/t_original/{image_id}.jpg"

                        # Проверяем, существует ли уже скриншот с таким URL
                        if image_url in existing_urls:
                            continue

                        screenshots_to_create.append(
                            Screenshot(
                                game=game_obj,
                                url=image_url,
                                w=s.get('width') or 1920,
                                h=s.get('height') or 1080,
                                primary=False
                            )
                        )

                    if screenshots_to_create:
                        with self._db_lock:
                            Screenshot.objects.bulk_create(screenshots_to_create, batch_size=10, ignore_conflicts=True)
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

        game_data_and_screenshots = (game_data_map, screenshots_info)

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

            query_limit = 500

            if avg_weight > 6:
                query_limit = min(500, 300)
            elif avg_weight < 2:
                query_limit = 500

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

        game_data_and_screenshots = (game_data_map, screenshots_info)

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
        if debug:
            with lock:
                self.stdout.write(
                    f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} компаний')

        id_list = ','.join(map(str, batch_ids))
        query = f'fields id,name; where id = ({id_list});'
        try:
            batch_data = self._rate_limited_request('companies', query, debug=debug)
        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка запроса для пачки компаний {batch_num}: {e}')
            return

        data_by_id = {item['id']: item for item in batch_data if 'id' in item}

        def process_single_company(company_id):
            if company_id not in data_by_id:
                return None

            company_data = data_by_id[company_id]
            company_name = company_data.get('name', f'Company {company_id}')

            with self._db_lock:
                existing = Company.objects.filter(igdb_id=company_id).first()

                if existing:
                    needs_update = False
                    if not existing.name.strip() and company_name.strip():
                        existing.name = company_name
                        needs_update = True

                    if needs_update:
                        existing.save()
                    return (company_id, existing)
                else:
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
                            self.stderr.write(f'            ❌ Ошибка future для компании {company_id}: {e}')

        with lock:
            company_map.update(batch_map)

        if debug:
            with lock:
                self.stdout.write(
                    f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_map)} компаний')

    def load_companies_parallel(self, company_ids, debug=False):
        """Параллельная загрузка компаний с логотипами"""
        return self._batch_processor_regular(company_ids, self._process_companies_batch, '🏢', 'компаний', debug)

    def update_games_with_covers(self, game_basic_map, cover_map, game_data_map, debug=False):
        """Обновляет игры обложками с проверкой доступности изображения"""
        import requests
        from urllib.parse import urlparse

        with self._db_lock:
            games = list(Game.objects.filter(igdb_id__in=game_basic_map.keys()))

        def check_image_accessible(url):
            """Проверяет доступность изображения по URL"""
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
                response = requests.head(url, headers=headers, timeout=5, allow_redirects=True)

                if response.status_code != 200:
                    if debug:
                        self.stdout.write(f'      ⚠️  Изображение недоступно, статус: {response.status_code}')
                    return False

                content_type = response.headers.get('content-type', '').lower()
                if 'image' not in content_type:
                    if debug:
                        self.stdout.write(f'      ⚠️  Не изображение, content-type: {content_type}')
                    return False

                return True
            except Exception as e:
                if debug:
                    self.stdout.write(f'      ❌ Ошибка проверки изображения {url}: {e}')
                return False

        def process_single_game(game):
            game_data = game_data_map.get(game.igdb_id)
            if game_data and game_data.get('cover'):
                cover_id = game_data['cover']
                if cover_id in cover_map:
                    new_cover_url = cover_map[cover_id]
                    current_url = game.cover_url or ""

                    if debug:
                        self.stdout.write(f'\n   🔍 Проверка обложки для {game.name}:')
                        self.stdout.write(f'      Текущая: {current_url}')
                        self.stdout.write(f'      Новая: {new_cover_url}')

                    if current_url:
                        current_accessible = check_image_accessible(current_url)
                        if not current_accessible:
                            new_accessible = check_image_accessible(new_cover_url)
                            if new_accessible:
                                if debug:
                                    self.stdout.write(f'   🔄 Обновление недоступной обложки: {game.name}')
                                game.cover_url = new_cover_url
                                return game
                            elif debug:
                                self.stdout.write(f'   ⚠️  Новая обложка тоже недоступна: {game.name}')
                        elif debug:
                            self.stdout.write(f'   ✅ Текущая обложка доступна: {game.name}')
                    else:
                        new_accessible = check_image_accessible(new_cover_url)
                        if new_accessible:
                            if debug:
                                self.stdout.write(f'   🖼️  Установка новой обложки: {game.name}')
                            game.cover_url = new_cover_url
                            return game
                        elif debug:
                            self.stdout.write(f'   ⚠️  Новая обложка недоступна: {game.name}')
            return None

        games_to_update = []
        with ThreadPoolExecutor(max_workers=min(len(games), 5)) as executor:
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
            if debug:
                self.stdout.write(f'\n   💾 Обновлено обложек в базе: {len(games_to_update)}')

        return len(games_to_update)

    def load_keywords_parallel_with_weights(self, keyword_ids, debug=False):
        """Параллельная загрузка ключевых слов с учетом веса"""

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_batch_template(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                'keywords', Keyword
            )

        return self._batch_processor_weighted(
            keyword_ids, process_batch, '🔑', 'ключевых слов',
            self._calculate_weight_for_simple, None, debug
        )