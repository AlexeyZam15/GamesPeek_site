# FILE: find_non_primary_games.py
# PATH: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\management\commands\find_non_primary_games.py

from django.core.management.base import BaseCommand
from django.utils import timezone
from games.models import Game, GameType
from games.igdb_api import make_igdb_request
import time
import concurrent.futures
from threading import Lock, Semaphore
import requests
import json
from django.core.cache import cache
from django.db import transaction, models
import os
import sys
from collections import defaultdict

# Импортируем из пакета game_types
from games.management.commands.game_types import (
    GAME_TYPE_CONFIG,
    get_game_type_info,
    get_game_type_description,
    get_type_statistics_key
)


class ProgressState:
    """Сохраняет состояние обработки между запусками"""
    STATE_KEY = "game_analysis_progress_state"

    @classmethod
    def save_state(cls, state_data):
        try:
            cache.set(cls.STATE_KEY, json.dumps(state_data), 60 * 60 * 24 * 30)
        except Exception as e:
            print(f"[WARNING] Не удалось сохранить состояние: {e}")

    @classmethod
    def load_state(cls):
        try:
            cached_state = cache.get(cls.STATE_KEY)
            if cached_state:
                return json.loads(cached_state)
        except:
            pass
        return cls.get_default_state()

    @classmethod
    def get_default_state(cls):
        return {
            'last_processed_id': 0,
            'total_processed': 0,
            'total_cached': 0,
            'non_primary_count': 0,
            'by_type': {},
            'failed_batches': 0,
            'completed': False,
            'total_assigned': 0
        }

    @classmethod
    def reset_state(cls):
        try:
            cache.delete(cls.STATE_KEY)
        except:
            pass
        return cls.get_default_state()


class GameAnalysisCache:
    """Кэширует результаты анализа игр"""
    CACHE_PREFIX = "game_analysis_"
    CACHE_TIMEOUT = 60 * 60 * 24 * 30

    @classmethod
    def get(cls, igdb_id):
        cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
        cached_data = cache.get(cache_key)
        if cached_data:
            try:
                return json.loads(cached_data)
            except:
                return None
        return None

    @classmethod
    def set(cls, igdb_id, analysis_data):
        cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
        if analysis_data:
            cache.set(cache_key, json.dumps(analysis_data), cls.CACHE_TIMEOUT)


class RateLimiter:
    """Ограничивает частоту запросов к IGDB API"""

    def __init__(self, max_concurrent=3, requests_per_second=3.5):
        self.max_concurrent = max_concurrent
        self.requests_per_second = requests_per_second
        self.semaphore = Semaphore(max_concurrent)
        self.last_request_time = 0
        self.min_interval = 1.0 / requests_per_second
        self.lock = Lock()

    def wait_for_rate_limit(self):
        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
            self.last_request_time = time.time()

    def acquire(self):
        return self.semaphore.acquire()

    def release(self):
        self.semaphore.release()


class GameTypeCache:
    """Кэш типов игр в памяти для быстрого доступа"""
    _cache = None
    _cache_loaded = False
    _config_cache = None
    _config_loaded = False

    @classmethod
    def initialize(cls):
        """Инициализирует кэш типов игр из конфигурации"""
        if not cls._config_loaded:
            cls._config_cache = {}
            for igdb_id, config in GAME_TYPE_CONFIG.items():
                cls._config_cache[igdb_id] = {
                    'name': config['description'],
                    'is_primary': config.get('is_primary', True),
                    'type': config['type'],
                    'description': config['description']
                }
            cls._config_loaded = True

    @classmethod
    def get_game_type_info(cls, igdb_id):
        """Быстро получает информацию о типе игры из кэша конфигурации"""
        cls.initialize()
        return cls._config_cache.get(igdb_id)

    @classmethod
    def get_game_type_obj(cls, igdb_id):
        """Получает объект GameType из базы (с кэшированием запросов)"""
        if igdb_id is None:
            return None

        # Инициализируем кэш объектов если нужно
        if cls._cache is None:
            cls._cache = {}

        # Проверяем в кэше памяти
        if igdb_id in cls._cache:
            return cls._cache[igdb_id]

        # Ищем в базе
        game_type = GameType.objects.filter(igdb_id=igdb_id).first()

        # Сохраняем в кэш памяти
        if game_type:
            cls._cache[igdb_id] = game_type

        return game_type

    @classmethod
    def preload_all_game_types(cls):
        """Предзагружает все GameType объекты в память"""
        print("   📦 Предзагрузка типов игр в память...")
        game_types = GameType.objects.all()
        cls._cache = {gt.igdb_id: gt for gt in game_types}
        print(f"   ✅ Загружено {len(cls._cache)} типов игр")
        return cls._cache

    @classmethod
    def clear_cache(cls):
        """Полностью очищает кэш"""
        cls._cache = None
        cls._cache_loaded = False
        cls._config_cache = None
        cls._config_loaded = False

        # Также очищаем кэш Django для объектов GameType
        try:
            # Удаляем все кэшированные объекты GameType
            for key in cache.keys('gametype_obj_*'):
                cache.delete(key)
        except:
            pass

        return True

    @classmethod
    def get_cache_stats(cls):
        """Возвращает статистику кэша"""
        return {
            'config_loaded': cls._config_loaded,
            'config_count': len(cls._config_cache) if cls._config_cache else 0,
            'objects_loaded': cls._cache is not None,
            'objects_count': len(cls._cache) if cls._cache else 0,
        }

    @classmethod
    def refresh_cache(cls):
        """Обновляет кэш из базы данных"""
        cls.clear_cache()
        return cls.preload_all_game_types()


class GameTypeFileManager:
    """Сохраняет игры в текстовые файлы, сгруппированные по типам"""

    def __init__(self, output_dir="games_by_game_types", reset=False):
        self.output_dir = output_dir
        self.file_handlers = {}
        self.file_counts = defaultdict(int)
        self.reset = reset

        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        elif self.reset:
            self.clear_directory()

        import atexit
        atexit.register(self.close_all_files)

    def clear_directory(self):
        """Очищает директорию с файлами типов игр"""
        try:
            for filename in os.listdir(self.output_dir):
                filepath = os.path.join(self.output_dir, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
            print(f"✅ Директория очищена")
        except Exception as e:
            print(f"⚠️  Не удалось очистить директорию: {e}")

    def get_game_type_filename(self, game_type):
        """Генерирует имя файла на основе типа игры"""
        if game_type is None:
            return "unknown.txt"

        info = GameTypeCache.get_game_type_info(game_type) or {'type': f'unknown_{game_type}'}
        return f"{game_type:02d}_{info['type']}.txt"

    def save_game_by_type(self, game_data, analysis):
        """Сохраняет игру в файл соответствующего типа"""
        game_type = analysis.get('game_type')
        filename = self.get_game_type_filename(game_type)
        filepath = os.path.join(self.output_dir, filename)

        # Открываем файл если еще не открыт
        if filename not in self.file_handlers:
            if self.reset and os.path.exists(filepath):
                mode = 'w'
            else:
                mode = 'a' if os.path.exists(filepath) else 'w'

            self.file_handlers[filename] = open(filepath, mode, encoding='utf-8')

            if mode == 'w' or self.reset:
                info = GameTypeCache.get_game_type_info(game_type) or {'description': 'Unknown Type'}
                self.file_handlers[filename].write(
                    f"=== ИГРЫ ТИПА: {info['name']} (game_type = {game_type if game_type is not None else 'None'}) ===\n\n"
                )

        game_name = game_data.get('name', 'Неизвестно')
        igdb_id = game_data.get('id', '')
        parent_id = analysis.get('parent_game')
        version_parent = analysis.get('version_parent')

        entry = f"ID: {igdb_id}\nНазвание: {game_name}\n"
        if parent_id:
            entry += f"Parent Game ID: {parent_id}\n"
        if version_parent:
            entry += f"Version Parent ID: {version_parent}\n"
        entry += f"Status: {'Primary' if analysis.get('is_primary') else 'Non-Primary'}\n"
        entry += "-" * 50 + "\n\n"

        self.file_handlers[filename].write(entry)
        self.file_handlers[filename].flush()
        self.file_counts[filename] += 1

    def close_all_files(self):
        """Закрывает все открытые файлы и выводит статистику"""
        for filename, file_handler in self.file_handlers.items():
            try:
                file_handler.close()
            except:
                pass

        print(f"\n📁 Игры сохранены по типам в папку: {self.output_dir}")
        for filename, count in sorted(self.file_counts.items()):
            print(f"   {filename}: {count:,} игр")


class GameBatchProcessor:
    """Обрабатывает игры пачками с оптимизацией запросов"""

    def __init__(self, debug=False):
        self.debug = debug
        self.game_type_cache = {}
        self.parent_games_cache = {}
        self.version_parents_cache = {}

    def preload_game_types(self):
        """Предзагружает все GameType в память"""
        game_types = GameType.objects.all()
        self.game_type_cache = {gt.igdb_id: gt for gt in game_types}
        return self.game_type_cache

    def preload_parent_games(self, parent_ids):
        """Предзагружает родительские игры"""
        if not parent_ids:
            return {}

        parent_games = Game.objects.filter(igdb_id__in=parent_ids)
        self.parent_games_cache.update({pg.igdb_id: pg for pg in parent_games})
        return self.parent_games_cache

    def preload_version_parents(self, version_parent_ids):
        """Предзагружает версии-родители"""
        if not version_parent_ids:
            return {}

        version_parents = Game.objects.filter(igdb_id__in=version_parent_ids)
        self.version_parents_cache.update({vp.igdb_id: vp for vp in version_parents})
        return self.version_parents_cache

    def analyze_game_batch(self, igdb_data_batch):
        """Анализирует пачку игр без запросов к базе"""
        results = []

        for igdb_id, game_data in igdb_data_batch.items():
            game_type_value = game_data.get('game_type')
            info = GameTypeCache.get_game_type_info(game_type_value) or {}

            analysis = {
                'type': info.get('type', 'unknown'),
                'game_type': game_type_value,
                'is_primary': info.get('is_primary', False),
                'game_name': game_data.get('name', 'Неизвестно'),
                'parent_game': game_data.get('parent_game'),
                'version_parent': game_data.get('version_parent'),
                'version_title': game_data.get('version_title'),
                'analyzed_at': timezone.now().isoformat()
            }

            results.append((igdb_id, analysis))

        return results

    def prepare_game_updates(self, games, analyses, force_assign=False):
        """Подготавливает игры для обновления"""
        games_to_update = []
        stats = {
            'assigned': 0,
            'skipped': 0,
            'needs_parent': set(),
            'needs_version': set()
        }

        for game, (igdb_id, analysis) in zip(games, analyses):
            # Проверяем нужно ли обновлять
            if not force_assign and game.game_type_id is not None:
                stats['skipped'] += 1
                continue

            # Получаем объекты связей
            game_type_obj = self.game_type_cache.get(analysis['game_type'])
            parent_game_obj = self.parent_games_cache.get(analysis['parent_game'])
            version_parent_obj = self.version_parents_cache.get(analysis['version_parent'])

            # Обновляем поля
            game.game_type = game_type_obj
            game.parent_game = parent_game_obj
            game.version_parent = version_parent_obj
            game.version_title = analysis['version_title']

            games_to_update.append(game)
            stats['assigned'] += 1

            # Запоминаем ID для которых нужно загрузить объекты
            if analysis['parent_game'] and not parent_game_obj:
                stats['needs_parent'].add(analysis['parent_game'])
            if analysis['version_parent'] and not version_parent_obj:
                stats['needs_version'].add(analysis['version_parent'])

        return games_to_update, stats

    def save_updates_batch(self, games_to_update):
        """Сохраняет обновления пачкой"""
        if not games_to_update:
            return 0

        # Используем bulk_update для оптимизации
        fields = ['game_type', 'parent_game', 'version_parent', 'version_title']

        # Разбиваем на пачки по 100 для bulk_update
        batch_size = 100
        total_updated = 0

        for i in range(0, len(games_to_update), batch_size):
            batch = games_to_update[i:i + batch_size]
            Game.objects.bulk_update(batch, fields)
            total_updated += len(batch)

        return total_updated


class Command(BaseCommand):
    """Основная команда для поиска и назначения типов игр"""
    help = 'Находит неосновные игры по данным IGDB, сохраняет по типам и назначает типы моделям'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=50,
            help='Количество игр в одной пачке для запроса к API (по умолчанию 50)'
        )
        parser.add_argument(
            '--max-concurrent',
            type=int,
            default=3,
            help='Максимальное количество одновременных запросов к API (по умолчанию 3)'
        )
        parser.add_argument(
            '--requests-per-second',
            type=float,
            default=3.5,
            help='Максимальное количество запросов в секунду к IGDB API (по умолчанию 3.5)'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Включить подробный вывод отладки'
        )
        parser.add_argument(
            '--reset',
            action='store_true',
            help='Сбросить сохраненный прогресс и начать заново'
        )
        parser.add_argument(
            '--games-per-run',
            type=int,
            default=500,
            help='Количество игр для обработки за один запуск (0 = все оставшиеся)'
        )
        parser.add_argument(
            '--max-retries',
            type=int,
            default=3,
            help='Максимальное количество повторных попыток при ошибках API'
        )
        parser.add_argument(
            '--games-by-types-dir',
            type=str,
            default='games_by_game_types',
            help='Директория для сохранения файлов с играми по типам'
        )
        parser.add_argument(
            '--skip-existing',
            action='store_true',
            help='Пропустить игры, которые уже есть в кэше'
        )
        parser.add_argument(
            '--assign-to-models',
            action='store_true',
            help='Назначить типы игр моделям Game в базе данных'
        )
        parser.add_argument(
            '--initialize-types',
            action='store_true',
            help='Инициализировать типы игр в базе данных перед началом обработки'
        )
        parser.add_argument(
            '--force-assign',
            action='store_true',
            help='Принудительно назначить типы всем играм, даже если уже назначены'
        )
        parser.add_argument(
            '--update-batch-size',
            type=int,
            default=100,
            help='Размер пачки для bulk_update (по умолчанию 100)'
        )

    def __init__(self):
        super().__init__()
        self.state = None
        self.debug_mode = False
        self.assign_to_models = False
        self.force_assign = False
        self.skip_existing = False
        self.batch_processor = None

    def initialize_game_types(self):
        """Инициализирует типы игр в базе данных"""
        print("🛠  Инициализация типов игр в базе данных...")

        created = 0
        updated = 0

        for igdb_id, config in GAME_TYPE_CONFIG.items():
            try:
                game_type, created_flag = GameType.objects.update_or_create(
                    igdb_id=igdb_id,
                    defaults={
                        'name': config['description'],
                        'description': f"Тип игры из IGDB: {config['type']}",
                        'is_primary': config.get('is_primary', True),
                    }
                )

                if created_flag:
                    created += 1
                else:
                    updated += 1

            except Exception as e:
                print(f"⚠️  Ошибка при создании типа игры {igdb_id}: {e}")

        print(f"✅ Типы игр инициализированы: создано {created}, обновлено {updated}")
        GameTypeCache.initialize()
        return created + updated

    def get_game_details_from_igdb(self, batch_ids, max_retries=3):
        """Получает данные о играх из IGDB API"""
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait_for_rate_limit()

                query = f"""
                fields 
                    id, name, game_type, parent_game, version_parent, version_title;
                where id = ({",".join(map(str, batch_ids))});
                limit {len(batch_ids)};
                """

                result = make_igdb_request('games', query)

                games_dict = {}
                for game in result:
                    game_id = game.get('id')
                    if game_id:
                        games_dict[game_id] = game

                return games_dict

            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    wait_time = 5 * (attempt + 1)
                    if self.debug_mode:
                        print(f"[DEBUG] ⚠  429, жду {wait_time} сек...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
            except Exception:
                if attempt < max_retries - 1:
                    wait_time = 5 * (attempt + 1)
                    if self.debug_mode:
                        print(f"[DEBUG] ⚠  Ошибка, повтор через {wait_time} сек...")
                    time.sleep(wait_time)
                else:
                    if self.debug_mode:
                        print(f"[DEBUG] ❌ Все попытки исчерпаны")
                    return {}

        return {}

    def get_empty_stats(self):
        """Возвращает пустую статистику"""
        return {
            'processed': 0,
            'cached': 0,
            'non_primary': 0,
            'by_type': {},
            'assigned': 0,
            'skipped': 0
        }

    def process_batch_optimized(self, batch_ids, skip_existing=False):
        """Оптимизированная обработка пачки игр"""
        try:
            if self.debug_mode:
                print(f"[DEBUG] Начинаем обработку пачки из {len(batch_ids)} игр")

            # 1. Получаем игры из базы одним запросом
            games = list(Game.objects.filter(igdb_id__in=batch_ids))
            if not games:
                return {'success': True, 'stats': self.get_empty_stats()}

            game_map = {game.igdb_id: game for game in games}

            # 2. Получаем данные из IGDB API
            igdb_data = self.get_game_details_from_igdb(batch_ids, self.max_retries)
            if not igdb_data:
                return {'success': False, 'error': 'No data from IGDB'}

            # 3. Анализируем игры
            analyses = self.batch_processor.analyze_game_batch(igdb_data)

            # 4. Подготавливаем данные для предзагрузки
            parent_ids = set()
            version_parent_ids = set()

            for igdb_id, analysis in analyses:
                if parent_id := analysis.get('parent_game'):
                    parent_ids.add(parent_id)
                if version_id := analysis.get('version_parent'):
                    version_parent_ids.add(version_id)

            # 5. Предзагружаем связанные данные
            self.batch_processor.preload_parent_games(parent_ids)
            self.batch_processor.preload_version_parents(version_parent_ids)

            # 6. Статистика и кэширование
            batch_stats = self.get_empty_stats()
            valid_games = []
            valid_analyses = []

            for igdb_id, analysis in analyses:
                if igdb_id not in game_map:
                    continue

                game = game_map[igdb_id]
                game_data = igdb_data.get(igdb_id, {})

                # Проверяем кэш
                cached_analysis = None
                if not skip_existing:
                    cached_analysis = GameAnalysisCache.get(igdb_id)

                if cached_analysis:
                    # Игра из кэша
                    batch_stats['cached'] += 1
                    analysis = cached_analysis
                else:
                    # Новая игра
                    batch_stats['processed'] += 1

                    if not skip_existing:
                        GameAnalysisCache.set(igdb_id, analysis)

                # Сохраняем в файл
                self.game_type_file_manager.save_game_by_type(game_data, analysis)

                # Собираем для обновления
                valid_games.append(game)
                valid_analyses.append((igdb_id, analysis))

                # Статистика по типам
                if not analysis.get('is_primary', True):
                    batch_stats['non_primary'] += 1
                    game_type = analysis.get('game_type')
                    stat_key = get_type_statistics_key(game_type)
                    batch_stats['by_type'][stat_key] = batch_stats['by_type'].get(stat_key, 0) + 1

            # 7. Назначаем типы моделям если нужно
            if self.assign_to_models and valid_games:
                games_to_update, update_stats = self.batch_processor.prepare_game_updates(
                    valid_games, valid_analyses, self.force_assign
                )

                if games_to_update:
                    updated_count = self.batch_processor.save_updates_batch(games_to_update)
                    batch_stats['assigned'] = updated_count
                    batch_stats['skipped'] = update_stats['skipped']

            return {'success': True, 'stats': batch_stats}

        except Exception as e:
            if self.debug_mode:
                print(f"[DEBUG] Ошибка в пачке: {e}")
            return {'success': False, 'error': str(e)}

    def process_games_optimized(self, start_id, limit, skip_existing=False):
        """Оптимизированная обработка игр"""
        # Получаем игры для обработки
        games_query = Game.objects.filter(id__gte=start_id).order_by('id')

        if skip_existing and not self.force_assign:
            games_query = games_query.filter(game_type__isnull=True)

        if limit > 0:
            games_query = games_query[:limit]

        games_chunk = list(games_query)

        # Если нет игр - завершаем
        if not games_chunk:
            if self.debug_mode:
                print(f"[DEBUG] Нет игр для обработки: start_id={start_id}, limit={limit}")
            return {
                'processed': 0,
                'cached': 0,
                'non_primary': 0,
                'by_type': {},
                'assigned': 0,
                'skipped': 0,
                'last_processed_id': start_id,
                'completed': True,
                'total_processed': 0
            }

        # Подготавливаем ID для запроса
        all_igdb_ids = [game.igdb_id for game in games_chunk]
        total_games = len(games_chunk)

        print(f"🔍 Обрабатываем {total_games:,} игр...")

        # Разбиваем на пачки для параллельной обработки
        batches = []
        for i in range(0, len(all_igdb_ids), self.batch_size):
            batch_end = min(i + self.batch_size, len(all_igdb_ids))
            batch_ids = all_igdb_ids[i:batch_end]
            batches.append(batch_ids)

        # Статистика
        chunk_stats = self.get_empty_stats()
        chunk_stats.update({
            'failed_batches': 0,
            'last_processed_id': games_chunk[-1].id,
            'total_processed': total_games,
            'completed': False
        })

        # Если вообще не было пачек для обработки
        if not batches:
            chunk_stats['completed'] = True
            return chunk_stats

        # Параллельная обработка
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            future_to_batch = {}

            for batch_ids in batches:
                self.rate_limiter.acquire()
                future = executor.submit(self.process_batch_optimized, batch_ids, skip_existing)
                future_to_batch[future] = batch_ids

                def callback(f):
                    self.rate_limiter.release()

                future.add_done_callback(callback)

            # Обрабатываем результаты
            completed = 0
            total_batches = len(batches)

            for future in concurrent.futures.as_completed(future_to_batch):
                completed += 1

                # Обновляем прогресс
                if completed % 5 == 0 or completed == total_batches:
                    percent = (completed / total_batches) * 100
                    print(f"\r📊 Прогресс: {percent:.1f}% ({completed}/{total_batches} пачек)", end="")

                try:
                    result = future.result()

                    if result['success']:
                        batch_stats = result['stats']
                        chunk_stats['processed'] += batch_stats['processed']
                        chunk_stats['cached'] += batch_stats['cached']
                        chunk_stats['non_primary'] += batch_stats['non_primary']
                        chunk_stats['assigned'] += batch_stats['assigned']
                        chunk_stats['skipped'] += batch_stats['skipped']

                        for game_type, count in batch_stats['by_type'].items():
                            chunk_stats['by_type'][game_type] = chunk_stats['by_type'].get(game_type, 0) + count
                    else:
                        chunk_stats['failed_batches'] += 1

                except Exception as e:
                    if self.debug_mode:
                        print(f"[DEBUG] Ошибка при обработке пачки: {e}")
                    chunk_stats['failed_batches'] += 1

        print()

        # Проверяем завершение: если обработали 0 игр или все игры были из кэша и пропущены
        if chunk_stats['processed'] == 0 and chunk_stats['assigned'] == 0:
            chunk_stats['completed'] = True

        return chunk_stats

    def get_current_stats(self):
        """Получает текущую статистику из базы данных"""
        total_games = Game.objects.count()
        games_with_type = Game.objects.filter(game_type__isnull=False).count()
        games_without_type = total_games - games_with_type

        # Статистика по типам
        type_stats = {}
        for game_type in GameType.objects.all():
            count = Game.objects.filter(game_type=game_type).count()
            if count > 0:
                type_stats[game_type.name] = count

        # Статистика по основным/неосновным
        primary_games = Game.objects.filter(game_type__is_primary=True).count()
        non_primary_games = Game.objects.filter(game_type__is_primary=False).count()

        return {
            'total_games': total_games,
            'games_with_type': games_with_type,
            'games_without_type': games_without_type,
            'type_stats': type_stats,
            'primary_games': primary_games,
            'non_primary_games': non_primary_games,
            'games_with_parent': Game.objects.filter(parent_game__isnull=False).count(),
            'games_with_version_parent': Game.objects.filter(version_parent__isnull=False).count(),
        }

    def handle(self, *args, **options):
        """Основной метод обработки команды"""
        # Устанавливаем параметры
        self.batch_size = options['batch_size']
        self.max_concurrent = options['max_concurrent']
        self.requests_per_second = options['requests_per_second']
        self.debug_mode = options['debug']
        self.games_per_run = options['games_per_run']
        self.max_retries = options['max_retries']
        self.skip_existing = options['skip_existing']
        games_by_types_dir = options['games_by_types_dir']
        reset_flag = options['reset']
        self.assign_to_models = options['assign_to_models']
        self.initialize_types = options['initialize_types']
        self.force_assign = options['force_assign']

        # Настройки
        self.game_type_file_manager = GameTypeFileManager(games_by_types_dir, reset=reset_flag)
        self.rate_limiter = RateLimiter(
            max_concurrent=self.max_concurrent,
            requests_per_second=self.requests_per_second
        )

        # Инициализируем процессор
        self.batch_processor = GameBatchProcessor(debug=self.debug_mode)

        # Состояние прогресса
        if reset_flag:
            self.state = ProgressState.reset_state()

            # ПОЛНАЯ ОЧИСТКА КЭША
            print("🔄 Полная очистка кэша...")

            # 1. Очищаем кэш анализа игр
            print("   Очищаем кэш анализа игр...")
            deleted_count = 0

            try:
                # Пытаемся удалить все ключи с префиксом game_analysis_
                keys_pattern = f"{GameAnalysisCache.CACHE_PREFIX}*"
                # Для простых случаев - очищаем весь кэш
                from django.core.cache import caches
                default_cache = caches['default']
                default_cache.clear()
                print("   ✅ Весь кэш Django очищен")
            except Exception as e:
                print(f"   ⚠️  Ошибка при очистке кэша: {e}")

            # 2. Очищаем кэш GameType объектов
            print("   Очищаем кэш типов игр...")
            GameTypeCache.clear_cache()

            # 3. Очищаем кэш процессора
            print("   Очищаем кэш процессора...")
            self.batch_processor = GameBatchProcessor(debug=self.debug_mode)

            print("✅ Полный сброс завершен")

        else:
            self.state = ProgressState.load_state()

        # Инициализируем типы игр если нужно
        if self.initialize_types:
            self.initialize_game_types()

        # Предзагружаем GameType
        self.batch_processor.preload_game_types()

        # Получаем начальную статистику
        initial_stats = self.get_current_stats()

        # Выводим подробную информацию о настройках
        print("=" * 80)
        print("🚀 ДЕТАЛИЗИРОВАННЫЙ ПОИСК И НАЗНАЧЕНИЕ ТИПОВ ИГР")
        print("=" * 80)

        print(f"\n⚙️  НАСТРОЙКИ КОМАНДЫ:")
        print(f"   Размер пачки API запросов: {self.batch_size}")
        print(f"   Максимум параллельных запросов: {self.max_concurrent}")
        print(f"   Лимит запросов в секунду: {self.requests_per_second}")
        print(f"   Игр за итерацию: {self.games_per_run if self.games_per_run > 0 else 'ВСЕ'}")
        print(f"   Сброс прогресса (--reset): {'ДА ✅' if reset_flag else 'НЕТ'}")

        print(f"\n🎯 РЕЖИМЫ РАБОТЫ:")
        print(f"   Назначение типов моделям: {'ВКЛЮЧЕНО ✅' if self.assign_to_models else 'ВЫКЛЮЧЕНО ⚠️'}")
        print(f"   Принудительное обновление: {'ВКЛЮЧЕНО ⚡' if self.force_assign else 'ВЫКЛЮЧЕНО'}")
        print(f"   Пропуск обработанных: {'ВКЛЮЧЕНО 🏃' if self.skip_existing else 'ВЫКЛЮЧЕНО'}")
        print(f"   Инициализация типов: {'ВЫПОЛНЕНА ✅' if self.initialize_types else 'НЕ ВЫПОЛНЕНА'}")

        print(f"\n📊 НАЧАЛЬНАЯ СТАТИСТИКА БАЗЫ ДАННЫХ:")
        print(f"   Всего игр в БД: {initial_stats['total_games']:,}")
        print(
            f"   Игр с назначенным типом: {initial_stats['games_with_type']:,} ({initial_stats['games_with_type'] / initial_stats['total_games'] * 100:.1f}%)")
        print(
            f"   Игр без типа: {initial_stats['games_without_type']:,} ({initial_stats['games_without_type'] / initial_stats['total_games'] * 100:.1f}%)")

        print(f"\n📦 СОСТОЯНИЕ КЭША:")
        if reset_flag:
            print(f"   Кэш анализа: ОЧИЩЕН ✅")
            print(f"   Кэш типов игр: ОЧИЩЕН ✅")
            print(f"   Прогресс обработки: СБРОШЕН ✅")
        else:
            print(f"   Кэш анализа: ИСПОЛЬЗУЕТСЯ")
            print(f"   Кэш типов игр: ИСПОЛЬЗУЕТСЯ")
            print(f"   Прогресс обработки: ВОССТАНОВЛЕН")

        print(f"\n📈 СОСТОЯНИЕ ПРОГРЕССА:")
        print(f"   Последняя обработанная ID: {self.state.get('last_processed_id', 0)}")
        print(f"   Всего новых запросов к API: {self.state.get('total_processed', 0):,}")
        print(f"   Всего использовано из кэша: {self.state.get('total_cached', 0):,}")
        print(f"   Всего назначено типов: {self.state.get('total_assigned', 0):,}")
        print(f"   Неосновных найдено: {self.state.get('non_primary_count', 0):,}")

        # ВАЖНО: Проверка для --reset режима
        if reset_flag:
            print(f"\n{'!' * 80}")
            print("⚠️  ВНИМАНИЕ: ИСПОЛЬЗУЕТСЯ РЕЖИМ --reset")
            print(f"   • Все игры будут запрошены заново из IGDB API")
            print(f"   • Кэш полностью очищен")
            print(f"   • Начинаем с первой игры")
            print(f"{'!' * 80}")

        # Проверяем нужно ли вообще запускать обработку
        if self.skip_existing and not self.force_assign and initial_stats['games_without_type'] == 0:
            print(f"\n{'!' * 80}")
            print("✅ ВСЕ ИГРЫ УЖЕ ИМЕЮТ НАЗНАЧЕННЫЕ ТИПЫ!")
            print(f"   Игр без типа: 0")
            print(f"   Используйте --force-assign для принудительного обновления всех игр")
            print(f"{'!' * 80}")
            return

        # Запускаем обработку
        iteration = 1
        completed = False
        max_iterations = 100

        print(f"\n{'=' * 80}")
        print(f"🚀 НАЧИНАЕМ ОБРАБОТКУ")
        print(f"{'=' * 80}")

        try:
            while not completed and iteration <= max_iterations:
                print(f"\n{'=' * 80}")
                print(f"🔄 ИТЕРАЦИЯ {iteration}")
                print(f"{'=' * 80}")

                # Выводим информацию о текущей итерации
                current_stats = self.get_current_stats()
                remaining_games = current_stats[
                    'games_without_type'] if self.skip_existing and not self.force_assign else current_stats[
                    'total_games']

                print(f"\n📊 ПЕРЕД ИТЕРАЦИЕЙ {iteration}:")
                print(f"   Осталось игр для обработки: {remaining_games:,}")
                print(f"   Начинаем с ID: {self.state.get('last_processed_id', 0)}")
                print(f"   Игр за итерацию: {self.games_per_run if self.games_per_run > 0 else 'ВСЕ оставшиеся'}")

                # Особое сообщение для первой итерации после reset
                if reset_flag and iteration == 1:
                    print(f"   ⚡ РЕЖИМ --reset: все игры будут запрошены из API")

                # Выполняем итерацию
                start_id = self.state.get('last_processed_id', 0)
                chunk_stats = self.process_games_optimized(
                    start_id,
                    self.games_per_run,
                    self.skip_existing
                )

                # Обновляем состояние
                self.state['total_processed'] += chunk_stats['processed']
                self.state['total_cached'] += chunk_stats['cached']
                self.state['non_primary_count'] += chunk_stats['non_primary']
                self.state['failed_batches'] += chunk_stats['failed_batches']
                self.state['last_processed_id'] = chunk_stats['last_processed_id']
                self.state['total_assigned'] = self.state.get('total_assigned', 0) + chunk_stats['assigned']

                for game_type, count in chunk_stats['by_type'].items():
                    self.state['by_type'][game_type] = self.state['by_type'].get(game_type, 0) + count

                # Сохраняем состояние
                ProgressState.save_state(self.state)

                # ДЕТАЛИЗИРОВАННЫЙ ВЫВОД РЕЗУЛЬТАТОВ
                print(f"\n📊 РЕЗУЛЬТАТЫ ИТЕРАЦИИ {iteration}:")
                print(f"   {'=' * 50}")

                # Статистика обработки
                total_processed = chunk_stats['processed'] + chunk_stats['cached']
                print(f"\n   📈 ОБРАБОТКА ДАННЫХ:")
                print(f"      Всего обработано игр: {total_processed:,}")

                # КРИТИЧЕСКАЯ ПРОВЕРКА: если использовался --reset, НЕ ДОЛЖНО БЫТЬ ИГР ИЗ КЭША
                if reset_flag and chunk_stats['cached'] > 0:
                    print(f"\n   ⚠️  ВНИМАНИЕ: Обнаружены игры из кэша после --reset!")
                    print(f"      Количество игр из кэша: {chunk_stats['cached']:,}")
                    print(f"      Это указывает на проблему с очисткой кэша!")

                if chunk_stats['processed'] > 0:
                    print(f"      Новые запросы к IGDB API: {chunk_stats['processed']:,}")

                if chunk_stats['cached'] > 0 and not reset_flag:
                    print(f"      Использовано из кэша: {chunk_stats['cached']:,}")

                # Статистика типов
                print(f"\n   🎮 АНАЛИЗ ТИПОВ ИГР:")
                print(f"      Неосновных игр найдено: {chunk_stats['non_primary']:,}")

                if chunk_stats['by_type']:
                    print(f"      Распределение неосновных игр:")
                    for game_type, count in sorted(chunk_stats['by_type'].items()):
                        percentage = (count / chunk_stats['non_primary'] * 100) if chunk_stats['non_primary'] > 0 else 0
                        print(f"         • {game_type}: {count:,} ({percentage:.1f}%)")

                # Статистика назначения
                if self.assign_to_models:
                    print(f"\n   💾 НАЗНАЧЕНИЕ МОДЕЛЯМ:")
                    print(f"      Назначено типов: {chunk_stats['assigned']:,}")
                    print(f"      Пропущено (уже есть тип): {chunk_stats['skipped']:,}")

                    if chunk_stats['assigned'] > 0:
                        print(f"      Эффективность: {(chunk_stats['assigned'] / total_processed * 100):.1f}%")

                # Статистика ошибок
                if chunk_stats['failed_batches'] > 0:
                    print(f"\n   ⚠️  ОШИБКИ:")
                    print(f"      Неудачных пачек: {chunk_stats['failed_batches']:,}")

                print(f"\n   📍 ПРОГРЕСС:")
                print(f"      Последняя обработанная ID: {chunk_stats['last_processed_id']:,}")
                print(f"      Всего игр в этой итерации: {chunk_stats['total_processed']:,}")

                # Проверяем завершение
                if chunk_stats.get('completed', False):
                    self.state['completed'] = True
                    completed = True
                    print(f"\n   ✅ ЗАВЕРШЕНИЕ:")
                    print(f"      Обработка завершена (флаг completed = True)")

                elif chunk_stats['processed'] == 0 and chunk_stats['assigned'] == 0:
                    print(f"\n   ⚠️  ВНИМАНИЕ:")
                    print(f"      Нет новых данных для обработки")
                    print(f"      • Новые запросы к API: 0")
                    print(f"      • Назначено типов: 0")
                    print(f"      • Возможно все игры уже обработаны")
                    completed = True

                # Обновляем статистику для следующей итерации
                if not completed:
                    current_stats = self.get_current_stats()
                    remaining = current_stats['games_without_type'] if self.skip_existing and not self.force_assign else \
                    current_stats['total_games']
                    print(f"\n   🔄 ПРОДОЛЖЕНИЕ:")
                    print(f"      Осталось игр: {remaining:,}")
                    print(f"      Следующая итерация: {iteration + 1}")
                    iteration += 1
                else:
                    print(f"\n   🏁 ФИНИШ:")
                    print(f"      Итераций выполнено: {iteration}")

        except KeyboardInterrupt:
            print(f"\n\n{'!' * 80}")
            print("⚠️  ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ")
            print(f"{'!' * 80}")

        except Exception as e:
            print(f"\n\n{'!' * 80}")
            print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
            print(f"{'!' * 80}")
            if self.debug_mode:
                import traceback
                traceback.print_exc()

        finally:
            # Закрываем файлы
            self.game_type_file_manager.close_all_files()

            # Получаем финальную статистику
            final_stats = self.get_current_stats()

            print(f"\n{'=' * 80}")
            print("📊 ИТОГОВАЯ СТАТИСТИКА ВЫПОЛНЕНИЯ")
            print(f"{'=' * 80}")

            print(f"\n📈 ОБЩАЯ СТАТИСТИКА:")
            print(f"   Всего итераций: {iteration}")
            print(f"   Всего новых запросов к API: {self.state.get('total_processed', 0):,}")
            print(f"   Всего использовано из кэша: {self.state.get('total_cached', 0):,}")
            print(f"   Всего назначено типов: {self.state.get('total_assigned', 0):,}")
            print(f"   Неосновных игр найдено: {self.state.get('non_primary_count', 0):,}")
            print(f"   Неудачных пачек: {self.state.get('failed_batches', 0):,}")

            # Особое сообщение если использовался reset
            if reset_flag:
                print(f"\n⚡ РЕЗУЛЬТАТ РЕЖИМА --reset:")
                if self.state.get('total_cached', 0) > 0:
                    print(f"   ⚠️  Обнаружены игры из кэша: {self.state.get('total_cached', 0):,}")
                    print(f"   ❌ Очистка кэша могла не сработать полностью")
                else:
                    print(f"   ✅ Очистка кэша успешна: 0 игр из кэша")

            print(f"{'=' * 80}")