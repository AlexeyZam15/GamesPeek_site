# management/commands/find_non_primary_games.py

from django.core.management.base import BaseCommand
from games.models import Game
from games.igdb_api import make_igdb_request, set_debug_mode
from datetime import datetime
import sys
import os
import math
import time
import concurrent.futures
from threading import Lock, Semaphore, Thread
import requests
import json
import pickle
from django.core.cache import cache
import queue
import csv

# Импортируем из пакета game_types
from games.management.commands.game_types import (
    GAME_TYPE_CONFIG,
    get_game_type_info,
    get_game_type_description,
    get_all_flags,
    get_type_statistics_key
)


class ProgressState:
    """Класс для сохранения состояния прогресса между запусками"""

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
            'completed': False
        }

    @classmethod
    def reset_state(cls):
        try:
            cache.delete(cls.STATE_KEY)
        except:
            pass
        return cls.get_default_state()


class GameAnalysisCache:
    """Класс для кэширования результатов анализа игр"""

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
    """Класс для контроля скорости запросов к IGDB API"""

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


class GameTypeFileManager:
    """Менеджер для сохранения игр по типам в отдельные файлы"""

    def __init__(self, output_dir="games_by_game_types", reset=False):
        self.output_dir = output_dir
        self.file_handlers = {}
        self.file_counts = {}
        self.reset = reset  # Добавляем флаг сброса

        # Создаем директорию если не существует
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        # Если reset=True, очищаем директорию
        elif self.reset:
            print(f"🔄 Очищаем директорию {self.output_dir}...")
            self.clear_directory()

        # Закрываем все файлы при выходе
        import atexit
        atexit.register(self.close_all_files)

    def clear_directory(self):
        """Очищает директорию с файлами типов"""
        try:
            # Удаляем все файлы в директории
            for filename in os.listdir(self.output_dir):
                filepath = os.path.join(self.output_dir, filename)
                if os.path.isfile(filepath):
                    os.remove(filepath)
            print(f"✅ Директория очищена")
        except Exception as e:
            print(f"⚠️  Не удалось очистить директорию: {e}")

    def get_game_type_filename(self, game_type):
        """Генерирует имя файла для типа игры"""
        if game_type is None:
            return "unknown.txt"

        info = get_game_type_info(game_type)
        return f"{game_type:02d}_{info['type']}.txt"

    def save_game_by_type(self, game_data, analysis):
        """Сохраняет игру в файл соответствующего типа"""
        game_type = analysis.get('game_type')
        filename = self.get_game_type_filename(game_type)
        filepath = os.path.join(self.output_dir, filename)

        # Открываем файл если еще не открыт
        if filename not in self.file_handlers:
            # Если reset=True, всегда открываем в режиме записи (перезапись)
            if self.reset and os.path.exists(filepath):
                mode = 'w'
            else:
                mode = 'a' if os.path.exists(filepath) else 'w'

            self.file_handlers[filename] = open(filepath, mode, encoding='utf-8')
            self.file_counts[filename] = 0

            # Пишем заголовок если это новый файл или reset=True
            if mode == 'w' or self.reset:
                type_info = get_game_type_info(game_type) if game_type is not None else {
                    'description': 'Unknown Type'
                }
                self.file_handlers[filename].write(
                    f"=== ИГРЫ ТИПА: {type_info['description']} (game_type = {game_type if game_type is not None else 'None'}) ===\n\n"
                )

        # Формируем запись
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

        # Сохраняем
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

        # Выводим статистику
        print(f"\n📁 Игры сохранены по типам в папку: {self.output_dir}")
        for filename, count in sorted(self.file_counts.items()):
            print(f"   {filename}: {count:,} игр")


class Command(BaseCommand):
    help = 'Находит неосновные игры по данным IGDB и сохраняет по типам'

    def __init__(self):
        super().__init__()
        self.GAME_TYPE_CONFIG = GAME_TYPE_CONFIG
        self.csv_file = None
        self.csv_writer = None
        self.game_type_manager = None
        self.rate_limiter = None
        self.state = None
        self.debug_mode = False

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=10,
            help='Размер пачки (по умолчанию 10 игр)'
        )
        parser.add_argument(
            '--max-concurrent',
            type=int,
            default=3,
            help='Максимальное количество одновременных запросов'
        )
        parser.add_argument(
            '--requests-per-second',
            type=float,
            default=3.5,
            help='Количество запросов в секунду'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Включить режим отладки'
        )
        parser.add_argument(
            '--reset',
            action='store_true',
            help='Сбросить сохраненный прогресс'
        )
        parser.add_argument(
            '--games-per-run',
            type=int,
            default=200,  # Было 0, теперь 200
            help='Количество игр за запуск (0 = все оставшиеся)'
        )
        parser.add_argument(
            '--max-retries',
            type=int,
            default=3,
            help='Максимальное количество повторных попыток'
        )
        parser.add_argument(
            '--games-by-types-dir',
            type=str,
            default='games_by_game_types',
            help='Директория для сохранения игр по типам (по умолчанию: games_by_game_types)'
        )
        parser.add_argument(
            '--skip-existing',
            action='store_true',
            help='Пропустить уже обработанные игры'
        )

    def analyze_game_relations(self, game_data):
        """Анализирует связи игры по game_type"""
        game_type = game_data.get('game_type')
        info = get_game_type_info(game_type)

        # Базовый результат
        result = {
            'type': info['type'],
            'game_type': game_type,
            'is_primary': info['is_primary'],
            'game_name': game_data.get('name', 'Неизвестно'),
            'parent_game': game_data.get('parent_game'),
            'version_parent': game_data.get('version_parent'),
            'version_title': game_data.get('version_title'),
            'analyzed_at': datetime.now().isoformat()
        }

        # Устанавливаем флаги
        if 'flag' in info:
            result[info['flag']] = True

        return result

    def get_game_details_from_igdb(self, batch_ids, max_retries=3):
        """Получает данные из IGDB с повторными попытками"""
        for attempt in range(max_retries):
            try:
                self.rate_limiter.wait_for_rate_limit()

                query = f"""
                fields 
                    id, name, game_type, parent_game, version_parent, version_title;
                where id = ({",".join(map(str, batch_ids))});
                limit {len(batch_ids)};
                """

                result = make_igdb_request('games', query, debug=self.debug_mode)

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

    def process_batch(self, batch_ids, batch_games_map, skip_existing=False):
        """Обрабатывает одну пачку данных"""
        try:
            igdb_data = self.get_game_details_from_igdb(batch_ids, self.max_retries)

            batch_stats = {
                'processed': 0,
                'cached': 0,
                'non_primary': 0,
                'by_type': {}
            }

            for igdb_id, game_data in igdb_data.items():
                if igdb_id in batch_games_map:
                    cached_analysis = GameAnalysisCache.get(igdb_id) if not skip_existing else None

                    if cached_analysis:
                        batch_stats['cached'] += 1
                        analysis = cached_analysis
                    else:
                        batch_stats['processed'] += 1
                        analysis = self.analyze_game_relations(game_data)

                        if not skip_existing:
                            GameAnalysisCache.set(igdb_id, analysis)

                    # Сохраняем игру по типу
                    self.game_type_manager.save_game_by_type(game_data, analysis)

                    # Статистика
                    if not analysis.get('is_primary', True):
                        batch_stats['non_primary'] += 1
                        game_type = analysis.get('game_type')
                        stat_key = get_type_statistics_key(game_type)
                        batch_stats['by_type'][stat_key] = batch_stats['by_type'].get(stat_key, 0) + 1

            return {
                'stats': batch_stats,
                'success': True
            }

        except Exception as e:
            if self.debug_mode:
                print(f"[DEBUG] Ошибка в пачке: {e}")
            return {
                'error': str(e),
                'success': False
            }

    def process_games(self, start_id, limit, skip_existing=False):
        """Обрабатывает игры параллельно"""
        games_query = Game.objects.all().order_by('id')

        if start_id > 0:
            games_query = games_query.filter(id__gte=start_id)

        if limit > 0:
            games_chunk = list(games_query[:limit])
        else:
            games_chunk = list(games_query)  # Все оставшиеся игры

        if not games_chunk:
            return {
                'processed': 0,
                'cached': 0,
                'non_primary': 0,
                'by_type': {},
                'last_processed_id': start_id,
                'completed': True
            }

        # Подготавливаем данные
        game_map = {game.igdb_id: game for game in games_chunk}
        all_igdb_ids = list(game_map.keys())

        # Прогресс-бар
        total_games = len(games_chunk)
        print(f"🔍 Обрабатываем {total_games:,} игр...")

        # Разбиваем на пачки
        batches = []
        for i in range(0, len(all_igdb_ids), self.batch_size):
            batch_end = min(i + self.batch_size, len(all_igdb_ids))
            batch_ids = all_igdb_ids[i:batch_end]
            batch_game_map = {igdb_id: game_map[igdb_id] for igdb_id in batch_ids}
            batches.append((batch_ids, batch_game_map))

        # Статистика чанка
        chunk_stats = {
            'processed': 0,
            'cached': 0,
            'non_primary': 0,
            'by_type': {},
            'failed_batches': 0,
            'last_processed_id': games_chunk[-1].id if games_chunk else start_id,
            'total_processed': total_games
        }

        # Инициализируем переменные для прогресса
        self._progress_lock = Lock()
        self._processed_batches = 0
        self._total_batches = len(batches)

        # Функция для обновления прогресса
        def update_progress():
            self._processed_batches += 1
            if self._processed_batches % 5 == 0 or self._processed_batches == self._total_batches:
                percent = (self._processed_batches / self._total_batches) * 100
                print(f"\r📊 Прогресс: {percent:.1f}% ({self._processed_batches}/{self._total_batches} пачек)", end="")

        # Параллельная обработка
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_concurrent) as executor:
            # Запускаем все пачки
            future_to_batch = {}

            for batch_num, (batch_ids, batch_game_map) in enumerate(batches, 1):
                self.rate_limiter.acquire()
                future = executor.submit(self.process_batch, batch_ids, batch_game_map, skip_existing)
                future_to_batch[future] = (batch_ids, batch_game_map, batch_num)

                def callback(f):
                    self.rate_limiter.release()

                    # Обновляем прогресс
                    with self._progress_lock:
                        update_progress()

                future.add_done_callback(callback)

            # Обрабатываем результаты
            for future in concurrent.futures.as_completed(future_to_batch):
                try:
                    result = future.result()

                    if result['success']:
                        batch_stats = result['stats']
                        chunk_stats['processed'] += batch_stats['processed']
                        chunk_stats['cached'] += batch_stats['cached']
                        chunk_stats['non_primary'] += batch_stats['non_primary']

                        for game_type, count in batch_stats['by_type'].items():
                            chunk_stats['by_type'][game_type] = chunk_stats['by_type'].get(game_type, 0) + count
                    else:
                        chunk_stats['failed_batches'] += 1

                except Exception:
                    chunk_stats['failed_batches'] += 1

        print()  # Новая строка после прогресса
        return chunk_stats

    def get_cache_stats(self):
        """Получает текущую статистику из кэша"""
        total_games = Game.objects.count()
        total_checked = self.state['total_processed'] + self.state['total_cached']
        unverified = total_games - total_checked
        non_primary = self.state['non_primary_count']
        primary = total_checked - non_primary

        return {
            'total_games': total_games,
            'total_checked': total_checked,
            'unverified': unverified,
            'primary': primary,
            'non_primary': non_primary
        }

    def run_iteration(self):
        """Выполняет одну итерацию обработки"""
        stats_before = self.get_cache_stats()

        # Определяем сколько обработать
        if self.games_per_run > 0:
            games_to_process = min(self.games_per_run, stats_before['unverified'])
        else:
            games_to_process = stats_before['unverified']  # Все оставшиеся

        if games_to_process <= 0:
            print("✅ Все игры уже обработаны!")
            self.state['completed'] = True
            return True

        print(f"\n🚀 Итерация: обрабатываем {games_to_process:,} игр...")

        # Обрабатываем игры
        start_id = self.state.get('last_processed_id', 0)
        chunk_stats = self.process_games(
            start_id,
            games_to_process,
            skip_existing=self.skip_existing
        )

        # Обновляем состояние
        self.state['total_processed'] += chunk_stats['processed']
        self.state['total_cached'] += chunk_stats['cached']
        self.state['non_primary_count'] += chunk_stats['non_primary']
        self.state['failed_batches'] += chunk_stats['failed_batches']
        self.state['last_processed_id'] = chunk_stats['last_processed_id']

        for game_type, count in chunk_stats['by_type'].items():
            self.state['by_type'][game_type] = self.state['by_type'].get(game_type, 0) + count

        # Получаем финальную статистику
        stats_after = self.get_cache_stats()

        # Выводим результаты итерации
        print(f"\n📊 РЕЗУЛЬТАТЫ ИТЕРАЦИИ:")
        print(f"   Обработано: {chunk_stats['processed'] + chunk_stats['cached']:,} игр")
        print(f"   Новых запросов: {chunk_stats['processed']:,}")
        print(f"   Из кэша: {chunk_stats['cached']:,}")
        print(f"   Неосновных найдено: {chunk_stats['non_primary']:,}")

        if chunk_stats['by_type']:
            print(f"\n📈 Распределение неосновных игр по типам:")
            for game_type, count in sorted(chunk_stats['by_type'].items()):
                if count > 0:
                    print(f"   {game_type}: {count:,}")

        print(f"\n📊 ОБЩАЯ СТАТИСТИКА:")
        print(f"   Всего игр в БД: {stats_after['total_games']:,}")
        print(
            f"   Проверено: {stats_after['total_checked']:,} ({stats_after['total_checked'] / stats_after['total_games'] * 100:.1f}%)")
        print(f"   Осталось: {stats_after['unverified']:,}")
        print(f"   Основных (всего): {stats_after['primary']:,}")
        print(f"   Неосновных (всего): {stats_after['non_primary']:,}")

        # Проверяем завершение
        if stats_after['unverified'] <= 0:
            self.state['completed'] = True
            return True

        return False

    def handle(self, *args, **kwargs):
        """Основной метод обработки команды"""

        # Устанавливаем параметры
        self.batch_size = kwargs['batch_size']
        self.max_concurrent = kwargs['max_concurrent']
        self.requests_per_second = kwargs['requests_per_second']
        self.debug_mode = kwargs['debug']
        self.games_per_run = kwargs['games_per_run']
        self.max_retries = kwargs['max_retries']
        self.skip_existing = kwargs['skip_existing']
        games_by_types_dir = kwargs['games_by_types_dir']
        reset_flag = kwargs['reset']  # Получаем флаг сброса

        # Настраиваем отладку
        set_debug_mode(self.debug_mode)

        # Инициализируем менеджер типов с флагом reset
        self.game_type_manager = GameTypeFileManager(games_by_types_dir, reset=reset_flag)

        # Инициализируем ограничитель скорости
        self.rate_limiter = RateLimiter(
            max_concurrent=self.max_concurrent,
            requests_per_second=self.requests_per_second
        )

        # Загружаем или сбрасываем состояние
        if reset_flag:
            self.state = ProgressState.reset_state()
            print("🔄 Прогресс сброшен")
        else:
            self.state = ProgressState.load_state()

        # Выводим информацию
        print(self.style.SUCCESS('🔍 ПОИСК И СОРТИРОВКА ИГР ПО ТИПАМ'))
        print(f"📦 Пачки: {self.batch_size} игр, {self.max_concurrent} параллельных")
        print(f"⚡ Скорость: {self.requests_per_second} запросов/сек")
        print(f"📁 Игры по типам будут сохранены в: {games_by_types_dir}")

        if self.games_per_run > 0:
            print(f"🎯 Игр за запуск: {self.games_per_run:,}")
        else:
            print(f"🎯 Игр за запуск: ВСЕ оставшиеся")

        # Выводим конфиг типов
        print(f"\n🎮 КОНФИГ GAME_TYPE:")
        for gt, info in sorted(self.GAME_TYPE_CONFIG.items()):
            status = "ОСНОВНАЯ" if info['is_primary'] else "НЕОСНОВНАЯ"
            print(f"  {gt}: {info['description']} - {status}")

        # Получаем начальную статистику
        stats_before = self.get_cache_stats()
        print(f"\n📊 НАЧАЛЬНАЯ СТАТИСТИКА:")
        print(f"   Всего игр в БД: {stats_before['total_games']:,}")
        print(
            f"   Проверено: {stats_before['total_checked']:,} ({stats_before['total_checked'] / stats_before['total_games'] * 100:.1f}%)")
        print(f"   Непроверенных: {stats_before['unverified']:,}")

        # Проверяем, все ли уже обработано
        if stats_before['unverified'] <= 0:
            print("✅ Все игры уже обработаны!")
            self.game_type_manager.close_all_files()
            return

        # Запускаем итерации
        iteration = 1
        completed = False

        try:
            while not completed:
                print(f"\n{'=' * 70}")
                print(f"🔄 ИТЕРАЦИЯ {iteration}")
                print(f"{'=' * 70}")

                # Выполняем итерацию
                completed = self.run_iteration()

                # Сохраняем состояние после каждой итерации
                ProgressState.save_state(self.state)

                if not completed:
                    iteration += 1
                    # Можно добавить паузу между итерациями если нужно
                    # time.sleep(1)

        except KeyboardInterrupt:
            print(f"\n\n⚠  Прервано пользователем")
            print(f"📊 Завершено итераций: {iteration - 1}")

        finally:
            # Закрываем файлы и выводим итоги
            self.game_type_manager.close_all_files()

            # Итог
            print(f"\n{'=' * 70}")
            if self.state['completed']:
                print("✅ ВСЕ ИГРЫ ОБРАБОТАНЫ!")
            else:
                stats_current = self.get_cache_stats()
                print(f"⏸  Обработка приостановлена")
                print(f"   Прогресс: {stats_current['total_checked']:,}/{stats_current['total_games']:,} игр")
                print(f"   Для продолжения: python manage.py find_non_primary_games")
            print("=" * 70)
