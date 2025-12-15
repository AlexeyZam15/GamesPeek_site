# games/management/commands/import_rawg_descriptions.py
import time
import re
import json
import os
import sqlite3
import hashlib
from datetime import datetime
import concurrent.futures
from pathlib import Path
from collections import defaultdict

from django.core.management.base import BaseCommand
from django.db.models import Q, Case, When, Value, TextField
from games.models import Game, GameType
from games.rawg_api import RAWGClient


class Command(BaseCommand):
    help = 'Импорт описаний из RAWG API с кэшированием и оптимизацией'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Только проверка без сохранения'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,  # Изменено с 100 на 0 (без лимита)
            help='Количество игр для обработки (0 = все игры)'
        )
        parser.add_argument(
            '--workers',
            type=int,
            default=4,
            help='Потоков (рекомендуется 4 для кэширования)'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=0.3,
            help='Задержка между запросами (секунды)'
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Перезаписать существующие описания'
        )
        parser.add_argument(
            '--api-key',
            type=str,
            help='RAWG API ключ (если не указан, берется из .env)'
        )
        parser.add_argument(
            '--min-length',
            type=int,
            default=1,
            help='Минимальная длина описания'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Подробный вывод отладки'
        )
        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Пропустить первые N игр'
        )
        parser.add_argument(
            '--order-by',
            type=str,
            default='id',
            choices=['id', 'name', '-rating', '-rating_count', '-first_release_date'],
            help='Поле для сортировки игр'
        )
        parser.add_argument(
            '--log-dir',
            type=str,
            default='logs',
            help='Директория для логов'
        )
        parser.add_argument(
            '--game-ids',
            type=str,
            help='ID конкретных игр для обработки (через запятую)'
        )
        parser.add_argument(
            '--repeat',
            type=int,
            default=0,  # Изменено с 1 на 0 (бесконечные повторения)
            help='Повторить команду N раз (0 = пока не обработаны все игры)'
        )
        parser.add_argument(
            '--repeat-delay',
            type=float,
            default=60.0,
            help='Пауза между повторами в секундах'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=50,  # Новая опция - размер батча
            help='Размер батча за один повтор (по умолчанию: 50)'
        )
        parser.add_argument(
            '--auto-offset',
            action='store_true',
            default=True,
            help='Автоматически пропускать не найденные игры (по умолчанию: ВКЛ)'
        )
        parser.add_argument(
            '--no-auto-offset',
            action='store_false',
            dest='auto_offset',
            help='Выключить автоматический пропуск не найденных игр'
        )
        parser.add_argument(
            '--auto-offset-file',
            type=str,
            default='auto_offset_log.json',
            help='Файл для хранения списка не найденных игр'
        )
        parser.add_argument(
            '--cache-ttl',
            type=int,
            default=30,
            help='Время жизни кэша в днях (0 = бесконечно)'
        )
        parser.add_argument(
            '--skip-cache',
            action='store_true',
            help='Пропустить кэш (для тестирования)'
        )
        parser.add_argument(
            '--reset',
            action='store_true',
            help='Удалить кэш перед началом работы'
        )
        parser.add_argument(
            '--include-all-gametypes',
            action='store_true',
            help='Включить все типы игр (не только основные)'
        )

    def __init__(self):
        super().__init__()
        self.rawg_client = None
        self.stats_db = None
        self.api_stats = defaultdict(int)
        self.init_stats_db()

    def init_stats_db(self):
        """Инициализация БД для статистики"""
        try:
            stats_dir = Path('stats')
            stats_dir.mkdir(exist_ok=True)

            self.stats_db = sqlite3.connect(stats_dir / 'api_stats.db', timeout=10)
            cursor = self.stats_db.cursor()

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS request_stats
                           (
                               date
                               TEXT
                               PRIMARY
                               KEY,
                               total_requests
                               INTEGER
                               DEFAULT
                               0,
                               cache_hits
                               INTEGER
                               DEFAULT
                               0,
                               cache_misses
                               INTEGER
                               DEFAULT
                               0,
                               search_requests
                               INTEGER
                               DEFAULT
                               0,
                               detail_requests
                               INTEGER
                               DEFAULT
                               0,
                               rate_limited
                               INTEGER
                               DEFAULT
                               0,
                               avg_response_time
                               REAL
                               DEFAULT
                               0
                           )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS game_stats
                           (
                               game_hash
                               TEXT
                               PRIMARY
                               KEY,
                               game_name
                               TEXT
                               NOT
                               NULL,
                               found_count
                               INTEGER
                               DEFAULT
                               0,
                               not_found_count
                               INTEGER
                               DEFAULT
                               0,
                               error_count
                               INTEGER
                               DEFAULT
                               0,
                               last_checked
                               TIMESTAMP,
                               first_found
                               TIMESTAMP
                           )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS efficiency_stats
                           (
                               timestamp
                               TEXT
                               PRIMARY
                               KEY,
                               cache_efficiency
                               REAL,
                               requests_saved
                               INTEGER,
                               avg_requests_per_game
                               REAL
                           )
                           ''')

            self.stats_db.commit()

        except Exception as e:
            self.stdout.write(f'⚠️ Ошибка инициализации статистики: {e}')
            self.stats_db = None

    def reset_cache(self):
        """Удаление кэша RAWG API"""
        cache_paths = [
            Path('cache') / 'rawg_cache.db',
            Path('cache') / 'rawg_cache.db-wal',
            Path('cache') / 'rawg_cache.db-shm',
            Path('cache') / 'rawg_cache.db-journal'
        ]

        deleted_count = 0
        for cache_path in cache_paths:
            if cache_path.exists():
                try:
                    os.remove(cache_path)
                    deleted_count += 1
                    self.stdout.write(f'   Удален: {cache_path}')
                except Exception as e:
                    self.stdout.write(f'   ⚠️ Не удалось удалить {cache_path}: {e}')

        if deleted_count > 0:
            self.stdout.write(self.style.SUCCESS(f'✅ Удалено {deleted_count} файлов кэша'))
        else:
            self.stdout.write('ℹ️ Файлы кэша не найдены')

    def normalize_game_name(self, game_name):
        """Нормализация названия игры для кэширования"""
        normalized = game_name.lower().strip()
        normalized = re.sub(r'\s+', ' ', normalized)

        # Для поиска удаляем версии и года в скобках
        search_name = re.sub(r'\([^)]*\)', '', normalized)
        search_name = re.sub(
            r'\b(remastered|definitive edition|game of the year|goty|enhanced edition)\b',
            '', search_name, flags=re.IGNORECASE
        )
        search_name = search_name.strip()

        # Хэш для уникальности
        name_hash = hashlib.md5(game_name.encode('utf-8')).hexdigest()

        return {
            'hash': name_hash,
            'original': game_name,
            'search': search_name if search_name else normalized
        }

    def update_stats(self, stat_type, value=1):
        """Обновление статистики"""
        self.api_stats[stat_type] += value

        # Обновляем в БД статистики
        if self.stats_db:
            try:
                today = datetime.now().strftime('%Y-%m-%d')

                cursor = self.stats_db.cursor()

                # Проверяем существующую запись за сегодня
                cursor.execute('SELECT 1 FROM request_stats WHERE date = ?', (today,))
                exists = cursor.fetchone()

                if exists:
                    # Обновляем существующую
                    if stat_type == 'cache_hit':
                        cursor.execute(
                            'UPDATE request_stats SET cache_hits = cache_hits + ? WHERE date = ?',
                            (value, today)
                        )
                    elif stat_type == 'cache_miss':
                        cursor.execute(
                            'UPDATE request_stats SET cache_misses = cache_misses + ? WHERE date = ?',
                            (value, today)
                        )
                    elif stat_type == 'search_request':
                        cursor.execute(
                            'UPDATE request_stats SET search_requests = search_requests + ? WHERE date = ?',
                            (value, today)
                        )
                    elif stat_type == 'detail_request':
                        cursor.execute(
                            'UPDATE request_stats SET detail_requests = detail_requests + ? WHERE date = ?',
                            (value, today)
                        )
                    elif stat_type == 'rate_limited':
                        cursor.execute(
                            'UPDATE request_stats SET rate_limited = rate_limited + ? WHERE date = ?',
                            (value, today)
                        )

                    # Всегда обновляем total_requests
                    cursor.execute(
                        'UPDATE request_stats SET total_requests = total_requests + ? WHERE date = ?',
                        (value, today)
                    )
                else:
                    # Создаем новую
                    initial_values = {
                        'cache_hits': 1 if stat_type == 'cache_hit' else 0,
                        'cache_misses': 1 if stat_type == 'cache_miss' else 0,
                        'search_requests': 1 if stat_type == 'search_request' else 0,
                        'detail_requests': 1 if stat_type == 'detail_request' else 0,
                        'rate_limited': 1 if stat_type == 'rate_limited' else 0,
                        'total_requests': 1
                    }

                    cursor.execute('''
                                   INSERT INTO request_stats
                                   (date, cache_hits, cache_misses, search_requests, detail_requests, rate_limited,
                                    total_requests)
                                   VALUES (?, ?, ?, ?, ?, ?, ?)
                                   ''', (
                                       today,
                                       initial_values['cache_hits'],
                                       initial_values['cache_misses'],
                                       initial_values['search_requests'],
                                       initial_values['detail_requests'],
                                       initial_values['rate_limited'],
                                       initial_values['total_requests']
                                   ))

                self.stats_db.commit()

            except Exception as e:
                if self.debug:
                    self.stdout.write(f'   ⚠️ Ошибка обновления статистики: {e}')

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        # Проверяем опцию reset и удаляем кэш если нужно
        if options.get('reset'):
            self.stdout.write('🧹 Удаление кэша RAWG API...')
            self.reset_cache()

            # Если указан только reset, выходим
            if len([k for k, v in options.items() if v and k not in ['reset', 'verbosity']]) == 1:
                return

        # Инициализация клиента RAWG
        if not self.init_rawg_client(options):
            return

        # Сохраняем опции
        self.original_options = options.copy()

        # Запускаем основной процесс импорта
        self.run_main_import_process(options)

    def init_rawg_client(self, options):
        """Инициализация клиента RAWG"""
        try:
            self.rawg_client = RAWGClient(
                api_key=options.get('api_key')  # Если не указан, берется из settings
            )
            self.stdout.write(self.style.SUCCESS('✅ RAWG клиент инициализирован'))
            return True
        except ValueError as e:
            self.stdout.write(self.style.ERROR(f'❌ {e}'))
            self.stdout.write(self.style.WARNING('💡 Укажите ключ через --api-key или добавьте RAWG_API_KEY в .env'))
            return False
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка инициализации RAWG клиента: {e}'))
            return False

    def run_main_import_process(self, options):
        """Основной процесс импорта с повторениями"""
        # Настройки повторения
        repeat_times = options['repeat']
        repeat_delay = options['repeat_delay']
        auto_offset = options.get('auto_offset', True)
        batch_size = options.get('batch_size', 50)

        # Если limit = 0 и repeat = 0, значит бесконечные повторения до обработки всех игр
        infinite_mode = options['limit'] == 0 and repeat_times == 0

        # Инициализация для auto-offset
        self.not_found_ids = set()
        if auto_offset:
            self.load_not_found_ids(options['auto_offset_file'])

        # Глобальная статистика
        global_stats = {
            'total_repeats': 0,
            'completed_repeats': 0,
            'total_games_processed': 0,
            'total_games_updated': 0,
            'total_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'start_time': time.time()
        }

        if infinite_mode:
            self.stdout.write(self.style.SUCCESS(
                '🔄 ЗАПУСК БЕСКОНЕЧНОГО ИМПОРТА: будут обрабатываться все игры батчами по 50'
            ))
        else:
            self.stdout.write(self.style.SUCCESS(
                f'🔄 ЗАПУСК ОПТИМИЗИРОВАННОЙ КОМАНДЫ: {repeat_times} раз(а)'
            ))

        # Показываем информацию о кэше
        self.show_cache_info(options)

        # Выполняем повторения
        if infinite_mode:
            self.execute_infinite_repeats(global_stats, options, repeat_delay, auto_offset, batch_size)
        else:
            self.execute_limited_repeats(global_stats, options, repeat_times, repeat_delay, auto_offset)

        # Выводим финальную статистику
        self.show_final_global_stats(global_stats)

    def execute_infinite_repeats(self, global_stats, options, repeat_delay, auto_offset, batch_size):
        """Выполняет бесконечные повторения до обработки всех игр"""
        offset = options.get('offset', 0)
        games_processed = 0
        repeat_num = 1

        while True:
            self.stdout.write(f'\n' + '=' * 50)
            self.stdout.write(f'🚀 ПОВТОРЕНИЕ {repeat_num} (бесконечный режим)')
            self.stdout.write(f'📦 Батч: {batch_size} игр, offset: {offset}')

            # Устанавливаем лимит для этого повторения
            current_options = options.copy()
            current_options['limit'] = batch_size
            current_options['offset'] = offset

            # Запускаем один импорт
            repeat_stats = self.run_single_import(repeat_num, auto_offset, current_options)

            # Обновляем глобальную статистику
            self.update_global_stats(global_stats, repeat_stats)

            # Обновляем счетчики
            games_processed_in_batch = repeat_stats.get('total', 0)
            games_processed += games_processed_in_batch
            offset += games_processed_in_batch

            # Сохраняем список не найденных игр
            if auto_offset and repeat_stats.get('new_not_found', 0) > 0:
                self.save_not_found_ids(options['auto_offset_file'])

            # Сохраняем статистику эффективности
            self.save_efficiency_stats(repeat_stats, repeat_num)

            # Проверяем, нужно ли продолжать
            if games_processed_in_batch == 0:
                self.stdout.write('\n🎉 Все игры обработаны!')
                break

            # Показываем прогресс
            try:
                total_games = Game.objects.count()
                progress = (offset / total_games * 100) if total_games > 0 else 0
                self.stdout.write(f'📊 Прогресс: {offset}/{total_games} игр ({progress:.1f}%)')
            except:
                pass

            # Пауза между повторами
            self.stdout.write(f'\n⏳ Пауза {repeat_delay} секунд...')
            time.sleep(repeat_delay)

            repeat_num += 1

    def execute_limited_repeats(self, global_stats, options, repeat_times, repeat_delay, auto_offset):
        """Выполняет ограниченное количество повторений"""
        for repeat_num in range(1, repeat_times + 1):
            self.stdout.write(f'\n' + '=' * 50)
            self.stdout.write(f'🚀 ПОВТОРЕНИЕ {repeat_num}/{repeat_times}')

            # Запускаем один импорт
            repeat_stats = self.run_single_import(repeat_num, auto_offset, options)

            # Обновляем глобальную статистику
            self.update_global_stats(global_stats, repeat_stats)

            # Сохраняем список не найденных игр
            if auto_offset and repeat_stats.get('new_not_found', 0) > 0:
                self.save_not_found_ids(options['auto_offset_file'])

            # Сохраняем статистику эффективности
            self.save_efficiency_stats(repeat_stats, repeat_num)

            # Если не последний повтор - пауза
            if repeat_num < repeat_times:
                self.stdout.write(f'\n⏳ Пауза {repeat_delay} секунд...')
                time.sleep(repeat_delay)

    def show_cache_info(self, options):
        """Показывает информацию о кэше"""
        if not options['skip_cache']:
            self.stdout.write('💾 Кэширование: ВКЛЮЧЕНО')
        else:
            self.stdout.write('💾 Кэширование: ВЫКЛЮЧЕНО')

        auto_offset = options.get('auto_offset', True)
        if auto_offset:
            self.stdout.write(f'⚡ Auto-offset: пропуск {len(self.not_found_ids)} не найденных игр')

    def execute_repeats(self, global_stats, options, repeat_times, repeat_delay, auto_offset):
        """Выполняет все повторения импорта"""
        for repeat_num in range(1, repeat_times + 1):
            self.stdout.write(f'\n' + '=' * 50)
            self.stdout.write(f'🚀 ПОВТОРЕНИЕ {repeat_num}/{repeat_times}')

            # Запускаем один импорт
            repeat_stats = self.run_single_import(repeat_num, auto_offset, options)

            # Обновляем глобальную статистику
            self.update_global_stats(global_stats, repeat_stats)

            # Сохраняем список не найденных игр
            if auto_offset and repeat_stats.get('new_not_found', 0) > 0:
                self.save_not_found_ids(options['auto_offset_file'])

            # Сохраняем статистику эффективности
            self.save_efficiency_stats(repeat_stats, repeat_num)

            # Если не последний повтор - пауза
            if repeat_num < repeat_times:
                self.stdout.write(f'\n⏳ Пауза {repeat_delay} секунд...')
                time.sleep(repeat_delay)

    def update_global_stats(self, global_stats, repeat_stats):
        """Обновляет глобальную статистику"""
        global_stats['completed_repeats'] += 1
        global_stats['total_games_processed'] += repeat_stats.get('total', 0)
        global_stats['total_games_updated'] += repeat_stats.get('updated', 0)
        global_stats['total_errors'] += repeat_stats.get('errors', 0)
        global_stats['cache_hits'] += repeat_stats.get('cache_hits', 0)
        global_stats['cache_misses'] += repeat_stats.get('cache_misses', 0)

    def show_final_global_stats(self, global_stats):
        """Показывает финальную глобальную статистику"""
        total_time = time.time() - global_stats['start_time']

        self.stdout.write('\n' + '🎉' * 20)
        self.stdout.write(self.style.SUCCESS('🏆 ВСЕ ПОВТОРЕНИЯ ЗАВЕРШЕНЫ!'))
        self.stdout.write('=' * 50)

        self.stdout.write(f'📊 ГЛОБАЛЬНАЯ СТАТИСТИКА:')
        self.stdout.write(f'   🔁 Выполнено повторов: {global_stats["completed_repeats"]}')
        self.stdout.write(f'   ⏱️  Общее время: {total_time:.1f} сек')
        self.stdout.write(f'   📈 Обработано игр: {global_stats["total_games_processed"]:,}')
        self.stdout.write(f'   ✅ Обновлено описаний: {global_stats["total_games_updated"]:,}')
        self.stdout.write(f'   💾 Попаданий в кэш: {global_stats["cache_hits"]:,}')
        self.stdout.write(f'   💥 Всего ошибок: {global_stats["total_errors"]}')

        if global_stats['total_games_processed'] > 0:
            overall_speed = global_stats['total_games_processed'] / total_time
            cache_efficiency = (global_stats['cache_hits'] / global_stats['total_games_processed'] * 100)

            self.stdout.write(f'\n📈 ЭФФЕКТИВНОСТЬ:')
            self.stdout.write(f'   ⚡ Общая скорость: {overall_speed:.1f} игр/сек')
            self.stdout.write(f'   💾 Общая эффективность кэша: {cache_efficiency:.1f}%')

            # Оценка экономии
            estimated_no_cache = global_stats['total_games_processed'] * 1.5
            requests_with_cache = (global_stats['total_games_processed'] - global_stats['cache_hits']) * 1.5
            requests_saved = estimated_no_cache - requests_with_cache

            self.stdout.write(f'   💰 Сэкономлено запросов API: ~{int(requests_saved):,}')

        # Показываем информацию о клиенте
        if self.rawg_client:
            client_stats = self.rawg_client.get_stats()
            self.stdout.write(f'\n🔧 RAWG КЛИЕНТ:')
            self.stdout.write(f'   🎯 Запросов к API: {client_stats["total_requests"]}')
            self.stdout.write(f'   🚫 Rate limited: {client_stats["rate_limited"]}')

        # ПОКАЗЫВАЕМ ФИНАЛЬНУЮ СТАТИСТИКУ БД
        self.stdout.write('\n' + '🏁' * 15)
        self.stdout.write('📊 ФИНАЛЬНАЯ СТАТИСТИКА БАЗЫ ДАННЫХ:')
        self.show_rawg_stats()

        # Показываем детальную статистику API
        self.show_api_statistics()

    def get_cache_stats(self):
        """Получает статистику кэша через RAWG клиент"""
        if not self.rawg_client or not hasattr(self.rawg_client, 'cache_conn'):
            return {'total': 0, 'hits': 0}

        try:
            cursor = self.rawg_client.cache_conn.cursor()

            # Общее количество записей
            cursor.execute('SELECT COUNT(*) FROM rawg_cache')
            total = cursor.fetchone()[0]

            # Количество записей с попаданиями
            cursor.execute('SELECT SUM(request_count) FROM rawg_cache')
            hits = cursor.fetchone()[0] or 0

            # Количество уникальных запросов за последние 7 дней
            cursor.execute('''
                           SELECT COUNT(DISTINCT game_hash)
                           FROM rawg_cache
                           WHERE updated_at > datetime('now', '-7 days')
                           ''')
            recent = cursor.fetchone()[0]

            return {
                'total': total,
                'hits': hits,
                'recent': recent
            }

        except Exception:
            return {'total': 0, 'hits': 0}

    def run_single_import(self, repeat_num, auto_offset=False, options=None):
        """Запускает один импорт"""
        if options is None:
            options = self.original_options.copy()

        # Инициализируем настройки импорта
        self.init_import_settings(options)

        # Auto-offset - теперь по умолчанию включен
        auto_offset = options.get('auto_offset', True)

        # Инициализируем статистику
        self.init_stats(repeat_num)

        # Получаем игры для обработки
        games = self.get_games_to_process(options.get('game_ids'), auto_offset)
        if not games:
            self.show_rawg_stats()
            return self.stats

        # Показываем стартовую информацию
        self.show_start_info(games, repeat_num, auto_offset)

        # Обрабатываем игры
        results = self.process_games_optimized(games)

        # Обновляем список не найденных игр
        self.update_not_found_list(results, auto_offset)

        # Сохраняем результаты
        self.save_import_results(games, results)

        # Показываем финальную статистику
        self.show_import_final_stats(results)

        # Сохраняем логи
        self.save_import_logs(results, games, repeat_num)

        # Показываем статистику БД - ВСЕГДА в конце
        self.show_rawg_stats()

        return self.stats

    def init_import_settings(self, options):
        """Инициализирует настройки для одного импорта"""
        self.dry_run = options['dry_run']
        self.limit = options['limit']
        self.workers = options['workers']
        self.delay = options['delay']
        self.overwrite = options['overwrite']
        self.min_length = options['min_length']
        self.debug = options['debug']
        self.offset = options['offset']
        self.order_by = options['order_by']
        self.log_dir = options['log_dir']
        self.skip_cache = options['skip_cache']
        self.include_all_gametypes = options.get('include_all_gametypes', False)

    def init_stats(self, repeat_num):
        """Инициализирует статистику для импорта"""
        self.stats = {
            'start': time.time(),
            'total': 0,
            'found': 0,
            'short': 0,
            'empty': 0,
            'errors': 0,
            'requests': 0,
            'rate_limited': 0,
            'not_found_count': 0,
            'updated': 0,
            'repeat_num': repeat_num,
            'new_not_found': 0,
            'auto_offset_skipped': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'search_requests': 0,
            'detail_requests': 0
        }

    def update_not_found_list(self, results, auto_offset):
        """Обновляет список не найденных игр"""
        if auto_offset and results['not_found']:
            new_not_found = set(results['not_found']) - self.not_found_ids
            self.not_found_ids.update(new_not_found)
            self.stats['new_not_found'] = len(new_not_found)

    def save_import_results(self, games, results):
        """Сохраняет результаты импорта"""
        self.stats['updated'] = len(results['descriptions'])

        if self.dry_run:
            self.show_dry_run_results(results)
        else:
            self.save_descriptions_to_db(results)

    def show_dry_run_results(self, results):
        """Показывает результаты dry run"""
        self.stdout.write('\n📊 [DRY RUN] РЕЗУЛЬТАТЫ:')
        self.stdout.write(f'   ✅ Найдено описаний: {len(results["descriptions"])}')
        self.stdout.write(f'   📏 Коротких: {self.stats["short"]}')
        self.stdout.write(f'   🚫 Пустых: {self.stats["empty"]}')
        self.stdout.write(f'   ❓ Не найдено: {len(results["not_found"])}')
        self.stdout.write(f'   💥 Ошибок: {len(results["errors"])}')
        self.stdout.write(f'   ⚠️  Rate limited: {self.stats["rate_limited"]}')
        self.stdout.write(f'   💾 Попаданий в кэш: {self.stats["cache_hits"]}')
        self.stdout.write(f'   🔍 Поисковых запросов: {self.stats["search_requests"]}')
        self.stdout.write(f'   📄 Запросов деталей: {self.stats["detail_requests"]}')

    def save_descriptions_to_db(self, results):
        """Сохраняет описания в базу данных"""
        if results['descriptions']:
            self.stdout.write(f'\n💾 Сохраняем {len(results["descriptions"])} описаний...')
            start_save = time.time()
            self.bulk_save_descriptions(results['descriptions'])
            save_time = time.time() - start_save
            self.stdout.write(f'✅ Готово за {save_time:.1f} сек!')

    def show_import_final_stats(self, results):
        """Показывает финальную статистику импорта"""
        total_time = time.time() - self.stats['start']

        self.stdout.write('\n' + '=' * 50)
        self.stdout.write(f'🏁 ИМПОРТ [{self.stats["repeat_num"]}] ЗАВЕРШЕН!')

        if self.stats['total'] > 0:
            games_per_sec = self.stats['total'] / total_time

            # Эффективность кэша
            cache_checks = self.stats['cache_hits'] + self.stats['cache_misses']
            cache_efficiency = (self.stats['cache_hits'] / cache_checks * 100) if cache_checks > 0 else 0

            # Экономия запросов
            estimated_no_cache = cache_checks * 1.5
            actual_requests = self.stats['search_requests'] + self.stats['detail_requests']
            requests_saved = estimated_no_cache - actual_requests

            self.stdout.write(f'⏱️  Время: {total_time:.1f} сек')
            self.stdout.write(f'⚡ Скорость: {games_per_sec:.1f} игр/сек')
            self.stdout.write(f'🎯 Эффективность кэша: {cache_efficiency:.1f}%')
            self.stdout.write(f'💰 Сэкономлено запросов: ~{int(requests_saved)}')

        self.stdout.write(f'📊 Всего игр: {self.stats["total"]}')
        self.stdout.write(f'✅ Найдено: {self.stats["found"]}')
        self.stdout.write(f'📏 Коротких: {self.stats["short"]}')
        self.stdout.write(f'🚫 Пустых: {self.stats["empty"]}')
        self.stdout.write(f'❓ Не найдено (IGDB_ID): {self.stats["not_found_count"]}')
        self.stdout.write(f'💥 Ошибок: {self.stats["errors"]}')
        self.stdout.write(f'💾 Кэш попаданий: {self.stats["cache_hits"]}')
        self.stdout.write(f'🔍 Запросов поиска: {self.stats["search_requests"]}')
        self.stdout.write(f'📄 Запросов деталей: {self.stats["detail_requests"]}')

        # Добавляем информацию о пропущенных играх через auto-offset
        if self.stats.get('auto_offset_skipped', 0) > 0:
            self.stdout.write(f'⏩ Пропущено через auto-offset: {self.stats["auto_offset_skipped"]}')

        if self.dry_run:
            self.stdout.write('\n⚠️  DRY RUN: данные НЕ сохранены')

    def save_import_logs(self, results, games, repeat_num):
        """Сохраняет логи импорта"""
        import os
        os.makedirs(self.log_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Получаем статистику от RAWG клиента
        client_stats = self.rawg_client.get_stats() if self.rawg_client else {}

        # Сохраняем статистику с информацией о кэше
        stats_data = {
            'meta': {
                'timestamp': datetime.now().isoformat(),
                'repeat': repeat_num,
                'cache_enabled': not self.skip_cache,
                'cache_ttl': self.original_options.get('cache_ttl', 30),
                'delay': self.delay,
                'workers': self.workers,
                'client_initialized': self.rawg_client is not None,
                'include_all_gametypes': self.include_all_gametypes,
                'auto_offset_enabled': self.original_options.get('auto_offset', True),
                'auto_offset_skipped': self.stats.get('auto_offset_skipped', 0)
            },
            'stats': self.stats.copy(),
            'rawg_client_stats': client_stats,
            'game_count': len(games),
            'results_summary': {
                'descriptions_found': len(results['descriptions']),
                'not_found': len(results['not_found']),
                'errors': len(results['errors']),
                'short': len(results['short'])
            },
            'auto_offset_data': {
                'not_found_ids_count': len(self.not_found_ids),
                'not_found_ids_sample': list(self.not_found_ids)[:10] if self.not_found_ids else [],
                'note': 'not_found_ids содержит IGDB_ID игр'
            }
        }

        stats_file = os.path.join(self.log_dir, f'stats_repeat{repeat_num}_{timestamp}.json')
        try:
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            if self.debug:
                self.stdout.write(f'   ⚠️ Ошибка сохранения логов: {e}')

    def process_games_optimized(self, games):
        """Оптимизированная обработка игр с кэшированием"""
        results = {
            'descriptions': {},
            'not_found': [],  # Будет хранить IGDB_ID игр
            'errors': [],
            'short': []
        }

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            # Создаем задачи
            future_to_game = {}
            for game in games:
                future = executor.submit(
                    self.get_game_description_optimized,
                    game
                )
                future_to_game[future] = game

            # Обрабатываем результаты
            completed = 0
            for future in concurrent.futures.as_completed(future_to_game):
                game = future_to_game[future]
                completed += 1

                try:
                    result = future.result(timeout=30)

                    if result['status'] == 'found':
                        desc = result['description']
                        if desc and len(desc.strip()) > 0:
                            results['descriptions'][game.id] = desc
                            self.stats['found'] += 1

                            if len(desc.strip()) < 10:
                                self.stats['short'] += 1
                                if self.debug:
                                    self.stdout.write(f'   📏 Короткое: {game.name}')
                        else:
                            self.stats['empty'] += 1
                            results['short'].append(game.id)

                    elif result['status'] == 'not_found':
                        # СОХРАНЯЕМ IGDB_ID ВМЕСТО Django ID
                        results['not_found'].append(game.igdb_id)
                        self.stats['not_found_count'] += 1

                    elif result['status'] == 'error':
                        results['errors'].append(game.id)
                        self.stats['errors'] += 1

                    # Обновляем статистику на основе источника
                    if 'source' in result:
                        if result['source'] == 'cache':
                            self.stats['cache_hits'] += 1
                            self.update_stats('cache_hit')
                        elif result['source'] == 'search':
                            self.stats['search_requests'] += 1
                            self.update_stats('search_request')
                        elif result['source'] == 'details':
                            self.stats['detail_requests'] += 1
                            self.update_stats('detail_request')
                        elif result['source'] == 'short':
                            self.stats['short'] += 1
                        elif result['source'] == 'empty':
                            self.stats['empty'] += 1
                        elif result['source'] == 'rate_limited':
                            self.stats['rate_limited'] += 1
                            self.update_stats('rate_limited')

                    # Обновляем статистику кэша
                    if result.get('source') != 'cache':
                        self.stats['cache_misses'] += 1
                        self.update_stats('cache_miss')

                    # Показываем прогресс с информацией о кэше
                    if completed % max(1, len(games) // 10) == 0 or completed == len(games):
                        progress = (completed / len(games)) * 100
                        elapsed = time.time() - self.stats['start']
                        games_per_sec = completed / elapsed if elapsed > 0 else 0

                        cache_hit_rate = (self.stats['cache_hits'] / completed * 100) if completed > 0 else 0

                        bar_length = 20
                        filled = int(bar_length * progress / 100)
                        bar = "[" + "=" * filled + " " * (bar_length - filled) + "]"

                        self.stdout.write(
                            f'{bar} {progress:.0f}% | '
                            f'{completed}/{len(games)} | '
                            f'{games_per_sec:.1f} игр/сек | '
                            f'Кэш: {cache_hit_rate:.0f}%'
                        )

                except concurrent.futures.TimeoutError:
                    self.stats['errors'] += 1
                    results['errors'].append(game.id)
                    self.stdout.write(f'   ⏰ Таймаут: {game.name}')
                except Exception as e:
                    self.stats['errors'] += 1
                    results['errors'].append(game.id)
                    if self.debug:
                        self.stdout.write(f'   💥 Ошибка обработки: {game.name} - {str(e)[:50]}')

        return results

    def get_game_description_optimized(self, game):
        """Оптимизированное получение описания с использованием RAWG клиента"""
        try:
            # Используем RAWG клиент для получения описания
            result = self.rawg_client.get_game_description(
                game_name=game.name,
                min_length=self.min_length,
                delay=self.delay,
                use_cache=not self.skip_cache,
                cache_ttl=self.original_options.get('cache_ttl', 30)
            )

            return result

        except Exception as e:
            if self.debug:
                self.stdout.write(f'   💥 Ошибка получения описания для {game.name}: {str(e)[:50]}')
            return {'status': 'error', 'error': str(e)}

    def save_efficiency_stats(self, stats, repeat_num):
        """Сохранение статистики эффективности"""
        if not self.stats_db:
            return

        try:
            timestamp = datetime.now().isoformat()

            # Рассчитываем эффективность кэша
            total_processed = stats.get('total', 0)
            cache_hits = stats.get('cache_hits', 0)

            cache_efficiency = (cache_hits / total_processed * 100) if total_processed > 0 else 0

            # Рассчитываем сэкономленные запросы
            requests_without_cache = total_processed * 1.5
            requests_with_cache = (total_processed - cache_hits) * 1.5
            requests_saved = int(requests_without_cache - requests_with_cache)

            cursor = self.stats_db.cursor()
            cursor.execute('''
                           INSERT INTO efficiency_stats
                               (timestamp, cache_efficiency, requests_saved, avg_requests_per_game)
                           VALUES (?, ?, ?, ?)
                           ''', (
                               timestamp,
                               cache_efficiency,
                               requests_saved,
                               requests_with_cache / total_processed if total_processed > 0 else 0
                           ))

            self.stats_db.commit()

        except Exception as e:
            if self.debug:
                self.stdout.write(f'   ⚠️ Ошибка сохранения статистики эффективности: {e}')

    def show_api_statistics(self):
        """Показывает детальную статистику использования API"""
        if not self.rawg_client:
            self.stdout.write('\n⚠️  RAWG клиент не инициализирован')
            return

        # Получаем статистику из клиента
        client_stats = self.rawg_client.get_stats()

        self.stdout.write('\n' + '📈' * 15)
        self.stdout.write('📊 ДЕТАЛЬНАЯ СТАТИСТИКА API:')

        total_requests = client_stats['total_requests']
        cache_hits = client_stats['cache_hits']
        cache_misses = client_stats['cache_misses']
        total_cache_checks = cache_hits + cache_misses

        if total_cache_checks > 0:
            cache_efficiency = (cache_hits / total_cache_checks) * 100
        else:
            cache_efficiency = 0

        self.stdout.write(f'   🔍 Поисковых запросов: {client_stats["search_requests"]}')
        self.stdout.write(f'   📄 Запросов деталей: {client_stats["detail_requests"]}')
        self.stdout.write(f'   🎯 Всего запросов к API: {total_requests}')
        self.stdout.write(f'   💾 Попаданий в кэш: {cache_hits}')
        self.stdout.write(f'   ❌ Промахов кэша: {cache_misses}')
        self.stdout.write(f'   ⚡ Эффективность кэша: {cache_efficiency:.1f}%')
        self.stdout.write(f'   🚫 Rate limited: {client_stats["rate_limited"]}')

        if total_requests > 0:
            # Оцениваем сэкономленные запросы
            estimated_without_cache = total_cache_checks * 1.5
            requests_saved = estimated_without_cache - total_requests

            self.stdout.write(f'   💰 Сэкономлено запросов: ~{int(requests_saved)}')

            # Экономия в процентах
            if estimated_without_cache > 0:
                savings_percent = (requests_saved / estimated_without_cache) * 100
                self.stdout.write(f'   📉 Экономия: {savings_percent:.1f}%')

        # Статистика из БД если есть
        if self.stats_db:
            try:
                cursor = self.stats_db.cursor()

                # Общая статистика за все время
                cursor.execute('''
                               SELECT SUM(total_requests) as total_reqs,
                                      SUM(cache_hits)     as total_hits,
                                      SUM(cache_misses)   as total_misses
                               FROM request_stats
                               ''')
                result = cursor.fetchone()

                if result and result[0]:
                    total_all_time = result[0] or 0
                    hits_all_time = result[1] or 0
                    misses_all_time = result[2] or 0

                    total_checks = hits_all_time + misses_all_time
                    if total_checks > 0:
                        overall_efficiency = (hits_all_time / total_checks) * 100
                        self.stdout.write(f'\n   🕰️  За все время:')
                        self.stdout.write(f'   📊 Всего проверок: {total_checks}')
                        self.stdout.write(f'   💾 Общая эффективность: {overall_efficiency:.1f}%')

            except Exception as e:
                if self.debug:
                    self.stdout.write(f'   ⚠️ Ошибка получения статистики из БД: {e}')

    def get_games_to_process(self, game_ids_str=None, auto_offset=False):
        """Получает игры для обработки"""
        # Базовый запрос
        if self.overwrite:
            games_query = Game.objects.all()
        else:
            # Только игры без описания
            games_query = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            )

        # Фильтр по game_type (только указанные типы, если не включены все)
        if not self.include_all_gametypes:
            games_query = games_query.filter(
                game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
            )

        # Фильтр по ID (IGDB ID, не Django ID)
        if game_ids_str:
            try:
                game_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                # Если переданы IGDB ID (предположительно)
                games_query = games_query.filter(igdb_id__in=game_ids)
            except ValueError:
                self.stdout.write(self.style.ERROR('❌ Некорректный формат game-ids'))
                return []

        # Исключаем не найденные игры если auto-offset включен - ИСПОЛЬЗУЕМ IGDB_ID
        if auto_offset and self.not_found_ids:
            games_query = games_query.exclude(igdb_id__in=self.not_found_ids)
            self.stats['auto_offset_skipped'] = len(self.not_found_ids)

        # Сортировка и лимиты
        if self.order_by.startswith('-'):
            order_field = self.order_by[1:]
            games_query = games_query.order_by(f'-{order_field}')
        else:
            games_query = games_query.order_by(self.order_by)

        if self.offset > 0:
            games_query = games_query[self.offset:]

        if self.limit > 0:
            games_query = games_query[:self.limit]

        games = list(games_query)
        self.stats['total'] = len(games)

        return games

    def show_start_info(self, games, repeat_num, auto_offset=False):
        """Показывает стартовую информацию"""
        self.stdout.write(f'🚀 [{repeat_num}] Начинаем импорт {len(games)} игр')
        self.stdout.write(f'⚡ Оптимизации: КЭШИРОВАНИЕ {"ВЫКЛ" if self.skip_cache else "ВКЛ"}')
        if auto_offset:
            self.stdout.write(f'⚡ Auto-offset: пропущено {self.stats["auto_offset_skipped"]} игр')
        self.stdout.write(f'👷 Потоков: {self.workers}')
        self.stdout.write(f'⏱️  Задержка: {self.delay} сек')
        self.stdout.write(f'📏 Мин. длина: {self.min_length}')
        self.stdout.write(f'🔧 Режим: {"OVERWRITE" if self.overwrite else "SKIP EXISTING"}')
        self.stdout.write(f'🎮 Типы игр: {"ВСЕ" if self.include_all_gametypes else "0,1,2,4,5,8,9,10,11"}')
        self.stdout.write(f'🐛 Debug: {"ВКЛ" if self.debug else "ВЫКЛ"}')
        if self.offset > 0:
            self.stdout.write(f'📍 Offset: {self.offset}')
        self.stdout.write('─' * 50)

    def process_results(self, games, results):
        """Обрабатывает результаты"""
        self.stats['updated'] = len(results['descriptions'])

        if self.dry_run:
            self.stdout.write('\n📊 [DRY RUN] РЕЗУЛЬТАТЫ:')
            self.stdout.write(f'   ✅ Найдено описаний: {len(results["descriptions"])}')
            self.stdout.write(f'   📏 Коротких: {self.stats["short"]}')
            self.stdout.write(f'   🚫 Пустых: {self.stats["empty"]}')
            self.stdout.write(f'   ❓ Не найдено: {len(results["not_found"])}')
            self.stdout.write(f'   💥 Ошибок: {len(results["errors"])}')
            self.stdout.write(f'   ⚠️  Rate limited: {self.stats["rate_limited"]}')
            self.stdout.write(f'   💾 Попаданий в кэш: {self.stats["cache_hits"]}')
            self.stdout.write(f'   🔍 Поисковых запросов: {self.stats["search_requests"]}')
            self.stdout.write(f'   📄 Запросов деталей: {self.stats["detail_requests"]}')
        else:
            # Сохраняем в БД
            if results['descriptions']:
                self.stdout.write(f'\n💾 Сохраняем {len(results["descriptions"])} описаний...')
                start_save = time.time()
                self.bulk_save_descriptions(results['descriptions'])
                save_time = time.time() - start_save
                self.stdout.write(f'✅ Готово за {save_time:.1f} сек!')

    def bulk_save_descriptions(self, descriptions):
        """Массовое сохранение"""
        game_ids = list(descriptions.keys())
        desc_texts = list(descriptions.values())

        cases = []
        for game_id, desc in zip(game_ids, desc_texts):
            cases.append(When(id=game_id, then=Value(desc)))

        Game.objects.filter(id__in=game_ids).update(
            rawg_description=Case(
                *cases,
                default=Value(''),
                output_field=TextField()
            )
        )

    def show_final_stats(self, results):
        """Показывает финальную статистику"""
        total_time = time.time() - self.stats['start']

        self.stdout.write('\n' + '=' * 50)
        self.stdout.write(f'🏁 ИМПОРТ [{self.stats["repeat_num"]}] ЗАВЕРШЕН!')

        if self.stats['total'] > 0:
            games_per_sec = self.stats['total'] / total_time

            # Эффективность кэша
            cache_checks = self.stats['cache_hits'] + self.stats['cache_misses']
            cache_efficiency = (self.stats['cache_hits'] / cache_checks * 100) if cache_checks > 0 else 0

            # Экономия запросов
            estimated_no_cache = cache_checks * 1.5
            actual_requests = self.stats['search_requests'] + self.stats['detail_requests']
            requests_saved = estimated_no_cache - actual_requests

            self.stdout.write(f'⏱️  Время: {total_time:.1f} сек')
            self.stdout.write(f'⚡ Скорость: {games_per_sec:.1f} игр/сек')
            self.stdout.write(f'🎯 Эффективность кэша: {cache_efficiency:.1f}%')
            self.stdout.write(f'💰 Сэкономлено запросов: ~{int(requests_saved)}')

        self.stdout.write(f'📊 Всего игр: {self.stats["total"]}')
        self.stdout.write(f'✅ Найдено: {self.stats["found"]}')
        self.stdout.write(f'📏 Коротких: {self.stats["short"]}')
        self.stdout.write(f'🚫 Пустых: {self.stats["empty"]}')
        self.stdout.write(f'❓ Не найдено (IGDB_ID): {self.stats["not_found_count"]}')
        self.stdout.write(f'💥 Ошибок: {self.stats["errors"]}')
        self.stdout.write(f'💾 Кэш попаданий: {self.stats["cache_hits"]}')
        self.stdout.write(f'🔍 Запросов поиска: {self.stats["search_requests"]}')
        self.stdout.write(f'📄 Запросов деталей: {self.stats["detail_requests"]}')

        # Добавляем информацию о пропущенных играх через auto-offset
        if self.stats.get('auto_offset_skipped', 0) > 0:
            self.stdout.write(f'⏩ Пропущено через auto-offset: {self.stats["auto_offset_skipped"]}')

        if self.dry_run:
            self.stdout.write('\n⚠️  DRY RUN: данные НЕ сохранены')

    def load_not_found_ids(self, filename):
        """Загружает список не найденных игр (IGDB_ID)"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    # Загружаем IGDB_ID из файла
                    self.not_found_ids = set(data.get('not_found_ids', []))
        except Exception:
            self.not_found_ids = set()

    def save_not_found_ids(self, filename):
        """Сохраняет список не найденных игр (IGDB_ID)"""
        try:
            data = {
                'not_found_ids': list(self.not_found_ids),  # Сохраняем IGDB_ID
                'last_updated': datetime.now().isoformat(),
                'count': len(self.not_found_ids),
                'note': 'Список содержит IGDB_ID игр, которые не были найдены в RAWG API'
            }
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            if self.debug:
                self.stdout.write(f'   ⚠️ Ошибка сохранения not_found_ids: {e}')

    def save_iteration_logs(self, results, games, repeat_num):
        """Сохраняет логи итерации"""
        import os
        os.makedirs(self.log_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Получаем статистику от RAWG клиента
        client_stats = self.rawg_client.get_stats() if self.rawg_client else {}

        # Сохраняем статистику с информацией о кэше
        stats_data = {
            'meta': {
                'timestamp': datetime.now().isoformat(),
                'repeat': repeat_num,
                'cache_enabled': not self.skip_cache,
                'cache_ttl': self.original_options.get('cache_ttl', 30),
                'delay': self.delay,
                'workers': self.workers,
                'client_initialized': self.rawg_client is not None,
                'include_all_gametypes': self.include_all_gametypes,
                'auto_offset_enabled': self.original_options.get('auto_offset', True),
                'auto_offset_skipped': self.stats.get('auto_offset_skipped', 0)
            },
            'stats': self.stats.copy(),
            'rawg_client_stats': client_stats,
            'game_count': len(games),
            'results_summary': {
                'descriptions_found': len(results['descriptions']),
                'not_found': len(results['not_found']),
                'errors': len(results['errors']),
                'short': len(results['short'])
            },
            'auto_offset_data': {
                'not_found_ids_count': len(self.not_found_ids),
                'not_found_ids_sample': list(self.not_found_ids)[:10] if self.not_found_ids else [],
                'note': 'not_found_ids содержит IGDB_ID игр'
            }
        }

        stats_file = os.path.join(self.log_dir, f'stats_repeat{repeat_num}_{timestamp}.json')
        try:
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            if self.debug:
                self.stdout.write(f'   ⚠️ Ошибка сохранения логов: {e}')

    def show_rawg_stats(self):
        """Показывает статистику по играм"""
        try:
            total_games = Game.objects.count()
            games_with_rawg = Game.objects.filter(
                ~Q(rawg_description__isnull=True) &
                ~Q(rawg_description__exact='')
            ).count()

            # Статистика по типам игр
            games_filtered = Game.objects.filter(
                game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
            ).count()
            games_filtered_with_rawg = Game.objects.filter(
                Q(game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]) &
                ~Q(rawg_description__isnull=True) &
                ~Q(rawg_description__exact='')
            ).count()

            # Проценты
            total_percentage = (games_with_rawg / total_games * 100) if total_games > 0 else 0
            filtered_percentage = (games_filtered_with_rawg / games_filtered * 100) if games_filtered > 0 else 0

            self.stdout.write('\n' + '📊' * 15)
            self.stdout.write('📈 СТАТИСТИКА БАЗЫ ДАННЫХ:')
            self.stdout.write(f'   Всего игр в БД: {total_games:,}')
            self.stdout.write(f'   ✅ С RAWG описанием: {games_with_rawg:,} ({total_percentage:.1f}%)')

            self.stdout.write(f'\n   🎮 Игр с типами 0,1,2,4,5,8,9,10,11: {games_filtered:,}')
            self.stdout.write(f'   ✅ С RAWG описанием: {games_filtered_with_rawg:,} ({filtered_percentage:.1f}%)')

            # Игры без описания (те, которые будут обработаны)
            games_without_rawg = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            ).count()
            games_filtered_without_rawg = Game.objects.filter(
                Q(game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]) &
                (Q(rawg_description__isnull=True) | Q(rawg_description__exact=''))
            ).count()

            self.stdout.write(f'\n   ⏳ Без RAWG описания: {games_without_rawg:,}')
            self.stdout.write(f'   ⏳ Из них с типами 0,1,2,4,5,8,9,10,11: {games_filtered_without_rawg:,}')

            # Показываем прогресс-бар для основных типов игр
            if games_filtered > 0:
                bar_length = 30
                filled = int(bar_length * filtered_percentage / 100)
                bar = "[" + "█" * filled + "░" * (bar_length - filled) + "]"
                self.stdout.write(f'\n   {bar} {filtered_percentage:.1f}% заполнено (основные типы)')

                # Оценка времени до полного заполнения
                if self.stats.get('total', 0) > 0 and self.stats.get('start', 0) > 0:
                    processed_time = time.time() - self.stats.get('start', time.time())
                    if processed_time > 0 and self.stats['total'] > 0:
                        games_per_second = self.stats['total'] / processed_time
                        if games_per_second > 0:
                            remaining_games = games_filtered_without_rawg
                            estimated_hours = remaining_games / games_per_second / 3600
                            estimated_days = estimated_hours / 24

                            if estimated_days > 1:
                                self.stdout.write(f'   ⏱️  Примерное время до завершения: ~{estimated_days:.1f} дней')
                            elif estimated_hours > 1:
                                self.stdout.write(f'   ⏱️  Примерное время до завершения: ~{estimated_hours:.1f} часов')
                            else:
                                self.stdout.write(
                                    f'   ⏱️  Примерное время до завершения: ~{(estimated_hours * 60):.0f} минут')

        except Exception as e:
            if self.debug:
                self.stdout.write(f'   ⚠️ Ошибка получения статистики БД: {e}')

    def show_global_stats(self, global_stats):
        """Показывает глобальную статистику"""
        total_time = time.time() - global_stats['start_time']

        self.stdout.write('\n' + '🎉' * 20)
        self.stdout.write(self.style.SUCCESS('🏆 ВСЕ ПОВТОРЕНИЯ ЗАВЕРШЕНЫ!'))
        self.stdout.write('=' * 50)

        self.stdout.write(f'📊 ГЛОБАЛЬНАЯ СТАТИСТИКА:')
        self.stdout.write(f'   🔁 Выполнено повторов: {global_stats["completed_repeats"]}')
        self.stdout.write(f'   ⏱️  Общее время: {total_time:.1f} сек')
        self.stdout.write(f'   📈 Обработано игр: {global_stats["total_games_processed"]:,}')
        self.stdout.write(f'   ✅ Обновлено описаний: {global_stats["total_games_updated"]:,}')
        self.stdout.write(f'   💾 Попаданий в кэш: {global_stats["cache_hits"]:,}')
        self.stdout.write(f'   💥 Всего ошибок: {global_stats["total_errors"]}')

        if global_stats['total_games_processed'] > 0:
            overall_speed = global_stats['total_games_processed'] / total_time
            cache_efficiency = (global_stats['cache_hits'] / global_stats['total_games_processed'] * 100)

            self.stdout.write(f'\n📈 ЭФФЕКТИВНОСТЬ:')
            self.stdout.write(f'   ⚡ Общая скорость: {overall_speed:.1f} игр/сек')
            self.stdout.write(f'   💾 Общая эффективность кэша: {cache_efficiency:.1f}%')

            # Оценка экономии
            estimated_no_cache = global_stats['total_games_processed'] * 1.5
            requests_with_cache = (global_stats['total_games_processed'] - global_stats['cache_hits']) * 1.5
            requests_saved = estimated_no_cache - requests_with_cache

            self.stdout.write(f'   💰 Сэкономлено запросов API: ~{int(requests_saved):,}')

        # Показываем информацию о клиенте
        if self.rawg_client:
            client_stats = self.rawg_client.get_stats()
            self.stdout.write(f'\n🔧 RAWG КЛИЕНТ:')
            self.stdout.write(f'   🎯 Запросов к API: {client_stats["total_requests"]}')
            self.stdout.write(f'   🚫 Rate limited: {client_stats["rate_limited"]}')