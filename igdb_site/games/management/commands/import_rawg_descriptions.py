# games/management/commands/import_rawg_descriptions.py
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
import signal
import sys

from django.core.management.base import BaseCommand
from django.db.models import Q, Case, When, Value, TextField
from games.models import Game
from games.rawg_api import RAWGClient


class Command(BaseCommand):
    help = 'Импорт описаний из RAWG API с кэшированием и оптимизацией'

    def __init__(self):
        super().__init__()
        self.rawg_client = None
        self.stats_db = None
        self.api_stats = defaultdict(int)
        self.not_found_ids = set()
        self.shutdown_flag = False
        self.balance_exceeded = False  # Флаг исчерпания баланса
        self.init_stats_db()

        # Регистрируем обработчик сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Обработчик сигналов для graceful shutdown"""
        self.shutdown_flag = True
        self.stdout.write("\n\n⚠️  Получен сигнал прерывания (Ctrl+C)...")
        self.stdout.write("🔄 Завершаю работу корректно...")

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Только проверка без сохранения'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
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
            default=0.5,
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
            default=0,
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
            default=50,
            help='Размер батча за один повтор (по умолчанию: 50)'
        )
        parser.add_argument(
            '--auto-offset',
            action='store_true',
            default=True,
            help='Автоматически пропускать не найденные игры'
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
        parser.add_argument(
            '--save-on-interrupt',
            action='store_true',
            default=True,
            help='Сохранять прогресс при прерывании'
        )
        parser.add_argument(
            '--no-save-on-interrupt',
            action='store_false',
            dest='save_on_interrupt',
            help='Выключить сохранение прогресса при прерывании'
        )

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

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS interruption_logs
                           (
                               timestamp
                               TEXT
                               PRIMARY
                               KEY,
                               repeat_num
                               INTEGER,
                               games_processed
                               INTEGER,
                               games_saved
                               INTEGER,
                               reason
                               TEXT,
                               interrupted_at
                               TEXT
                           )
                           ''')

            self.stats_db.commit()

        except Exception as e:
            self.stdout.write(f'⚠️ Ошибка инициализации статистики: {e}')
            self.stats_db = None

    def handle(self, *args, **options):
        """Основной обработчик команды с обработкой KeyboardInterrupt"""
        try:
            self._handle_with_interrupt(*args, **options)
        except KeyboardInterrupt:
            self.handle_keyboard_interrupt(options)
        except SystemExit:
            raise
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Необработанная ошибка: {e}'))
            import traceback
            traceback.print_exc()
            sys.exit(1)

    def _handle_with_interrupt(self, *args, **options):
        """Основной обработчик команды (внутренний метод)"""
        if options.get('reset'):
            self.stdout.write('🧹 Удаление кэша RAWG API...')
            self.reset_cache()

            if len([k for k, v in options.items()
                    if v and k not in ['reset', 'verbosity']]) == 1:
                return

        if not self.init_rawg_client(options):
            return

        self.original_options = options.copy()
        self.save_on_interrupt = options.get('save_on_interrupt', True)
        self.current_results = None
        self.current_games = None
        self.current_repeat_num = 1
        self.balance_exceeded = False  # Сброс флага при новом запуске

        try:
            self.run_main_import_process(options)
        except KeyboardInterrupt:
            self.log_interruption('KeyboardInterrupt in main process')
            raise

    def handle_keyboard_interrupt(self, options):
        """Обработка KeyboardInterrupt"""
        self.stdout.write("\n\n" + "⚠️" * 20)
        self.stdout.write(self.style.WARNING("🚨 ПРЕРЫВАНИЕ ВЫПОЛНЕНИЯ КОМАНДЫ"))
        self.stdout.write("=" * 50)

        # Показываем текущую статистику
        if hasattr(self, 'stats'):
            self.stdout.write(f"📊 Текущая статистика:")
            self.stdout.write(f"   Обработано игр: {self.stats.get('total', 0)}")
            self.stdout.write(f"   Найдено описаний: {self.stats.get('found', 0)}")
            self.stdout.write(f"   Сохранено описаний: {self.stats.get('updated', 0)}")
            self.stdout.write(f"   Ошибок: {self.stats.get('errors', 0)}")

        # Сохраняем прогресс если нужно
        if self.save_on_interrupt and self.current_results:
            self.stdout.write("\n💾 Сохраняю найденные описания перед выходом...")
            try:
                self.save_descriptions_to_db(self.current_results)
                self.stdout.write(
                    self.style.SUCCESS(f"✅ Сохранено {len(self.current_results['descriptions'])} описаний"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Ошибка сохранения: {e}"))

        # Сохраняем список ненайденных игр
        if hasattr(self, 'not_found_ids') and self.not_found_ids:
            self.stdout.write("\n💾 Сохраняю список ненайденных игр...")
            try:
                self.save_not_found_ids(options.get('auto_offset_file', 'auto_offset_log.json'))
                self.stdout.write(self.style.SUCCESS(f"✅ Сохранено {len(self.not_found_ids)} ненайденных игр"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Ошибка сохранения: {e}"))

        # Показываем итоговую статистику
        self.show_interrupt_stats(options)

        sys.exit(130)

    def log_interruption(self, reason):
        """Логирование прерывания"""
        if not self.stats_db:
            return

        try:
            cursor = self.stats_db.cursor()
            timestamp = datetime.now().isoformat()

            cursor.execute('''
                           INSERT INTO interruption_logs
                           (timestamp, repeat_num, games_processed, games_saved, reason, interrupted_at)
                           VALUES (?, ?, ?, ?, ?, ?)
                           ''', (
                               timestamp,
                               self.current_repeat_num if hasattr(self, 'current_repeat_num') else 0,
                               self.stats.get('total', 0) if hasattr(self, 'stats') else 0,
                               self.stats.get('updated', 0) if hasattr(self, 'stats') else 0,
                               reason,
                               datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                           ))

            self.stats_db.commit()
        except Exception as e:
            if self.debug:
                self.stdout.write(f'   ⚠️ Ошибка логирования прерывания: {e}')

    def show_interrupt_stats(self, options):
        """Показывает статистику при прерывании"""
        self.stdout.write("\n" + "📊" * 15)
        self.stdout.write("📈 СТАТИСТИКА ПРИ ПРЕРЫВАНИИ:")

        if hasattr(self, 'stats') and self.stats.get('start'):
            total_time = time.time() - self.stats['start']
            self.stdout.write(f"⏱️  Время работы: {total_time:.1f} сек")

            if self.stats.get('total', 0) > 0:
                games_per_sec = self.stats['total'] / total_time
                self.stdout.write(f"⚡ Скорость: {games_per_sec:.1f} игр/сек")

        # Показываем статистику БД
        self.show_rawg_stats()

        # Показываем рекомендации
        self.stdout.write("\n💡 РЕКОМЕНДАЦИИ:")
        if not options.get('save_on_interrupt', True):
            self.stdout.write(
                "   • Используйте --save-on-interrupt чтобы сохранять прогресс при прерывании")
        self.stdout.write("   • Используйте --auto-offset-file для сохранения списка ненайденных игр")

        self.stdout.write("\n🔄 Для продолжения используйте:")
        cmd = "python manage.py import_rawg_descriptions"
        if options.get('auto_offset', True):
            cmd += f" --auto-offset --auto-offset-file {options.get('auto_offset_file', 'auto_offset_log.json')}"
        if not options.get('save_on_interrupt', True):
            cmd += " --no-save-on-interrupt"
        self.stdout.write(f"   {cmd}")

    def run_main_import_process(self, options):
        """Основной процесс импорта"""
        repeat_times = options['repeat']
        repeat_delay = options['repeat_delay']
        auto_offset = options.get('auto_offset', True)
        batch_size = options.get('batch_size', 50)
        infinite_mode = options['limit'] == 0 and repeat_times == 0

        if auto_offset:
            self.load_not_found_ids(options['auto_offset_file'])

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

        self.show_cache_info(options)

        try:
            if infinite_mode:
                self.execute_infinite_repeats(global_stats, options, repeat_delay, auto_offset, batch_size)
            else:
                self.execute_limited_repeats(global_stats, options, repeat_times, repeat_delay, auto_offset)
        except KeyboardInterrupt:
            self.log_interruption('KeyboardInterrupt during import process')
            raise

        self.show_final_global_stats(global_stats)

    def execute_infinite_repeats(self, global_stats, options, repeat_delay, auto_offset, batch_size):
        """Выполняет бесконечные повторения до обработки всех игр"""
        offset = options.get('offset', 0)
        repeat_num = 1
        total_processed_games = 0

        total_to_process = self.get_total_games_to_process(options, auto_offset)

        if total_to_process == 0:
            self.stdout.write('🎉 Нет игр для обработки!')
            return

        self.stdout.write(f'🎯 Всего игр для обработки: {total_to_process:,}')
        self.stdout.write(f'📦 Батч: {batch_size} игр за повтор')
        save_on_interrupt = options.get('save_on_interrupt', True)
        if save_on_interrupt:
            self.stdout.write('💾 Сохранение при прерывании: ВКЛ')
        else:
            self.stdout.write('💾 Сохранение при прерывании: ВЫКЛ')

        while True:
            # Проверяем флаг прерывания
            if self.shutdown_flag or self.balance_exceeded:
                if self.balance_exceeded:
                    self.stdout.write("\n🚫 Превышен лимит API - остановка")
                break

            self.stdout.write(f'\n{"=" * 50}')
            self.stdout.write(f'🚀 ПОВТОРЕНИЕ {repeat_num} (бесконечный режим)')
            self.stdout.write(f'📦 Батч: {batch_size} игр, offset: {offset}')

            current_options = options.copy()
            current_options['limit'] = batch_size
            current_options['offset'] = offset

            try:
                repeat_stats = self.run_single_import(repeat_num, auto_offset, current_options)
            except KeyboardInterrupt:
                self.log_interruption(f'KeyboardInterrupt in repeat {repeat_num}')
                if save_on_interrupt and hasattr(self, 'current_results'):
                    self.save_descriptions_to_db(self.current_results)
                raise

            self.update_global_stats(global_stats, repeat_stats)

            games_processed_in_batch = repeat_stats.get('total', 0)
            total_processed_games += games_processed_in_batch
            offset += games_processed_in_batch

            if auto_offset and repeat_stats.get('new_not_found', 0) > 0:
                self.save_not_found_ids(options['auto_offset_file'])

            self.save_efficiency_stats(repeat_stats, repeat_num)

            # Показываем прогресс
            progress = (total_processed_games / total_to_process * 100) if total_to_process > 0 else 0
            self.stdout.write(f'📊 ПРОГРЕСС: {total_processed_games:,}/{total_to_process:,} игр ({progress:.1f}%)')

            if games_processed_in_batch == 0 or self.balance_exceeded:
                if self.balance_exceeded:
                    self.stdout.write('\n🚫 Остановка: лимит API исчерпан')
                else:
                    self.stdout.write('\n🎉 Все игры обработаны!')
                break

            if total_processed_games >= total_to_process:
                self.stdout.write('\n🎉 Все запланированные игры обработаны!')
                break

            # Проверяем флаг прерывания перед паузой
            if self.shutdown_flag or self.balance_exceeded:
                break

            self.stdout.write(f'\n⏳ Пауза {repeat_delay} секунд...')

            # Пауза с проверкой флага прерывания
            start_sleep = time.time()
            while time.time() - start_sleep < repeat_delay:
                if self.shutdown_flag or self.balance_exceeded:
                    break
                time.sleep(0.1)

            if self.shutdown_flag or self.balance_exceeded:
                break

            repeat_num += 1

    def execute_limited_repeats(self, global_stats, options, repeat_times, repeat_delay, auto_offset):
        """Выполняет ограниченное количество повторений"""
        save_on_interrupt = options.get('save_on_interrupt', True)
        if save_on_interrupt:
            self.stdout.write('💾 Сохранение при прерывании: ВКЛ')
        else:
            self.stdout.write('💾 Сохранение при прерывании: ВЫКЛ')

        for repeat_num in range(1, repeat_times + 1):
            # Проверяем флаг прерывания
            if self.shutdown_flag or self.balance_exceeded:
                if self.balance_exceeded:
                    self.stdout.write("\n🚫 Превышен лимит API - остановка")
                break

            self.stdout.write(f'\n{"=" * 50}')
            self.stdout.write(f'🚀 ПОВТОРЕНИЕ {repeat_num}/{repeat_times}')

            try:
                repeat_stats = self.run_single_import(repeat_num, auto_offset, options)
            except KeyboardInterrupt:
                self.log_interruption(f'KeyboardInterrupt in repeat {repeat_num}')
                if save_on_interrupt and hasattr(self, 'current_results'):
                    self.save_descriptions_to_db(self.current_results)
                raise

            self.update_global_stats(global_stats, repeat_stats)

            if auto_offset and repeat_stats.get('new_not_found', 0) > 0:
                self.save_not_found_ids(options['auto_offset_file'])

            self.save_efficiency_stats(repeat_stats, repeat_num)

            if repeat_num < repeat_times:
                # Проверяем флаг прерывания перед паузой
                if self.shutdown_flag or self.balance_exceeded:
                    break

                self.stdout.write(f'\n⏳ Пауза {repeat_delay} секунд...')

                # Пауза с проверкой флага
                start_sleep = time.time()
                while time.time() - start_sleep < repeat_delay:
                    if self.shutdown_flag or self.balance_exceeded:
                        break
                    time.sleep(0.1)

                if self.shutdown_flag or self.balance_exceeded:
                    break

    def run_single_import(self, repeat_num, auto_offset=False, options=None):
        """Запускает один импорт с обработкой прерывания"""
        if options is None:
            options = self.original_options.copy()

        self.init_import_settings(options)
        auto_offset = options.get('auto_offset', True)
        self.current_repeat_num = repeat_num
        self.balance_exceeded = False  # Сброс флага для каждого повторения

        save_on_interrupt = options.get('save_on_interrupt', True)
        if save_on_interrupt:
            self.stdout.write('💾 Сохранение при прерывании: ВКЛ')
        else:
            self.stdout.write('💾 Сохранение при прерывании: ВЫКЛ')

        if auto_offset:
            self.load_not_found_games_from_rawg_cache()
            self.create_fresh_not_found_file(options, repeat_num)

        self.init_stats(repeat_num)
        games = self.get_games_to_process(options.get('game_ids'), auto_offset)

        if not games:
            self.stdout.write('ℹ️ Нет игр для обработки')
            return {
                'total': 0,
                'updated': 0,
                'errors': 0,
                'new_not_found': 0,
                'cache_hits': 0,
                'cache_misses': 0,
                'found': 0,
                'short': 0,
                'empty': 0,
                'not_found_count': 0,
                'total_processed': 0
            }

        self.stats['total'] = len(games)
        self.current_games = games

        self.show_start_info(games, repeat_num, auto_offset)

        try:
            results = self.process_games_optimized(games)
        except KeyboardInterrupt:
            self.log_interruption(f'KeyboardInterrupt during processing in repeat {repeat_num}')
            if save_on_interrupt and hasattr(self, 'current_results'):
                results = self.current_results
            else:
                results = {
                    'descriptions': {},
                    'not_found': [],
                    'errors': [],
                    'short': []
                }
            raise

        self.update_not_found_list(results, auto_offset)

        if auto_offset:
            self.save_not_found_ids(options['auto_offset_file'])

        self.process_import_results(games, results)
        self.show_import_final_stats(results)
        self.save_import_logs(results, games, repeat_num)

        games_processed_in_batch = len(games)

        return {
            'total': games_processed_in_batch,
            'updated': self.stats['updated'],
            'errors': self.stats['errors'],
            'new_not_found': self.stats['new_not_found'],
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'found': self.stats['found'],
            'short': self.stats['short'],
            'empty': self.stats['empty'],
            'not_found_count': self.stats['not_found_count'],
            'total_processed': games_processed_in_batch,
            'balance_exceeded': self.balance_exceeded
        }

    def process_games_optimized(self, games):
        """Оптимизированная обработка игр с кэшированием и проверкой прерывания"""
        results = {
            'descriptions': {},
            'not_found': [],
            'errors': [],
            'short': [],
            'balance_exceeded': False
        }

        self.current_results = results

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.workers) as executor:
            future_to_game = {}
            for game in games:
                # Проверяем флаги прерывания
                if self.shutdown_flag or self.balance_exceeded:
                    if self.balance_exceeded:
                        self.stdout.write("\n🚫 Превышен лимит API - остановка обработки")
                    else:
                        self.stdout.write("\n⚠️  Прерывание обработки...")
                    break

                future = executor.submit(
                    self.get_game_description_optimized,
                    game
                )
                future_to_game[future] = game

            completed = 0
            total_games = len(future_to_game)

            for future in concurrent.futures.as_completed(future_to_game):
                # Проверяем флаги прерывания
                if self.shutdown_flag or self.balance_exceeded:
                    self.stdout.write("\n⚠️  Завершаю обработку...")
                    for f in future_to_game:
                        if not f.done():
                            f.cancel()
                    break

                game = future_to_game[future]
                completed += 1

                try:
                    result = future.result(timeout=30)

                    # Проверяем, не исчерпан ли баланс
                    if result.get('status') == 'balance_exceeded':
                        self.balance_exceeded = True
                        self.stats['errors'] += 1
                        results['errors'].append(game.id)
                        results['balance_exceeded'] = True
                        self.stdout.write(self.style.ERROR(f"🚫 ЛИМИТ API ИСЧЕРПАН! Прекращаю обработку."))
                        break

                    self.process_single_game_result(game, result, results)

                    if completed % max(1, total_games // 10) == 0 or completed == total_games:
                        self.show_progress(completed, total_games)

                except concurrent.futures.TimeoutError:
                    self.handle_processing_error(game, results, '⏰ Таймаут')
                except Exception as e:
                    self.handle_processing_error(game, results, f'💥 Ошибка обработки: {str(e)[:50]}')

        self.current_results = results
        return results

    def get_game_description_optimized(self, game):
        """Оптимизированное получение описания с использованием RAWG клиента"""
        try:
            return self.rawg_client.get_game_description(
                game_name=game.name,
                min_length=self.min_length,
                delay=self.delay,
                use_cache=not self.skip_cache,
                cache_ttl=self.original_options.get('cache_ttl', 30)
            )
        except ValueError as e:
            # Проверяем, не ошибка ли 429 (баланс исчерпан)
            if "лимит исчерпан" in str(e) or "429" in str(e):
                self.balance_exceeded = True
                self.stdout.write(self.style.ERROR(f"🚫 ЛИМИТ API ИСЧЕРПАН: {e}"))
                return {'status': 'balance_exceeded', 'error': str(e)}
            else:
                if self.debug:
                    self.stdout.write(f'   💥 Ошибка получения описания для {game.name}: {str(e)[:50]}')
                return {'status': 'error', 'error': str(e)}
        except Exception as e:
            if self.debug:
                self.stdout.write(f'   💥 Ошибка получения описания для {game.name}: {str(e)[:50]}')
            return {'status': 'error', 'error': str(e)}

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
                    updates = {
                        'cache_hit': ('cache_hits', 'cache_hits + ?'),
                        'cache_miss': ('cache_misses', 'cache_misses + ?'),
                        'search_request': ('search_requests', 'search_requests + ?'),
                        'detail_request': ('detail_requests', 'detail_requests + ?'),
                        'rate_limited': ('rate_limited', 'rate_limited + ?')
                    }

                    if stat_type in updates:
                        column, increment = updates[stat_type]
                        cursor.execute(f'''
                            UPDATE request_stats 
                            SET {column} = {increment}
                            WHERE date = ?
                        ''', (value, today))

                    # Всегда обновляем total_requests
                    cursor.execute('''
                                   UPDATE request_stats
                                   SET total_requests = total_requests + ?
                                   WHERE date = ?
                                   ''', (value, today))
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
                                   (date, cache_hits, cache_misses, search_requests,
                                    detail_requests, rate_limited, total_requests)
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

    def init_rawg_client(self, options):
        """Инициализация клиента RAWG"""
        try:
            self.rawg_client = RAWGClient(
                api_key=options.get('api_key')
            )

            # Проверяем баланс один раз при инициализации
            balance_info = self.rawg_client.check_balance(force=True)

            if self.rawg_client.balance_exceeded:
                self.stdout.write(self.style.ERROR(f'❌ Проблема с API ключом: {self.rawg_client.balance_error}'))
                return False
            else:
                self.stdout.write(self.style.SUCCESS('✅ RAWG клиент инициализирован'))
                return True

        except ValueError as e:
            self.stdout.write(self.style.ERROR(f'❌ {e}'))
            self.stdout.write(self.style.WARNING(
                '💡 Укажите ключ через --api-key или добавьте RAWG_API_KEY в .env'
            ))
            return False
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка инициализации RAWG клиента: {e}'))
            return False

    def show_cache_info(self, options):
        """Показывает информацию о кэше"""
        if not options['skip_cache']:
            self.stdout.write('💾 Кэширование: ВКЛЮЧЕНО')
        else:
            self.stdout.write('💾 Кэширование: ВЫКЛЮЧЕНО')

        auto_offset = options.get('auto_offset', True)
        if auto_offset:
            self.stdout.write(f'⚡ Auto-offset: пропуск {len(self.not_found_ids)} не найденных игр')

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

            estimated_no_cache = global_stats['total_games_processed'] * 1.5
            requests_with_cache = (global_stats['total_games_processed'] - global_stats['cache_hits']) * 1.5
            requests_saved = estimated_no_cache - requests_with_cache

            self.stdout.write(f'   💰 Сэкономлено запросов API: ~{int(requests_saved):,}')

        if self.rawg_client:
            client_stats = self.rawg_client.get_stats()
            self.stdout.write(f'\n🔧 RAWG КЛИЕНТ:')
            self.stdout.write(f'   🎯 Запросов к API: {client_stats["total_requests"]}')
            self.stdout.write(f'   🚫 Rate limited: {client_stats["rate_limited"]}')

        self.stdout.write('\n' + '🏁' * 15)
        self.stdout.write('📊 ФИНАЛЬНАЯ СТАТИСТИКА БАЗЫ ДАННЫХ:')
        self.show_rawg_stats()

        self.show_api_statistics()

    def create_fresh_not_found_file(self, options, repeat_num):
        """Создает новый файл ненайденных игр"""
        try:
            auto_offset_file = options.get('auto_offset_file', 'auto_offset_log.json')

            data = {
                'meta': {
                    'created_at': datetime.now().isoformat(),
                    'iteration': repeat_num,
                    'source': 'rawg_cache',
                    'cache_entries_count': len(self.not_found_ids),
                    'note': 'Файл создан в начале итерации на основе кэша RAWG'
                },
                'not_found_games': [],
                'summary': {
                    'total_count': len(self.not_found_ids),
                    'loaded_from_cache': True,
                    'cache_timestamp': datetime.now().isoformat()
                }
            }

            if self.not_found_ids:
                not_found_details = []
                for igdb_id in list(self.not_found_ids)[:100]:
                    try:
                        game = Game.objects.filter(igdb_id=igdb_id).first()
                        if game:
                            not_found_details.append({
                                'igdb_id': igdb_id,
                                'name': game.name,
                                'game_type': game.game_type.igdb_id if game.game_type else None
                            })
                    except:
                        continue

                data['not_found_games'] = not_found_details
                data['summary']['games_with_details'] = len(not_found_details)

            with open(auto_offset_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.stdout.write(f'📝 Создан файл ненайденных игр: {auto_offset_file}')
            self.stdout.write(f'   📊 Игр из кэша: {len(self.not_found_ids)}')

        except Exception as e:
            self.stdout.write(f'⚠️ Ошибка создания файла: {e}')

    def load_not_found_games_from_rawg_cache(self):
        """Загружает ненайденные игры из кэша RAWG API"""
        try:
            if not self.rawg_client:
                self.stdout.write('⚠️ RAWG клиент не инициализирован')
                return

            cache_conn = self.rawg_client.get_cache_connection()
            if not cache_conn:
                self.stdout.write('⚠️ Не удалось получить соединение с кэшем RAWG')
                return

            cursor = cache_conn.cursor()
            cursor.execute('''
                           SELECT game_name, game_hash, updated_at
                           FROM rawg_cache
                           WHERE found = 0
                             AND updated_at
                               > datetime('now'
                               , '-30 days')
                           ORDER BY updated_at DESC
                           ''')

            not_found_records = cursor.fetchall()

            if not not_found_records:
                self.stdout.write('ℹ️ В кэше RAWG нет записей о ненайденных играх')
                self.not_found_ids = set()
                return

            self.stdout.write(f'📂 Загружаю ненайденные игры из кэша RAWG...')
            self.not_found_ids = set()
            games_found_in_db = 0

            for game_name, game_hash, updated_at in not_found_records:
                try:
                    game = Game.objects.filter(name__iexact=game_name).first()

                    if not game:
                        game = Game.objects.filter(name__icontains=game_name[:20]).first()

                    if game:
                        self.not_found_ids.add(game.igdb_id)
                        games_found_in_db += 1

                except Exception as e:
                    if self.debug:
                        self.stdout.write(f'   ⚠️ Ошибка поиска игры "{game_name}": {e}')
                    continue

            self.stdout.write(f'✅ Загружено {len(self.not_found_ids)} ненайденных игр из кэша RAWG')
            self.stdout.write(f'   📊 Найдено в БД: {games_found_in_db}')

            if self.not_found_ids:
                sample_ids = list(self.not_found_ids)[:3]
                self.stdout.write(f'   📋 Примеры из кэша:')
                for igdb_id in sample_ids:
                    game = Game.objects.filter(igdb_id=igdb_id).first()
                    if game:
                        self.stdout.write(f'      • {igdb_id}: {game.name}')

        except Exception as e:
            self.stdout.write(f'⚠️ Ошибка загрузки из кэша RAWG: {e}')
            self.not_found_ids = set()

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
            if new_not_found:
                self.not_found_ids.update(new_not_found)
                self.stats['new_not_found'] = len(new_not_found)
                self.show_new_not_found_info(new_not_found)

    def show_new_not_found_info(self, new_not_found):
        """Показывает информацию о новых ненайденных играх"""
        self.stdout.write(f'   🔍 Новых ненайденных игр: {len(new_not_found)}')

        if new_not_found:
            sample_size = min(3, len(new_not_found))
            sample_ids = list(new_not_found)[:sample_size]

            for igdb_id in sample_ids:
                try:
                    game = Game.objects.filter(igdb_id=igdb_id).first()
                    if game:
                        self.stdout.write(f'      • {igdb_id}: {game.name}')
                    else:
                        self.stdout.write(f'      • {igdb_id}: (не найдено в БД)')
                except:
                    self.stdout.write(f'      • {igdb_id}: (ошибка получения названия)')

    def process_import_results(self, games, results):
        """Обрабатывает результаты импорта"""
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

        if self.stats.get('new_not_found', 0) > 0:
            self.stdout.write(f'   🆕 Новых ненайденных: {self.stats["new_not_found"]}')

    def save_descriptions_to_db(self, results):
        """Сохраняет описания в базу данных"""
        if results['descriptions']:
            self.stdout.write(f'\n💾 Сохраняем {len(results['descriptions'])} описаний...')
            start_save = time.time()
            self.bulk_save_descriptions(results['descriptions'])
            save_time = time.time() - start_save
            self.stdout.write(f'✅ Готово за {save_time:.1f} сек!')

    def show_import_final_stats(self, results):
        """Показывает финальную статистику импорта"""
        total_time = time.time() - self.stats['start']

        self.stdout.write('\n' + '=' * 50)
        self.stdout.write(f'🏁 ИМПОРТ [{self.stats["repeat_num"]}] ЗАВЕРШЕН!')

        self.show_performance_metrics(total_time)
        self.show_processing_stats()
        self.show_cache_api_stats()

    def show_performance_metrics(self, total_time):
        """Показывает метрики производительности"""
        if self.stats['total'] > 0:
            games_per_sec = self.stats['total'] / total_time
            cache_checks = self.stats['cache_hits'] + self.stats['cache_misses']
            cache_efficiency = (self.stats['cache_hits'] / cache_checks * 100) if cache_checks > 0 else 0

            estimated_no_cache = cache_checks * 1.5
            actual_requests = self.stats['search_requests'] + self.stats['detail_requests']
            requests_saved = estimated_no_cache - actual_requests

            self.stdout.write(f'⏱️  Время: {total_time:.1f} сек')
            self.stdout.write(f'⚡ Скорость: {games_per_sec:.1f} игр/сек')
            self.stdout.write(f'🎯 Эффективность кэша: {cache_efficiency:.1f}%')
            self.stdout.write(f'💰 Сэкономлено запросов: ~{int(requests_saved)}')

    def show_processing_stats(self):
        """Показывает статистику обработки игр"""
        self.stdout.write(f'📊 Всего игр: {self.stats["total"]}')
        self.stdout.write(f'✅ Найдено: {self.stats["found"]}')
        self.stdout.write(f'📏 Коротких: {self.stats["short"]}')
        self.stdout.write(f'🚫 Пустых: {self.stats["empty"]}')
        self.stdout.write(f'❓ Не найдено (IGDB_ID): {self.stats["not_found_count"]}')
        self.stdout.write(f'💥 Ошибок: {self.stats["errors"]}')

        if self.stats['total'] > 0:
            success_rate = (self.stats['found'] / self.stats['total']) * 100
            self.stdout.write(f'📈 Успешность: {success_rate:.1f}%')

    def show_cache_api_stats(self):
        """Показывает статистику кэша и API"""
        self.stdout.write(f'💾 Кэш попаданий: {self.stats["cache_hits"]}')
        self.stdout.write(f'🔍 Запросов поиска: {self.stats["search_requests"]}')
        self.stdout.write(f'📄 Запросов деталей: {self.stats["detail_requests"]}')
        self.stdout.write(f'🚫 Rate limited: {self.stats["rate_limited"]}')

        if self.stats.get('auto_offset_skipped', 0) > 0:
            self.stdout.write(f'⏩ Пропущено через auto-offset: {self.stats["auto_offset_skipped"]}')

        if self.stats.get('new_not_found', 0) > 0:
            self.stdout.write(f'🆕 Новых ненайденных игр: {self.stats["new_not_found"]}')

        if self.dry_run:
            self.stdout.write('\n⚠️  DRY RUN: данные НЕ сохранены')

    def save_import_logs(self, results, games, repeat_num):
        """Сохраняет логи импорта"""
        os.makedirs(self.log_dir, exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        client_stats = self.rawg_client.get_stats() if self.rawg_client else {}
        not_found_details = self.get_not_found_details_for_logs()

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
                'not_found_games_sample': not_found_details,
                'note': 'not_found_ids содержит IGDB_ID игр с названиями'
            }
        }

        stats_file = os.path.join(self.log_dir, f'stats_repeat{repeat_num}_{timestamp}.json')
        try:
            with open(stats_file, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            if self.debug:
                self.stdout.write(f'   ⚠️ Ошибка сохранения логов: {e}')

    def get_not_found_details_for_logs(self, max_samples=20):
        """Получает детали ненайденных игр для логов"""
        not_found_details = []
        sample_ids = list(self.not_found_ids)[:max_samples] if self.not_found_ids else []

        for igdb_id in sample_ids:
            try:
                game = Game.objects.filter(igdb_id=igdb_id).first()
                if game:
                    not_found_details.append({
                        'igdb_id': igdb_id,
                        'name': game.name,
                        'game_type': game.game_type.igdb_id if game.game_type else None,
                        'game_type_name': game.game_type.name if game.game_type else None
                    })
                else:
                    not_found_details.append({
                        'igdb_id': igdb_id,
                        'name': 'Не найдено в БД',
                        'game_type': None,
                        'game_type_name': None
                    })
            except Exception:
                not_found_details.append({
                    'igdb_id': igdb_id,
                    'name': 'Ошибка получения',
                    'game_type': None,
                    'game_type_name': None
                })

        return not_found_details

    def process_single_game_result(self, game, result, results):
        """Обрабатывает результат обработки одной игры"""
        # Проверяем что result не None и является словарем
        if not result or not isinstance(result, dict):
            self.stats['errors'] += 1
            results['errors'].append(game.id)
            self.stdout.write(f'   💥 Некорректный результат для {game.name}')
            return

        if result.get('status') == 'found':
            desc = result.get('description', '')
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

        elif result.get('status') == 'not_found':
            results['not_found'].append(game.igdb_id)
            self.stats['not_found_count'] += 1

        elif result.get('status') == 'error':
            results['errors'].append(game.id)
            self.stats['errors'] += 1
            if self.debug:
                error_msg = result.get('error', 'Неизвестная ошибка')
                self.stdout.write(f'   💥 Ошибка: {game.name} - {error_msg[:50]}')

        elif result.get('status') == 'balance_exceeded':
            self.balance_exceeded = True
            self.stats['errors'] += 1
            results['errors'].append(game.id)
            if self.debug:
                error_msg = result.get('error', 'Баланс исчерпан')
                self.stdout.write(f'   🚫 Баланс исчерпан: {game.name} - {error_msg[:50]}')

        source = result.get('source')
        if source:
            source_handlers = {
                'cache': ('cache_hits', 'cache_hit'),
                'search': ('search_requests', 'search_request'),
                'details': ('detail_requests', 'detail_request'),
                'short': ('short', None),
                'empty': ('empty', None),
                'rate_limited': ('rate_limited', 'rate_limited')
            }

            if source in source_handlers:
                stat_key, stat_type = source_handlers[source]
                self.stats[stat_key] += 1
                if stat_type:
                    self.update_stats(stat_type)

        if result.get('source') != 'cache':
            self.stats['cache_misses'] += 1
            self.update_stats('cache_miss')

    def show_progress(self, completed, total):
        """Показывает прогресс обработки"""
        progress = (completed / total) * 100
        elapsed = time.time() - self.stats['start']
        games_per_sec = completed / elapsed if elapsed > 0 else 0
        cache_hit_rate = (self.stats['cache_hits'] / completed * 100) if completed > 0 else 0

        bar_length = 20
        filled = int(bar_length * progress / 100)
        bar = "[" + "=" * filled + " " * (bar_length - filled) + "]"

        self.stdout.write(
            f'{bar} {progress:.0f}% | '
            f'{completed}/{total} | '
            f'{games_per_sec:.1f} игр/сек | '
            f'Кэш: {cache_hit_rate:.0f}%'
        )

    def handle_processing_error(self, game, results, message):
        """Обрабатывает ошибку при обработке игры"""
        self.stats['errors'] += 1
        results['errors'].append(game.id)
        self.stdout.write(f'   {message}: {game.name}')

    def save_efficiency_stats(self, stats, repeat_num):
        """Сохранение статистики эффективности"""
        if not self.stats_db:
            return

        try:
            timestamp = datetime.now().isoformat()
            total_processed = stats.get('total', 0)
            cache_hits = stats.get('cache_hits', 0)

            cache_efficiency = (cache_hits / total_processed * 100) if total_processed > 0 else 0

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
            estimated_without_cache = total_cache_checks * 1.5
            requests_saved = estimated_without_cache - total_requests

            self.stdout.write(f'   💰 Сэкономлено запросов: ~{int(requests_saved)}')

            if estimated_without_cache > 0:
                savings_percent = (requests_saved / estimated_without_cache) * 100
                self.stdout.write(f'   📉 Экономия: {savings_percent:.1f}%')

        if self.stats_db:
            try:
                cursor = self.stats_db.cursor()
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
        if self.overwrite:
            games_query = Game.objects.all()
        else:
            games_query = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            )

        if not self.include_all_gametypes:
            games_query = games_query.filter(
                game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
            )

        if game_ids_str:
            try:
                game_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                games_query = games_query.filter(igdb_id__in=game_ids)
            except ValueError:
                self.stdout.write(self.style.ERROR('❌ Некорректный формат game-ids'))
                return []

        # Подсчет общего количества подходящих игр до применения auto-offset
        total_before_filter = games_query.count()

        if auto_offset and self.not_found_ids:
            games_query = games_query.exclude(igdb_id__in=self.not_found_ids)
            self.stats['auto_offset_skipped'] = len(self.not_found_ids)

            # Показываем разницу
            total_after_filter = games_query.count()
            skipped_count = total_before_filter - total_after_filter
            if skipped_count > 0:
                self.stdout.write(f'🔍 Auto-offset: исключено {skipped_count} ненайденных игр')

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

        # Показываем информацию о реальном количестве игр
        if self.debug and len(games) < (self.limit if self.limit > 0 else float('inf')):
            self.stdout.write(f'   ℹ️  Фактически найдено для обработки: {len(games)} игр')

        return games

    def show_start_info(self, games, repeat_num, auto_offset=False):
        """Показывает стартовую информацию"""
        actual_count = len(games)
        self.stdout.write(f'🚀 [{repeat_num}] Начинаем импорт {actual_count} игр')
        if self.limit > 0 and actual_count < self.limit:
            self.stdout.write(f'   ⚠️  Фактически найдено: {actual_count} из {self.limit}')
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

    def load_not_found_ids(self, filename):
        """Загружает список не найденных игр (IGDB_ID)"""
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                    if 'not_found_games' in data:
                        not_found_ids = [game['igdb_id'] for game in data['not_found_games'] if 'igdb_id' in game]
                        self.not_found_ids = set(not_found_ids)

                        summary = data.get('summary', {})
                        self.stdout.write(f'📂 Загружено {len(not_found_ids)} не найденных игр из файла')
                        if 'last_updated' in summary:
                            self.stdout.write(f'   📅 Последнее обновление: {summary["last_updated"]}')

                    elif 'not_found_ids' in data:
                        self.not_found_ids = set(data.get('not_found_ids', []))
                        self.stdout.write(f'📂 Загружено {len(self.not_found_ids)} не найденных игр (старый формат)')
                    else:
                        self.not_found_ids = set()

        except Exception as e:
            self.stdout.write(f'   ⚠️ Ошибка загрузки not_found_ids: {e}')
            self.not_found_ids = set()

    def save_not_found_ids(self, filename):
        """Сохраняет список не найденных игр (IGDB_ID и названия)"""
        try:
            not_found_details = []

            if self.not_found_ids:
                self.stdout.write(f'💾 Сохранение {len(self.not_found_ids)} ненайденных игр в файл...')

                for igdb_id in self.not_found_ids:
                    try:
                        game = Game.objects.filter(igdb_id=igdb_id).first()
                        if game:
                            not_found_details.append({
                                'igdb_id': igdb_id,
                                'name': game.name,
                                'game_type': game.game_type.igdb_id if game.game_type else None,
                                'game_type_name': game.game_type.name if game.game_type else None,
                                'first_release_date': game.first_release_date.isoformat() if game.first_release_date else None,
                                'rating': game.rating,
                                'rating_count': game.rating_count
                            })
                        else:
                            not_found_details.append({
                                'igdb_id': igdb_id,
                                'name': 'Игра удалена из БД',
                                'game_type': None,
                                'game_type_name': None,
                                'first_release_date': None,
                                'rating': None,
                                'rating_count': None
                            })
                    except Exception as e:
                        if self.debug:
                            self.stdout.write(f'   ⚠️ Ошибка получения деталей игры {igdb_id}: {e}')
                        not_found_details.append({
                            'igdb_id': igdb_id,
                            'name': 'Ошибка получения названия',
                            'game_type': None,
                            'game_type_name': None,
                            'first_release_date': None,
                            'rating': None,
                            'rating_count': None
                        })

            data = {
                'not_found_games': not_found_details,
                'summary': {
                    'total_count': len(self.not_found_ids),
                    'last_updated': datetime.now().isoformat(),
                    'cache_enabled': not self.skip_cache,
                    'games_processed_in_session': self.stats.get('total', 0),
                    'not_found_in_session': self.stats.get('not_found_count', 0)
                },
                'meta': {
                    'note': 'Список содержит IGDB_ID игр, которые не были найдены в RAWG API',
                    'format': 'Каждая запись содержит igdb_id, name, game_type, first_release_date и др.',
                    'important': 'Эти игры также сохранены в кэше RAWG и будут пропускаться при auto-offset'
                }
            }

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.stdout.write(f'✅ Файл не найденных игр сохранен: {filename}')
            self.stdout.write(f'   📊 Всего ненайденных игр: {len(self.not_found_ids)}')
            self.stdout.write(f'   💾 Также сохранено в кэш RAWG')

            if not_found_details:
                self.stdout.write(f'   📋 Примеры ненайденных игр:')
                for game in not_found_details[:5]:
                    self.stdout.write(f'      • {game["igdb_id"]}: {game["name"]}')
                if len(not_found_details) > 5:
                    self.stdout.write(f'      ... и еще {len(not_found_details) - 5} игр')

        except Exception as e:
            self.stdout.write(f'   ⚠️ Ошибка сохранения not_found_ids: {e}')
            try:
                data = {
                    'not_found_ids': list(self.not_found_ids),
                    'last_updated': datetime.now().isoformat(),
                    'count': len(self.not_found_ids),
                    'error': str(e)
                }
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                self.stdout.write(f'   💾 Сохранены только ID (без названий)')
            except:
                self.stdout.write(f'   💥 Критическая ошибка сохранения файла')

    def get_total_games_to_process(self, options, auto_offset=False):
        """Получает общее количество игр, которые нужно обработать"""
        try:
            # Базовый запрос - игры без описания
            games_query = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            )

            # Фильтр по типам игр
            if not options.get('include_all_gametypes', False):
                games_query = games_query.filter(
                    game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
                )

            # Фильтр по конкретным ID
            game_ids_str = options.get('game_ids')
            if game_ids_str:
                try:
                    game_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                    games_query = games_query.filter(igdb_id__in=game_ids)
                except ValueError:
                    return 0

            # Исключаем ненайденные игры если auto-offset включен
            if auto_offset:
                # Загружаем ненайденные игры перед подсчетом
                self.load_not_found_ids(options.get('auto_offset_file', 'auto_offset_log.json'))
                if self.not_found_ids:
                    games_query = games_query.exclude(igdb_id__in=self.not_found_ids)

            # Применяем offset
            offset = options.get('offset', 0)
            if offset > 0:
                # Получаем общее количество с учетом offset
                total_count = games_query.count()
                games_after_offset = max(0, total_count - offset)
            else:
                games_after_offset = games_query.count()

            # Применяем лимит
            limit = options.get('limit', 0)
            if limit > 0:
                games_after_offset = min(games_after_offset, limit)

            return games_after_offset

        except Exception as e:
            if self.debug:
                self.stdout.write(f'⚠️ Ошибка при подсчете игр для обработки: {e}')
            return 0

    def show_rawg_stats(self):
        """Показывает статистику по играм"""
        try:
            total_games = Game.objects.count()
            games_with_rawg = Game.objects.filter(
                ~Q(rawg_description__isnull=True) &
                ~Q(rawg_description__exact='')
            ).count()

            games_filtered = Game.objects.filter(
                game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
            ).count()
            games_filtered_with_rawg = Game.objects.filter(
                Q(game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]) &
                ~Q(rawg_description__isnull=True) &
                ~Q(rawg_description__exact='')
            ).count()

            total_percentage = (games_with_rawg / total_games * 100) if total_games > 0 else 0
            filtered_percentage = (games_filtered_with_rawg / games_filtered * 100) if games_filtered > 0 else 0

            self.stdout.write('\n' + '📊' * 15)
            self.stdout.write('📈 СТАТИСТИКА БАЗЫ ДАННЫХ:')
            self.stdout.write(f'   Всего игр в БД: {total_games:,}')
            self.stdout.write(f'   ✅ С RAWG описанием: {games_with_rawg:,} ({total_percentage:.1f}%)')

            self.stdout.write(f'\n   🎮 Игр с типами 0,1,2,4,5,8,9,10,11: {games_filtered:,}')
            self.stdout.write(f'   ✅ С RAWG описанием: {games_filtered_with_rawg:,} ({filtered_percentage:.1f}%)')

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

            if games_filtered > 0:
                bar_length = 30
                filled = int(bar_length * filtered_percentage / 100)
                bar = "[" + "█" * filled + "░" * (bar_length - filled) + "]"
                self.stdout.write(f'\n   {bar} {filtered_percentage:.1f}% заполнено (основные типы)')

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
            self.stdout.write(f'   ⚠️ Ошибка получения статистики БД: {e}')
