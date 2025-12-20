# FILE: base_command.py
import time
import json
import os
from pathlib import Path
from collections import defaultdict
from django.core.management.base import BaseCommand
from django.db.models import Q
from games.models import Game


class ImportRawgBaseCommand(BaseCommand):
    """Базовый класс для импорта RAWG описаний"""

    def __init__(self):
        super().__init__()
        self.rawg_client = None
        self.stats_db = None
        self.api_stats = defaultdict(int)
        self.not_found_ids = set()
        self.shutdown_flag = False
        self.balance_exceeded = False

        # Статистика ВСЕХ обработанных игр за время выполнения
        self.global_stats = {
            'total_processed': 0,  # Всего игр обработано за всё время
            'found': 0,  # Найдено с описанием
            'not_found': 0,  # Не найдено (нет игры или пустое описание)
            'errors': 0,  # Ошибки обработки
            'cache_hits': 0,  # Попаданий в кэш
            'cache_misses': 0,  # Промахов кэша
            'start_time': time.time(),  # Время начала работы
            'sessions_processed': 0  # Количество обработанных сессий
        }

        self.progress_data = {
            'total_games': 0,  # Всего игр для обработки в текущем запуске
            'processed_games': 0,  # Обработано в текущем запуске
            'session_start_time': 0,  # Время начала текущей сессии
            'last_progress_length': 0,
            'current_status': '',
            'current_batch': 0,  # Текущий батч
            'current_batch_total': 0  # Всего в текущем батче
        }

    def update_api_balance_in_progress(self):
        """Обновляет информацию о балансе API для прогресс-бара"""
        if hasattr(self, 'rawg_client') and self.rawg_client:
            try:
                balance = self.rawg_client.check_balance()

                # Сохраняем в прогресс-данных
                self.progress_data['api_remaining'] = balance['remaining']
                self.progress_data['api_percentage'] = balance['percentage']
                self.progress_data['api_exceeded'] = balance['exceeded']
                self.progress_data['api_is_low'] = balance['is_low']

                # Если лимит исчерпан
                if balance['exceeded']:
                    self.progress_data['current_status'] = "🚫 Лимит исчерпан"
                    self.clear_progress_line()
                    self.stdout.write(f"\n🚫 ЛИМИТ API ИСЧЕРПАН: {balance['used']:,}/{balance['limit']:,}")
                    self.stdout.write(f"\n   Остановка обработки...\n")
                    return balance

                # Предупреждения при низком балансе
                remaining = balance['remaining']

                if remaining < 50 and not self.progress_data.get('warning_50_shown'):
                    self.clear_progress_line()
                    self.stdout.write(f"\n🔴 КРИТИЧЕСКИ МАЛО ЗАПРОСОВ: {remaining:,}")
                    self.stdout.write(f"\n   Следующий батч может не завершиться!")
                    self.update_single_progress_line()
                    self.progress_data['warning_50_shown'] = True

                elif remaining < 100 and not self.progress_data.get('warning_100_shown'):
                    self.clear_progress_line()
                    self.stdout.write(f"\n🟠 МАЛО ЗАПРОСОВ: {remaining:,}")
                    self.stdout.write(f"\n   Рекомендуется остановиться после этого батча.")
                    self.update_single_progress_line()
                    self.progress_data['warning_100_shown'] = True

                elif remaining < 500 and not self.progress_data.get('warning_500_shown'):
                    self.clear_progress_line()
                    self.stdout.write(f"\n🟡 ВНИМАНИЕ: Осталось {remaining:,} запросов")
                    self.update_single_progress_line()
                    self.progress_data['warning_500_shown'] = True

                elif remaining < 1000 and not self.progress_data.get('warning_1000_shown'):
                    self.clear_progress_line()
                    self.stdout.write(f"\n🟡 Осталось менее 1,000 запросов: {remaining:,}")
                    self.update_single_progress_line()
                    self.progress_data['warning_1000_shown'] = True

                # Обновляем прогресс-бар
                self.update_single_progress_line()

                return balance

            except Exception as e:
                if hasattr(self, 'original_options') and self.original_options.get('debug'):
                    print(f"[DEBUG] Ошибка обновления баланса API: {e}")
                return None

    def show_progress_summary(self):
        """Показывает сводную статистику из прогресс-бара"""
        print("\n" + "=" * 60)
        print("📊 ГЛОБАЛЬНАЯ СТАТИСТИКА (за всё время работы)")
        print("=" * 60)
        print(f"🎮 Всего обработано игр: {self.global_stats['total_processed']:,}")

        if self.global_stats['total_processed'] > 0:
            elapsed = time.time() - self.global_stats['start_time']
            games_per_sec = self.global_stats['total_processed'] / elapsed

            print(f"⏱️  Общее время работы: {elapsed:.1f} сек")
            print(f"⚡ Средняя скорость: {games_per_sec:.1f} игр/сек")
            print(f"🔁 Обработанных сессий: {self.global_stats['sessions_processed']}")

            # Проценты от общего количества
            found_pct = (self.global_stats['found'] / self.global_stats['total_processed'] * 100)
            not_found_pct = (self.global_stats['not_found'] / self.global_stats['total_processed'] * 100)
            errors_pct = (self.global_stats['errors'] / self.global_stats['total_processed'] * 100)

            print(f"\n📈 РАСПРЕДЕЛЕНИЕ РЕЗУЛЬТАТОВ:")
            print(f"✅ Найдено описаний: {self.global_stats['found']:,} ({found_pct:.1f}%)")
            print(f"❌ Не найдено игр: {self.global_stats['not_found']:,} ({not_found_pct:.1f}%)")
            print(f"💥 Ошибок обработки: {self.global_stats['errors']:,} ({errors_pct:.1f}%)")

            # Проверка суммы (должно быть 100%)
            total_pct = found_pct + not_found_pct + errors_pct
            print(f"📊 Сумма категорий: {total_pct:.1f}%")

            # Статистика кэша
            if self.global_stats['cache_hits'] + self.global_stats['cache_misses'] > 0:
                cache_total = self.global_stats['cache_hits'] + self.global_stats['cache_misses']
                cache_rate = (self.global_stats['cache_hits'] / cache_total * 100)
                print(f"\n💾 ЭФФЕКТИВНОСТЬ КЭША:")
                print(f"✅ Попаданий в кэш: {self.global_stats['cache_hits']:,}")
                print(f"❌ Промахов кэша: {self.global_stats['cache_misses']:,}")
                print(f"📊 Эффективность: {cache_rate:.1f}%")

        print("=" * 60)

    def update_global_stats_from_batch(self, batch_stats):
        """Обновляет глобальную статистику из статистики батча"""
        if not batch_stats:
            return

        # Важно: batch_stats должен содержать статистику только для одного батча
        self.global_stats['total_processed'] += batch_stats.get('total_processed', 0)
        self.global_stats['found'] += batch_stats.get('found', 0)
        self.global_stats['not_found'] += batch_stats.get('not_found_count', 0)
        self.global_stats['errors'] += batch_stats.get('errors', 0)
        self.global_stats['cache_hits'] += batch_stats.get('cache_hits', 0)
        self.global_stats['cache_misses'] += batch_stats.get('cache_misses', 0)

    def update_progress_from_stats(self, stats):
        """Обновляет прогресс-бар из статистики обработки"""
        if not stats:
            return

        # Обновляем общее количество обработанных игр в текущем запуске
        games_in_batch = stats.get('total_processed', 0)
        if games_in_batch > 0:
            self.progress_data['processed_games'] += games_in_batch

        # Обновляем прогресс-бар
        self.update_single_progress_line()

    def safe_print(self, message, newline=True):
        """Безопасный вывод поверх строки прогресса"""
        self.clear_progress_line()

        if newline:
            self.stdout.write(message + '\n')
        else:
            self.stdout.write(message)

        self.stdout.flush()

        if (self.progress_data.get('total_games', 0) > 0 and
                self.progress_data.get('processed_games', 0) < self.progress_data.get('total_games', 0)):
            time.sleep(0.1)
            self.update_single_progress_line()

    def update_single_progress_line(self, message=None):
        """Обновляет единую строку прогресса с улучшенным форматированием"""
        if message:
            # Обрезаем слишком длинные сообщения
            if len(message) > 15:
                message = message[:12] + "..."
            self.progress_data['current_status'] = message

        processed = self.progress_data.get('processed_games', 0)
        total = self.progress_data.get('total_games', 0)

        if total == 0:
            status_msg = self.progress_data.get('current_status', '')
            progress_str = f'\r{status_msg}'
            self.stdout.write(progress_str, ending='')
            self.stdout.flush()
            self.progress_data['last_progress_length'] = len(progress_str) - 1
            return

        # Процент выполнения текущей сессии
        progress = (processed / total * 100) if total > 0 else 0

        # Время текущей сессии
        elapsed = time.time() - self.progress_data.get('session_start_time', time.time())
        games_per_sec = processed / elapsed if elapsed > 0 else 0

        # Оставшееся время в текущей сессии
        remaining = total - processed
        eta_seconds = (elapsed / processed * remaining) if processed > 0 else 0

        # Форматирование времени ETA
        if eta_seconds < 60:
            eta_str = f"{eta_seconds:.0f}с"
        elif eta_seconds < 3600:
            eta_str = f"{eta_seconds / 60:.0f}м"
        else:
            eta_str = f"{eta_seconds / 3600:.1f}ч"

        # Используем ГЛОБАЛЬНУЮ статистику за всё время
        total_processed = self.global_stats['total_processed']
        total_found = self.global_stats['found']
        total_not_found = self.global_stats['not_found']
        total_errors = self.global_stats['errors']

        # Рассчитываем проценты от общего количества
        if total_processed > 0:
            found_pct = (total_found / total_processed * 100)
            not_found_pct = (total_not_found / total_processed * 100)
            errors_pct = (total_errors / total_processed * 100)
        else:
            found_pct = not_found_pct = errors_pct = 0

        # Компактная статистика с иконками
        stats_compact = (
            f"✅{total_found:>5d}({found_pct:>3.0f}%) "
            f"❌{total_not_found:>5d}({not_found_pct:>3.0f}%) "
            f"💥{total_errors:>5d}({errors_pct:>3.0f}%)"
        )

        # Информация о батче
        current_batch = self.progress_data.get('current_batch', 0)
        current_batch_total = self.progress_data.get('current_batch_total', 0)
        status_msg = self.progress_data.get('current_status', '')

        if current_batch_total > 0:
            batch_progress = (current_batch / current_batch_total * 100) if current_batch_total > 0 else 0
            batch_info = f"[Батч:{current_batch:>3d}/{current_batch_total:>3d}] {status_msg}"
        else:
            batch_info = status_msg

        # Ограничиваем длину
        if len(batch_info) > 25:
            batch_info = batch_info[:22] + "..."

        # Прогресс-бар текущей сессии
        bar_length = 20
        filled = int(bar_length * progress / 100)
        bar = "[" + "█" * filled + "░" * (bar_length - filled) + "]"

        # ПОЛУЧАЕМ И ОФОРМЛЯЕМ БАЛАНС API
        api_display = "∞"  # Значение по умолчанию
        api_color = "🟢"  # Цвет по умолчанию

        if hasattr(self, 'rawg_client') and self.rawg_client:
            try:
                balance = self.rawg_client.check_balance()
                api_remaining = balance['remaining']
                api_percentage = balance['percentage']

                # ФОРМАТИРОВАНИЕ ЧИСЛА ДЛЯ ОТОБРАЖЕНИЯ
                if api_remaining >= 1000000:
                    api_display = f"{api_remaining / 1000000:.1f}M"
                elif api_remaining >= 100000:
                    api_display = f"{api_remaining / 1000:.0f}K"
                elif api_remaining >= 10000:
                    api_display = f"{api_remaining / 1000:.1f}K"
                elif api_remaining >= 1000:
                    api_display = f"{api_remaining / 1000:.1f}K"
                else:
                    # ДЛЯ ЧИСЕЛ МЕНЬШЕ 1000 - ПОКАЗЫВАЕМ ПОЛНОЕ ЧИСЛО
                    api_display = f"{api_remaining}"

                # ЦВЕТОВАЯ ИНДИКАЦИЯ
                if api_percentage >= 90 or api_remaining < 100:
                    api_color = "🔴"  # Красный - критически мало
                elif api_percentage >= 80 or api_remaining < 500:
                    api_color = "🟠"  # Оранжевый - очень мало
                elif api_percentage >= 70 or api_remaining < 1000:
                    api_color = "🟡"  # Желтый - мало
                elif api_percentage >= 50:
                    api_color = "🟢"  # Зеленый - нормально
                else:
                    api_color = "🟢"  # Зеленый - много

            except Exception:
                api_display = "?"
                api_color = "⚫"

        # Форматируем API информацию с выравниванием
        api_info = f"{api_color}{api_display:>6s}"

        # Если осталось меньше 100 запросов, добавляем восклицательный знак
        if hasattr(self, 'rawg_client') and self.rawg_client:
            try:
                balance = self.rawg_client.check_balance()
                if balance['remaining'] < 100:
                    api_info = f"{api_color}{api_display:>5s}!"
            except:
                pass

        # Формируем итоговую строку прогресса
        progress_str = (
            f'\r{bar} {progress:>3.0f}% | '
            f'{processed:>5d}/{total:>5d} | '
            f'{games_per_sec:>4.1f} и/с | '
            f'ETA:{eta_str:>4s} | '
            f'{stats_compact} | '
            f'{api_info} | '  # БАЛАНС API
            f'{batch_info}'
        )

        self.stdout.write(progress_str, ending='')
        self.stdout.flush()
        self.progress_data['last_progress_length'] = len(progress_str) - 1

    def add_arguments(self, parser):
        """Добавление аргументов команды"""
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
            default=0.1,
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
            default=10.0,
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
            help='Удалить кэш и список ненайденных игр перед началом'
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
        parser.add_argument(
            '--ignore-auto-offset',
            action='store_true',
            help='Игнорировать auto-offset для конкретных игр (принудительная обработка)'
        )
        parser.add_argument(
            '--api-limit',
            type=int,
            default=None,
            help='Максимальное количество запросов к API (по умолчанию: 20000 или из кэша)'
        )

    def init_import_settings(self, options):
        """Инициализирует настройки для импорта"""
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

    def clear_progress_line(self):
        """Очищает строку прогресса"""
        if self.progress_data.get('last_progress_length', 0) > 0:
            # Очищаем полностью строку
            self.stdout.write('\r' + ' ' * self.progress_data['last_progress_length'] + '\r', ending='')
            self.stdout.flush()
            self.progress_data['last_progress_length'] = 0