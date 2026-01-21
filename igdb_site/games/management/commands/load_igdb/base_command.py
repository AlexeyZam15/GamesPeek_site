# games/management/commands/load_igdb/base_command.py
import sys
import os
import time
import signal
import threading
from django.core.management.base import BaseCommand
from django.utils import timezone
from games.igdb_api import make_igdb_request
from games.models import Game

try:
    from .offset_manager import OffsetManager
except ImportError:
    class OffsetManager:
        @staticmethod
        def get_query_key(where_clause, **params):
            import hashlib
            key_parts = [where_clause]
            for key, value in sorted(params.items()):
                if value is not None:
                    key_parts.append(f"{key}={value}")
            key_string = "|".join(str(part) for part in key_parts)
            return hashlib.md5(key_string.encode()).hexdigest()[:16]

        @staticmethod
        def save_offset(query_key, offset):
            return True

        @staticmethod
        def load_offset(query_key):
            return None

        @staticmethod
        def clear_offset(query_key=None):
            return True


class BaseProgressBar:
    """Базовый класс для прогресс-баров"""

    def __init__(self, stdout, total_games=0, total_loaded=0):
        self.stdout = stdout
        self.total_games = total_games
        self.total_loaded = total_loaded
        self.current_iteration = 0
        self.iterations_without_new = 0
        self.is_tty = False
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.update_interval = 0.1
        self.desc = "Загрузка игр"
        self.bar_length = 30
        self.stat_width = 5
        self.emoji_spacing = 1
        self._enabled = True
        self.filled_char = '█'
        self.empty_char = '░'

        # Детальная статистика как в progress_bar.py
        self.stats = {
            'found_count': 0,
            'total_criteria_found': 0,
            'skipped_total': 0,
            'errors': 0,
            'updated': 0,
            'created': 0,
            'processed': 0,
        }

    def set_enabled(self, enabled: bool):
        """Включить/выключить прогресс-бар"""
        self._enabled = enabled

    def update_stats(self, stats: dict):
        """Обновить статистику"""
        if not self._enabled:
            return
        self.stats.update(stats)

    def _calculate_time_string(self, elapsed_time, current, total):
        """Рассчитывает строку времени"""
        if current > 0 and current < total:
            # Оставшееся время
            remaining_time = (elapsed_time / current) * (total - current)
            return f"{elapsed_time:.0f}s < {remaining_time:.0f}s"
        else:
            return f"{elapsed_time:.0f}s"

    def update(self, total_games=None, total_loaded=None, current_iteration=None,
               iterations_without_new=None, updated_count=0, failed_count=0,
               skipped_count=0, created_count=0, processed_count=0, errors=0):
        """Обновляет прогресс со статистикой"""
        if not self._enabled:
            return

        # Обновляем базовые параметры
        if total_games is not None:
            self.total_games = total_games
        if total_loaded is not None:
            self.total_loaded = total_loaded
        if current_iteration is not None:
            self.current_iteration = current_iteration
        if iterations_without_new is not None:
            self.iterations_without_new = iterations_without_new

        # Обновляем статистику
        self.stats['updated'] = updated_count
        self.stats['errors'] = errors
        self.stats['skipped_total'] = skipped_count
        self.stats['created'] = created_count
        self.stats['processed'] = processed_count

        current_time = time.time()

        # Обновляем не чаще чем update_interval секунд
        if current_time - self.last_update_time < self.update_interval:
            return

        self.last_update_time = current_time

        # Показываем прогресс
        self._display()

    def _display(self):
        """Отображает прогресс-бар - должен быть переопределен в наследниках"""
        raise NotImplementedError

    def clear(self):
        """Очищает прогресс-бар"""
        pass

    def final_message(self, message):
        """Выводит финальное сообщение"""
        if self._enabled:
            self.stdout.write('\n' + message + '\n')


class TopProgressBar(BaseProgressBar):
    """Отображает прогресс вверху терминала с ANSI escape codes"""

    def __init__(self, stdout, total_games=0, total_loaded=0):
        super().__init__(stdout, total_games, total_loaded)
        self.is_tty = sys.stdout.isatty() and self._supports_ansi()
        self.last_printed_length = 0

    def _supports_ansi(self):
        """Проверяет поддержку ANSI escape codes"""
        if os.name == 'nt':
            return os.environ.get('TERM') == 'xterm' or \
                os.environ.get('WT_SESSION') is not None
        else:
            return True

    def _display(self):
        """Отображает прогресс вверху терминала"""
        if not self._enabled or not self.is_tty:
            return

        # Сохраняем позицию курсора
        sys.stdout.write('\x1b[s')

        # Перемещаемся в начало
        sys.stdout.write('\x1b[1;1H')

        # Очищаем строку
        sys.stdout.write('\x1b[2K')

        # Рассчитываем процент (но не больше 100%)
        if self.total_games > 0:
            percentage = (self.total_loaded / self.total_games * 100) if self.total_games > 0 else 0
            if percentage > 100:
                percentage = 100
        else:
            percentage = 0

        # Рассчитываем заполненную часть
        if self.total_games > 0:
            filled_length = int(self.bar_length * self.total_loaded // self.total_games)
            if filled_length > self.bar_length:
                filled_length = self.bar_length
        else:
            filled_length = 0

        bar = self.filled_char * filled_length + self.empty_char * (self.bar_length - filled_length)

        # Рассчитываем время
        elapsed_time = time.time() - self.start_time
        time_str = self._calculate_time_string(elapsed_time, self.total_loaded, self.total_games)

        # Формируем основное сообщение
        if self.total_games > 0:
            message = f"\r{self.desc}: {percentage:3.0f}% [{self.total_loaded}/{self.total_games}] [{bar}] "
        else:
            message = f"\r{self.desc}: [{self.total_loaded} игр] "

        # Добавляем детальную статистику с эмодзи
        spacing = " " * self.emoji_spacing

        # Основные эмодзи
        message += f"✅{spacing}{self.stats['created']:>{self.stat_width}} "
        message += f"💾{spacing}{self.stats['updated']:>{self.stat_width}} "
        message += f"❌{spacing}{self.stats['errors']:>{self.stat_width}} "
        message += f"⏭️{spacing}{self.stats['skipped_total']:>{self.stat_width}} "

        # Если есть итерации
        if self.current_iteration > 0:
            message += f"🔄{spacing}{self.current_iteration:>{self.stat_width}} "
        if self.iterations_without_new > 0:
            message += f"🚫{spacing}{self.iterations_without_new:>{self.stat_width}} "

        message += f"({time_str})"

        # Очищаем остаток строки
        terminal_width = 150  # Минимальная ширина для очистки
        message_length = len(message)
        if message_length < terminal_width:
            message += " " * (terminal_width - message_length)

        # Сохраняем длину для очистки
        self.last_printed_length = len(message)

        # Выводим сообщение
        sys.stdout.write(message)

        # Восстанавливаем позицию курсора
        sys.stdout.write('\x1b[u')
        sys.stdout.flush()

    def clear(self):
        """Очищает прогресс-бар"""
        if not self._enabled or not self.is_tty:
            return

        sys.stdout.write('\x1b[s')
        sys.stdout.write('\x1b[1;1H')
        sys.stdout.write('\x1b[2K')
        sys.stdout.write('\x1b[u')
        sys.stdout.flush()

    def final_message(self, message):
        """Выводит финальное сообщение"""
        if not self._enabled:
            self.stdout.write('\n' + message + '\n')
            return

        if self.is_tty:
            self.clear()
            sys.stdout.write('\x1b[s')
            sys.stdout.write('\x1b[1;1H')
            sys.stdout.write('\x1b[2K')
            sys.stdout.write(message)
            sys.stdout.write('\x1b[u')
            sys.stdout.write('\n')
            sys.stdout.flush()
        else:
            self.stdout.write('\n' + message + '\n')


class SimpleProgressBar(BaseProgressBar):
    """Простой прогресс-бар для PowerShell и других без ANSI поддержки"""

    def __init__(self, stdout, total_games=0, total_loaded=0):
        super().__init__(stdout, total_games, total_loaded)
        self.last_update_time = time.time()
        self.update_interval = 2  # Реже обновляем для простого режима
        self.last_printed_length = 0

    def _display(self):
        """Отображает прогресс в одной строке"""
        if not self._enabled:
            return

        # Очищаем предыдущую строку
        if self.last_printed_length > 0:
            sys.stdout.write('\r' + ' ' * self.last_printed_length + '\r')

        # Рассчитываем процент
        if self.total_games > 0:
            percentage = (self.total_loaded / self.total_games * 100) if self.total_games > 0 else 0
            if percentage > 100:
                percentage = 100

            # Рассчитываем заполненную часть
            filled_length = int(self.bar_length * self.total_loaded // self.total_games)
            if filled_length > self.bar_length:
                filled_length = self.bar_length
            bar = self.filled_char * filled_length + self.empty_char * (self.bar_length - filled_length)

            message = f"{self.desc}: {percentage:3.0f}% [{self.total_loaded}/{self.total_games}] [{bar}] "
        else:
            message = f"{self.desc}: [{self.total_loaded} игр] "

        # Рассчитываем время
        elapsed_time = time.time() - self.start_time
        time_str = self._calculate_time_string(elapsed_time, self.total_loaded, self.total_games)

        # Добавляем детальную статистику
        spacing = " " * self.emoji_spacing

        # Основные эмодзи
        message += f"✅{spacing}{self.stats['created']:>{self.stat_width}} "
        message += f"💾{spacing}{self.stats['updated']:>{self.stat_width}} "
        message += f"❌{spacing}{self.stats['errors']:>{self.stat_width}} "
        message += f"⏭️{spacing}{self.stats['skipped_total']:>{self.stat_width}} "

        # Если есть итерации
        if self.current_iteration > 0:
            message += f"🔄{spacing}{self.current_iteration:>{self.stat_width}} "
        if self.iterations_without_new > 0:
            message += f"🚫{spacing}{self.iterations_without_new:>{self.stat_width}} "

        message += f"({time_str})"

        # Сохраняем длину для следующей очистки
        self.last_printed_length = len(message)

        # Выводим без перевода строки
        sys.stdout.write('\r' + message)
        sys.stdout.flush()

    def clear(self):
        """Очищает прогресс-бар"""
        if not self._enabled:
            return

        if self.last_printed_length > 0:
            sys.stdout.write('\r' + ' ' * self.last_printed_length + '\r')
            sys.stdout.flush()
            self.last_printed_length = 0

    def final_message(self, message):
        """Выводит финальное сообщение"""
        if not self._enabled:
            self.stdout.write('\n' + message + '\n')
            return

        # Очищаем прогресс-бар
        self.clear()
        self.stdout.write('\n' + message + '\n')


class BaseGamesCommand(BaseCommand):
    """Базовый класс для команд загрузки IGDB - БЕЗ add_arguments"""

    DEFAULT_ITERATION_LIMIT = 100
    max_consecutive_no_new_games = 3  # Добавляем значение по умолчанию

    def __init__(self):
        super().__init__()
        self.debug_mode = False  # Добавляем атрибут
        # Инициализация GameCacheManager
        try:
            from .game_cache import GameCacheManager
            self.cache_manager = GameCacheManager
        except ImportError:
            class GameCacheManager:
                @staticmethod
                def clear_cache():
                    return True

            self.cache_manager = GameCacheManager

    def _get_offset_params(self, options):
        """Получает параметры для создания ключа offset"""
        # ВСЕГДА в одном порядке для одинаковых параметров
        return {
            'game_modes': options.get('game_modes', ''),
            'game_names': options.get('game_names', ''),
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': self._get_loading_mode(options),
        }

    def _get_saved_offset(self, options):
        """Получает сохраненный offset для текущих параметров"""
        params = self._get_offset_params(options)
        return OffsetManager.load_offset(params)

    def _save_offset_for_continuation(self, options, current_offset):
        """Сохраняет offset для продолжения"""
        params = self._get_offset_params(options)
        saved = OffsetManager.save_offset(params, current_offset)

        if saved and options.get('debug', False):
            self.stdout.write(f'   💾 Сохранен offset для параметров {params}: {current_offset}')

        return saved

    def _handle_reset_offset(self, options, debug):
        """Обрабатывает сброс сохраненного offset"""
        params = self._get_offset_params(options)
        cleared = OffsetManager.clear_offset(params)

        if cleared:
            self.stdout.write('🔄 Сброшен сохраненный offset для текущих параметров')
        else:
            self.stdout.write('⚠️  Не удалось сбросить offset или offset не существует')

    def handle(self, *args, **options):
        """Основной метод выполнения команды - должен быть переопределен в наследниках"""
        raise NotImplementedError("Метод handle должен быть переопределен в наследниках")

    def _create_progress_bar(self):
        """Создает подходящий прогресс-бар для текущего терминала"""
        import os
        import sys

        # Проверяем поддержку ANSI
        supports_ansi = False
        if hasattr(sys.stdout, 'isatty') and sys.stdout.isatty():
            if os.name == 'nt':
                supports_ansi = os.environ.get('TERM') == 'xterm' or \
                                os.environ.get('WT_SESSION') is not None or \
                                os.environ.get('ANSICON') is not None
            else:
                supports_ansi = True

        if supports_ansi:
            return TopProgressBar(self.stdout)
        else:
            return SimpleProgressBar(self.stdout)

    def _determine_execution_mode(self, repeat_count):
        """Определяет режим выполнения команды"""
        infinite_mode = repeat_count == 0
        single_run_mode = repeat_count == -1
        finite_mode = repeat_count > 0

        if single_run_mode:
            repeat_count = 1
            self.stdout.write('🔄 РЕЖИМ: ОДНА ИТЕРАЦИЯ (--repeat -1)')
        elif infinite_mode:
            self.stdout.write('🔄 РЕЖИМ: БЕСКОНЕЧНО (--repeat 0) - пока не закончатся игры')
            repeat_count = 999999
        elif finite_mode:
            self.stdout.write(f'🔄 РЕЖИМ: {repeat_count} ПОВТОРЕНИЙ')
        else:
            raise ValueError(
                'Неверное значение --repeat. Используйте: -1 (один раз), 0 (бесконечно), >0 (фиксированно)')

        return {
            'infinite_mode': infinite_mode,
            'single_run_mode': single_run_mode,
            'finite_mode': finite_mode,
            'repeat_count': repeat_count
        }

    def _initialize_total_stats(self, original_offset):
        """Инициализирует общую статистику"""
        return {
            'iterations': 0,
            'total_games_found': 0,
            'total_games_checked': 0,
            'total_games_created': 0,
            'total_games_skipped': 0,
            'total_games_updated': 0,
            'total_time': 0,
            'last_checked_offset': original_offset,
            'errors': 0,
            'iterations_with_errors': 0,
            'iterations_with_limit_reached': 0,
            'iterations_with_no_new_games': 0,
            'max_iterations_reached': False,
            'interrupted': False,
        }

    def _display_startup_info(self, execution_mode, iteration_limit, limit):
        """Отображает информацию о запуске команды"""
        if execution_mode['repeat_count'] > 1 or execution_mode['infinite_mode']:
            repeat_display = execution_mode["repeat_count"] if not execution_mode[
                'infinite_mode'] else "до исчерпания игр"
            self.stdout.write(f'🔄 КОМАНДА БУДЕТ ПОВТОРЕНА {repeat_display} РАЗ')
            self.stdout.write(f'📊 Игр за итерацию: {iteration_limit}')
            if limit > 0:
                self.stdout.write(f'🎯 Общий лимит игр: {limit}')
            self.stdout.write('=' * 60)

    def _get_execution_parameters(self, options):
        """Получает параметры выполнения из options"""
        return {
            'genres_str': options['genres'],
            'description_contains': options['description_contains'],
            'overwrite': options['overwrite'],
            'debug': options['debug'],
            'limit': options['limit'],
            'offset': options['offset'],
            'min_rating_count': options['min_rating_count'],
            'keywords_str': options['keywords'],
            'count_only': options['count_only'],
            'game_types_str': options['game_types'],
            'iteration_limit': options['iteration_limit'],
        }

    def _calculate_iteration_limit(self, limit, iteration_limit, total_stats):
        """Рассчитывает лимит для текущей итерации"""
        if limit > 0:
            remaining_limit = limit - total_stats['total_games_created']
            if remaining_limit <= 0:
                return 0, False
            iteration_limit_actual = min(iteration_limit, remaining_limit)
            return iteration_limit_actual, True
        else:
            return iteration_limit, True

    def _should_continue_iteration(self, iteration, execution_mode, total_stats, limit, max_consecutive_no_new_games):
        """Проверяет, следует ли продолжать выполнение"""
        infinite_mode = execution_mode['infinite_mode']
        single_run_mode = execution_mode['single_run_mode']
        finite_mode = execution_mode['finite_mode']
        repeat_count = execution_mode['repeat_count']

        # Проверяем условия остановки для бесконечного режима
        if infinite_mode and iteration > 1:
            if total_stats['iterations_with_no_new_games'] >= max_consecutive_no_new_games:
                self.stdout.write(f'\n⚠️  ОСТАНОВКА: {max_consecutive_no_new_games} итераций подряд без новых игр')
                return False

            if limit > 0 and total_stats['total_games_created'] >= limit:
                self.stdout.write(f'\n✅ ДОСТИГНУТ ЛИМИТ: {limit} игр загружено')
                return False

        # Для конечного режима проверяем лимит итераций
        if finite_mode and iteration > repeat_count:
            total_stats['max_iterations_reached'] = True
            return False

        # Для режима одного раза
        if single_run_mode and iteration > 1:
            return False

        return True

    def _update_total_stats(self, total_stats, iteration_stats, iteration,
                            current_offset, execution_mode, progress_bar):
        """Обновляет общую статистику"""
        # Обновляем статистику
        total_stats['iterations'] += 1
        total_stats['total_games_found'] += iteration_stats.get('total_games_found', 0)
        total_stats['total_games_checked'] += iteration_stats.get('total_games_checked',
                                                                  iteration_stats.get('total_games_found', 0))
        total_stats['total_games_created'] += iteration_stats.get('created_count', 0)
        total_stats['total_games_skipped'] += iteration_stats.get('skipped_count', 0)
        total_stats['total_games_updated'] += iteration_stats.get('updated_count', 0)
        total_stats['total_time'] += iteration_stats.get('total_time', 0)

        # Проверяем, были ли найдены новые игры в этой итерации
        new_games_this_iteration = iteration_stats.get('created_count', 0)
        if new_games_this_iteration == 0 and iteration_stats.get('total_games_found', 0) == 0:
            total_stats['iterations_with_no_new_games'] += 1
        else:
            total_stats['iterations_with_no_new_games'] = 0

        # ОБНОВЛЯЕМ ПРОГРЕСС-БАР
        if progress_bar:
            progress_bar.update(
                total_loaded=total_stats['total_games_created'],
                current_iteration=iteration,
                iterations_without_new=total_stats['iterations_with_no_new_games'],
                created_count=total_stats['total_games_created'],
                updated_count=total_stats['total_games_updated'],
                skipped_count=total_stats['total_games_skipped'],
                errors=total_stats['errors']
            )

        # Добавляем ошибки из итерации
        iteration_errors = iteration_stats.get('errors', 0)
        if iteration_errors > 0:
            total_stats['errors'] += iteration_errors
            total_stats['iterations_with_errors'] += 1

        # Получаем последний проверенный offset
        limit_reached_offset = iteration_stats.get('limit_reached_at_offset')
        if limit_reached_offset is not None:
            last_checked_this_iteration = limit_reached_offset
        else:
            last_checked_this_iteration = iteration_stats.get('last_checked_offset',
                                                              current_offset + iteration_stats.get(
                                                                  'total_games_checked',
                                                                  iteration_stats.get('total_games_found', 0)) - 1)

        total_stats['last_checked_offset'] = last_checked_this_iteration
        new_offset = last_checked_this_iteration + 1

        if self.debug_mode:
            self.stdout.write(f'   📊 Итерация {iteration}:')
            self.stdout.write(f'      • Начальный offset: {current_offset}')
            self.stdout.write(f'      • Просмотрено игр: {iteration_stats.get("total_games_checked", 0)}')
            self.stdout.write(f'      • Найдено новых: {iteration_stats.get("total_games_found", 0)}')
            self.stdout.write(f'      • Загружено: {iteration_stats.get("created_count", 0)}')
            self.stdout.write(f'      • Обновлено: {iteration_stats.get("updated_count", 0)}')
            self.stdout.write(f'      • Ошибок: {iteration_errors}')
            self.stdout.write(f'      • Последний проверенный offset: {last_checked_this_iteration}')
            self.stdout.write(f'      • Следующий offset: {new_offset}')

        return new_offset

    def _handle_global_interrupt(self, total_stats, execution_mode,
                                 original_offset, current_offset,
                                 limit, progress_bar):
        """Обрабатывает глобальное прерывание команды (Ctrl+C)"""
        self.stdout.write('\n\n🛑 КОМАНДА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')

        if progress_bar:
            progress_bar.final_message("🛑 ВЫПОЛНЕНИЕ КОМАНДЫ ПРЕРВАНО")
            progress_bar.clear()

        self._display_interrupted_statistics(total_stats, execution_mode,
                                             original_offset, current_offset, limit)

        total_stats['interrupted'] = True

    def _display_interrupted_statistics(self, total_stats, execution_mode,
                                        original_offset, current_offset, limit):
        """Выводит статистику при прерывании команды"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('🛑 СТАТИСТИКА ПРЕРВАННОЙ КОМАНДЫ')
        self.stdout.write('=' * 60)

        if execution_mode['infinite_mode']:
            self.stdout.write(f'🔄 РЕЖИМ: БЕСКОНЕЧНЫЙ (прервано пользователем)')
        elif execution_mode['single_run_mode']:
            self.stdout.write(f'🔄 РЕЖИМ: ОДНА ИТЕРАЦИЯ (прервано)')
        else:
            self.stdout.write(f'🔄 Итераций выполнено: {total_stats["iterations"]} (прервано)')

        self.stdout.write(f'📍 Начальный offset: {original_offset}')
        self.stdout.write(f'📍 Текущий offset: {current_offset}')
        self.stdout.write(f'👀 Всего просмотрено игр: {total_stats["total_games_checked"]}')
        self.stdout.write(f'🎮 Всего найдено новых: {total_stats["total_games_found"]}')
        self.stdout.write(f'✅ Всего загружено игр: {total_stats["total_games_created"]}')
        self.stdout.write(f'💾 Всего обновлено игр: {total_stats["total_games_updated"]}')
        self.stdout.write(f'⏭️  Всего пропущено игр: {total_stats["total_games_skipped"]}')
        self.stdout.write(f'❌ Ошибок: {total_stats["errors"]}')

        if limit > 0:
            self.stdout.write(f'🎯 Общий лимит игр: {limit} (загружено: {total_stats["total_games_created"]})')

        self.stdout.write(f'⏱️  Общее время: {total_stats["total_time"]:.2f}с')

    def _display_final_statistics(self, total_stats, execution_mode, original_offset,
                                  current_offset, limit, overwrite):
        """Выводит финальную статистику"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 ОБЩАЯ СТАТИСТИКА ВСЕХ ИТЕРАЦИЙ')
        self.stdout.write('=' * 60)

        if execution_mode['infinite_mode']:
            self.stdout.write(
                f'🔄 РЕЖИМ: БЕСКОНЕЧНЫЙ (остановка после {self.max_consecutive_no_new_games} итераций без игр)')
        elif execution_mode['single_run_mode']:
            self.stdout.write(f'🔄 РЕЖИМ: ОДНА ИТЕРАЦИЯ')
        else:
            self.stdout.write(f'🔄 Итераций выполнено: {total_stats["iterations"]}/{execution_mode["repeat_count"]}')

        if total_stats['max_iterations_reached']:
            self.stdout.write(f'⚠️  ДОСТИГНУТ МАКСИМАЛЬНЫЙ ЛИМИТ ИТЕРАЦИЙ: {execution_mode["repeat_count"]}')

        self.stdout.write(f'📍 Начальный offset: {original_offset}')
        self.stdout.write(f'📍 Последний проверенный offset: {total_stats["last_checked_offset"]}')
        self.stdout.write(f'📍 Следующий offset (для продолжения): {current_offset}')
        self.stdout.write(f'👀 Всего просмотрено игр: {total_stats["total_games_checked"]}')
        self.stdout.write(f'🎮 Всего найдено новых: {total_stats["total_games_found"]}')
        self.stdout.write(f'✅ Всего загружено игр: {total_stats["total_games_created"]}')
        self.stdout.write(f'💾 Всего обновлено игр: {total_stats["total_games_updated"]}')
        self.stdout.write(f'⏭️  Всего пропущено игр: {total_stats["total_games_skipped"]}')
        self.stdout.write(f'❌ Ошибок: {total_stats["errors"]}')
        self.stdout.write(f'⚠️  Итераций с ошибками: {total_stats["iterations_with_errors"]}')
        self.stdout.write(f'🚫 Итераций без новых игр: {total_stats["iterations_with_no_new_games"]}')

        if limit > 0:
            self.stdout.write(f'🎯 Общий лимит игр: {limit} (достигнуто: {total_stats["total_games_created"]})')

        self.stdout.write(f'⏱️  Общее время: {total_stats["total_time"]:.2f}с')

    def _display_final_status(self, total_stats, limit):
        """Выводит итоговый статус команды"""
        self.stdout.write('=' * 60)
        if total_stats['errors'] > 0:
            self.stdout.write('⚠️  ЗАГРУЗКА ЗАВЕРШЕНА С ОШИБКАМИ!')
        elif total_stats['iterations_with_no_new_games'] >= self.max_consecutive_no_new_games:
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА: ИГРЫ ЗАКОНЧИЛИСЬ')
        elif total_stats['max_iterations_reached']:
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА: ДОСТИГНУТ ЛИМИТ ИТЕРАЦИЙ')
        elif limit > 0 and total_stats['total_games_created'] >= limit:
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА: ДОСТИГНУТ ЛИМИТ ИГР')
        elif total_stats['interrupted']:
            self.stdout.write('🛑 ЗАГРУЗКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ')
        else:
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО!')

    def _finalize_execution(self, total_stats, limit, progress_bar,
                            execution_mode, original_offset,
                            current_offset, limit_val, overwrite):
        """Завершает выполнение команда"""
        if progress_bar:
            if limit > 0:
                if total_stats['total_games_created'] >= limit:
                    progress_bar.final_message(
                        f"✅ ЗАГРУЗКА ЗАВЕРШЕНА: {total_stats['total_games_created']}/{limit} игр загружено")
                else:
                    progress_bar.final_message(
                        f"⚠️  ЗАГРУЗКА ОСТАНОВЛЕНА: {total_stats['total_games_created']}/{limit} игр загружено")
            else:
                progress_bar.final_message(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА: {total_stats['total_games_created']} игр загружено")

            progress_bar.clear()

        self._display_final_statistics(
            total_stats, execution_mode, original_offset,
            current_offset, limit_val, overwrite
        )

    def clear_game_cache(self):
        """Очищает кэш проверенных игр"""
        try:
            cleared = self.cache_manager.clear_cache()
            self.stdout.write(f"✅ Кэш проверенных игр очищен")
            return cleared
        except Exception as e:
            self.stderr.write(f"❌ Ошибка при очистке кэша: {e}")
            return False

    def _get_where_clause_for_current_command(self, options):
        """Получает where_clause для текущей команды"""
        game_names_str = options.get('game_names', '')
        game_modes_str = options.get('game_modes', '')  # НОВЫЙ ПАРАМЕТР
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')
        game_types_str = options.get('game_types', '')
        min_rating_count = options.get('min_rating_count', 0)

        where_parts = []

        # Режимы игры - просто указываем шаблон, так как ID будет найден позже
        if game_modes_str:
            where_parts.append('game_modes = (...)')
        # Имена игр
        elif game_names_str:
            name_list = [n.strip() for n in game_names_str.split(',') if n.strip()]
            name_conditions = [f'name ~ *"{name}"*' for name in name_list]
            where_parts.append(f'({" | ".join(name_conditions)})')
        # Определяем режим загрузки
        elif genres_str and description_contains:
            where_parts.append('genres = (...)')
            where_parts.append(f'(name ~ *"{description_contains}"* | summary ~ *"{description_contains}"*)')
        elif genres_str:
            where_parts.append('genres = (...)')
        elif description_contains:
            where_parts.append(f'(name ~ *"{description_contains}"* | summary ~ *"{description_contains}"*)')
        elif keywords_str:
            where_parts.append('keywords = (...)')

        # Обязательные условия
        if game_modes_str or game_names_str:
            # Для поиска по режимам или именам rating_count может быть 0
            where_parts.append('name != null')
            if min_rating_count > 0:
                where_parts.append(f'rating_count >= {min_rating_count}')
        else:
            where_parts.append('rating_count > 0')
            where_parts.append('name != null')
            if min_rating_count > 0:
                where_parts.append(f'rating_count >= {min_rating_count}')

        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_parts.append(f'game_type = ({game_types_str_query})')
            except ValueError:
                pass

        return ' & '.join(where_parts) if where_parts else 'rating_count > 0 & name != null'

    def _get_loading_mode(self, options):
        """Определяет режим загрузки для ключа offset"""
        game_names_str = options.get('game_names', '')
        game_modes_str = options.get('game_modes', '')  # НОВЫЙ
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')

        if game_modes_str:
            return 'game_modes'  # НОВЫЙ РЕЖИМ
        elif game_names_str:
            return 'game_names'
        elif genres_str and description_contains:
            return 'genres_and_description'
        elif genres_str:
            return 'genres'
        elif description_contains:
            return 'description'
        elif keywords_str:
            return 'keywords'
        else:
            return 'popular'

    def _get_query_key_for_current_command(self, options, where_clause):
        """Создает ключ запроса для текущей команды"""
        from .offset_manager import OffsetManager

        params = {
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': self._get_loading_mode(options),
        }

        return OffsetManager.get_query_key(where_clause, **params)