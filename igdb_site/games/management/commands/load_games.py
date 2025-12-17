# games/management/commands/load_games.py

from django.core.management.base import BaseCommand
from django.utils import timezone
from games.igdb_api import make_igdb_request
from games.models import Game
from .load_igdb.data_collector import DataCollector
import sys
import os
import time

# В начало файла добавляем импорт
try:
    from .load_igdb.offset_manager import OffsetManager
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
        self.is_tty = False  # По умолчанию

    def update(self, total_games=None, total_loaded=None, current_iteration=None,
               iterations_without_new=None):
        """Обновляет прогресс - должен быть переопределен в наследниках"""
        raise NotImplementedError

    def clear(self):
        """Очищает прогресс-бар"""
        pass

    def final_message(self, message):
        """Выводит финальное сообщение"""
        self.stdout.write('\n' + message + '\n')


class TopProgressBar(BaseProgressBar):
    """Отображает прогресс вверху терминала с ANSI escape codes"""

    def __init__(self, stdout, total_games=0, total_loaded=0):
        super().__init__(stdout, total_games, total_loaded)
        self.is_tty = sys.stdout.isatty() and self._supports_ansi()

    def _supports_ansi(self):
        """Проверяет поддержку ANSI escape codes"""
        if os.name == 'nt':
            # Windows - проверяем версию
            return os.environ.get('TERM') == 'xterm' or \
                os.environ.get('WT_SESSION') is not None
        else:
            return True

    def update(self, total_games=None, total_loaded=None, current_iteration=None,
               iterations_without_new=None):
        """Обновляет прогресс"""
        if total_games is not None:
            self.total_games = total_games
        if total_loaded is not None:
            self.total_loaded = total_loaded
        if current_iteration is not None:
            self.current_iteration = current_iteration
        if iterations_without_new is not None:
            self.iterations_without_new = iterations_without_new

        self._display()

    def _display(self):
        """Отображает прогресс вверху терминала"""
        if not self.is_tty:
            return

        # Сохраняем позицию курсора
        sys.stdout.write('\x1b[s')

        # Перемещаемся в начало
        sys.stdout.write('\x1b[1;1H')

        # Очищаем строку
        sys.stdout.write('\x1b[2K')

        # Формируем строку прогресса
        if self.total_games > 0:
            percentage = (self.total_loaded / self.total_games * 100) if self.total_games > 0 else 0
            progress_bar = self._create_progress_bar(percentage, 30)
            progress_text = f"📊 Прогресс: {self.total_loaded}/{self.total_games} ({percentage:.1f}%) {progress_bar}"
        else:
            progress_text = f"📊 Загружено: {self.total_loaded} игр"

        # Добавляем информацию об итерациях
        if self.iterations_without_new > 0:
            progress_text += f" | 🚫 Итераций без новых игр: {self.iterations_without_new}"

        if self.current_iteration > 0:
            progress_text += f" | 🔄 Итерация: {self.current_iteration}"

        # Выводим прогресс
        sys.stdout.write(progress_text)

        # Восстанавливаем позицию курсора
        sys.stdout.write('\x1b[u')
        sys.stdout.flush()

    def _create_progress_bar(self, percentage, width):
        """Создает текстовый прогресс-бар"""
        filled = int(width * percentage / 100)
        bar = '█' * filled + '░' * (width - filled)
        return f"[{bar}]"

    def clear(self):
        """Очищает прогресс-бар"""
        if not self.is_tty:
            return

        sys.stdout.write('\x1b[s')
        sys.stdout.write('\x1b[1;1H')
        sys.stdout.write('\x1b[2K')
        sys.stdout.write('\x1b[u')
        sys.stdout.flush()

    def final_message(self, message):
        """Выводит финальное сообщение"""
        if not self.is_tty:
            self.stdout.write('\n' + message + '\n')
            return

        sys.stdout.write('\x1b[s')
        sys.stdout.write('\x1b[1;1H')
        sys.stdout.write('\x1b[2K')
        sys.stdout.write(message)
        sys.stdout.write('\x1b[u')
        sys.stdout.write('\n')
        sys.stdout.flush()


class SimpleProgressBar(BaseProgressBar):
    """Простой прогресс-бар для PowerShell и других без ANSI поддержки"""

    def __init__(self, stdout, total_games=0, total_loaded=0):
        super().__init__(stdout, total_games, total_loaded)
        self.last_update_time = time.time()
        self.update_interval = 5  # Обновлять каждые 5 секунд

    def update(self, total_games=None, total_loaded=None, current_iteration=None,
               iterations_without_new=None):
        """Обновляет прогресс"""
        if total_games is not None:
            self.total_games = total_games
        if total_loaded is not None:
            self.total_loaded = total_loaded
        if current_iteration is not None:
            self.current_iteration = current_iteration
        if iterations_without_new is not None:
            self.iterations_without_new = iterations_without_new

        # Обновляем только каждые N секунд
        current_time = time.time()
        if current_time - self.last_update_time >= self.update_interval:
            self._display()
            self.last_update_time = current_time

    def _display(self):
        """Отображает прогресс"""
        if self.total_games > 0:
            percentage = (self.total_loaded / self.total_games * 100) if self.total_games > 0 else 0
            progress_text = f"\n📊 Прогресс: {self.total_loaded}/{self.total_games} ({percentage:.1f}%)"
        else:
            progress_text = f"\n📊 Загружено: {self.total_loaded} игр"

        # Добавляем информацию об итерациях
        if self.iterations_without_new > 0:
            progress_text += f" | 🚫 Итераций без новых игр: {self.iterations_without_new}"

        if self.current_iteration > 0:
            progress_text += f" | 🔄 Итерация: {self.current_iteration}"

        # Выводим прогресс
        self.stdout.write(progress_text)
        self.stdout.flush()


class BaseGamesCommand(BaseCommand):
    """Базовый класс для команд загрузки IGDB"""

    DEFAULT_ITERATION_LIMIT = 100  # По умолчанию 100 игр за итерацию

    def __init__(self):
        super().__init__()
        # Инициализация GameCacheManager
        try:
            from .load_igdb.game_cache import GameCacheManager
            self.cache_manager = GameCacheManager
        except ImportError:
            # Fallback если файл не найден
            class GameCacheManager:
                @staticmethod
                def clear_cache():
                    return True

            self.cache_manager = GameCacheManager

    def add_arguments(self, parser):
        """Общие аргументы для всех команд"""
        parser.add_argument('--genres', type=str, default='',
                            help='Загружать игры с указанными жанрами (логика И между жанрами). Формат: "Жанр1,Жанр2,Жанр3"')
        parser.add_argument('--description-contains', type=str, default='',
                            help='Загружать игры с указанным текстом в описании или названии')
        parser.add_argument('--overwrite', action='store_true',
                            help='Удалить существующие игры и загрузить заново')
        parser.add_argument('--debug', action='store_true',
                            help='Включить режим отладки')
        parser.add_argument('--limit', type=int, default=0,
                            help='Общий лимит загружаемых игр (0 - без общего лимита)')
        parser.add_argument('--offset', type=int, default=0,
                            help='Пропустить указанное количество игр из результатов поиска. Если 0 - использует сохраненный offset')
        parser.add_argument('--min-rating-count', type=int, default=0,
                            help='Минимальное количество оценок для фильтрации (0 - без фильтра)')
        parser.add_argument('--keywords', type=str, default='',
                            help='Загружать игры с указанными ключевыми словами (логика И). Формат: "word1,word2,word3"')
        parser.add_argument('--count-only', action='store_true',
                            help='Только подсчитать количество НОВЫХ игр (которых нет в базе) без сохранения')
        parser.add_argument('--repeat', type=int, default=0,
                            help='Количество повторений (0 = бесконечно до исчерпания игр, -1 = только один раз)')
        parser.add_argument('--game-types', type=str, default='0,1,2,4,5,8,9,10,11',
                            help='Типы игр для загрузки (через запятую). По умолчанию: 0,1,2,4,5,8,9,10,11')
        parser.add_argument('--iteration-limit', type=int, default=self.DEFAULT_ITERATION_LIMIT,
                            help=f'Количество игр за одну итерацию (по умолчанию: {self.DEFAULT_ITERATION_LIMIT})')
        parser.add_argument('--clear-cache', action='store_true',
                            help='Очистить кэш проверенных игр перед началом')
        parser.add_argument('--reset-offset', action='store_true',
                            help='Сбросить сохраненный offset и начать с начала')

    def handle(self, *args, **options):
        """Основной метод выполнения команды"""
        # Инициализация параметров
        repeat_count = options['repeat']
        original_offset = options['offset']
        limit = options['limit']
        iteration_limit = options['iteration_limit']
        debug = options['debug']
        overwrite = options['overwrite']
        count_only = options['count_only']
        clear_cache = options.get('clear_cache', False)
        reset_offset = options.get('reset_offset', False)

        # Сохраняем режим отладки как атрибут
        self.debug_mode = debug

        # Очищаем кэш если нужно
        if clear_cache:
            self.clear_game_cache()

        # Сбрасываем offset если нужно
        if reset_offset:
            self._handle_reset_offset(options, debug)

        # Определяем режим выполнения
        try:
            execution_mode = self._determine_execution_mode(repeat_count)
        except ValueError as e:
            self.stderr.write(str(e))
            return

        # Инициализируем прогресс-бар
        progress_bar = self._create_progress_bar()

        # Если есть общий лимит, показываем его как цель
        if limit > 0:
            progress_bar.total_games = limit
            progress_bar.update()

        # Выводим информацию о запуске
        self._display_startup_info(execution_mode, iteration_limit, limit)

        # Инициализация статистики
        current_offset = original_offset

        # 🔴 ЗАГРУЖАЕМ СОХРАНЕННЫЙ OFFSET ЕСЛИ НЕ УКАЗАН ЯВНО И НЕ СБРАСЫВАЕМ
        if original_offset == 0 and not reset_offset:
            saved_offset = self._get_saved_offset(options)
            if saved_offset is not None:
                current_offset = saved_offset
                self.stdout.write(f'📍 Начинаем с сохраненного offset: {current_offset}')

        self.max_consecutive_no_new_games = 3
        total_stats = self._initialize_total_stats(original_offset)

        # Если это TTY терминал, оставляем место для прогресс-бара
        if (hasattr(progress_bar, 'is_tty') and progress_bar.is_tty and not count_only) or \
                (isinstance(progress_bar, TopProgressBar) and not count_only):
            self.stdout.write('\n' * 2)  # Оставляем 2 пустые строки для прогресс-бара

        # ОСНОВНОЙ ЦИКЛ ИТЕРАЦИЙ
        iteration = 1
        try:
            while True:
                # Выполняем одну итерацию
                should_continue, current_offset, total_stats = self._execute_single_iteration(
                    iteration, current_offset, total_stats, execution_mode,
                    limit, iteration_limit, options, progress_bar, args
                )

                if not should_continue:
                    break

                # Пауза между итерациями
                if iteration < execution_mode['repeat_count'] or execution_mode['infinite_mode']:
                    pause_time = 2
                    if self.debug_mode:
                        self.stdout.write(f'   ⏸️  Пауза {pause_time} секунд...')
                    time.sleep(pause_time)

                iteration += 1

        except KeyboardInterrupt:
            # Глобальное прерывание команды
            self._handle_global_interrupt(total_stats, execution_mode,
                                          original_offset, current_offset,
                                          limit, progress_bar)
            # 🔴 СОХРАНЯЕМ OFFSET ПРИ ПРЕРЫВАНИИ
            if not reset_offset:  # Не сохраняем если был сброс
                self._save_offset_for_continuation(options, current_offset)
            return total_stats

        # ФИНАЛЬНЫЙ ЭТАП
        self._finalize_execution(total_stats, limit, progress_bar,
                                 execution_mode, original_offset,
                                 current_offset, limit, overwrite)

        # Итоговый статус
        self._display_final_status(total_stats, limit)

        return total_stats

    def _get_saved_offset(self, options):
        """Получает сохраненный offset для текущих параметров"""
        from .load_igdb.offset_manager import OffsetManager

        # Получаем where_clause
        where_clause = self._get_where_clause_for_current_command(options)
        if not where_clause:
            return None

        # Получаем ключ запроса
        query_key = self._get_query_key_for_current_command(options, where_clause)

        # Загружаем сохраненный offset
        return OffsetManager.load_offset(query_key)

    def _save_offset_for_continuation(self, options, current_offset):
        """Сохраняет offset для продолжения"""
        from .load_igdb.offset_manager import OffsetManager

        # Получаем where_clause
        where_clause = self._get_where_clause_for_current_command(options)
        if not where_clause:
            return False

        # Получаем ключ запроса
        query_key = self._get_query_key_for_current_command(options, where_clause)

        # Сохраняем offset
        saved = OffsetManager.save_offset(query_key, current_offset)

        if saved and options.get('debug', False):
            self.stdout.write(f'   💾 Сохранен offset для продолжения: {current_offset}')

        return saved

    def _handle_reset_offset(self, options, debug):
        """Обрабатывает сброс сохраненного offset"""
        from .load_igdb.offset_manager import OffsetManager

        # Получаем where_clause
        where_clause = self._get_where_clause_for_current_command(options)
        if not where_clause:
            if debug:
                self.stdout.write('⚠️  Не удалось определить запрос для сброса offset')
            return

        # Получаем ключ запроса
        query_key = self._get_query_key_for_current_command(options, where_clause)

        # Сбрасываем offset
        cleared = OffsetManager.clear_offset(query_key)

        if cleared:
            self.stdout.write('🔄 Сброшен сохраненный offset для этого запроса')
            if debug:
                self.stdout.write(f'   🔑 Ключ запроса: {query_key}')
        else:
            self.stdout.write('⚠️  Не удалось сбросить offset')

    def _get_query_key_for_current_command(self, options, where_clause):
        """Создает ключ запроса для текущей команды"""
        from .load_igdb.offset_manager import OffsetManager

        # Параметры для ключа
        params = {
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': self._get_loading_mode(options),  # Режим загрузки
        }

        return OffsetManager.get_query_key(where_clause, **params)

    def _get_loading_mode(self, options):
        """Определяет режим загрузки для ключа offset"""
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')

        if genres_str and description_contains:
            return 'genres_and_description'
        elif genres_str:
            return 'genres'
        elif description_contains:
            return 'description'
        elif keywords_str:
            return 'keywords'
        else:
            return 'popular'

    def _get_where_clause_for_current_command(self, options):
        """Получает where_clause для текущей команды"""
        # Определяем тип загрузки и формируем where_clause
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')
        game_types_str = options.get('game_types', '')
        min_rating_count = options.get('min_rating_count', 0)

        where_parts = []

        # Определяем режим загрузки
        if genres_str and description_contains:
            # Режим: жанры И текст в описании
            # Упрощенно, без реальных ID жанров
            where_parts.append('genres = (...)')  # Заполнитель
            where_parts.append(f'(name ~ *"{description_contains}"* | summary ~ *"{description_contains}"*)')
        elif genres_str:
            # Режим: только жанры
            where_parts.append('genres = (...)')  # Заполнитель
        elif description_contains:
            # Режим: только текст в описании
            where_parts.append(f'(name ~ *"{description_contains}"* | summary ~ *"{description_contains}"*)')
        elif keywords_str:
            # Режим: ключевые слова
            where_parts.append('keywords = (...)')  # Заполнитель
        else:
            # Режим: популярные игры
            where_parts.append('rating_count > 0')
            where_parts.append('name != null')

        # Добавляем общие фильтры
        if min_rating_count > 0:
            where_parts.append(f'rating_count >= {min_rating_count}')
        else:
            where_parts.append('rating_count > 0')

        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_parts.append(f'game_type = ({game_types_str_query})')
            except ValueError:
                pass
        else:
            # Значение по умолчанию
            where_parts.append('game_type = (0,1,2,4,5,8,9,10,11)')

        return ' & '.join(where_parts)

    def _load_saved_offset(self, options, where_clause, current_offset):
        """Загружает сохраненный offset если текущий равен 0"""
        if current_offset > 0:
            return current_offset  # Используем указанный пользователем

        if not where_clause:
            return current_offset

        query_key = self._get_query_key_for_current_command(options, where_clause)
        saved_offset = OffsetManager.load_offset(query_key)

        if saved_offset is not None:
            if options.get('debug', False):
                self.stdout.write(f'   📍 Загружен сохраненный offset: {saved_offset}')
            return saved_offset

        return current_offset

    def _handle_iteration_error(self, e, iteration, execution_mode, total_stats, progress_bar):
        """Обрабатывает ошибки в итерации"""
        # 1. Проверка типа ошибки
        if self._is_keyboard_interrupt(e):
            return self._handle_keyboard_interrupt_error(e, iteration, total_stats, progress_bar)

        # 2. Обработка обычных ошибок
        return self._handle_regular_error(e, iteration, execution_mode, total_stats, progress_bar)

    def _is_keyboard_interrupt(self, error):
        """Проверяет, является ли ошибка KeyboardInterrupt"""
        return isinstance(error, KeyboardInterrupt)

    def _handle_keyboard_interrupt_error(self, error, iteration, total_stats, progress_bar):
        """Обрабатывает ошибку KeyboardInterrupt"""
        self.stdout.write(f'\n⚠️  ПРЕРЫВАНИЕ КОМАНДЫ (Ctrl+C) в итерации {iteration} - завершение...')
        total_stats['interrupted'] = True

        # Обновляем прогресс-бар
        if progress_bar:
            progress_bar.final_message("🛑 КОМАНДА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ")
            progress_bar.clear()

        # Поднимаем исключение чтобы полностью остановить выполнение
        raise error

    def _handle_regular_error(self, error, iteration, execution_mode, total_stats, progress_bar):
        """Обрабатывает обычные ошибки в итерации"""
        # 1. Обновление статистики ошибок
        self._update_error_statistics(total_stats)

        # 2. Вывод информации об ошибке
        self._display_error_details(error, iteration)

        # 3. Определение режима продолжения
        should_continue = self._determine_continuation_mode(
            execution_mode, iteration, total_stats, progress_bar
        )

        return should_continue

    def _update_error_statistics(self, total_stats):
        """Обновляет статистику ошибок"""
        total_stats['errors'] += 1
        total_stats['iterations_with_errors'] += 1

    def _display_error_details(self, error, iteration):
        """Выводит детали ошибки"""
        self.stderr.write(f'❌ ОШИБКА в итерации {iteration}: {str(error)}')
        if self.debug_mode:
            import traceback
            self.stderr.write(f'📋 Трассировка ошибки:')
            self.stderr.write(traceback.format_exc())

    def _determine_continuation_mode(self, execution_mode, iteration, total_stats, progress_bar):
        """Определяет режим продолжения после ошибки"""
        infinite_mode = execution_mode['infinite_mode']
        finite_mode = execution_mode['finite_mode']
        repeat_count = execution_mode['repeat_count']

        # Бесконечный режим
        if infinite_mode:
            return self._handle_error_in_infinite_mode(
                iteration, total_stats, progress_bar
            )

        # Конечный режим
        elif finite_mode and iteration < repeat_count:
            return self._handle_error_in_finite_mode(iteration, total_stats)

        # Режим одного раза или последняя итерация
        else:
            return self._handle_error_in_single_or_last_iteration(iteration, total_stats)

    def _handle_error_in_infinite_mode(self, iteration, total_stats, progress_bar):
        """Обрабатывает ошибку в бесконечном режиме"""
        self.stdout.write(f'   ⏩ Пропускаем итерацию {iteration} из-за ошибки')
        total_stats['iterations_with_no_new_games'] += 1
        total_stats['iterations'] += 1

        # Обновляем прогресс-бар
        if progress_bar:
            progress_bar.update(
                total_loaded=total_stats['total_games_created'],
                current_iteration=iteration,
                iterations_without_new=total_stats['iterations_with_no_new_games']
            )

        return True  # Продолжаем выполнение

    def _handle_error_in_finite_mode(self, iteration, total_stats):
        """Обрабатывает ошибку в конечном режиме"""
        self.stdout.write(f'   ⏩ Пропускаем итерацию {iteration} из-за ошибки')
        total_stats['iterations'] += 1

        return True  # Продолжаем выполнение

    def _handle_error_in_single_or_last_iteration(self, iteration, total_stats):
        """Обрабатывает ошибку в режиме одного раза или последней итерации"""
        self.stdout.write(f'   ⚠️  Ошибка в итерации {iteration} - завершение выполнения')
        total_stats['iterations'] += 1

        return False  # Не продолжаем выполнение



    def _finalize_execution(self, total_stats, limit, progress_bar,
                            execution_mode, original_offset,
                            current_offset, limit_val, overwrite):
        """Завершает выполнение команды"""
        # Финальное сообщение в прогресс-баре
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

            # Очищаем прогресс-бар
            progress_bar.clear()

        # Выводим финальную статистику
        self._display_final_statistics(
            total_stats, execution_mode, original_offset,
            current_offset, limit_val, overwrite
        )

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

    def _handle_global_interrupt(self, total_stats, execution_mode,
                                 original_offset, current_offset,
                                 limit, progress_bar):
        """Обрабатывает глобальное прерывание команды (Ctrl+C)"""
        self.stdout.write('\n\n🛑 КОМАНДА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')

        # Обновляем прогресс-бар
        if progress_bar:
            progress_bar.final_message("🛑 ВЫПОЛНЕНИЕ КОМАНДЫ ПРЕРВАНО")
            progress_bar.clear()

        # Выводим статистику прерывания
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
        self.stdout.write(f'⏭️  Всего пропущено игр: {total_stats["total_games_skipped"]}')
        self.stdout.write(f'❌ Ошибок: {total_stats["errors"]}')

        if limit > 0:
            self.stdout.write(f'🎯 Общий лимит игр: {limit} (загружено: {total_stats["total_games_created"]})')

        self.stdout.write(f'⏱️  Общее время: {total_stats["total_time"]:.2f}с')

        # Инструкция для продолжения
        if current_offset > original_offset:
            self.stdout.write('\n📋 ДЛЯ ПРОДОЛЖЕНИЯ:')
            self.stdout.write(f'   Используйте --offset {current_offset}')
            self.stdout.write(f'   Пример: python manage.py load_games --offset {current_offset} ...')

    def _should_continue_iteration(self, iteration, execution_mode, total_stats, limit, max_consecutive_no_new_games):
        """Проверяет, следует ли продолжать выполнение"""
        infinite_mode = execution_mode['infinite_mode']
        single_run_mode = execution_mode['single_run_mode']
        finite_mode = execution_mode['finite_mode']
        repeat_count = execution_mode['repeat_count']

        # Проверяем условия остановки для бесконечного режима
        if infinite_mode and iteration > 1:
            # Проверяем, не закончились ли игры
            if total_stats['iterations_with_no_new_games'] >= max_consecutive_no_new_games:
                self.stdout.write(f'\n⚠️  ОСТАНОВКА: {max_consecutive_no_new_games} итераций подряд без новых игр')
                return False

            # Если есть лимит на игры и он достигнут
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

    def _calculate_iteration_limit(self, limit, iteration_limit, total_stats):
        """Рассчитывает лимит для текущей итерации"""
        if limit > 0:
            # Рассчитываем, сколько игр еще нужно загрузить
            remaining_limit = limit - total_stats['total_games_created']
            if remaining_limit <= 0:
                return 0, False  # Лимит достигнут

            # Используем iteration_limit, но не больше remaining_limit
            iteration_limit_actual = min(iteration_limit, remaining_limit)
            return iteration_limit_actual, True
        else:
            # Если нет общего лимита, используем iteration_limit
            return iteration_limit, True


    def _execute_single_iteration(self, iteration, current_offset, total_stats, execution_mode,
                                  limit, iteration_limit, options, progress_bar, args):
        """Выполняет одну итерацию загрузки"""
        # Проверяем, следует ли продолжать
        should_continue = self._should_continue_iteration(
            iteration, execution_mode, total_stats, limit, self.max_consecutive_no_new_games
        )

        if not should_continue:
            return False, current_offset, total_stats

        # Выводим информацию о текущей итерации
        if execution_mode['repeat_count'] > 1 or execution_mode['infinite_mode']:
            self.stdout.write(f'\n🌀 ИТЕРАЦИЯ {iteration}')
            if execution_mode['infinite_mode']:
                self.stdout.write(
                    f'🌀 (бесконечный режим, итераций без новых игр: {total_stats["iterations_with_no_new_games"]}/{self.max_consecutive_no_new_games})')
            self.stdout.write('=' * 40)

            if current_offset > options['offset']:
                self.stdout.write(f'📊 Начинаем с offset: {current_offset}')

        # Рассчитываем лимит для этой итерации
        iteration_limit_actual, can_continue = self._calculate_iteration_limit(
            limit, iteration_limit, total_stats
        )

        if not can_continue or iteration_limit_actual <= 0:
            self.stdout.write(f'\n✅ ДОСТИГНУТ ОБЩИЙ ЛИМИТ: {limit} игр загружено')
            return False, current_offset, total_stats

        self.stdout.write(f'🎯 Цель итерации: найти {iteration_limit_actual} новых игр')
        if limit > 0:
            remaining_limit = limit - total_stats['total_games_created']
            self.stdout.write(f'   (осталось до лимита: {remaining_limit})')

        try:
            # Выполняем итерацию
            iteration_result = self._process_single_iteration(
                iteration, current_offset, iteration_limit_actual,
                options, execution_mode, progress_bar, *args
            )

            if iteration_result['success']:
                # Обновляем общую статистику
                current_offset = self._update_total_stats(
                    total_stats, iteration_result['stats'], iteration,
                    current_offset, execution_mode, progress_bar
                )
            else:
                # Если итерация не вернула результат
                if self.debug_mode:
                    self.stdout.write(f'   ⚠️  Итерация {iteration} не вернула результат')
                total_stats['iterations'] += 1
                total_stats['iterations_with_no_new_games'] += 1

                # Обновляем прогресс-бар
                if progress_bar:
                    progress_bar.update(
                        total_loaded=total_stats['total_games_created'],
                        current_iteration=iteration,
                        iterations_without_new=total_stats['iterations_with_no_new_games']
                    )

        except KeyboardInterrupt:
            # Пробрасываем прерывание наверх для глобальной обработки
            raise

        except Exception as e:
            # Обработка ошибок в итерации
            self._handle_iteration_error(
                e, iteration, execution_mode, total_stats, progress_bar
            )
            # После обработки ошибки продолжаем с следующей итерации
            return True, current_offset, total_stats

        return True, current_offset, total_stats

    def _create_progress_bar(self):
        """Создает подходящий прогресс-бар для текущего терминала"""
        import os
        import sys

        # Проверяем поддержку ANSI
        supports_ansi = False
        if hasattr(sys.stdout, 'isatty') and sys.stdout.isatty():
            if os.name == 'nt':
                # Windows - проверяем Windows Terminal
                supports_ansi = os.environ.get('TERM') == 'xterm' or \
                                os.environ.get('WT_SESSION') is not None or \
                                os.environ.get('ANSICON') is not None
            else:
                # Linux/macOS
                supports_ansi = True

        if supports_ansi:
            return TopProgressBar(self.stdout)
        else:
            return SimpleProgressBar(self.stdout)

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

    def _initialize_total_stats(self, original_offset):
        """Инициализирует общую статистику"""
        return {
            'iterations': 0,
            'total_games_found': 0,
            'total_games_checked': 0,
            'total_games_created': 0,
            'total_games_skipped': 0,
            'total_time': 0,
            'last_checked_offset': original_offset,
            'errors': 0,
            'iterations_with_errors': 0,
            'iterations_with_limit_reached': 0,
            'iterations_with_no_new_games': 0,
            'max_iterations_reached': False,
            'interrupted': False,
        }

    def handle_single_iteration(self, *args, **options):
        """Обработка одной итерации команды"""
        import time

        # 1. Подготовка и инициализация
        params, iteration_params = self._prepare_iteration_parameters(options)

        # 2. Настройка информации об итерации
        iteration_info = self._setup_iteration_information(options, iteration_params)

        # 3. Отображение заголовка и получение параметров
        header_result = self._display_and_get_iteration_header(params, iteration_info)
        actual_offset = header_result['actual_offset']
        actual_limit = header_result['actual_limit']

        # 4. Определение режимов
        skip_existing = self._determine_skip_mode(params)
        debug = params['debug']
        errors = 0
        iteration_start_time = time.time()

        # 5. Загрузка игр
        result = self._load_games_for_iteration(params, actual_limit, actual_offset, skip_existing, debug)

        # 6. Обработка результатов загрузки
        if result is None:
            return self._handle_failed_loading(iteration_start_time, errors, actual_offset)

        # 7. Проверка наличия игр
        if not result.get('new_games'):
            return self._handle_empty_results(result, errors, params, actual_offset, iteration_start_time)

        # 8. Обработка режима count-only
        if params['count_only']:
            return self._handle_count_only_mode(result, errors, iteration_start_time, actual_offset)

        # 9. Обработка данных игр
        result_stats, errors = self._process_game_data_for_iteration(
            result, params, iteration_start_time, errors
        )

        # 10. Подготовка финальной статистики
        final_stats = self._prepare_final_iteration_stats(
            result, result_stats, actual_offset, actual_limit,
            errors, iteration_info, params, iteration_start_time
        )

        # 11. Отображение статистики итерации
        return self._display_iteration_statistics_complete(
            final_stats, result, actual_offset, actual_limit,
            params, iteration_info, errors, result_stats
        )

    def _prepare_iteration_parameters(self, options):
        """Подготавливает параметры для итерации"""
        params = self._get_execution_parameters(options)

        iteration_offset = options.get('iteration_offset', params['offset'])
        iteration_limit_actual = options.get('iteration_limit', params['iteration_limit'])

        return params, {
            'iteration_offset': iteration_offset,
            'iteration_limit_actual': iteration_limit_actual
        }

    def _setup_iteration_information(self, options, iteration_params):
        """Настраивает информацию об итерации"""
        iteration_number = options.get('_iteration_number', 1)
        repeat_count = options.get('repeat', 1)

        return {
            'iteration_number': iteration_number,
            'repeat_count': repeat_count,
            'iteration_offset': iteration_params['iteration_offset'],
            'iteration_limit_actual': iteration_params['iteration_limit_actual'],
        }

    def _display_and_get_iteration_header(self, params, iteration_info):
        """Отображает заголовок итерации и возвращает параметры"""
        self.stdout.write('🎮 ЗАГРУЗКА ИГР ИЗ IGDB')
        self.stdout.write('=' * 60)

        # Определяем тип загрузки
        self._display_loading_type(params)

        # Информация об итерации
        self._display_iteration_info(params, iteration_info)

        # Используем offset и limit для этой конкретной итерации
        actual_offset = iteration_info['iteration_offset']
        actual_limit = iteration_info['iteration_limit_actual']

        return {
            'actual_offset': actual_offset,
            'actual_limit': actual_limit,
        }

    def _display_loading_type(self, params):
        """Отображает тип загрузки"""
        genres_str = params['genres_str']
        description_contains = params['description_contains']
        keywords_str = params['keywords_str']
        game_types_str = params['game_types_str']

        if params['count_only']:
            self.stdout.write('🔢 РЕЖИМ: ПОДСЧЕТ НОВЫХ ИГР (которых нет в базе)')
            self.stdout.write('⚠️  Игры не будут сохранены в базу данных!')

        if genres_str and description_contains:
            genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]
            self.stdout.write(
                f'🎭📝 РЕЖИМ: Игры со всеми жанрами ({len(genre_list)}) И текстом "{description_contains}" в описании/названии')
            self.stdout.write(f'   🎭 Жанры: {", ".join(genre_list)}')
        elif genres_str:
            genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]
            self.stdout.write(
                f'🎭 РЕЖИМ: Игры со всеми жанрами ({len(genre_list)}): {", ".join(genre_list)}')
        elif description_contains:
            self.stdout.write(f'📝 РЕЖИМ: Игры с текстом "{description_contains}" в описании/названии')
        elif keywords_str:
            keyword_list = [k.strip() for k in keywords_str.split(',') if k.strip()]
            self.stdout.write(
                f'🔑 РЕЖИМ: Игры с ключевыми словами ({len(keyword_list)} слов): {", ".join(keyword_list)}')
        else:
            self.stdout.write('📊 РЕЖИМ: Все популярные игры')

    def _display_iteration_info(self, params, iteration_info):
        """Отображает информацию об итерации"""
        game_types_str = params['game_types_str']
        iteration_number = iteration_info['iteration_number']
        repeat_count = iteration_info['repeat_count']
        actual_limit = iteration_info['iteration_limit_actual']
        actual_offset = iteration_info['iteration_offset']

        # Показываем информацию о типах игр
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                self.stdout.write(f'🎮 ФИЛЬТР ПО ТИПАМ ИГР: {game_types}')
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        if repeat_count > 1:
            self.stdout.write(f'🔄 Итерация {iteration_number}/{repeat_count}')

        if actual_limit > 0:
            self.stdout.write(f'📊 ЛИМИТ ИТЕРАЦИИ: {actual_limit} НОВЫХ игр')
        else:
            self.stdout.write(f'📊 ИТЕРАЦИЯ: загрузка без лимита')

        if actual_offset > 0:
            self.stdout.write(f'⏭️  OFFSET: начинаем с позиции {actual_offset} в результатах поиска')

        if params['min_rating_count'] > 0:
            self.stdout.write(f'⭐ ФИЛЬТР: игры с не менее {params["min_rating_count"]} оценками')

        if params['overwrite'] and not params['count_only']:
            self.stdout.write('🔄 OVERWRITE: найденные игры будут удалены и загружены заново')

        if params['count_only'] and params['overwrite']:
            self.stdout.write('⚠️  Overwrite игнорируется в режиме count-only')

        if params['debug']:
            self.stdout.write('🐛 РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН')
            self.stdout.write('-' * 40)

    def _determine_skip_mode(self, params):
        """Определяет режим пропуска существующих игр"""
        if params['overwrite']:
            return False  # Не пропускаем существующие, перезаписываем их
        else:
            return True  # Обычный режим: пропускаем существующие игры

    def _load_games_for_iteration(self, params, actual_limit, actual_offset, skip_existing, debug):
        """Загружает игры для итерации"""
        try:
            # Просто вызываем существующие методы
            if params['genres_str'] and params['description_contains']:
                return self.load_games_by_genres_and_description(
                    params['genres_str'], params['description_contains'], debug,
                    actual_limit, actual_offset, params['min_rating_count'],
                    skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str']:
                return self.load_games_by_genres(
                    params['genres_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'],
                    params['game_types_str']
                )
            elif params['description_contains']:
                return self.load_games_by_description(
                    params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'],
                    params['game_types_str']
                )
            elif params['keywords_str']:
                return self.load_games_by_keywords(
                    params['keywords_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'],
                    params['game_types_str']
                )
            else:
                return self.load_all_popular_games(
                    debug, actual_limit, actual_offset, params['min_rating_count'],
                    skip_existing, params['count_only'], params['game_types_str']
                )
        except Exception as e:
            self.stderr.write(f'❌ ОШИБКА при загрузке игр: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            return None

    def _simplified_get_where_clause(self, params):
        """Упрощенное получение where_clause из параметров"""
        # Эта функция формирует where_clause похожий на тот, что формируется в методах загрузки
        genres_str = params.get('genres_str', '')
        description_contains = params.get('description_contains', '')
        keywords_str = params.get('keywords_str', '')
        game_types_str = params.get('game_types_str', '0,1,2,4,5,8,9,10,11')
        min_rating_count = params.get('min_rating_count', 0)

        where_parts = []

        # Базовая логика (упрощенная)
        if genres_str:
            where_parts.append('genres = (...)')  # Заполнитель
        if description_contains:
            where_parts.append(f'(name ~ *"{description_contains}"* | summary ~ *"{description_contains}"*)')
        if keywords_str:
            where_parts.append('keywords = (...)')  # Заполнитель

        # Обязательные условия
        where_parts.append('rating_count > 0')
        where_parts.append('name != null')

        if min_rating_count > 0:
            where_parts.append(f'rating_count >= {min_rating_count}')

        if game_types_str and game_types_str != '0,1,2,4,5,8,9,10,11':
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_parts.append(f'game_type = ({game_types_str_query})')
            except ValueError:
                pass

        return ' & '.join(where_parts) if where_parts else 'rating_count > 0 & name != null'

    def _get_where_clause_for_params(self, params):
        """Получает where_clause для параметров"""
        # Эта логика должна соответствовать _get_where_clause_for_current_command
        # но использовать params вместо options

        genres_str = params['genres_str']
        description_contains = params['description_contains']
        keywords_str = params['keywords_str']
        game_types_str = params['game_types_str']
        min_rating_count = params['min_rating_count']

        where_parts = []

        # ... та же логика формирования where_clause ...

        if where_parts:
            return ' & '.join(where_parts)
        else:
            return 'rating_count > 0 & name != null'

    def _handle_failed_loading(self, iteration_start_time, errors, actual_offset):
        """Обрабатывает неудачную загрузку"""
        import time
        iteration_time = time.time() - iteration_start_time
        self.stdout.write('❌ Ошибка при загрузке игр, результат None')

        return {
            'total_games_found': 0,
            'total_games_checked': 0,
            'created_count': 0,
            'skipped_count': 0,
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': actual_offset,
            'limit_reached': False,
            'limit_reached_at_offset': None,
        }

    def _handle_empty_results(self, result, errors, params, actual_offset, iteration_start_time):
        """Обрабатывает пустые результаты"""
        import time
        iteration_time = time.time() - iteration_start_time

        if result and result.get('total_games_checked', 0) > 0:
            if params['overwrite']:
                self.stdout.write(f'ℹ️  Найдено {result.get("total_games_checked", 0)} игр для перезаписи')
            else:
                self.stdout.write(
                    f'❌ Найдено {result.get("total_games_checked", 0)} игр, но все они уже есть в базе')
        else:
            if errors == 0:
                self.stdout.write('❌ Не найдено игр для загрузки')

        # Получаем последний проверенный offset
        last_checked = result.get('last_checked_offset', actual_offset) if result else actual_offset

        return {
            'total_games_found': 0,
            'total_games_checked': result.get('total_games_checked', 0) if result else 0,
            'created_count': 0,
            'skipped_count': result.get('existing_games_skipped', 0) if result else 0,
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': last_checked,
            'limit_reached': result.get('limit_reached', False) if result else False,
            'limit_reached_at_offset': result.get('limit_reached_at_offset'),
        }

    def _handle_count_only_mode(self, result, errors, iteration_start_time, actual_offset):
        """Обрабатывает режим только подсчета"""
        import time
        iteration_time = time.time() - iteration_start_time

        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('✅ ПОДСЧЕТ ЗАВЕРШЕН!')
        self.stdout.write(f'🎮 Игр можно загрузить (которых нет в базе): {result["new_games_count"]}')

        if errors > 0:
            self.stdout.write(f'❌ Ошибок при подсчете: {errors}')

        return {
            'total_games_found': result['new_games_count'],
            'total_games_checked': result['total_games_checked'],
            'created_count': 0,
            'skipped_count': result['existing_games_skipped'],
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': result.get('last_checked_offset', actual_offset),
            'limit_reached': result.get('limit_reached', False),
            'limit_reached_at_offset': result.get('limit_reached_at_offset'),
        }

    def _process_game_data_for_iteration(self, result, params, iteration_start_time, errors):
        """Обрабатывает данные игр для итерации"""
        from .load_igdb.data_collector import DataCollector

        # Обработка режима перезаписи
        if params['overwrite'] and result['new_games']:
            try:
                self._handle_overwrite_mode(result['new_games'], params['debug'])
            except Exception as e:
                errors += 1
                self.stderr.write(f'❌ ОШИБКА при удалении игр: {str(e)}')
                if params['debug']:
                    import traceback
                    self.stderr.write(f'📋 Трассировка ошибки:')
                    self.stderr.write(traceback.format_exc())

        # Обработка данных
        result_stats = None
        try:
            collector = DataCollector(self.stdout, self.stderr)
            result_stats = collector.process_all_data_sequentially(result['new_games'], params['debug'])
        except Exception as e:
            errors += 1
            self.stderr.write(f'❌ ОШИБКА при обработке данных: {str(e)}')
            if params['debug']:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            # Создаем пустую статистику при ошибке
            import time
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }

        return result_stats, errors

    def _prepare_final_iteration_stats(self, result, result_stats, actual_offset, actual_limit,
                                       errors, iteration_info, params, iteration_start_time):
        """Подготавливает финальную статистику итерации"""
        import time

        if result_stats:
            result_stats['total_games_checked'] = result['total_games_checked']
            result_stats['total_games_found'] = result['new_games_count']
            result_stats['errors'] = errors
            result_stats['last_checked_offset'] = result.get('last_checked_offset', actual_offset)
            result_stats['limit_reached'] = result.get('limit_reached', False)
            result_stats['limit_reached_at_offset'] = result.get('limit_reached_at_offset')
        else:
            # Если result_stats не создан
            iteration_time = time.time() - iteration_start_time
            result_stats = {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': result['new_games_count'],
                'created_count': 0,
                'skipped_count': 0,
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', actual_offset),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        return result_stats

    def _display_iteration_statistics_complete(self, final_stats, result, actual_offset, actual_limit,
                                               params, iteration_info, errors, result_stats):
        """Отображает полную статистику итерации"""
        all_games = result['new_games']
        total_games_checked = result['total_games_checked']
        new_games_count = result['new_games_count']
        existing_games_skipped = result['existing_games_skipped']
        limit_reached = result.get('limit_reached', False)
        limit_reached_at_offset = result.get('limit_reached_at_offset')

        # Получаем последний проверенный offset
        if limit_reached_at_offset is not None:
            last_checked_offset = limit_reached_at_offset
        else:
            last_checked_offset = result.get('last_checked_offset',
                                             actual_offset + total_games_checked - 1)

        # Вывод основной информации
        self._display_main_iteration_info(params, new_games_count, total_games_checked,
                                          existing_games_skipped, last_checked_offset,
                                          actual_limit, limit_reached)

        # Вывод краткой или подробной статистики
        if not params['debug']:
            self._display_short_iteration_stats(result_stats, iteration_info, errors,
                                                limit_reached, last_checked_offset, params)
        else:
            self._display_detailed_iteration_stats(result_stats, iteration_info, actual_offset,
                                                   last_checked_offset, total_games_checked,
                                                   new_games_count, errors, limit_reached)

        return {
            'total_games_checked': total_games_checked,
            'total_games_found': new_games_count,
            'created_count': result_stats.get('created_count', 0),
            'skipped_count': existing_games_skipped,
            'total_time': result_stats.get('total_time', 0),
            'errors': errors,
            'last_checked_offset': last_checked_offset,
            'limit_reached': limit_reached,
            'limit_reached_at_offset': limit_reached_at_offset,
        }

    def _display_main_iteration_info(self, params, new_games_count, total_games_checked,
                                     existing_games_skipped, last_checked_offset,
                                     actual_limit, limit_reached):
        """Отображает основную информацию об итерации"""
        if params['overwrite']:
            self.stdout.write(f'📥 Найдено игр для перезаписи: {new_games_count}')
        else:
            self.stdout.write(f'📥 Найдено игр для обработки: {new_games_count}')

        self.stdout.write(f'👀 Всего просмотрено игр из IGDB: {total_games_checked}')
        self.stdout.write(f'📍 Последний проверенный offset: {last_checked_offset}')
        self.stdout.write(f'📍 Следующий offset для продолжения: {last_checked_offset + 1}')

        if limit_reached:
            self.stdout.write(f'🎯 Лимит {actual_limit} достигнут на offset {last_checked_offset}')

        if existing_games_skipped > 0 and not params['overwrite']:
            self.stdout.write(f'⏭️  Пропущено существующих игр: {existing_games_skipped}')

    def _display_short_iteration_stats(self, result_stats, iteration_info, errors,
                                       limit_reached, last_checked_offset, params):
        """Отображает краткую статистику итерации"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
        self.stdout.write(f'⏱️  Время: {result_stats["total_time"]:.2f}с')

        if iteration_info['repeat_count'] > 1:
            self.stdout.write(f'🔄 Итерация {iteration_info["iteration_number"]}/{iteration_info["repeat_count"]}')

        if errors > 0:
            self.stdout.write(f'❌ Ошибок в итерации: {errors}')

        if limit_reached:
            self.stdout.write(f'🎯 Лимит достигнут на offset {last_checked_offset}')

        if params['overwrite']:
            self.stdout.write(f'🔄 Перезаписано игр: {result_stats.get("created_count", 0)}')
        else:
            self.stdout.write(f'✅ Загружено игр: {result_stats.get("created_count", 0)}')

    def _display_detailed_iteration_stats(self, result_stats, iteration_info, actual_offset,
                                          last_checked_offset, total_games_checked,
                                          new_games_count, errors, limit_reached):
        """Отображает подробную статистику итерации"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 ПОДРОБНАЯ СТАТИСТИКА ИТЕРАЦИИ')
        self.stdout.write('=' * 60)
        self.stdout.write(f'🔄 Итерация: {iteration_info["iteration_number"]}/{iteration_info["repeat_count"]}')
        self.stdout.write(f'📍 Начальный offset: {actual_offset}')
        self.stdout.write(f'📍 Последний проверенный offset: {last_checked_offset}')
        self.stdout.write(f'📍 Следующий offset: {last_checked_offset + 1}')
        self.stdout.write(f'👀 Просмотрено игр: {total_games_checked}')
        self.stdout.write(f'🎮 Найдено новых: {new_games_count}')
        self.stdout.write(f'✅ Загружено игр: {result_stats.get("created_count", 0)}')
        self.stdout.write(f'❌ Ошибок: {errors}')

        if limit_reached:
            self.stdout.write(f'🎯 Лимит достигнут: ДА (на offset {last_checked_offset})')

        self.stdout.write(f'⏱️  Время: {result_stats.get("total_time", 0):.2f}с')

        if errors > 0:
            self.stdout.write('⚠️  ИТЕРАЦИЯ ЗАВЕРШЕНА С ОШИБКАМИ')
        else:
            self.stdout.write('✅ ИТЕРАЦИЯ ЗАВЕРШЕНА УСПЕШНО')

    def _get_iteration_params(self, options, params):
        """Получает параметры для конкретной итерации"""
        iteration_offset = options.get('iteration_offset', params['offset'])
        iteration_limit_actual = options.get('iteration_limit', params['iteration_limit'])

        # Получаем сохраненный offset если нужно
        if iteration_offset == 0:
            where_clause = self._get_current_where_clause(params)
            if where_clause:
                query_key = self._get_query_key(params, where_clause)
                saved_offset = self._load_last_offset(query_key)
                if saved_offset > 0:
                    iteration_offset = saved_offset
                    self.stdout.write(f'   📍 Продолжаем с сохраненного offset: {saved_offset}')

        return {
            'iteration_offset': iteration_offset,
            'iteration_limit_actual': iteration_limit_actual,
        }

    def _get_current_where_clause(self, params):
        """Получает текущий where_clause на основе параметров"""
        # Этот метод должен возвращать where_clause в зависимости от режима
        # Например: "genres = (12) & game_type = (0)" для RPG игр
        # В реальном коде это будет сложнее
        return None  # Заглушка - в реальном коде нужно реализовать

    def _display_iteration_header(self, params, info):
        """Отображает заголовок итерации"""
        genres_str = params['genres_str']
        description_contains = params['description_contains']
        keywords_str = params['keywords_str']
        game_types_str = params['game_types_str']
        iteration_number = info['iteration_number']
        repeat_count = info['repeat_count']
        iteration_limit_actual = info['iteration_limit_actual']
        iteration_offset = info['iteration_offset']

        self.stdout.write('🎮 ЗАГРУЗКА ИГР ИЗ IGDB')
        self.stdout.write('=' * 60)

        if params['count_only']:
            self.stdout.write('🔢 РЕЖИМ: ПОДСЧЕТ НОВЫХ ИГР (которых нет в базе)')
            self.stdout.write('⚠️  Игры не будут сохранены в базу данных!')

        # Определяем тип загрузки
        if genres_str and description_contains:
            genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]
            self.stdout.write(
                f'🎭📝 РЕЖИМ: Игры со всеми жанрами ({len(genre_list)}) И текстом "{description_contains}" в описании/названии')
            self.stdout.write(f'   🎭 Жанры: {", ".join(genre_list)}')
        elif genres_str:
            genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]
            self.stdout.write(
                f'🎭 РЕЖИМ: Игры со всеми жанрами ({len(genre_list)}): {", ".join(genre_list)}')
        elif description_contains:
            self.stdout.write(f'📝 РЕЖИМ: Игры с текстом "{description_contains}" в описании/названии')
        elif keywords_str:
            keyword_list = [k.strip() for k in keywords_str.split(',') if k.strip()]
            self.stdout.write(
                f'🔑 РЕЖИМ: Игры с ключевыми словами ({len(keyword_list)} слов): {", ".join(keyword_list)}')
        else:
            self.stdout.write('📊 РЕЖИМ: Все популярные игры')

        # Показываем информацию о типах игр
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                self.stdout.write(f'🎮 ФИЛЬТР ПО ТИПАМ ИГР: {game_types}')
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        if repeat_count > 1:
            self.stdout.write(f'🔄 Итерация {iteration_number}/{repeat_count}')

        # Используем offset и limit для этой конкретной итерации
        actual_offset = iteration_offset
        actual_limit = iteration_limit_actual

        if actual_limit > 0:
            self.stdout.write(f'📊 ЛИМИТ ИТЕРАЦИИ: {actual_limit} НОВЫХ игр')
        else:
            self.stdout.write(f'📊 ИТЕРАЦИЯ: загрузка без лимита')

        if actual_offset > 0:
            self.stdout.write(f'⏭️  OFFSET: начинаем с позиции {actual_offset} в результатах поиска')
        if params['min_rating_count'] > 0:
            self.stdout.write(f'⭐ ФИЛЬТР: игры с не менее {params["min_rating_count"]} оценками')

        if params['overwrite'] and not params['count_only']:
            self.stdout.write('🔄 OVERWRITE: найденные игры будут удалены и загружены заново')

        if params['count_only'] and params['overwrite']:
            self.stdout.write('⚠️  Overwrite игнорируется в режиме count-only')

        if params['debug']:
            self.stdout.write('🐛 РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН')
            self.stdout.write('-' * 40)

        # ВАЖНО: Возвращаем словарь с actual_offset и actual_limit
        return {
            'actual_offset': actual_offset,
            'actual_limit': actual_limit,
        }

    def _load_games_with_offset(self, params, actual_limit, actual_offset, skip_existing, iteration_info):
        """Загружает игры с учетом сохраненного offset"""
        try:
            # Получаем where_clause для сохранения offset
            where_clause = self._get_where_clause_for_mode(params)
            if where_clause:
                query_key = self._get_query_key(params, where_clause)

                # Сохраняем начальный offset для этой итерации
                self._save_iteration_start_offset(query_key, actual_offset)

            # Загружаем игры в зависимости от режима
            result = self._load_games_based_on_mode(
                params, actual_limit, actual_offset, skip_existing
            )

            # Сохраняем конечный offset
            if result and where_clause and 'last_checked_offset' in result:
                last_offset = result['last_checked_offset']
                self._save_last_offset(query_key, last_offset + 1)
                if params['debug']:
                    self.stdout.write(f'   💾 Сохранен offset для продолжения: {last_offset + 1}')

            return result

        except Exception as e:
            self.stderr.write(f'❌ ОШИБКА при загрузке игр: {str(e)}')
            if params['debug']:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            return None

    def _get_where_clause_for_mode(self, params):
        """Возвращает where_clause для текущего режима загрузки"""
        # Эта логика зависит от того, какой метод загрузки используется
        # В реальном коде нужно определить based on params
        return None  # Заглушка

    def _create_error_result(self, iteration_time, errors, actual_offset):
        """Создает результат при ошибке"""
        return {
            'total_games_found': 0,
            'total_games_checked': 0,
            'created_count': 0,
            'skipped_count': 0,
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': actual_offset,
            'limit_reached': False,
            'limit_reached_at_offset': None,
        }

    def _handle_empty_result(self, result, errors, overwrite, actual_offset, iteration_time):
        """Обрабатывает пустой результат загрузки"""
        if result and result.get('total_games_checked', 0) > 0:
            if overwrite:
                self.stdout.write(f'\n📊 РЕЗУЛЬТАТ: Найдено {result.get("total_games_checked", 0)} игр для перезаписи')
            else:
                self.stdout.write(
                    f'\n📊 РЕЗУЛЬТАТ: Найдено {result.get("total_games_checked", 0)} игр, но все они уже есть в базе')

            if result.get('existing_games_skipped', 0) > 0:
                self.stdout.write(f'⏭️  Пропущено существующих игр: {result.get("existing_games_skipped", 0)}')
        else:
            if errors == 0:
                self.stdout.write('\n📊 РЕЗУЛЬТАТ: ❌ Не найдено игр для загрузки')

        # Получаем последний проверенный offset
        last_checked = result.get('last_checked_offset', actual_offset) if result else actual_offset

        return {
            'total_games_found': 0,
            'total_games_checked': result.get('total_games_checked', 0) if result else 0,
            'created_count': 0,
            'skipped_count': result.get('existing_games_skipped', 0) if result else 0,
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': last_checked,
            'limit_reached': result.get('limit_reached', False) if result else False,
            'limit_reached_at_offset': result.get('limit_reached_at_offset'),
        }

    def _handle_count_only_result(self, result, errors, iteration_time, actual_offset):
        """Обрабатывает результат только подсчета"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 РЕЗУЛЬТАТЫ ПОДСЧЕТА:')
        self.stdout.write('=' * 60)
        self.stdout.write(f'🎮 Игр можно загрузить (которых нет в базе): {result["new_games_count"]}')
        self.stdout.write(f'👀 Всего просмотрено игр: {result["total_games_checked"]}')

        if result.get('existing_games_skipped', 0) > 0:
            self.stdout.write(f'⏭️  Игр уже есть в базе: {result.get("existing_games_skipped", 0)}')

        if errors > 0:
            self.stdout.write(f'❌ Ошибок при подсчете: {errors}')

        return {
            'total_games_found': result['new_games_count'],
            'total_games_checked': result['total_games_checked'],
            'created_count': 0,
            'skipped_count': result['existing_games_skipped'],
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': result.get('last_checked_offset', actual_offset),
            'limit_reached': result.get('limit_reached', False),
            'limit_reached_at_offset': result.get('limit_reached_at_offset'),
        }

    def _prepare_final_stats(self, result, result_stats, actual_offset, actual_limit,
                             errors, iteration_info, params, iteration_start_time):
        """Подготавливает финальную статистику"""
        if result_stats:
            result_stats['total_games_checked'] = result['total_games_checked']
            result_stats['total_games_found'] = result['new_games_count']
            result_stats['errors'] = errors
            result_stats['last_checked_offset'] = result.get('last_checked_offset', actual_offset)
            result_stats['limit_reached'] = result.get('limit_reached', False)
            result_stats['limit_reached_at_offset'] = result.get('limit_reached_at_offset')
        else:
            # Если result_stats не создан
            iteration_time = time.time() - iteration_start_time
            result_stats = {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': result['new_games_count'],
                'created_count': 0,
                'skipped_count': 0,
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', actual_offset),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        return result_stats

    def _save_iteration_start_offset(self, query_key, offset):
        """Сохраняет начальный offset итерации"""
        from .load_igdb.offset_manager import OffsetManager

        # Сохраняем как "начальный" для отладки
        OffsetManager.save_offset(f"{query_key}_start", offset)

    def _get_query_key(self, params, where_clause):
        """Создает уникальный ключ для запроса"""
        from .load_igdb.offset_manager import OffsetManager

        # Параметры для ключа
        key_params = {
            'genres': params.get('genres_str', ''),
            'description_contains': params.get('description_contains', ''),
            'keywords': params.get('keywords_str', ''),
            'game_types': params.get('game_types_str', ''),
            'min_rating_count': params.get('min_rating_count', 0),
        }

        return OffsetManager.get_query_key(where_clause, **key_params)

    def _save_last_offset(self, query_key, offset):
        """Сохраняет последний offset"""
        from .load_igdb.offset_manager import OffsetManager
        return OffsetManager.save_offset(query_key, offset)

    def _load_last_offset(self, query_key):
        """Загружает последний offset"""
        from .load_igdb.offset_manager import OffsetManager
        return OffsetManager.load_offset(query_key)

    def _display_iteration_statistics(self, final_stats, result, actual_offset, actual_limit,
                                      params, iteration_info, errors, result_stats):
        """Отображает статистику итерации"""
        all_games = result['new_games']
        total_games_checked = result['total_games_checked']
        new_games_count = result['new_games_count']
        existing_games_skipped = result['existing_games_skipped']
        limit_reached = result.get('limit_reached', False)
        limit_reached_at_offset = result.get('limit_reached_at_offset')

        # Получаем последний проверенный offset
        if limit_reached_at_offset is not None:
            last_checked_offset = limit_reached_at_offset
        else:
            last_checked_offset = result.get('last_checked_offset',
                                             actual_offset + total_games_checked - 1)

        # ВЫВОДИМ ИНФОРМАЦИЮ О НАЙДЕННЫХ ИГРАХ СРАЗУ
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 РЕЗУЛЬТАТЫ ПОИСКА:')
        self.stdout.write('=' * 60)

        if params['overwrite']:
            self.stdout.write(f'📥 Найдено игр для перезаписи: {new_games_count}')
        else:
            self.stdout.write(f'📥 Найдено новых игр (не в базе): {new_games_count}')

        self.stdout.write(f'👀 Всего просмотрено игр из IGDB: {total_games_checked}')

        if existing_games_skipped > 0 and not params['overwrite']:
            self.stdout.write(f'⏭️  Пропущено игр (уже есть в базе): {existing_games_skipped}')

        self.stdout.write(f'📍 Последний проверенный offset: {last_checked_offset}')
        self.stdout.write(f'📍 Следующий offset для продолжения: {last_checked_offset + 1}')

        if limit_reached:
            self.stdout.write(f'🎯 Лимит {actual_limit} достигнут на offset {last_checked_offset}')

        # КРАТКАЯ статистика в конце
        if not params['debug']:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
            self.stdout.write(f'⏱️  Время: {result_stats["total_time"]:.2f}с')

            if iteration_info['repeat_count'] > 1:
                self.stdout.write(f'🔄 Итерация {iteration_info["iteration_number"]}/{iteration_info["repeat_count"]}')

            if errors > 0:
                self.stdout.write(f'❌ Ошибок в итерации: {errors}')

            if limit_reached:
                self.stdout.write(f'🎯 Лимит достигнут на offset {last_checked_offset}')

            if params['overwrite']:
                self.stdout.write(f'🔄 Перезаписано игр: {result_stats["created_count"]}')
            else:
                self.stdout.write(f'✅ Загружено игр: {result_stats["created_count"]}')

        # Подробная статистика в режиме отладки
        elif params['debug']:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('📊 ПОДРОБНАЯ СТАТИСТИКА ИТЕРАЦИИ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🔄 Итерация: {iteration_info["iteration_number"]}/{iteration_info["repeat_count"]}')
            self.stdout.write(f'📍 Начальный offset: {actual_offset}')
            self.stdout.write(f'📍 Последний проверенный offset: {last_checked_offset}')
            self.stdout.write(f'📍 Следующий offset: {last_checked_offset + 1}')
            self.stdout.write(f'👀 Просмотрено игр: {total_games_checked}')
            self.stdout.write(f'🎮 Найдено новых: {new_games_count}')
            self.stdout.write(f'✅ Загружено игр: {result_stats.get("created_count", 0)}')
            self.stdout.write(f'❌ Ошибок: {errors}')

            if limit_reached:
                self.stdout.write(f'🎯 Лимит достигнут: ДА (на offset {last_checked_offset})')

            self.stdout.write(f'⏱️  Время: {result_stats.get("total_time", 0):.2f}с')

            if errors > 0:
                self.stdout.write('⚠️  ИТЕРАЦИЯ ЗАВЕРШЕНА С ОШИБКАМИ')
            else:
                self.stdout.write('✅ ИТЕРАЦИЯ ЗАВЕРШЕНА УСПЕШНО')

        return {
            'total_games_checked': total_games_checked,
            'total_games_found': new_games_count,
            'created_count': result_stats.get('created_count', 0),
            'skipped_count': existing_games_skipped,
            'total_time': result_stats.get('total_time', 0),
            'errors': errors,
            'last_checked_offset': last_checked_offset,
            'limit_reached': limit_reached,
            'limit_reached_at_offset': limit_reached_at_offset,
        }

    def clear_game_cache(self):
        """Очищает кэш проверенных игр"""
        try:
            cleared = self.cache_manager.clear_cache()
            self.stdout.write(f"✅ Кэш проверенных игр очищен")
            return cleared
        except Exception as e:
            self.stderr.write(f"❌ Ошибка при очистке кэша: {e}")
            return False

    def _determine_execution_mode(self, repeat_count):
        """Определяет режим выполнения команды"""
        infinite_mode = repeat_count == 0  # 0 = бесконечно
        single_run_mode = repeat_count == -1  # -1 = только один раз
        finite_mode = repeat_count > 0  # > 0 = фиксированное количество повторений

        if single_run_mode:
            repeat_count = 1
            self.stdout.write('🔄 РЕЖИМ: ОДНА ИТЕРАЦИЯ (--repeat -1)')
        elif infinite_mode:
            self.stdout.write('🔄 РЕЖИМ: БЕСКОНЕЧНО (--repeat 0) - пока не закончатся игры')
            # Устанавливаем большое число для бесконечного цикла
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


    def _process_single_iteration(self, iteration, current_offset, iteration_limit_actual,
                                  options, execution_mode, progress_bar, *args):
        """Обрабатывает одну итерацию"""
        # Подготавливаем параметры для итерации
        iteration_options = options.copy()
        iteration_options['iteration_offset'] = current_offset
        iteration_options['iteration_limit'] = iteration_limit_actual
        iteration_options['_iteration_number'] = iteration

        # Выполняем итерацию
        iteration_result = self.handle_single_iteration(*args, **iteration_options)

        # Обрабатываем результаты итерации
        return self._process_iteration_result(
            iteration, iteration_result, execution_mode, progress_bar
        )

    def _process_iteration_result(self, iteration, iteration_result, execution_mode, progress_bar):
        """Обрабатывает результаты итерации"""
        result = {
            'success': False,
            'stats': {
                'total_games_found': 0,
                'total_games_checked': 0,
                'created_count': 0,
                'skipped_count': 0,
                'total_time': 0,
                'errors': 0,
                'last_checked_offset': 0,
                'limit_reached': False,
                'limit_reached_at_offset': None,
                'new_games_this_iteration': 0
            }
        }

        if iteration_result:
            result['success'] = True
            result['stats'] = iteration_result.copy()
            result['stats']['new_games_this_iteration'] = iteration_result.get('created_count', 0)
        else:
            # Если итерация не вернула результат
            if self.debug_mode:
                self.stdout.write(f'   ⚠️  Итерация {iteration} не вернула результат')

        return result

    def _update_total_stats(self, total_stats, iteration_stats, iteration, current_offset,
                            execution_mode, progress_bar):
        """Обновляет общую статистику"""
        # Обновляем статистику
        total_stats['iterations'] += 1
        total_stats['total_games_found'] += iteration_stats.get('total_games_found', 0)
        total_stats['total_games_checked'] += iteration_stats.get('total_games_checked',
                                                                  iteration_stats.get('total_games_found', 0))
        total_stats['total_games_created'] += iteration_stats.get('created_count', 0)
        total_stats['total_games_skipped'] += iteration_stats.get('skipped_count', 0)
        total_stats['total_time'] += iteration_stats.get('total_time', 0)

        # Проверяем, были ли найдены новые игры в этой итерации
        new_games_this_iteration = iteration_stats.get('created_count', 0)
        if new_games_this_iteration == 0 and iteration_stats.get('total_games_found', 0) == 0:
            total_stats['iterations_with_no_new_games'] += 1
        else:
            total_stats['iterations_with_no_new_games'] = 0  # Сбрасываем счетчик

        # ОБНОВЛЯЕМ ПРОГРЕСС-БАР
        if progress_bar:
            progress_bar.update(
                total_loaded=total_stats['total_games_created'],
                current_iteration=iteration,
                iterations_without_new=total_stats['iterations_with_no_new_games']
            )

        # Добавляем ошибки из итерации
        iteration_errors = iteration_stats.get('errors', 0)
        if iteration_errors > 0:
            total_stats['errors'] += iteration_errors
            total_stats['iterations_with_errors'] += 1

        # Проверяем, достиг ли лимит в этой итерации
        iteration_limit_reached = iteration_stats.get('limit_reached', False)
        if iteration_limit_reached:
            total_stats['iterations_with_limit_reached'] += 1

        # Получаем последний проверенный offset
        limit_reached_offset = iteration_stats.get('limit_reached_at_offset')
        if limit_reached_offset is not None:
            last_checked_this_iteration = limit_reached_offset
        else:
            last_checked_this_iteration = iteration_stats.get('last_checked_offset',
                                                              current_offset + iteration_stats.get(
                                                                  'total_games_checked',
                                                                  iteration_stats.get('total_games_found', 0)) - 1)

        # Обновляем offset для следующей итерации
        previous_offset = current_offset
        new_offset = last_checked_this_iteration + 1
        total_stats['last_checked_offset'] = last_checked_this_iteration

        if self.debug_mode:
            self.stdout.write(f'   📊 Итерация {iteration}:')
            self.stdout.write(f'      • Начальный offset: {previous_offset}')
            self.stdout.write(f'      • Просмотрено игр: {iteration_stats.get("total_games_checked", 0)}')
            self.stdout.write(f'      • Найдено новых: {iteration_stats.get("total_games_found", 0)}')
            self.stdout.write(f'      • Загружено: {iteration_stats.get("created_count", 0)}')
            self.stdout.write(f'      • Ошибок: {iteration_errors}')
            if iteration_limit_reached:
                self.stdout.write(f'      • Лимит итерации достигнут: ДА')
            self.stdout.write(f'      • Последний проверенный offset: {last_checked_this_iteration}')
            self.stdout.write(f'      • Следующий offset: {new_offset}')
            if new_games_this_iteration == 0:
                self.stdout.write(
                    f'      • Новых игр не найдено (счетчик: {total_stats["iterations_with_no_new_games"]}/{self.max_consecutive_no_new_games})')

        return new_offset


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
        self.stdout.write(f'⏭️  Всего пропущено игр: {total_stats["total_games_skipped"]}')
        self.stdout.write(f'❌ Ошибок: {total_stats["errors"]}')
        self.stdout.write(f'⚠️  Итераций с ошибками: {total_stats["iterations_with_errors"]}')
        self.stdout.write(f'🚫 Итераций без новых игр: {total_stats["iterations_with_no_new_games"]}')

        if limit > 0:
            self.stdout.write(f'🎯 Общий лимит игр: {limit} (достигнуто: {total_stats["total_games_created"]})')

            if total_stats['total_games_created'] >= limit:
                self.stdout.write(f'✅ ЛИМИТ ИГР ДОСТИГНУТ!')
            else:
                self.stdout.write(f'⚠️  ЛИМИТ ИГР НЕ ДОСТИГНУТ')

        self.stdout.write(f'⏱️  Общее время: {total_stats["total_time"]:.2f}с')

        # Статистика эффективности
        if total_stats['iterations'] > 0:
            success_rate = ((total_stats['iterations'] - total_stats['iterations_with_errors']) /
                            total_stats['iterations'] * 100)
            self.stdout.write(f'📈 Успешных итераций: {success_rate:.1f}%')

            if total_stats['total_games_created'] > 0:
                # Скорость загрузки
                if total_stats['total_time'] > 0:
                    games_per_second = total_stats['total_games_created'] / total_stats['total_time']
                    self.stdout.write(f'🚀 Скорость загрузки: {games_per_second:.2f} игр/сек')
                    checked_per_second = total_stats['total_games_checked'] / total_stats['total_time']
                    self.stdout.write(f'🔍 Скорость проверки: {checked_per_second:.2f} игр/сек')

                # Эффективность поиска
                if total_stats['total_games_checked'] > 0:
                    efficiency = total_stats['total_games_checked'] / total_stats['total_games_created']
                    self.stdout.write(f'📊 Эффективность поиска: {efficiency:.1f} просмотренных на 1 новую игру')

            avg_time = total_stats['total_time'] / total_stats['iterations']
            self.stdout.write(f'📈 Среднее время на итерацию: {avg_time:.2f}с')

        # Дополнительная информация для режима overwrite
        if overwrite:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'✅ Перезаписано игр: {total_stats["total_games_created"]}')

            # Правильная информация о offset
            self.stdout.write(f'📍 Начальный offset: {original_offset}')
            self.stdout.write(f'📍 Последний проверенный offset: {total_stats["last_checked_offset"]}')
            self.stdout.write(f'📍 Следующий offset (для продолжения): {current_offset}')

            # Рассчитываем проверенные позиции
            if total_stats['last_checked_offset'] >= original_offset:
                checked_positions = total_stats['last_checked_offset'] - original_offset + 1
                self.stdout.write(f'📊 Проверено позиций в IGDB: {checked_positions}')

                # Показываем соотношение проверенных позиций и загруженных игр
                if total_stats['total_games_created'] > 0:
                    self.stdout.write(
                        f'📊 Соотношение позиций/игр: {checked_positions}:{total_stats["total_games_created"]}')

                    # Эффективность (сколько позиций нужно проверить для одной игры)
                    efficiency = checked_positions / total_stats['total_games_created']
                    self.stdout.write(f'📊 Эффективность: {efficiency:.1f} позиций на 1 игру')

            self.stdout.write(f'👀 Просмотрено игр из IGDB: {total_stats["total_games_checked"]}')

            # Эффективность поиска
            if total_stats['total_games_created'] > 0 and total_stats['total_games_checked'] > 0:
                search_efficiency = total_stats['total_games_checked'] / total_stats['total_games_created']
                self.stdout.write(
                    f'🔍 Эффективность поиска: {search_efficiency:.1f} просмотренных на 1 перезаписанную игру')

            # Статистика по лимиту
            if limit > 0:
                self.stdout.write(f'🎯 Лимит достигнут в {total_stats["iterations_with_limit_reached"]} итерациях')

            if total_stats['errors'] > 0:
                self.stdout.write(f'⚠️  Ошибок при перезаписи: {total_stats["errors"]}')


    def _load_initial_offset(self, options, debug):
        """Загружает начальный offset из сохраненного"""
        from .load_igdb.offset_manager import OffsetManager

        # Получаем where_clause для текущей команды
        where_clause = self._get_where_clause_for_current_command(options)
        if not where_clause:
            if debug:
                self.stdout.write('   ⚠️  Не удалось определить where_clause для загрузки offset')
            return 0

        # Получаем ключ запроса
        query_key = self._get_query_key_for_current_command(options, where_clause)

        if debug:
            self.stdout.write(f'   🔑 Ищу сохраненный offset для ключа: {query_key}')

        # Загружаем сохраненный offset
        saved_offset = OffsetManager.load_offset(query_key)

        if saved_offset is not None:
            self.stdout.write(f'   📍 Загружен сохраненный offset: {saved_offset}')

            # Показываем дополнительную информацию в режиме отладки
            if debug:
                # Проверяем стартовый offset для диагностики
                start_offset = OffsetManager.load_offset(f"{query_key}_start")
                if start_offset is not None:
                    self.stdout.write(f'   📍 Стартовый offset: {start_offset}')

                self.stdout.write(f'   📋 Параметры запроса:')
                self.stdout.write(f'      • Жанры: {options.get("genres", "")}')
                self.stdout.write(f'      • Типы игр: {options.get("game_types", "")}')
                self.stdout.write(f'      • Минимальные оценки: {options.get("min_rating_count", 0)}')
                self.stdout.write(f'      • Режим загрузки: {self._get_loading_mode(options)}')

            return saved_offset
        else:
            if debug:
                self.stdout.write('   ℹ️  Сохраненный offset не найден, начинаем с 0')
            return 0


    def _load_saved_offset_for_execution(self, options, execution_mode):
        """Загружает сохраненный offset для выполнения команды"""
        from .load_igdb.offset_manager import OffsetManager

        # Получаем where_clause для текущей команды
        where_clause = self._get_where_clause_for_current_command(options)
        if not where_clause:
            return options['offset']  # Возвращаем оригинальный offset

        # Получаем ключ запроса
        query_key = self._get_query_key_for_current_command(options, where_clause)

        # Загружаем сохраненный offset
        saved_offset = OffsetManager.load_offset(query_key)

        if saved_offset is not None:
            self.stdout.write(f'   📍 Загружен сохраненный offset: {saved_offset}')
            if options.get('debug', False):
                self.stdout.write(f'   🔑 Ключ запроса: {query_key}')

            # Для режима перезаписи показываем предупреждение
            if options.get('overwrite', False):
                self.stdout.write('   ⚠️  Режим overwrite: offset используется только для позиционирования')

            return saved_offset

        return options['offset']  # Возвращаем оригинальный offset

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

    def _setup_iteration_info(self, options, iteration_offset, iteration_limit_actual):
        """Настраивает информацию об итерации"""
        # Получаем номер итерации для информационных сообщений
        iteration_number = options.get('_iteration_number', 1)
        repeat_count = options.get('repeat', 1)

        return {
            'iteration_number': iteration_number,
            'repeat_count': repeat_count,
            'iteration_offset': iteration_offset,
            'iteration_limit_actual': iteration_limit_actual,
        }

    def _determine_skip_existing_mode(self, overwrite, count_only):
        """Определяет режим пропуска существующих игр"""
        if overwrite:
            return False  # Не пропускаем существующие, перезаписываем их
        else:
            return True  # Обычный режим: пропускаем существующие игры

    def _load_games_based_on_mode(self, params, actual_limit, actual_offset, skip_existing):
        """Загружает игры в зависимости от режима"""
        genres_str = params['genres_str']
        description_contains = params['description_contains']
        keywords_str = params['keywords_str']
        game_types_str = params['game_types_str']
        min_rating_count = params['min_rating_count']
        count_only = params['count_only']
        debug = params['debug']

        if genres_str and description_contains:
            return self.load_games_by_genres_and_description(
                genres_str, description_contains, debug, actual_limit, actual_offset,
                min_rating_count, skip_existing, count_only, game_types_str
            )
        elif genres_str:
            return self.load_games_by_genres(
                genres_str, debug, actual_limit, actual_offset,
                min_rating_count, skip_existing, count_only, game_types_str
            )
        elif description_contains:
            return self.load_games_by_description(
                description_contains, debug, actual_limit, actual_offset,
                min_rating_count, skip_existing, count_only, game_types_str
            )
        elif keywords_str:
            return self.load_games_by_keywords(
                keywords_str, debug, actual_limit, actual_offset,
                min_rating_count, skip_existing, count_only, game_types_str
            )
        else:
            return self.load_all_popular_games(
                debug, actual_limit, actual_offset,
                min_rating_count, skip_existing, count_only, game_types_str
            )

    def _process_game_data(self, all_games, overwrite, debug, iteration_start_time, errors):
        """Обрабатывает данные игр"""
        # Обработка режима перезаписи
        if overwrite and all_games:
            try:
                self._handle_overwrite_mode(all_games, debug)
            except Exception as e:
                errors += 1
                self.stderr.write(f'❌ ОШИБКА при удалении игр: {str(e)}')
                if debug:
                    import traceback
                    self.stderr.write(f'📋 Трассировка ошибки:')
                    self.stderr.write(traceback.format_exc())

        # Обработка данных
        result_stats = None
        try:
            collector = DataCollector(self.stdout, self.stderr)
            result_stats = collector.process_all_data_sequentially(all_games, debug)
        except Exception as e:
            errors += 1
            self.stderr.write(f'❌ ОШИБКА при обработке данных: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            # Создаем пустую статистику при ошибке
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }

        return result_stats, errors

    def load_games_by_genres(self, genres_str, debug=False, limit=0, offset=0, min_rating_count=0,
                             skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по жанрам с логикой И (должны быть ВСЕ указанные жанры)"""
        collector = DataCollector(self.stdout, self.stderr)

        genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]

        if not genre_list:
            self.stdout.write('⚠️  Не указаны жанры')
            return []

        if debug:
            self.stdout.write(f'🔍 Поиск жанров: {", ".join(genre_list)}')

        # Получаем ID для всех жанров
        genre_ids = []
        for genre in genre_list:
            query = f'fields id,name; where name = "{genre}";'
            result = make_igdb_request('genres', query, debug=False)
            if result:
                genre_ids.append(str(result[0]['id']))
                if debug:
                    self.stdout.write(f'   ✅ Жанр "{genre}" найден: ID {result[0]["id"]}')
            else:
                if debug:
                    self.stdout.write(f'   ❌ Жанр "{genre}" не найден')

        if not genre_ids:
            self.stdout.write('❌ Не найдены указанные жанры')
            return []

        if debug:
            self.stdout.write(f'📋 Найдено ID жанров: {", ".join(genre_ids)}')

        # Формируем условие для поиска игр (логика И - должны быть ВСЕ жанры)
        genre_conditions = [f'genres = ({genre_id})' for genre_id in genre_ids]
        where_clause = ' & '.join(genre_conditions)

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_clause = f'{where_clause} & game_type = ({game_types_str_query})'
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        if debug:
            self.stdout.write(f'🎯 Условие поиска (И): {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_genres_and_description(self, genres_str, description_text, debug=False, limit=0, offset=0,
                                             min_rating_count=0, skip_existing=True, count_only=False,
                                             game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по жанрам И тексту в описании"""
        collector = DataCollector(self.stdout, self.stderr)

        genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]

        if not genre_list:
            self.stdout.write('⚠️  Не указаны жанры')
            return []

        if debug:
            self.stdout.write(f'🔍 Поиск жанров: {", ".join(genre_list)}')
            self.stdout.write(f'🔍 Текст для поиска: "{description_text}"')

        # Получаем ID для всех жанров
        genre_ids = []
        for genre in genre_list:
            query = f'fields id,name; where name = "{genre}";'
            result = make_igdb_request('genres', query, debug=False)
            if result:
                genre_ids.append(str(result[0]['id']))
                if debug:
                    self.stdout.write(f'   ✅ Жанр "{genre}" найден: ID {result[0]["id"]}')
            else:
                if debug:
                    self.stdout.write(f'   ❌ Жанр "{genre}" не найден')

        if not genre_ids:
            self.stdout.write('❌ Не найдены указанные жанры')
            return []

        if debug:
            self.stdout.write(f'📋 Найдено ID жанров: {", ".join(genre_ids)}')

        # Формируем условие для поиска игр (логика И между жанрами)
        genre_conditions = [f'genres = ({genre_id})' for genre_id in genre_ids]
        genres_condition = ' & '.join(genre_conditions)

        # Формируем общее условие: жанры И (текст в названии ИЛИ описании)
        text_condition = f'(name ~ *"{description_text}"* | summary ~ *"{description_text}"* | storyline ~ *"{description_text}"*)'
        where_clause = f'{genres_condition} & {text_condition}'

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_clause = f'{where_clause} & game_type = ({game_types_str_query})'
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        if debug:
            self.stdout.write(f'🎯 Итоговое условие поиска: {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_description(self, description_text, debug=False, limit=0, offset=0, min_rating_count=0,
                                  skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по тексту в описании или названии"""
        collector = DataCollector(self.stdout, self.stderr)

        if debug:
            self.stdout.write(f'🔍 Ищу игры с текстом: "{description_text}"')

        # Формируем базовое условие для поиска
        where_conditions = [
            f'name ~ *"{description_text}"* | summary ~ *"{description_text}"* | storyline ~ *"{description_text}"*']

        if min_rating_count > 0:
            where_conditions.append(f'rating_count >= {min_rating_count}')
        else:
            where_conditions.append('rating_count > 0')

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_conditions.append(f'game_type = ({game_types_str_query})')
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        where_clause = ' & '.join(where_conditions)

        if debug:
            self.stdout.write(f'   🎯 Условие поиска: {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_keywords(self, keywords_str, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по ключевым словам с логикой И"""
        collector = DataCollector(self.stdout, self.stderr)

        keyword_list = [k.strip() for k in keywords_str.split(',') if k.strip()]

        if not keyword_list:
            self.stdout.write('⚠️  Не указаны ключевые слова')
            return []

        if debug:
            self.stdout.write(f'🔍 Поиск ключевых слов: {", ".join(keyword_list)}')

        # Получаем ID для всех ключевых слов
        keyword_ids = []
        for keyword in keyword_list:
            query = f'fields id,name; where name = "{keyword}";'
            result = make_igdb_request('keywords', query, debug=False)
            if result:
                keyword_ids.append(str(result[0]['id']))
                if debug:
                    self.stdout.write(f'   ✅ Ключевое слово "{keyword}" найдено: ID {result[0]["id"]}')
            else:
                if debug:
                    self.stderr.write(f'   ❌ Ключевое слово "{keyword}" не найдено')

        if not keyword_ids:
            self.stdout.write('❌ Не найдены указанные ключевые слова')
            return []

        if debug:
            self.stdout.write(f'📋 Найдено ID ключевых слов: {", ".join(keyword_ids)}')

        # Формируем условие для поиска игр (логика И)
        keyword_conditions = [f'keywords = ({keyword_id})' for keyword_id in keyword_ids]
        where_clause = ' & '.join(keyword_conditions)

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_clause = f'{where_clause} & game_type = ({game_types_str_query})'
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        if debug:
            self.stdout.write(f'🎯 Условие поиска: {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        collector = DataCollector(self.stdout, self.stderr)
        return collector.load_all_popular_games(debug, limit, offset, min_rating_count, skip_existing, count_only,
                                                game_types_str)

    def _handle_overwrite_mode(self, all_games, debug):
        """Обрабатывает режим перезаписи"""
        self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ - найденные игры будут удалены и загружены заново!')

        # Получаем ID найденных игр
        game_ids_to_delete = [game_data.get('id') for game_data in all_games if game_data.get('id')]

        if game_ids_to_delete:
            if debug:
                self.stdout.write(f'   🔍 Поиск игр для удаления: {len(game_ids_to_delete)} ID')

            # Находим игры в базе по igdb_id
            games_to_delete = Game.objects.filter(igdb_id__in=game_ids_to_delete)
            count_before = games_to_delete.count()

            if debug:
                self.stdout.write(f'   📊 Найдено игр для удаления в базе: {count_before}')

            if count_before > 0:
                # Удаляем найденные игры (связанные объекты удалятся каскадно)
                deleted_info = games_to_delete.delete()

                # Разбираем результат delete()
                if isinstance(deleted_info, tuple) and len(deleted_info) == 2:
                    total_deleted, deleted_details = deleted_info

                    # Выводим детализированную статистику
                    self.stdout.write(f'🗑️  УДАЛЕНИЕ ЗАВЕРШЕНО:')
                    self.stdout.write(f'   • Всего удалено объектов: {total_deleted}')

                    # Выводим детали по моделям
                    for model_name, count in deleted_details.items():
                        model_display = model_name.split('.')[-1]  # Извлекаем имя модели
                        if count > 0:
                            self.stdout.write(f'   • {model_display}: {count}')
                else:
                    # Для старых версий Django
                    self.stdout.write(f'🗑️  Удалено игр и связанных данных: {deleted_info}')
            else:
                self.stdout.write('   ℹ️  Не найдено игр для удаления в базе данных')
        else:
            self.stdout.write('   ⚠️  Не найдено ID игр для удаления')

    def create_game_object(self, game_data, cover_map):
        """Создает объект игры"""
        game = Game(
            igdb_id=game_data.get('id'),
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
            from datetime import datetime
            naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
            game.first_release_date = timezone.make_aware(naive_datetime)

        cover_id = game_data.get('cover')
        if cover_id and cover_id in cover_map:
            game.cover_url = cover_map[cover_id]

        return game


class Command(BaseGamesCommand):
    """Команда для загрузки игр из IGDB"""

    help = 'Загрузка игр из IGDB с разными фильтрами'

    def handle(self, *args, **options):
        """Основной метод"""
        super().handle(*args, **options)
