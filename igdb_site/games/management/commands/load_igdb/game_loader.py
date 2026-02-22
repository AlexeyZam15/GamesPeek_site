# games/management/commands/load_igdb/game_loader.py
import time
import signal
import threading
import concurrent.futures
from django.utils import timezone
from games.models import Game, Genre, Platform, Keyword, GameEngine, Series, Company, Theme, PlayerPerspective, \
    GameMode, Screenshot
from games.igdb_api import make_igdb_request, OPTIMAL_CONFIG
from .base_command import BaseGamesCommand
from .data_collector import DataCollector
from .data_loader import DataLoader
from .relations_handler import RelationsHandler
from .statistics import Statistics
from .offset_manager import OffsetManager


class ProcessingProgressBar:
    """Прогресс-бар для этапа обработки данных - работает в одной строке"""

    def __init__(self, stdout, total_games):
        self.stdout = stdout
        self.total_games = total_games
        self.steps = {
            'collecting_ids': ('📊', 'Сбор ID данных'),
            'creating_games': ('🎮', 'Создание игр'),
            'loading_all_types': ('🖼️', 'Типы данных'),
            'loading_screenshots': ('📸', 'Скриншоты'),
            'loading_additional': ('📚', 'Доп. данные'),
            'updating_covers': ('💾', 'Обложки'),
            'preparing_relations': ('🔗', 'Связи'),
            'creating_relations': ('⚙️', 'Создание связей'),
        }
        self.step_order = [
            'collecting_ids', 'creating_games', 'loading_all_types',
            'loading_screenshots', 'loading_additional', 'updating_covers',
            'preparing_relations', 'creating_relations'
        ]
        self.current_step_index = -1
        self.last_printed_length = 0
        self.is_tty = hasattr(stdout, 'isatty') and stdout.isatty()
        self.step_results = {}
        self.start_time = time.time()

    def start_step(self, step_key, info=''):
        """Начинает новый шаг"""
        if not self.is_tty:
            return

        self.current_step_index += 1
        if step_key not in self.steps:
            return

        emoji, name = self.steps[step_key]
        step_num = self.current_step_index + 1
        total_steps = len(self.step_order)

        message = f'   [{step_num}/{total_steps}] {emoji} {name}'
        if info:
            message += f': {info}'

        if self.last_printed_length > 0:
            self.stdout.write('\r' + ' ' * self.last_printed_length + '\r')

        self.stdout.write('\r' + message)
        self.stdout.flush()

        self.last_printed_length = len(message)

    def complete_step(self, step_key, result_info=''):
        """Завершает шаг и показывает результат"""
        if step_key not in self.steps:
            return

        emoji, name = self.steps[step_key]
        self.step_results[step_key] = result_info

        if self.is_tty:
            self.stdout.write('\r' + ' ' * self.last_printed_length + '\r')
            self.last_printed_length = 0

        if result_info:
            self.stdout.write(f'   ✅ {emoji} {name}: {result_info}')
        else:
            self.stdout.write(f'   ✅ {emoji} {name} завершено')

    def clear(self):
        """Очищает строку прогресса"""
        if self.is_tty and self.last_printed_length > 0:
            self.stdout.write('\r' + ' ' * self.last_printed_length + '\r')
            self.stdout.flush()
            self.last_printed_length = 0

    def show_summary(self, total_time, created_count):
        """Показывает итоговую статистику"""
        self.clear()

        if total_time > 0 and created_count > 0:
            speed = created_count / total_time
            speed_str = f' ({speed:.1f} игр/с)'
        else:
            speed_str = ''

        self.stdout.write(f'   ✅ Обработано {created_count} игр за {total_time:.1f}с{speed_str}')


class GameLoader(BaseGamesCommand):
    """Основной класс для выполнения команды загрузки игр"""

    def __init__(self, stdout, stderr):
        super().__init__()
        self.stdout = stdout
        self.stderr = stderr
        self.max_consecutive_no_new_games = 3
        self.debug_mode = False
        self._last_processed_count = 0

    def execute_command(self, options):
        """Основной метод выполнения команды"""
        self.debug_mode = options.get('debug', False)
        self.current_options = options

        log_dir = self.ensure_logs_directory(self.debug_mode)
        if self.debug_mode:
            self.stdout.write(f'📁 Логи будут сохраняться в: {log_dir}')

        options['use_cache'] = options.get('use_cache', True)
        options['cache_ttl'] = options.get('cache_ttl', 3600)

        if options.get('clear_db_cache', False):
            self.clear_db_cache()

        setup_result = self._setup_execution_environment(options)

        if setup_result[6] if len(setup_result) > 6 else False:
            return

        execution_mode, progress_bar, current_offset, total_stats, options, limit = setup_result[:6]

        self._run_execution_loop(execution_mode, progress_bar, current_offset, total_stats, options, limit)

    def _setup_execution_environment(self, options):
        """Основная настройка окружения выполнения команды"""
        self.current_options = options

        repeat_count = options['repeat']
        original_offset = options['offset']
        limit = options['limit']
        iteration_limit = options['iteration_limit']
        debug = options['debug']
        overwrite = options['overwrite']
        count_only = options['count_only']
        clear_cache = options.get('clear_cache', False)
        reset_offset = options.get('reset_offset', False)
        update_missing_data = options.get('update_missing_data', False)
        update_covers = options.get('update_covers', False)

        actual_offset = self._display_offset_info(options, original_offset)

        if update_covers:
            return self._setup_update_covers_environment(options, debug, actual_offset)
        elif update_missing_data:
            return self._setup_update_mode_environment(options, debug, actual_offset)

        return self._setup_standard_environment(options, debug, repeat_count, actual_offset,
                                                limit, iteration_limit, clear_cache, reset_offset)

    def _display_offset_info(self, options, original_offset):
        """Показывает информацию об offset - ТОЛЬКО ОДНО СООБЩЕНИЕ"""
        if options.get('reset_offset', False):
            self.stdout.write('🔄 Offset сброшен по запросу')
            return original_offset

        if original_offset == 0:
            saved_offset = self.get_saved_offset(options)
            if saved_offset is not None:
                return saved_offset
            else:
                return 0
        else:
            return original_offset

    def _setup_standard_environment(self, options, debug, repeat_count, original_offset,
                                    limit, iteration_limit, clear_cache, reset_offset):
        """Настройка окружения для стандартного режима загрузки"""
        self.debug_mode = debug

        if clear_cache:
            self.clear_game_cache()

        if reset_offset:
            self.handle_reset_offset(options, debug)

        execution_mode = self.determine_execution_mode(repeat_count)

        progress_bar = self.create_progress_bar()

        if limit > 0:
            progress_bar.total_games = limit
            progress_bar.update()
        elif execution_mode['infinite_mode']:
            progress_bar.total_games = 0
            progress_bar.update()
        else:
            progress_bar.total_games = 0
            progress_bar.update()

        current_offset = original_offset

        if original_offset == 0 and not reset_offset:
            saved_offset = self.get_saved_offset(options)
            if saved_offset is not None:
                current_offset = saved_offset
                self.stdout.write(f'📍 Начинаем с сохраненного offset: {current_offset}')

        total_stats = self.initialize_total_stats(original_offset)

        self.display_startup_info(execution_mode, iteration_limit, limit, current_offset, options)

        if (hasattr(progress_bar, 'is_tty') and progress_bar.is_tty and not count_only and not debug):
            self.stdout.write('\n' * 2)

        return execution_mode, progress_bar, current_offset, total_stats, options, limit, False

    def _run_execution_loop(self, execution_mode, progress_bar, current_offset, total_stats, options, limit):
        """Выполнение основного цикла команды"""
        debug = options.get('debug', False)
        reset_offset = options.get('reset_offset', False)
        iteration = 1

        try:
            while True:
                should_continue, current_offset, total_stats = self._execute_single_iteration(
                    iteration, current_offset, total_stats, execution_mode,
                    limit, options['iteration_limit'], options, progress_bar
                )

                if not should_continue:
                    break

                if iteration < execution_mode['repeat_count'] or execution_mode['infinite_mode']:
                    pause_time = 2
                    if self.debug_mode:
                        self.stdout.write(f'   ⏸️  Пауза {pause_time} секунд...')
                    time.sleep(pause_time)

                iteration += 1

        except KeyboardInterrupt:
            self.handle_global_interrupt(total_stats, execution_mode,
                                         options['offset'], current_offset,
                                         limit, progress_bar, options)
            return

        except Exception as e:
            self.stderr.write(f'\n❌ Неожиданная ошибка: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())

            if not reset_offset:
                self.save_offset_for_continuation(options, current_offset)

            return

        self.finalize_execution(total_stats, limit, progress_bar,
                                execution_mode, options['offset'],
                                current_offset, limit, options['overwrite'])

        self.display_final_status(total_stats, limit)

    def _execute_single_iteration(self, iteration, current_offset, total_stats, execution_mode,
                                  limit, iteration_limit, options, progress_bar):
        """Выполняет одну итерацию загрузки"""
        should_continue = self.should_continue_iteration(
            iteration, execution_mode, total_stats, limit
        )

        if not should_continue:
            return False, current_offset, total_stats

        # Подсчитываем ВСЕ обработанные игры
        processed = (total_stats['total_games_created'] +
                     total_stats['total_games_updated'] +
                     total_stats['total_games_skipped'])

        if self.debug_mode:
            if execution_mode['repeat_count'] > 1 or execution_mode['infinite_mode']:
                self.stdout.write(f'\n🌀 ИТЕРАЦИЯ {iteration}')
                if execution_mode['infinite_mode']:
                    self.stdout.write(
                        f'🌀 (бесконечный режим, итераций без новых игр: {total_stats["iterations_with_no_new_games"]}/{self.max_consecutive_no_new_games})')
                self.stdout.write('=' * 40)

                if current_offset > options['offset']:
                    self.stdout.write(f'📊 Начинаем с offset: {current_offset}')

            iteration_limit_actual, can_continue = self.calculate_iteration_limit(
                limit, iteration_limit, total_stats
            )

            if not can_continue or iteration_limit_actual <= 0:
                self.stdout.write(f'\n✅ ДОСТИГНУТ ОБЩИЙ ЛИМИТ: {limit} игр обработано')
                return False, current_offset, total_stats

            self.stdout.write(f'🎯 Цель итерации: найти {iteration_limit_actual} новых игр')
            if limit > 0:
                remaining_limit = limit - processed
                self.stdout.write(f'   (осталось до лимита: {remaining_limit} игр)')
        else:
            iteration_limit_actual, can_continue = self.calculate_iteration_limit(
                limit, iteration_limit, total_stats
            )

            if not can_continue or iteration_limit_actual <= 0:
                if limit > 0 and processed >= limit:
                    progress_bar.final_message(f"✅ ДОСТИГНУТ ОБЩИЙ ЛИМИТ: {limit} игр обработано")
                return False, current_offset, total_stats

        try:
            iteration_result = self.handle_single_iteration(
                iteration=iteration,
                current_offset=current_offset,
                iteration_limit_actual=iteration_limit_actual,
                options=options
            )

            if iteration_result.get('success', True):
                current_offset = self.update_total_stats(
                    total_stats, iteration_result, iteration,
                    current_offset, execution_mode, progress_bar, self.debug_mode
                )

                # После обновления статистики проверяем лимит
                if limit > 0:
                    processed_after = (total_stats['total_games_created'] +
                                       total_stats['total_games_updated'] +
                                       total_stats['total_games_skipped'])
                    if processed_after >= limit:
                        if self.debug_mode:
                            self.stdout.write(f'\n✅ ДОСТИГНУТ ЛИМИТ: {limit} игр обработано')
                        return False, current_offset, total_stats
            else:
                if self.debug_mode:
                    self.stdout.write(f'   ⚠️  Итерация {iteration} не вернула результат')
                total_stats['iterations'] += 1
                total_stats['iterations_with_no_new_games'] += 1

                if progress_bar:
                    progress_bar.update(
                        total_loaded=total_stats['total_games_created'],
                        current_iteration=iteration,
                        iterations_without_new=total_stats['iterations_with_no_new_games']
                    )

        except KeyboardInterrupt:
            raise
        except Exception as e:
            self._handle_iteration_error(e, iteration, execution_mode, total_stats, progress_bar)

        return True, current_offset, total_stats

    def handle_single_iteration(self, iteration, current_offset, iteration_limit_actual, options):
        """Обработка одной итерации команды"""
        debug = options.get('debug', False)

        params = self.get_execution_parameters(options)

        params['update_missing_data'] = options.get('update_missing_data', False)
        params['update_covers'] = options.get('update_covers', False)

        skip_existing = self._determine_skip_mode(params)

        errors = 0
        iteration_start_time = time.time()

        result = self._load_games_for_iteration(params, iteration_limit_actual, current_offset, skip_existing, debug)

        if result is None:
            return self._handle_failed_loading(iteration_start_time, errors, current_offset)

        if isinstance(result, tuple):
            if debug:
                self.stdout.write(f'   ⚠️  Получен кортеж вместо словаря, преобразую...')
            if len(result) >= 2 and isinstance(result[0], dict):
                result = result[0]
            else:
                result = self.empty_result()

        if not result.get('all_found_games') and not result.get('new_games'):
            return self._handle_empty_results(result, errors, params, current_offset, iteration_start_time)

        new_games_count = result.get('new_games_count', 0)
        if new_games_count == 0:
            iteration_time = time.time() - iteration_start_time
            return {
                'total_games_checked': result.get('total_games_checked', 0),
                'total_games_found': 0,
                'created_count': 0,
                'skipped_count': result.get('existing_games_skipped', 0),
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', current_offset),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        if params.get('update_missing_data'):
            result_stats = self._update_existing_game_data(
                result, params, iteration_start_time, errors
            )
        elif params.get('update_covers'):
            result_stats = self._update_game_covers(
                result, params, iteration_start_time, errors
            )
        else:
            result_stats = self._process_standard_game_data(
                result, params, iteration_start_time, errors
            )

        if isinstance(result_stats, dict):
            errors = result_stats.get('errors', 0)
            iteration_time = result_stats.get('total_time', time.time() - iteration_start_time)
            created_count = result_stats.get('created_count', 0)
        else:
            if debug:
                self.stdout.write(f'   ⚠️  result_stats не словарь: {type(result_stats)}')
            iteration_time = time.time() - iteration_start_time
            created_count = 0
            errors = errors

        return {
            'total_games_checked': result.get('total_games_checked', 0),
            'total_games_found': new_games_count,
            'created_count': created_count,
            'skipped_count': result.get('existing_games_skipped', 0),
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': result.get('last_checked_offset', current_offset),
            'limit_reached': result.get('limit_reached', False),
            'limit_reached_at_offset': result.get('limit_reached_at_offset'),
        }

    def _determine_skip_mode(self, params):
        """Определяет режим пропуска существующих игр"""
        if params['overwrite']:
            return False
        else:
            return True

    def _load_games_for_iteration(self, params, actual_limit, actual_offset, skip_existing, debug):
        """Загружает игры для итерации"""
        try:
            if params.get('game_modes_str'):
                result = self.load_games_by_game_mode(
                    params['game_modes_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params.get('game_names_str'):
                result = self.load_games_by_names(
                    params['game_names_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str'] and params['description_contains']:
                result = self.load_games_by_genres_and_description(
                    params['genres_str'], params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str']:
                result = self.load_games_by_genres(
                    params['genres_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['description_contains']:
                result = self.load_games_by_description(
                    params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['keywords_str']:
                result = self.load_games_by_keywords(
                    params['keywords_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            else:
                result = self.load_all_popular_games(
                    debug, actual_limit, actual_offset, params['min_rating_count'],
                    skip_existing, params['count_only'], params['game_types_str']
                )

            if debug and result and isinstance(result, dict):
                total_checked = result.get('total_games_checked', 0)
                new_games = result.get('new_games_count', 0)
                skipped = result.get('existing_games_skipped', 0)

                self.stdout.write(f'   📊 Результат загрузки:')
                self.stdout.write(f'      👀 Просмотрено игр: {total_checked}')
                self.stdout.write(f'      🆕 Найдено новых: {new_games}')
                if skipped > 0:
                    self.stdout.write(f'      ⏭️  Пропущено (уже в базе): {skipped}')

            return result

        except Exception as e:
            self.stderr.write(f'❌ ОШИБКА при загрузке игр: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            return self.empty_result()

    def _handle_failed_loading(self, iteration_start_time, errors, actual_offset):
        """Обрабатывает неудачную загрузку"""
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

    def _process_standard_game_data(self, result, params, iteration_start_time, errors):
        """Обрабатывает стандартную загрузку игр (не update-missing-data)"""
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

        if params['count_only']:
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
                'last_checked_offset': result.get('last_checked_offset', 0),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        result_stats = None

        total_games = result.get('new_games_count', 0)
        progress_bar = ProcessingProgressBar(self.stdout, total_games)

        try:
            collector = DataCollector(self.stdout, self.stderr)
            loader = DataLoader(self.stdout, self.stderr)
            handler = RelationsHandler(self.stdout, self.stderr)
            stats = Statistics(self.stdout, self.stderr)

            interrupted = threading.Event()

            def signal_handler(sig, frame):
                interrupted.set()
                loader.set_interrupted()
                progress_bar.clear()
                if params['debug']:
                    self.stdout.write('\n   ⏹️  Получен сигнал прерывания в обработке данных')

            original_sigint = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, signal_handler)

            try:
                progress_bar.start_step('collecting_ids', f'{total_games} игр')
                collected_data = collector.collect_all_data_ids(result['new_games'], params['debug'])
                game_ids_count = len(collected_data.get('all_game_ids', []))
                progress_bar.complete_step('collecting_ids', f'{game_ids_count} ID собрано')

                if interrupted.is_set():
                    progress_bar.clear()
                    raise KeyboardInterrupt()

                progress_bar.start_step('creating_games', 'создание...')
                created_count, game_basic_map, skipped_games = loader.create_basic_games(
                    result['new_games'], params['debug']
                )
                progress_bar.complete_step('creating_games', f'создано: {created_count}, пропущено: {skipped_games}')

                if created_count == 0:
                    signal.signal(signal.SIGINT, original_sigint)
                    progress_bar.show_summary(time.time() - iteration_start_time, 0)
                    return {
                        'created_count': 0,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                if interrupted.is_set():
                    progress_bar.clear()
                    signal.signal(signal.SIGINT, original_sigint)
                    progress_bar.show_summary(time.time() - iteration_start_time, created_count)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                progress_bar.start_step('loading_all_types', 'обложки, жанры, платформы...')

                data_maps, step_times = loader.load_all_data_types_sequentially(
                    collected_data, params['debug']
                )

                covers_count = len(data_maps.get('cover_map', {}))
                genres_count = len(data_maps.get('genre_map', {}))
                platforms_count = len(data_maps.get('platform_map', {}))

                progress_bar.complete_step('loading_all_types',
                                           f'обложек: {covers_count}, жанров: {genres_count}, платформ: {platforms_count}'
                                           )

                screenshots_info = collected_data.get('screenshots_info', {})

                screenshots_loaded = 0
                if 'all_screenshot_games' in collected_data and screenshots_info:
                    progress_bar.start_step('loading_screenshots', 'загрузка...')

                    screenshots_loaded = loader.load_screenshots_parallel(
                        collected_data['all_screenshot_games'],
                        collected_data['game_data_map'],
                        screenshots_info,
                        params['debug']
                    )

                    progress_bar.complete_step('loading_screenshots', f'загружено: {screenshots_loaded}')

                progress_bar.start_step('loading_additional', 'компании, серии, темы...')

                additional_data_map, additional_ids = loader.load_and_process_additional_data(
                    list(game_basic_map.keys()),
                    collected_data['game_data_map'],
                    screenshots_info,
                    params['debug']
                )

                collected_data.update(additional_ids)

                companies_count = len(additional_ids.get('all_company_ids', []))
                series_count = len(additional_ids.get('all_series_ids', []))

                progress_bar.complete_step('loading_additional',
                                           f'компаний: {companies_count}, серий: {series_count}'
                                           )

                if interrupted.is_set():
                    progress_bar.clear()
                    signal.signal(signal.SIGINT, original_sigint)
                    progress_bar.show_summary(time.time() - iteration_start_time, created_count)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                        'screenshots_loaded': screenshots_loaded,
                    }, errors

                progress_bar.start_step('updating_covers', 'проверка и обновление...')

                cover_updates = loader.update_games_with_covers(
                    game_basic_map, data_maps.get('cover_map', {}),
                    collected_data['game_data_map'], params['debug']
                )

                progress_bar.complete_step('updating_covers', f'обновлено: {cover_updates}')

                if interrupted.is_set():
                    progress_bar.clear()
                    signal.signal(signal.SIGINT, original_sigint)
                    progress_bar.show_summary(time.time() - iteration_start_time, created_count)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                        'screenshots_loaded': screenshots_loaded,
                    }, errors

                progress_bar.start_step('preparing_relations', 'подготовка M2M связей...')

                all_game_relations, relations_prep_time = handler.prepare_game_relations(
                    game_basic_map, collected_data['game_data_map'],
                    additional_data_map, data_maps, params['debug']
                )

                relations_count = len(all_game_relations)
                progress_bar.complete_step('preparing_relations', f'подготовлено: {relations_count} связей')

                progress_bar.start_step('creating_relations', 'создание связей...')

                relations_results, relations_possible, relations_time = handler.create_all_relations(
                    all_game_relations, params['debug']
                )

                genres_relations = relations_results.get('genre_relations', 0)
                platforms_relations = relations_results.get('platform_relations', 0)
                keywords_relations = relations_results.get('keyword_relations', 0)

                progress_bar.complete_step('creating_relations',
                                           f'жанров: {genres_relations}, платформ: {platforms_relations}, ключ. слов: {keywords_relations}'
                                           )

                total_time = time.time() - iteration_start_time
                progress_bar.show_summary(total_time, created_count)

                if params['debug']:
                    loaded_data_stats = {
                        'collected': collected_data,
                        'loaded': {k: len(v) for k, v in data_maps.items()}
                    }

                    step_times['relations_preparation'] = relations_prep_time
                    step_times['relations_creation'] = relations_time

                    objects_stats = stats._collect_objects_statistics(
                        game_basic_map, data_maps, loaded_data_stats, params['debug']
                    )

                    objects_stats['games']['skipped'] = skipped_games
                    objects_stats['screenshots']['created'] = screenshots_loaded

                    relations_stats = stats._collect_relations_statistics(
                        all_game_relations, relations_results, params['debug']
                    )

                    stats._print_detailed_statistics(
                        objects_stats, relations_stats,
                        total_time,
                        params['debug']
                    )

                    final_stats = stats._collect_final_statistics(
                        result['new_games_count'], created_count, skipped_games, screenshots_loaded,
                        total_time, loaded_data_stats, step_times,
                        relations_results, relations_possible, params['debug']
                    )

                    stats._print_complete_statistics(final_stats)

                result_stats = {
                    'created_count': created_count,
                    'skipped_count': skipped_games,
                    'total_time': total_time,
                    'screenshots_loaded': screenshots_loaded,
                    'relations_created': sum(relations_results.values()) if relations_results else 0,
                }

            finally:
                signal.signal(signal.SIGINT, original_sigint)

        except KeyboardInterrupt:
            progress_bar.clear()
            self.stdout.write('\n   ⏹️  Прерывание в обработке данных')
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }
            errors += 1
        except Exception as e:
            errors += 1
            progress_bar.clear()
            self.stderr.write(f'❌ ОШИБКА при обработке данных: {str(e)}')
            if params['debug']:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }

        if result_stats is None:
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
                'errors': errors
            }
        else:
            result_stats['errors'] = errors

        return result_stats

    def _update_existing_game_data(self, result, params, iteration_start_time, errors):
        """Обновляет данные существующих игр"""
        try:
            updated_count = 0
            failed_count = 0
            update_details = []

            games_to_process = []
            if result.get('all_found_games'):
                games_to_process = result['all_found_games']
            elif result.get('new_games'):
                games_to_process = result['new_games']

            if params['debug']:
                self.stdout.write(f'\n🔄 ОБНОВЛЕНИЕ ДАННЫХ ДЛЯ СУЩЕСТВУЮЩИХ ИГР')
                self.stdout.write(f'   • Всего найдено игр для обработки: {len(games_to_process)}')

            for i, game_data in enumerate(games_to_process, 1):
                game_id = game_data.get('id')
                game_name = game_data.get('name', f'ID {game_id}')

                if params['debug']:
                    self.stdout.write(f'\n   🔄 [{i}/{len(games_to_process)}] Обновление: {game_name}')

                if game_id:
                    success, details = self.update_missing_game_data(game_id, params['debug'])
                    # ИЗМЕНЕНИЕ: Проверяем, были ли реальные обновления
                    if success and details and not details.get('skipped', False):
                        if details.get('updated_fields') or details.get('updated_relations') or details.get(
                                'screenshots_added', 0) > 0:
                            updated_count += 1
                            update_details.append({
                                'game_name': game_name,
                                'game_id': game_id,
                                'details': details
                            })
                            if params['debug']:
                                self.stdout.write(f'   ✅ Успешно обновлена: {game_name}')
                        else:
                            if params['debug']:
                                self.stdout.write(f'   ⏭️ Пропущена (нет изменений): {game_name}')
                    elif success and details.get('skipped', False):
                        if params['debug']:
                            self.stdout.write(f'   ⏭️ Пропущена: {details.get("reason", "неизвестная причина")}')
                    else:
                        failed_count += 1
                        if params['debug']:
                            self.stdout.write(f'   ❌ Не удалось обновить: {game_name}')
                else:
                    failed_count += 1
                    if params['debug']:
                        self.stdout.write(f'   ❌ Нет ID у игры: {game_name}')

            iteration_time = time.time() - iteration_start_time

            self._log_batch_update(update_details, len(games_to_process), updated_count,
                                   failed_count, iteration_start_time, time.time(), params['debug'])

            self.stdout.write(f'\n' + '=' * 60)
            self.stdout.write(f'📊 ФИНАЛЬНАЯ СТАТИСТИКА ОБНОВЛЕНИЯ ДАННЫХ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🔄 ОБРАБОТАНО ИГР: {len(games_to_process)}')
            self.stdout.write(f'✅ УСПЕШНО ОБНОВЛЕНО: {updated_count}')
            self.stdout.write(f'❌ НЕ УДАЛОСЬ ОБНОВИТЬ: {failed_count}')
            self.stdout.write(f'⏭️  ПРОПУЩЕНО (НЕТ ИЗМЕНЕНИЙ): {len(games_to_process) - updated_count - failed_count}')
            self.stdout.write(f'⏱️  ВРЕМЯ: {iteration_time:.2f}с')

            if iteration_time > 0:
                speed = len(games_to_process) / iteration_time
                self.stdout.write(f'🚀 Скорость: {speed:.1f} игр/сек')

            return {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': len(games_to_process),
                'created_count': 0,
                'skipped_count': result.get('existing_games_skipped', 0),
                'updated_count': updated_count,
                'update_details': update_details,
                'total_time': iteration_time,
                'errors': errors + failed_count,
                'last_checked_offset': result.get('last_checked_offset', 0),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        except Exception as e:
            errors += 1
            self.stderr.write(f'❌ ОШИБКА при обновлении данных: {str(e)}')
            if params['debug']:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())

            iteration_time = time.time() - iteration_start_time
            return {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': result.get('new_games_count', 0),
                'created_count': 0,
                'skipped_count': result.get('existing_games_skipped', 0),
                'updated_count': 0,
                'update_details': [],
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', 0),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

    def _setup_update_covers_environment(self, options, debug, original_offset):
        """Настройка окружения для режима обновления обложек - оптимизированная"""
        from games.models import Game
        import time

        self.stdout.write(f'\n🖼️  ЗАПУСК ОБНОВЛЕНИЯ ОБЛОЖЕК ДЛЯ ИГР В БАЗЕ')
        self.stdout.write('=' * 60)
        self.stdout.write(f'📍 Начинаем с offset: {original_offset}')

        total_in_db = Game.objects.count()
        if debug:
            self.stdout.write(f'📊 Всего игр в базе: {total_in_db}')

        update_all_covers = not any([
            options.get('game_names'),
            options.get('game_modes'),
            options.get('genres'),
            options.get('description_contains'),
            options.get('keywords')
        ])

        start_time = time.time()

        try:
            if update_all_covers:
                if debug:
                    self.stdout.write(f'🎯 РЕЖИМ: ОБНОВЛЕНИЕ ВСЕХ ОБЛОЖЕК')
                updated_count = self.update_all_game_covers(options, debug)
            else:
                if debug:
                    self.stdout.write(f'🎯 РЕЖИМ: ОБНОВЛЕНИЕ ПО ФИЛЬТРАМ')
                updated_count = self.update_filtered_game_covers(options, debug)

            total_time = time.time() - start_time

            self.stdout.write(f'\n' + '=' * 60)
            self.stdout.write(f'✅ ОБНОВЛЕНИЕ ОБЛОЖЕК ЗАВЕРШЕНО!')
            self.stdout.write(f'🖼️  Обновлено обложек: {updated_count}')
            self.stdout.write(f'⏱️  Общее время: {total_time:.2f}с')

            if total_time > 0 and updated_count > 0:
                speed = updated_count / total_time
                self.stdout.write(f'🚀 Скорость обновления: {speed:.1f} обложек/сек')

            return None, None, None, None, None, None, True

        except KeyboardInterrupt:
            total_time = time.time() - start_time
            self.stdout.write(f'\n🛑 ОБНОВЛЕНИЕ ОБЛОЖЕК ПРЕРВАНО')
            self.stdout.write(f'⏱️  Время выполнения: {total_time:.2f}с')
            raise

        except Exception as e:
            total_time = time.time() - start_time
            self.stderr.write(f'\n❌ Ошибка при обновлении обложек: {str(e)}')
            self.stderr.write(f'⏱️  Время до ошибки: {total_time:.2f}с')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            return None, None, None, None, None, None, True

    def update_all_game_covers(self, options, debug=False):
        """Обновляет обложки для всех игр в базе - с прогресс-баром"""
        from games.models import Game
        import time
        import concurrent.futures

        MAX_WORKERS = OPTIMAL_CONFIG['MAX_WORKERS']
        BATCH_SIZE = OPTIMAL_CONFIG['BATCH_SIZE']
        DELAY_BETWEEN_BATCHES = OPTIMAL_CONFIG['DELAY_BETWEEN_REQUESTS']

        offset = options.get('offset', 0)
        limit = options.get('limit', 0)

        if offset == 0 and not options.get('reset_offset', False):
            saved_offset = self._get_saved_offset_for_update_covers(options)
            if saved_offset is not None:
                offset = saved_offset

        all_games_query = Game.objects.all().order_by('id')
        total_in_db = Game.objects.count()

        if offset > 0:
            all_games_query = all_games_query[offset:]

        all_game_ids = list(all_games_query.values_list('igdb_id', flat=True))

        if limit > 0:
            all_game_ids = all_game_ids[:limit]

        total_games = len(all_game_ids)

        if total_games == 0:
            self.stdout.write('❌ В базе нет игр для обновления обложек')
            return 0

        progress_bar = None
        if not debug:
            progress_bar = self.create_progress_bar(total_games)
            progress_bar.desc = "Обновление обложек"
            progress_bar.update(
                total_loaded=0,
                current_iteration=1,
                iterations_without_new=0,
                created_count=0,
                updated_count=0,
                skipped_count=0,
                processed_count=0,
                errors=0
            )

        if debug:
            self.stdout.write(f'\n🎯 ЗАПУСК ОБНОВЛЕНИЯ ОБЛОЖЕК')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🎮 Всего игр в базе: {total_in_db}')
            self.stdout.write(f'📍 Будет обновлено: {total_games} игр')
            self.stdout.write(f'⚡ Параллельных воркеров: {MAX_WORKERS}')
            self.stdout.write(f'📦 Размер пачки: {BATCH_SIZE} игр')
            self.stdout.write(f'⏸️  Задержка между пачками: {DELAY_BETWEEN_BATCHES} сек')
            self.stdout.write('=' * 60)
        else:
            self.stdout.write(f'\n🖼️  ОБНОВЛЕНИЕ ОБЛОЖЕК ДЛЯ ИГР')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🎮 Всего игр в базе: {total_in_db}')
            self.stdout.write(f'📍 Будет обновлено: {total_games} игр')
            if offset > 0:
                self.stdout.write(f'📍 Начинаем с offset: {offset}')
            self.stdout.write('=' * 60)

        start_time = time.time()
        updated_count = 0
        processed_count = 0
        error_count = 0

        games_by_igdb_id = {}
        try:
            games = Game.objects.filter(igdb_id__in=all_game_ids).only('id', 'igdb_id', 'cover_url', 'name')
            for game in games:
                games_by_igdb_id[game.igdb_id] = game
        except Exception as e:
            self.stderr.write(f'❌ Ошибка загрузки игр из базы: {e}')
            if progress_bar:
                progress_bar.final_message("❌ Ошибка загрузки игр из базы")
                progress_bar.clear()
            return 0

        all_batches = []
        for i in range(0, len(all_game_ids), BATCH_SIZE):
            batch = all_game_ids[i:i + BATCH_SIZE]
            if batch:
                all_batches.append(batch)

        total_batches = len(all_batches)
        all_updates = {}

        try:
            for group_start in range(0, total_batches, MAX_WORKERS):
                group_end = min(group_start + MAX_WORKERS, total_batches)
                current_group = all_batches[group_start:group_end]

                if debug:
                    progress_percent = (group_end / total_batches) * 100
                    games_processed = group_end * BATCH_SIZE
                    self.stdout.write(f'📊 Прогресс: {progress_percent:.1f}% ({games_processed}/{total_games} игр)')

                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = []
                    for i, batch in enumerate(current_group):
                        future = executor.submit(
                            self._load_single_update_batch,
                            group_start + i + 1,
                            batch,
                            games_by_igdb_id,
                            debug
                        )
                        futures.append(future)

                    for future in concurrent.futures.as_completed(futures):
                        try:
                            batch_updated, batch_updates = future.result(timeout=20)
                            updated_count += batch_updated

                            for update in batch_updates:
                                if isinstance(update, dict) and 'id' in update and 'cover_url' in update:
                                    all_updates[update['id']] = update['cover_url']

                        except concurrent.futures.TimeoutError:
                            error_count += 1
                        except Exception as e:
                            error_count += 1
                            if debug:
                                self.stderr.write(f'   ❌ Ошибка future: {e}')

                for batch in current_group:
                    processed_count += len(batch)

                if progress_bar and not debug:
                    progress_bar.update(
                        total_loaded=processed_count,
                        current_iteration=1,
                        iterations_without_new=0,
                        created_count=0,
                        updated_count=updated_count,
                        skipped_count=processed_count - updated_count,
                        processed_count=processed_count,
                        errors=error_count
                    )

                if group_end < total_batches:
                    time.sleep(DELAY_BETWEEN_BATCHES)

        except KeyboardInterrupt:
            next_offset = offset + processed_count
            self._save_offset_for_update_covers(options, next_offset)

            if progress_bar:
                progress_bar.final_message(f"🛑 Прервано на {processed_count}/{total_games} игр")
                progress_bar.clear()

            self.stdout.write(f'\n🛑 ОБНОВЛЕНИЕ ПРЕРВАНО')
            self.stdout.write(f'📍 Обработано: {processed_count}/{total_games} игр')
            self.stdout.write(f'📍 Обновлено: {updated_count} обложек')
            self.stdout.write(f'📍 Следующий offset: {next_offset}')
            raise

        if all_updates:
            try:
                from django.db.models import Case, When, Value
                from django.db.models import CharField

                when_conditions = []
                for game_id, cover_url in all_updates.items():
                    when_conditions.append(When(id=game_id, then=Value(cover_url)))

                Game.objects.filter(id__in=all_updates.keys()).update(
                    cover_url=Case(*when_conditions, default=Value(''), output_field=CharField())
                )

            except Exception as e:
                if debug:
                    self.stderr.write(f'❌ Ошибка массового сохранения: {e}')

        next_offset = offset + processed_count
        self._save_offset_for_update_covers(options, next_offset)

        total_time = time.time() - start_time

        if progress_bar and not debug:
            if updated_count > 0:
                progress_bar.final_message(f"✅ Обновлено {updated_count}/{total_games} обложек")
            else:
                progress_bar.final_message(f"⚠️  Нет обновлений ({processed_count} игр проверено)")
            progress_bar.clear()

        self.stdout.write(f'\n📊 ИТОГОВАЯ СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'👀 Обработано игр: {processed_count}/{total_games}')
        self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
        self.stdout.write(f'⏭️  Пропущено (уже актуальны): {processed_count - updated_count}')
        self.stdout.write(f'❌ Ошибок: {error_count}')
        self.stdout.write(f'⏱️  Время: {total_time:.2f}с')

        if total_time > 0:
            games_per_second = processed_count / total_time
            self.stdout.write(f'🚀 Скорость: {games_per_second:.1f} игр/сек')

        return updated_count

    def _get_saved_offset_for_update_covers(self, options):
        """Получает сохраненный offset для режима обновления обложек"""
        params = self._get_offset_params_for_update_covers(options)
        return OffsetManager.load_offset(params)

    def _save_offset_for_update_covers(self, options, current_offset):
        """Сохраняет offset для режима обновления обложек"""
        params = self._get_offset_params_for_update_covers(options)
        saved = OffsetManager.save_offset(params, current_offset)

        if saved:
            if options.get('debug', False):
                self.stdout.write(f'   💾 Offset для обновления обложек сохранен: {current_offset}')
        return saved

    def _get_offset_params_for_update_covers(self, options):
        """Получает параметры для создания ключа offset для режима обновления обложек"""
        params = {
            'update_covers': True,
            'game_modes': options.get('game_modes', ''),
            'game_names': options.get('game_names', ''),
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': 'update_covers',
        }

        has_filters = any([
            options.get('game_names'),
            options.get('game_modes'),
            options.get('genres'),
            options.get('description_contains'),
            options.get('keywords')
        ])

        if has_filters:
            params['has_filters'] = True
        else:
            params['has_filters'] = False

        return params

    def update_filtered_game_covers(self, options, debug=False):
        """Обновляет обложки для игр по фильтрам - максимально оптимизированная версия"""
        from games.models import Game
        from django.db.models import Q
        import time
        import concurrent.futures

        offset = options.get('offset', 0)
        limit = options.get('limit', 0)
        game_names = options.get('game_names', '')
        game_modes = options.get('game_modes', '')
        genres = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords = options.get('keywords', '')

        if game_names:
            offset = 0

        query = Game.objects.all()

        if game_names:
            name_list = [n.strip() for n in game_names.split(',') if n.strip()]
            name_filters = Q()
            for name in name_list:
                name_filters |= Q(name__icontains=name)
            query = query.filter(name_filters)

        if genres:
            genre_list = [g.strip() for g in genres.split(',') if g.strip()]
            for genre in genre_list:
                query = query.filter(genres__name__icontains=genre)

        if description_contains:
            text = description_contains
            query = query.filter(Q(summary__icontains=text) | Q(storyline__icontains=text))

        if keywords:
            keyword_list = [k.strip() for k in keywords.split(',') if k.strip()]
            for keyword in keyword_list:
                query = query.filter(keywords__name__icontains=keyword)

        query = query.order_by('id')

        games = list(query.values_list('id', 'igdb_id', 'cover_url', 'name'))
        total_games = len(games)

        if total_games == 0:
            self.stdout.write('❌ Не найдено игр для обновления обложек')
            return 0

        if limit > 0:
            games = games[:limit]
            total_games = len(games)

        start_time = time.time()
        updated_count = 0
        error_count = 0

        games_by_id = {}
        games_by_igdb_id = {}
        for game_id, igdb_id, cover_url, name in games:
            games_by_id[game_id] = {
                'igdb_id': igdb_id,
                'cover_url': cover_url,
                'name': name
            }
            games_by_igdb_id[igdb_id] = game_id

        all_igdb_ids = [igdb_id for _, igdb_id, _, _ in games]

        if debug:
            self.stdout.write(f'\n🎯 ОБНОВЛЕНИЕ ОБЛОЖЕК ПО ФИЛЬТРАМ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'👀 Найдено игр по фильтрам: {total_games}')
            self.stdout.write(f'⚡ Параллельных воркеров: 8')
            self.stdout.write(f'📦 Размер пачки: 10 игр')
            self.stdout.write('=' * 60)

        BATCH_SIZE = 10
        MAX_WORKERS = 8
        all_batches = []

        for i in range(0, len(all_igdb_ids), BATCH_SIZE):
            batch = all_igdb_ids[i:i + BATCH_SIZE]
            if batch:
                all_batches.append(batch)

        total_batches = len(all_batches)

        if debug:
            self.stdout.write(f'📊 Создано {total_batches} пачек')

        def process_batch(batch_num, batch_igdb_ids):
            try:
                id_list = ','.join(map(str, batch_igdb_ids))
                query = f'fields id,cover.image_id; where id = ({id_list});'

                games_data = make_igdb_request('games', query, debug=False)
                if not games_data:
                    return batch_num, [], 0

                updates = []
                local_updated = 0

                for game_data in games_data:
                    igdb_id = game_data.get('id')
                    if not igdb_id or igdb_id not in games_by_igdb_id:
                        continue

                    game_id = games_by_igdb_id[igdb_id]
                    game_info = games_by_id[game_id]

                    cover_data = game_data.get('cover', {})
                    image_id = cover_data.get('image_id')

                    if not image_id:
                        continue

                    new_cover_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"
                    current_cover_url = game_info['cover_url'] or ""

                    if current_cover_url != new_cover_url:
                        updates.append({
                            'id': game_id,
                            'cover_url': new_cover_url
                        })
                        local_updated += 1

                return batch_num, updates, len(batch_igdb_ids), local_updated

            except Exception as e:
                if debug:
                    self.stderr.write(f'      ❌ Ошибка в пачке {batch_num}: {e}')
                return batch_num, [], len(batch_igdb_ids), 0

        all_updates = []
        processed_count = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_batch = {}
            for batch_num, batch_ids in enumerate(all_batches, 1):
                future = executor.submit(process_batch, batch_num, batch_ids)
                future_to_batch[future] = batch_num

            completed = 0

            for future in concurrent.futures.as_completed(future_to_batch):
                batch_num = future_to_batch[future]
                try:
                    batch_num, updates, processed, local_updated = future.result(timeout=15)

                    all_updates.extend(updates)
                    updated_count += local_updated
                    processed_count += processed
                    completed += 1

                    if debug and completed % 10 == 0:
                        progress = (completed / total_batches) * 100
                        self.stdout.write(f'   📊 Прогресс: {progress:.1f}% ({completed}/{total_batches})')

                except concurrent.futures.TimeoutError:
                    error_count += 1
                    if debug:
                        self.stdout.write(f'   ⏱️  Таймаут пачки {batch_num}')
                except Exception as e:
                    error_count += 1
                    if debug:
                        self.stderr.write(f'   ❌ Ошибка пачки {batch_num}: {e}')

        if all_updates:
            try:
                update_dict = {}
                for update in all_updates:
                    update_dict[update['id']] = update['cover_url']

                games_to_update = []
                for game_id, cover_url in update_dict.items():
                    try:
                        game = Game.objects.get(id=game_id)
                        game.cover_url = cover_url
                        games_to_update.append(game)
                    except Game.DoesNotExist:
                        continue

                if games_to_update:
                    Game.objects.bulk_update(games_to_update, ['cover_url'])

            except Exception as e:
                if debug:
                    self.stderr.write(f'   ❌ Ошибка массового обновления: {e}')
                for update in all_updates:
                    try:
                        Game.objects.filter(id=update['id']).update(cover_url=update['cover_url'])
                    except Exception:
                        continue

        total_time = time.time() - start_time

        self.stdout.write(f'\n' + '=' * 60)
        self.stdout.write(f'📊 ИТОГОВАЯ СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'👀 Обработано игр: {processed_count}/{total_games}')
        self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
        self.stdout.write(f'⏭️  Пропущено (уже актуальны): {processed_count - updated_count}')
        self.stdout.write(f'❌ Ошибок: {error_count}')
        self.stdout.write(f'⏱️  Общее время: {total_time:.2f}с')

        if total_time > 0:
            games_per_second = processed_count / total_time
            self.stdout.write(f'🚀 Скорость обработки: {games_per_second:.1f} игр/сек')

        return updated_count

    def _setup_update_mode_environment(self, options, debug, original_offset):
        """Настройка окружения для режима обновления данных"""
        update_all_games = not any([
            options.get('game_names'),
            options.get('game_modes'),
            options.get('genres'),
            options.get('description_contains'),
            options.get('keywords')
        ])

        if update_all_games:
            self.stdout.write(f'\n🎯 ЗАПУСК ОБНОВЛЕНИЯ ДАННЫХ ДЛЯ ВСЕХ ИГР В БАЗЕ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'📍 Начинаем с offset: {original_offset}')
            self.stdout.write('=' * 60)

            try:
                updated_count, update_details = self.update_all_games_missing_data(options, debug)

                self.stdout.write(f'\n' + '=' * 60)
                self.stdout.write(f'✅ ОБНОВЛЕНИЕ ВСЕХ ИГР ЗАВЕРШЕНО!')
                self.stdout.write(f'🎯 Обновлено игр: {updated_count}')

            except KeyboardInterrupt:
                self.stdout.write(f'\n🛑 ОБНОВЛЕНИЕ ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ')
                return None, None, None, None, None, None, True

            except Exception as e:
                self.stderr.write(f'\n❌ Ошибка при обновлении: {str(e)}')
                if debug:
                    import traceback
                    self.stderr.write(f'📋 Трассировка ошибки:')
                    self.stderr.write(traceback.format_exc())
                return None, None, None, None, None, None, True

            return None, None, None, None, None, None, True

        self.stdout.write(f'\n🎯 ЗАПУСК ОБНОВЛЕНИЯ ДАННЫХ ДЛЯ ИГР ПО ФИЛЬТРАМ')
        self.stdout.write('=' * 60)
        self.stdout.write(f'📍 Начинаем с offset: {original_offset}')
        self.stdout.write('=' * 60)

        try:
            updated_count, update_details = self._handle_update_mode_with_filters(options, debug)

            self.stdout.write(f'\n' + '=' * 60)
            self.stdout.write(f'✅ ОБНОВЛЕНИЕ ПО ФИЛЬТРАМ ЗАВЕРШЕНО!')
            self.stdout.write(f'🎯 Обновлено игр: {updated_count}')

        except KeyboardInterrupt:
            self.stdout.write(f'\n🛑 ОБНОВЛЕНИЕ ПРЕРВАНО ПОЛЬЗОВАТЕЛЕМ')
            return None, None, None, None, None, None, True

        except Exception as e:
            self.stderr.write(f'\n❌ Ошибка при обновлении: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            return None, None, None, None, None, None, True

        return None, None, None, None, None, None, True

    def _handle_update_mode_with_filters(self, options, debug):
        """Обрабатывает режим обновления данных с фильтрами и offset"""
        from games.models import Game
        from django.db.models import Q
        import time

        offset = options.get('offset', 0)
        limit = options.get('limit', 0)
        game_names = options.get('game_names', '')
        game_modes = options.get('game_modes', '')
        genres = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords = options.get('keywords', '')

        query = Game.objects.all()

        if game_names:
            name_list = [n.strip() for n in game_names.split(',') if n.strip()]
            name_filters = Q()
            for name in name_list:
                name_filters |= Q(name__icontains=name)
            query = query.filter(name_filters)
            if debug:
                self.stdout.write(f'   🔍 Фильтр по именам: {name_list}')

        if genres:
            genre_list = [g.strip() for g in genres.split(',') if g.strip()]
            for genre in genre_list:
                query = query.filter(genres__name__icontains=genre)
            if debug:
                self.stdout.write(f'   🔍 Фильтр по жанрам: {genre_list}')

        if description_contains:
            text = description_contains
            query = query.filter(Q(summary__icontains=text) | Q(storyline__icontains=text))
            if debug:
                self.stdout.write(f'   🔍 Фильтр по тексту: "{text}"')

        if keywords:
            keyword_list = [k.strip() for k in keywords.split(',') if k.strip()]
            for keyword in keyword_list:
                query = query.filter(keywords__name__icontains=keyword)
            if debug:
                self.stdout.write(f'   🔍 Фильтр по ключевым словам: {keyword_list}')

        if game_modes and debug:
            self.stdout.write(f'   ⚠️  Фильтр по режимам игры пока не поддерживается в режиме обновления')

        query = query.order_by('id')

        if offset > 0:
            query = query[offset:]

        if limit > 0:
            game_ids = list(query.values_list('igdb_id', flat=True)[:limit])
        else:
            game_ids = list(query.values_list('igdb_id', flat=True))

        total_games = len(game_ids)

        if debug:
            self.stdout.write(f'   📊 Найдено игр по фильтрам: {total_games}')
            self.stdout.write(f'   📍 Начальный offset: {offset}')
            if limit > 0:
                self.stdout.write(f'   🎯 Лимит обновления: {limit} игр')

        if total_games == 0:
            self.stdout.write('❌ Не найдено игр для обновления')
            return 0, []

        progress_bar = None
        if not debug:
            progress_bar = self.create_progress_bar(total_games)
            progress_bar.desc = "Обновление данных игр"
            progress_bar.update(
                total_loaded=0,
                current_iteration=1,
                iterations_without_new=0,
                created_count=0,
                updated_count=0,
                skipped_count=0,
                processed_count=0,
                errors=0
            )

        start_time = time.time()
        updated_count = 0
        update_details = []

        try:
            for i, game_id in enumerate(game_ids, 1):
                if progress_bar:
                    progress_bar.update(
                        total_loaded=i,
                        current_iteration=1,
                        iterations_without_new=0,
                        created_count=0,
                        updated_count=updated_count,
                        skipped_count=i - updated_count - 1,
                        processed_count=i,
                        errors=0
                    )

                if debug:
                    self.stdout.write(f'\n   🔄 [{i}/{total_games}] Обновление игры ID: {game_id}')

                game = Game.objects.filter(igdb_id=game_id).first()
                if not game:
                    if debug:
                        self.stdout.write(f'      ❌ Игра с ID {game_id} не найдена в базе')
                    continue

                missing_data, missing_count, cover_status = self.check_missing_game_data(game)

                if debug:
                    self.stdout.write(f'      🔍 "{game.name}": {missing_count} недостающих данных')
                    self.stdout.write(f'      📋 Статус обложки: {cover_status}')
                    if missing_count > 0:
                        missing_list = [key.replace('has_', '') for key, has_data in missing_data.items() if
                                        not has_data]
                        self.stdout.write(f'      📋 Отсутствует: {", ".join(missing_list)}')

                if missing_count == 0:
                    if debug:
                        self.stdout.write(f'      ⏭️  Все данные уже есть, пропускаем')
                    continue

                success, details = self.update_missing_game_data(game_id, debug)

                if success:
                    updated_count += 1

                    update_details.append({
                        'game_name': game.name,
                        'game_id': game_id,
                        'details': details
                    })

                    if debug:
                        if details.get('updated_fields'):
                            self.stdout.write(f'      ✅ Обновлены поля: {", ".join(details["updated_fields"])}')
                        if details.get('updated_relations'):
                            self.stdout.write(f'      🔗 Добавлены связи: {", ".join(details["updated_relations"])}')
                        if details.get('screenshots_added', 0) > 0:
                            self.stdout.write(f'      📸 Добавлено скриншотов: {details["screenshots_added"]}')
                else:
                    if debug:
                        self.stdout.write(f'      ❌ Не удалось обновить: {game.name}')

            total_time = time.time() - start_time

            if progress_bar:
                progress_bar.final_message(
                    f"✅ Обновлено: {updated_count} | ⏭️  Пропущено: {total_games - updated_count}"
                )
                progress_bar.clear()

            next_offset = offset + total_games
            self.save_offset_for_continuation(options, next_offset)

            if debug:
                self.stdout.write(f'\n' + '=' * 60)
                self.stdout.write(f'📊 ИТОГОВАЯ СТАТИСТИКА ОБНОВЛЕНИЯ:')
                self.stdout.write('=' * 60)
                self.stdout.write(f'📍 Обработано игр: {total_games}')
                self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
                self.stdout.write(f'⏭️  Пропущено (все данные уже есть): {total_games - updated_count}')
                self.stdout.write(f'⏱️  Время: {total_time:.2f}с')
                self.stdout.write(f'📍 Следующий offset для продолжения: {next_offset}')

            return updated_count, update_details

        except KeyboardInterrupt:
            if progress_bar:
                processed_count = progress_bar.total_loaded
            else:
                processed_count = len(update_details) + (total_games - updated_count)

            next_offset = offset + processed_count

            self.save_offset_for_continuation(options, next_offset)

            if progress_bar:
                progress_bar.final_message(
                    f"🛑 Прервано на: {processed_count}/{total_games} игр"
                )
                progress_bar.clear()

            self.stdout.write(f'\n🛑 КОМАНДА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')
            self.stdout.write(f'📍 Обработано игр: {processed_count}/{total_games}')
            self.stdout.write(f'📍 Следующий offset: {next_offset}')

            raise

    def update_all_games_missing_data(self, options, debug=False):
        """Обновляет недостающие данные для всех игр в базе - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
        from games.models import Game
        from collections import defaultdict
        import time
        import concurrent.futures
        import signal
        import sys

        offset = options.get('offset', 0)
        limit = options.get('limit', 0)

        use_cache = options.get('use_cache', True)
        cache_ttl = options.get('cache_ttl', 3600)

        if debug:
            self.stdout.write(f'   🔧 Параметры кэширования: use_cache={use_cache}, cache_ttl={cache_ttl}с')

        total_start_time = time.time()

        self.interrupted = threading.Event()

        def signal_handler(sig, frame):
            self.interrupted.set()
            self.stdout.write('\n\n⚠️  ПРЕРЫВАНИЕ (Ctrl+C) - завершаю загрузку из базы данных...')
            sys.exit(1)

        original_sigint = signal.signal(signal.SIGINT, signal_handler)

        if offset == 0 and not options.get('reset_offset', False):
            saved_offset = self.get_saved_offset(options)
            if saved_offset is not None:
                offset = saved_offset
                if debug:
                    self.stdout.write(f'   📍 Загружен сохраненный offset: {offset}')

        try:
            self.stdout.write('\n🔍 Загрузка игр из базы данных...')
            self.stdout.write('   ⏳ Это может занять некоторое время...')
            self.stdout.write('   ❗ Нажмите Ctrl+C для прерывания')

            start_load = time.time()

            games_map, games_needs_update = self._load_all_games_with_relations(
                offset, limit, self.interrupted,
                chunk_size=100,
                use_cache=use_cache,
                cache_ttl=cache_ttl
            )

            load_time = time.time() - start_load

            if games_map:
                self.stdout.write(f'   ✅ Загружено {len(games_map)} игр из БД за {load_time:.1f}с')

            # ВАЖНО: Находим игры, которые ДЕЙСТВИТЕЛЬНО НУЖДАЮТСЯ в обновлении
            games_to_update = {}
            for igdb_id, info in games_needs_update.items():
                if info['missing_count'] > 0:
                    games_to_update[igdb_id] = info

            self.stdout.write(f'📊 Игр, нуждающихся в обновлении: {len(games_to_update)}')

            total_games = len(games_to_update)

            # ЕСЛИ ЕСТЬ ЛИМИТ, БЕРЕМ ТОЛЬКО ПЕРВУЮ ИГРУ
            if limit > 0:
                # Берем первые `limit` игр из списка нуждающихся в обновлении
                limited_games = {}
                count = 0
                for igdb_id, info in games_to_update.items():
                    if count >= limit:
                        break
                    limited_games[igdb_id] = info
                    count += 1

                games_to_update = limited_games
                total_games = len(games_to_update)
                self.stdout.write(f'🎯 Лимит {limit}: обрабатываем {total_games} игр')

            if total_games == 0:
                self.stdout.write('✅ Все игры уже имеют полные данные')
                signal.signal(signal.SIGINT, original_sigint)
                return 0, []

            if debug:
                self.stdout.write(f'\n🎯 Будет обновлено игр: {total_games}')
                missing_stats = defaultdict(int)
                for igdb_id, info in games_to_update.items():
                    for key, has_data in info['missing_data'].items():
                        if not has_data:
                            missing_stats[key] += 1

                self.stdout.write('📊 Статистика недостающих данных:')
                for key, count in sorted(missing_stats.items(), key=lambda x: -x[1]):
                    if count > 0:
                        display_name = {
                            'has_cover': 'Обложки',
                            'has_screenshots': 'Скриншоты',
                            'has_description': 'Описание',
                            'has_rating': 'Рейтинг',
                            'has_release_date': 'Дата релиза',
                            'has_genres': 'Жанры',
                            'has_platforms': 'Платформы',
                            'has_keywords': 'Ключевые слова',
                            'has_engines': 'Движки',
                            'has_series': 'Серии',
                            'has_developers': 'Разработчики',
                            'has_publishers': 'Издатели',
                            'has_themes': 'Темы',
                            'has_perspectives': 'Перспективы',
                            'has_modes': 'Режимы',
                        }.get(key, key)
                        self.stdout.write(f'   • {display_name}: {count} игр')

        except KeyboardInterrupt:
            self.stdout.write('\n⚠️ Загрузка из БД прервана пользователем')
            signal.signal(signal.SIGINT, original_sigint)
            return 0, []
        except SystemExit:
            self.stdout.write('\n⚠️ Загрузка из БД прервана пользователем')
            signal.signal(signal.SIGINT, original_sigint)
            return 0, []
        except Exception as e:
            self.stdout.write(f'\n❌ Ошибка при загрузке из БД: {e}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка: {traceback.format_exc()}')
            signal.signal(signal.SIGINT, original_sigint)
            return 0, []

        signal.signal(signal.SIGINT, original_sigint)

        progress_bar = None
        if not debug:
            # ДОБАВЛЯЕМ ПУСТУЮ СТРОКУ ДЛЯ ОТСТУПА ПЕРЕД ПРОГРЕСС-БАРОМ
            self.stdout.write('')
            progress_bar = self.create_progress_bar(total_games)
            progress_bar.desc = "Обновление данных игр"
            # ИНИЦИАЛИЗАЦИЯ ПРОГРЕСС-БАРА С ПРАВИЛЬНЫМИ ЗНАЧЕНИЯМИ
            progress_bar.update(
                total_loaded=0,
                created_count=0,
                updated_count=0,
                skipped_count=0,
                processed_count=0,
                errors=0
            )

        start_time = time.time()
        updated_count = 0
        all_update_details = []
        processed_count = 0
        skipped_count = 0

        BATCH_SIZE = 10
        igdb_ids = list(games_to_update.keys())

        if debug:
            self.stdout.write(f'\n📦 Разбивка на пачки по {BATCH_SIZE} игр')
            self.stdout.write(f'📊 Всего пачек: {(len(igdb_ids) + BATCH_SIZE - 1) // BATCH_SIZE}')

        batches = [igdb_ids[i:i + BATCH_SIZE] for i in range(0, len(igdb_ids), BATCH_SIZE)]

        self.interrupted.clear()

        def processing_signal_handler(sig, frame):
            self.interrupted.set()
            self.stdout.write('\n\n⚠️  ПРЕРЫВАНИЕ (Ctrl+C) - завершаю обработку...')

        signal.signal(signal.SIGINT, processing_signal_handler)

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                future_to_batch = {}

                for batch_num, batch_ids in enumerate(batches, 1):
                    if self.interrupted.is_set():
                        self.stdout.write('\n⚠️ Прерывание обнаружено, останавливаем обработку...')
                        break

                    if debug:
                        self.stdout.write(f'\n   📦 СОЗДАНИЕ ПАЧКИ {batch_num}: {len(batch_ids)} игр')
                        if batch_ids:
                            self.stdout.write(f'      🆔 ID: {batch_ids[:5]}...')

                    batch_info = []
                    for igdb_id in batch_ids:
                        info = games_to_update[igdb_id]
                        batch_info.append({
                            'igdb_id': igdb_id,
                            'game': info['game'],
                            'missing_data': info['missing_data']
                        })

                    future = executor.submit(
                        self._process_update_batch_optimized,
                        batch_num, batch_info, debug
                    )
                    future_to_batch[future] = (batch_num, len(batch_ids))

                completed_batches = 0
                for future in concurrent.futures.as_completed(future_to_batch):
                    if self.interrupted.is_set():
                        self.stdout.write('\n⚠️ Прерывание обнаружено, отменяем оставшиеся задачи...')
                        for f in future_to_batch:
                            if not f.done():
                                f.cancel()
                        break

                    batch_num, batch_size = future_to_batch[future]
                    completed_batches += 1

                    try:
                        batch_updated, batch_details = future.result(timeout=120)

                        updated_count += batch_updated
                        all_update_details.extend(batch_details)
                        processed_count += batch_size

                        # ИЗМЕНЕНИЕ: Правильно рассчитываем пропущенные игры
                        skipped_count = processed_count - updated_count

                        if progress_bar and not debug:
                            # ИЗМЕНЕНИЕ: Передаем все значения в прогресс-бар
                            progress_bar.update(
                                total_loaded=processed_count,
                                updated_count=updated_count,
                                skipped_count=skipped_count,
                                errors=0
                            )

                        if debug and batch_updated > 0:
                            self.stdout.write(f'      ✅ Пачка {batch_num}: обновлено {batch_updated}/{batch_size} игр')

                        if debug and completed_batches % 10 == 0:
                            percent = (completed_batches / len(batches)) * 100
                            self.stdout.write(
                                f'      📊 Прогресс: {completed_batches}/{len(batches)} пачек ({percent:.1f}%)')

                    except concurrent.futures.TimeoutError:
                        if debug:
                            self.stderr.write(f'      ❌ ТАЙМАУТ ПАЧКИ {batch_num} (120 сек)')
                    except Exception as e:
                        if debug:
                            self.stderr.write(f'      ❌ ОШИБКА ПАЧКИ {batch_num}: {e}')
                            import traceback
                            self.stderr.write(f'         📋 {traceback.format_exc()[:200]}')

        except KeyboardInterrupt:
            self.interrupted.set()
            self.stdout.write(f'\n🛑 Прервано пользователем во время обработки')
            processed = (len(all_update_details) // BATCH_SIZE) * BATCH_SIZE
            next_offset = offset + processed
            self.save_offset_for_continuation(options, next_offset)
            self.stdout.write(f'📍 Сохранен offset: {next_offset}')

            signal.signal(signal.SIGINT, original_sigint)
            raise

        total_time = time.time() - start_time

        next_offset = offset + total_games
        if not self.interrupted.is_set():
            self.save_offset_for_continuation(options, next_offset)

        signal.signal(signal.SIGINT, original_sigint)

        # ИЗМЕНЕНИЕ: Финальное сообщение прогресс-бара
        if progress_bar and not debug:
            progress_bar.final_message(
                f"✅ Обновлено: {updated_count} | ⏭️ Пропущено: {total_games - updated_count}"
            )
            progress_bar.clear()

        self.stdout.write(f'\n' + '=' * 60)
        self.stdout.write(f'📊 ИТОГОВАЯ СТАТИСТИКА ОБНОВЛЕНИЯ')
        self.stdout.write('=' * 60)
        self.stdout.write(f'📍 Начальный offset: {offset}')
        self.stdout.write(f'📍 Обработано игр: {total_games}')
        self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
        self.stdout.write(f'⏭️  Пропущено: {total_games - updated_count}')
        self.stdout.write(f'⏱️  Время обработки: {total_time:.2f}с')
        self.stdout.write(f'⏱️  Общее время: {time.time() - total_start_time:.2f}с')
        self.stdout.write(f'📍 Следующий offset: {next_offset}')

        if total_time > 0:
            speed = total_games / total_time
            self.stdout.write(f'🚀 Скорость обработки: {speed:.1f} игр/сек')

        return updated_count, all_update_details

    def _load_all_games_with_relations(self, offset, limit, interrupted=None, chunk_size=100, use_cache=True,
                                       cache_ttl=3600):
        """Загружает игры с предзагрузкой связей, пока не наберет нужное количество нуждающихся в обновлении"""
        from django.db.models import Count
        from django.core.cache import cache
        from django.conf import settings
        from django.db.models.base import ModelState
        from games.models import Game, Screenshot
        from collections import defaultdict
        import time

        games_map = {}
        games_needs_update = {}

        current_offset = offset
        total_loaded = 0
        games_needed = limit if limit > 0 else float('inf')  # Сколько игр с недостающими данными нужно найти
        games_found_with_issues = 0

        start_time = time.time()
        last_update_time = start_time

        stats = {
            'total_checked': 0,
            'games_with_missing': 0,
            'missing_by_type': defaultdict(int)
        }

        self.stdout.write(f'\n🔍 Поиск игр с недостающими данными (нужно: {games_needed if limit > 0 else "все"})...')

        while games_found_with_issues < games_needed:
            if interrupted and interrupted.is_set():
                self.stdout.write('\n   ⚠️ Загрузка прервана пользователем')
                return games_map, games_needs_update

            query = Game.objects.all().order_by('id').only(
                'id', 'igdb_id', 'name', 'cover_url', 'summary',
                'rating', 'first_release_date'
            )

            if current_offset > 0:
                query = query[current_offset:]

            chunk = list(query[:chunk_size])

            if not chunk:
                self.stdout.write(f'\n   ✅ Достигнут конец базы данных')
                break

            game_ids = [game.id for game in chunk]
            igdb_ids = [game.igdb_id for game in chunk]

            if interrupted and interrupted.is_set():
                return games_map, games_needs_update

            screenshots = Screenshot.objects.filter(game_id__in=game_ids).values_list('game_id', flat=True)
            games_with_screenshots = set(screenshots)

            if interrupted and interrupted.is_set():
                return games_map, games_needs_update

            games_with_counts = Game.objects.filter(id__in=game_ids).annotate(
                genre_count=Count('genres', distinct=True),
                platform_count=Count('platforms', distinct=True),
                keyword_count=Count('keywords', distinct=True),
                engine_count=Count('engines', distinct=True),
                series_count=Count('series', distinct=True),
                developer_count=Count('developers', distinct=True),
                publisher_count=Count('publishers', distinct=True),
                theme_count=Count('themes', distinct=True),
                perspective_count=Count('player_perspectives', distinct=True),
                mode_count=Count('game_modes', distinct=True)
            ).values(
                'id', 'igdb_id',
                'genre_count', 'platform_count', 'keyword_count', 'engine_count',
                'series_count', 'developer_count', 'publisher_count',
                'theme_count', 'perspective_count', 'mode_count'
            )

            if interrupted and interrupted.is_set():
                return games_map, games_needs_update

            counts_map = {}
            for item in games_with_counts:
                counts_map[item['igdb_id']] = item

            chunk_has_issues = False

            for game in chunk:
                if interrupted and interrupted.is_set():
                    return games_map, games_needs_update

                games_map[game.igdb_id] = game
                stats['total_checked'] += 1

                counts = counts_map.get(game.igdb_id, {})

                # ЯВНАЯ ПРОВЕРКА ДАТЫ РЕЛИЗА
                has_release_date = False
                if game.first_release_date:
                    if hasattr(game.first_release_date, 'year') and game.first_release_date.year > 1900:
                        has_release_date = True
                    try:
                        timestamp = int(game.first_release_date.timestamp())
                        if timestamp > 0:
                            has_release_date = True
                    except:
                        pass

                missing_data = {
                    'has_cover': bool(game.cover_url and game.cover_url.strip()),
                    'has_screenshots': game.id in games_with_screenshots,
                    'has_description': bool(game.summary and game.summary.strip()),
                    'has_rating': game.rating is not None,
                    'has_release_date': has_release_date,
                    'has_genres': counts.get('genre_count', 0) > 0,
                    'has_platforms': counts.get('platform_count', 0) > 0,
                    'has_keywords': counts.get('keyword_count', 0) > 0,
                    'has_engines': counts.get('engine_count', 0) > 0,
                    'has_series': counts.get('series_count', 0) > 0,
                    'has_developers': counts.get('developer_count', 0) > 0,
                    'has_publishers': counts.get('publisher_count', 0) > 0,
                    'has_themes': counts.get('theme_count', 0) > 0,
                    'has_perspectives': counts.get('perspective_count', 0) > 0,
                    'has_modes': counts.get('mode_count', 0) > 0,
                }

                missing_count = sum(1 for has_data in missing_data.values() if not has_data)

                if missing_count > 0:
                    games_needs_update[game.igdb_id] = {
                        'game': game,
                        'missing_data': missing_data,
                        'missing_count': missing_count
                    }
                    stats['games_with_missing'] += 1
                    games_found_with_issues += 1
                    chunk_has_issues = True

                    for key, has_data in missing_data.items():
                        if not has_data:
                            stats['missing_by_type'][key] += 1

            total_loaded += len(chunk)
            current_offset += len(chunk)

            if not interrupted or not interrupted.is_set():
                current_time = time.time()
                elapsed = current_time - start_time

                if elapsed > 0:
                    speed = total_loaded / elapsed
                else:
                    speed = 0

                percentage = (total_loaded / Game.objects.count() * 100) if Game.objects.count() > 0 else 0

                status_msg = (f'\r   ⏳ Проверено {total_loaded} игр ({percentage:.1f}%) | '
                              f'Найдено с проблемами: {games_found_with_issues} | '
                              f'🚀 {speed:.1f} игр/с')

                if limit > 0:
                    status_msg += f' | Нужно еще: {games_needed - games_found_with_issues}'

                self.stdout.write(status_msg, ending='')
                self.stdout.flush()
                last_update_time = current_time

            # Если нашли достаточно игр с проблемами - останавливаемся
            if games_found_with_issues >= games_needed:
                self.stdout.write(f'\n   ✅ Найдено достаточно игр с недостающими данными: {games_found_with_issues}')
                break

        self.stdout.write('\r' + ' ' * 100 + '\r')

        total_elapsed = time.time() - start_time
        avg_speed = total_loaded / total_elapsed if total_elapsed > 0 else 0
        self.stdout.write(f'   ✅ Проверено {total_loaded} игр за {total_elapsed:.1f}с')
        self.stdout.write(f'   📊 Найдено игр с недостающими данными: {len(games_needs_update)}')

        return games_map, games_needs_update

    def _process_update_batch_optimized(self, batch_num, batch_info, debug=False):
        """Оптимизированная обработка одной пачки игр (10 игр)"""
        from games.models import Genre, Platform, Keyword, GameEngine, Series, Company, Theme, PlayerPerspective, \
            GameMode
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if not batch_info:
            if debug:
                self.stdout.write(f'      ⚠️ ПАЧКА {batch_num}: пустая пачка')
            return 0, []

        igdb_ids = [info['igdb_id'] for info in batch_info]
        id_list = ','.join(map(str, igdb_ids))

        if debug:
            self.stdout.write(f'\n      📦 ПАЧКА {batch_num}: НАЧАЛО ОБРАБОТКИ')
            self.stdout.write(f'      🔢 ID игр в пачке: {igdb_ids}')

        query = f'''
            fields id,name,summary,storyline,genres,keywords,rating,rating_count,
                   first_release_date,platforms,cover,game_type,screenshots,
                   collections,involved_companies.company,involved_companies.developer,
                   involved_companies.publisher,themes,player_perspectives,
                   game_modes,game_engines;
            where id = ({id_list});
            limit 500;
        '''

        games_data = None
        for attempt in range(OPTIMAL_CONFIG['MAX_RETRIES'] + 1):
            try:
                if debug:
                    self.stdout.write(f'      🌐 ПАЧКА {batch_num}: Запрос к IGDB (попытка {attempt + 1})')

                games_data = make_igdb_request('games', query, debug=False)

                if debug:
                    self.stdout.write(
                        f'      ✅ ПАЧКА {batch_num}: Получен ответ от IGDB, игр: {len(games_data) if games_data else 0}')
                break
            except Exception as e:
                if attempt < OPTIMAL_CONFIG['MAX_RETRIES']:
                    delay = OPTIMAL_CONFIG['RETRY_DELAYS'][attempt]
                    if debug:
                        self.stdout.write(
                            f'      ⏸️ ПАЧКА {batch_num}: retry {attempt + 1} через {delay}с, ошибка: {e}')
                    time.sleep(delay)
                else:
                    if debug:
                        self.stderr.write(f'      ❌ ПАЧКА {batch_num}: Ошибка API после ретраев: {e}')
                    return 0, []

        if not games_data:
            if debug:
                self.stdout.write(f'      ⚠️ ПАЧКА {batch_num}: нет данных от IGDB')
            return 0, []

        games_data_map = {gd['id']: gd for gd in games_data if 'id' in gd}

        if debug:
            self.stdout.write(f'      📥 ПАЧКА {batch_num}: получено {len(games_data_map)}/{len(igdb_ids)} игр')
            not_found = set(igdb_ids) - set(games_data_map.keys())
            if not_found:
                self.stdout.write(f'      ⚠️ Не найдены в IGDB: {not_found}')

        all_ids = self._collect_needed_ids(batch_info, games_data_map, debug)

        if debug:
            total_ids = sum(len(v) for k, v in all_ids.items() if isinstance(v, list) and k != 'games_with_screenshots')
            self.stdout.write(f'      📊 ПАЧКА {batch_num}: Собрано ID для загрузки:')
            self.stdout.write(f'         • Обложек: {len(all_ids["cover_ids"])}')
            self.stdout.write(f'         • Жанров: {len(all_ids["genre_ids"])}')
            self.stdout.write(f'         • Платформ: {len(all_ids["platform_ids"])}')
            self.stdout.write(f'         • Ключевых слов: {len(all_ids["keyword_ids"])}')
            self.stdout.write(f'         • Движков: {len(all_ids["engine_ids"])}')
            self.stdout.write(f'         • Серий: {len(all_ids["series_ids"])}')
            self.stdout.write(f'         • Компаний: {len(all_ids["company_ids"])}')
            self.stdout.write(f'         • Тем: {len(all_ids["theme_ids"])}')
            self.stdout.write(f'         • Перспектив: {len(all_ids["perspective_ids"])}')
            self.stdout.write(f'         • Режимов: {len(all_ids["mode_ids"])}')
            self.stdout.write(f'         • Игр со скриншотами: {len(all_ids["games_with_screenshots"])}')

        loader = DataLoader(self.stdout, self.stderr)

        data_maps = {}

        if all_ids['cover_ids']:
            if debug:
                self.stdout.write(f'      🖼️ ПАЧКА {batch_num}: Загрузка {len(all_ids["cover_ids"])} обложек...')
            data_maps['cover_map'] = loader.load_covers_parallel(all_ids['cover_ids'], debug)

        if all_ids['genre_ids']:
            if debug:
                self.stdout.write(f'      🎭 ПАЧКА {batch_num}: Загрузка {len(all_ids["genre_ids"])} жанров...')
            data_maps['genre_map'] = loader.load_data_parallel_generic(
                all_ids['genre_ids'], 'genres', Genre, '🎭', 'жанров', debug
            )

        if all_ids['platform_ids']:
            if debug:
                self.stdout.write(f'      🖥️ ПАЧКА {batch_num}: Загрузка {len(all_ids["platform_ids"])} платформ...')
            data_maps['platform_map'] = loader.load_data_parallel_generic(
                all_ids['platform_ids'], 'platforms', Platform, '🖥️', 'платформ', debug
            )

        if all_ids['keyword_ids']:
            if debug:
                self.stdout.write(f'      🔑 ПАЧКА {batch_num}: Загрузка {len(all_ids["keyword_ids"])} ключевых слов...')
            data_maps['keyword_map'] = loader.load_keywords_parallel_with_weights(all_ids['keyword_ids'], debug)

        if all_ids['engine_ids']:
            if debug:
                self.stdout.write(f'      ⚙️ ПАЧКА {batch_num}: Загрузка {len(all_ids["engine_ids"])} движков...')
            data_maps['engine_map'] = loader.load_engines_parallel(all_ids['engine_ids'], debug)

        if all_ids['series_ids']:
            if debug:
                self.stdout.write(f'      📚 ПАЧКА {batch_num}: Загрузка {len(all_ids["series_ids"])} серий...')
            data_maps['series_map'] = loader.load_data_parallel_generic(
                all_ids['series_ids'], 'collections', Series, '📚', 'серий', debug
            )

        if all_ids['company_ids']:
            if debug:
                self.stdout.write(f'      🏢 ПАЧКА {batch_num}: Загрузка {len(all_ids["company_ids"])} компаний...')
            data_maps['company_map'] = loader.load_companies_parallel(all_ids['company_ids'], debug)

        if all_ids['theme_ids']:
            if debug:
                self.stdout.write(f'      🎨 ПАЧКА {batch_num}: Загрузка {len(all_ids["theme_ids"])} тем...')
            data_maps['theme_map'] = loader.load_data_parallel_generic(
                all_ids['theme_ids'], 'themes', Theme, '🎨', 'тем', debug
            )

        if all_ids['perspective_ids']:
            if debug:
                self.stdout.write(
                    f'      👁️ ПАЧКА {batch_num}: Загрузка {len(all_ids["perspective_ids"])} перспектив...')
            data_maps['perspective_map'] = loader.load_data_parallel_generic(
                all_ids['perspective_ids'], 'player_perspectives', PlayerPerspective, '👁️', 'перспектив', debug
            )

        if all_ids['mode_ids']:
            if debug:
                self.stdout.write(f'      🎮 ПАЧКА {batch_num}: Загрузка {len(all_ids["mode_ids"])} режимов...')
            data_maps['mode_map'] = loader.load_data_parallel_generic(
                all_ids['mode_ids'], 'game_modes', GameMode, '🎮', 'режимов', debug
            )

        if debug:
            self.stdout.write(f'      ✅ ПАЧКА {batch_num}: Загружено данных:')
            self.stdout.write(f'         • Обложек: {len(data_maps.get("cover_map", {}))}')
            self.stdout.write(f'         • Жанров: {len(data_maps.get("genre_map", {}))}')
            self.stdout.write(f'         • Платформ: {len(data_maps.get("platform_map", {}))}')
            self.stdout.write(f'         • Ключевых слов: {len(data_maps.get("keyword_map", {}))}')
            self.stdout.write(f'         • Движков: {len(data_maps.get("engine_map", {}))}')
            self.stdout.write(f'         • Серий: {len(data_maps.get("series_map", {}))}')
            self.stdout.write(f'         • Компаний: {len(data_maps.get("company_map", {}))}')
            self.stdout.write(f'         • Тем: {len(data_maps.get("theme_map", {}))}')
            self.stdout.write(f'         • Перспектив: {len(data_maps.get("perspective_map", {}))}')
            self.stdout.write(f'         • Режимов: {len(data_maps.get("mode_map", {}))}')

        if all_ids['games_with_screenshots']:
            if debug:
                self.stdout.write(
                    f'      📸 ПАЧКА {batch_num}: Загрузка скриншотов для {len(all_ids["games_with_screenshots"])} игр...')
            self._load_screenshots_for_batch(all_ids['games_with_screenshots'], games_data_map, debug)
            if debug:
                self.stdout.write(f'      ✅ ПАЧКА {batch_num}: Скриншоты загружены')

        batch_updated = 0
        batch_details = []

        if debug:
            self.stdout.write(f'      🔄 ПАЧКА {batch_num}: Обновление отдельных игр...')

        for info in batch_info:
            igdb_id = info['igdb_id']
            game = info['game']
            missing_data = info['missing_data']
            game_data = games_data_map.get(igdb_id)

            if debug:
                self.stdout.write(f'\n         🎯 Обработка игры: {game.name} (ID: {igdb_id})')
                missing_count_before = sum(1 for has_data in missing_data.values() if not has_data)
                self.stdout.write(f'            📊 Недостающих данных ДО: {missing_count_before}')

            if not game_data:
                if debug:
                    self.stdout.write(f'            ⚠️ Игра не найдена в ответе IGDB')
                continue

            success, details = self._update_single_game_with_existing_data(
                game, game_data, data_maps, {'game_data_map': games_data_map, 'screenshots_info': {}}, debug
            )

            # ИЗМЕНЕНИЕ: Проверяем, были ли реальные обновления
            if success and (
                    details['updated_fields'] or details['updated_relations'] or details['screenshots_added'] > 0):
                batch_updated += 1
                batch_details.append({
                    'game_name': game.name,
                    'game_id': igdb_id,
                    'details': details
                })

                if debug:
                    updated_items = []
                    if details['updated_fields']:
                        updated_items.append(f"поля: {', '.join(details['updated_fields'])}")
                    if details['updated_relations']:
                        updated_items.append(f"связи: {', '.join(details['updated_relations'])}")
                    if updated_items:
                        self.stdout.write(f'            ✅ Обновлено: {", ".join(updated_items)}')
                    else:
                        self.stdout.write(f'            ⏭️ Нет обновлений')
            elif success:
                if debug:
                    self.stdout.write(f'            ⏭️ Нет обновлений (все данные уже были)')
            else:
                if debug:
                    self.stdout.write(f'            ❌ Ошибка обновления')

        if debug and batch_updated > 0:
            self.stdout.write(f'\n      📊 ПАЧКА {batch_num}: обновлено {batch_updated}/{len(batch_info)} игр')
            self.stdout.write(f'      ✅ ПАЧКА {batch_num}: ЗАВЕРШЕНА')
        elif debug:
            self.stdout.write(f'\n      📊 ПАЧКА {batch_num}: не обновлено ни одной игры')
            self.stdout.write(f'      ✅ ПАЧКА {batch_num}: ЗАВЕРШЕНА')

        return batch_updated, batch_details

    def _collect_needed_ids(self, batch_info, games_data_map, debug=False):
        """Собирает ID только тех данных, которые действительно нужны"""
        all_ids = {
            'cover_ids': [],
            'genre_ids': set(),
            'platform_ids': set(),
            'keyword_ids': set(),
            'engine_ids': set(),
            'series_ids': set(),
            'company_ids': set(),
            'theme_ids': set(),
            'perspective_ids': set(),
            'mode_ids': set(),
            'games_with_screenshots': [],
        }

        if debug:
            self.stdout.write(f'         🔍 Сбор ID для {len(batch_info)} игр...')

        for info in batch_info:
            igdb_id = info['igdb_id']
            missing_data = info['missing_data']
            game_data = games_data_map.get(igdb_id)

            if not game_data:
                if debug:
                    self.stdout.write(f'            ⚠️ Игра {igdb_id} не найдена в данных IGDB')
                continue

            if debug:
                self.stdout.write(f'            • Игра {igdb_id}: анализ данных')

            if not missing_data.get('has_cover', True) and game_data.get('cover'):
                all_ids['cover_ids'].append(game_data['cover'])
                if debug:
                    self.stdout.write(f'               🖼️ Нужна обложка: {game_data["cover"]}')

            if not missing_data.get('has_genres', True) and game_data.get('genres'):
                all_ids['genre_ids'].update(game_data['genres'])
                if debug:
                    self.stdout.write(f'               🎭 Нужны жанры: {game_data["genres"]}')

            if not missing_data.get('has_platforms', True) and game_data.get('platforms'):
                all_ids['platform_ids'].update(game_data['platforms'])
                if debug:
                    self.stdout.write(f'               🖥️ Нужны платформы: {game_data["platforms"]}')

            if not missing_data.get('has_keywords', True) and game_data.get('keywords'):
                all_ids['keyword_ids'].update(game_data['keywords'])
                if debug:
                    self.stdout.write(f'               🔑 Нужны ключевые слова: {game_data["keywords"]}')

            if not missing_data.get('has_engines', True) and game_data.get('game_engines'):
                for engine in game_data['game_engines']:
                    if isinstance(engine, dict):
                        all_ids['engine_ids'].add(engine.get('id'))
                    elif isinstance(engine, int):
                        all_ids['engine_ids'].add(engine)
                if debug:
                    self.stdout.write(f'               ⚙️ Нужны движки: {game_data["game_engines"]}')

            if not missing_data.get('has_series', True) and game_data.get('collections'):
                all_ids['series_ids'].update(game_data['collections'])
                if debug:
                    self.stdout.write(f'               📚 Нужны серии: {game_data["collections"]}')

            if not missing_data.get('has_screenshots', True) and game_data.get('screenshots'):
                all_ids['games_with_screenshots'].append(igdb_id)
                if debug:
                    self.stdout.write(f'               📸 Нужны скриншоты ({len(game_data["screenshots"])} шт)')

            if (not missing_data.get('has_developers', True) or
                not missing_data.get('has_publishers', True)) and game_data.get('involved_companies'):
                for company_data in game_data['involved_companies']:
                    if company_data.get('company'):
                        all_ids['company_ids'].add(company_data['company'])
                if debug:
                    self.stdout.write(f'               🏢 Нужны компании')

            if not missing_data.get('has_themes', True) and game_data.get('themes'):
                all_ids['theme_ids'].update(game_data['themes'])
                if debug:
                    self.stdout.write(f'               🎨 Нужны темы: {game_data["themes"]}')

            if not missing_data.get('has_perspectives', True) and game_data.get('player_perspectives'):
                all_ids['perspective_ids'].update(game_data['player_perspectives'])
                if debug:
                    self.stdout.write(f'               👁️ Нужны перспективы: {game_data["player_perspectives"]}')

            if not missing_data.get('has_modes', True) and game_data.get('game_modes'):
                all_ids['mode_ids'].update(game_data['game_modes'])
                if debug:
                    self.stdout.write(f'               🎮 Нужны режимы: {game_data["game_modes"]}')

        for key in ['genre_ids', 'platform_ids', 'keyword_ids', 'engine_ids',
                    'series_ids', 'company_ids', 'theme_ids', 'perspective_ids', 'mode_ids']:
            all_ids[key] = list(all_ids[key])

        if debug:
            total_ids = sum(len(v) for k, v in all_ids.items() if isinstance(v, list) and k != 'games_with_screenshots')
            self.stdout.write(f'         📊 ИТОГО собрано ID для загрузки: {total_ids}')

        return all_ids

    def _load_screenshots_for_batch(self, game_ids, games_data_map, debug=False):
        """Загружает скриншоты для игр в пачке"""
        from .data_collector import DataCollector

        if not game_ids:
            return

        collector = DataCollector(self.stdout, self.stderr)
        loader = DataLoader(self.stdout, self.stderr)

        games_data = [games_data_map[gid] for gid in game_ids if gid in games_data_map]

        if not games_data:
            return

        collected = collector.collect_all_data_ids(games_data, False)
        screenshots_info = collected.get('screenshots_info', {})
        game_data_map = collected.get('game_data_map', {})

        loader.load_screenshots_parallel(game_ids, game_data_map, screenshots_info, False)

    def _load_single_update_batch(self, batch_num, batch_ids, games_map, debug=False):
        """Загружает и обновляет одну пачку из 10 игр - оптимизированная версия"""
        import time

        if not batch_ids:
            if debug:
                self.stdout.write(f'      ⚠️ Пачка {batch_num}: пустая пачка')
            return 0, []

        if debug:
            self.stdout.write(f'\n      📦 ПАЧКА {batch_num}: загрузка {len(batch_ids)} игр')
            self.stdout.write(f'      🔢 ID игр: {batch_ids}')

        try:
            MAX_RETRIES = OPTIMAL_CONFIG['MAX_RETRIES']
            RETRY_DELAYS = OPTIMAL_CONFIG['RETRY_DELAYS']

            games_to_update = []
            games_needing_update = {}

            for game_id in batch_ids:
                game = games_map.get(game_id)
                if not game:
                    if debug:
                        self.stdout.write(f'         ⚠️ Игра {game_id} не найдена в базе')
                    continue

                missing_data, missing_count, cover_status = self.check_missing_game_data(game)

                if missing_count > 0:
                    games_to_update.append(game_id)
                    games_needing_update[game_id] = {
                        'game': game,
                        'missing_data': missing_data,
                        'missing_count': missing_count
                    }
                    if debug:
                        missing_fields = [k.replace('has_', '') for k, v in missing_data.items() if not v]
                        self.stdout.write(
                            f'         🔍 {game.name}: нужно обновить {missing_count} полей: {", ".join(missing_fields)}')
                else:
                    if debug:
                        self.stdout.write(f'         ✅ {game.name}: все данные уже есть')

            if not games_to_update:
                if debug:
                    self.stdout.write(f'      ⏭️ Пачка {batch_num}: все игры уже имеют полные данные')
                return 0, []

            if debug:
                self.stdout.write(
                    f'      📊 Пачка {batch_num}: нужно обновить {len(games_to_update)}/{len(batch_ids)} игр')

            id_list = ','.join(map(str, games_to_update))

            query = f'''
                fields id,name,summary,storyline,genres,keywords,rating,rating_count,
                       first_release_date,platforms,cover,game_type,screenshots,
                       collections,involved_companies.company,involved_companies.developer,
                       involved_companies.publisher,themes,player_perspectives,
                       game_modes,game_engines;
                where id = ({id_list});
                limit 500;
            '''

            if debug:
                self.stdout.write(f'      🌐 Пачка {batch_num}: запрос к IGDB')

            games_data = None
            last_error = None

            for attempt in range(MAX_RETRIES + 1):
                try:
                    games_data = make_igdb_request('games', query, debug=False)
                    if debug and games_data:
                        self.stdout.write(
                            f'      📥 Пачка {batch_num}: получено {len(games_data)}/{len(games_to_update)} игр из IGDB')
                    break
                except Exception as e:
                    last_error = e
                    if attempt < MAX_RETRIES:
                        delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 5.0
                        if debug:
                            error_msg = str(e).lower()
                            if "429" in str(e) or "too many" in error_msg:
                                self.stdout.write(f'      ⏸️ Пачка {batch_num}: Rate limit, пауза {delay:.1f} сек...')
                            else:
                                self.stdout.write(f'      ⏸️ Пачка {batch_num}: Ошибка API, пауза {delay:.1f} сек...')
                        time.sleep(delay)

            if not games_data:
                if debug and last_error:
                    self.stdout.write(f'      ⚠️ Пачка {batch_num}: ошибка после ретраев')
                return 0, []

            games_data_map = {gd['id']: gd for gd in games_data if 'id' in gd}

            all_cover_ids = []
            all_genre_ids = set()
            all_platform_ids = set()
            all_keyword_ids = set()
            all_engine_ids = set()
            all_series_ids = set()
            all_company_ids = set()
            all_theme_ids = set()
            all_perspective_ids = set()
            all_mode_ids = set()
            games_with_screenshots = []

            if debug:
                self.stdout.write(f'      🔍 Пачка {batch_num}: сбор ID данных для загрузки...')

            for game_id, game_info in games_needing_update.items():
                game_data = games_data_map.get(game_id)
                if not game_data:
                    if debug:
                        self.stdout.write(f'         ⚠️ Игра {game_id} не найдена в данных IGDB')
                    continue

                missing_data = game_info['missing_data']

                if not missing_data['has_cover'] and game_data.get('cover'):
                    all_cover_ids.append(game_data['cover'])

                if not missing_data['has_genres'] and game_data.get('genres'):
                    all_genre_ids.update(game_data['genres'])

                if not missing_data['has_platforms'] and game_data.get('platforms'):
                    all_platform_ids.update(game_data['platforms'])

                if not missing_data['has_keywords'] and game_data.get('keywords'):
                    all_keyword_ids.update(game_data['keywords'])

                if not missing_data['has_engines'] and game_data.get('game_engines'):
                    for engine in game_data['game_engines']:
                        if isinstance(engine, dict):
                            all_engine_ids.add(engine.get('id'))
                        elif isinstance(engine, int):
                            all_engine_ids.add(engine)

                if not missing_data['has_series'] and game_data.get('collections'):
                    all_series_ids.update(game_data['collections'])

                if not missing_data['has_screenshots'] and game_data.get('screenshots'):
                    games_with_screenshots.append(game_id)

                if (not missing_data['has_developers'] or not missing_data['has_publishers']) and game_data.get(
                        'involved_companies'):
                    for company_data in game_data['involved_companies']:
                        if company_data.get('company'):
                            all_company_ids.add(company_data['company'])

                if not missing_data['has_themes'] and game_data.get('themes'):
                    all_theme_ids.update(game_data['themes'])

                if not missing_data['has_perspectives'] and game_data.get('player_perspectives'):
                    all_perspective_ids.update(game_data['player_perspectives'])

                if not missing_data['has_modes'] and game_data.get('game_modes'):
                    all_mode_ids.update(game_data['game_modes'])

            loader = DataLoader(self.stdout, self.stderr)
            handler = RelationsHandler(self.stdout, self.stderr)

            if debug:
                self.stdout.write(f'      ⚡ Пачка {batch_num}: параллельная загрузка данных...')

            data_maps = {}

            with ThreadPoolExecutor(max_workers=3) as executor:
                futures = {}

                if all_cover_ids:
                    if debug:
                        self.stdout.write(f'         🖼️ Загрузка {len(all_cover_ids)} обложек...')
                    futures['cover_map'] = executor.submit(loader.load_covers_parallel, all_cover_ids, False)
                if all_genre_ids:
                    if debug:
                        self.stdout.write(f'         🎭 Загрузка {len(all_genre_ids)} жанров...')
                    futures['genre_map'] = executor.submit(
                        loader.load_data_parallel_generic, list(all_genre_ids), 'genres', Genre, '🎭', 'жанров', False
                    )
                if all_platform_ids:
                    if debug:
                        self.stdout.write(f'         🖥️ Загрузка {len(all_platform_ids)} платформ...')
                    futures['platform_map'] = executor.submit(
                        loader.load_data_parallel_generic, list(all_platform_ids), 'platforms', Platform, '🖥️',
                        'платформ', False
                    )
                if all_keyword_ids:
                    if debug:
                        self.stdout.write(f'         🔑 Загрузка {len(all_keyword_ids)} ключевых слов...')
                    futures['keyword_map'] = executor.submit(
                        loader.load_keywords_parallel_with_weights, list(all_keyword_ids), False
                    )
                if all_engine_ids:
                    if debug:
                        self.stdout.write(f'         ⚙️ Загрузка {len(all_engine_ids)} движков...')
                    futures['engine_map'] = executor.submit(loader.load_engines_parallel, list(all_engine_ids), False)
                if all_series_ids:
                    if debug:
                        self.stdout.write(f'         📚 Загрузка {len(all_series_ids)} серий...')
                    futures['series_map'] = executor.submit(
                        loader.load_data_parallel_generic, list(all_series_ids), 'collections', Series, '📚', 'серий',
                        False
                    )
                if all_company_ids:
                    if debug:
                        self.stdout.write(f'         🏢 Загрузка {len(all_company_ids)} компаний...')
                    futures['company_map'] = executor.submit(loader.load_companies_parallel, list(all_company_ids),
                                                             False)
                if all_theme_ids:
                    if debug:
                        self.stdout.write(f'         🎨 Загрузка {len(all_theme_ids)} тем...')
                    futures['theme_map'] = executor.submit(
                        loader.load_data_parallel_generic, list(all_theme_ids), 'themes', Theme, '🎨', 'тем', False
                    )
                if all_perspective_ids:
                    if debug:
                        self.stdout.write(f'         👁️ Загрузка {len(all_perspective_ids)} перспектив...')
                    futures['perspective_map'] = executor.submit(
                        loader.load_data_parallel_generic, list(all_perspective_ids), 'player_perspectives',
                        PlayerPerspective, '👁️', 'перспектив', False
                    )
                if all_mode_ids:
                    if debug:
                        self.stdout.write(f'         🎮 Загрузка {len(all_mode_ids)} режимов...')
                    futures['mode_map'] = executor.submit(
                        loader.load_data_parallel_generic, list(all_mode_ids), 'game_modes', GameMode, '🎮', 'режимов',
                        False
                    )

                for key, future in futures.items():
                    try:
                        data_maps[key] = future.result(timeout=30)
                        if debug:
                            self.stdout.write(f'         ✅ {key}: загружено {len(data_maps[key])} объектов')
                    except Exception as e:
                        if debug:
                            self.stderr.write(f'         ⚠️ Ошибка загрузки {key}: {e}')
                        data_maps[key] = {}

            screenshots_loaded_by_game = {}
            if games_with_screenshots:
                if debug:
                    self.stdout.write(f'         📸 Загрузка скриншотов для {len(games_with_screenshots)} игр...')
                from .data_collector import DataCollector
                collector = DataCollector(self.stdout, self.stderr)

                screenshot_games_data = [games_data_map[gid] for gid in games_with_screenshots if gid in games_data_map]
                if screenshot_games_data:
                    collected_screenshot_data = collector.collect_all_data_ids(screenshot_games_data, False)
                    screenshots_info = collected_screenshot_data.get('screenshots_info', {})
                    screenshots_loaded = loader.load_screenshots_parallel(
                        games_with_screenshots,
                        {gid: games_data_map.get(gid, {}) for gid in games_with_screenshots},
                        screenshots_info,
                        False
                    )
                    if debug:
                        self.stdout.write(f'         ✅ Загружено скриншотов: {screenshots_loaded}')

            batch_updated = 0
            batch_details = []

            if debug:
                self.stdout.write(f'      🔄 Пачка {batch_num}: обновление игр...')

            for game_id, game_info in games_needing_update.items():
                game = game_info['game']
                game_data = games_data_map.get(game_id)

                if not game_data:
                    if debug:
                        self.stdout.write(f'         ⚠️ Игра {game.name} не найдена в данных IGDB')
                    continue

                if debug:
                    self.stdout.write(f'\n         🎯 Обновление: {game.name}')

                missing_data = game_info['missing_data']
                details = {
                    'updated_fields': [],
                    'updated_relations': [],
                    'screenshots_added': 0,
                    'still_missing': [],
                }

                fields_to_update = []

                if not missing_data['has_cover'] and game_data.get('cover'):
                    cover_id = game_data['cover']
                    if cover_id in data_maps.get('cover_map', {}):
                        new_cover_url = data_maps['cover_map'][cover_id]
                        if game.cover_url != new_cover_url:
                            game.cover_url = new_cover_url
                            fields_to_update.append('cover_url')
                            details['updated_fields'].append('cover_url')
                            if debug:
                                self.stdout.write(f'            🖼️ Обновление обложки')

                if not missing_data['has_description'] and game_data.get('summary'):
                    if not game.summary or not game.summary.strip():
                        game.summary = game_data.get('summary', '')
                        fields_to_update.append('summary')
                        details['updated_fields'].append('summary')
                        if debug:
                            self.stdout.write(f'            📝 Обновление описания')

                if not missing_data['has_rating'] and 'rating' in game_data:
                    if game.rating != game_data.get('rating'):
                        game.rating = game_data.get('rating')
                        fields_to_update.append('rating')
                        details['updated_fields'].append('rating')
                        if debug:
                            self.stdout.write(f'            ⭐ Обновление рейтинга')

                if not missing_data['has_release_date'] and game_data.get('first_release_date'):
                    from datetime import datetime
                    from django.utils import timezone
                    naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                    new_date = timezone.make_aware(naive_datetime)
                    if game.first_release_date != new_date:
                        game.first_release_date = new_date
                        fields_to_update.append('first_release_date')
                        details['updated_fields'].append('first_release_date')
                        if debug:
                            self.stdout.write(f'            📅 Обновление даты релиза')

                if fields_to_update:
                    game.save(update_fields=fields_to_update)
                    if debug:
                        self.stdout.write(f'            💾 Сохранены поля: {", ".join(fields_to_update)}')

                game_basic_map = {game_id: game}
                additional_data_map = {game_id: game_data}
                game_data_map = {game_id: game_data}

                all_game_relations, _ = handler.prepare_game_relations(
                    game_basic_map, game_data_map, additional_data_map, data_maps, False
                )

                if all_game_relations:
                    needs_genres = not missing_data['has_genres'] and game_data.get('genres')
                    needs_platforms = not missing_data['has_platforms'] and game_data.get('platforms')
                    needs_keywords = not missing_data['has_keywords'] and game_data.get('keywords')
                    needs_engines = not missing_data['has_engines'] and game_data.get('game_engines')

                    if needs_genres or needs_platforms or needs_keywords or needs_engines:
                        genre_count, platform_count, keyword_count, engine_count = handler.create_relations_batch(
                            all_game_relations, False
                        )

                        if genre_count > 0:
                            details['updated_relations'].append(f'жанры ({genre_count})')
                            if debug:
                                self.stdout.write(f'            ✅ Добавлено жанров: {genre_count}')
                        if platform_count > 0:
                            details['updated_relations'].append(f'платформы ({platform_count})')
                            if debug:
                                self.stdout.write(f'            ✅ Добавлено платформ: {platform_count}')
                        if keyword_count > 0:
                            details['updated_relations'].append(f'ключевые слова ({keyword_count})')
                            if debug:
                                self.stdout.write(f'            ✅ Добавлено ключевых слов: {keyword_count}')
                        if engine_count > 0:
                            details['updated_relations'].append(f'движки ({engine_count})')
                            if debug:
                                self.stdout.write(f'            ✅ Добавлено движков: {engine_count}')

                    needs_series = not missing_data['has_series'] and game_data.get('collections')
                    needs_developers = not missing_data['has_developers'] and game_data.get('involved_companies')
                    needs_publishers = not missing_data['has_publishers'] and game_data.get('involved_companies')
                    needs_themes = not missing_data['has_themes'] and game_data.get('themes')
                    needs_perspectives = not missing_data['has_perspectives'] and game_data.get('player_perspectives')
                    needs_modes = not missing_data['has_modes'] and game_data.get('game_modes')

                    if any([needs_series, needs_developers, needs_publishers, needs_themes, needs_perspectives,
                            needs_modes]):
                        additional_results = handler.create_all_additional_relations(all_game_relations, False)

                        for rel_type, count in additional_results.items():
                            if count > 0:
                                rel_name = rel_type.replace('_relations', '').replace('_', ' ')
                                should_add = False
                                if rel_type == 'series_relations' and needs_series:
                                    should_add = True
                                elif rel_type == 'developer_relations' and needs_developers:
                                    should_add = True
                                elif rel_type == 'publisher_relations' and needs_publishers:
                                    should_add = True
                                elif rel_type == 'theme_relations' and needs_themes:
                                    should_add = True
                                elif rel_type == 'perspective_relations' and needs_perspectives:
                                    should_add = True
                                elif rel_type == 'mode_relations' and needs_modes:
                                    should_add = True

                                if should_add:
                                    details['updated_relations'].append(f'{rel_name} ({count})')
                                    if debug:
                                        self.stdout.write(f'            ✅ Добавлено {rel_name}: {count}')

                if any([details['updated_fields'], details['updated_relations']]):
                    batch_updated += 1
                    batch_details.append({
                        'game_name': game.name,
                        'game_id': game_id,
                        'details': details
                    })

                    if debug:
                        self.stdout.write(f'            ✅ Обновление завершено')
                else:
                    if debug:
                        self.stdout.write(f'            ⏭️ Нет обновлений')

            if debug and batch_updated > 0:
                self.stdout.write(f'      📊 Пачка {batch_num}: обновлено {batch_updated}/{len(games_to_update)} игр')

            return batch_updated, batch_details

        except Exception as e:
            if debug:
                self.stderr.write(f'      ❌ Ошибка пачки {batch_num}: {e}')
                import traceback
                self.stderr.write(f'      📋 Трассировка: {traceback.format_exc()[:500]}...')
            return 0, []

    def _update_single_game_with_existing_data(self, game, game_data, data_maps, collected_data, debug):
        """Обновляет одну игру используя уже загруженные данные"""
        details = {
            'updated_fields': [],
            'updated_relations': [],
            'screenshots_added': 0,
            'still_missing': [],
        }

        if debug:
            self.stdout.write(f'            🔧 Обновление игры {game.name} (ID: {game.igdb_id})')

        try:
            missing_data, missing_count, cover_status = self.check_missing_game_data(game)

            if debug:
                self.stdout.write(f'            📊 Недостающих данных ДО: {missing_count}')
                self.stdout.write(f'            📋 Статус обложки: {cover_status}')
                if missing_count > 0:
                    missing_list = [key.replace('has_', '') for key, has_data in missing_data.items() if not has_data]
                    self.stdout.write(f'            📋 Отсутствует: {", ".join(missing_list)}')

            if missing_count == 0:
                if debug:
                    self.stdout.write(f'            ✅ Все данные уже есть, обновление не требуется')
                return True, details

            if not missing_data['has_cover'] and game_data.get('cover'):
                cover_id = game_data['cover']
                if cover_id in data_maps.get('cover_map', {}):
                    new_cover_url = data_maps['cover_map'][cover_id]
                    if game.cover_url != new_cover_url:
                        if debug:
                            self.stdout.write(f'            🖼️ Обновление обложки')
                        game.cover_url = new_cover_url
                        details['updated_fields'].append('cover_url')

            if not missing_data['has_description'] and game_data.get('summary'):
                if not game.summary or not game.summary.strip():
                    if debug:
                        self.stdout.write(f'            📝 Обновление описания')
                    game.summary = game_data.get('summary', '')
                    details['updated_fields'].append('summary')

            if not missing_data['has_rating'] and 'rating' in game_data:
                if game.rating != game_data.get('rating'):
                    if debug:
                        self.stdout.write(f'            ⭐ Обновление рейтинга')
                    game.rating = game_data.get('rating')
                    details['updated_fields'].append('rating')

            if not missing_data['has_release_date'] and game_data.get('first_release_date'):
                from datetime import datetime
                from django.utils import timezone
                naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                new_date = timezone.make_aware(naive_datetime)
                if game.first_release_date != new_date:
                    if debug:
                        self.stdout.write(f'            📅 Обновление даты релиза')
                    game.first_release_date = new_date
                    details['updated_fields'].append('first_release_date')

            if details['updated_fields']:
                game.save(update_fields=details['updated_fields'])
                if debug:
                    self.stdout.write(f'            💾 Сохранены поля: {", ".join(details["updated_fields"])}')

            if not missing_data['has_screenshots'] and collected_data['screenshots_info'].get(game.igdb_id, 0) > 0:
                if debug:
                    self.stdout.write(f'            📸 Загрузка скриншотов...')
                screenshots_info = collected_data['screenshots_info']
                data_loader = DataLoader(self.stdout, self.stderr)
                screenshots_loaded = data_loader.load_screenshots_parallel(
                    [game.igdb_id], collected_data['game_data_map'],
                    screenshots_info, debug
                )
                if screenshots_loaded > 0:
                    details['screenshots_added'] = screenshots_loaded
                    if debug:
                        self.stdout.write(f'            ✅ Загружено скриншотов: {screenshots_loaded}')

            game_basic_map = {game.igdb_id: game}
            additional_data_map = {game.igdb_id: game_data}

            handler = RelationsHandler(self.stdout, self.stderr)

            if debug:
                self.stdout.write(f'            🔗 Подготовка связей...')

            all_game_relations, relations_prep_time = handler.prepare_game_relations(
                game_basic_map, collected_data['game_data_map'],
                additional_data_map, data_maps, debug
            )

            if all_game_relations:
                if debug:
                    self.stdout.write(f'            🔗 Создание основных связей...')

                genre_count, platform_count, keyword_count, engine_count = handler.create_relations_batch(
                    all_game_relations, debug
                )

                if debug:
                    self.stdout.write(f'            🔗 Создание дополнительных связей...')

                additional_results = handler.create_all_additional_relations(
                    all_game_relations, debug
                )

                if genre_count > 0:
                    details['updated_relations'].append(f'жанры ({genre_count})')
                    if debug:
                        self.stdout.write(f'            ✅ Добавлено жанров: {genre_count}')
                if platform_count > 0:
                    details['updated_relations'].append(f'платформы ({platform_count})')
                    if debug:
                        self.stdout.write(f'            ✅ Добавлено платформ: {platform_count}')
                if keyword_count > 0:
                    details['updated_relations'].append(f'ключевые слова ({keyword_count})')
                    if debug:
                        self.stdout.write(f'            ✅ Добавлено ключевых слов: {keyword_count}')
                if engine_count > 0:
                    details['updated_relations'].append(f'движки ({engine_count})')
                    if debug:
                        self.stdout.write(f'            ✅ Добавлено движков: {engine_count}')

                for rel_type, count in additional_results.items():
                    if count > 0:
                        rel_name = rel_type.replace('_relations', '').replace('_', ' ')
                        details['updated_relations'].append(f'{rel_name} ({count})')
                        if debug:
                            self.stdout.write(f'            ✅ Добавлено {rel_name}: {count}')

            new_missing_data, new_missing_count, new_cover_status = self.check_missing_game_data(game)
            for data_type, has_data in new_missing_data.items():
                if not has_data:
                    details['still_missing'].append(data_type.replace('has_', ''))

            if debug:
                missing_after = len(details['still_missing'])
                self.stdout.write(f'            📊 Недостающих данных ПОСЛЕ: {missing_after}')
                if missing_after > 0:
                    self.stdout.write(f'            ⚠️ Все еще отсутствует: {", ".join(details["still_missing"])}')
                else:
                    self.stdout.write(f'            ✅ Все данные теперь есть!')

            return True, details

        except Exception as e:
            if debug:
                self.stderr.write(f'            ❌ Ошибка обновления игры {game.igdb_id}: {e}')
                import traceback
                self.stderr.write(f'            📋 Трассировка: {traceback.format_exc()}')
            return False, details

    def load_games_by_names(self, game_names_str, debug=False, limit=0, offset=0, min_rating_count=0,
                            skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка САМОЙ ПОПУЛЯРНОЙ игры по точному названию"""
        collector = DataCollector(self.stdout, self.stderr)

        effective_limit = 1

        skip_for_update = skip_existing

        if hasattr(self, 'current_options') and self.current_options.get('update_missing_data'):
            skip_for_update = False
            if debug:
                self.stdout.write(f'   🔄 РЕЖИМ ОБНОВЛЕНИЯ: не пропускаем существующие игры')

        return collector.load_games_by_names(
            game_names_str, debug, effective_limit, offset, min_rating_count,
            skip_for_update, count_only, game_types_str
        )

    def load_games_by_genres(self, genres_str, debug=False, limit=0, offset=0, min_rating_count=0,
                             skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по жанрам с логикой И (должны быть ВСЕ указанные жанры)"""
        collector = DataCollector(self.stdout, self.stderr)

        genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]

        if not genre_list:
            self.stdout.write('⚠️  Не указаны жанры')
            return self.empty_result()

        if debug:
            self.stdout.write(f'🔍 Поиск жанров: {", ".join(genre_list)}')

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
            return self.empty_result()

        if debug:
            self.stdout.write(f'📋 Найдено ID жанров: {", ".join(genre_ids)}')

        genre_conditions = [f'genres = ({genre_id})' for genre_id in genre_ids]
        where_clause = ' & '.join(genre_conditions)

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

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

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False
        )

    def load_games_by_genres_and_description(self, genres_str, description_text, debug=False, limit=0, offset=0,
                                             min_rating_count=0, skip_existing=True, count_only=False,
                                             game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по жанрам И тексту в описании"""
        collector = DataCollector(self.stdout, self.stderr)

        genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]

        if not genre_list:
            self.stdout.write('⚠️  Не указаны жанры')
            return self.empty_result()

        if debug:
            self.stdout.write(f'🔍 Поиск жанров: {", ".join(genre_list)}')
            self.stdout.write(f'🔍 Текст для поиска: "{description_text}"')

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
            return self.empty_result()

        if debug:
            self.stdout.write(f'📋 Найдено ID жанров: {", ".join(genre_ids)}')

        genre_conditions = [f'genres = ({genre_id})' for genre_id in genre_ids]
        genres_condition = ' & '.join(genre_conditions)

        text_condition = f'(name ~ *"{description_text}"* | summary ~ *"{description_text}"* | storyline ~ *"{description_text}"*)'
        where_clause = f'{genres_condition} & {text_condition}'

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

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

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False
        )

    def load_games_by_description(self, description_text, debug=False, limit=0, offset=0, min_rating_count=0,
                                  skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по тексту в описании или названии"""
        collector = DataCollector(self.stdout, self.stderr)

        if debug:
            self.stdout.write(f'🔍 Ищу игры с текстом: "{description_text}"')

        where_conditions = [
            f'name ~ *"{description_text}"* | summary ~ *"{description_text}"* | storyline ~ *"{description_text}"*']

        if min_rating_count > 0:
            where_conditions.append(f'rating_count >= {min_rating_count}')
        else:
            where_conditions.append('rating_count > 0')

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

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False
        )

    def load_games_by_keywords(self, keywords_str, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по ключевым словам с логикой И"""
        collector = DataCollector(self.stdout, self.stderr)

        keyword_list = [k.strip() for k in keywords_str.split(',') if k.strip()]

        if not keyword_list:
            self.stdout.write('⚠️  Не указаны ключевые слова')
            return self.empty_result()

        if debug:
            self.stdout.write(f'🔍 Поиск ключевых слов: {", ".join(keyword_list)}')

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
            return self.empty_result()

        if debug:
            self.stdout.write(f'📋 Найдено ID ключевых слов: {", ".join(keyword_ids)}')

        keyword_conditions = [f'keywords = ({keyword_id})' for keyword_id in keyword_ids]
        where_clause = ' & '.join(keyword_conditions)

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

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

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False
        )

    def load_games_by_game_mode(self, game_mode_name, debug=False, limit=0, offset=0, min_rating_count=0,
                                skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по режиму игры (например, Battle Royale)"""
        collector = DataCollector(self.stdout, self.stderr)

        if debug:
            self.stdout.write(f'🔍 Поиск режима игры: "{game_mode_name}"')

        query = f'fields id,name; where name = "{game_mode_name}";'

        try:
            result = make_igdb_request('game_modes', query, debug=False)
        except Exception as e:
            if debug:
                self.stderr.write(f'❌ Ошибка при поиске режима игры: {e}')
            return collector.empty_result()

        if not result:
            if debug:
                self.stdout.write(f'❌ Режим игры "{game_mode_name}" не найден')
            return collector.empty_result()

        game_mode_id = result[0]['id']
        found_mode_name = result[0].get('name', game_mode_name)

        if debug:
            self.stdout.write(f'✅ Режим игры "{found_mode_name}" найден: ID {game_mode_id}')

        where_conditions = [f'game_modes = ({game_mode_id})']

        if min_rating_count > 0:
            where_conditions.append(f'rating_count >= {min_rating_count}')
        else:
            where_conditions.append('rating_count > 0')

        where_conditions.append('name != null')

        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_conditions.append(f'game_type = ({game_types_str_query})')
            except ValueError:
                if debug:
                    self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        where_clause = ' & '.join(where_conditions)

        if debug:
            self.stdout.write(f'🎯 Условие поиска: {where_clause}')

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            query_context={'is_specific_search': True, 'mode_id': game_mode_id},
            show_progress=False
        )

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        collector = DataCollector(self.stdout, self.stderr)
        return collector.load_all_popular_games(
            debug, limit, offset, min_rating_count,
            skip_existing, count_only, game_types_str
        )

    def _handle_overwrite_mode(self, all_games, debug):
        """Обрабатывает режим перезаписи"""
        self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ - найденные игры будут удалены и загружены заново!')

        game_ids_to_delete = [game_data.get('id') for game_data in all_games if game_data.get('id')]

        if game_ids_to_delete:
            if debug:
                self.stdout.write(f'   🔍 Поиск игр для удаления: {len(game_ids_to_delete)} ID')

            games_to_delete = Game.objects.filter(igdb_id__in=game_ids_to_delete)
            count_before = games_to_delete.count()

            if debug:
                self.stdout.write(f'   📊 Найдено игр для удаления в базе: {count_before}')

            if count_before > 0:
                deleted_info = games_to_delete.delete()

                if isinstance(deleted_info, tuple) and len(deleted_info) == 2:
                    total_deleted, deleted_details = deleted_info
                    self.stdout.write(f'🗑️  УДАЛЕНИЕ ЗАВЕРШЕНО:')
                    self.stdout.write(f'   • Всего удалено объектов: {total_deleted}')

                    for model_name, count in deleted_details.items():
                        model_display = model_name.split('.')[-1]
                        if count > 0:
                            self.stdout.write(f'   • {model_display}: {count}')
                else:
                    self.stdout.write(f'🗑️  Удалено игр и связанных данных: {deleted_info}')
            else:
                self.stdout.write('   ℹ️  Не найдено игр для удаления в базе данных')
        else:
            self.stdout.write('   ⚠️  Не найдено ID игр для удаления')

    def update_missing_game_data(self, game_id, debug=False):
        """Обновляет недостающие данные для конкретной игры"""
        from games.models import Game
        from datetime import datetime
        from django.core.cache import cache
        from django.utils import timezone
        from .game_cache import GameCacheManager

        details = {
            'updated_fields': [],
            'updated_relations': [],
            'screenshots_added': 0,
            'still_missing': [],
            'cover_url': None,
            'summary': None,
            'rating': None,
            'release_date': None,
            'game_name': None,
            'timestamp': None
        }

        try:
            game = Game.objects.filter(igdb_id=game_id).first()
            if not game:
                if debug:
                    self.stdout.write(f'   ❌ Игра с ID {game_id} не найдена в базе')
                return False, details

            details['game_name'] = game.name
            details['timestamp'] = datetime.now().isoformat()

            if debug:
                self.stdout.write(f'\n   🔍 ПРОВЕРКА НЕДОСТАЮЩИХ ДАННЫХ ДЛЯ: {game.name} (ID: {game_id})')

            cache_key = f"updated_game_{game_id}_{datetime.now().strftime('%Y%m%d')}"
            already_updated_today = cache.get(cache_key)

            if already_updated_today:
                if debug:
                    self.stdout.write(f'   ⏭️ [КЭШ] Игра {game.name} уже обновлялась сегодня, пропускаем')
                return True, {'skipped': True, 'reason': 'already_updated_today'}

            missing_data, missing_count, cover_status = self.check_missing_game_data(game)

            if debug:
                self.stdout.write(f'   📊 СТАТУС ДАННЫХ:')
                for key, value in missing_data.items():
                    status = "✅ ЕСТЬ" if value else "❌ ОТСУТСТВУЕТ"
                    self.stdout.write(f'      • {key}: {status}')
                self.stdout.write(f'      • Обложка: {cover_status}')

            if missing_count == 0:
                if debug:
                    self.stdout.write(f'   ⏭️ У игры "{game.name}" все данные уже есть, пропускаем')

                cache.set(cache_key, True, 24 * 60 * 60)
                try:
                    GameCacheManager.mark_game_checked(game_id)
                except:
                    pass

                return True, {'skipped': True, 'reason': 'all_data_present'}

            if debug:
                self.stdout.write(f'   📊 Недостающих данных: {missing_count} из {len(missing_data)}')

            query = f'''
                fields id,name,summary,storyline,genres,keywords,rating,rating_count,
                       first_release_date,platforms,cover,game_type,screenshots,
                       collections,franchises,involved_companies.company,
                       involved_companies.developer,involved_companies.publisher,
                       themes,player_perspectives,game_modes,game_engines;
                where id = {game_id};
            '''

            games_data = make_igdb_request('games', query, debug=False)
            if not games_data:
                if debug:
                    self.stdout.write(f'   ❌ Данные игры {game_id} не найдены в IGDB')
                return False, details

            game_data = games_data[0]

            collector = DataCollector(self.stdout, self.stderr)
            loader = DataLoader(self.stdout, self.stderr)
            handler = RelationsHandler(self.stdout, self.stderr)

            collected_data = collector.collect_all_data_ids([game_data], debug)

            # ВАЖНО: Явно собираем ID компаний из involved_companies
            company_ids = set()
            if game_data.get('involved_companies'):
                for company_data in game_data['involved_companies']:
                    if company_data.get('company'):
                        company_ids.add(company_data['company'])

            if company_ids:
                collected_data['all_company_ids'] = list(company_ids)
                if debug:
                    self.stdout.write(f'   🏢 Собрано ID компаний для загрузки: {company_ids}')

            data_maps, step_times = loader.load_all_data_types_sequentially(
                collected_data, debug
            )

            cover_updated = self.update_game_cover(game, game_data, data_maps, details, debug)

            if not missing_data['has_description'] and game_data.get('summary'):
                if not game.summary or not game.summary.strip():
                    game.summary = game_data.get('summary', '')
                    details['updated_fields'].append('summary')
                    details['summary'] = game.summary

            if not missing_data['has_rating'] and 'rating' in game_data:
                if game.rating != game_data.get('rating'):
                    game.rating = game_data.get('rating')
                    details['updated_fields'].append('rating')
                    details['rating'] = game.rating

            if not missing_data['has_release_date'] and game_data.get('first_release_date'):
                from datetime import datetime as dt

                igdb_timestamp = game_data['first_release_date']
                naive_datetime = dt.fromtimestamp(igdb_timestamp)
                new_date = timezone.make_aware(naive_datetime)

                should_update = False

                if game.first_release_date is None:
                    should_update = True
                    if debug:
                        self.stdout.write(f'   📅 Дата отсутствует в БД, устанавливаем')
                else:
                    current_timestamp = int(game.first_release_date.timestamp())
                    if current_timestamp != igdb_timestamp:
                        should_update = True
                        if debug:
                            self.stdout.write(f'   📅 Разные timestamp: {current_timestamp} vs {igdb_timestamp}')
                    else:
                        if debug:
                            self.stdout.write(f'   📅 Дата уже актуальна, пропускаем')

                if should_update:
                    game.first_release_date = new_date
                    details['updated_fields'].append('first_release_date')
                    details['release_date'] = new_date

            if details['updated_fields']:
                try:
                    game.save(update_fields=details['updated_fields'])
                except Exception as e:
                    if debug:
                        self.stderr.write(f'   ❌ Ошибка сохранения игры {game_id}: {e}')
                    return False, details

            if not missing_data['has_screenshots'] and game_data.get('screenshots'):
                screenshots_info = collected_data.get('screenshots_info', {})
                screenshots_loaded = loader.load_screenshots_parallel(
                    [game_id], collected_data['game_data_map'],
                    screenshots_info, debug
                )
                if screenshots_loaded > 0:
                    details['screenshots_added'] = screenshots_loaded

            game_basic_map = {game_id: game}
            additional_data_map = {game_id: game_data}

            all_game_relations, relations_prep_time = handler.prepare_game_relations(
                game_basic_map, collected_data['game_data_map'],
                additional_data_map, data_maps, debug
            )

            if all_game_relations:
                needs_genres = not missing_data['has_genres'] and game_data.get('genres')
                needs_platforms = not missing_data['has_platforms'] and game_data.get('platforms')
                needs_keywords = not missing_data['has_keywords'] and game_data.get('keywords')
                needs_engines = not missing_data['has_engines'] and game_data.get('game_engines')
                needs_series = not missing_data['has_series'] and game_data.get('collections')
                needs_developers = not missing_data['has_developers'] and game_data.get('involved_companies')
                needs_publishers = not missing_data['has_publishers'] and game_data.get('involved_companies')
                needs_themes = not missing_data['has_themes'] and game_data.get('themes')
                needs_perspectives = not missing_data['has_perspectives'] and game_data.get('player_perspectives')
                needs_modes = not missing_data['has_modes'] and game_data.get('game_modes')

                if needs_genres or needs_platforms or needs_keywords or needs_engines:
                    genre_count, platform_count, keyword_count, engine_count = handler.create_relations_batch(
                        all_game_relations, debug
                    )

                    if genre_count > 0:
                        details['updated_relations'].append(f'жанры ({genre_count})')
                    if platform_count > 0:
                        details['updated_relations'].append(f'платформы ({platform_count})')
                    if keyword_count > 0:
                        details['updated_relations'].append(f'ключевые слова ({keyword_count})')
                    if engine_count > 0:
                        details['updated_relations'].append(f'движки ({engine_count})')

                if any([needs_series, needs_developers, needs_publishers, needs_themes, needs_perspectives,
                        needs_modes]):
                    additional_results = handler.create_all_additional_relations(
                        all_game_relations, debug
                    )

                    for rel_type, count in additional_results.items():
                        if count > 0:
                            rel_name = rel_type.replace('_relations', '').replace('_', ' ')

                            should_add = False
                            if rel_type == 'series_relations' and needs_series:
                                should_add = True
                            elif rel_type == 'developer_relations' and needs_developers:
                                should_add = True
                            elif rel_type == 'publisher_relations' and needs_publishers:
                                should_add = True
                            elif rel_type == 'theme_relations' and needs_themes:
                                should_add = True
                            elif rel_type == 'perspective_relations' and needs_perspectives:
                                should_add = True
                            elif rel_type == 'mode_relations' and needs_modes:
                                should_add = True

                            if should_add:
                                details['updated_relations'].append(f'{rel_name} ({count})')

            new_missing_data, new_missing_count, new_cover_status = self.check_missing_game_data(game)
            for data_type, has_data in new_missing_data.items():
                if not has_data:
                    details['still_missing'].append(data_type.replace('has_', ''))

            cache.set(cache_key, True, 24 * 60 * 60)
            try:
                GameCacheManager.mark_game_checked(game_id)
            except:
                pass

            if debug:
                self.stdout.write(f'   ✅ Обновление завершено для игры "{game.name}"')

                if details['updated_fields'] or details['updated_relations'] or details['screenshots_added'] > 0:
                    self.stdout.write(f'   📈 ОБНОВЛЕНО:')
                    if details['updated_fields']:
                        self.stdout.write(f'      • Поля: {", ".join(details["updated_fields"])}')
                    if details['updated_relations']:
                        self.stdout.write(f'      • Связи: {", ".join(details["updated_relations"])}')
                    if details['screenshots_added'] > 0:
                        self.stdout.write(f'      • Скриншотов: {details["screenshots_added"]}')

                if new_missing_count < missing_count:
                    self.stdout.write(f'   📊 УЛУЧШЕНИЕ: было {missing_count} недостающих → стало {new_missing_count}')
                else:
                    self.stdout.write(f'   ⚠️  Недостающих данных осталось: {new_missing_count}')

            return True, details

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка при обновлении игры {game_id}: {str(e)}')
                import traceback
                self.stderr.write(f'   📋 Трассировка: {traceback.format_exc()}')
            return False, details

    def update_game_cover(self, game, game_data, data_maps, details, debug=False):
        """Обновляет обложку игры без проверки доступности"""
        if not game_data.get('cover'):
            return False

        cover_id = game_data['cover']
        if cover_id not in data_maps.get('cover_map', {}):
            return False

        new_cover_url = data_maps['cover_map'][cover_id]

        if 't_thumb' in new_cover_url:
            new_cover_url = new_cover_url.replace('t_thumb', 't_cover_big')

        if not new_cover_url.endswith('.jpg'):
            if new_cover_url.endswith('.webp'):
                new_cover_url = new_cover_url.replace('.webp', '.jpg')
            else:
                new_cover_url += '.jpg'

        current_url = game.cover_url or ""

        if debug:
            self.stdout.write(f'   🔍 Обновление обложки для {game.name}:')
            self.stdout.write(f'      Текущая URL: {current_url}')
            self.stdout.write(f'      Новая URL: {new_cover_url}')

        if current_url == new_cover_url:
            if debug:
                self.stdout.write(f'   ⏭️  Обложка уже актуальна: {current_url}')
            return False

        game.cover_url = new_cover_url
        if 'cover_url' not in details['updated_fields']:
            details['updated_fields'].append('cover_url')
        details['cover_url'] = new_cover_url

        if debug:
            self.stdout.write(f'   🖼️  Обновляем обложку')
        return True

    def check_missing_game_data(self, game_obj, check_cover_online=False):
        """Проверяет, каких данных не хватает у игры (оптимизированная версия)"""
        has_cover_url = bool(game_obj.cover_url and game_obj.cover_url.strip())

        has_cover_accessible = has_cover_url
        current_cover_status = "есть URL" if has_cover_url else "нет"

        # Принудительно обновляем из БД
        from django.db.models import Prefetch
        fresh_game = Game.objects.filter(id=game_obj.id).prefetch_related(
            'engines', 'themes', 'genres', 'platforms', 'keywords',
            'series', 'developers', 'publishers', 'player_perspectives', 'game_modes'
        ).first()

        if not fresh_game:
            fresh_game = game_obj

        # ПРОВЕРКА ДАТЫ РЕЛИЗА
        has_release_date = False

        if fresh_game.first_release_date:
            # Проверяем, что дата валидная
            if hasattr(fresh_game.first_release_date, 'year'):
                if fresh_game.first_release_date.year > 1900:
                    has_release_date = True

            # Дополнительная проверка через timestamp
            try:
                timestamp = int(fresh_game.first_release_date.timestamp())
                if timestamp > 0:
                    has_release_date = True
            except:
                pass

        missing_data = {
            'has_cover': has_cover_accessible,
            'has_screenshots': fresh_game.screenshots.exists(),
            'has_genres': fresh_game.genres.exists(),
            'has_platforms': fresh_game.platforms.exists(),
            'has_keywords': fresh_game.keywords.exists(),
            'has_engines': fresh_game.engines.exists(),
            'has_description': bool(fresh_game.summary and fresh_game.summary.strip()),
            'has_rating': fresh_game.rating is not None,
            'has_release_date': has_release_date,
            'has_series': fresh_game.series.exists(),
            'has_developers': fresh_game.developers.exists(),
            'has_publishers': fresh_game.publishers.exists(),
            'has_themes': fresh_game.themes.exists(),
            'has_perspectives': fresh_game.player_perspectives.exists(),
            'has_modes': fresh_game.game_modes.exists(),
        }

        missing_count = sum(1 for has_data in missing_data.values() if not has_data)

        return missing_data, missing_count, current_cover_status

    def _log_batch_update(self, update_details, total_games, updated_count, failed_count,
                          start_time, end_time, debug=False):
        """Сохраняет лог пакетного обновления"""
        import os
        import json
        from datetime import datetime

        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        log_dir = os.path.join(project_root, 'load_games_logs')
        os.makedirs(log_dir, exist_ok=True)

        today = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_file = os.path.join(log_dir, f'batch_update_{today}.json')

        batch_data = {
            'batch_info': {
                'total_games': total_games,
                'updated_count': updated_count,
                'failed_count': failed_count,
                'success_rate': (updated_count / total_games * 100) if total_games > 0 else 0,
                'start_time': datetime.fromtimestamp(start_time).isoformat(),
                'end_time': datetime.fromtimestamp(end_time).isoformat(),
                'duration_seconds': end_time - start_time,
            },
            'updated_games': [
                {
                    'game_id': detail['game_id'],
                    'game_name': detail['game_name'],
                    'details': detail['details']
                }
                for detail in update_details
            ],
            'summary': {
                'by_field': self._summarize_updates_by_field(update_details),
                'by_game': len(update_details)
            }
        }

        try:
            with open(batch_file, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, indent=2, ensure_ascii=False, default=str)

            if debug:
                self.stdout.write(f'\n📁 Пакетный лог сохранен в: {batch_file}')

        except Exception as e:
            if debug:
                self.stderr.write(f'   ⚠️  Не удалось сохранить пакетный лог: {e}')

    def _summarize_updates_by_field(self, update_details):
        """Суммирует обновления по типам полей"""
        summary = {
            'cover_url': 0,
            'summary': 0,
            'rating': 0,
            'first_release_date': 0,
            'screenshots': 0,
            'genres': 0,
            'platforms': 0,
            'keywords': 0,
            'series': 0,
            'developers': 0,
            'publishers': 0,
            'themes': 0,
            'perspectives': 0,
            'modes': 0,
        }

        for detail in update_details:
            details = detail['details']

            for field in details.get('updated_fields', []):
                if field in summary:
                    summary[field] += 1

            for relation in details.get('updated_relations', []):
                rel_type = relation.split(' ')[0]
                if rel_type in ['жанры', 'genres']:
                    summary['genres'] += 1
                elif rel_type in ['платформы', 'platforms']:
                    summary['platforms'] += 1
                elif rel_type in ['ключевые', 'keywords']:
                    summary['keywords'] += 1
                elif rel_type in ['серии', 'series']:
                    summary['series'] += 1
                elif rel_type in ['разработчики', 'developers']:
                    summary['developers'] += 1
                elif rel_type in ['издатели', 'publishers']:
                    summary['publishers'] += 1
                elif rel_type in ['темы', 'themes']:
                    summary['themes'] += 1
                elif rel_type in ['перспективы', 'perspectives']:
                    summary['perspectives'] += 1
                elif rel_type in ['режимы', 'modes']:
                    summary['modes'] += 1

            if details.get('screenshots_added', 0) > 0:
                summary['screenshots'] += 1

        return summary

    def _handle_iteration_error(self, error, iteration, execution_mode, total_stats, progress_bar):
        """Обрабатывает ошибки в итерации"""
        if isinstance(error, KeyboardInterrupt):
            raise error

        self._update_error_statistics(total_stats)
        self._display_error_details(error, iteration)

        should_continue = self._determine_continuation_mode(
            execution_mode, iteration, total_stats, progress_bar
        )

        if should_continue:
            self.stdout.write(f'   ⏩ Пропускаем итерацию {iteration} из-за ошибки')
            total_stats['iterations_with_no_new_games'] += 1
            total_stats['iterations'] += 1

            if progress_bar:
                progress_bar.update(
                    total_loaded=total_stats['total_games_created'],
                    current_iteration=iteration,
                    iterations_without_new=total_stats['iterations_with_no_new_games']
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

        if infinite_mode:
            return True
        elif finite_mode and iteration < repeat_count:
            return True
        else:
            return False