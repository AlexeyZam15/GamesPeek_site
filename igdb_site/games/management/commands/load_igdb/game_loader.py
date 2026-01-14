# games/management/commands/load_igdb/game_loader.py
import time
import signal
import threading
from django.utils import timezone
from games.models import Game
from games.igdb_api import make_igdb_request
from .data_collector import DataCollector
from .data_loader import DataLoader
from .relations_handler import RelationsHandler
from .statistics import Statistics
from .offset_manager import OffsetManager


class GameLoader:
    """Основной класс для выполнения команды загрузки игр"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr
        self.max_consecutive_no_new_games = 3
        self.debug_mode = False

    def load_games_by_names(self, game_names_str, debug=False, limit=0, offset=0, min_rating_count=0,
                            skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка САМОЙ ПОПУЛЯРНОЙ игры по точному названию"""
        collector = DataCollector(self.stdout, self.stderr)

        # Для режима game-names используем лимит 1 - только самую популярную игру
        effective_limit = 1

        return collector.load_games_by_names(
            game_names_str, debug, effective_limit, offset, min_rating_count,
            skip_existing, count_only, game_types_str
        )

    def execute_command(self, options):
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

        # Сохраняем режим отладки
        self.debug_mode = debug

        # Очищаем кэш если нужно
        if clear_cache:
            self.clear_game_cache()

        # Сбрасываем offset если нужно
        if reset_offset:
            self._handle_reset_offset(options, debug)

        # Определяем режим выполнения
        execution_mode = self._determine_execution_mode(repeat_count)

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

        # Загружаем сохраненный offset если не указан явно
        if original_offset == 0 and not reset_offset:
            saved_offset = self._get_saved_offset(options)
            if saved_offset is not None:
                current_offset = saved_offset
                self.stdout.write(f'📍 Начинаем с сохраненного offset: {current_offset}')

        total_stats = self._initialize_total_stats(original_offset)

        # Если это TTY терминал, оставляем место для прогресс-бара
        if (hasattr(progress_bar, 'is_tty') and progress_bar.is_tty and not count_only):
            self.stdout.write('\n' * 2)

        # ОСНОВНОЙ ЦИКЛ ИТЕРАЦИЙ
        iteration = 1
        try:
            while True:
                # Выполняем одну итерацию
                should_continue, current_offset, total_stats = self._execute_single_iteration(
                    iteration, current_offset, total_stats, execution_mode,
                    limit, iteration_limit, options, progress_bar
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
            if not reset_offset:
                self._save_offset_for_continuation(options, current_offset)

            # УДАЛЕНО: лишний вывод статистики
            # if debug:
            #     self.stdout.write(f'\n🛑 Команда прервана, статистика: {total_stats}')
            return  # Просто выходим

        except Exception as e:
            # Обработка других исключений
            self.stderr.write(f'\n❌ Неожиданная ошибка: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())

            # Все равно сохраняем offset если нужно
            if not reset_offset:
                self._save_offset_for_continuation(options, current_offset)

            # УДАЛЕНО: лишний вывод статистики
            # if debug:
            #     self.stdout.write(f'\n❌ Завершено с ошибкой, статистика: {total_stats}')
            return  # Просто выходим

        # ФИНАЛЬНЫЙ ЭТАП
        self._finalize_execution(total_stats, limit, progress_bar,
                                 execution_mode, original_offset,
                                 current_offset, limit, overwrite)

        # Итоговый статус
        self._display_final_status(total_stats, limit)

        # УДАЛЕНО: лишний вывод отладочной статистики
        # if debug:
        #     self.stdout.write(f'\n✅ Команда завершена успешно, статистика: {total_stats}')

    def _execute_single_iteration(self, iteration, current_offset, total_stats, execution_mode,
                                  limit, iteration_limit, options, progress_bar):
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
            iteration_result = self.handle_single_iteration(
                iteration=iteration,
                current_offset=current_offset,
                iteration_limit_actual=iteration_limit_actual,
                options=options
            )

            if iteration_result.get('success', True):
                # Обновляем общую статистику
                current_offset = self._update_total_stats(
                    total_stats, iteration_result, iteration,
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
            raise
        except Exception as e:
            self._handle_iteration_error(e, iteration, execution_mode, total_stats, progress_bar)

        return True, current_offset, total_stats

    def handle_single_iteration(self, iteration, current_offset, iteration_limit_actual, options):
        """Обработка одной итерации команды"""
        # Подготовка параметров
        params = self._get_execution_parameters(options)

        # Информация об итерации
        iteration_info = {
            'iteration_number': iteration,
            'repeat_count': options.get('repeat', 1),
            'iteration_offset': current_offset,
            'iteration_limit_actual': iteration_limit_actual,
        }

        # Отображение заголовка
        self.stdout.write('🎮 ЗАГРУЗКА ИГР ИЗ IGDB')
        self.stdout.write('=' * 60)

        # Определяем тип загрузки
        self._display_loading_type(params)

        # Информация об итерации
        self._display_iteration_info(params, iteration_info)

        # Используем offset и limit для этой конкретной итерации
        actual_offset = current_offset
        actual_limit = iteration_limit_actual

        # Определение режимов
        skip_existing = self._determine_skip_mode(params)
        debug = params['debug']
        errors = 0
        iteration_start_time = time.time()

        # Загрузка игр
        result = self._load_games_for_iteration(params, actual_limit, actual_offset, skip_existing, debug)

        # Обработка результатов загрузки
        if result is None:
            return self._handle_failed_loading(iteration_start_time, errors, actual_offset)

        # Проверка наличия игр
        if not result.get('new_games'):
            return self._handle_empty_results(result, errors, params, actual_offset, iteration_start_time)

        # Обработка режима count-only
        if params['count_only']:
            return self._handle_count_only_mode(result, errors, iteration_start_time, actual_offset)

        # Обработка данных игр
        result_stats, errors = self._process_game_data_for_iteration(
            result, params, iteration_start_time, errors
        )

        # Подготовка финальной статистики
        final_stats = self._prepare_final_iteration_stats(
            result, result_stats, actual_offset, actual_limit,
            errors, iteration_info, params, iteration_start_time
        )

        # Отображение статистики итерации
        return self._display_iteration_statistics_complete(
            final_stats, result, actual_offset, actual_limit,
            params, iteration_info, errors, result_stats
        )

    def _display_loading_type(self, params):
        """Отображает тип загрузки"""
        game_names_str = params.get('game_names_str', '')
        genres_str = params['genres_str']
        description_contains = params['description_contains']
        keywords_str = params['keywords_str']
        game_types_str = params['game_types_str']

        if params['count_only']:
            self.stdout.write('🔢 РЕЖИМ: ПОДСЧЕТ НОВЫХ ИГР (которых нет в базе)')
            self.stdout.write('⚠️  Игры не будут сохранены в базу данных!')

        # НОВАЯ ВЕТКА: поиск самой популярной игры по точному имени
        if game_names_str:
            name_list = [n.strip() for n in game_names_str.split(',') if n.strip()]
            if name_list:
                self.stdout.write(f'🎮 РЕЖИМ: САМАЯ ПОПУЛЯРНАЯ игра по имени: "{name_list[0]}"')
                self.stdout.write('   🔍 Поиск самой популярной игры с указанным точным названием')

                # Предупреждение о том, что используются только первое имя
                if len(name_list) > 1:
                    self.stdout.write(
                        f'   ⚠️  Указано {len(name_list)} имен, используется только первое: "{name_list[0]}"')
        elif genres_str and description_contains:
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

        # Информация о фильтрах
        if game_types_str and game_types_str != '0,1,2,4,5,8,9,10,11':
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                self.stdout.write(f'🎮 ФИЛЬТР ПО ТИПАМ ИГР: {game_types}')
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

    def _display_iteration_info(self, params, iteration_info):
        """Отображает информацию об итерации"""
        game_names_str = params.get('game_names_str', '')
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

        # Специальное сообщение для режима поиска по имени
        if game_names_str:
            name_list = [n.strip() for n in game_names_str.split(',') if n.strip()]
            if name_list:
                self.stdout.write(f'🔎 Будет загружена САМАЯ ПОПУЛЯРНАЯ игра с названием: "{name_list[0]}"')
        elif repeat_count > 1:
            self.stdout.write(f'🔄 Итерация {iteration_number}/{repeat_count}')

        # Для режима поиска по имени показываем специальный лимит
        if game_names_str:
            self.stdout.write(f'📊 ЛИМИТ: 1 игра (самая популярная с указанным названием)')
        elif actual_limit > 0:
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
            return False
        else:
            return True

    def _load_games_for_iteration(self, params, actual_limit, actual_offset, skip_existing, debug):
        """Загружает игры для итерации"""
        try:
            # НОВАЯ ВЕТКА: поиск по именам игр (первый приоритет)
            if params.get('game_names_str'):
                return self.load_games_by_names(
                    params['game_names_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str'] and params['description_contains']:
                return self.load_games_by_genres_and_description(
                    params['genres_str'], params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str']:
                return self.load_games_by_genres(
                    params['genres_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['description_contains']:
                return self.load_games_by_description(
                    params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['keywords_str']:
                return self.load_games_by_keywords(
                    params['keywords_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
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

    def _handle_count_only_mode(self, result, errors, iteration_start_time, actual_offset):
        """Обрабатывает режим только подсчета"""
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
            # Создаем экземпляры классов для обработки данных
            collector = DataCollector(self.stdout, self.stderr)
            loader = DataLoader(self.stdout, self.stderr)
            handler = RelationsHandler(self.stdout, self.stderr)
            stats = Statistics(self.stdout, self.stderr)

            # Устанавливаем обработчик прерывания
            interrupted = threading.Event()

            def signal_handler(sig, frame):
                interrupted.set()
                loader.set_interrupted()
                if params['debug']:
                    self.stdout.write('\n   ⏹️  Получен сигнал прерывания в обработке данных')

            original_sigint = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, signal_handler)

            try:
                # Шаг 1: Собираем все ID данных
                if params['debug']:
                    self.stdout.write('\n📊 СБОР ВСЕХ ID ДАННЫХ...')

                collected_data = collector.collect_all_data_ids(result['new_games'], params['debug'])

                # Проверка прерывания
                if interrupted.is_set():
                    self.stdout.write('   ⏹️  Прерывание: пропускаем создание игр')
                    raise KeyboardInterrupt()

                # Шаг 2: Создаем основные объекты игр
                if params['debug']:
                    self.stdout.write('\n🎮 СОЗДАНИЕ ОСНОВНЫХ ОБЪЕКТОВ ИГР...')

                created_count, game_basic_map = loader.create_basic_games(
                    result['new_games'], params['debug']
                )

                # Если не создано игр, возвращаем нулевую статистику
                if created_count == 0:
                    signal.signal(signal.SIGINT, original_sigint)
                    return {
                        'created_count': 0,
                        'skipped_count': 0,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                # Проверка прерывания
                if interrupted.is_set():
                    self.stdout.write('   ⏹️  Прерывание: пропускаем загрузку данных')
                    signal.signal(signal.SIGINT, original_sigint)
                    return {
                        'created_count': created_count,
                        'skipped_count': 0,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                # Шаг 3: Загружаем все остальные данные
                if params['debug']:
                    self.stdout.write('\n📥 ЗАГРУЗКА ВСЕХ ДАННЫХ...')

                # Получаем информацию о скриншотах из collected_data
                screenshots_info = collected_data.get('screenshots_info', {})

                # Загружаем скриншоты ПЕРЕД дополнительными данными
                screenshots_loaded = 0
                if 'all_screenshot_games' in collected_data and screenshots_info:
                    if params['debug']:
                        self.stdout.write(f'\n📸 ЗАГРУЗКА СКРИНШОТОВ...')
                        self.stdout.write(f'   📊 Информация о скриншотах: {screenshots_info}')

                    screenshots_loaded = loader.load_screenshots_parallel(
                        collected_data['all_screenshot_games'],
                        collected_data['game_data_map'],
                        screenshots_info,
                        params['debug']
                    )

                    if params['debug']:
                        self.stdout.write(f'   ✅ Загружено скриншотов: {screenshots_loaded}')

                # Загружаем дополнительные данные
                additional_data_map, additional_ids = loader.load_and_process_additional_data(
                    list(game_basic_map.keys()),
                    collected_data['game_data_map'],
                    screenshots_info,  # Передаем информацию о скриншотах
                    params['debug']
                )

                # Обновляем collected_data с дополнительными ID
                collected_data.update(additional_ids)

                # Шаг 4: Загружаем все типы данных
                data_maps, step_times = loader.load_all_data_types_sequentially(
                    collected_data, params['debug']
                )

                # Проверка прерывания
                if interrupted.is_set():
                    self.stdout.write('   ⏹️  Прерывание: пропускаем создание связей')
                    signal.signal(signal.SIGINT, original_sigint)
                    return {
                        'created_count': created_count,
                        'skipped_count': 0,
                        'total_time': time.time() - iteration_start_time,
                        'screenshots_loaded': screenshots_loaded,
                    }, errors

                # Шаг 5: Обновляем игры с обложками
                cover_updates = loader.update_games_with_covers(
                    game_basic_map, data_maps.get('cover_map', {}),
                    collected_data['game_data_map'], params['debug']
                )

                # Проверка прерывания
                if interrupted.is_set():
                    self.stdout.write('   ⏹️  Прерывание: пропускаем создание связей')
                    signal.signal(signal.SIGINT, original_sigint)
                    return {
                        'created_count': created_count,
                        'screenshots_loaded': screenshots_loaded,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                # Шаг 6: Подготавливаем и создаем связи
                all_game_relations, relations_prep_time = handler.prepare_game_relations(
                    game_basic_map, collected_data['game_data_map'],
                    additional_data_map, data_maps, params['debug']
                )

                # Создаем все связи
                relations_results, relations_possible, relations_time = handler.create_all_relations(
                    all_game_relations, params['debug']
                )

                # Шаг 7: Собираем статистику
                loaded_data_stats = {
                    'collected': collected_data,
                    'loaded': {k: len(v) for k, v in data_maps.items()}
                }

                step_times['relations_preparation'] = relations_prep_time
                step_times['relations_creation'] = relations_time

                # Собираем полную статистику
                final_stats = stats._collect_final_statistics(
                    result['new_games_count'], created_count, 0, screenshots_loaded,
                    time.time() - iteration_start_time, loaded_data_stats, step_times,
                    relations_results, relations_possible, params['debug']
                )

                # Выводим статистику
                stats._print_complete_statistics(final_stats)

                result_stats = {
                    'created_count': created_count,
                    'skipped_count': result.get('existing_games_skipped', 0),
                    'total_time': time.time() - iteration_start_time,
                    'screenshots_loaded': screenshots_loaded,
                    'relations_created': sum(relations_results.values()) if relations_results else 0,
                }

            finally:
                # Восстанавливаем оригинальный обработчик сигнала
                signal.signal(signal.SIGINT, original_sigint)

        except KeyboardInterrupt:
            # Обработка прерывания в обработке данных
            self.stdout.write('\n   ⏹️  Прерывание в обработке данных')
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }
            errors += 1
        except Exception as e:
            errors += 1
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

        return result_stats, errors

    def _prepare_final_iteration_stats(self, result, result_stats, actual_offset, actual_limit,
                                       errors, iteration_info, params, iteration_start_time):
        """Подготавливает финальную статистику итерации"""
        if result_stats:
            result_stats['total_games_checked'] = result['total_games_checked']
            result_stats['total_games_found'] = result['new_games_count']
            result_stats['errors'] = errors
            result_stats['last_checked_offset'] = result.get('last_checked_offset', actual_offset)
            result_stats['limit_reached'] = result.get('limit_reached', False)
            result_stats['limit_reached_at_offset'] = result.get('limit_reached_at_offset')
        else:
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

        # Возвращаем статистику, но НЕ выводим здесь общую статистику
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
        game_names_str = params.get('game_names_str', '')

        if game_names_str:
            name_list = [n.strip() for n in game_names_str.split(',') if n.strip()]
            if name_list:
                self.stdout.write(f'🔍 Поиск самой популярной игры с названием: "{name_list[0]}"')
        elif params['overwrite']:
            self.stdout.write(f'📥 Найдено игр для перезаписи: {new_games_count}')
        else:
            self.stdout.write(f'📥 Найдено игр для обработки: {new_games_count}')

        # Для режима поиска по имени показываем специфичную информацию
        if not game_names_str:
            self.stdout.write(f'👀 Всего просмотрено игр из IGDB: {total_games_checked}')
            self.stdout.write(f'📍 Последний проверенный offset: {last_checked_offset}')
            self.stdout.write(f'📍 Следующий offset для продолжения: {last_checked_offset + 1}')

            if limit_reached:
                self.stdout.write(f'🎯 Лимит {actual_limit} достигнут на offset {last_checked_offset}')

            if existing_games_skipped > 0 and not params['overwrite']:
                self.stdout.write(f'⏭️  Пропущено существующих игр: {existing_games_skipped}')
        else:
            # Для режима поиска по имени - упрощенная информация
            if new_games_count > 0:
                self.stdout.write(f'✅ Найдена игра для загрузки')
            else:
                if existing_games_skipped > 0:
                    self.stdout.write(f'ℹ️  Игра уже есть в базе данных')
                else:
                    self.stdout.write(f'❌ Игра с таким названием не найдена')

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

    # Методы загрузки игр из IGDB
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
                # Удаляем найденные игры
                deleted_info = games_to_delete.delete()

                # Разбираем результат delete()
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

    # Вспомогательные методы из base_command
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
            from .base_command import TopProgressBar
            return TopProgressBar(self.stdout)
        else:
            from .base_command import SimpleProgressBar
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
            'game_names_str': options['game_names'],  # НОВЫЙ ПАРАМЕТР
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
                iterations_without_new=total_stats['iterations_with_no_new_games']
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
        """Завершает выполнение команды"""
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

    def _handle_reset_offset(self, options, debug):
        """Обрабатывает сброс сохраненного offset"""
        where_clause = self._get_where_clause_for_current_command(options)
        if not where_clause:
            if debug:
                self.stdout.write('⚠️  Не удалось определить запрос для сброса offset')
            return

        query_key = self._get_query_key_for_current_command(options, where_clause)
        cleared = OffsetManager.clear_offset(query_key)

        if cleared:
            self.stdout.write('🔄 Сброшен сохраненный offset для этого запроса')
        else:
            self.stdout.write('⚠️  Не удалось сбросить offset')

    def clear_game_cache(self):
        """Очищает кэш проверенных игр"""
        try:
            from .game_cache import GameCacheManager
            cleared = GameCacheManager.clear_cache()
            self.stdout.write(f"✅ Кэш проверенных игр очищен")
            return cleared
        except Exception as e:
            self.stderr.write(f"❌ Ошибка при очистке кэша: {e}")
            return False

    def _get_where_clause_for_current_command(self, options):
        """Получает where_clause для текущей команды"""
        game_names_str = options.get('game_names', '')  # НОВОЕ
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')
        game_types_str = options.get('game_types', '')
        min_rating_count = options.get('min_rating_count', 0)

        where_parts = []

        # НОВАЯ ВЕТКА: поиск по именам
        if game_names_str:
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
        if game_names_str:
            # Для поиска по именам rating_count может быть 0
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
        game_names_str = options.get('game_names', '')  # НОВОЕ
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')

        if game_names_str:
            return 'game_names'  # НОВЫЙ РЕЖИМ
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
        params = {
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': self._get_loading_mode(options),
        }

        return OffsetManager.get_query_key(where_clause, **params)

    def _handle_iteration_error(self, error, iteration, execution_mode, total_stats, progress_bar):
        """Обрабатывает ошибки в итерации"""
        # Проверка типа ошибки
        if isinstance(error, KeyboardInterrupt):
            raise error

        # Обработка обычных ошибок
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

    def _get_saved_offset(self, options):
        """Получает сохраненный offset для текущих параметров"""
        where_clause = self._get_where_clause_for_current_command(options)
        if not where_clause:
            return None

        query_key = self._get_query_key_for_current_command(options, where_clause)
        return OffsetManager.load_offset(query_key)

    def _save_offset_for_continuation(self, options, current_offset):
        """Сохраняет offset для продолжения"""
        where_clause = self._get_where_clause_for_current_command(options)
        if not where_clause:
            return False

        query_key = self._get_query_key_for_current_command(options, where_clause)
        saved = OffsetManager.save_offset(query_key, current_offset)

        if saved and options.get('debug', False):
            self.stdout.write(f'   💾 Сохранен offset для продолжения: {current_offset}')

        return saved