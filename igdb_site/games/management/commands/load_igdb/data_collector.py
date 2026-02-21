# games/management/commands/load_igdb/data_collector.py
import time
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from games.igdb_api import make_igdb_request

try:
    from .game_cache import GameCacheManager
except ImportError:
    class GameCacheManager:
        @staticmethod
        def is_game_checked(igdb_id):
            return False

        @staticmethod
        def mark_game_checked(igdb_id):
            pass

        @staticmethod
        def clear_cache():
            return True

        @staticmethod
        def get_checked_count():
            return 0

        @staticmethod
        def batch_check_games(igdb_ids):
            return {igdb_id: False for igdb_id in igdb_ids if igdb_id}

        @staticmethod
        def batch_mark_checked(igdb_ids):
            pass


class DataCollector:
    """Класс для сбора и обработки данных"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr

    def _load_single_game_by_exact_name_for_update(self, where_clause, debug=False, skip_existing=True,
                                                   count_only=False):
        """Загружает САМУЮ ПОПУЛЯРНУЮ игру по точному имени - ВСЕГДА возвращает найденную игру"""
        # Загружаем существующие ID игр для фильтрации
        from games.models import Game
        existing_game_ids = set()
        if skip_existing:
            existing_game_ids = set(Game.objects.values_list('igdb_id', flat=True))
            if debug:
                self.stdout.write(f'   📊 Игр в базе для фильтрации: {len(existing_game_ids)}')

        try:
            if debug:
                self.stdout.write(f'   🎯 Запрос самой популярной игры...')

            # ЗАПРОС ДОЛЖЕН ВКЛЮЧАТЬ screenshots!
            query = f'''
                fields id,name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover,game_type,screenshots;
                where {where_clause};
                sort rating_count desc;
                limit 1;
            '''.strip()

            games = make_igdb_request('games', query, debug=False)

            if not games:
                if debug:
                    self.stdout.write('   ❌ Игра с таким названием не найдена')
                return self._empty_result()

            game = games[0]
            game_id = game.get('id')

            if debug:
                self.stdout.write(
                    f'   ✅ Найдена игра: "{game.get("name")}" (ID: {game_id}, rating_count: {game.get("rating_count", 0)})')
                if game.get('screenshots'):
                    self.stdout.write(f'   📸 Скриншотов у игры: {len(game.get("screenshots", []))}')

            # ВАЖНОЕ ИЗМЕНЕНИЕ: В режиме обновления мы ВСЕГДА возвращаем найденную игру
            # но отмечаем, существует ли она уже в базе
            game_exists = game_id in existing_game_ids

            if game_exists:
                if debug:
                    self.stdout.write(f'   ⏭️  Игра уже есть в базе, но будет использована для обновления')

            # ВОЗВРАЩАЕМ ВСЕГДА игру, даже если она уже есть в базе
            return {
                'new_games': [] if game_exists and skip_existing else [game],
                'all_found_games': [game],  # ВАЖНО: возвращаем ВСЕ найденные игры
                'total_games_checked': 1,
                'new_games_count': 0 if game_exists and skip_existing else 1,
                'existing_games_skipped': 1 if game_exists else 0,
                'last_checked_offset': 0,
                'limit_reached': False,
                'limit_reached_at_offset': None,
                'interrupted': False,
            }

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка при запросе игры: {str(e)}')
            return self._empty_result()

    def load_games_by_names(self, game_names_str, debug=False, limit=0, offset=0, min_rating_count=0,
                            skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка САМОЙ ПОПУЛЯРНОЙ игры по точному названию - ВСЕГДА возвращает найденную игру"""
        game_names = [name.strip() for name in game_names_str.split(',') if name.strip()]

        if not game_names:
            self.stdout.write('⚠️  Не указаны имена игр')
            return self._empty_result()

        if debug:
            self.stdout.write(f'🔍 Поиск САМОЙ ПОПУЛЯРНОЙ игры по имени: "{game_names[0]}"')

        # Формируем условие для поиска игры по ТОЧНОМУ названию (без wildcard)
        where_clause = f'name = "{game_names[0]}"'

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
            self.stdout.write(f'🎯 Условие поиска (точное название): {where_clause}')

        # Вместо load_games_by_query используем прямой запрос за ОДНОЙ игрой
        return self._load_single_game_by_exact_name_for_update(where_clause, debug, skip_existing, count_only)

    def _load_single_game_by_exact_name(self, where_clause, debug=False, skip_existing=True, count_only=False):
        """Загружает САМУЮ ПОПУЛЯРНУЮ игру по точному имени С СОБИРАНИЕМ ID СКРИНШОТОВ"""
        # Загружаем существующие ID игр для фильтрации
        from games.models import Game
        existing_game_ids = set()
        if skip_existing:
            existing_game_ids = set(Game.objects.values_list('igdb_id', flat=True))
            if debug:
                self.stdout.write(f'   📊 Игр в базе для фильтрации: {len(existing_game_ids)}')

        try:
            if debug:
                self.stdout.write(f'   🎯 Запрос самой популярной игры...')

            # ЗАПРОС ДОЛЖЕН ВКЛЮЧАТЬ screenshots!
            query = f'''
                fields id,name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover,game_type,screenshots;
                where {where_clause};
                sort rating_count desc;
                limit 1;
            '''.strip()

            games = make_igdb_request('games', query, debug=False)

            if not games:
                if debug:
                    self.stdout.write('   ❌ Игра с таким названием не найдена')
                return self._empty_result()

            game = games[0]
            game_id = game.get('id')

            if debug:
                self.stdout.write(
                    f'   ✅ Найдена игра: "{game.get("name")}" (ID: {game_id}, rating_count: {game.get("rating_count", 0)})')
                if game.get('screenshots'):
                    self.stdout.write(f'   📸 Скриншотов у игры: {len(game.get("screenshots", []))}')

            # Проверяем существование в базе
            if skip_existing and game_id in existing_game_ids:
                if debug:
                    self.stdout.write(f'   ⏭️  Игра уже есть в базе, пропускаем')
                return {
                    'new_games': [],
                    'all_found_games': [game],
                    'total_games_checked': 1,
                    'new_games_count': 0,
                    'existing_games_skipped': 1,
                    'last_checked_offset': 0,
                    'limit_reached': False,
                    'limit_reached_at_offset': None,
                    'interrupted': False,
                }

            # Возвращаем только одну игру
            return {
                'new_games': [game],
                'all_found_games': [game],
                'total_games_checked': 1,
                'new_games_count': 1,
                'existing_games_skipped': 0,
                'last_checked_offset': 0,
                'limit_reached': False,
                'limit_reached_at_offset': None,
                'interrupted': False,
            }

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка при запросе игры: {str(e)}')
            return self._empty_result()

    def _empty_result(self):
        """Возвращает пустой результат"""
        return {
            'new_games': [],
            'all_found_games': [],
            'total_games_checked': 0,
            'new_games_count': 0,
            'existing_games_skipped': 0,
            'last_checked_offset': 0,
            'limit_reached': False,
            'limit_reached_at_offset': None,
            'interrupted': False,
        }

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        # Формируем базовое условие для поиска
        where_conditions = ['rating_count > 0', 'name != null']

        if min_rating_count > 0:
            where_conditions.append(f'rating_count >= {min_rating_count}')

        # Добавляем фильтр по game_type если указаны типы
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
            self.stdout.write(f'   🎯 Условие поиска популярных игр: {where_clause}')

        return self.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False  # НЕ показываем прогресс загрузки
        )

    def load_games_by_query(self, where_clause, debug=False, limit=0, offset=0,
                            skip_existing=True, count_only=False, query_context=None,
                            show_progress=True):  # НОВЫЙ ПАРАМЕТР
        """Загрузка игр по запросу с пагинацией и offset"""
        # Инициализация
        self._init_loading_session(debug, limit, offset, count_only)
        start_time = time.time()

        # Загрузка существующих ID игр
        existing_game_ids = self._load_existing_ids_for_filtering(skip_existing, debug)

        # Основные структуры данных
        new_games, all_found_games = [], []
        stats = self._init_loading_stats()

        # Обработчик прерывания
        interrupted = threading.Event()
        original_sigint = signal.getsignal(signal.SIGINT)

        def signal_handler(sig, frame):
            interrupted.set()
            self.stdout.write('\n\n⚠️  ПРЕРЫВАНИЕ (Ctrl+C) - завершаю...')

        signal.signal(signal.SIGINT, signal_handler)

        try:
            # Основной цикл загрузки
            result = self._execute_loading_main_loop(
                where_clause, limit, offset, skip_existing,
                existing_game_ids, new_games, all_found_games,
                stats, debug, interrupted, show_progress  # Передаем show_progress
            )

        except KeyboardInterrupt:
            interrupted.set()
            self.stdout.write('\n\n⚠️  ПРЕРЫВАНИЕ ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')
            last_offset = stats.get('last_checked_offset', offset)
            result = {
                'last_checked_offset': last_offset,
                'interrupted': True,
                'limit_reached': False
            }
        finally:
            signal.signal(signal.SIGINT, original_sigint)

        # Финальная обработка
        final_result = self._finalize_and_return_results(
            new_games, all_found_games, stats, result,
            limit, offset, start_time, debug, interrupted.is_set()
        )

        if interrupted.is_set():
            final_result['should_save_offset'] = True
            final_result['interrupted'] = True

        return final_result

    def _init_loading_session(self, debug, limit, offset, count_only):
        """Инициализирует сессию загрузки"""
        if debug:
            self.stdout.write('   📥 Начало загрузки игр...')
        else:
            self.stdout.write('   🔍 Поиск игр...')

        if limit > 0:
            if count_only:
                self.stdout.write(f'   🎯 Цель: найти {limit} НОВЫХ игр (которых нет в базе)')
            else:
                self.stdout.write(f'   🎯 Цель: загрузить {limit} НОВЫХ игр')
        if offset > 0:
            self.stdout.write(f'   ⏭️  Начинаем с позиции: {offset}')

    def _load_existing_ids_for_filtering(self, skip_existing, debug):
        """Загружает существующие ID игр для фильтрации"""
        from games.models import Game

        if not skip_existing:
            return set()

        # Используем values_list для оптимизации запроса
        try:
            existing_game_ids = set(Game.objects.values_list('igdb_id', flat=True))
            if debug:
                self.stdout.write(f'   📊 Игр в базе для фильтрации: {len(existing_game_ids)}')
            return existing_game_ids
        except Exception as e:
            if debug:
                self.stderr.write(f'   ⚠️  Ошибка загрузки существующих ID: {e}')
            return set()

    def _init_loading_stats(self):
        """Инициализирует статистику загрузки"""
        return {
            'total_checked': 0,
            'already_in_db': 0,
            'batches_processed': 0,
            'empty_batches': 0,
            'cycles': 0
        }

    def _execute_loading_main_loop(self, where_clause, limit, offset, skip_existing,
                                   existing_game_ids, new_games, all_found_games,
                                   stats, debug, interrupted, show_progress=True):  # НОВЫЙ ПАРАМЕТР
        """Выполняет основной цикл загрузки"""
        # Параметры загрузки
        BATCH_SIZE = 100
        BATCHES_PER_CYCLE = 2
        MAX_WORKERS = 3
        MAX_EMPTY_BATCHES = 5

        # Определяем, это ли специфический поиск (по режимам или именам)?
        is_specific_search = 'game_modes = (' in where_clause or 'name ~ *"' in where_clause

        # Для специфического поиска меняем параметры
        if is_specific_search:
            BATCH_SIZE = 20  # Меньшие пачки
            BATCHES_PER_CYCLE = 1  # По одной пачке за цикл
            MAX_EMPTY_BATCHES = 10  # Больше пустых пачек разрешено

        current_offset = offset
        batch_number = 1
        empty_batches_in_a_row = 0
        last_checked_offset = offset
        start_time = time.time()

        # ДЛЯ СПЕЦИФИЧЕСКОГО ПОИСКА: Считаем сколько игр просмотрено
        games_checked_for_new = 0
        MAX_GAMES_TO_CHECK = 1000  # Максимум проверить 1000 игр перед остановкой

        while not interrupted.is_set():
            # Для специфического поиска: проверяем лимит просмотренных игр
            if is_specific_search and games_checked_for_new >= MAX_GAMES_TO_CHECK:
                if debug:
                    self.stdout.write(f'   ⚠️  Проверено {MAX_GAMES_TO_CHECK} игр, новых не найдено - останавливаемся')
                break

            # Проверка условий завершения
            if self._check_loading_completion_conditions(
                    limit, len(new_games), empty_batches_in_a_row,
                    MAX_EMPTY_BATCHES, start_time, debug
            ):
                break

            # Создание и выполнение пачек загрузки
            batch_results = self._create_and_execute_batch_cycle(
                where_clause, limit, len(new_games), current_offset,
                batch_number, BATCH_SIZE, BATCHES_PER_CYCLE,
                MAX_WORKERS, debug, interrupted
            )

            if interrupted.is_set():
                break

            # Обработка результатов пачек
            cycle_result = self._process_batch_cycle_results(
                batch_results, existing_game_ids, skip_existing, limit,
                new_games, all_found_games, stats, debug, show_progress  # Передаем show_progress
            )

            empty_batches_in_a_row = cycle_result['empty_batches']
            last_checked_offset = cycle_result['last_offset']

            # Для специфического поиска: обновляем счетчик проверенных игр
            if is_specific_search:
                games_in_this_cycle = sum(len(games) for _, _, games, _, _ in batch_results)
                games_checked_for_new += games_in_this_cycle
                if debug and show_progress:  # Только если показываем прогресс
                    self.stdout.write(
                        f'   📊 Проверено игр в этом цикле: {games_in_this_cycle}, всего: {games_checked_for_new}')

            # Проверка достижения лимита
            if limit > 0 and len(new_games) >= limit:
                if debug and show_progress:
                    self.stdout.write(f'   🎯 Достигнут лимит {limit} новых игр на offset {last_checked_offset}')
                break

            # Подготовка к следующему циклу
            current_offset += BATCH_SIZE * BATCHES_PER_CYCLE
            batch_number += len(batch_results)
            stats['cycles'] += 1

            # Короткая пауза для снижения нагрузки
            time.sleep(0.3)

        return {
            'last_checked_offset': last_checked_offset,
            'limit_reached': limit > 0 and len(new_games) >= limit,
            'interrupted': interrupted.is_set()
        }

    def _check_loading_completion_conditions(self, limit, new_games_count, empty_batches,
                                             max_empty_batches, start_time, debug):
        """Проверяет условия завершения загрузки"""
        # Лимит новых игр достигнут
        if limit > 0 and new_games_count >= limit:
            return True

        # Слишком много пустых пачек подряд
        if empty_batches >= max_empty_batches:
            if debug:
                self.stdout.write(f'   💤 {empty_batches} пустых пачек подряд - достигнут конец результатов')
            return True

        # Превышено максимальное время выполнения
        if time.time() - start_time > 120:
            self.stdout.write(f'   ⏱️  Превышено время выполнения (2 минуты)')
            self.stdout.write(f'   📊 Найдено за это время: {new_games_count} новых игр')
            return True

        return False

    def _create_and_execute_batch_cycle(self, where_clause, limit, current_new_games, current_offset,
                                        batch_number, batch_size, batches_per_cycle, max_workers, debug, interrupted):
        """Создает и выполняет цикл загрузки пачек"""
        # Создание задач для пачек
        batch_tasks = self._create_batch_tasks_for_cycle(
            batch_number, current_offset, batch_size, batches_per_cycle,
            limit, current_new_games
        )

        if not batch_tasks:
            return []

        if debug:
            self.stdout.write(f'   🔄 Цикл загрузки: {len(batch_tasks)} пачек, смещение: {current_offset}')

        # Параллельное выполнение загрузки пачек
        return self._execute_batch_tasks_in_parallel(
            batch_tasks, where_clause, max_workers, debug, interrupted
        )

    def _create_batch_tasks_for_cycle(self, batch_number, current_offset, batch_size,
                                      batches_per_cycle, limit, current_new_games):
        """Создает задачи для пачек в цикле"""
        batch_tasks = []
        batch_size_actual = batch_size

        # Корректировка размера пачки если есть лимит
        if limit > 0:
            needed = limit - current_new_games
            if needed <= 0:
                return []
            batch_size_actual = min(batch_size, needed)

        # Создание задач для каждой пачки
        for i in range(batches_per_cycle):
            batch_offset = current_offset + (i * batch_size_actual)
            batch_tasks.append((batch_number + i, batch_offset, batch_size_actual))

        return batch_tasks

    def _execute_batch_tasks_in_parallel(self, batch_tasks, where_clause, max_workers, debug, interrupted):
        """Выполняет задачи пачек параллельно"""
        batch_results = []

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            # Запуск загрузки пачек
            for batch_num, batch_offset, batch_limit in batch_tasks:
                if interrupted.is_set():
                    break

                future = executor.submit(
                    self._load_single_batch,
                    batch_num, batch_offset, batch_limit, where_clause, debug
                )
                futures[future] = (batch_num, batch_offset, batch_limit)

            # Обработка результатов
            for future in as_completed(futures):
                if interrupted.is_set():
                    break

                batch_num, batch_offset, batch_limit = futures[future]
                try:
                    batch_games = future.result()
                    if batch_games:
                        batch_results.append((batch_num, batch_offset, batch_games, len(batch_games), False))
                    else:
                        batch_results.append((batch_num, batch_offset, [], 0, True))
                except Exception as e:
                    if debug:
                        self.stderr.write(f'      ❌ Ошибка пачки {batch_num}: {e}')
                    batch_results.append((batch_num, batch_offset, [], 0, True))

        return batch_results

    def _load_single_batch(self, batch_num, batch_offset, batch_limit, where_clause, debug):
        """Загружает одну пачку игр"""
        try:
            if debug:
                self.stdout.write(f'      📦 Пачка {batch_num}: загрузка {batch_offset}-{batch_offset + batch_limit}...')

            query = f'''
                fields id,name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover,game_type,screenshots;
                where {where_clause};
                sort rating_count desc;
                limit {batch_limit};
                offset {batch_offset};
            '''.strip()

            return make_igdb_request('games', query, debug=False)

        except Exception as e:
            if debug:
                self.stderr.write(f'      ❌ Ошибка пачки {batch_num}: {e}')
            return []

    def _process_batch_cycle_results(self, batch_results, existing_game_ids, skip_existing, limit,
                                     new_games, all_found_games, stats, debug, show_progress=True):
        """Обрабатывает результаты цикла пачек"""
        game_lock = threading.Lock()
        empty_batches = 0
        last_offset = 0

        for batch_num, batch_offset, games, games_loaded, is_empty in batch_results:
            if not is_empty and games_loaded > 0:
                with game_lock:
                    # Обработка каждой пачки игр
                    batch_stats = self._process_individual_batch(
                        games, batch_offset, existing_game_ids, skip_existing,
                        limit, new_games, all_found_games
                    )

                    # Обновление статистики
                    stats['total_checked'] += batch_stats['total']
                    stats['already_in_db'] += batch_stats['in_db']
                    stats['batches_processed'] += 1
                    last_offset = batch_stats['last_offset']

                    # Вывод прогресса ТОЛЬКО если включен show_progress
                    if show_progress:
                        self._display_loading_progress(
                            limit, len(new_games), stats, last_offset, debug
                        )

                empty_batches = 0  # Сброс счетчика пустых пачек
            else:
                empty_batches += 1
                last_offset = max(last_offset, batch_offset + 100)
                stats['empty_batches'] += 1

        return {
            'empty_batches': empty_batches,
            'last_offset': last_offset
        }

    def _process_individual_batch(self, games, batch_offset, existing_game_ids, skip_existing,
                                  limit, new_games, all_found_games):
        """Обрабатывает отдельную пачку игр"""
        batch_stats = {
            'total': 0,
            'in_db': 0,
            'last_offset': batch_offset
        }

        for i, game in enumerate(games):
            game_id = game.get('id')
            if not game_id:
                continue

            # Вычисление offset для этой игры
            current_game_offset = batch_offset + i
            batch_stats['last_offset'] = current_game_offset
            batch_stats['total'] += 1

            # Добавление в общий список
            all_found_games.append(game)

            # Проверка существования в базе
            if skip_existing and game_id in existing_game_ids:
                batch_stats['in_db'] += 1
                continue

            # Добавление новой игры
            new_games.append(game)

            # Проверка достижения лимита
            if limit > 0 and len(new_games) >= limit:
                break

        return batch_stats

    def _display_loading_progress(self, limit, new_games_count, stats, last_offset, debug):
        """Отображает прогресс загрузки"""
        if limit > 0 and new_games_count % 10 == 0:
            progress_msg = f'   📊 Прогресс: {new_games_count}/{limit} новых игр (просмотрено: {stats["total_checked"]}, уже в БД: {stats["already_in_db"]}, offset: {last_offset})'
            self.stdout.write(progress_msg)
        elif stats['total_checked'] % 200 == 0:
            progress_msg = f'   📊 Просмотрено: {stats["total_checked"]} игр (новых: {new_games_count}, уже в БД: {stats["already_in_db"]})'
            self.stdout.write(progress_msg)

    def _finalize_and_return_results(self, new_games, all_found_games, stats, result,
                                     limit, offset, start_time, debug, interrupted):
        """Завершает загрузку и возвращает результаты"""
        # Обрезка результатов если превышен лимит
        if limit > 0 and len(new_games) > limit:
            new_games = new_games[:limit]
            if debug:
                self.stdout.write(f'   ✂️  Обрезано новых игр до лимита {limit}: {len(new_games)}')

        # Расчет итогового времени
        total_time = time.time() - start_time

        # Получение итоговых значений
        last_checked_offset = result.get('last_checked_offset', offset)
        limit_reached = result.get('limit_reached', False)

        # Вывод финальной статистики
        if debug or interrupted:
            self._display_final_loading_stats(
                total_time, stats, len(new_games), last_checked_offset,
                limit_reached, interrupted
            )

        # Формирование и возврат результатов
        return self._format_loading_results(
            new_games, all_found_games, stats, last_checked_offset,
            limit_reached, interrupted
        )

    def _display_final_loading_stats(self, total_time, stats, new_games_count,
                                     last_checked_offset, limit_reached, interrupted):
        """Отображает финальную статистику загрузки"""
        self.stdout.write(f'\n📊 ИТОГОВАЯ СТАТИСТИКА:')
        self.stdout.write(f'   📍 Последний проверенный offset: {last_checked_offset}')
        self.stdout.write(f'   👀 Всего просмотрено игр: {stats["total_checked"]}')
        self.stdout.write(f'   🗄️  Игр уже в БД: {stats["already_in_db"]}')
        self.stdout.write(f'   🆕 Найдено новых игр: {new_games_count}')
        self.stdout.write(f'   ⏱️  Время: {total_time:.1f}с')

        if interrupted:
            self.stdout.write(f'   ⚠️  Загрузка прервана пользователем')
        elif limit_reached:
            self.stdout.write(f'   🎯 Лимит достигнут на offset: {last_checked_offset}')

        if total_time > 0:
            speed = stats['total_checked'] / total_time
            self.stdout.write(f'   🚀 Скорость: {speed:.1f} игр/сек')

    def _format_loading_results(self, new_games, all_found_games, stats, last_checked_offset,
                                limit_reached, interrupted):
        """Форматирует результаты загрузки"""
        return {
            'new_games': new_games,
            'all_found_games': all_found_games,
            'total_games_checked': stats['total_checked'],
            'new_games_count': len(new_games),
            'existing_games_skipped': stats['already_in_db'],
            'last_checked_offset': last_checked_offset,
            'limit_reached': limit_reached,
            'limit_reached_at_offset': last_checked_offset if limit_reached else None,
            'interrupted': interrupted,
        }

    def collect_all_data_ids(self, all_games_data, debug=False):
        """Собирает все ID для последующей загрузки"""
        all_game_ids = []
        all_cover_ids = []
        all_genre_ids = set()
        all_platform_ids = set()
        all_keyword_ids = set()
        all_engine_ids = set()  # НОВОЕ: для движков
        game_data_map = {}

        # НОВОЕ: собираем информацию о скриншотах
        screenshots_info = {}  # game_id -> количество скриншотов

        if debug:
            self.stdout.write('   📊 Сбор всех ID данных...')

        for game_data in all_games_data:
            game_id = game_data.get('id')
            if not game_id:
                continue

            all_game_ids.append(game_id)
            game_data_map[game_id] = game_data

            if game_data.get('cover'):
                all_cover_ids.append(game_data['cover'])

            if game_data.get('genres'):
                all_genre_ids.update(game_data['genres'])

            if game_data.get('platforms'):
                all_platform_ids.update(game_data['platforms'])

            if game_data.get('keywords'):
                all_keyword_ids.update(game_data['keywords'])

            # НОВОЕ: собираем ID движков
            if game_data.get('game_engines'):
                for engine in game_data['game_engines']:
                    if isinstance(engine, dict) and engine.get('id'):
                        all_engine_ids.add(engine['id'])
                    elif isinstance(engine, int):
                        all_engine_ids.add(engine)

            # НОВОЕ: собираем информацию о скриншотах
            if game_data.get('screenshots'):
                screenshots_info[game_id] = len(game_data['screenshots'])
                if debug:
                    self.stdout.write(f'      • Игра {game_id}: {len(game_data["screenshots"])} скриншотов')
            else:
                screenshots_info[game_id] = 0

        if debug:
            self.stdout.write(f'   ✅ Собрано ID:')
            self.stdout.write(f'      • Игр: {len(all_game_ids)}')
            self.stdout.write(f'      • Обложек: {len(set(all_cover_ids))}')
            self.stdout.write(f'      • Жанров: {len(all_genre_ids)}')
            self.stdout.write(f'      • Платформ: {len(all_platform_ids)}')
            self.stdout.write(f'      • Ключевых слов: {len(all_keyword_ids)}')
            self.stdout.write(f'      • Движков: {len(all_engine_ids)}')  # НОВОЕ
            # НОВОЕ:
            games_with_screenshots = len([v for v in screenshots_info.values() if v > 0])
            self.stdout.write(f'      • Игр со скриншотами: {games_with_screenshots}')

        return {
            'game_data_map': game_data_map,
            'all_game_ids': all_game_ids,
            'all_cover_ids': list(set(all_cover_ids)),
            'all_genre_ids': list(all_genre_ids),
            'all_platform_ids': list(all_platform_ids),
            'all_keyword_ids': list(all_keyword_ids),
            'all_engine_ids': list(all_engine_ids),  # НОВОЕ
            'all_screenshot_games': all_game_ids,  # Все игры могут иметь скриншоты
            'screenshots_info': screenshots_info,  # НОВОЕ: передаем информацию о скриншотах
        }
