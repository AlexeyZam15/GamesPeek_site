# games/management/commands/load_igdb/data_collector.py
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import Counter
from games.igdb_api import make_igdb_request
from games.models import Game

try:
    from .game_cache import GameCacheManager
except ImportError:
    # Fallback если файл не найден
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

    def load_games_by_query(self, where_clause, debug=False, limit=0, offset=0,
                            skip_existing=True, count_only=False, query_context=None):
        """Загрузка игр по запросу с пагинацией и offset"""
        import threading
        import time
        import signal

        # 1. Инициализация сессии
        self._init_loading_session(debug, limit, offset, count_only)
        start_time = time.time()

        # 2. Загрузка существующих ID игр
        existing_game_ids = self._load_existing_ids_for_filtering(skip_existing, debug)

        # 3. Основные структуры данных
        new_games, all_found_games = [], []
        stats = self._init_loading_stats()

        # 4. Обработчик прерывания
        interrupted = threading.Event()
        original_sigint = signal.getsignal(signal.SIGINT)

        def signal_handler(sig, frame):
            interrupted.set()
            self.stdout.write('\n\n⚠️  ПРЕРЫВАНИЕ (Ctrl+C) - завершаю...')

        signal.signal(signal.SIGINT, signal_handler)

        try:
            # 5. Основной цикл загрузки
            result = self._execute_loading_main_loop(
                where_clause, limit, offset, skip_existing,
                existing_game_ids, new_games, all_found_games,
                stats, debug, interrupted
            )

        except KeyboardInterrupt:
            interrupted.set()
            self.stdout.write('\n\n⚠️  ПРЕРЫВАНИЕ ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')
            # Получаем последний offset из stats если есть
            last_offset = stats.get('last_checked_offset', offset)
            result = {
                'last_checked_offset': last_offset,
                'interrupted': True,
                'limit_reached': False
            }
        finally:
            signal.signal(signal.SIGINT, original_sigint)

        # 6. Финальная обработка и вывод результатов
        final_result = self._finalize_and_return_results(
            new_games, all_found_games, stats, result,
            limit, offset, start_time, debug, interrupted.is_set()
        )

        # 7. 🔴 ИСПРАВЛЕНИЕ: Сохраняем offset при прерывании
        # Нужно сохранять где-то выше по стеку вызовов, не здесь
        # Но добавим информацию в результат для сохранения
        if interrupted.is_set():
            final_result['should_save_offset'] = True
            final_result['interrupted'] = True

        return final_result

    def _save_interrupted_offset(self, final_result, query_context, where_clause, last_offset, debug):
        """Сохраняет offset при прерывании"""
        try:
            from .offset_manager import OffsetManager

            # Получаем параметры запроса из контекста
            params = {
                'genres': query_context.get('genres', ''),
                'description_contains': query_context.get('description_contains', ''),
                'keywords': query_context.get('keywords', ''),
                'game_types': query_context.get('game_types', ''),
                'min_rating_count': query_context.get('min_rating_count', 0),
                'mode': query_context.get('loading_mode', 'popular'),
            }

            # Создаем уникальный ключ запроса
            query_key = OffsetManager.get_query_key(where_clause, **params)

            # Сохраняем offset для продолжения
            next_offset = last_offset + 1
            saved = OffsetManager.save_offset(query_key, next_offset)

            if saved:
                self.stdout.write(f'\n💾 Сохранен offset для продолжения: {next_offset}')
                self.stdout.write(f'📋 Для продолжения используйте: --offset {next_offset}')

                # Сохраняем также стартовый offset для диагностики
                OffsetManager.save_offset(f"{query_key}_start", 0)

                return True

        except Exception as e:
            if query_context.get('debug', False):
                self.stderr.write(f'   ❌ Ошибка при сохранении offset: {e}')

        return False

    def _save_offset_for_continuation(self, final_result, query_context, where_clause,
                                      interrupted, limit, new_games_count):
        """Сохраняет offset для продолжения загрузки"""
        try:
            from .offset_manager import OffsetManager

            # Проверяем условия для сохранения offset
            should_save_offset = (
                    interrupted or  # Прерывание пользователем
                    (limit > 0 and new_games_count >= limit) or  # Достигнут лимит
                    final_result.get('limit_reached', False)  # Лимит достигнут в цикле
            )

            if should_save_offset:
                last_checked_offset = final_result.get('last_checked_offset', 0)
                next_offset = last_checked_offset + 1

                # Получаем параметры запроса из контекста
                params = {
                    'genres': query_context.get('genres', ''),
                    'description_contains': query_context.get('description_contains', ''),
                    'keywords': query_context.get('keywords', ''),
                    'game_types': query_context.get('game_types', ''),
                    'min_rating_count': query_context.get('min_rating_count', 0),
                    'mode': query_context.get('loading_mode', 'popular'),
                }

                # Создаем уникальный ключ запроса
                query_key = OffsetManager.get_query_key(where_clause, **params)

                # Сохраняем offset для продолжения
                saved = OffsetManager.save_offset(query_key, next_offset)

                if saved:
                    self.stdout.write(f'   💾 Сохранен offset для продолжения: {next_offset}')

                    # Информация для пользователя
                    if interrupted:
                        self.stdout.write(f'   📋 Для продолжения используйте: --offset {next_offset}')
                    elif limit > 0 and new_games_count >= limit:
                        self.stdout.write(f'   📋 Достигнут лимит {limit}, offset сохранен для продолжения')

                    return True

        except Exception as e:
            if query_context.get('debug', False):
                self.stderr.write(f'   ❌ Ошибка при сохранении offset: {e}')

        return False

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
        existing_game_ids = set()
        if skip_existing:
            existing_game_ids = set(Game.objects.values_list('igdb_id', flat=True))
            if debug:
                self.stdout.write(f'   📊 Игр в базе для фильтрации: {len(existing_game_ids)}')
        return existing_game_ids

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
                                   stats, debug, interrupted):
        """Выполняет основной цикл загрузки"""
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Параметры загрузки
        BATCH_SIZE = 100
        BATCHES_PER_CYCLE = 2
        MAX_WORKERS = 3
        MAX_EMPTY_BATCHES = 5

        current_offset = offset
        batch_number = 1
        empty_batches_in_a_row = 0
        last_checked_offset = offset  # 🔴 Храним last_offset внутри метода
        start_time = time.time()

        while not interrupted.is_set():
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
                new_games, all_found_games, stats, debug
            )

            empty_batches_in_a_row = cycle_result['empty_batches']
            last_checked_offset = cycle_result['last_offset']  # 🔴 Обновляем last_offset

            # Проверка достижения лимита
            if limit > 0 and len(new_games) >= limit:
                if debug:
                    self.stdout.write(f'   🎯 Достигнут лимит {limit} новых игр на offset {last_checked_offset}')
                break

            # Подготовка к следующему циклу
            current_offset += BATCH_SIZE * BATCHES_PER_CYCLE
            batch_number += len(batch_results)
            stats['cycles'] += 1

            # Короткая пауза для снижения нагрузки
            time.sleep(0.3)

        return {
            'last_checked_offset': last_checked_offset,  # 🔴 Возвращаем last_offset
            'limit_reached': limit > 0 and len(new_games) >= limit,
            'interrupted': interrupted.is_set()
        }

    def _check_loading_completion_conditions(self, limit, new_games_count, empty_batches,
                                             max_empty_batches, start_time, debug):
        """Проверяет условия завершения загрузки"""
        import time

        # Лимит новых игр достигнут
        if limit > 0 and new_games_count >= limit:
            return True

        # Слишком много пустых пачек подряд
        if empty_batches >= max_empty_batches:
            if debug:
                self.stdout.write(f'   💤 {empty_batches} пустых пачек подряд - достигнут конец результатов')
            return True

        # Превышено максимальное время выполнения
        if time.time() - start_time > 120:  # 2 минуты максимум
            self.stdout.write(f'   ⏱️  Превышено время выполнения (2 минуты)')
            self.stdout.write(f'   📊 Найдено за это время: {new_games_count} новых игр')
            return True

        return False

    def _create_and_execute_batch_cycle(self, where_clause, limit, current_new_games, current_offset,
                                        batch_number, batch_size, batches_per_cycle, max_workers, debug, interrupted):
        """Создает и выполняет цикл загрузки пачек"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

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
        from concurrent.futures import ThreadPoolExecutor, as_completed

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

    def _process_batch_cycle_results(self, batch_results, existing_game_ids, skip_existing, limit,
                                     new_games, all_found_games, stats, debug):
        """Обрабатывает результаты цикла пачек"""
        import threading
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

                    # Вывод прогресса
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
        import time

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

    def _get_existing_game_ids(self, skip_existing, debug):
        """Получает существующие ID игр"""
        from games.models import Game
        existing_game_ids = set()
        if skip_existing:
            existing_game_ids = set(Game.objects.values_list('igdb_id', flat=True))
            if debug:
                self.stdout.write(f'   📊 Игр в базе для фильтрации: {len(existing_game_ids)}')
        return existing_game_ids

    def _init_stats(self):
        """Инициализирует статистику"""
        return {
            'total_checked': 0,
            'already_in_db': 0,
            'batches_processed': 0,
            'empty_batches': 0
        }

    def _execute_loading_loop(self, where_clause, limit, offset, skip_existing,
                              existing_game_ids, new_games, all_found_games,
                              stats, debug, interrupted):
        """Выполняет основной цикл загрузки"""
        import time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Параметры
        BATCH_SIZE = 100
        BATCHES_PER_CYCLE = 2
        MAX_WORKERS = 3
        MAX_EMPTY_BATCHES = 5

        current_offset = offset
        batch_number = 1
        empty_batches_in_a_row = 0
        last_checked_offset = offset
        start_time = time.time()

        while not interrupted[0]:
            # Проверка условий завершения
            if self._should_stop_loading(limit, len(new_games), empty_batches_in_a_row,
                                         MAX_EMPTY_BATCHES, start_time, debug):
                break

            # Загрузка пачек
            batch_results = self._load_batch_cycle(
                where_clause, limit, len(new_games), current_offset,
                batch_number, BATCH_SIZE, BATCHES_PER_CYCLE,
                MAX_WORKERS, debug, interrupted
            )

            if interrupted[0]:
                break

            # Обработка результатов
            cycle_result = self._process_batch_cycle(
                batch_results, existing_game_ids, skip_existing, limit,
                new_games, all_found_games, stats, debug, interrupted
            )

            empty_batches_in_a_row = cycle_result['empty_batches']
            last_checked_offset = cycle_result['last_offset']

            # Проверка лимита
            if limit > 0 and len(new_games) >= limit:
                if debug:
                    self.stdout.write(f'   🎯 Достигнут лимит {limit} новых игр на offset {last_checked_offset}')
                break

            # Следующий цикл
            current_offset += BATCH_SIZE * BATCHES_PER_CYCLE
            batch_number += len(batch_results)
            time.sleep(0.3)

        return {
            'last_checked_offset': last_checked_offset,
            'limit_reached': limit > 0 and len(new_games) >= limit
        }

    def _should_stop_loading(self, limit, new_games_count, empty_batches,
                             max_empty_batches, start_time, debug):
        """Проверяет, нужно ли остановить загрузку"""
        import time

        # Лимит достигнут
        if limit > 0 and new_games_count >= limit:
            return True

        # Слишком много пустых пачек
        if empty_batches >= max_empty_batches:
            if debug:
                self.stdout.write(f'   💤 {empty_batches} пустых пачек подряд - достигнут конец результатов')
            return True

        # Превышено время
        if time.time() - start_time > 120:
            self.stdout.write(f'   ⏱️  Превышено время выполнения (2 минуты)')
            self.stdout.write(f'   📊 Найдено за это время: {new_games_count} новых игр')
            return True

        return False

    def _load_batch_cycle(self, where_clause, limit, current_new_games, current_offset,
                          batch_number, batch_size, batches_per_cycle, max_workers, debug, interrupted):
        """Загружает цикл пачек"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        # Создаем задачи
        batch_tasks = []
        batch_size_actual = batch_size

        if limit > 0:
            needed = limit - current_new_games
            if needed <= 0:
                return []
            batch_size_actual = min(batch_size, needed)

        for i in range(batches_per_cycle):
            batch_offset = current_offset + (i * batch_size_actual)
            batch_tasks.append((batch_number + i, batch_offset, batch_size_actual))

        if not batch_tasks:
            return []

        if debug:
            self.stdout.write(f'   🔄 Цикл загрузки: {len(batch_tasks)} пачек, смещение: {current_offset}')

        # Параллельная загрузка
        batch_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            for batch_num, batch_offset, batch_limit in batch_tasks:
                if interrupted[0]:
                    break
                future = executor.submit(self._load_single_batch, batch_num, batch_offset, batch_limit, where_clause,
                                         debug)
                futures[future] = (batch_num, batch_offset, batch_limit)

            for future in as_completed(futures):
                if interrupted[0]:
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

    def _process_batch_cycle(self, batch_results, existing_game_ids, skip_existing, limit,
                             new_games, all_found_games, stats, debug, interrupted):
        """Обрабатывает результаты цикла пачек"""
        import threading
        game_lock = threading.Lock()

        empty_batches = 0
        last_offset = 0

        for batch_num, batch_offset, games, games_loaded, is_empty in batch_results:
            if interrupted[0]:
                break

            if not is_empty and games_loaded > 0:
                with game_lock:
                    # Обработка игр
                    batch_stats = self._process_games_batch(
                        games, batch_offset, existing_game_ids, skip_existing,
                        limit, new_games, all_found_games
                    )

                    stats['total_checked'] += batch_stats['total']
                    stats['already_in_db'] += batch_stats['in_db']
                    stats['batches_processed'] += 1
                    last_offset = batch_stats['last_offset']

                    # Вывод прогресса
                    if limit > 0 and len(new_games) % 10 == 0:
                        progress_msg = f'   📊 Прогресс: {len(new_games)}/{limit} новых игр (просмотрено: {stats["total_checked"]}, уже в БД: {stats["already_in_db"]}, offset: {last_offset})'
                        self.stdout.write(progress_msg)
                    elif stats['total_checked'] % 200 == 0:
                        progress_msg = f'   📊 Просмотрено: {stats["total_checked"]} игр (новых: {len(new_games)}, уже в БД: {stats["already_in_db"]})'
                        self.stdout.write(progress_msg)

                empty_batches = 0  # Сбрасываем счетчик пустых пачек
            else:
                empty_batches += 1
                last_offset = max(last_offset, batch_offset + 100)
                stats['empty_batches'] += 1

        return {
            'empty_batches': empty_batches,
            'last_offset': last_offset
        }

    def _process_games_batch(self, games, batch_offset, existing_game_ids, skip_existing,
                             limit, new_games, all_found_games):
        """Обрабатывает пачку игр"""
        batch_stats = {
            'total': 0,
            'in_db': 0,
            'last_offset': batch_offset
        }

        for i, game in enumerate(games):
            game_id = game.get('id')
            if not game_id:
                continue

            current_offset = batch_offset + i
            batch_stats['last_offset'] = current_offset
            batch_stats['total'] += 1

            all_found_games.append(game)

            if skip_existing and game_id in existing_game_ids:
                batch_stats['in_db'] += 1
                continue

            new_games.append(game)

            if limit > 0 and len(new_games) >= limit:
                break

        return batch_stats

    def _finalize_loading(self, new_games, all_found_games, stats, result,
                          limit, offset, start_time, debug, interrupted):
        """Завершает загрузку и возвращает результат"""
        import time

        # Обрезка если превышен лимит
        if limit > 0 and len(new_games) > limit:
            new_games = new_games[:limit]
            if debug:
                self.stdout.write(f'   ✂️  Обрезано новых игр до лимита {limit}: {len(new_games)}')

        total_time = time.time() - start_time
        last_checked_offset = result.get('last_checked_offset', offset)
        limit_reached = result.get('limit_reached', False)

        # Вывод статистики
        if debug or interrupted:
            self.stdout.write(f'\n📊 ИТОГОВАЯ СТАТИСТИКА:')
            self.stdout.write(f'   📍 Последний проверенный offset: {last_checked_offset}')
            self.stdout.write(f'   👀 Всего просмотрено игр: {stats["total_checked"]}')
            self.stdout.write(f'   🗄️  Игр уже в БД: {stats["already_in_db"]}')
            self.stdout.write(f'   🆕 Найдено новых игр: {len(new_games)}')
            self.stdout.write(f'   ⏱️  Время: {total_time:.1f}с')

            if interrupted:
                self.stdout.write(f'   ⚠️  Загрузка прервана пользователем')
            elif limit_reached:
                self.stdout.write(f'   🎯 Лимит достигнут на offset: {last_checked_offset}')

            if total_time > 0:
                speed = stats['total_checked'] / total_time
                self.stdout.write(f'   🚀 Скорость: {speed:.1f} игр/сек')

        # Возвращаем результат
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

    def _display_loading_info(self, limit, offset, count_only):
        """Отображает информацию о загрузке"""
        if limit > 0:
            if count_only:
                self.stdout.write(f'   🎯 Цель: найти {limit} НОВЫХ игр (которых нет в базе)')
            else:
                self.stdout.write(f'   🎯 Цель: загрузить {limit} НОВЫХ игр')
        if offset > 0:
            self.stdout.write(f'   ⏭️  Начинаем с позиции: {offset}')

    def _initialize_loading_stats(self):
        """Инициализирует статистику загрузки"""
        return {
            'total_checked': 0,
            'already_in_db': 0,
            'batches_processed': 0,
            'empty_batches': 0,
            'cycles': 0
        }

    def _load_existing_game_ids(self, skip_existing, debug):
        """Загружает существующие ID игр из базы"""
        existing_game_ids = set()
        if skip_existing:
            existing_game_ids = set(Game.objects.values_list('igdb_id', flat=True))
            if debug:
                self.stdout.write(f'   📊 Игр в базе для фильтрации: {len(existing_game_ids)}')
        return existing_game_ids

    def _load_games_main_loop(self, where_clause, limit, offset, batch_size, batches_per_cycle,
                              max_workers, max_empty_batches, new_games, all_found_games,
                              existing_game_ids, skip_existing, game_lock, stats, debug):
        """Основной цикл загрузки игр"""
        import time

        current_offset = offset
        batch_number = 1
        empty_batches_in_a_row = 0
        last_checked_offset = offset
        limit_reached = False
        limit_reached_offset = offset
        start_time = time.time()

        while not limit_reached:
            # Проверка условий завершения
            should_break = self._check_exit_conditions(
                limit, len(new_games), last_checked_offset,
                empty_batches_in_a_row, max_empty_batches,
                start_time, debug
            )
            if should_break:
                limit_reached = True
                break

            # Создание задач для этого цикла
            batch_tasks = self._create_batch_loading_tasks(
                batch_number, current_offset, batch_size,
                batches_per_cycle, limit, len(new_games)
            )

            if not batch_tasks:
                break

            # Параллельная загрузка пачек
            batch_results = self._load_batches_in_parallel(
                batch_tasks, where_clause, max_workers, debug
            )

            # Обработка результатов
            cycle_result = self._process_batch_results(
                batch_results, game_lock, new_games, all_found_games,
                existing_game_ids, skip_existing, limit, stats, debug
            )

            # Обновление состояния
            empty_batches_in_a_row = cycle_result['empty_batches_in_a_row']
            last_checked_offset = cycle_result['last_checked_offset']

            # Проверка лимита
            if limit > 0 and len(new_games) >= limit:
                limit_reached = True
                limit_reached_offset = last_checked_offset
                if debug:
                    self.stdout.write(f'   🎯 Достигнут лимит {limit} новых игр на offset {last_checked_offset}')
                break

            # Переход к следующему offset
            current_offset = batch_tasks[-1][1] + batch_size
            batch_number += len(batch_tasks)
            stats['cycles'] += 1

            # Пауза между циклами
            time.sleep(0.3)

        return {
            'limit_reached': limit_reached,
            'limit_reached_offset': limit_reached_offset,
            'last_checked_offset': last_checked_offset,
            'empty_batches_in_a_row': empty_batches_in_a_row,
            'stats': stats
        }

    def _check_exit_conditions(self, limit, new_games_count, last_checked_offset,
                               empty_batches_in_a_row, max_empty_batches,
                               start_time, debug):
        """Проверяет условия для выхода из цикла"""
        # Проверка лимита
        if limit > 0 and new_games_count >= limit:
            return True

        # Проверка пустых пачек
        if empty_batches_in_a_row >= max_empty_batches:
            if debug:
                self.stdout.write(f'   💤 {empty_batches_in_a_row} пустых пачек подряд - достигнут конец результатов')
            return True

        # Проверка времени выполнения
        if time.time() - start_time > 120:  # 2 минуты максимум
            self.stdout.write(f'   ⏱️  Превышено время выполнения (2 минуты)')
            self.stdout.write(f'   📊 Найдено за это время: {new_games_count} новых игр')
            return True

        return False

    def _create_batch_loading_tasks(self, batch_number, current_offset, batch_size,
                                    batches_per_cycle, limit, current_new_games):
        """Создает задачи для загрузки пачек"""
        batch_tasks = []

        # Если есть лимит, рассчитываем сколько еще нужно
        if limit > 0:
            needed = limit - current_new_games
            if needed <= 0:
                return []
            batch_size_actual = min(batch_size, needed)
        else:
            batch_size_actual = batch_size

        # Создаем задачи
        for i in range(batches_per_cycle):
            batch_offset = current_offset + (i * batch_size_actual)
            batch_tasks.append((batch_number + i, batch_offset, batch_size_actual))

        return batch_tasks

    def _load_batches_in_parallel(self, batch_tasks, where_clause, max_workers, debug):
        """Параллельная загрузка пачек"""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        if debug:
            self.stdout.write(f'   🔄 Цикл загрузки: {len(batch_tasks)} пачек, смещение: {batch_tasks[0][1]}')

        batch_results = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}

            # Запускаем загрузку пачек
            for batch_num, batch_offset, batch_limit in batch_tasks:
                future = executor.submit(
                    self._load_single_batch,
                    batch_num, batch_offset, batch_limit, where_clause, debug
                )
                futures[future] = (batch_num, batch_offset, batch_limit)

            # Обрабатываем результаты
            for future in as_completed(futures):
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
                fields id,name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover,game_type;
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

    def _process_batch_results(self, batch_results, game_lock, new_games, all_found_games,
                               existing_game_ids, skip_existing, limit, stats, debug):
        """Обрабатывает результаты пачек"""
        empty_batches_in_a_row = 0
        last_checked_offset = 0

        for batch_num, batch_offset, games, games_loaded, is_empty in batch_results:
            if not is_empty and games_loaded > 0:
                with game_lock:
                    batch_stats = self._process_single_batch(
                        games, batch_offset, new_games, all_found_games,
                        existing_game_ids, skip_existing, limit
                    )

                    # Обновляем статистику
                    stats['total_checked'] += batch_stats['total_games']
                    stats['already_in_db'] += batch_stats['already_in_db']
                    stats['batches_processed'] += 1

                    last_checked_offset = max(last_checked_offset, batch_stats['last_offset'])

                    # Вывод прогресса
                    self._display_progress(limit, len(new_games), stats, last_checked_offset, debug)

                empty_batches_in_a_row = 0  # Сбрасываем счетчик пустых пачек
            else:
                empty_batches_in_a_row += 1
                last_checked_offset = max(last_checked_offset, batch_offset + 100)
                stats['empty_batches'] += 1

        return {
            'empty_batches_in_a_row': empty_batches_in_a_row,
            'last_checked_offset': last_checked_offset
        }

    def _process_single_batch(self, games, batch_offset, new_games, all_found_games,
                              existing_game_ids, skip_existing, limit):
        """Обрабатывает одну пачку игр"""
        batch_stats = {
            'total_games': 0,
            'already_in_db': 0,
            'last_offset': batch_offset
        }

        for i, game in enumerate(games):
            game_id = game.get('id')
            if not game_id:
                continue

            # Вычисляем offset этой игры
            current_game_offset = batch_offset + i
            batch_stats['last_offset'] = current_game_offset
            batch_stats['total_games'] += 1

            # Добавляем в общий список
            all_found_games.append(game)

            # Проверяем есть ли игра в базе
            if skip_existing and game_id in existing_game_ids:
                batch_stats['already_in_db'] += 1
                continue

            # Новая игра
            new_games.append(game)

            # Проверяем лимит
            if limit > 0 and len(new_games) >= limit:
                break

        return batch_stats

    def _display_progress(self, limit, new_games_count, stats, last_offset, debug):
        """Отображает прогресс загрузки"""
        if limit > 0 and new_games_count % 10 == 0:
            progress_msg = f'   📊 Прогресс: {new_games_count}/{limit} новых игр (просмотрено: {stats["total_checked"]}, уже в БД: {stats["already_in_db"]}, offset: {last_offset})'
            self.stdout.write(progress_msg)
        elif stats['total_checked'] % 200 == 0:
            progress_msg = f'   📊 Просмотрено: {stats["total_checked"]} игр (новых: {new_games_count}, уже в БД: {stats["already_in_db"]})'
            self.stdout.write(progress_msg)

    def _process_final_results(self, result, new_games, all_found_games, stats,
                               limit, offset, start_time, debug):
        """Обрабатывает финальные результаты загрузки"""
        # Обрезка если превышен лимит
        if limit > 0 and len(new_games) > limit:
            new_games = new_games[:limit]
            if debug:
                self.stdout.write(f'   ✂️  Обрезано новых игр до лимита {limit}: {len(new_games)}')

        # Корректировка offset
        last_checked_offset = result.get('last_checked_offset', offset)
        if result['limit_reached']:
            last_checked_offset = result['limit_reached_offset']
            if debug:
                self.stdout.write(f'   🎯 Лимит достигнут на offset: {last_checked_offset}')

        total_time = time.time() - start_time

        # Вывод статистики
        self._display_final_stats(
            debug, total_time, stats['total_checked'], stats['already_in_db'],
            len(new_games), last_checked_offset, result['limit_reached'],
            result['limit_reached_offset']
        )

        # Возвращаем результат
        return {
            'new_games': new_games,
            'all_found_games': all_found_games,
            'total_games_checked': stats['total_checked'],
            'new_games_count': len(new_games),
            'existing_games_skipped': stats['already_in_db'],
            'last_checked_offset': last_checked_offset,
            'limit_reached': result['limit_reached'],
            'limit_reached_at_offset': result['limit_reached_offset'] if result['limit_reached'] else None,
        }

    def _display_final_stats(self, debug, total_time, total_checked, already_in_db,
                             new_games_count, last_checked_offset, limit_reached,
                             limit_reached_offset):
        """Выводит финальную статистику"""
        if debug:
            self.stdout.write(f'   📊 ИТОГОВАЯ СТАТИСТИКА:')
            self.stdout.write(f'   📍 Последний проверенный offset: {last_checked_offset}')
            self.stdout.write(f'   👀 Всего просмотрено игр: {total_checked}')
            self.stdout.write(f'   🗄️  Игр уже в БД: {already_in_db}')
            self.stdout.write(f'   🆕 Найдено новых игр: {new_games_count}')
            self.stdout.write(f'   ⏱️  Время: {total_time:.1f}с')

            if limit_reached:
                self.stdout.write(f'   🎯 Лимит достигнут на offset: {limit_reached_offset}')

            if total_time > 0:
                speed = total_checked / total_time
                self.stdout.write(f'   🚀 Скорость: {speed:.1f} игр/сек')

    # === ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ===

    def _initialize_stats(self):
        """Инициализирует статистику"""
        return {
            'total_checked': 0,
            'from_cache': 0,
            'from_db': 0,
            'batches_processed': 0,
            'empty_batches': 0,
            'cycles': 0
        }

    def _load_existing_ids(self, skip_existing, debug):
        """Не загружаем ID из базы - используем только кэш"""
        if debug:
            self.stdout.write('   💾 Используется только кэш для фильтрации игр')
        return set()  # Пустое множество

    def _create_batch_tasks(self, batch_number, current_offset, batch_size,
                            batches_per_cycle, limit, current_new_games):
        """Создает задачи для загрузки пачек"""
        batch_tasks = []

        # Если есть лимит, рассчитываем сколько еще нужно
        if limit > 0:
            needed = limit - current_new_games
            if needed <= 0:
                return []
            batch_size_actual = min(batch_size, needed)
        else:
            batch_size_actual = batch_size

        # Создаем задачи
        for i in range(batches_per_cycle):
            batch_offset = current_offset + (i * batch_size_actual)
            batch_tasks.append((batch_number + i, batch_offset, batch_size_actual))

        return batch_tasks


    def _process_cycle_results(self, batch_results, game_lock, new_games, all_found_games,
                               skip_existing, limit, debug):
        """Обрабатывает результаты цикла загрузки"""
        import threading

        cycle_stats = {
            'new_games_count': 0,
            'total_games': 0,
            'empty_batches': 0,
            'last_offset': 0,
            'from_cache': 0,
            'from_db': 0
        }

        for batch_num, batch_offset, games, games_loaded, is_empty in batch_results:
            with game_lock:
                if not is_empty and games_loaded > 0:
                    # Обрабатываем игры пачкой
                    batch_stats = self._process_games_batch(
                        games, batch_offset, new_games, all_found_games,
                        skip_existing, limit
                    )

                    # Обновляем статистику цикла
                    cycle_stats['new_games_count'] += batch_stats['new_games']
                    cycle_stats['total_games'] += batch_stats['total_games']
                    cycle_stats['from_cache'] += batch_stats['from_cache']
                    cycle_stats['from_db'] += batch_stats['from_db']
                    cycle_stats['last_offset'] = max(cycle_stats['last_offset'], batch_stats['last_offset'])

                    # Вывод прогресса
                    if limit > 0:
                        progress_msg = f'   📊 Прогресс: {len(new_games)}/{limit} новых игр (просмотрено: {len(all_found_games)}, из кэша: {cycle_stats["from_cache"]}, текущий offset: {batch_stats["last_offset"]})'
                        self.stdout.write(progress_msg)
                    elif len(all_found_games) % 500 == 0:
                        progress_msg = f'   📊 Просмотрено: {len(all_found_games)} игр (новых: {len(new_games)}, из кэша: {cycle_stats["from_cache"]})'
                        self.stdout.write(progress_msg)

                else:
                    cycle_stats['empty_batches'] += 1
                    cycle_stats['last_offset'] = max(cycle_stats['last_offset'],
                                                     batch_offset + 100)  # Предполагаем размер пачки

        return cycle_stats


    def _update_stats(self, stats, cycle_stats):
        """Обновляет общую статистику"""
        stats['total_checked'] += cycle_stats['total_games']
        stats['from_cache'] += cycle_stats['from_cache']
        stats['from_db'] += cycle_stats['from_db']
        stats['batches_processed'] += 1
        stats['empty_batches'] += cycle_stats['empty_batches']
        return stats

    def _update_empty_batches_counter(self, current_count, new_empty_batches):
        """Обновляет счетчик пустых пачек"""
        if new_empty_batches > 0:
            return current_count + 1
        return 0  # Сбрасываем если была непустая пачка

    def _print_final_stats(self, debug, total_time, total_checked, from_cache,
                           from_db, new_games_count, last_checked_offset,
                           limit_reached, limit_reached_offset):
        """Выводит финальную статистику"""
        if debug:
            self.stdout.write(f'   📊 ИТОГОВАЯ СТАТИСТИКА:')
            self.stdout.write(f'   📍 Последний проверенный offset: {last_checked_offset}')
            self.stdout.write(f'   👀 Всего просмотрено игр: {total_checked}')
            self.stdout.write(f'   💾 Игр из кэша: {from_cache}')
            self.stdout.write(f'   🗄️  Игр уже в БД: {from_db}')
            self.stdout.write(f'   🆕 Найдено новых игр: {new_games_count}')
            self.stdout.write(f'   ⏱️  Время: {total_time:.1f}с')

            if limit_reached:
                self.stdout.write(f'   🎯 Лимит достигнут на offset: {limit_reached_offset}')

            if total_time > 0:
                speed = total_checked / total_time
                self.stdout.write(f'   🚀 Скорость: {speed:.1f} игр/сек')

    # ОСТАВШИЕСЯ МЕТОДЫ КЛАССА DataCollector

    def collect_all_data_ids(self, all_games_data, debug=False):
        """Собирает все ID для последующей загрузки"""
        all_game_ids = []
        all_cover_ids = []
        all_genre_ids = set()
        all_platform_ids = set()
        all_keyword_ids = set()
        game_data_map = {}

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

        if debug:
            self.stdout.write(f'   ✅ Собрано ID:')
            self.stdout.write(f'      • Игр: {len(all_game_ids)}')
            self.stdout.write(f'      • Обложек: {len(set(all_cover_ids))}')
            self.stdout.write(f'      • Жанров: {len(all_genre_ids)}')
            self.stdout.write(f'      • Платформ: {len(all_platform_ids)}')
            self.stdout.write(f'      • Ключевых слов: {len(all_keyword_ids)}')

        return {
            'game_data_map': game_data_map,
            'all_game_ids': all_game_ids,
            'all_cover_ids': list(set(all_cover_ids)),  # Удаляем дубликаты
            'all_genre_ids': list(all_genre_ids),
            'all_platform_ids': list(all_platform_ids),
            'all_keyword_ids': list(all_keyword_ids),
            'all_screenshot_games': all_game_ids,  # Все игры могут иметь скриншоты
        }

    def collect_all_data_with_stats(self, all_games_data, debug=False):
        """Собирает все данные со статистикой"""
        total_games = len(all_games_data)

        if debug:
            self.stdout.write(f'📊 Всего игр: {total_games}')

        start_total_time = time.time()
        collection_stats = {}

        # 1️⃣ Сбор основных ID из игр
        if debug:
            self.stdout.write('\n1️⃣  🔍 СБОР ОСНОВНЫХ ID ИЗ ИГР...')

        start_collect_time = time.time()
        collected_data = self.collect_all_data_ids(all_games_data, debug)
        collect_time = time.time() - start_collect_time
        collection_stats['collect_time'] = collect_time

        if debug:
            self.stdout.write(f'   ✅ Основные ID собраны за {collect_time:.2f}с')

        # 2️⃣ Сбор информации о скриншотах
        if debug:
            self.stdout.write('\n2️⃣  📸 СБОР ИНФОРМАЦИИ О СКРИНШОТАХ...')

        start_screenshots_info = time.time()
        game_ids_for_screenshots = collected_data['all_game_ids']

        if debug:
            self.stdout.write(f'   🔍 Проверка скриншотов для {len(game_ids_for_screenshots)} игр...')

        screenshots_info_result = self.collect_screenshots_info(game_ids_for_screenshots, debug)
        collected_data['screenshots_info'] = screenshots_info_result.get('screenshots_info', {})
        collected_data['total_possible_screenshots'] = screenshots_info_result.get('total_possible_screenshots', 0)

        screenshots_info_time = time.time() - start_screenshots_info
        collection_stats['screenshots_info_time'] = screenshots_info_time

        if debug:
            discovered = collected_data['total_possible_screenshots']
            games_with_screenshots = len(
                [v for v in screenshots_info_result.get('screenshots_info', {}).values() if v > 0])
            self.stdout.write(
                f'   ✅ Найдено скриншотов: {discovered} для {games_with_screenshots} игр за {screenshots_info_time:.2f}с')

        # 3️⃣ Загрузка дополнительных данных
        if debug:
            self.stdout.write('\n3️⃣  📚 ЗАГРУЗКА ДОПОЛНИТЕЛЬНЫХ ДАННЫХ...')

        start_additional = time.time()
        from .data_loader import DataLoader
        loader = DataLoader(self.stdout, self.stderr)

        # Используем обновленный метод с 3 параметрами
        additional_data_map, additional_stats = loader.load_and_process_additional_data(
            collected_data['all_game_ids'],
            collected_data['game_data_map'],
            collected_data['screenshots_info'],
            debug
        )
        collected_data['additional_data_map'] = additional_data_map

        collected_data['all_series_ids'] = additional_stats.get('all_series_ids', [])
        collected_data['all_company_ids'] = additional_stats.get('all_company_ids', [])
        collected_data['all_theme_ids'] = additional_stats.get('all_theme_ids', [])
        collected_data['all_perspective_ids'] = additional_stats.get('all_perspective_ids', [])
        collected_data['all_mode_ids'] = additional_stats.get('all_mode_ids', [])

        additional_time = time.time() - start_additional
        collection_stats['additional_time'] = additional_time

        if debug:
            self.stdout.write(f'   ✅ Дополнительные данные загружены за {additional_time:.2f}с')

        # 4️⃣ Общая статистика
        if debug:
            self.stdout.write('\n📊 ОБЩАЯ СТАТИСТИКА СОБРАННЫХ ДАННЫХ:')
            self.stdout.write('   ────────────────────────────────')

            self.stdout.write(f'   🎮 Игр: {len(collected_data["all_game_ids"])}')
            self.stdout.write(f'   🖼️  Обложек: {len(collected_data["all_cover_ids"])}')
            self.stdout.write(f'   🎭 Жанров: {len(collected_data["all_genre_ids"])}')
            self.stdout.write(f'   🖥️  Платформ: {len(collected_data["all_platform_ids"])}')
            self.stdout.write(f'   🔑 Ключевых слов: {len(collected_data["all_keyword_ids"])}')
            self.stdout.write(f'   📚 Серий: {len(collected_data.get("all_series_ids", []))}')
            self.stdout.write(f'   🏢 Компаний: {len(collected_data.get("all_company_ids", []))}')
            self.stdout.write(f'   🎨 Тем: {len(collected_data.get("all_theme_ids", []))}')
            self.stdout.write(f'   👁️  Перспектив: {len(collected_data.get("all_perspective_ids", []))}')
            self.stdout.write(f'   🎮 Режимов: {len(collected_data.get("all_mode_ids", []))}')

            discovered = collected_data.get('total_possible_screenshots', 0)
            if discovered > 0:
                games_with = len([v for v in collected_data.get('screenshots_info', {}).values() if v > 0])
                self.stdout.write(f'   📸 Скриншотов: {discovered} (в {games_with} играх)')
            else:
                self.stdout.write(f'   📸 Скриншотов: {discovered}')

            total_collection_time = collect_time + screenshots_info_time + additional_time
            self.stdout.write(f'   ⏱️  Общее время сбора: {total_collection_time:.2f}с')

        collected_data['screenshots_discovered'] = collected_data.get('total_possible_screenshots', 0)

        stats = {
            'collect_time': collect_time,
            'screenshots_info_time': screenshots_info_time,
            'additional_time': additional_time,
            'total_games': total_games,
            'total_collection_time': collect_time + screenshots_info_time + additional_time,
            'collected_counts': {
                'games': len(collected_data.get('all_game_ids', [])),
                'covers': len(collected_data.get('all_cover_ids', [])),
                'genres': len(collected_data.get('all_genre_ids', [])),
                'platforms': len(collected_data.get('all_platform_ids', [])),
                'keywords': len(collected_data.get('all_keyword_ids', [])),
                'series': len(collected_data.get('all_series_ids', [])),
                'companies': len(collected_data.get('all_company_ids', [])),
                'themes': len(collected_data.get('all_theme_ids', [])),
                'perspectives': len(collected_data.get('all_perspective_ids', [])),
                'modes': len(collected_data.get('all_mode_ids', [])),
                'screenshots': collected_data.get('total_possible_screenshots', 0),
                'games_with_screenshots': len(
                    [v for v in collected_data.get('screenshots_info', {}).values() if v > 0]),
            }
        }

        return collected_data, stats

    def collect_screenshots_info(self, game_ids, debug=False):
        """Собирает ПРАВИЛЬНУЮ информацию о скриншотах для списка игр"""
        if not game_ids:
            if debug:
                self.stdout.write('   ⚠️  Нет ID игр для проверки скриншотов')
            return {
                'screenshots_info': {},
                'total_possible_screenshots': 0
            }

        screenshots_info = {}
        total_screenshots = 0

        if debug:
            self.stdout.write(f'   🔍 Сбор информации о скриншотах для {len(game_ids)} игр...')

        # Разбиваем на пачки по 50 игр
        batches = [game_ids[i:i + 50] for i in range(0, len(game_ids), 50)]
        total_batches = len(batches)

        if debug:
            self.stdout.write(f'      Разбито на {total_batches} пачек по 50 игр')

        for batch_num, batch_ids in enumerate(batches, 1):
            try:
                id_list = ','.join(map(str, batch_ids))
                # Запрашиваем ВСЕ скриншоты (без лимита per game, но с общим лимитом 500)
                query = f'fields game; where game = ({id_list}); limit 500;'

                screenshots_data = make_igdb_request('screenshots', query, debug=False)

                if debug:
                    self.stdout.write(f'      Пачка {batch_num}: получено {len(screenshots_data)} записей скриншотов')

                # Считаем скриншоты по играм
                for screenshot_data in screenshots_data:
                    game_id = screenshot_data.get('game')
                    if game_id:
                        # Увеличиваем счетчик скриншотов для этой игры
                        screenshots_info[game_id] = screenshots_info.get(game_id, 0) + 1
                        total_screenshots += 1

                if debug and (batch_num % 10 == 0 or batch_num == total_batches):
                    self.stdout.write(
                        f'      📊 Обработано {batch_num}/{total_batches} пачек, найдено {total_screenshots} скриншотов')

            except Exception as e:
                if debug:
                    self.stderr.write(f'      ❌ Ошибка при сборе информации о скриншотах для пачки {batch_num}: {e}')

        if debug:
            games_with_screenshots = len([v for v in screenshots_info.values() if v > 0])
            games_total = len(game_ids)

            self.stdout.write(f'   ✅ Сбор информации о скриншотах завершен:')
            self.stdout.write(f'      • Всего игр: {games_total}')
            self.stdout.write(f'      • Игр со скриншотами: {games_with_screenshots}')
            self.stdout.write(f'      • Обнаружено скриншотов: {total_screenshots}')

            # Детальная статистика
            if screenshots_info:
                avg_screenshots = total_screenshots / games_with_screenshots if games_with_screenshots > 0 else 0
                self.stdout.write(f'      • Среднее скриншотов на игру: {avg_screenshots:.1f}')

        return {
            'screenshots_info': screenshots_info,
            'total_possible_screenshots': total_screenshots,
            'games_with_screenshots': len([v for v in screenshots_info.values() if v > 0]),
            'games_total': len(game_ids)
        }

    def process_all_data_sequentially(self, all_games_data, debug=False):
        """Обрабатывает все данные последовательно по типам, но с параллельными пачками внутри каждого типа"""
        from .data_loader import DataLoader
        from .relations_handler import RelationsHandler
        from .statistics import Statistics

        loader = DataLoader(self.stdout, self.stderr)
        relations_handler = RelationsHandler(self.stdout, self.stderr)
        stats_handler = Statistics(self.stdout, self.stderr)

        total_games = len(all_games_data)

        if debug:
            self.stdout.write(f'📊 Всего игр: {total_games}')
            self.stdout.write('🚀 Используется оптимизированная загрузка с учетом веса данных')

        start_total_time = time.time()
        all_step_times = {}
        loaded_data_stats = {}  # Статистика загруженных данных

        # 1️⃣ Сбор всех данных
        collected_data, collection_stats = self.collect_all_data_with_stats(all_games_data, debug)
        all_step_times['collect'] = collection_stats['collect_time']
        all_step_times['screenshots_info'] = collection_stats.get('screenshots_info_time', 0)
        all_step_times['additional'] = collection_stats['additional_time']

        # Сохраняем статистику собранных данных
        loaded_data_stats['collected'] = {
            'games': len(collected_data['all_game_ids']),
            'covers': len(collected_data['all_cover_ids']),
            'genres': len(collected_data['all_genre_ids']),
            'platforms': len(collected_data['all_platform_ids']),
            'keywords': len(collected_data['all_keyword_ids']),
            'series': len(collected_data['all_series_ids']),
            'companies': len(collected_data['all_company_ids']),
            'themes': len(collected_data['all_theme_ids']),
            'perspectives': len(collected_data['all_perspective_ids']),
            'modes': len(collected_data['all_mode_ids']),
            'screenshots_discovered': collected_data.get('total_possible_screenshots', 0),
        }

        # 2️⃣ Создание основных данных игр
        if debug:
            self.stdout.write('\n1️⃣  🎮 СОЗДАНИЕ ОСНОВНЫХ ДАННЫХ ИГР...')
        start_step = time.time()
        games_data_list = list(collected_data['game_data_map'].values())
        created_count, game_basic_map = loader.create_basic_games(games_data_list, debug)
        all_step_times['basic_games'] = time.time() - start_step

        if debug:
            self.stdout.write(f'   ✅ Создано игр: {created_count}/{total_games}')
            self.stdout.write(f'   ⏱️  Время: {all_step_times["basic_games"]:.2f}с')

        # Если не создано ни одной игры, выходим
        if created_count == 0:
            if debug:
                self.stdout.write('   ⚠️  Нет новых игр для загрузки')

            total_time = time.time() - start_total_time
            skipped_count = total_games  # Все игры пропущены

            # Собираем статистику даже если игр нет
            stats = stats_handler._collect_final_statistics(
                total_games, 0, skipped_count, 0, total_time,
                loaded_data_stats, all_step_times, debug
            )

            if debug:
                stats_handler._print_complete_statistics(stats)
            else:
                # Выводим минимальную статистику даже без debug
                self.stdout.write('\n' + '=' * 60)
                self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
                self.stdout.write(f'⏱️  Время: {total_time:.2f}с')
                if total_time > 0:
                    speed = total_games / total_time
                    self.stdout.write(f'🚀 СКОРОСТЬ: {speed:.1f} игр/сек')
                self.stdout.write(f'🎮 Найдено: {total_games}')
                self.stdout.write(f'✅ Загружено: 0')
                self.stdout.write(f'⏭️  Пропущено: {skipped_count}')

            return stats

        # 3️⃣ Загрузка всех типов данных последовательно
        data_maps, data_step_times = loader.load_all_data_types_sequentially(collected_data, debug)
        all_step_times.update(data_step_times)

        # Сохраняем статистику загруженных данных
        loaded_data_stats['loaded'] = {
            'covers': len(data_maps.get('cover_map', {})),
            'genres': len(data_maps.get('genre_map', {})),
            'platforms': len(data_maps.get('platform_map', {})),
            'keywords': len(data_maps.get('keyword_map', {})),
            'series': len(data_maps.get('series_map', {})),
            'companies': len(data_maps.get('company_map', {})),
            'themes': len(data_maps.get('theme_map', {})),
            'perspectives': len(data_maps.get('perspective_map', {})),
            'modes': len(data_maps.get('mode_map', {})),
        }

        # 4️⃣ Обновление игр обложками
        if debug:
            self.stdout.write('\n📝 ОБНОВЛЕНИЕ ИГР ОБЛОЖКАМИ...')
        start_step = time.time()
        updated_covers = loader.update_games_with_covers(
            game_basic_map, data_maps['cover_map'], collected_data['game_data_map'], debug
        )
        all_step_times['update_covers'] = time.time() - start_step

        if debug:
            self.stdout.write(f'   ✅ Обновлено обложек: {updated_covers}')

        # 5️⃣ Загрузка скриншотов
        if debug:
            self.stdout.write('\n📸 ПАРАЛЛЕЛЬНАЯ ЗАГРУЗКА СКРИНШОТОВ...')
        start_step = time.time()

        screenshots_info = collected_data.get('screenshots_info', {})
        game_data_map = collected_data.get('game_data_map', {})
        game_ids = list(game_basic_map.keys())

        screenshots_loaded = loader.load_screenshots_parallel(
            game_ids, game_data_map, screenshots_info, debug=debug
        )

        all_step_times['screenshots'] = time.time() - start_step

        if debug:
            self.stdout.write(f'   ✅ Загружено скриншотов: {screenshots_loaded}')
            self.stdout.write(f'   ⏱️  Время: {all_step_times["screenshots"]:.2f}с')

        # 6️⃣ Подготовка связей
        all_game_relations, prepare_time = relations_handler.prepare_game_relations(
            game_basic_map, collected_data['game_data_map'],
            collected_data['additional_data_map'], data_maps, debug
        )
        all_step_times['prepare_relations'] = prepare_time

        # 7️⃣ Создание всех связей
        relations_results, possible_stats, relations_time = relations_handler.create_all_relations(
            all_game_relations, data_maps, debug
        )
        all_step_times['relations'] = relations_time

        total_time = time.time() - start_total_time
        skipped_count = total_games - created_count

        # 8️⃣ Собираем полную финальную статистику
        stats = stats_handler._collect_final_statistics(
            total_games, created_count, skipped_count, screenshots_loaded,
            total_time, loaded_data_stats, all_step_times,
            relations_results, possible_stats, debug
        )

        # 9️⃣ Выводим полную статистику
        if debug:
            stats_handler._print_complete_statistics(stats)
        else:
            # Без debug - только итоговая статистика
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
            self.stdout.write(f'⏱️  Время: {total_time:.2f}с')
            if total_time > 0:
                speed = total_games / total_time
                self.stdout.write(f'🚀 СКОРОСТЬ: {speed:.1f} игр/сек')
            self.stdout.write(f'🎮 Найдено: {total_games}')
            self.stdout.write(f'✅ Загружено: {created_count}')
            self.stdout.write(f'⏭️  Пропущено: {skipped_count}')

        return stats

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0, skip_existing=False,
                               count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        self.stdout.write('🔍 Загрузка популярных игр...')

        if limit > 0:
            self.stdout.write(f'   🔒 Установлен лимит: {limit} игр')
        if offset > 0:
            self.stdout.write(f'   ⏭️  Пропуск первых: {offset} игр')
        if min_rating_count > 0:
            self.stdout.write(f'   ⭐ Минимальное количество оценок: {min_rating_count}')
        if skip_existing:
            self.stdout.write(f'   ⏭️  Режим skip-existing: пропуск игр, которые уже есть в базе')
        if count_only:
            self.stdout.write(f'   🔢 РЕЖИМ COUNT-ONLY: только подсчет количества игр')

        # Парсим game_types
        game_types = []
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')
                game_types = [0, 1, 2, 4, 5, 8, 9, 10, 11]  # Значения по умолчанию

        if game_types:
            self.stdout.write(f'   🎮 Фильтр по типам игр: {game_types}')
        else:
            self.stdout.write(f'   🎮 Загрузка всех типов игр (фильтр отключен)')

        # Базовое условие - исключаем игры без названия и с нулевым rating_count
        where_conditions = ['name != null']

        if min_rating_count > 0:
            where_conditions.append(f'rating_count >= {min_rating_count}')
        else:
            # Если не указан min_rating_count, все равно фильтруем игры с хотя бы одной оценкой
            where_conditions.append('rating_count > 0')

        # Добавляем фильтр по game_type если указаны типы
        if game_types:
            game_types_str_query = ','.join(map(str, game_types))
            where_conditions.append(f'game_type = ({game_types_str_query})')

        where_clause = ' & '.join(where_conditions)

        if debug:
            self.stdout.write('   🎯 Построение запроса...')
            self.stdout.write(f'   📋 Условие: {where_clause}')
            self.stdout.write('   📊 Сортировка: по количеству оценок (rating_count)')

        return self.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

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

        return self.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

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

        return self.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

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

        return self.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_search(self, search_text, debug=False, limit=0, offset=0, skip_existing=True, min_rating_count=0,
                             count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по поисковому запросу"""
        if debug:
            self.stdout.write(f'🔍 Поиск игр по запросу: "{search_text}"')
        else:
            self.stdout.write(f'   🔍 Поиск по тексту: "{search_text}"...')

        # Формируем базовое условие для поиска
        where_conditions = [f'name ~ *"{search_text}"* | summary ~ *"{search_text}"* | storyline ~ *"{search_text}"*']

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

        return self.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)
