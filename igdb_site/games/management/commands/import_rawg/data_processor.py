# games/management/commands/import_rawg/data_processor.py
import concurrent.futures
import time
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.db.models import Case, When, Value, TextField
from games.models import Game
from typing import Dict, Any, List, Optional


class DataProcessor:
    """Класс для обработки данных игр с поддержкой graceful shutdown"""

    def __init__(self, rawg_client, options, signal_handler=None):
        self.rawg_client = rawg_client
        self.options = options
        self.signal_handler = signal_handler
        self.stats = {}
        self.executor = None
        self.futures = {}
        self.last_progress_length = 0

    def init_stats(self, repeat_num):
        """Инициализирует статистику"""
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
            'detail_requests': 0,
            'completed': 0,
            'processing_times': []
        }
        self.last_progress_length = 0

    def process_games_batch(self, games):
        """Обработка батча игр с использованием as_completed для параллельности"""
        if not games:
            return self._empty_results()

        results = {
            'descriptions': {},
            'not_found': [],
            'errors': [],
            'short': [],
            'balance_exceeded': False
        }

        workers = min(self.options.get('workers', 4), len(games))

        # Используем контекстный менеджер для executor
        with ThreadPoolExecutor(max_workers=workers) as executor:
            # Создаем futures для всех игр
            future_to_game = {
                executor.submit(self.get_game_description, game): game
                for game in games
            }

            completed = 0
            total_games = len(games)
            last_progress_update = time.time()
            progress_update_interval = 0.5

            # Обрабатываем завершенные задачи по мере их готовности
            for future in as_completed(future_to_game):
                # Проверяем флаг прерывания
                if self.signal_handler and self.signal_handler.is_shutdown():
                    print("\n⚠️  Прерывание во время обработки...")
                    # Отменяем оставшиеся задачи
                    for f in future_to_game.keys():
                        if not f.done():
                            f.cancel()
                    break

                game = future_to_game[future]

                try:
                    # Получаем результат без дополнительного ожидания
                    start_time = time.time()
                    result = future.result(timeout=5.0)
                    processing_time = time.time() - start_time
                    self.stats['processing_times'].append(processing_time)

                    # Проверяем, не исчерпан ли баланс
                    if result and result.get('status') == 'balance_exceeded':
                        results['balance_exceeded'] = True
                        self.stats['errors'] += 1
                        results['errors'].append(game.id)

                        # Показываем сообщение об ошибке лимита
                        error_msg = result.get('error', 'Неизвестная ошибка лимита')
                        print(f"\n🚫 ЛИМИТ API ИСЧЕРПАН: {error_msg}")
                        print("🛑 Прекращаю обработку немедленно!")

                        # Отменяем все оставшиеся задачи
                        for f in future_to_game.keys():
                            if not f.done():
                                f.cancel()
                        break

                    # Обрабатываем результат
                    self.process_single_game_result(game, result, results)

                except concurrent.futures.TimeoutError:
                    self.handle_processing_error(game, results, f'⏱️ Таймаут ожидания: {game.name}')
                except Exception as e:
                    error_msg = str(e)
                    if "ЛИМИТ API" in error_msg or "лимит исчерпан" in error_msg:
                        results['balance_exceeded'] = True
                        self.stats['errors'] += 1
                        results['errors'].append(game.id)
                        print(f"\n🚫 ЛИМИТ API ИСЧЕРПАН: {error_msg}")
                        print("🛑 Прекращаю обработку немедленно!")

                        # Отменяем все оставшиеся задачи
                        for f in future_to_game.keys():
                            if not f.done():
                                f.cancel()
                        break
                    else:
                        self.handle_processing_error(game, results, f'💥 Ошибка: {error_msg[:50]}')

                completed += 1
                self.stats['completed'] = completed

                # Показываем прогресс с регулируемой частотой
                current_time = time.time()
                if (current_time - last_progress_update >= progress_update_interval or
                        completed == total_games):
                    self.show_progress_single_line(completed, total_games)
                    last_progress_update = current_time

        # После завершения выводим финальную строку
        if completed > 0:
            self.show_progress_single_line(completed, total_games)
            print()  # Переход на новую строку после завершения

        # Если лимит исчерпан, выводим дополнительное сообщение
        if results['balance_exceeded']:
            print("\n" + "🚫" * 20)
            print("🚫 ЛИМИТ API КЛЮЧА ИСЧЕРПАН!")
            print("🚫 Дальнейшая обработка невозможна.")
            print("🚫 Подождите сутки или обновите API ключ.")
            print("🚫" * 20)

        return results

    def show_progress_single_line(self, completed, total):
        """Показывает прогресс обработки в одной изменяющейся строке"""
        if total == 0:
            return

        progress = (completed / total) * 100
        elapsed = time.time() - self.stats['start']
        games_per_sec = completed / elapsed if elapsed > 0 else 0
        cache_hits = self.stats.get('cache_hits', 0)
        cache_hit_rate = (cache_hits / completed * 100) if completed > 0 else 0

        # Рассчитываем оставшееся время
        remaining = total - completed
        eta_seconds = (elapsed / completed * remaining) if completed > 0 else 0
        eta_minutes = eta_seconds / 60

        # Создаем прогресс-бар
        bar_length = 20
        filled = int(bar_length * progress / 100)
        bar = "[" + "=" * filled + " " * (bar_length - filled) + "]"

        # Формируем строку прогресса
        progress_str = (
            f'{bar} {progress:.0f}% | '
            f'{completed}/{total} | '
            f'{games_per_sec:.1f} игр/сек | '
            f'Кэш: {cache_hit_rate:.0f}% | '
            f'ETA: {eta_minutes:.1f} мин'
        )

        # Очищаем предыдущую строку
        if self.last_progress_length > 0:
            sys.stdout.write('\r' + ' ' * self.last_progress_length + '\r')

        # Выводим новую строку
        sys.stdout.write(progress_str)
        sys.stdout.flush()

        # Сохраняем длину строки для следующего обновления
        self.last_progress_length = len(progress_str)

    def _empty_results(self):
        """Возвращает пустые результаты"""
        return {
            'descriptions': {},
            'not_found': [],
            'errors': [],
            'short': [],
            'balance_exceeded': False
        }

    def process_games(self, games):
        """Основной метод обработки игр"""
        # Если игр много, обрабатываем батчами
        if len(games) > 100:
            return self._process_large_batch(games)
        else:
            return self.process_games_batch(games)

    def _process_large_batch(self, games):
        """Обработка большого количества игр батчами"""
        batch_size = 100  # Размер батча для обработки
        total_games = len(games)
        all_results = self._empty_results()

        for i in range(0, total_games, batch_size):
            # Проверяем прерывание
            if self.signal_handler and self.signal_handler.is_shutdown():
                print(f"\n⚠️  Прерывание на батче {i // batch_size + 1}")
                break

            batch = games[i:i + batch_size]
            print(f'🔄 Обработка батча {i // batch_size + 1}/{(total_games + batch_size - 1) // batch_size}...')

            batch_results = self.process_games_batch(batch)

            # Объединяем результаты
            all_results['descriptions'].update(batch_results['descriptions'])
            all_results['not_found'].extend(batch_results['not_found'])
            all_results['errors'].extend(batch_results['errors'])
            all_results['short'].extend(batch_results['short'])

            if batch_results['balance_exceeded']:
                all_results['balance_exceeded'] = True
                break

        return all_results

    def get_game_description(self, game):
        """Получение описания игры из RAWG API"""
        # 1. Проверка прерывания
        if self._check_shutdown():
            raise InterruptedError("Прерывание запрошено")

        try:
            # 2. Получение описания
            return self._fetch_game_description(game)
        except Exception as e:
            # 3. Обработка ошибок
            return self._handle_game_description_error(e)

    def _check_shutdown(self):
        """Проверяет, было ли запрошено прерывание"""
        return self.signal_handler and self.signal_handler.is_shutdown()

    def _fetch_game_description(self, game):
        """Получает описание игры через RAWG API"""
        return self.rawg_client.get_game_description(
            game_name=game.name,
            min_length=self.options.get('min_length', 1),
            delay=self.options.get('delay', 0.5),
            use_cache=not self.options.get('skip_cache', False),
            cache_ttl=self.options.get('cache_ttl', 30),
            timeout=15
        )

    def _handle_game_description_error(self, error):
        """Обрабатывает ошибки при получении описания игры"""
        error_str = str(error)

        # Проверка лимита API
        if self._is_api_limit_error(error_str):
            return {
                'status': 'balance_exceeded',
                'error': error_str,
                'source': 'api_limit'
            }

        # Проверка прерывания
        elif self._check_shutdown():
            raise InterruptedError("Прерывание запрошено")

        # Общая ошибка
        else:
            return {
                'status': 'error',
                'error': error_str[:100],
                'source': 'exception'
            }

    def _is_api_limit_error(self, error_str):
        """Проверяет, является ли ошибка ошибкой лимита API"""
        limit_keywords = ["ЛИМИТ API", "лимит исчерпан", "429", "rate limit", "too many requests"]
        return any(keyword in error_str.lower() for keyword in [kw.lower() for kw in limit_keywords])

    def process_single_game_result(self, game, result, results):
        """Обрабатывает результат обработки одной игры"""
        if not result or not isinstance(result, dict):
            self.stats['errors'] += 1
            results['errors'].append(game.id)
            return

        status = result.get('status')
        found = result.get('found', False)
        description = result.get('description')

        if status == 'found':
            # Нормальное описание
            if description and len(description.strip()) > 0:
                results['descriptions'][game.id] = description
                self.stats['found'] += 1
            else:
                self.stats['empty'] += 1
                results['short'].append(game.id)

        elif status == 'short':
            # Короткое описание - ВСЁ РАВНО СОХРАНЯЕМ
            if description and len(description.strip()) > 0:
                results['descriptions'][game.id] = description
                self.stats['found'] += 1  # Считаем как найденное!
                self.stats['short'] += 1  # Но отмечаем как короткое
                results['short'].append(game.id)
            else:
                self.stats['empty'] += 1
                results['short'].append(game.id)

        elif status == 'empty':
            # Описание пустое - НЕ СОХРАНЯЕМ
            self.stats['empty'] += 1
            results['short'].append(game.id)

            # Добавляем в not_found чтобы пропускать в будущем
            results['not_found'].append(game.igdb_id)
            self.stats['not_found_count'] += 1

        elif status == 'not_found':
            results['not_found'].append(game.igdb_id)
            self.stats['not_found_count'] += 1

        elif status == 'error':
            results['errors'].append(game.id)
            self.stats['errors'] += 1

        elif status == 'balance_exceeded':
            results['balance_exceeded'] = True
            self.stats['errors'] += 1
            results['errors'].append(game.id)

        # Обновляем статистику источников
        source = result.get('source')
        source_stats = {
            'cache': ('cache_hits', 'cache_hit'),
            'search': ('search_requests', 'search_request'),
            'details': ('detail_requests', 'detail_request'),
            'rate_limited': ('rate_limited', 'rate_limited')
        }

        if source in source_stats:
            stat_key, _ = source_stats[source]
            self.stats[stat_key] += 1

        if source != 'cache':
            self.stats['cache_misses'] += 1

    def show_progress(self, completed, total):
        """Показывает прогресс обработки (многострочный вывод)"""
        progress = (completed / total) * 100
        elapsed = time.time() - self.stats['start']
        games_per_sec = completed / elapsed if elapsed > 0 else 0
        cache_hit_rate = (self.stats['cache_hits'] / completed * 100) if completed > 0 else 0

        # Рассчитываем оставшееся время
        remaining = total - completed
        eta_seconds = (elapsed / completed * remaining) if completed > 0 else 0
        eta_minutes = eta_seconds / 60

        bar_length = 20
        filled = int(bar_length * progress / 100)
        bar = "[" + "=" * filled + " " * (bar_length - filled) + "]"

        print(
            f'{bar} {progress:.0f}% | '
            f'{completed}/{total} | '
            f'{games_per_sec:.1f} игр/сек | '
            f'Кэш: {cache_hit_rate:.0f}% | '
            f'ETA: {eta_minutes:.1f} мин'
        )

    def handle_processing_error(self, game, results, message):
        """Обрабатывает ошибку при обработке игры"""
        self.stats['errors'] += 1
        results['errors'].append(game.id)
        if self.options.get('debug'):
            print(f'   {message}: {game.name}')

    def save_descriptions(self, descriptions):
        """Сохраняет описания в базу данных с использованием bulk_update"""
        if not descriptions:
            return 0

        game_ids = list(descriptions.keys())
        batch_size = 500
        total_updated = 0

        for i in range(0, len(game_ids), batch_size):
            batch_ids = game_ids[i:i + batch_size]

            games = Game.objects.filter(id__in=batch_ids).only('id', 'rawg_description')

            games_to_update = []
            for game in games:
                if game.id in descriptions:
                    new_description = descriptions[game.id]
                    old_description = game.rawg_description

                    # ОТЛАДКА: сравним старые и новые описания
                    if self.options.get('debug'):
                        print(f'[DEBUG save_descriptions] Game {game.id}:')
                        print(f'  Old desc length: {len(old_description) if old_description else 0}')
                        print(f'  New desc length: {len(new_description) if new_description else 0}')
                        print(f'  Different: {old_description != new_description}')

                    game.rawg_description = new_description
                    games_to_update.append(game)

            if games_to_update:
                Game.objects.bulk_update(games_to_update, ['rawg_description'])
                total_updated += len(games_to_update)

                # ОТЛАДКА: проверим что сохранилось
                if self.options.get('debug'):
                    for game in games_to_update[:3]:  # Проверим первые 3
                        refreshed = Game.objects.get(id=game.id)
                        print(
                            f'[DEBUG after save] Game {game.id} desc length: {len(refreshed.rawg_description) if refreshed.rawg_description else 0}')

        return total_updated

    def save_descriptions_batch(self, descriptions_batch):
        """Сохраняет батч описаний (оптимизированная версия)"""
        if not descriptions_batch:
            return 0

        # Создаем список объектов для bulk_create или bulk_update
        updates = []
        for game_id, description in descriptions_batch.items():
            updates.append(Game(id=game_id, rawg_description=description))

        # Используем bulk_update
        if updates:
            Game.objects.bulk_update(updates, ['rawg_description'])
            return len(updates)

        return 0
