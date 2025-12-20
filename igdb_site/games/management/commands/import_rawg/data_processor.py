# FILE: data_processor.py
import concurrent.futures
import time
import logging
import json
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from games.models import Game


class DataProcessor:
    """Класс для обработки данных игр"""

    def __init__(self, rawg_client, options, signal_handler=None, command=None):
        self.rawg_client = rawg_client
        self.options = options
        self.signal_handler = signal_handler
        self.command = command
        self.stats = {}
        self.logger = None
        self.error_logger = None  # Отдельный логгер для ошибок
        self.error_json_file = None  # Файл для JSON ошибок
        self.errors_data = None  # Данные ошибок в JSON формате
        self.setup_loggers()

    def log_info(self, message):
        """Записывает информационное сообщение в лог (только основной лог)"""
        if self.logger:
            self.logger.info(message)

    def log_debug(self, message):
        """Записывает отладочное сообщение в лог"""
        if self.logger and self.options.get('debug'):
            self.logger.debug(message)

    def log_error(self, message, game=None, error_type=None, result=None):
        """Записывает сообщение об ошибке в лог и в отдельный файл (ТОЛЬКО РЕАЛЬНЫЕ ОШИБКИ)"""
        # В обычный лог
        if self.logger:
            self.logger.error(message)

        # В лог ошибок
        if self.error_logger:
            self.error_logger.error(message)

        # Детальная информация в JSON (только для реальных ошибок)
        if game and error_type:
            self.log_error_detail(game, error_type, message, result)

    def setup_loggers(self):
        """Настройка логгеров для записи в файлы"""
        log_dir = Path(self.options.get('log_dir', 'logs'))
        log_dir.mkdir(exist_ok=True)

        # Текущая временная метка для имен файлов
        timestamp = time.strftime("%Y%m%d_%H%M%S")

        # Основной логгер
        log_file = log_dir / f'import_rawg_{timestamp}.log'

        self.logger = logging.getLogger('import_rawg')
        self.logger.setLevel(logging.DEBUG)

        if self.logger.handlers:
            self.logger.handlers.clear()

        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)

        # Логгер для текстовых ошибок (отдельный файл)
        error_log_file = log_dir / f'errors_{timestamp}.log'

        self.error_logger = logging.getLogger('import_rawg_errors')
        self.error_logger.setLevel(logging.ERROR)

        if self.error_logger.handlers:
            self.error_logger.handlers.clear()

        error_file_handler = logging.FileHandler(error_log_file, encoding='utf-8')
        error_file_handler.setLevel(logging.ERROR)

        error_formatter = logging.Formatter(
            '%(asctime)s - ERROR - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        error_file_handler.setFormatter(error_formatter)
        self.error_logger.addHandler(error_file_handler)

        # Файл для структурированных данных об ошибках в JSON
        self.error_json_file = log_dir / f'error_details_{timestamp}.json'
        self.errors_data = {
            'session_info': {
                'start_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'options': {
                    'dry_run': self.options.get('dry_run', False),
                    'workers': self.options.get('workers', 4),
                    'delay': self.options.get('delay', 0.1),
                    'min_length': self.options.get('min_length', 1),
                    'skip_cache': self.options.get('skip_cache', False)
                }
            },
            'statistics': {
                'total_errors': 0,
                'errors_by_type': {},
                'errors_by_game_type': {},
                'games_with_errors': set()
            },
            'error_details': [],
            'summary': {}
        }

        # Выводим информацию о созданных файлах
        if self.options.get('debug'):
            print(f"[DEBUG] Созданы файлы логов:")
            print(f"  Основной лог: {log_file}")
            print(f"  Лог ошибок: {error_log_file}")
            print(f"  JSON ошибок: {self.error_json_file}")

    def log_error_detail(self, game, error_type, error_message, result=None):
        """Записывает детальную информацию об ошибке в JSON файл (ТОЛЬКО РЕАЛЬНЫЕ ОШИБКИ)"""
        # ФИЛЬТРУЕМ: какие типы ошибок действительно нужно сохранять
        real_errors = [
            'api_limit_exceeded', 'api_error', 'timeout_error',
            'processing_error', 'exception', 'api_status_none',
            'unknown_status', 'empty_result', 'batch_processing_error'
        ]

        # Игнорируем "не ошибки"
        ignored_types = ['game_not_found', 'empty_description', 'short_description']

        if error_type in ignored_types:
            # Не сохраняем в JSON - это не реальные ошибки
            return

        if error_type not in real_errors and not self.options.get('log_all_details', False):
            # По умолчанию сохраняем только реальные ошибки
            return

        try:
            # Подготавливаем данные об игре
            game_info = {
                'game_id': game.id if hasattr(game, 'id') else None,
                'igdb_id': game.igdb_id if hasattr(game, 'igdb_id') else None,
                'game_name': game.name if hasattr(game, 'name') else str(game),
                'game_type': game.game_type if hasattr(game, 'game_type') else None
            }

            error_detail = {
                'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                'game_info': game_info,
                'error_type': error_type,
                'error_message': error_message[:500] if error_message else 'Без сообщения',
                'rawg_result': self._sanitize_rawg_result(result) if result else None,
                'is_real_error': True  # Флаг, что это реальная ошибка
            }

            # ОСОБАЯ ОБРАБОТКА ДЛЯ STATUS = None
            if error_type == 'api_status_none' and result:
                error_detail['debug_info'] = {
                    'result_type': type(result).__name__,
                    'result_keys': list(result.keys()) if isinstance(result, dict) else 'not_a_dict'
                }

            # Добавляем в данные
            self.errors_data['error_details'].append(error_detail)
            self.errors_data['statistics']['total_errors'] += 1

            # Обновляем статистику по типам ошибок
            if error_type in self.errors_data['statistics']['errors_by_type']:
                self.errors_data['statistics']['errors_by_type'][error_type] += 1
            else:
                self.errors_data['statistics']['errors_by_type'][error_type] = 1

            # Периодически сохраняем в файл
            if len(self.errors_data['error_details']) % 5 == 0:
                self.save_errors_to_json()

        except Exception as e:
            if self.options.get('debug'):
                print(f"⚠️ Ошибка при логировании деталей ошибки: {e}")

    def _sanitize_rawg_result(self, result):
        """Очищает результат RAWG от лишних данных для сохранения в JSON"""
        if not result or not isinstance(result, dict):
            return None

        sanitized = {}
        safe_keys = ['status', 'source', 'found', 'error', 'rawg_id']

        for key in safe_keys:
            if key in result:
                sanitized[key] = result[key]

        # Добавляем информацию о описании (если есть)
        if 'description' in result and result['description']:
            sanitized['description_length'] = len(str(result['description']))

        # Добавляем информацию о RAWG данных (если есть)
        if 'rawg_data' in result and result['rawg_data']:
            sanitized['rawg_data'] = {
                'name': result['rawg_data'].get('name'),
                'released': result['rawg_data'].get('released'),
                'has_rating': 'rating' in result['rawg_data']
            }

        return sanitized

    def save_errors_to_json(self):
        """Сохраняет ошибки в JSON файл"""
        try:
            # Конвертируем set в list для сериализации JSON
            if 'games_with_errors' in self.errors_data['statistics']:
                self.errors_data['statistics']['games_with_errors'] = list(
                    self.errors_data['statistics']['games_with_errors']
                )

            with open(self.error_json_file, 'w', encoding='utf-8') as f:
                json.dump(self.errors_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            error_msg = f'Ошибка сохранения JSON ошибок: {e}'
            if self.logger:
                self.logger.error(error_msg)
            if self.options.get('debug'):
                print(f"⚠️ {error_msg}")

    def finalize_error_logs(self):
        """Финализирует логи ошибок и сохраняет сводку"""
        try:
            # Добавляем сводку
            total_processed = self.stats.get('total_processed', 0)
            total_errors = self.errors_data['statistics']['total_errors']

            self.errors_data['summary'] = {
                'end_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'processing_time_seconds': time.time() - self.stats.get('start_time', time.time()),
                'total_processed': total_processed,
                'total_errors': total_errors,
                'error_rate': (total_errors / total_processed * 100) if total_processed > 0 else 0,
                'most_common_error': max(
                    self.errors_data['statistics']['errors_by_type'].items(),
                    key=lambda x: x[1]
                )[0] if self.errors_data['statistics']['errors_by_type'] else 'Нет ошибок',
                'error_distribution': self.errors_data['statistics']['errors_by_type'],
                'games_with_errors_count': len(self.errors_data['statistics'].get('games_with_errors', [])),
                'suggestions': self._generate_error_suggestions()
            }

            # Сохраняем финальную версию
            self.save_errors_to_json()

            # УБИРАЕМ вывод сообщения о файле ошибок здесь
            # Это будет показано только в финальной сводке

        except Exception as e:
            error_msg = f'Ошибка финализации логов ошибок: {e}'
            if self.logger:
                self.logger.error(error_msg)
            if self.options.get('debug'):
                print(f"⚠️ {error_msg}")

    def _generate_error_suggestions(self):
        """Генерирует рекомендации на основе анализа ошибок"""
        suggestions = []
        error_types = self.errors_data['statistics']['errors_by_type']

        if error_types.get('api_limit_exceeded', 0) > 0:
            suggestions.append({
                'type': 'api_limit',
                'message': 'Превышен лимит запросов к API',
                'recommendation': 'Увеличьте задержку между запросами или обновите API ключ',
                'priority': 'high'
            })

        if error_types.get('timeout_error', 0) > 0:
            suggestions.append({
                'type': 'timeout',
                'message': f'Таймауты при запросах: {error_types.get("timeout_error", 0)}',
                'recommendation': 'Увеличьте timeout или проверьте стабильность соединения',
                'priority': 'medium'
            })

        if error_types.get('game_not_found', 0) > 10:
            suggestions.append({
                'type': 'not_found',
                'message': f'Много игр не найдено: {error_types.get("game_not_found", 0)}',
                'recommendation': 'Проверьте качество названий игр или рассмотрите альтернативные источники',
                'priority': 'low'
            })

        if error_types.get('empty_description', 0) > 10:
            suggestions.append({
                'type': 'empty_description',
                'message': f'Много пустых описаний: {error_types.get("empty_description", 0)}',
                'recommendation': 'Рассмотрите ручную обработку этих игр',
                'priority': 'low'
            })

        return suggestions

    def init_stats(self, repeat_num):
        """Инициализирует статистику для нового батча"""
        # Статистика только для текущего батча
        self.stats = {
            'start_time': time.time(),
            'total_processed': 0,  # Всего игр обработано в этом батче
            'found': 0,  # Найдено с описанием
            'not_found_count': 0,  # Не найдено (нет игры или пустое описание)
            'errors': 0,  # Ошибки обработки
            'cache_hits': 0,  # Попаданий в кэш
            'cache_misses': 0,  # Промахов кэша
            'search_requests': 0,
            'detail_requests': 0,
            'repeat_num': repeat_num,
            'balance_exceeded': False,
            'short_descriptions': 0  # Короткие описания
        }

    def get_game_description(self, game):
        """Получает описание игры из RAWG API с проверкой лимита"""
        try:
            if self.signal_handler and self.signal_handler.is_shutdown():
                raise InterruptedError("Прерывание запрошено")

            # Проверяем баланс API перед запросом
            balance = self.rawg_client.check_balance()
            if balance['exceeded']:
                error_msg = f'🚫 ЛИМИТ API ИСЧЕРПАН: использовано {balance["used"]}/{balance["limit"]} запросов. Остановка.'
                self.log_error(error_msg, game, 'api_limit_exceeded')
                return {
                    'status': 'balance_exceeded',
                    'error': error_msg,
                    'source': 'api_limit',
                    'should_stop': True
                }
            elif balance['is_low']:
                if self.options.get('debug'):
                    self.log_info(f'⚠️  Мало запросов: осталось {balance["remaining"]}')

            result = self.rawg_client.get_game_description(
                game_name=game.name,
                min_length=self.options.get('min_length', 1),
                delay=self.options.get('delay', 0.5),
                use_cache=not self.options.get('skip_cache', False),
                cache_ttl=self.options.get('cache_ttl', 30),
                timeout=15
            )

            # ГАРАНТИРУЕМ, ЧТО РЕЗУЛЬТАТ ИМЕЕТ СТАТУС
            if not result or not isinstance(result, dict):
                return {
                    'status': 'error',
                    'error': 'Пустой ответ от RAWG API',
                    'source': 'api_error'
                }

            # ЕСЛИ СТАТУС НЕТ - СОЗДАЕМ ЕГО
            if 'status' not in result:
                error_msg = result.get('error', 'Неизвестная ошибка API')
                result['status'] = 'error'
                result['error'] = error_msg
                if 'source' not in result:
                    result['source'] = 'api_error'

            # ДОБАВЛЯЕМ ПРОВЕРКУ ДЛЯ ОСТАНОВКИ
            if result.get('status') == 'balance_exceeded':
                result['should_stop'] = True

            return result

        except Exception as e:
            error_str = str(e)
            if "ЛИМИТ API" in error_str or "лимит исчерпан" in error_str or "ЛИМИТ API ИСЧЕРПАН" in error_str:
                error_msg = f'🚫 Лимит API при обработке {game.name}: {error_str}'
                self.log_error(error_msg, game, 'api_limit_exceeded')
                return {
                    'status': 'balance_exceeded',
                    'error': error_str,
                    'source': 'api_limit',
                    'should_stop': True
                }
            elif self.signal_handler and self.signal_handler.is_shutdown():
                raise InterruptedError("Прерывание запрошено")
            else:
                error_msg = f'Исключение при обработке {game.name}: {error_str[:100]}'
                self.log_error(error_msg, game, 'exception')
                return {
                    'status': 'error',
                    'error': error_str[:100],
                    'source': 'exception'
                }

    def process_games_batch(self, games):
        """Обработка батча игр с правильным учетом статистики и остановкой при лимите"""
        if not games:
            return {
                'results': self._empty_results(),
                'stats': self._empty_stats()
            }

        # Инициализируем результаты и статистику
        results = self._empty_results()
        batch_stats = self._empty_stats()
        batch_stats['total_processed'] = len(games)  # ← ЗДЕСЬ ПРАВИЛЬНО

        workers = min(self.options.get('workers', 4), len(games))

        if self.options.get('debug'):
            self.log_debug(f'Начинаем обработку батча: {len(games)} игр, потоки: {workers}')
            print(f"\n[DEBUG] Начинаем обработку батча из {len(games)} игр")

        # Создаем пул потоков
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="rawg_worker") as executor:
            future_to_game = {
                executor.submit(self.get_game_description, game): game
                for game in games
            }

            completed = 0
            total_games = len(games)
            should_stop = False

            try:
                for future in as_completed(future_to_game):
                    # Проверка прерывания
                    if self.signal_handler and self.signal_handler.is_shutdown():
                        if self.options.get('debug'):
                            self.log_debug("Прерывание во время обработки")
                        for f in future_to_game.keys():
                            if not f.done():
                                f.cancel()
                        break

                    game = future_to_game[future]

                    try:
                        result = future.result(timeout=30.0)

                        # УВЕЛИЧИВАЕМ счетчик обработанных игр
                        completed += 1
                        batch_stats['completed'] = completed  # ← ЭТО ПРОСТО ДЛЯ ОТСЛЕЖИВАНИЯ

                        # ПРОВЕРЯЕМ, НУЖНО ЛИ ОСТАНОВИТЬСЯ ИЗ-ЗА ЛИМИТА API
                        if result and result.get('should_stop'):
                            should_stop = True
                            results['balance_exceeded'] = True
                            batch_stats['errors'] += 1
                            results['errors'].append(game.id)

                            error_msg = result.get('error', 'Лимит API исчерпан')
                            self.log_error(f'🚫 {error_msg}', game, 'api_limit_exceeded', result)

                            if self.options.get('debug'):
                                self.log_debug(f'🚫 Лимит API исчерпан: {error_msg}')

                            # ОСТАНАВЛИВАЕМ ВСЕ ЗАДАЧИ НЕМЕДЛЕННО
                            for f in future_to_game.keys():
                                if not f.done():
                                    f.cancel()
                            break

                        # Обрабатываем результат одной игры
                        self._process_single_game_result(game, result, results,
                                                         batch_stats)  # ← ЗДЕСЬ ДОБАВЛЯЕТСЯ В СТАТИСТИКУ

                    except concurrent.futures.TimeoutError:
                        error_msg = f'Таймаут обработки: {game.name}'
                        batch_stats['errors'] += 1
                        results['errors'].append(game.id)
                        completed += 1

                        self.log_error(error_msg, game, 'timeout_error')
                        if self.options.get('debug'):
                            self.log_debug(error_msg)

                    except Exception as e:
                        error_msg = str(e)
                        if "ЛИМИТ API" in error_msg or "лимит исчерпан" in error_msg or "ЛИМИТ API ИСЧЕРПАН" in error_msg:
                            should_stop = True
                            results['balance_exceeded'] = True
                            batch_stats['errors'] += 1
                            results['errors'].append(game.id)
                            completed += 1

                            self.log_error(f'🚫 Лимит API исчерпан: {error_msg}', game, 'api_limit_exceeded')

                            if self.options.get('debug'):
                                self.log_debug(f'🚫 Лимит API исчерпан: {error_msg}')

                            # ОСТАНАВЛИВАЕМ ВСЕ ЗАДАЧИ НЕМЕДЛЕННО
                            for f in future_to_game.keys():
                                if not f.done():
                                    f.cancel()
                            break
                        else:
                            error_msg = f'Ошибка обработки {game.name}: {error_msg}'
                            batch_stats['errors'] += 1
                            results['errors'].append(game.id)
                            completed += 1

                            self.log_error(error_msg, game, 'processing_error')
                            if self.options.get('debug'):
                                self.log_debug(error_msg)

                    # Обновляем прогресс в реальном времени
                    if self.command:
                        self.command.progress_data['current_batch'] = completed

                        # ОБНОВЛЯЕМ БАЛАНС API КАЖДЫЕ 5 ИГР
                        if completed % 5 == 0:
                            self.command.update_api_balance_in_progress()
                        else:
                            self.command.update_single_progress_line()

                        # ЕСЛИ ДОСТИГНУТ ЛИМИТ - ОБНОВЛЯЕМ СТАТУС
                        if should_stop:
                            self.command.progress_data['current_status'] = "🚫 Лимит API исчерпан"
                            self.command.update_single_progress_line()

            except KeyboardInterrupt:
                # Пропускаем KeyboardInterrupt
                pass
            except Exception as e:
                error_msg = f'Неожиданная ошибка в process_games_batch: {e}'
                self.log_error(error_msg, None, 'batch_processing_error')
                if self.options.get('debug'):
                    self.log_debug(error_msg)

        # В КОНЦЕ СРАЗУ ВЫВОДИМ ДЕБАГ
        if self.options.get('debug'):
            print(f"\n[DEBUG] ФИНАЛЬНАЯ СТАТИСТИКА БАТЧА:")
            print(f"  Исходно в батче: {len(games)} игр")
            print(f"  Обработано: {completed} игр")
            print(f"  batch_stats['total_processed']: {batch_stats['total_processed']}")
            print(f"  batch_stats['found']: {batch_stats['found']}")
            print(f"  batch_stats['not_found_count']: {batch_stats['not_found_count']}")
            print(f"  batch_stats['errors']: {batch_stats['errors']}")
            print(f"  Сумма категорий: {batch_stats['found'] + batch_stats['not_found_count'] + batch_stats['errors']}")

        # Финализируем логи ошибок для этого батча
        self.finalize_error_logs()

        # Добавляем флаг в статистику
        batch_stats['should_stop'] = should_stop

        return {
            'results': results,
            'stats': batch_stats
        }

    def _process_single_game_result(self, game, result, results, stats):
        """Обрабатывает результат обработки одной игры"""
        if not result or not isinstance(result, dict):
            stats['errors'] += 1
            results['errors'].append(game.id)
            self.log_error(f'Пустой результат для {game.name}', game, 'empty_result')
            if self.options.get('debug'):
                print(f"  💥 Пустой результат для {game.name}")
            return

        status = result.get('status')

        # ЕСЛИ СТАТУС NONE - ОБРАБАТЫВАЕМ КАК ОШИБКУ
        if status is None:
            stats['errors'] += 1
            results['errors'].append(game.id)

            error_msg = result.get('error', 'Неизвестная ошибка API')
            self.log_error(f'Неизвестный статус (None) для {game.name}: {error_msg}',
                           game, 'api_status_none', result)

            if self.options.get('debug'):
                print(f"  ❓ Неизвестный статус (None) для {game.name}")
                print(f"     Результат: {result}")

            return

        description = result.get('description')

        if self.options.get('debug'):
            print(f"[DEBUG] Игра: {game.name}, статус: {status}")

        # ВАЖНО: УБЕДИТЕСЬ, ЧТО ИГРА УЧИТЫВАЕТСЯ ТОЛЬКО ОДИН РАЗ
        # Текущая игра уже учтена в completed, но нам нужно классифицировать ее

        if status in ['found', 'short']:
            # Найдено описание (даже если короткое)
            if description and len(description.strip()) > 0:
                results['descriptions'][game.id] = description
                stats['found'] += 1

                if status == 'short':
                    stats['short_descriptions'] += 1

                if self.options.get('debug'):
                    print(f"  ✅ Найдено описание ({len(description)} символов)")
            else:
                # Статус 'found' или 'short', но описание пустое -> считаем как не найдено
                stats['not_found_count'] += 1
                if hasattr(game, 'igdb_id'):
                    results['not_found'].append(game.igdb_id)
                if self.options.get('debug'):
                    print(f"  ⚠️  Статус '{status}' но описание пустое -> не найдено")

        elif status in ['empty', 'not_found']:
            # Не найдено описание
            stats['not_found_count'] += 1
            if hasattr(game, 'igdb_id'):
                results['not_found'].append(game.igdb_id)
            if self.options.get('debug'):
                if status == 'empty':
                    print(f"  🚫 Пустое описание")
                else:
                    print(f"  ❓ Игра не найдена в RAWG")

        elif status in ['error', 'balance_exceeded']:
            # Техническая ошибка
            results['errors'].append(game.id)
            stats['errors'] += 1

            if status == 'balance_exceeded':
                results['balance_exceeded'] = True
            if self.options.get('debug'):
                print(f"  💥 {'Лимит API исчерпан' if status == 'balance_exceeded' else 'Ошибка обработки'}")

        else:
            # Неизвестный статус - это ошибка
            if self.options.get('debug'):
                print(f"  ❓ Неизвестный статус: {status}")
            stats['errors'] += 1
            results['errors'].append(game.id)

        # Статистика источников
        source = result.get('source')
        if source == 'cache':
            stats['cache_hits'] += 1
        elif source == 'search':
            stats['search_requests'] += 1
            stats['cache_misses'] += 1
        elif source == 'details':
            stats['detail_requests'] += 1
            stats['cache_misses'] += 1
        elif source == 'rate_limited':
            stats['rate_limited'] += 1

        # В КОНЦЕ ДЕБАГ СТАТИСТИКИ
        if self.options.get('debug'):
            print(
                f"  [Промежуточная статистика] found: {stats['found']}, not_found: {stats['not_found_count']}, errors: {stats['errors']}")

    def save_descriptions(self, descriptions):
        """Сохраняет описания в базу данных"""
        if not descriptions:
            return 0

        game_ids = list(descriptions.keys())
        batch_size = 500
        total_updated = 0

        self.log_info(f'Начинаем сохранение {len(descriptions)} описаний')

        for i in range(0, len(game_ids), batch_size):
            batch_ids = game_ids[i:i + batch_size]

            games = Game.objects.filter(id__in=batch_ids).only('id', 'rawg_description')

            games_to_update = []
            for game in games:
                if game.id in descriptions:
                    new_description = descriptions[game.id]
                    old_description = game.rawg_description

                    if old_description != new_description:
                        game.rawg_description = new_description
                        games_to_update.append(game)

            if games_to_update:
                Game.objects.bulk_update(games_to_update, ['rawg_description'])
                total_updated += len(games_to_update)

        self.log_info(f'Сохранено {total_updated} описаний')
        return total_updated

    def _empty_results(self):
        """Возвращает пустые результаты"""
        return {
            'descriptions': {},
            'not_found': [],
            'errors': [],
            'balance_exceeded': False
        }

    def _empty_stats(self):
        """Возвращает пустую статистику"""
        return {
            'total_processed': 0,
            'found': 0,
            'not_found_count': 0,
            'errors': 0,
            'short_descriptions': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'search_requests': 0,
            'detail_requests': 0,
            'rate_limited': 0,
            'completed': 0
        }

    def process_games(self, games):
        """Основной метод обработки игр"""
        if len(games) > 100:
            return self._process_large_batch(games)
        else:
            return self.process_games_batch(games)

    def _process_large_batch(self, games):
        """Обработка большого количества игр батчами"""
        batch_size = 100
        total_games = len(games)
        all_results = self._empty_results()
        total_stats = self._empty_stats()

        for i in range(0, total_games, batch_size):
            if self.signal_handler and self.signal_handler.is_shutdown():
                if self.command:
                    self.command.stdout.write(f"\n⚠️  Прерывание на батче {i // batch_size + 1}")
                break

            batch = games[i:i + batch_size]
            if self.command:
                self.command.stdout.write(
                    f'\r🔄 Обработка батча {i // batch_size + 1}/{(total_games + batch_size - 1) // batch_size}...')

            batch_result = self.process_games_batch(batch)

            all_results['descriptions'].update(batch_result['descriptions'])
            all_results['not_found'].extend(batch_result['not_found'])
            all_results['errors'].extend(batch_result['errors'])

            if batch_result['balance_exceeded']:
                all_results['balance_exceeded'] = True
                break

            # Суммируем статистику
            for key in ['total_processed', 'found', 'not_found_count', 'errors',
                        'short_descriptions', 'cache_hits', 'cache_misses']:
                if key in batch_result['stats']:
                    total_stats[key] += batch_result['stats'][key]

        # Финализируем логи ошибок для всего большого батча
        self.finalize_error_logs()

        return {
            'results': all_results,
            'stats': total_stats
        }
