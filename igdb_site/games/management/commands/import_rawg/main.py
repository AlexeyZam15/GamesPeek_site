# games/management/commands/import_rawg/main.py
import sys
import time
import json
import os
import signal
from datetime import datetime
from django.core.management.base import BaseCommand
from games.rawg_api import RAWGClient
from games.models import Game
from .base_command import ImportRawgBaseCommand
from .cache_manager import CacheManager
from .import_processor import ImportProcessor
from .repeat_processor import RepeatProcessor
from .signal_handler import SignalHandler
from .stats_manager import StatsManager
from .game_fetcher import GameFetcher


class Command(ImportRawgBaseCommand):
    help = 'Импорт описаний из RAWG API с кэшированием и оптимизацией'

    def __init__(self):
        super().__init__()
        self.signal_handler = SignalHandler(self)
        self.cache_manager = None
        self.stats_manager = None
        self.original_options = None
        self.import_processor = None
        self.total_games_processed = 0
        self.start_time = None

    def handle(self, *args, **options):
        """Основной обработчик команды с graceful shutdown"""
        try:
            return self._handle_with_interrupt(*args, **options)
        except SystemExit:
            raise
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Необработанная ошибка: {e}'))
            import traceback
            traceback.print_exc()
            sys.exit(1)

    def _handle_with_interrupt(self, *args, **options):
        """Основной обработчик команды (внутренний метод)"""
        self.start_time = time.time()

        # Сброс кэша и ненайденных игр если нужно
        if options.get('reset'):
            self.stdout.write('=' * 60)
            self.stdout.write('🧹 ПОЛНЫЙ СБРОС: удаление кэша и списка ненайденных игр')

            # 1. Удаление кэша RAWG
            self.stdout.write('   🔹 Удаление кэша RAWG API...')
            cache_manager = CacheManager(debug=options.get('debug', False))
            deleted_count = cache_manager.reset_cache()
            self.stdout.write(f'   ✅ Удалено {deleted_count} файлов кэша')

            # 2. Удаление файла с ненайденными играми
            auto_offset_file = options.get('auto_offset_file', 'auto_offset_log.json')
            if os.path.exists(auto_offset_file):
                os.remove(auto_offset_file)
                self.stdout.write(f'   🗑️  Удален файл: {auto_offset_file}')
            else:
                self.stdout.write(f'   ℹ️  Файл не найден: {auto_offset_file}')

            # 3. Сброс статистики БД если она есть
            if cache_manager.stats_db:
                try:
                    cursor = cache_manager.stats_db.cursor()
                    cursor.execute('DELETE FROM interruption_logs')
                    cursor.execute('DELETE FROM efficiency_stats')
                    cache_manager.stats_db.commit()
                    self.stdout.write('   📊 Очищена статистика прерываний')
                except:
                    pass

            self.stdout.write('=' * 60)
            self.stdout.write(self.style.SUCCESS('✅ ПОЛНЫЙ СБРОС ВЫПОЛНЕН'))

            # Если только reset без других параметров - выходим
            if len([k for k, v in options.items()
                    if v and k not in ['reset', 'verbosity', 'debug']]) == 0:
                return

        # Инициализация клиента RAWG
        if not self.init_rawg_client(options):
            return

        # Инициализация менеджеров
        self.cache_manager = CacheManager(debug=options.get('debug', False))
        self.stats_manager = StatsManager(self.stdout, self.style)
        self.original_options = options.copy()

        # Запуск основного процесса
        return self.run_main_import_process(options)

    def run_main_import_process(self, options):
        """Основной процесс импорта с оптимизацией"""
        # Загружаем ненайденные игры ИЗ КЭША RAWG
        auto_offset = options.get('auto_offset', True)

        if auto_offset:
            # Основной источник - кэш RAWG
            self.load_not_found_from_rawg_cache_batch()

            # Дополнительно: загружаем из файла (если есть) и объединяем
            # НО ТОЛЬКО ЕСЛИ НЕ БЫЛ СБРОС
            if not options.get('reset'):
                auto_offset_file = options.get('auto_offset_file', 'auto_offset_log.json')
                file_ids = self.load_not_found_ids_from_file(auto_offset_file)

                if file_ids:
                    self.not_found_ids.update(file_ids)
                    self.stdout.write(f'📄 Дополнительно из файла: {len(file_ids)} игр')
            else:
                self.stdout.write('ℹ️  Файл ненайденных игр был удален при сбросе')

        # Показываем информацию о кэше
        self.show_cache_info(options)

        # Создаем процессор
        self.import_processor = ImportProcessor(
            self.rawg_client,
            options,
            self.cache_manager,
            self.signal_handler
        )
        self.import_processor.not_found_ids = self.not_found_ids

        # Проверяем прерывание перед началом
        if self.signal_handler.is_shutdown():
            self.stdout.write("⚠️  Прерывание запрошено до начала обработки")
            return

        # Определяем режим работы
        repeat_times = options['repeat']
        limit = options['limit']

        # Определяем оптимальный размер батча
        batch_size = self.calculate_optimal_batch_size(options)
        options['batch_size'] = batch_size

        self.stdout.write(self.style.SUCCESS(
            f'⚡ ОПТИМИЗИРОВАННЫЙ ИМПОРТ: batch_size={batch_size}, workers={options.get("workers", 4)}'
        ))

        try:
            if limit == 0 and repeat_times == 0:
                # Бесконечный режим с батчами
                self.execute_infinite_batch_mode(auto_offset, batch_size, options)
            elif repeat_times > 0:
                # Ограниченное количество повторов с батчами
                self.execute_limited_repeats_batch(repeat_times, auto_offset, batch_size, options)
            else:
                # Одиночный запуск с батчами
                self.execute_single_batch_mode(auto_offset, batch_size, options)

        except KeyboardInterrupt:
            # Обрабатываем KeyboardInterrupt в основном потоке
            self.stdout.write("\n\n⚠️  Получен KeyboardInterrupt в основном потоке")
            self.signal_handler.signal_handler(signal.SIGINT, None)
        finally:
            # Всегда показываем финальную статистику
            self.show_final_statistics_comprehensive()

            # Сохраняем ненайденные игры в файл (если есть)
            if auto_offset and self.not_found_ids and not options.get('dry_run'):
                auto_offset_file = options.get('auto_offset_file', 'auto_offset_log.json')
                self.save_not_found_ids_to_file(auto_offset_file)

    def calculate_optimal_batch_size(self, options):
        """Рассчитывает оптимальный размер батча на основе настроек"""
        workers = options.get('workers', 4)
        delay = options.get('delay', 0.1)  # Уменьшили delay по умолчанию до 0.1 секунды

        # Формула для расчета оптимального батча:
        # Чем больше воркеров и меньше задержка, тем больше может быть батч
        base_batch = workers * 25  # Базовый размер

        # Корректируем на основе задержки
        if delay < 0.05:
            # Быстрые запросы - можно большие батчи
            batch_size = base_batch * 2
        elif delay < 0.1:
            batch_size = base_batch
        else:
            # Медленные запросы - уменьшаем батч
            batch_size = max(10, base_batch // 2)

        # Ограничиваем максимальный размер
        max_batch = options.get('batch_size', 100)
        return min(batch_size, max_batch)

    def execute_single_batch_mode(self, auto_offset, batch_size, options):
        """Запускает импорт в батчевом режиме (один проход)"""
        offset = options.get('offset', 0)
        limit = options.get('limit', 0)
        processed_total = 0

        game_fetcher = GameFetcher(options, self.not_found_ids)

        while True:
            # Проверяем прерывание
            if self.signal_handler.is_shutdown():
                self.stdout.write("⚠️  Прерывание запрошено")
                break

            # Получаем батч игр
            games = game_fetcher.get_games_batch(offset, batch_size, auto_offset)

            if not games:
                self.stdout.write("✅ Все игры обработаны!")
                break

            self.stdout.write(f'🔄 Батч {offset // batch_size + 1}: {len(games)} игр')

            # Обрабатываем батч
            stats = self.import_processor.run_single_import_batch(1, auto_offset, games)
            processed_total += len(games)

            # Обновляем статистику
            self.total_games_processed += stats.get('total', 0)
            self.not_found_ids.update(self.import_processor.not_found_ids)

            # Сдвигаем offset
            offset += len(games)

            # Проверяем лимит
            if limit > 0 and processed_total >= limit:
                self.stdout.write(f"✅ Достигнут лимит: {limit} игр")
                break

            # Уменьшенная пауза между батчами
            time.sleep(0.05)  # Уменьшили паузу до 0.05 секунд

    def execute_infinite_batch_mode(self, auto_offset, batch_size, options):
        """Выполняет бесконечные повторения - каждый повтор как отдельный запуск команды"""
        repeat_num = 1
        repeat_delay = options.get('repeat_delay', 10.0)

        # Статистика сессии
        games_processed_this_session = 0
        consecutive_empty_results = 0
        max_consecutive_empty = 3

        try:
            while True:
                # 1. Проверка прерывания
                if self.signal_handler.is_shutdown():
                    self.stdout.write("\n⚠️  Прерывание запрошено, завершаю бесконечный цикл...")
                    break

                # 2. Получаем актуальный статус
                self.stdout.write(f'\n{"=" * 60}')
                self.stdout.write(f'🔄 ПОВТОРЕНИЕ {repeat_num}')
                self.stdout.write(f'📊 Обработано в этой сессии: {games_processed_this_session:,} игр')

                # 3. Проверяем доступные игры
                game_fetcher = GameFetcher(options, self.not_found_ids)
                available_games = game_fetcher.get_total_games_to_process(auto_offset)

                if available_games == 0:
                    if games_processed_this_session > 0:
                        self.stdout.write(self.style.SUCCESS('🎉 ВСЕ ДОСТУПНЫЕ ИГРЫ ОБРАБОТАНЫ!'))
                        self.stdout.write(f'   ✅ В этой сессии обработано: {games_processed_this_session:,} игр')
                    else:
                        self.stdout.write('🎉 Нет игр для обработки!')
                    break

                self.stdout.write(f'📈 Доступно для обработки: {available_games:,} игр')

                # 4. Проверяем баланс API
                self.stdout.write(f'🔍 Проверка баланса API...')

                balance_check = self.rawg_client.check_balance(force=True)
                if balance_check.get('balance_exceeded', False):
                    error_msg = balance_check.get('error', 'Неизвестная ошибка баланса')
                    self.stdout.write(self.style.ERROR(f'🚫 ОШИБКА БАЛАНСА: {error_msg}'))

                    # Проверяем, это временный лимит или окончательный
                    if "секунд" in error_msg or "минут" in error_msg or "час" in error_msg:
                        import re
                        match = re.search(r'(\d+)\s*(секунд|минут|час|часов)', error_msg)
                        wait_time = 300  # По умолчанию 5 минут

                        if match:
                            wait_time = int(match.group(1))
                            unit = match.group(2)
                            if "минут" in unit:
                                wait_time *= 60
                            elif "час" in unit:
                                wait_time *= 3600

                        self.stdout.write(f'⏳ Временный лимит. Ожидание {wait_time} секунд...')

                        # Ждем с возможностью прерывания
                        start_wait = time.time()
                        while time.time() - start_wait < wait_time:
                            if self.signal_handler.is_shutdown():
                                break

                            elapsed = time.time() - start_wait
                            remaining = wait_time - elapsed

                            # Показываем прогресс каждые 10 секунд
                            if int(elapsed) % 10 == 0:
                                mins = int(remaining // 60)
                                secs = int(remaining % 60)
                                self.stdout.write(f'\r   ⏳ Осталось: {mins:02d}:{secs:02d}', ending='')
                                self.stdout.flush()

                            time.sleep(1)

                        self.stdout.write('\r' + ' ' * 25 + '\r')
                        self.stdout.write('✅ Ожидание завершено, продолжаем...')
                        continue
                    else:
                        # Это окончательный лимит
                        self.stdout.write(self.style.ERROR('🚫 КРИТИЧЕСКАЯ ОШИБКА БАЛАНСА!'))
                        self.stdout.write('💡 Подождите сутки или обновите API ключ')
                        break

                # Показываем информацию о балансе
                requests_remaining = balance_check.get('requests_remaining')
                if requests_remaining:
                    self.stdout.write(f'📊 Осталось запросов: {requests_remaining:,}')
                    if requests_remaining < 50:
                        self.stdout.write(self.style.WARNING('⚠️  ОЧЕНЬ МАЛО ЗАПРОСОВ! Подумайте о паузе'))

                # 5. Получаем батч игр
                current_options = options.copy()
                current_options['limit'] = min(batch_size, available_games)
                current_options['offset'] = 0

                game_fetcher = GameFetcher(current_options, self.not_found_ids)
                games = game_fetcher.get_games_to_process(None, auto_offset)

                # Отладочный вывод
                if not games and options.get('debug'):
                    self.stdout.write(f'[DEBUG] Пустой батч с параметрами:')
                    self.stdout.write(f'  - limit: {current_options["limit"]}')
                    self.stdout.write(f'  - offset: 0')
                    self.stdout.write(f'  - available_games: {available_games}')

                if not games:
                    consecutive_empty_results += 1
                    self.stdout.write(f'⚠️  Пустой батч #{consecutive_empty_results}')

                    if consecutive_empty_results >= max_consecutive_empty:
                        if games_processed_this_session > 0:
                            self.stdout.write(
                                self.style.SUCCESS(f'🎉 Завершаю. Обработано: {games_processed_this_session:,} игр'))
                        else:
                            self.stdout.write('ℹ️  Не найдено игр для обработки')
                        break

                    time.sleep(2)
                    continue

                # Сбрасываем счетчик пустых батчей
                consecutive_empty_results = 0

                self.stdout.write(f'🔄 Начинаем обработку {len(games)} игр...')

                # 6. Запускаем обработку
                processing_result = self._process_games_batch(repeat_num, auto_offset, games)

                if processing_result.get('interrupted'):
                    break

                if processing_result.get('balance_exceeded'):
                    self.stdout.write(self.style.ERROR('\n🚫 ЛИМИТ API ПРЕВЫШЕН ВО ВРЕМЯ ОБРАБОТКИ!'))
                    break

                # 7. Обновляем статистику
                games_processed = processing_result.get('total', 0)
                if games_processed > 0:
                    games_processed_this_session += games_processed
                    self.total_games_processed += games_processed

                    # Обновляем список не найденных игр
                    self.not_found_ids.update(self.import_processor.not_found_ids)

                    # Показываем результат
                    self._show_batch_result(processing_result)

                    # 8. Пауза между повторами
                    if repeat_delay > 0:
                        self._pause_between_repeats(repeat_delay)
                        if self.signal_handler.is_shutdown():
                            break
                else:
                    self.stdout.write('⚠️  Игры были, но не обработаны (games_processed = 0)')
                    # Показываем отладку
                    if options.get('debug'):
                        self.stdout.write(f'[DEBUG] processing_result:')
                        for key, value in processing_result.items():
                            self.stdout.write(f'  {key}: {value}')

                    # Если есть новые ненайденные игры, покажем их
                    new_not_found = processing_result.get('new_not_found', 0)
                    if new_not_found > 0:
                        self.stdout.write(f'   🔍 Новых ненайденных игр: {new_not_found}')

                    # Небольшая пауза чтобы не зациклиться
                    time.sleep(1)

                repeat_num += 1

        except KeyboardInterrupt:
            self.stdout.write("\n\n⚠️  Получен KeyboardInterrupt")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'\n❌ Неожиданная ошибка: {e}'))
            import traceback
            traceback.print_exc()
        finally:
            self._show_session_summary(repeat_num, games_processed_this_session)

    def _pause_between_repeats(self, repeat_delay):
        """Выполняет паузу между повторами"""
        self.stdout.write(f'\n⏳ Пауза {repeat_delay} секунд...')

        start_sleep = time.time()

        while time.time() - start_sleep < repeat_delay:
            if self.signal_handler.is_shutdown():
                self.stdout.write("\n⚠️  Прерывание во время паузы...")
                break

            elapsed = time.time() - start_sleep
            remaining = repeat_delay - elapsed

            # Показываем таймер каждые 5 секунд
            if int(elapsed) % 5 == 0:
                mins = int(remaining // 60)
                secs = int(remaining % 60)
                self.stdout.write(f'\r   ⏳ Пауза: {mins:02d}:{secs:02d}', ending='')
                self.stdout.flush()

            time.sleep(0.5)

        # Очищаем строку таймера
        self.stdout.write('\r' + ' ' * 25 + '\r')

    def _show_session_summary(self, repeat_num, games_processed_this_session):
        """Показывает итоги сессии"""
        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write(self.style.SUCCESS('🏁 СЕССИЯ ЗАВЕРШЕНА'))

        if games_processed_this_session > 0:
            self.stdout.write(f'📊 ИТОГИ СЕССИИ:')
            self.stdout.write(f'   🔁 Выполнено повторов: {repeat_num - 1}')
            self.stdout.write(f'   🎮 Обработано игр: {games_processed_this_session:,}')
            self.stdout.write(f'   📈 Всего обработано в системе: {self.total_games_processed:,}')

            if self.not_found_ids:
                self.stdout.write(f'   ❓ Ненайденных игр в кэше: {len(self.not_found_ids)}')

            # Рассчитываем эффективность
            if repeat_num - 1 > 0:
                avg_games_per_repeat = games_processed_this_session / (repeat_num - 1)
                self.stdout.write(f'   ⚡ В среднем за повтор: {avg_games_per_repeat:.1f} игр')
        else:
            self.stdout.write('ℹ️  Игры не обрабатывались в этой сессии')

    def _process_games_batch(self, repeat_num, auto_offset, games_batch):
        """Обрабатывает батч игр и возвращает результат"""
        # Обновляем опции в процессоре
        self.import_processor.options = {
            'dry_run': self.original_options.get('dry_run', False),
            'min_length': self.original_options.get('min_length', 1),
            'debug': self.original_options.get('debug', False),
            'workers': self.original_options.get('workers', 4),
            'delay': self.original_options.get('delay', 0.1),
            'skip_cache': self.original_options.get('skip_cache', False),
            'cache_ttl': self.original_options.get('cache_ttl', 30)
        }

        # Запускаем импорт для батча
        return self.import_processor.run_single_import_batch(repeat_num, auto_offset, games_batch)

    def _show_repeat_header(self, repeat_num, games_processed):
        """Показывает заголовок повтора"""
        self.stdout.write(f'\n{"=" * 60}')
        self.stdout.write(f'🔄 ПОВТОРЕНИЕ {repeat_num}')
        self.stdout.write(f'📊 Обработано в этой сессии: {games_processed:,} игр')

    def _get_available_games_count(self, options, auto_offset):
        """Получает количество доступных для обработки игр"""
        game_fetcher = GameFetcher(options, self.not_found_ids)
        return game_fetcher.get_total_games_to_process(auto_offset)

    def _handle_no_games(self, games_processed):
        """Обрабатывает ситуацию, когда нет игр для обработки"""
        if games_processed > 0:
            self.stdout.write(self.style.SUCCESS('🎉 ВСЕ ДОСТУПНЫЕ ИГРЫ ОБРАБОТАНЫ!'))
            self.stdout.write(f'   ✅ В этой сессии обработано: {games_processed:,} игр')
        else:
            self.stdout.write('🎉 Нет игр для обработки!')

    def _check_api_balance(self):
        """Проверяет баланс API"""
        self.stdout.write(f'🔍 Проверка баланса API...')

        balance_check = self.rawg_client.check_balance(force=True)
        if balance_check.get('balance_exceeded', False):
            error_msg = balance_check.get('error', 'Неизвестная ошибка баланса')
            self.stdout.write(self.style.ERROR(f'🚫 ОШИБКА БАЛАНСА: {error_msg}'))

            # Проверяем, это временный лимит или окончательный
            if "секунд" in error_msg or "минут" in error_msg or "час" in error_msg:
                # Это временный лимит - пробуем подождать
                import re
                match = re.search(r'(\d+)\s*(секунд|минут|час|часов)', error_msg)
                wait_time = 300  # По умолчанию 5 минут

                if match:
                    wait_time = int(match.group(1))
                    unit = match.group(2)
                    if "минут" in unit:
                        wait_time *= 60
                    elif "час" in unit:
                        wait_time *= 3600

                self.stdout.write(f'⏳ Временный лимит. Ожидание {wait_time} секунд...')

                # Ждем с возможностью прерывания
                start_wait = time.time()
                while time.time() - start_wait < wait_time:
                    if self.signal_handler.is_shutdown():
                        return False

                    elapsed = time.time() - start_wait
                    remaining = wait_time - elapsed

                    # Показываем прогресс каждые 10 секунд
                    if int(elapsed) % 10 == 0:
                        mins = int(remaining // 60)
                        secs = int(remaining % 60)
                        self.stdout.write(f'\r   ⏳ Осталось: {mins:02d}:{secs:02d}', ending='')
                        self.stdout.flush()

                    time.sleep(1)

                self.stdout.write('\r' + ' ' * 25 + '\r')
                self.stdout.write('✅ Ожидание завершено, продолжаем...')
                return True  # Продолжаем после ожидания
            else:
                # Это окончательный лимит
                self.stdout.write(self.style.ERROR('🚫 КРИТИЧЕСКАЯ ОШИБКА БАЛАНСА!'))
                self.stdout.write('💡 Подождите сутки или обновите API ключ')
                return False

        # Показываем информацию о балансе
        requests_remaining = balance_check.get('requests_remaining')
        if requests_remaining:
            self.stdout.write(f'📊 Осталось запросов: {requests_remaining:,}')
            if requests_remaining < 50:
                self.stdout.write(self.style.WARNING('⚠️  ОЧЕНЬ МАЛО ЗАПРОСОВ! Подумайте о паузе'))

        return True

    def _get_games_batch(self, options, auto_offset, batch_size, available_games):
        """Получает батч игр для обработки"""
        current_options = options.copy()
        current_options['limit'] = min(batch_size, available_games)
        current_options['offset'] = 0

        game_fetcher = GameFetcher(current_options, self.not_found_ids)
        return game_fetcher.get_games_to_process(None, auto_offset)

    def _handle_consecutive_empty_batches(self, games_processed):
        """Обрабатывает несколько пустых батчей подряд"""
        if games_processed > 0:
            self.stdout.write(self.style.SUCCESS(f'🎉 Завершаю. Обработано: {games_processed:,} игр'))
        else:
            self.stdout.write('ℹ️  Не найдено игр для обработки')

    def _show_batch_result(self, processing_result):
        """Показывает детальный результат обработки батча"""
        games_processed = processing_result.get('total', 0)
        found = processing_result.get('found', 0)
        not_found = processing_result.get('not_found_count', 0)
        errors = processing_result.get('errors', 0)
        updated = processing_result.get('updated', 0)
        short = processing_result.get('short', 0)
        empty = processing_result.get('empty', 0)
        new_not_found = processing_result.get('new_not_found', 0)

        if games_processed > 0:
            # Основные метрики
            success_rate = (found / games_processed * 100) if games_processed > 0 else 0

            self.stdout.write(f'📊 РЕЗУЛЬТАТ БАТЧА:')
            self.stdout.write(f'   🎮 Обработано игр: {games_processed}')
            self.stdout.write(f'   ✅ Найдено с описаниями: {found} ({success_rate:.1f}%)')

            if not_found > 0:
                self.stdout.write(f'   ❓ Не найдено в RAWG: {not_found}')

            if new_not_found > 0:
                self.stdout.write(f'   🔍 Новых ненайденных: {new_not_found}')

            if short > 0:
                self.stdout.write(f'   📏 Слишком короткие описания: {short}')

            if empty > 0:
                self.stdout.write(f'   🚫 Пустые описания: {empty}')

            if errors > 0:
                self.stdout.write(f'   💥 Ошибок: {errors}')

            if updated > 0:
                self.stdout.write(f'   💾 Сохранено в БД: {updated}')

            # Статистика API и кэша
            cache_hits = processing_result.get('cache_hits', 0)
            cache_misses = processing_result.get('cache_misses', 0)
            search_requests = processing_result.get('search_requests', 0)
            detail_requests = processing_result.get('detail_requests', 0)

            if cache_hits + cache_misses > 0:
                cache_efficiency = (cache_hits / (cache_hits + cache_misses) * 100)
                self.stdout.write(f'   💾 КЭШ: {cache_hits}/{cache_hits + cache_misses} ({cache_efficiency:.1f}%)')

            if search_requests > 0 or detail_requests > 0:
                self.stdout.write(f'   🌐 API запросов: {search_requests} поиск, {detail_requests} детали')

            # Время обработки
            processing_time = processing_result.get('processing_time', 0)
            if processing_time > 0:
                games_per_sec = games_processed / processing_time
                self.stdout.write(f'   ⏱️  Время: {processing_time:.1f} сек ({games_per_sec:.1f} игр/сек)')

            # Дополнительная информация при отладке
            if self.original_options.get('debug'):
                balance_exceeded = processing_result.get('balance_exceeded_during_processing', False)
                interrupted = processing_result.get('interrupted', False)

                if balance_exceeded:
                    self.stdout.write('   🚫 ФЛАГ: Превышен лимит API во время обработки')
                if interrupted:
                    self.stdout.write('   ⚠️  ФЛАГ: Обработка была прервана')

                # Показываем все ключи для отладки
                debug_keys = [k for k in processing_result.keys() if k not in
                              ['total', 'found', 'not_found_count', 'errors', 'updated',
                               'short', 'empty', 'new_not_found', 'cache_hits', 'cache_misses',
                               'search_requests', 'detail_requests', 'processing_time',
                               'balance_exceeded_during_processing', 'interrupted']]
                if debug_keys:
                    self.stdout.write(f'   [DEBUG] Другие ключи: {debug_keys}')

        elif games_processed == 0 and self.original_options.get('debug'):
            # Отладка когда games_processed = 0
            self.stdout.write(f'[DEBUG] games_processed = 0, но processing_result:')
            for key, value in processing_result.items():
                self.stdout.write(f'   {key}: {value}')

    def execute_limited_repeats_batch(self, repeat_times, auto_offset, batch_size, options):
        """Выполняет ограниченное количество повторений с батчами"""
        offset = options.get('offset', 0)

        for repeat_num in range(1, repeat_times + 1):
            # Проверяем прерывание
            if self.signal_handler.is_shutdown():
                self.stdout.write("⚠️  Прерывание запрошено, завершаю...")
                break

            self.stdout.write(f'\n{"=" * 50}')
            self.stdout.write(f'🚀 ПОВТОРЕНИЕ {repeat_num}/{repeat_times}')

            # Обновляем опции для текущего повторения
            current_options = options.copy()
            current_options['limit'] = batch_size
            current_options['offset'] = offset
            self.import_processor.options = current_options

            # Запускаем импорт
            stats = self.import_processor.run_single_import(repeat_num, auto_offset)

            # Обновляем список не найденных игр
            self.not_found_ids.update(self.import_processor.not_found_ids)

            # Обновляем offset
            games_processed = stats.get('total', 0)
            offset += games_processed
            self.total_games_processed += games_processed

            if repeat_num < repeat_times:
                repeat_delay = options.get('repeat_delay', 10.0)  # Уменьшили паузу между повторами до 10 секунд
                if repeat_delay > 0:
                    self.stdout.write(f'\n⏳ Пауза {repeat_delay} секунд...')

                    # Пауза с проверкой прерывания
                    start_sleep = time.time()
                    while time.time() - start_sleep < repeat_delay:
                        if self.signal_handler.is_shutdown():
                            break
                        time.sleep(0.1)

                    if self.signal_handler.is_shutdown():
                        break

    def init_rawg_client(self, options):
        """Инициализация клиента RAWG с оптимизациями"""
        try:
            # Убрали параметр debug из конструктора, так как его нет в RAWGClient
            self.rawg_client = RAWGClient(
                api_key=options.get('api_key')
            )

            # Устанавливаем debug режим если нужно
            if options.get('debug'):
                self.rawg_client.set_debug(True)

            # Быстрая проверка баланса
            balance_check = self.rawg_client.check_balance(force=True, quick_check=True)

            if balance_check.get('balance_exceeded', False):
                self.stdout.write(
                    self.style.ERROR(f'❌ Проблема с API ключом: {balance_check.get("error", "Неизвестная ошибка")}'))
                return False
            else:
                self.stdout.write(self.style.SUCCESS('✅ RAWG клиент инициализирован'))

                # Показываем информацию о балансе
                if balance_check.get('requests_remaining'):
                    self.stdout.write(f'   📊 Осталось запросов: {balance_check["requests_remaining"]:,}')

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

    def load_not_found_from_rawg_cache_batch(self):
        """Загружает ненайденные игры из кэша RAWG (оптимизированная версия)"""
        if not self.rawg_client:
            self.stdout.write('⚠️ RAWG клиент не инициализирован')
            return

        try:
            # Получаем соединение с кэшем RAWG
            cache_conn = self.rawg_client.get_cache_connection()
            if not cache_conn:
                self.stdout.write('⚠️ Не удалось получить соединение с кэшем RAWG')
                return

            cursor = cache_conn.cursor()

            # Оптимизированный запрос: получаем только нужные данные
            cursor.execute('''
                           SELECT DISTINCT game_name
                           FROM rawg_cache
                           WHERE found = 0
                             AND updated_at
                               > datetime('now'
                               , '-30 days')
                               LIMIT 5000 -- Ограничиваем количество для производительности
                           ''')

            not_found_names = [row[0] for row in cursor.fetchall()]

            if not not_found_names:
                self.stdout.write('ℹ️ В кэше RAWG нет записей о ненайденных играх')
                self.not_found_ids = set()
                return

            self.stdout.write(f'📂 Загружаю {len(not_found_names)} ненайденных игр из кэша RAWG...')

            # Используем батчевый поиск в базе данных
            batch_size = 100
            self.not_found_ids = set()

            for i in range(0, len(not_found_names), batch_size):
                batch_names = not_found_names[i:i + batch_size]

                # Ищем игры батчем
                games = Game.objects.filter(name__in=batch_names).only('id', 'igdb_id', 'name')
                self.not_found_ids.update(game.igdb_id for game in games)

                # Прогресс
                if i % 1000 == 0:
                    self.stdout.write(
                        f'   ⏳ Обработано {min(i + batch_size, len(not_found_names))}/{len(not_found_names)}')

            self.stdout.write(f'✅ Загружено {len(self.not_found_ids)} не найденных игр из кэша RAWG')

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

    def load_not_found_ids_from_file(self, filename):
        """Загружает список не найденных игр из файла (оптимизированная версия)"""
        file_ids = set()
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # Обрабатываем разные форматы файлов
                if 'not_found_games' in data:
                    # Новый формат с деталями игр
                    not_found_games = data['not_found_games']
                    file_ids = {game['igdb_id'] for game in not_found_games if 'igdb_id' in game}

                    summary = data.get('summary', {})
                    self.stdout.write(f'📄 Загружено из файла: {len(file_ids)} ненайденных игр')

                    if 'last_updated' in summary:
                        self.stdout.write(f'   📅 Последнее обновление файла: {summary["last_updated"]}')

                elif 'not_found_ids' in data:
                    # Старый формат только с ID
                    file_ids = set(data.get('not_found_ids', []))
                    self.stdout.write(f'📄 Загружено из файла (старый формат): {len(file_ids)} игр')

        except Exception as e:
            self.stdout.write(f'   ⚠️ Ошибка загрузки из файла: {e}')

        return file_ids

    def save_not_found_ids_to_file(self, filename):
        """Сохраняет список не найденных игр в файл (оптимизированная версия)"""
        try:
            if not self.not_found_ids:
                return

            self.stdout.write(f'\n💾 Сохраняю {len(self.not_found_ids)} ненайденных игр в файл...')

            # Получаем детали игр батчами для производительности
            not_found_details = []
            batch_size = 200

            igdb_ids = list(self.not_found_ids)
            total_ids = len(igdb_ids)

            for i in range(0, total_ids, batch_size):
                batch_ids = igdb_ids[i:i + batch_size]

                games = Game.objects.filter(
                    igdb_id__in=batch_ids
                ).select_related('game_type').only(
                    'id', 'igdb_id', 'name', 'game_type_id',
                    'first_release_date', 'rating', 'rating_count'
                )

                for game in games:
                    not_found_details.append({
                        'igdb_id': game.igdb_id,
                        'name': game.name,
                        'game_type': game.game_type_id,
                        'game_type_name': game.game_type.name if game.game_type else None,
                        'first_release_date': game.first_release_date.isoformat() if game.first_release_date else None,
                        'rating': game.rating,
                        'rating_count': game.rating_count
                    })

                # Прогресс - показываем каждый батч или каждые 5 батчей
                processed = min(i + batch_size, total_ids)
                if i % (batch_size * 5) == 0 or processed == total_ids:
                    self.stdout.write(f'   ⏳ Подготовлено {processed}/{total_ids} игр')

            data = {
                'not_found_games': not_found_details,
                'summary': {
                    'total_count': len(self.not_found_ids),
                    'last_updated': datetime.now().isoformat(),
                    'cache_enabled': not self.original_options.get('skip_cache', False),
                    'total_games_processed': self.total_games_processed,
                    'processing_time_seconds': time.time() - self.start_time
                },
                'meta': {
                    'source': 'rawg_cache_and_file_backup',
                    'primary_source': 'rawg_cache.db',
                    'file_is_backup': True,
                    'important': 'Основные данные хранятся в кэше RAWG, этот файл - резервная копия'
                }
            }

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.stdout.write(f'✅ Файл не найденных игр сохранен: {filename}')
            self.stdout.write(f'   📊 Всего ненайденных игр: {len(self.not_found_ids)}')

        except Exception as e:
            self.stdout.write(f'   ⚠️ Ошибка сохранения в файл: {e}')
            # Сохраняем упрощенную версию
            try:
                data = {
                    'not_found_ids': list(self.not_found_ids),
                    'last_updated': datetime.now().isoformat(),
                    'count': len(self.not_found_ids),
                    'error': str(e),
                    'note': 'Упрощенный формат из-за ошибки'
                }
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                self.stdout.write(f'   💾 Сохранены только ID (без названий)')
            except:
                self.stdout.write(f'   💥 Критическая ошибка сохранения файла')

    def show_cache_info(self, options):
        """Показывает информацию о кэше"""
        if not options.get('skip_cache'):
            self.stdout.write('💾 Кэширование: ВКЛЮЧЕНО')
            self.stdout.write(f'   📦 Не найденных игр в кэше RAWG: {len(self.not_found_ids)}')

            # Информация о размере кэша
            if self.rawg_client:
                cache_size = self.rawg_client.get_cache_size()
                if cache_size:
                    self.stdout.write(f'   📊 Размер кэша: {cache_size} записей')
        else:
            self.stdout.write('💾 Кэширование: ВЫКЛЮЧЕНО')

        auto_offset = options.get('auto_offset', True)
        if auto_offset:
            self.stdout.write(f'⚡ Auto-offset: пропуск {len(self.not_found_ids)} не найденных игр')
            self.stdout.write(f'   📄 Файл резервной копии: {options.get("auto_offset_file", "auto_offset_log.json")}')

    def show_final_statistics_comprehensive(self):
        """Показывает всеобъемлющую финальную статистику"""
        total_time = time.time() - self.start_time

        self.stdout.write('\n' + '📊' * 20)
        self.stdout.write('🏁 ИТОГОВАЯ СТАТИСТИКА')
        self.stdout.write('=' * 50)

        # Общая статистика
        self.stdout.write(f'⏱️  Общее время выполнения: {total_time:.1f} сек')
        self.stdout.write(f'🎮 Всего обработано игр: {self.total_games_processed:,}')

        if self.total_games_processed > 0:
            games_per_sec = self.total_games_processed / total_time
            self.stdout.write(f'⚡ Средняя скорость: {games_per_sec:.1f} игр/сек')

        # Статистика базы данных
        self.stats_manager.show_rawg_stats()

        # Статистика API
        if hasattr(self, 'rawg_client') and self.rawg_client:
            self.stats_manager.show_api_statistics(self.rawg_client)

        # Статистика кэша
        if self.not_found_ids:
            self.stdout.write(f'\n📋 Не найденных игр в системе: {len(self.not_found_ids)}')
            self.stdout.write(f'   💾 Основное хранилище: кэш RAWG')
            self.stdout.write(f'   📄 Резервная копия: файл auto_offset_log.json')
            self.stdout.write('💡 Эти игры будут пропускаться при следующих запусках')

        self.stdout.write('\n' + '🎉' * 20)
        self.stdout.write('✅ ИМПОРТ ЗАВЕРШЕН УСПЕШНО!')

    def show_final_statistics(self):
        """Показывает финальную статистику (обратная совместимость)"""
        self.show_final_statistics_comprehensive()

    def check_and_wait_for_balance(self):
        """Проверяет баланс и ждет если нужно"""
        if not self.rawg_client:
            return True

        balance_check = self.rawg_client.check_balance(force=True)

        if balance_check.get('balance_exceeded', False):
            error_msg = balance_check.get('error', 'Неизвестная ошибка баланса')
            self.stdout.write(self.style.ERROR(f'\n🚫 ЛИМИТ API ИСЧЕРПАН: {error_msg}'))

            # Если это временный лимит (429), ждем
            if "секунд" in error_msg or "минут" in error_msg:
                # Пытаемся извлечь время ожидания
                import re
                match = re.search(r'(\d+)\s*(секунд|минут|час)', error_msg)
                if match:
                    wait_time = int(match.group(1))
                    if "минут" in error_msg:
                        wait_time *= 60
                    elif "час" in error_msg:
                        wait_time *= 3600

                    self.stdout.write(f'⏳ Ожидание {wait_time} секунд...')
                    time.sleep(wait_time)
                    return True
                else:
                    # Ждем по умолчанию 5 минут
                    self.stdout.write(f'⏳ Ожидание 300 секунд (5 минут)...')
                    time.sleep(300)
                    return True
            else:
                # Если это окончательный лимит (например, суточный)
                return False

        return True
