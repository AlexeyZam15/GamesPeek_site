# FILE: main.py
import sys
import time
import json
import os
from datetime import datetime
from pathlib import Path
from .rawg_api import RAWGClient
from games.models import Game
from django.db.models import Q
from .base_command import ImportRawgBaseCommand
from .cache_manager import CacheManager
from .import_processor import ImportProcessor
from .signal_handler import SignalHandler
from .stats_manager import StatsManager
from .game_fetcher import GameFetcher


class Command(ImportRawgBaseCommand):
    help = 'Импорт описаний из RAWG API с кэшированием и оптимизацией'

    def __init__(self):
        super().__init__()
        self.signal_handler = None
        self.cache_manager = None
        self.stats_manager = None
        self.original_options = None
        self.import_processor = None
        self.start_time = None
        self.current_batch_num = 0

    def update_api_balance_in_progress(self):
        """Обновляет информацию о балансе API для прогресс-бара"""
        if hasattr(self, 'rawg_client') and self.rawg_client:
            try:
                balance = self.rawg_client.check_balance()

                # Сохраняем в прогресс-данных для использования в update_single_progress_line
                self.progress_data['api_remaining'] = balance['remaining']
                self.progress_data['api_percentage'] = balance['percentage']
                self.progress_data['api_exceeded'] = balance['exceeded']
                self.progress_data['api_is_low'] = balance['is_low']

                # Если лимит исчерпан, обновляем статус
                if balance['exceeded']:
                    self.progress_data['current_status'] = "🚫 Лимит API исчерпан"

                # Обновляем прогресс-бар
                self.update_single_progress_line()

                # Если баланс критически низкий, показываем предупреждение
                if balance['remaining'] < 100 and not self.progress_data.get('low_balance_warning_shown'):
                    self.clear_progress_line()
                    self.stdout.write(f"\n⚠️  ВНИМАНИЕ: Мало запросов API - осталось {balance['remaining']:,}")
                    self.stdout.write(f"\n   Следующий батч может не успеть завершиться.")
                    self.stdout.write(f"\n   Рекомендуется остановиться и пополнить баланс.\n")

                    # Показываем прогресс-бар снова
                    self.update_single_progress_line()
                    self.progress_data['low_balance_warning_shown'] = True

                return balance

            except Exception as e:
                if hasattr(self, 'original_options') and self.original_options.get('debug'):
                    print(f"[DEBUG] Ошибка обновления баланса API: {e}")
                return None

    def _update_progress_stats_from_result(self, result):
        """Обновляет статистику прогресс-бара из результатов обработки батча"""
        # result уже содержит статистику батча
        stats = result

        if hasattr(self, 'original_options') and self.original_options.get('debug'):
            print(f"\n[DEBUG] Получена статистика батча:")
            print(f"  total_processed: {stats.get('total_processed', 0)}")
            print(f"  found: {stats.get('found', 0)}")
            print(f"  not_found_count: {stats.get('not_found_count', 0)}")
            print(f"  errors: {stats.get('errors', 0)}")
            print(f"  cache_hits: {stats.get('cache_hits', 0)}")
            print(f"  cache_misses: {stats.get('cache_misses', 0)}")

        # Обновляем ГЛОБАЛЬНУЮ статистику
        self.update_global_stats_from_batch(stats)

        # Обновляем прогресс текущей сессии
        games_in_batch = stats.get('total_processed', 0)
        if games_in_batch > 0:
            self.progress_data['processed_games'] += games_in_batch

        # Обновляем прогресс-бар
        self.update_single_progress_line()

        if hasattr(self, 'original_options') and self.original_options.get('debug'):
            print(f"  ПОСЛЕ обновления:")
            print(f"    Глобальная статистика:")
            print(f"      Всего обработано: {self.global_stats['total_processed']}")
            print(f"      Найдено: {self.global_stats['found']}")
            print(f"      Не найдено: {self.global_stats['not_found']}")
            print(f"      Ошибок: {self.global_stats['errors']}")

    def handle(self, *args, **options):
        """Основной обработчик команды с graceful shutdown"""
        try:
            self.start_time = time.time()
            self.signal_handler = SignalHandler(self)

            # Инициализируем глобальную статистику
            self.global_stats = {
                'total_processed': 0,
                'found': 0,
                'not_found': 0,
                'errors': 0,
                'cache_hits': 0,
                'cache_misses': 0,
                'start_time': time.time(),
                'sessions_processed': 0
            }

            # Инициализируем прогресс текущей сессии
            self.progress_data = {
                'total_games': 0,
                'processed_games': 0,
                'session_start_time': time.time(),
                'last_progress_length': 0,
                'current_status': '',
                'current_batch': 0,
                'current_batch_total': 0
            }

            self.progress_data['current_status'] = "🚀 Инициализация..."
            self.update_single_progress_line()

            return self._handle_with_interrupt(*args, **options)
        except SystemExit:
            raise
        except Exception as e:
            self.clear_progress_line()
            self.stdout.write(self.style.ERROR(f'❌ Необработанная ошибка: {e}'))
            import traceback
            traceback.print_exc()
            sys.exit(1)

    def _handle_with_interrupt(self, *args, **options):
        """Основной обработчик команды (внутренний метод)"""
        if options.get('reset'):
            self.clear_progress_line()
            self.stdout.write('=' * 60)
            self.stdout.write('🧹 ПОЛНЫЙ СБРОС: удаление кэша и списка ненайденных игр')

            self.stdout.write('   🔹 Удаление кэша RAWG API...')
            cache_manager = CacheManager(debug=options.get('debug', False))
            deleted_count = cache_manager.reset_cache()
            self.stdout.write(f'   ✅ Удалено {deleted_count} файлов кэша')

            auto_offset_file = options.get('auto_offset_file', 'auto_offset_log.json')
            if os.path.exists(auto_offset_file):
                os.remove(auto_offset_file)
                self.stdout.write(f'   🗑️  Удален файл: {auto_offset_file}')
            else:
                self.stdout.write(f'   ℹ️  Файл не найден: {auto_offset_file}')

            self.stdout.write('=' * 60)
            self.stdout.write(self.style.SUCCESS('✅ ПОЛНЫЙ СБРОС ВЫПОЛНЕН'))

            if len([k for k, v in options.items()
                    if v and k not in ['reset', 'verbosity', 'debug']]) == 0:
                return

        self.progress_data['current_status'] = "🔗 Подключение к RAWG API..."
        self.update_single_progress_line()

        if not self.init_rawg_client(options):
            self.clear_progress_line()
            return

        self.progress_data['current_status'] = "⚙️ Настройка менеджеров..."
        self.update_single_progress_line()

        self.cache_manager = CacheManager(debug=options.get('debug', False))
        self.stats_manager = StatsManager(self.stdout, self.style)
        self.original_options = options.copy()

        self.progress_data['current_status'] = "🚀 Запуск импорта..."
        self.update_single_progress_line()

        return self.run_main_import_process(options)

    def init_rawg_client(self, options):
        """Инициализация клиента RAWG"""
        try:
            # ПЕРЕДАЕМ api_limit В КОНСТРУКТОР - ОН САМ СБРОСИТ СЧЕТЧИК
            api_limit = options.get('api_limit')
            self.rawg_client = RAWGClient(
                api_key=options.get('api_key'),
                api_limit=api_limit  # Передаем явно, даже если None
            )

            if options.get('debug'):
                self.rawg_client.set_debug(True)

            # Простая проверка подключения
            try:
                test_params = {'key': self.rawg_client.api_key, 'page_size': 1}
                response = self.rawg_client.session.get(
                    f"{self.rawg_client.base_url}/games",
                    params=test_params,
                    timeout=5
                )

                if response.status_code == 200:
                    self.stdout.write(self.style.SUCCESS('✅ RAWG клиент инициализирован'))

                    # Показываем информацию о балансе
                    balance = self.rawg_client.check_balance()
                    self.stdout.write(f'   📊 Лимит API: {balance["limit"]:,}')
                    self.stdout.write(f'   📊 Использовано: {balance["used"]:,}')

                    # ЕСЛИ УКАЗАН НОВЫЙ ЛИМИТ, СООБЩАЕМ О СБРОСЕ
                    if api_limit is not None:
                        self.stdout.write(self.style.SUCCESS(f'   🔄 Счетчик запросов сброшен (указан новый лимит)'))

                    self.stdout.write(f'   📊 Осталось: {balance["remaining"]:,} ({balance["percentage"]:.1f}%)')

                    if balance['is_low']:
                        self.stdout.write(
                            self.style.WARNING(f'   ⚠️  Мало запросов: осталось {balance["remaining"]:,}'))

                    return True
                elif response.status_code == 401:
                    self.stdout.write(self.style.ERROR('❌ Неверный API ключ RAWG'))
                    return False
                else:
                    self.stdout.write(
                        self.style.WARNING(f'⚠️  API ответил с кодом {response.status_code}, продолжаем...'))
                    return True

            except Exception as e:
                self.stdout.write(self.style.WARNING(f'⚠️  Ошибка проверки API: {e}, продолжаем...'))
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

    def run_main_import_process(self, options):
        """Основной процесс импорта с единым прогресс-баром"""
        # Сбрасываем статистику текущей сессии
        self.progress_data = {
            'total_games': 0,
            'processed_games': 0,
            'session_start_time': time.time(),
            'last_progress_length': 0,
            'current_status': '',
            'current_batch': 0,
            'current_batch_total': 0,
            'api_remaining': 0,  # ДОБАВЛЯЕМ для хранения баланса API
            'api_percentage': 0,  # ДОБАВЛЯЕМ для процентного соотношения
            'api_exceeded': False,  # ДОБАВЛЯЕМ флаг превышения лимита
            'api_is_low': False  # ДОБАВЛЯЕМ флаг низкого баланса
        }

        self.progress_data['current_status'] = "📥 Загрузка данных из кэша..."
        self.update_single_progress_line()

        auto_offset = options.get('auto_offset', True)

        if auto_offset:
            self.progress_data['current_status'] = "📂 Загрузка из кэша RAWG..."
            self.update_single_progress_line()
            self.load_not_found_from_rawg_cache_batch()

            if not options.get('reset'):
                auto_offset_file = options.get('auto_offset_file', 'auto_offset_log.json')
                file_ids = self.load_not_found_ids_from_file(auto_offset_file)

                if file_ids:
                    self.not_found_ids.update(file_ids)

        self.progress_data['current_status'] = "⚙️ Настройка процессора..."
        self.update_single_progress_line()

        self.import_processor = ImportProcessor(
            self.rawg_client,
            options,
            self.cache_manager,
            self.signal_handler,
            self
        )
        self.import_processor.not_found_ids = self.not_found_ids

        if self.signal_handler.is_shutdown():
            self.progress_data['current_status'] = "⚠️ Прерывание запрошено"
            self.update_single_progress_line()
            self.log_to_file("⚠️ Прерывание запрошено до начала обработки")
            self.show_final_summary()
            return

        self.progress_data['current_status'] = "📊 Рассчет параметров..."
        self.update_single_progress_line()

        repeat_times = options['repeat']
        limit = options['limit']

        batch_size = self.calculate_optimal_batch_size(options)
        options['batch_size'] = batch_size

        # ОБНОВЛЯЕМ БАЛАНС API ПРИ СТАРТЕ
        if self.rawg_client:
            try:
                balance = self.rawg_client.check_balance()
                self.progress_data['api_remaining'] = balance['remaining']
                self.progress_data['api_percentage'] = balance['percentage']
                self.progress_data['api_exceeded'] = balance['exceeded']
                self.progress_data['api_is_low'] = balance['is_low']

                if balance['exceeded']:
                    self.clear_progress_line()
                    self.stdout.write("\n" + "🚫" * 30)
                    self.stdout.write("\n🚫 ЛИМИТ API УЖЕ ИСЧЕРПАН ПРИ СТАРТЕ!")
                    self.stdout.write(f"\n🚫 Использовано: {balance['used']:,}/{balance['limit']:,}")
                    self.stdout.write("\n" + "🚫" * 30)
                    self.stdout.write("\n\n💡 Рекомендации:")
                    self.stdout.write("\n   1. Подождите сутки для сброса лимита")
                    self.stdout.write("\n   2. Купите больше запросов на сайте RAWG")
                    self.stdout.write("\n   3. Используйте другой API ключ")
                    self.stdout.write("\n   4. Используйте --api-limit с новым значением")
                    self.stdout.write("\n")
                    self.show_final_summary()
                    return

            except Exception as e:
                if options.get('debug'):
                    self.stdout.write(f'\n⚠️ Ошибка получения баланса при старте: {e}')

        self.progress_data['current_status'] = f"⚡ Батч: {batch_size}, Потоков: {options.get('workers', 4)}"
        self.update_single_progress_line()

        try:
            # Получаем общее количество игр для обработки
            game_fetcher = GameFetcher(options, self.not_found_ids)
            total_to_process = game_fetcher.get_total_games_to_process(auto_offset)

            # Устанавливаем общее количество для прогресс-бара
            self.progress_data['total_games'] = total_to_process

            if total_to_process == 0:
                self.clear_progress_line()
                self.stdout.write("✅ Нет игр для обработки\n")
                self.show_final_summary()
                return

            # Показываем стартовую информацию
            self.clear_progress_line()
            self.stdout.write(f"\n🎯 ЦЕЛЬ: Обработать {total_to_process:,} игр")
            self.stdout.write(f"\n📦 Стратегия: Батчи по {batch_size} игр")
            self.stdout.write(f"\n👷 Рабочих потоков: {options.get('workers', 4)}")
            self.stdout.write(f"\n⏱️  Задержка: {options.get('delay', 0.1)} сек")

            # Показываем стартовый баланс
            if self.progress_data['api_remaining'] > 0:
                self.stdout.write(f"\n💳 Стартовый баланс API: {self.progress_data['api_remaining']:,} запросов")

                # Оцениваем сколько игр можно обработать
                estimated_games = int(self.progress_data['api_remaining'] * 0.67)  # Примерно 1.5 запроса на игру
                if estimated_games < total_to_process:
                    self.stdout.write(self.style.WARNING(
                        f"\n⚠️  ВНИМАНИЕ: Запросов хватит на ~{estimated_games:,} игр из {total_to_process:,}"))
                else:
                    self.stdout.write(f"\n✅ Запросов хватит на все {total_to_process:,} игр")

            self.stdout.write("\n" + "-" * 70 + "\n")

            # Очищаем прогресс-бар и запускаем обработку
            time.sleep(1)  # Даем время прочитать стартовую информацию

            if limit == 0 and repeat_times == 0:
                self.execute_infinite_batch_mode_with_progress(auto_offset, batch_size, options)
            elif repeat_times > 0:
                self.execute_limited_repeats_with_progress(repeat_times, auto_offset, batch_size, options)
            else:
                self.execute_single_batch_mode_with_progress(auto_offset, batch_size, options)

        except KeyboardInterrupt:
            self.clear_progress_line()
            self.stdout.write("\n⚠️ Получен KeyboardInterrupt\n")
            self.log_to_file("\n⚠️ Получен KeyboardInterrupt в основном потоке")
        except Exception as e:
            self.clear_progress_line()
            self.stdout.write(f"\n❌ Ошибка: {str(e)[:50]}\n")
            self.log_to_file(f'\n❌ Неожиданная ошибка: {e}')
            import traceback
            traceback.print_exc()
        finally:
            # Всегда показываем финальную статистику
            self.show_final_summary()

            if auto_offset and self.not_found_ids and not options.get('dry_run'):
                auto_offset_file = options.get('auto_offset_file', 'auto_offset_log.json')
                self.save_not_found_ids_to_file(auto_offset_file)

    def calculate_optimal_batch_size(self, options):
        """Рассчитывает оптимальный размер батча на основе настроек"""
        workers = options.get('workers', 4)
        delay = options.get('delay', 0.1)

        base_batch = workers * 25

        if delay < 0.05:
            batch_size = base_batch * 2
        elif delay < 0.1:
            batch_size = base_batch
        else:
            batch_size = max(10, base_batch // 2)

        max_batch = options.get('batch_size', 100)
        return min(batch_size, max_batch)

    def execute_infinite_batch_mode_with_progress(self, auto_offset, batch_size, options):
        """Бесконечный режим с единым прогресс-баром и остановкой при лимите"""
        repeat_num = 1
        repeat_delay = options.get('repeat_delay', 2.0)
        game_ids_str = options.get('game_ids')

        game_fetcher = GameFetcher(options, self.not_found_ids)
        total_to_process = game_fetcher.get_total_games_to_process(auto_offset)

        # Устанавливаем счетчики для текущей сессии
        self.progress_data['total_games'] = total_to_process
        self.progress_data['processed_games'] = 0
        self.progress_data['current_batch'] = 0
        self.progress_data['current_batch_total'] = 0
        self.progress_data['current_status'] = "Начинаем..."
        self.progress_data['session_start_time'] = time.time()

        # Очищаем и показываем прогресс-бар
        self.clear_progress_line()
        self.update_single_progress_line()

        try:
            while True:
                if self.signal_handler.is_shutdown():
                    self.clear_progress_line()
                    self.stdout.write("\n⚠️ Прерывание\n")
                    break

                # Получаем игры для батча
                games = self._get_games_batch(options, batch_size, auto_offset, game_ids_str)

                if not games:
                    self.clear_progress_line()
                    self.stdout.write("\n✅ Все игры обработаны!\n")
                    break

                # Устанавливаем счетчик для текущего батча
                self.progress_data['current_batch'] = 0
                self.progress_data['current_batch_total'] = len(games)
                self.progress_data['current_status'] = f"Батч {repeat_num}"
                self.update_single_progress_line()

                # Обрабатываем батч
                processing_result = self._process_games_batch(repeat_num, auto_offset, games)

                if processing_result.get('interrupted'):
                    self.clear_progress_line()
                    self.stdout.write("\n⚠️ Прервано\n")
                    break

                # ПРОВЕРЯЕМ, НУЖНО ЛИ ОСТАНОВИТЬСЯ ИЗ-ЗА ЛИМИТА API
                should_stop = processing_result.get('should_stop', False)
                if should_stop:
                    self.clear_progress_line()
                    self.stdout.write("\n" + "🚫" * 30)
                    self.stdout.write("\n🚫 ЛИМИТ API ИСЧЕРПАН!")
                    self.stdout.write("\n🚫 ОБРАБОТКА ОСТАНОВЛЕНА")
                    self.stdout.write("\n" + "🚫" * 30)

                    # Показываем баланс
                    if self.rawg_client:
                        try:
                            balance = self.rawg_client.check_balance()
                            self.stdout.write(f"\n📊 Использовано запросов: {balance['used']:,}/{balance['limit']:,}")
                            self.stdout.write(f"\n📊 Осталось: {balance['remaining']:,}")
                        except:
                            pass

                    self.stdout.write(f"\n\n💡 Рекомендации:")
                    self.stdout.write(f"\n   1. Подождите сутки для сброса лимита")
                    self.stdout.write(f"\n   2. Купите больше запросов на сайте RAWG")
                    self.stdout.write(f"\n   3. Используйте другой API ключ")
                    self.stdout.write(f"\n   4. При следующем запуске будет продолжено с места остановки")
                    break

                if processing_result.get('balance_exceeded'):
                    self.handle_balance_exceeded_during_processing_with_progress()
                    continue

                # Обновляем общую статистику
                if self.import_processor:
                    self.not_found_ids.update(self.import_processor.not_found_ids)

                # Обновляем прогресс-бар
                self._update_progress_stats_from_result(processing_result)

                # Пауза между итерациями
                if repeat_delay > 0:
                    time.sleep(repeat_delay)
                    if self.signal_handler.is_shutdown():
                        break

                repeat_num += 1

        except KeyboardInterrupt:
            self.clear_progress_line()
            self.stdout.write("\n⚠️ Прервано\n")
        except Exception as e:
            self.clear_progress_line()
            self.stdout.write(f"\n❌ Ошибка: {str(e)[:100]}\n")
            import traceback
            traceback.print_exc()

    def execute_single_batch_mode_with_progress(self, auto_offset, batch_size, options):
        """Одиночный запуск с единым прогресс-баром"""
        game_ids_str = options.get('game_ids')
        offset = options.get('offset', 0)
        limit = options.get('limit', 0)

        game_fetcher = GameFetcher(options, self.not_found_ids)

        # Получаем общее количество игр для обработки
        total_to_process = game_fetcher.get_total_games_to_process(auto_offset)
        if limit > 0:
            total_to_process = min(total_to_process, limit)

        # Устанавливаем общее количество для прогресс-бара
        self.progress_data['total_games'] = total_to_process
        self.progress_data['processed_games'] = 0
        self.progress_data['session_start_time'] = time.time()

        self.stdout.write(f"\n📊 Всего игр для обработки: {total_to_process:,}")
        self.stdout.write(f"📦 Размер батча: {batch_size} игр\n")

        batch_num = 1

        try:
            while True:
                # Проверяем прерывание
                if self.signal_handler and self.signal_handler.is_shutdown():
                    self.stdout.write("\n⚠️ Прерывание запрошено\n")
                    break

                # Получаем батч игр
                games = game_fetcher.get_games_batch(offset, batch_size, auto_offset, game_ids_str)

                if not games:
                    self.stdout.write("\n✅ Все игры обработаны!\n")
                    break

                self.stdout.write(f"\n🔄 Батч {batch_num}: {len(games)} игр... ")

                # Обрабатываем батч
                stats = self.import_processor.run_single_import_batch(batch_num, auto_offset, games)

                offset += len(games)

                # Обновляем статистику
                self.not_found_ids.update(self.import_processor.not_found_ids)

                # Обновляем статистику прогресс-бара
                self._update_progress_stats_from_result(stats)

                batch_num += 1

                # Проверяем лимит
                if limit > 0 and self.progress_data['processed_games'] >= limit:
                    self.stdout.write(f"\n✅ Достигнут лимит: {limit} игр\n")
                    break

        except KeyboardInterrupt:
            self.stdout.write("\n⚠️ Получен KeyboardInterrupt\n")
        except Exception as e:
            self.stdout.write(f"\n❌ Ошибка: {str(e)[:100]}\n")
            import traceback
            traceback.print_exc()

    def execute_limited_repeats_with_progress(self, repeat_times, auto_offset, batch_size, options):
        """Ограниченные повторы с единым прогресс-баром"""
        offset = options.get('offset', 0)
        game_ids_str = options.get('game_ids')

        game_fetcher = GameFetcher(options, self.not_found_ids)
        total_to_process = game_fetcher.get_total_games_to_process(auto_offset)
        if total_to_process == 0:
            self.update_single_progress_line("✅ Нет игр для обработки")
            self.log_to_file("✅ Нет игр для обработки")
            return

        self.progress_data['total_games'] = total_to_process
        self.progress_data['processed_games'] = 0
        self.progress_data['session_start_time'] = time.time()

        for repeat_num in range(1, repeat_times + 1):
            if self.signal_handler.is_shutdown():
                self.update_single_progress_line("⚠️ Прерывание запрошено, завершаю...")
                self.log_to_file("⚠️ Прерывание запрошено, завершаю...")
                break

            self.log_to_file(f'\n{"=" * 50}')
            self.log_to_file(f'🚀 ПОВТОРЕНИЕ {repeat_num}/{repeat_times}')

            current_options = options.copy()
            current_options['limit'] = batch_size
            current_options['offset'] = offset
            self.import_processor.options = current_options

            self.update_single_progress_line(f"🔄 Повтор {repeat_num}/{repeat_times}...")

            stats = self.import_processor.run_single_import(repeat_num, auto_offset)

            self.not_found_ids.update(self.import_processor.not_found_ids)

            games_processed = stats.get('total_processed', 0)
            offset += games_processed

            self._update_progress_stats_from_result(stats)

            self.progress_data['processed_games'] = offset

            self.log_batch_result(stats, games_processed)

            if repeat_num < repeat_times:
                repeat_delay = options.get('repeat_delay', 10.0)
                if repeat_delay > 0:
                    self.pause_between_repeats_with_progress(repeat_delay)
                    if self.signal_handler.is_shutdown():
                        break

    def pause_between_repeats_with_progress(self, repeat_delay):
        """Пауза с обновлением прогресс-бара"""
        start_time = time.time()

        for i in range(int(repeat_delay)):
            if self.signal_handler.is_shutdown():
                break

            elapsed = i + 1
            remaining = repeat_delay - elapsed

            if elapsed % 5 == 0 or remaining <= 3:
                self.update_single_progress_line(f"⏳ Пауза {int(remaining)}с...")

            time.sleep(1)

        self.update_single_progress_line()

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
        result = self.import_processor.run_single_import_batch(repeat_num, auto_offset, games_batch)

        # Отладка
        if self.original_options.get('debug'):
            print(f"\n[DEBUG] Получен result для батча из {len(games_batch)} игр:")

        return result

    def _get_games_batch(self, options, batch_size, auto_offset, game_ids_str):
        """Получает батч игр для обработки"""
        current_options = options.copy()
        current_options['limit'] = batch_size
        current_options['offset'] = 0

        game_fetcher = GameFetcher(current_options, self.not_found_ids)
        return game_fetcher.get_games_to_process(game_ids_str, auto_offset)

    def handle_balance_exceeded_during_processing_with_progress(self):
        """Обрабатывает превышение лимита во время обработки"""
        self.safe_print("\n🚫 Превышен лимит API во время обработки!")
        self.safe_print("⏳ Делаю паузу 30 секунд...")
        time.sleep(30)

    def log_batch_result(self, processing_result, games_processed):
        """Логирует результат батча"""
        found = processing_result.get('found', 0)
        not_found = processing_result.get('not_found_count', 0)
        errors = processing_result.get('errors', 0)
        updated = processing_result.get('updated', 0)
        cache_hits = processing_result.get('cache_hits', 0)
        cache_misses = processing_result.get('cache_misses', 0)

        self.log_to_file(f'📊 РЕЗУЛЬТАТ БАТЧА:')
        self.log_to_file(f'   🎮 Обработано: {games_processed} игр')

        if games_processed > 0:
            found_pct = (found / games_processed * 100)
            not_found_pct = (not_found / games_processed * 100)
            errors_pct = (errors / games_processed * 100)

            self.log_to_file(f'   ✅ Найдено: {found} ({found_pct:.1f}%)')
            self.log_to_file(f'   ❌ Не найдено: {not_found} ({not_found_pct:.1f}%)')
            self.log_to_file(f'   💥 Ошибок: {errors} ({errors_pct:.1f}%)')

        if updated > 0:
            self.log_to_file(f'   💾 Сохранено: {updated}')

        if cache_hits + cache_misses > 0:
            cache_total = cache_hits + cache_misses
            cache_efficiency = (cache_hits / cache_total * 100) if cache_total > 0 else 0
            self.log_to_file(f'   💾 Кэш: {cache_hits}/{cache_total} ({cache_efficiency:.1f}%)')

    def show_final_summary(self):
        """Показывает финальную сводку с глобальной статистикой"""
        # Очищаем прогресс-бар
        time.sleep(0.5)
        self.clear_progress_line()
        self.stdout.write("\n")

        self.stdout.write("=" * 70)
        self.stdout.write("\n🏁 ФИНАЛЬНАЯ СВОДКА ИМПОРТА")
        self.stdout.write("\n" + "=" * 70)

        # Показываем ГЛОБАЛЬНУЮ статистику за всё время работы
        if self.global_stats['total_processed'] > 0:
            total_time = time.time() - self.global_stats['start_time']

            self.stdout.write(f"\n📊 ГЛОБАЛЬНАЯ СТАТИСТИКА (за всё время работы):")
            self.stdout.write(f"\n   🎮 Всего обработано игр: {self.global_stats['total_processed']:,}")
            self.stdout.write(f"\n   ⏱️  Общее время работы: {total_time:.1f} сек")

            if total_time > 0:
                games_per_sec = self.global_stats['total_processed'] / total_time
                self.stdout.write(f"\n   ⚡ Средняя скорость: {games_per_sec:.1f} игр/сек")

            # Расчет процентов
            if self.global_stats['total_processed'] > 0:
                found_pct = (self.global_stats['found'] / self.global_stats['total_processed'] * 100)
                not_found_pct = (self.global_stats['not_found'] / self.global_stats['total_processed'] * 100)
                errors_pct = (self.global_stats['errors'] / self.global_stats['total_processed'] * 100)

                self.stdout.write(f"\n   📈 РАСПРЕДЕЛЕНИЕ РЕЗУЛЬТАТОВ:")
                self.stdout.write(f"\n   ✅ Найдено описаний: {self.global_stats['found']:,} ({found_pct:.1f}%)")
                self.stdout.write(f"\n   ❌ Не найдено игр: {self.global_stats['not_found']:,} ({not_found_pct:.1f}%)")
                self.stdout.write(f"\n   💥 Ошибок обработки: {self.global_stats['errors']:,} ({errors_pct:.1f}%)")

            # Проверка суммы
            if self.global_stats['total_processed'] > 0:
                sum_categories = self.global_stats['found'] + self.global_stats['not_found'] + self.global_stats[
                    'errors']
                if sum_categories != self.global_stats['total_processed']:
                    self.stdout.write(
                        f"\n   ⚠️  ВНИМАНИЕ: сумма категорий ({sum_categories}) не равна общему количеству ({self.global_stats['total_processed']})")

            # Статистика кэша
            if self.global_stats['cache_hits'] + self.global_stats['cache_misses'] > 0:
                cache_total = self.global_stats['cache_hits'] + self.global_stats['cache_misses']
                cache_efficiency = (self.global_stats['cache_hits'] / cache_total * 100)
                self.stdout.write(f"\n   💾 ЭФФЕКТИВНОСТЬ КЭША:")
                self.stdout.write(f"\n   ✅ Попаданий в кэш: {self.global_stats['cache_hits']:,}")
                self.stdout.write(f"\n   ❌ Промахов кэша: {self.global_stats['cache_misses']:,}")
                self.stdout.write(f"\n   📊 Эффективность: {cache_efficiency:.1f}%")

            self.stdout.write("-" * 70)
        else:
            self.stdout.write(f"\nℹ️ Игры не обрабатывались или статистика недоступна")

        # Показываем статистику текущей сессии
        if self.progress_data.get('processed_games', 0) > 0:
            session_time = time.time() - self.progress_data.get('session_start_time', time.time())

            self.stdout.write(f"\n📊 СТАТИСТИКА ТЕКУЩЕЙ СЕССИИ:")
            self.stdout.write(f"\n   🎮 Обработано игр: {self.progress_data['processed_games']:,}")
            self.stdout.write(f"\n   ⏱️  Время сессии: {session_time:.1f} сек")

            if session_time > 0:
                session_speed = self.progress_data['processed_games'] / session_time
                self.stdout.write(f"\n   ⚡ Скорость сессии: {session_speed:.1f} игр/сек")

        # Показываем итоговую статистику базы данных
        try:
            from django.db.models import Q
            from games.models import Game

            total_games_db = Game.objects.count()
            games_with_rawg = Game.objects.filter(
                ~Q(rawg_description__isnull=True) &
                ~Q(rawg_description__exact='')
            ).count()

            percentage = (games_with_rawg / total_games_db * 100) if total_games_db > 0 else 0

            self.stdout.write(f"\n\n📊 СТАТИСТИКА БАЗЫ ДАННЫХ:")
            self.stdout.write(f"\n   📁 Всего игр в БД: {total_games_db:,}")
            self.stdout.write(f"\n   ✅ С RAWG описанием: {games_with_rawg:,} ({percentage:.1f}%)")

            # Показываем прогресс-бар заполнения БД
            bar_length = 30
            filled = int(bar_length * percentage / 100)
            bar = "[" + "█" * filled + "░" * (bar_length - filled) + "]"
            self.stdout.write(f"\n   {bar} {percentage:.1f}% заполнено")

        except Exception as e:
            if hasattr(self, 'original_options') and self.original_options.get('debug'):
                self.stdout.write(f"\n   ⚠️ Ошибка получения статистики БД: {e}")

        # Показываем информацию о балансе API
        if self.rawg_client:
            try:
                balance = self.rawg_client.check_balance()
                self.stdout.write(f"\n\n💳 БАЛАНС API:")
                self.stdout.write(f"\n   📊 Лимит: {balance['limit']:,}")
                self.stdout.write(f"\n   📊 Использовано: {balance['used']:,} ({balance['percentage']:.1f}%)")
                self.stdout.write(f"\n   📊 Осталось: {balance['remaining']:,}")

                if balance['is_low']:
                    self.stdout.write(
                        self.style.WARNING(f"\n   ⚠️  Внимание: мало запросов ({balance['remaining']:,})"))
                if balance['exceeded']:
                    self.stdout.write(self.style.ERROR(f"\n   🚫 Лимит превышен! Нужно обновить баланс"))

            except Exception as e:
                if hasattr(self, 'original_options') and self.original_options.get('debug'):
                    self.stdout.write(f"\n   ⚠️ Ошибка получения баланса: {e}")

        # Показываем информацию о файлах логов только если есть ошибки
        log_dir = Path(self.original_options.get('log_dir', 'logs')) if hasattr(self, 'original_options') else Path(
            'logs')

        if log_dir.exists() and self.global_stats['errors'] > 0:
            log_files = list(log_dir.glob('error_details_*.json'))
            if log_files:
                # Берем последний файл ошибок
                latest_error_file = max(log_files, key=lambda f: f.stat().st_mtime)
                try:
                    with open(latest_error_file, 'r', encoding='utf-8') as f:
                        error_data = json.load(f)

                    total_errors = error_data.get('statistics', {}).get('total_errors', 0)
                    if total_errors > 0:
                        self.stdout.write(f"\n\n📄 ФАЙЛЫ ОШИБОК:")
                        self.stdout.write(f"\n   🔴 Обнаружено {total_errors} ошибок")
                        self.stdout.write(f"\n   📁 Подробности в: {latest_error_file}")

                        # Показываем топ ошибок
                        errors_by_type = error_data.get('statistics', {}).get('errors_by_type', {})
                        if errors_by_type:
                            top_errors = sorted(errors_by_type.items(), key=lambda x: x[1], reverse=True)[:3]
                            self.stdout.write(f"\n   📊 Топ ошибок:")
                            for err_type, count in top_errors:
                                self.stdout.write(f"\n     • {err_type}: {count}")
                except Exception as e:
                    pass  # Не показываем, если не удалось прочитать

        # Рекомендации на основе статистики
        if self.global_stats['total_processed'] > 0:
            self.stdout.write(f"\n\n💡 РЕКОМЕНДАЦИИ:")

            # Рекомендации на основе процента ошибок
            error_rate = (self.global_stats['errors'] / self.global_stats['total_processed'] * 100)
            if error_rate > 20:
                self.stdout.write(self.style.ERROR(
                    f"\n   🔴 Высокий уровень ошибок ({error_rate:.1f}%)"))
            elif error_rate > 5:
                self.stdout.write(self.style.WARNING(
                    f"\n   🟡 Умеренный уровень ошибок ({error_rate:.1f}%)"))

            # Рекомендации на основе процента не найденных игр
            not_found_rate = (self.global_stats['not_found'] / self.global_stats['total_processed'] * 100)
            if not_found_rate > 40:
                self.stdout.write(f"\n   🟡 Много ненайденных игр ({not_found_rate:.1f}%)")

        # Финальное сообщение
        self.stdout.write("\n" + "=" * 70)

        if self.global_stats['errors'] > 0:
            self.stdout.write(self.style.WARNING(f"\n⚠️  ВНИМАНИЕ: Было {self.global_stats['errors']} ошибок!"))

        self.stdout.write(self.style.SUCCESS("\n✅ ИМПОРТ ЗАВЕРШЕН"))

        if self.global_stats['total_processed'] > 0:
            self.stdout.write(f" (обработано {self.global_stats['total_processed']:,} игр)")

            if self.global_stats['found'] > 0:
                self.stdout.write(self.style.SUCCESS(
                    f", найдено {self.global_stats['found']:,} описаний"))

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write("\n")
        self.stdout.flush()

    def load_not_found_from_rawg_cache_batch(self):
        """Загружает ненайденные игры из кэша RAWG"""
        if not self.rawg_client:
            self.stdout.write('⚠️ RAWG клиент не инициализирован')
            return

        try:
            cache_conn = self.rawg_client.get_cache_connection()
            if not cache_conn:
                self.stdout.write('⚠️ Не удалось получить соединение с кэшем RAWG')
                return

            cursor = cache_conn.cursor()

            cursor.execute('''
                           SELECT DISTINCT game_name
                           FROM rawg_cache
                           WHERE found = 0
                             AND updated_at
                               > datetime('now'
                               , '-30 days')
                               LIMIT 5000
                           ''')

            not_found_names = [row[0] for row in cursor.fetchall()]

            if not not_found_names:
                self.stdout.write('ℹ️ В кэше RAWG нет записей о ненайденных играх')
                self.not_found_ids = set()
                return

            self.stdout.write(f'📂 Загружаю {len(not_found_names)} ненайденных игр из кэша RAWG...')

            batch_size = 100
            self.not_found_ids = set()

            for i in range(0, len(not_found_names), batch_size):
                batch_names = not_found_names[i:i + batch_size]

                games = Game.objects.filter(name__in=batch_names).only('id', 'igdb_id', 'name')
                self.not_found_ids.update(game.igdb_id for game in games)

            self.stdout.write(f'✅ Загружено {len(self.not_found_ids)} не найденных игр из кэша RAWG')

        except Exception as e:
            self.stdout.write(f'⚠️ Ошибка загрузки из кэша RAWG: {e}')
            self.not_found_ids = set()

    def load_not_found_ids_from_file(self, filename):
        """Загружает список не найденных игр из файла"""
        file_ids = set()
        try:
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                if 'not_found_games' in data:
                    not_found_games = data['not_found_games']
                    file_ids = {game['igdb_id'] for game in not_found_games if 'igdb_id' in game}
                elif 'not_found_ids' in data:
                    file_ids = set(data.get('not_found_ids', []))

                self.stdout.write(f'📄 Загружено из файла: {len(file_ids)} игр')

        except Exception as e:
            self.stdout.write(f'   ⚠️ Ошибка загрузки из файла: {e}')

        return file_ids

    def save_not_found_ids_to_file(self, filename):
        """Сохраняет список не найденных игр в файл"""
        try:
            if not self.not_found_ids:
                return

            self.stdout.write(f'\n💾 Сохраняю {len(self.not_found_ids)} ненайденных игр в файл...')

            not_found_details = []
            batch_size = 200

            igdb_ids = list(self.not_found_ids)
            total_ids = len(igdb_ids)

            for i in range(0, total_ids, batch_size):
                batch_ids = igdb_ids[i:i + batch_size]

                games = Game.objects.filter(
                    igdb_id__in=batch_ids
                ).only(
                    'id', 'igdb_id', 'name',
                    'first_release_date', 'rating', 'rating_count', 'game_type'
                )

                for game in games:
                    not_found_details.append({
                        'igdb_id': game.igdb_id,
                        'name': game.name,
                        'game_type': game.game_type,
                        'first_release_date': game.first_release_date.isoformat() if game.first_release_date else None,
                        'rating': game.rating,
                        'rating_count': game.rating_count
                    })

            data = {
                'not_found_games': not_found_details,
                'summary': {
                    'total_count': len(self.not_found_ids),
                    'last_updated': datetime.now().isoformat(),
                    'cache_enabled': not self.original_options.get('skip_cache', False),
                    'global_stats': self.global_stats.copy()  # Сохраняем глобальную статистику
                }
            }

            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

            self.stdout.write(f'✅ Файл не найденных игр сохранен: {filename}')

        except Exception as e:
            self.stdout.write(f'   ⚠️ Ошибка сохранения в файл: {e}')

            try:
                data = {
                    'not_found_ids': list(self.not_found_ids),
                    'last_updated': datetime.now().isoformat(),
                    'count': len(self.not_found_ids)
                }
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                self.stdout.write(f'   💾 Сохранены только ID')
            except:
                self.stdout.write(f'   💥 Критическая ошибка сохранения файла')

    def safe_print(self, message, newline=True):
        """Безопасный вывод поверх строки прогресса"""
        self.clear_progress_line()

        if newline:
            self.stdout.write(message + '\n')
        else:
            self.stdout.write(message)

        self.stdout.flush()

        if hasattr(self, 'progress_data') and self.progress_data.get('total_games', 0) > 0:
            self.update_single_progress_line()

    def log_to_file(self, message, level="INFO"):
        """Записывает сообщение в лог-файл (без вывода в консоль)"""
        log_dir = Path(self.original_options.get('log_dir', 'logs'))
        log_dir.mkdir(exist_ok=True)

        log_file = log_dir / f'import_rawg_{time.strftime("%Y%m%d")}.log'

        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"[{timestamp}] [{level}] {message}\n"

        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                f.write(log_entry)
        except Exception:
            pass