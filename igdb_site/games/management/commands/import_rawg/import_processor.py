# FILE: import_processor.py
import time
from .game_fetcher import GameFetcher
from .data_processor import DataProcessor


class ImportProcessor:
    """Класс для управления процессом импорта"""

    def __init__(self, rawg_client, options, cache_manager, signal_handler=None, command=None):
        self.rawg_client = rawg_client
        self.options = options
        self.cache_manager = cache_manager
        self.signal_handler = signal_handler
        self.command = command
        self.data_processor = DataProcessor(rawg_client, options, signal_handler, command)
        self.not_found_ids = set()

    def run_single_import(self, repeat_num, auto_offset=False, games=None):
        """Запускает один импорт с обработкой ошибок лимита"""
        if self.signal_handler and self.signal_handler.is_shutdown():
            print("⚠️  Прерывание запрошено, пропускаем импорт...")
            return self.get_empty_stats()

        self.data_processor.init_stats(repeat_num)

        if games is not None:
            self.data_processor.stats['total_processed'] = len(games)
        else:
            game_fetcher = GameFetcher(self.options, self.not_found_ids)
            games = game_fetcher.get_games_to_process(
                self.options.get('game_ids'),
                auto_offset
            )

            if not games:
                print('ℹ️ Нет игр для обработки')
                stats = self.get_empty_stats()
                stats['no_games'] = True
                return stats

            self.data_processor.stats['total_processed'] = len(games)

        self._show_start_info(games, repeat_num)

        if self.signal_handler and self.signal_handler.is_shutdown():
            print("⚠️  Прерывание перед началом обработки...")
            stats = self.get_empty_stats()
            stats['interrupted'] = True
            return stats

        try:
            batch_result = self.data_processor.process_games(games)

            results = batch_result.get('results', self._empty_results())
            stats = batch_result.get('stats', {})

            # Копируем статистику из batch_result в data_processor.stats
            for key, value in stats.items():
                self.data_processor.stats[key] = value

            balance_exceeded_during_processing = results.get('balance_exceeded', False)

            if balance_exceeded_during_processing:
                print("\n" + "🚫" * 20)
                print("🚫 ОБНАРУЖЕНО ПРЕВЫШЕНИЕ ЛИМИТА API!")
                print("🚫 Обработка текущего батча прервана.")
                print("🚫" * 20)

            if self.signal_handler and self.signal_handler.is_shutdown():
                print("\n⚠️  Импорт прерван, сохраняем то что успели...")

            new_not_found = self.update_not_found_list(results, auto_offset)

            updated_count = 0
            if not self.options.get('dry_run') and results.get('descriptions'):
                try:
                    updated_count = self.data_processor.save_descriptions(results['descriptions'])
                    print(f'\n💾 Сохранено {updated_count} описаний')
                except Exception as e:
                    print(f'❌ Ошибка сохранения описаний: {str(e)[:100]}')

            self.data_processor.stats['updated'] = updated_count

            # Проверяем, что сумма категорий равна total_processed
            sum_categories = (
                    self.data_processor.stats.get('found', 0) +
                    self.data_processor.stats.get('not_found_count', 0) +
                    self.data_processor.stats.get('errors', 0)
            )

            if sum_categories != self.data_processor.stats.get('total_processed', 0):
                if self.options.get('debug'):
                    print(
                        f"[DEBUG] Корректируем total_processed: было {self.data_processor.stats.get('total_processed', 0)}, сумма категорий: {sum_categories}")
                self.data_processor.stats['total_processed'] = sum_categories

            if self.command and hasattr(self.command, 'update_progress_from_stats'):
                self.command.update_progress_from_stats(stats)

            if not balance_exceeded_during_processing:
                self._show_final_stats()
            else:
                self._show_partial_stats()

            if self.cache_manager:
                try:
                    self.cache_manager.save_efficiency_stats(self.data_processor.stats, repeat_num)
                except Exception as e:
                    if self.options.get('debug'):
                        print(f'⚠️  Ошибка сохранения статистики эффективности: {e}')

            stats_summary = self.get_stats_summary()
            stats_summary['new_not_found_list'] = list(new_not_found) if new_not_found else []
            stats_summary['balance_exceeded_during_processing'] = balance_exceeded_during_processing
            stats_summary['interrupted'] = False
            stats_summary['no_games'] = False

            if balance_exceeded_during_processing:
                stats_summary['stopped_early'] = True
                stats_summary['processed_before_stop'] = len(results.get('descriptions', {})) + len(
                    results.get('errors', []))
            else:
                stats_summary['stopped_early'] = False
                stats_summary['processed_before_stop'] = 0

            return stats_summary

        except KeyboardInterrupt:
            print("\n\n⚠️  Получен KeyboardInterrupt во время обработки")
            stats = self.get_empty_stats()
            stats['interrupted'] = True
            stats['keyboard_interrupt'] = True
            return stats

        except Exception as e:
            print(f'\n❌ Критическая ошибка в run_single_import: {str(e)[:100]}')
            if self.options.get('debug'):
                import traceback
                traceback.print_exc()

            stats = self.get_empty_stats()
            stats['interrupted'] = True
            stats['exception'] = str(e)[:200]
            return stats

    def run_single_import_batch(self, repeat_num, auto_offset=False, games=None):
        """Запускает импорт для конкретного батча игр с проверкой остановки"""
        if games is None:
            return self.run_single_import(repeat_num, auto_offset)

        if self.signal_handler and self.signal_handler.is_shutdown():
            return self.get_empty_stats()

        self.data_processor.init_stats(repeat_num)
        self.data_processor.stats['total_processed'] = len(games)

        batch_result = self.data_processor.process_games_batch(games)

        results = batch_result.get('results', self._empty_results())
        stats = batch_result.get('stats', {})

        # Если было прерывание, сразу возвращаем
        if self.signal_handler and self.signal_handler.is_shutdown():
            stats_summary = self.get_stats_summary()
            stats_summary['interrupted'] = True
            return stats_summary

        # ПРОВЕРЯЕМ, НУЖНО ЛИ ОСТАНОВИТЬСЯ ИЗ-ЗА ЛИМИТА
        should_stop = stats.get('should_stop', False)
        if should_stop:
            print("\n" + "🚫" * 30)
            print("🚫 ЛИМИТ API ИСЧЕРПАН! ОСТАНАВЛИВАЮ ОБРАБОТКУ...")
            print("🚫" * 30)

            # Устанавливаем флаг остановки в статистику
            self.data_processor.stats['balance_exceeded'] = True
            self.data_processor.stats['should_stop'] = True

        new_not_found = self.update_not_found_list(results, auto_offset)

        # Сохранение результатов (только если не остановлено из-за лимита)
        updated_count = 0
        if not self.options.get('dry_run') and results.get('descriptions') and not should_stop:
            updated_count = self.data_processor.save_descriptions(results['descriptions'])

        for key, value in stats.items():
            if key in self.data_processor.stats:
                self.data_processor.stats[key] = value

        self.data_processor.stats['updated'] = updated_count

        # Проверяем, что сумма категорий равна total_processed
        sum_categories = (
                self.data_processor.stats.get('found', 0) +
                self.data_processor.stats.get('not_found_count', 0) +
                self.data_processor.stats.get('errors', 0)
        )

        if sum_categories != self.data_processor.stats.get('total_processed', 0):
            if self.options.get('debug'):
                print(
                    f"[DEBUG] Корректируем total_processed: было {self.data_processor.stats.get('total_processed', 0)}, сумма категорий: {sum_categories}")
            self.data_processor.stats['total_processed'] = sum_categories

        if self.command and hasattr(self.command, 'update_progress_from_stats'):
            self.command.update_progress_from_stats(stats)

        if self.cache_manager:
            self.cache_manager.save_efficiency_stats(self.data_processor.stats, repeat_num)

        stats_summary = self.get_stats_summary()
        stats_summary['new_not_found_list'] = list(new_not_found) if new_not_found else []
        stats_summary['should_stop'] = should_stop  # ДОБАВЛЯЕМ ФЛАГ ОСТАНОВКИ

        return stats_summary

    def update_not_found_list(self, results, auto_offset):
        """Обновляет список не найденных игр без лишнего логирования"""
        new_not_found = set()

        if auto_offset and results.get('not_found'):
            new_not_found = set(results['not_found']) - self.not_found_ids
            if new_not_found:
                self.not_found_ids.update(new_not_found)
                self.data_processor.stats['new_not_found'] = len(new_not_found)

                # ИЗМЕНЯЕМ: только короткое сообщение, не детали по каждой игре
                if self.options.get('debug'):
                    self.data_processor.log_info(f'🔍 Новых ненайденных игр: {len(new_not_found)}')

                    # Только в отладочном режиме показываем несколько примеров
                    if len(new_not_found) <= 5:
                        for igdb_id in list(new_not_found)[:3]:
                            try:
                                from games.models import Game
                                game = Game.objects.filter(igdb_id=igdb_id).first()
                                if game:
                                    self.data_processor.log_debug(f'   • {igdb_id}: {game.name}')
                            except:
                                pass
                else:
                    # В обычном режиме - только общее количество
                    if len(new_not_found) > 10:
                        self.data_processor.log_info(f'🔍 Добавлено {len(new_not_found)} игр в список исключений')

        return new_not_found

    def get_empty_stats(self):
        """Возвращает пустую статистику"""
        return {
            'total_processed': 0,
            'updated': 0,
            'errors': 0,
            'new_not_found': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'found': 0,
            'not_found_count': 0,
            'processing_time': 0,
            'repeat_num': 0,
            'api_limit_hit': False,
            'balance_exceeded_during_processing': False,
            'interrupted': False,
            'no_games': False,
            'stopped_early': False,
            'processed_before_stop': 0,
            'keyboard_interrupt': False,
            'exception': None
        }

    def get_stats_summary(self):
        """Получает сводку статистики для текущего батча"""
        # Берем статистику из data_processor.stats
        stats = {
            'total_processed': self.data_processor.stats.get('total_processed', 0),
            'found': self.data_processor.stats.get('found', 0),
            'not_found_count': self.data_processor.stats.get('not_found_count', 0),
            'errors': self.data_processor.stats.get('errors', 0),
            'cache_hits': self.data_processor.stats.get('cache_hits', 0),
            'cache_misses': self.data_processor.stats.get('cache_misses', 0),
            'updated': self.data_processor.stats.get('updated', 0),
            'processing_time': time.time() - self.data_processor.stats['start_time'],
            'api_limit_hit': self.data_processor.stats.get('balance_exceeded', False)
        }

        # Проверка суммы
        if self.options.get('debug'):
            sum_categories = stats['found'] + stats['not_found_count'] + stats['errors']
            if sum_categories != stats['total_processed']:
                print(
                    f"[DEBUG] ВНИМАНИЕ: В статистике батча сумма категорий ({sum_categories}) не равна total_processed ({stats['total_processed']})")

        return stats

    def _empty_results(self):
        """Возвращает пустые результаты"""
        return {
            'descriptions': {},
            'not_found': [],
            'errors': [],
            'balance_exceeded': False
        }

    def _show_start_info(self, games, repeat_num):
        """Показывает стартовую информацию"""
        print(f'\n🚀 [{repeat_num}] Начинаем импорт {len(games)} игр')
        print(f'⚡ Оптимизации: КЭШИРОВАНИЕ {"ВЫКЛ" if self.options.get("skip_cache") else "ВКЛ"}')
        print(f'👷 Потоков: {self.options.get("workers", 4)}')
        print(f'⏱️  Задержка: {self.options.get("delay", 0.5)} сек')
        print(f'📏 Мин. длина: {self.options.get("min_length", 1)}')
        print('─' * 50)

    def _show_final_stats(self):
        """Показывает финальную статистику"""
        total_time = time.time() - self.data_processor.stats['start_time']
        total_processed = self.data_processor.stats.get('total_processed', 0)

        print('\n' + '=' * 50)
        print(f'🏁 ИМПОРТ [{self.data_processor.stats["repeat_num"]}] ЗАВЕРШЕН!')

        if total_processed > 0:
            games_per_sec = total_processed / total_time
            cache_hits = self.data_processor.stats.get('cache_hits', 0)
            cache_misses = self.data_processor.stats.get('cache_misses', 0)
            cache_checks = cache_hits + cache_misses
            cache_efficiency = (cache_hits / cache_checks * 100) if cache_checks > 0 else 0

            print(f'⏱️  Время: {total_time:.1f} сек')
            print(f'⚡ Скорость: {games_per_sec:.1f} игр/сек')
            print(f'🎯 Эффективность кэша: {cache_efficiency:.1f}%')

            found = self.data_processor.stats.get('found', 0)
            not_found = self.data_processor.stats.get('not_found_count', 0)
            errors = self.data_processor.stats.get('errors', 0)

            # Проценты
            found_pct = (found / total_processed * 100) if total_processed > 0 else 0
            not_found_pct = (not_found / total_processed * 100) if total_processed > 0 else 0
            errors_pct = (errors / total_processed * 100) if total_processed > 0 else 0

            print(f'📊 Всего обработано: {total_processed}')
            print(f'✅ Найдено: {found} ({found_pct:.1f}%)')
            print(f'❌ Не найдено: {not_found} ({not_found_pct:.1f}%)')
            print(f'💥 Ошибок: {errors} ({errors_pct:.1f}%)')

            # Проверка суммы
            sum_categories = found + not_found + errors
            if sum_categories != total_processed:
                print(
                    f'⚠️  ВНИМАНИЕ: сумма категорий ({sum_categories}) не равна общему количеству ({total_processed})')

        if self.options.get('dry_run'):
            print('\n⚠️  DRY RUN: данные НЕ сохранены')

    def _show_partial_stats(self):
        """Показывает частичную статистику при прерывании из-за лимита API"""
        total_time = time.time() - self.data_processor.stats['start_time']
        total_processed = self.data_processor.stats.get('total_processed', 0)

        print('\n' + '⚠️' * 20)
        print('⚠️  ИМПОРТ ПРЕРВАН ИЗ-ЗА ЛИМИТА API!')
        print('=' * 50)

        if total_processed > 0:
            games_per_sec = total_processed / total_time
            cache_hits = self.data_processor.stats.get('cache_hits', 0)
            cache_misses = self.data_processor.stats.get('cache_misses', 0)
            cache_checks = cache_hits + cache_misses
            cache_efficiency = (cache_hits / cache_checks * 100) if cache_checks > 0 else 0

            print(f'⏱️  Время работы: {total_time:.1f} сек')
            print(f'⚡ Скорость: {games_per_sec:.1f} игр/сек')
            print(f'🎯 Эффективность кэша: {cache_efficiency:.1f}%')

            found = self.data_processor.stats.get('found', 0)
            not_found = self.data_processor.stats.get('not_found_count', 0)
            errors = self.data_processor.stats.get('errors', 0)

            print(f'📊 Успели обработать: {total_processed}')
            print(f'✅ Найдено: {found}')
            print(f'❌ Не найдено: {not_found}')
            print(f'💥 Ошибок: {errors}')

            if total_processed > 0:
                success_rate = (found / total_processed * 100)
                print(f'📈 Успешность: {success_rate:.1f}%')

        print(f'\n💡 Рекомендации:')
        print(f'   1. Подождите некоторое время (обычно 1 минута для rate limit)')
        print(f'   2. Проверьте баланс API ключа на сайте RAWG')
        print(f'   3. При следующем запуске будет продолжено с места остановки')

    def _show_new_not_found_info(self, new_not_found):
        """Показывает информацию о новых ненайденных играх"""
        log_message = f'🔍 Новых ненайденных игр: {len(new_not_found)}'
        self.data_processor.log_info(log_message)

        if new_not_found:
            sample_size = min(3, len(new_not_found))
            sample_ids = list(new_not_found)[:sample_size]

            for igdb_id in sample_ids:
                try:
                    from games.models import Game
                    game = Game.objects.filter(igdb_id=igdb_id).first()
                    if game:
                        self.data_processor.log_info(f'   • {igdb_id}: {game.name}')
                except:
                    self.data_processor.log_info(f'   • {igdb_id}: (ошибка получения названия)')