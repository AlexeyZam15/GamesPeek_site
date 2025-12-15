# games/management/commands/import_rawg/import_processor.py
import time
from .game_fetcher import GameFetcher
from .data_processor import DataProcessor


class ImportProcessor:
    """Класс для управления процессом импорта"""

    def __init__(self, rawg_client, options, cache_manager, signal_handler=None):
        self.rawg_client = rawg_client
        self.options = options
        self.cache_manager = cache_manager
        self.signal_handler = signal_handler
        self.data_processor = DataProcessor(rawg_client, options, signal_handler)
        self.not_found_ids = set()

    def run_single_import(self, repeat_num, auto_offset=False, games=None):
        """Запускает один импорт с обработкой ошибок лимита"""
        # Проверяем прерывание
        if self.signal_handler and self.signal_handler.is_shutdown():
            print("⚠️  Прерывание запрошено, пропускаем импорт...")
            stats = self.get_empty_stats()
            stats['balance_exceeded_during_processing'] = False
            stats['interrupted'] = True
            return stats

        # Инициализация
        self.data_processor.init_stats(repeat_num)

        # Если игры переданы, используем их
        if games is not None:
            self.data_processor.stats['total'] = len(games)
        else:
            # Получаем игры
            game_fetcher = GameFetcher(self.options, self.not_found_ids)
            games = game_fetcher.get_games_to_process(
                self.options.get('game_ids'),
                auto_offset
            )

            if not games:
                print('ℹ️ Нет игр для обработки')
                stats = self.get_empty_stats()
                stats['balance_exceeded_during_processing'] = False
                stats['no_games'] = True
                return stats

            self.data_processor.stats['total'] = len(games)

        # Показываем стартовую информацию
        self.show_start_info(games, repeat_num)

        # Проверяем прерывание перед началом обработки
        if self.signal_handler and self.signal_handler.is_shutdown():
            print("⚠️  Прерывание перед началом обработки...")
            stats = self.get_empty_stats()
            stats['balance_exceeded_during_processing'] = False
            stats['interrupted'] = True
            return stats

        try:
            # Обработка игр
            results = self.data_processor.process_games(games)

            # Проверяем, не был ли превышен лимит API во время обработки
            balance_exceeded_during_processing = results.get('balance_exceeded', False)

            if balance_exceeded_during_processing:
                print("\n" + "🚫" * 20)
                print("🚫 ОБНАРУЖЕНО ПРЕВЫШЕНИЕ ЛИМИТА API!")
                print("🚫 Обработка текущего батча прервана.")
                print("🚫" * 20)

            # Проверяем прерывание после обработки
            if self.signal_handler and self.signal_handler.is_shutdown():
                print("\n⚠️  Импорт прерван, сохраняем то что успели...")

            # Обработка результатов
            new_not_found = self.update_not_found_list(results, auto_offset)

            # Сохранение результатов (только если не dry_run)
            updated_count = 0
            if not self.options.get('dry_run') and results['descriptions']:
                try:
                    updated_count = self.data_processor.save_descriptions(results['descriptions'])
                    print(f'\n💾 Сохранено {updated_count} описаний')

                    if updated_count < len(results['descriptions']):
                        print(f'⚠️  Предупреждение: не все описания сохранены '
                              f'({updated_count}/{len(results["descriptions"])})')
                except Exception as e:
                    print(f'❌ Ошибка сохранения описаний: {str(e)[:100]}')
                    if self.options.get('debug'):
                        import traceback
                        traceback.print_exc()

            self.data_processor.stats['updated'] = updated_count

            # Показываем статистику (если не было критической ошибки лимита)
            if not balance_exceeded_during_processing:
                self.show_final_stats(results)
            else:
                # Для случая с превышением лимита показываем сокращенную статистику
                self.show_partial_stats(results)

            # Сохраняем статистику эффективности
            if self.cache_manager:
                try:
                    self.cache_manager.save_efficiency_stats(self.data_processor.stats, repeat_num)
                except Exception as e:
                    if self.options.get('debug'):
                        print(f'⚠️  Ошибка сохранения статистики эффективности: {e}')

            # Возвращаем статистику и новые ненайденные игры
            stats_summary = self.get_stats_summary(results)
            stats_summary['new_not_found_list'] = list(new_not_found) if new_not_found else []
            stats_summary['balance_exceeded_during_processing'] = balance_exceeded_during_processing
            stats_summary['interrupted'] = False
            stats_summary['no_games'] = False

            # Добавляем дополнительную информацию о лимите
            if balance_exceeded_during_processing:
                stats_summary['stopped_early'] = True
                stats_summary['processed_before_stop'] = len(results['descriptions']) + len(results['errors'])
            else:
                stats_summary['stopped_early'] = False
                stats_summary['processed_before_stop'] = 0

            return stats_summary

        except KeyboardInterrupt:
            print("\n\n⚠️  Получен KeyboardInterrupt во время обработки")
            stats = self.get_empty_stats()
            stats['balance_exceeded_during_processing'] = False
            stats['interrupted'] = True
            stats['keyboard_interrupt'] = True
            return stats

        except Exception as e:
            print(f'\n❌ Критическая ошибка в run_single_import: {str(e)[:100]}')
            if self.options.get('debug'):
                import traceback
                traceback.print_exc()

            stats = self.get_empty_stats()
            stats['balance_exceeded_during_processing'] = False
            stats['interrupted'] = True
            stats['exception'] = str(e)[:200]
            return stats

    def show_partial_stats(self, results):
        """Показывает частичную статистику при прерывании из-за лимита API"""
        total_time = time.time() - self.data_processor.stats['start']

        print('\n' + '⚠️' * 20)
        print('⚠️  ИМПОРТ ПРЕРВАН ИЗ-ЗА ЛИМИТА API!')
        print('=' * 50)

        # Метрики производительности
        total_processed = (self.data_processor.stats.get('found', 0) +
                           self.data_processor.stats.get('not_found_count', 0) +
                           self.data_processor.stats.get('errors', 0))

        if total_processed > 0:
            games_per_sec = total_processed / total_time
            cache_checks = self.data_processor.stats['cache_hits'] + self.data_processor.stats['cache_misses']
            cache_efficiency = (self.data_processor.stats['cache_hits'] / cache_checks * 100) if cache_checks > 0 else 0

            print(f'⏱️  Время работы: {total_time:.1f} сек')
            print(f'⚡ Скорость: {games_per_sec:.1f} игр/сек')
            print(f'🎯 Эффективность кэша: {cache_efficiency:.1f}%')

        # Статистика обработки
        print(f'📊 Успели обработать: {total_processed}')
        print(f'✅ Найдено: {self.data_processor.stats.get("found", 0)}')
        print(f'❓ Не найдено: {self.data_processor.stats.get("not_found_count", 0)}')
        print(f'💥 Ошибок: {self.data_processor.stats.get("errors", 0)}')

        if total_processed > 0:
            success_rate = (self.data_processor.stats.get('found', 0) / total_processed * 100)
            print(f'📈 Успешность: {success_rate:.1f}%')

        print(f'\n💡 Рекомендации:')
        print(f'   1. Подождите некоторое время (обычно 1 минута для rate limit)')
        print(f'   2. Проверьте баланс API ключа на сайте RAWG')
        print(f'   3. При следующем запуске будет продолжено с места остановки')

    def get_stats_summary(self, results):
        """Получает сводку статистики"""
        stats = {
            'total': self.data_processor.stats['total'],
            'updated': self.data_processor.stats['updated'],
            'errors': self.data_processor.stats['errors'],
            'new_not_found': self.data_processor.stats.get('new_not_found', 0),
            'cache_hits': self.data_processor.stats['cache_hits'],
            'cache_misses': self.data_processor.stats['cache_misses'],
            'found': self.data_processor.stats['found'],
            'short': self.data_processor.stats['short'],
            'empty': self.data_processor.stats['empty'],
            'not_found_count': self.data_processor.stats['not_found_count'],
            'total_processed': self.data_processor.stats['total'],
            'processing_time': time.time() - self.data_processor.stats['start'],
            'repeat_num': self.data_processor.stats['repeat_num']
        }

        # Добавляем информацию из results если есть
        if results.get('balance_exceeded', False):
            stats['api_limit_hit'] = True
        else:
            stats['api_limit_hit'] = False

        return stats

    def get_empty_stats(self):
        """Возвращает пустую статистику"""
        return {
            'total': 0,
            'updated': 0,
            'errors': 0,
            'new_not_found': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'found': 0,
            'short': 0,
            'empty': 0,
            'not_found_count': 0,
            'total_processed': 0,
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

    def run_single_import_batch(self, repeat_num, auto_offset=False, games=None):
        """Запускает импорт для конкретного батча игр"""
        if games is None:
            return self.run_single_import(repeat_num, auto_offset)

        # Проверяем прерывание
        if self.signal_handler and self.signal_handler.is_shutdown():
            print("⚠️  Прерывание запрошено, пропускаем импорт...")
            return self.get_empty_stats()

        # Инициализация
        self.data_processor.init_stats(repeat_num)
        self.data_processor.stats['total'] = len(games)

        # Обработка игр
        results = self.data_processor.process_games_batch(games)

        # Обработка результатов
        new_not_found = self.update_not_found_list(results, auto_offset)

        # Сохранение результатов
        updated_count = 0
        if not self.options.get('dry_run') and results['descriptions']:
            updated_count = self.data_processor.save_descriptions(results['descriptions'])

        self.data_processor.stats['updated'] = updated_count

        # Сохраняем статистику эффективности
        if self.cache_manager:
            self.cache_manager.save_efficiency_stats(self.data_processor.stats, repeat_num)

        # Возвращаем статистику
        return self.get_stats_summary(results)

    def update_not_found_list(self, results, auto_offset):
        """Обновляет список не найденных игр, возвращает новые"""
        new_not_found = set()
        if auto_offset and results['not_found']:
            new_not_found = set(results['not_found']) - self.not_found_ids
            if new_not_found:
                self.not_found_ids.update(new_not_found)
                self.data_processor.stats['new_not_found'] = len(new_not_found)
                self.show_new_not_found_info(new_not_found)

        return new_not_found

    def show_new_not_found_info(self, new_not_found):
        """Показывает информацию о новых ненайденных играх"""
        print(f'   🔍 Новых ненайденных игр: {len(new_not_found)}')

        if new_not_found:
            sample_size = min(3, len(new_not_found))
            sample_ids = list(new_not_found)[:sample_size]

            for igdb_id in sample_ids:
                try:
                    from games.models import Game
                    game = Game.objects.filter(igdb_id=igdb_id).first()
                    if game:
                        print(f'      • {igdb_id}: {game.name}')
                    else:
                        print(f'      • {igdb_id}: (не найдено в БД)')
                except:
                    print(f'      • {igdb_id}: (ошибка получения названия)')

    def show_start_info(self, games, repeat_num):
        """Показывает стартовую информацию"""
        print(f'\n🚀 [{repeat_num}] Начинаем импорт {len(games)} игр')
        print(f'⚡ Оптимизации: КЭШИРОВАНИЕ {"ВЫКЛ" if self.options.get("skip_cache") else "ВКЛ"}')
        print(f'👷 Потоков: {self.options.get("workers", 4)}')
        print(f'⏱️  Задержка: {self.options.get("delay", 0.5)} сек')
        print(f'📏 Мин. длина: {self.options.get("min_length", 1)}')
        print('─' * 50)

    def show_final_stats(self, results):
        """Показывает финальную статистику"""
        total_time = time.time() - self.data_processor.stats['start']

        print('\n' + '=' * 50)
        print(f'🏁 ИМПОРТ [{self.data_processor.stats["repeat_num"]}] ЗАВЕРШЕН!')

        # Метрики производительности
        if self.data_processor.stats['total'] > 0:
            games_per_sec = self.data_processor.stats['total'] / total_time
            cache_checks = self.data_processor.stats['cache_hits'] + self.data_processor.stats['cache_misses']
            cache_efficiency = (self.data_processor.stats['cache_hits'] / cache_checks * 100) if cache_checks > 0 else 0

            print(f'⏱️  Время: {total_time:.1f} сек')
            print(f'⚡ Скорость: {games_per_sec:.1f} игр/сек')
            print(f'🎯 Эффективность кэша: {cache_efficiency:.1f}%')

        # Статистика обработки
        print(f'📊 Всего игр: {self.data_processor.stats["total"]}')
        print(f'✅ Найдено: {self.data_processor.stats["found"]}')
        print(f'📏 Коротких: {self.data_processor.stats["short"]}')
        print(f'🚫 Пустых: {self.data_processor.stats["empty"]}')
        print(f'❓ Не найдено: {self.data_processor.stats["not_found_count"]}')
        print(f'💥 Ошибок: {self.data_processor.stats["errors"]}')

        if self.data_processor.stats['total'] > 0:
            success_rate = (self.data_processor.stats['found'] / self.data_processor.stats['total']) * 100
            print(f'📈 Успешность: {success_rate:.1f}%')

        if self.options.get('dry_run'):
            print('\n⚠️  DRY RUN: данные НЕ сохранены')
