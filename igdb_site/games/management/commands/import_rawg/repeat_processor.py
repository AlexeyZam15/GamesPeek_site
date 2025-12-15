# games/management/commands/import_rawg/repeat_processor.py
import time
from .game_fetcher import GameFetcher


class RepeatProcessor:
    """Класс для обработки повторных импортов"""

    def __init__(self, import_processor, options):
        self.import_processor = import_processor
        self.options = options
        self.global_stats = {
            'total_repeats': 0,
            'completed_repeats': 0,
            'total_games_processed': 0,
            'total_games_updated': 0,
            'total_errors': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'start_time': time.time()
        }

    def execute_infinite_repeats(self, auto_offset, batch_size):
        """Выполняет бесконечные повторения до обработки всех игр"""
        offset = self.options.get('offset', 0)
        repeat_num = 1
        total_processed_games = 0

        # Получаем общее количество игр
        game_fetcher = GameFetcher(self.options, self.import_processor.not_found_ids)
        total_to_process = game_fetcher.get_total_games_to_process(auto_offset)

        if total_to_process == 0:
            print('🎉 Нет игр для обработки!')
            return

        print(f'🎯 Всего игр для обработки: {total_to_process:,}')
        print(f'📦 Батч: {batch_size} игр за повтор')

        while True:
            # Проверяем флаг прерывания
            if self.import_processor.data_processor.stats.get('balance_exceeded', False):
                print("\n🚫 Превышен лимит API - остановка")
                break

            print(f'\n{"=" * 50}')
            print(f'🚀 ПОВТОРЕНИЕ {repeat_num} (бесконечный режим)')
            print(f'📦 Батч: {batch_size} игр, offset: {offset}')

            # Обновляем опции для текущего батча
            current_options = self.options.copy()
            current_options['limit'] = batch_size
            current_options['offset'] = offset
            self.import_processor.options = current_options

            # Запускаем импорт
            repeat_stats = self.import_processor.run_single_import(repeat_num, auto_offset)

            # Обновляем глобальную статистику
            self.update_global_stats(repeat_stats)

            games_processed_in_batch = repeat_stats.get('total', 0)
            total_processed_games += games_processed_in_batch
            offset += games_processed_in_batch

            # Показываем прогресс
            progress = (total_processed_games / total_to_process * 100) if total_to_process > 0 else 0
            print(f'📊 ПРОГРЕСС: {total_processed_games:,}/{total_to_process:,} игр ({progress:.1f}%)')

            if games_processed_in_batch == 0:
                print('\n🎉 Все игры обработаны!')
                break

            if total_processed_games >= total_to_process:
                print('\n🎉 Все запланированные игры обработаны!')
                break

            # Пауза между повторами
            repeat_delay = self.options.get('repeat_delay', 60.0)
            print(f'\n⏳ Пауза {repeat_delay} секунд...')
            time.sleep(repeat_delay)

            repeat_num += 1

        self.show_final_stats()

    def execute_limited_repeats(self, repeat_times, auto_offset):
        """Выполняет ограниченное количество повторений"""
        save_on_interrupt = self.options.get('save_on_interrupt', True)

        for repeat_num in range(1, repeat_times + 1):
            print(f'\n{"=" * 50}')
            print(f'🚀 ПОВТОРЕНИЕ {repeat_num}/{repeat_times}')

            # Запускаем импорт
            repeat_stats = self.import_processor.run_single_import(repeat_num, auto_offset)

            # Обновляем глобальную статистику
            self.update_global_stats(repeat_stats)

            if repeat_num < repeat_times:
                repeat_delay = self.options.get('repeat_delay', 60.0)
                print(f'\n⏳ Пауза {repeat_delay} секунд...')
                time.sleep(repeat_delay)

        self.show_final_stats()

    def update_global_stats(self, repeat_stats):
        """Обновляет глобальную статистику"""
        self.global_stats['completed_repeats'] += 1
        self.global_stats['total_games_processed'] += repeat_stats.get('total', 0)
        self.global_stats['total_games_updated'] += repeat_stats.get('updated', 0)
        self.global_stats['total_errors'] += repeat_stats.get('errors', 0)
        self.global_stats['cache_hits'] += repeat_stats.get('cache_hits', 0)
        self.global_stats['cache_misses'] += repeat_stats.get('cache_misses', 0)

    def show_final_stats(self):
        """Показывает финальную глобальную статистику"""
        total_time = time.time() - self.global_stats['start_time']

        print('\n' + '🎉' * 20)
        print('🏆 ВСЕ ПОВТОРЕНИЯ ЗАВЕРШЕНЫ!')
        print('=' * 50)

        print(f'📊 ГЛОБАЛЬНАЯ СТАТИСТИКА:')
        print(f'   🔁 Выполнено повторов: {self.global_stats["completed_repeats"]}')
        print(f'   ⏱️  Общее время: {total_time:.1f} сек')
        print(f'   📈 Обработано игр: {self.global_stats["total_games_processed"]:,}')
        print(f'   ✅ Обновлено описаний: {self.global_stats["total_games_updated"]:,}')
        print(f'   💾 Попаданий в кэш: {self.global_stats["cache_hits"]:,}')
        print(f'   💥 Всего ошибок: {self.global_stats["total_errors"]}')

        if self.global_stats['total_games_processed'] > 0:
            overall_speed = self.global_stats['total_games_processed'] / total_time
            cache_efficiency = (self.global_stats['cache_hits'] / self.global_stats['total_games_processed'] * 100)

            print(f'\n📈 ЭФФЕКТИВНОСТЬ:')
            print(f'   ⚡ Общая скорость: {overall_speed:.1f} игр/сек')
            print(f'   💾 Общая эффективность кэша: {cache_efficiency:.1f}%')