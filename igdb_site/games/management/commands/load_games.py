# games/management/commands/load_games.py
from django.core.management.base import BaseCommand
from .load_igdb.game_loader import GameLoader


class Command(BaseCommand):
    """Команда для загрузки игр из IGDB"""

    help = 'Загрузка игр из IGDB с разными фильтрами'

    def add_arguments(self, parser):
        """Аргументы для команды load_games"""
        parser.add_argument('--game-names', type=str, default='',
                            help='Загружать самые популярные найденные игры по указанным именам (логика ИЛИ). Пример: "The Witcher,Cyberpunk"')
        parser.add_argument('--genres', type=str, default='',
                            help='Загружать игры с указанными жанрами (логика И между жанрами).')
        parser.add_argument('--description-contains', type=str, default='',
                            help='Загружать игры с указанным текстом в описании или названии')
        parser.add_argument('--overwrite', action='store_true',
                            help='Удалить существующие игры и загрузить заново')
        parser.add_argument('--debug', action='store_true',
                            help='Включить режим отладки')
        parser.add_argument('--limit', type=int, default=0,
                            help='Общий лимит загружаемых игр (0 - без общего лимита). ДЛЯ --game-names ЛИМИТЫ ОТКЛЮЧЕНЫ')
        parser.add_argument('--offset', type=int, default=0,
                            help='Пропустить указанное количество игр из результатов поиска')
        parser.add_argument('--min-rating-count', type=int, default=0,
                            help='Минимальное количество оценок для фильтрации')
        parser.add_argument('--keywords', type=str, default='',
                            help='Загружать игры с указанными ключевыми словами (логика И)')
        parser.add_argument('--count-only', action='store_true',
                            help='Только подсчитать количество НОВЫХ игр без сохранения')
        parser.add_argument('--repeat', type=int, default=0,
                            help='Количество повторений (0 = бесконечно, -1 = только один раз)')
        parser.add_argument('--game-types', type=str, default='0,1,2,4,5,8,9,10,11',
                            help='Типы игр для загрузки')
        parser.add_argument('--iteration-limit', type=int, default=100,
                            help='Количество игр за одну итерацию. ДЛЯ --game-names ИТЕРАЦИИ ОТКЛЮЧЕНЫ')
        parser.add_argument('--clear-cache', action='store_true',
                            help='Очистить кэш проверенных игр перед началом')
        parser.add_argument('--reset-offset', action='store_true',
                            help='Сбросить сохраненный offset и начать с начала')

    def handle(self, *args, **options):
        """Основной метод выполнения команды"""
        # Если используется --game-names, отключаем итерации и лимиты
        if options['game_names']:
            self.stdout.write('🎮 РЕЖИМ ЗАГРУЗКИ ПО ИМЕНАМ ИГР')
            self.stdout.write('📢 ЛИМИТЫ И ИТЕРАЦИИ ОТКЛЮЧЕНЫ')
            self.stdout.write('🔍 Будет загружено ВСЕ найденное по указанным именам')

            # Принудительно устанавливаем однократное выполнение без лимитов
            options['repeat'] = -1  # Только одна итерация
            options['limit'] = 0  # Без общего лимита
            options['iteration_limit'] = 1000  # Большой лимит для одной итерации

        # Создаем экземпляр GameLoader и делегируем ему работу
        loader = GameLoader(self.stdout, self.stderr)
        loader.execute_command(options)  # Не возвращаем результат

        # УДАЛЕНО: лишний вывод
        # if options['debug']:
        #     self.stdout.write('✅ Команда выполнена')
