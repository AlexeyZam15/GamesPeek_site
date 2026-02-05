# games/management/commands/load_games.py
from django.core.management.base import BaseCommand
from .load_igdb.game_loader import GameLoader


class Command(BaseCommand):
    """Команда для загрузки игр из IGDB"""

    help = 'Загрузка игр из IGDB с разными фильтрами'

    def add_arguments(self, parser):
        """Аргументы для команды load_games"""
        parser.add_argument('--game-modes', type=str, default='',
                            help='Загружать игры с указанными режимами (логика ИЛИ). Пример: "Battle Royale,Single player". Будет загружена САМАЯ ПОПУЛЯРНАЯ игра с ПЕРВЫМ указанным режимом')
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
                            help='Общий лимит загружаемых игр (0 - без общего лимита)')
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
                            help='Количество игр за одну итерацию')
        parser.add_argument('--clear-cache', action='store_true',
                            help='Очистить кэш проверенных игр перед началом')
        parser.add_argument('--reset-offset', action='store_true',
                            help='Сбросить сохраненный offset и начать с начала')
        parser.add_argument('--update-missing-data', action='store_true',
                            help='Обновить отсутствующие данные у существующих игр. Можно использовать с --game-names или без для обновления всех игр')
        # НОВЫЙ АРГУМЕНТ
        parser.add_argument('--update-covers', action='store_true',
                            help='Обновить только обложки у существующих игр')

    def handle(self, *args, **options):
        """Основной метод выполнения команды"""
        # Если используется --update-covers
        if options['update_covers']:
            self.stdout.write('🖼️  РЕЖИМ: ОБНОВЛЕНИЕ ОБЛОЖЕК')

            # Отключаем другие режимы
            options['overwrite'] = False
            options['count_only'] = False
            options['update_missing_data'] = False

            # Определяем, какие игры будут обновляться
            update_all_covers = not any([
                options['game_names'],
                options['game_modes'],
                options['genres'],
                options['description_contains'],
                options['keywords']
            ])

            if update_all_covers:
                self.stdout.write('🎮 Обновление обложек для ВСЕХ игр в базе')
            elif options['game_names']:
                self.stdout.write(f'🎮 Обновление обложек для указанных игр: {options["game_names"]}')
            elif options['game_modes']:
                self.stdout.write(f'🎮 Обновление обложек для игр с режимами: {options["game_modes"]}')
            elif options['genres']:
                self.stdout.write(f'🎮 Обновление обложек для игр с жанрами: {options["genres"]}')
            elif options['description_contains']:
                self.stdout.write(f'🎮 Обновление обложек для игр с текстом: {options["description_contains"]}')
            elif options['keywords']:
                self.stdout.write(f'🎮 Обновление обложек для игр с ключевыми словами: {options["keywords"]}')

            # Создаем экземпляр GameLoader и делегируем ему работу
            loader = GameLoader(self.stdout, self.stderr)
            loader.execute_command(options)
            return

        # Если используется --update-missing-data
        elif options['update_missing_data']:
            self.stdout.write('🔄 РЕЖИМ: ОБНОВЛЕНИЕ ОТСУТСТВУЮЩИХ ДАННЫХ')

            # Отключаем overwrite и count-only в этом режиме
            options['overwrite'] = False
            options['count_only'] = False

            # Определяем, какие игры будут обновляться
            update_all_games = not any([
                options['game_names'],
                options['game_modes'],
                options['genres'],
                options['description_contains'],
                options['keywords']
            ])

            if update_all_games:
                self.stdout.write('🎮 Обновление данных для ВСЕХ игр в базе')
            elif options['game_names']:
                self.stdout.write(f'🎮 Обновление данных для указанных игр: {options["game_names"]}')
            elif options['game_modes']:
                self.stdout.write(f'🎮 Обновление данных для игр с режимами: {options["game_modes"]}')
            elif options['genres']:
                self.stdout.write(f'🎮 Обновление данных для игр с жанрами: {options["genres"]}')
            elif options['description_contains']:
                self.stdout.write(f'🎮 Обновление данных для игр с текстом: {options["description_contains"]}')
            elif options['keywords']:
                self.stdout.write(f'🎮 Обновление данных для игр с ключевыми словами: {options["keywords"]}')

            # Устанавливаем специальный флаг для режима обновления всех игр
            options['update_all_games'] = update_all_games

        # Если используется --game-modes без update-missing-data
        elif options['game_modes']:
            self.stdout.write(f'🎮 РЕЖИМ ЗАГРУЗКИ ПО РЕЖИМАМ ИГРЫ: {options["game_modes"]}')

        # Если используется --game-names без update-missing-data
        elif options['game_names']:
            self.stdout.write(f'🎮 РЕЖИМ ЗАГРУЗКИ ПО ИМЕНАМ ИГР: {options["game_names"]}')
            # Принудительно устанавливаем однократное выполнение без лимитов
            options['repeat'] = -1
            options['limit'] = 0
            options['iteration_limit'] = 1000

        # Создаем экземпляр GameLoader и делегируем ему работу
        loader = GameLoader(self.stdout, self.stderr)
        loader.execute_command(options)
