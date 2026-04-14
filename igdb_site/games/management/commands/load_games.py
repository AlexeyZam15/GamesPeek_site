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
        parser.add_argument('--update-covers', action='store_true',
                            help='Обновить только обложки у существующих игр')
        parser.add_argument('--restore-genre', type=str, default='',
                            help='Восстановить указанный жанр для всех игр, у которых он есть в IGDB (например, --restore-genre action)')
        parser.add_argument('--restore-theme', type=str, default='',
                            help='Восстановить указанную тему для всех игр, у которых она есть в IGDB (например, --restore-theme action)')

        parser.add_argument('--no-cache', action='store_true',
                            help='Отключить кэширование загрузки из БД')
        parser.add_argument('--cache-ttl', type=int, default=3600,
                            help='Время жизни кэша в секундах (по умолчанию 3600 - 1 час)')
        parser.add_argument('--clear-db-cache', action='store_true',
                            help='Очистить кэш данных из БД перед запуском')
        parser.add_argument('--restore-genres-themes', action='store_true',
                            help='Восстановить удаленные жанры и темы для всех игр в базе')

    def handle(self, *args, **options):
        """Основной метод выполнения команды"""
        from django.core.cache import cache
        from .load_igdb.game_cache import GameCacheManager

        options['use_cache'] = not options.get('no_cache', False)

        if options.get('clear_cache', False):
            self.stdout.write('\n🧹 ОЧИСТКА КЭША')
            self.stdout.write('=' * 50)

            try:
                cleared_count = GameCacheManager.clear_cache()
                self.stdout.write(f'   ✅ Кэш проверенных игр очищен: {cleared_count} записей')
            except Exception as e:
                self.stdout.write(f'   ⚠️ Ошибка очистки кэша проверенных игр: {e}')

            try:
                cache.delete("games_relations_cache")
                self.stdout.write('   ✅ Кэш relations очищен')
            except Exception as e:
                self.stdout.write(f'   ⚠️ Ошибка очистки кэша relations: {e}')

            self.stdout.write('=' * 50)

        if options.get('clear_db_cache', False):
            self.stdout.write('\n🧹 ОЧИСТКА КЭША БД')
            self.stdout.write('=' * 50)

            try:
                cleared_count = 0
                cache_keys = []

                try:
                    cache_keys = cache.keys("games_relations_*")
                    if cache_keys:
                        cache.delete_many(cache_keys)
                        cleared_count = len(cache_keys)
                except:
                    cache.clear()
                    cleared_count = -1

                if cleared_count == -1:
                    self.stdout.write('   ✅ Весь кэш БД очищен')
                elif cleared_count > 0:
                    self.stdout.write(f'   ✅ Удалено {cleared_count} записей кэша игр')
                else:
                    self.stdout.write('   📭 Кэш игр пуст')

            except Exception as e:
                self.stdout.write(f'   ❌ Ошибка очистки кэша: {e}')

            self.stdout.write('=' * 50)

            if not options.get('force', False):
                response = input('\nПродолжить выполнение команды? (y/n): ')
                if response.lower() != 'y':
                    self.stdout.write('⏹️ Команда отменена')
                    return

        if options.get('restore_genre'):
            genre_name = options['restore_genre'].strip()
            self.stdout.write(f'🎭 РЕЖИМ: ВОССТАНОВЛЕНИЕ ЖАНРА "{genre_name.upper()}"')
            self.stdout.write('=' * 60)

            loader = GameLoader(self.stdout, self.stderr)
            updated_count = loader.restore_genres_and_themes(options, options.get('debug', False),
                                                             target_genre=genre_name)

            self.stdout.write(f'\n✅ Восстановлено жанров: {updated_count} игр получили жанр "{genre_name}"')
            return

        if options.get('restore_theme'):
            theme_name = options['restore_theme'].strip()
            self.stdout.write(f'🎭 РЕЖИМ: ВОССТАНОВЛЕНИЕ ТЕМЫ "{theme_name.upper()}"')
            self.stdout.write('=' * 60)

            loader = GameLoader(self.stdout, self.stderr)
            updated_count = loader.restore_genres_and_themes(options, options.get('debug', False),
                                                             target_theme=theme_name)

            self.stdout.write(f'\n✅ Восстановлено тем: {updated_count} игр получили тему "{theme_name}"')
            return

        if options.get('restore_genres_themes', False):
            self.stdout.write('🎭 РЕЖИМ: ВОССТАНОВЛЕНИЕ ЖАНРОВ И ТЕМ')
            self.stdout.write('=' * 60)

            loader = GameLoader(self.stdout, self.stderr)
            updated_count = loader.restore_genres_and_themes(options, options.get('debug', False))

            self.stdout.write(f'\n✅ Восстановлено {updated_count} игр')
            return

        if options['update_covers']:
            self.stdout.write('🖼️  РЕЖИМ: ОБНОВЛЕНИЕ ОБЛОЖЕК')

            options['overwrite'] = False
            options['count_only'] = False
            options['update_missing_data'] = False

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

            loader = GameLoader(self.stdout, self.stderr)
            loader.execute_command(options)
            return

        elif options['update_missing_data']:
            self.stdout.write('🔄 РЕЖИМ: ОБНОВЛЕНИЕ ОТСУТСТВУЮЩИХ ДАННЫХ')

            options['overwrite'] = False
            options['count_only'] = False

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

            options['update_all_games'] = update_all_games

        elif options['game_modes']:
            self.stdout.write(f'🎮 РЕЖИМ ЗАГРУЗКИ ПО РЕЖИМАМ ИГРЫ: {options["game_modes"]}')

        elif options['game_names']:
            self.stdout.write(f'🎮 РЕЖИМ ЗАГРУЗКИ ПО ИМЕНАМ ИГР: {options["game_names"]}')
            options['repeat'] = -1
            options['limit'] = 0
            options['iteration_limit'] = 1000

        loader = GameLoader(self.stdout, self.stderr)
        loader.execute_command(options)
