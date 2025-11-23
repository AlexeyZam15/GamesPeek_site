from django.core.management.base import BaseCommand
from django.utils import timezone
from games.igdb_api import make_igdb_request, set_debug_mode
from games.models import Game, Genre, Keyword, Platform


class Command(BaseCommand):
    help = 'Поиск тактических RPG по жанру Tactical или ключевому слову tactical turn-based combat'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            default=200,
            help='Количество RPG для анализа'
        )
        parser.add_argument(
            '--min-rating',
            type=int,
            default=70,
            help='Минимальный рейтинг'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Включить режим отладки IGDB API'
        )

    def handle(self, *args, **options):
        limit = options['limit']
        min_rating = options['min_rating']
        debug = options['debug']

        # Устанавливаем режим отладки для IGDB API
        set_debug_mode(debug)

        self.stdout.write('🎮 ПОИСК ТАКТИЧЕСКИХ RPG')
        self.stdout.write('Критерии: жанр "Tactical" ИЛИ ключевое слово "tactical turn-based combat"')
        self.stdout.write('=' * 60)

        RPG_GENRE_ID = 12

        try:
            # 1. Загружаем популярные RPG игры
            self.stdout.write('📥 Загрузка популярных RPG игр...')

            query = f'''
                fields name,summary,genres,keywords,rating,rating_count;
                where genres = ({RPG_GENRE_ID}) & rating >= {min_rating};
                sort rating_count desc;
                limit {limit};
            '''.strip()

            rpg_games = make_igdb_request('games', query, debug=debug)
            self.stdout.write(f'📊 Загружено RPG игр: {len(rpg_games)}')

            if not rpg_games:
                self.stdout.write('❌ Не удалось загрузить RPG игры')
                return

            # 2. Ищем тактические RPG
            self.stdout.write('\n🔍 Поиск тактических RPG...')
            tactical_games = []

            for game in rpg_games:
                is_tactical = self.is_tactical_rpg(game, debug)
                if is_tactical:
                    tactical_games.append(game)
                    self.stdout.write(f'   ✅ Тактическая RPG: {game.get("name", "Unknown")}')

            self.stdout.write(f'\n🎯 Найдено тактических RPG: {len(tactical_games)}')

            if not tactical_games:
                self.stdout.write('❌ Тактические RPG не найдены')
                return

            # 3. Загружаем полную информацию о тактических RPG
            self.stdout.write('\n📥 Загрузка тактических RPG в базу...')
            loaded_count = 0

            for game_data in tactical_games:
                game_id = game_data.get('id')
                game_name = game_data.get('name', 'Unknown')

                if not game_id:
                    continue

                if not Game.objects.filter(igdb_id=game_id).exists():
                    self.stdout.write(f'   🔍 Загрузка: {game_name}')

                    full_query = f'''
                        fields name,summary,rating,rating_count,first_release_date,genres,keywords,platforms,cover;
                        where id = {game_id};
                    '''.strip()

                    try:
                        full_game_data = make_igdb_request('games', full_query, debug=debug)
                        if full_game_data:
                            result = self.process_game(full_game_data[0])
                            if result == 'created':
                                loaded_count += 1
                                self.stdout.write(f'   ✅ Загружена: {game_name}')
                            elif result == 'skipped':
                                self.stdout.write(f'   ⏭️  Уже в базе: {game_name}')
                    except Exception as e:
                        self.stderr.write(f'   ❌ Ошибка загрузки {game_name}: {e}')
                else:
                    self.stdout.write(f'   ⏭️  Уже в базе: {game_name}')

            # Итоги
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write(self.style.SUCCESS('✅ ПОИСК ЗАВЕРШЕН!'))
            self.stdout.write(f'• Проанализировано RPG: {len(rpg_games)}')
            self.stdout.write(f'• Найдено тактических RPG: {len(tactical_games)}')
            self.stdout.write(f'• Новых игр загружено: {loaded_count}')
            self.stdout.write(f'• Всего RPG в базе: {Game.objects.filter(genres__igdb_id=12).count()}')

            # Список найденных тактических RPG
            self.stdout.write('\n🎯 НАЙДЕННЫЕ ТАКТИЧЕСКИЕ RPG:')
            for i, game in enumerate(tactical_games, 1):
                self.stdout.write(f'   {i}. {game.get("name", "Unknown")}')

        except Exception as e:
            self.stderr.write(f'❌ Ошибка: {e}')
            import traceback
            self.stderr.write(traceback.format_exc())

    def is_tactical_rpg(self, game_data, debug=False):
        """Определяет, является ли игра тактической RPG"""

        # 1. Проверяем наличие жанра "Tactical"
        if 'genres' in game_data and game_data['genres']:
            genre_ids = game_data['genres']
            id_list = ','.join(map(str, genre_ids))
            genre_query = f'fields name; where id = ({id_list});'.strip()

            try:
                genres = make_igdb_request('genres', genre_query, debug=debug)
                for genre in genres:
                    genre_name = genre.get('name', '').lower()
                    if 'tactical' in genre_name:
                        if debug:
                            self.stdout.write(f'      ✅ Найден тактический жанр: {genre_name}')
                        return True
            except Exception as e:
                if debug:
                    self.stderr.write(f'   ⚠️ Ошибка загрузки жанров: {e}')

        # 2. Проверяем ключевые слова на "tactical turn-based combat"
        if 'keywords' in game_data and game_data['keywords']:
            keyword_ids = game_data['keywords']
            id_list = ','.join(map(str, keyword_ids))
            kw_query = f'fields name; where id = ({id_list});'.strip()

            try:
                keywords = make_igdb_request('keywords', kw_query, debug=debug)
                for keyword in keywords:
                    keyword_name = keyword.get('name', '').lower()
                    if 'tactical turn-based combat' in keyword_name:
                        if debug:
                            self.stdout.write(f'      ✅ Найдено ключевое слово: {keyword_name}')
                        return True
            except Exception as e:
                if debug:
                    self.stderr.write(f'   ⚠️ Ошибка загрузки ключевых слов: {e}')

        return False

    def process_game(self, game_data):
        """Обработать одну игру"""
        igdb_id = game_data['id']
        game_name = game_data.get('name', 'Unknown')

        try:
            if Game.objects.filter(igdb_id=igdb_id).exists():
                return 'skipped'

            game = Game(igdb_id=igdb_id)
            game.name = game_data.get('name', '')
            game.summary = game_data.get('summary', '')
            game.rating = game_data.get('rating')
            game.rating_count = game_data.get('rating_count', 0)

            if game_data.get('first_release_date'):
                from datetime import datetime
                naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                game.first_release_date = timezone.make_aware(naive_datetime)

            if game_data.get('cover'):
                cover_url = self.get_cover_url(game_data['cover'])
                if cover_url:
                    game.cover_url = cover_url

            game.save()

            if game_data.get('genres'):
                self.process_genres(game, game_data['genres'])

            if game_data.get('keywords'):
                self.process_keywords(game, game_data['keywords'])

            if game_data.get('platforms'):
                self.process_platforms(game, game_data['platforms'])

            return 'created'

        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки {game_name}: {e}')
            return 'error'

    def process_genres(self, game, genre_ids):
        genres = []
        for genre_id in genre_ids:
            try:
                genre, created = Genre.objects.get_or_create(
                    igdb_id=genre_id,
                    defaults={'name': f'Genre {genre_id}'}
                )
                if created or genre.name.startswith('Genre '):
                    genre_info = self.get_genre_info(genre_id)
                    if genre_info:
                        genre.name = genre_info['name']
                        genre.save()
                genres.append(genre)
            except Exception as e:
                self.stderr.write(f'❌ Ошибка обработки жанра {genre_id}: {e}')
        game.genres.set(genres)

    def process_keywords(self, game, keyword_ids):
        keywords = []
        for keyword_id in keyword_ids:
            try:
                keyword, created = Keyword.objects.get_or_create(
                    igdb_id=keyword_id,
                    defaults={'name': f'Keyword {keyword_id}'}
                )
                if created or keyword.name.startswith('Keyword '):
                    keyword_info = self.get_keyword_info(keyword_id)
                    if keyword_info:
                        keyword.name = keyword_info['name']
                        keyword.save()
                keywords.append(keyword)
            except Exception as e:
                self.stderr.write(f'❌ Ошибка обработки ключевого слова {keyword_id}: {e}')
        game.keywords.set(keywords)

    def process_platforms(self, game, platform_ids):
        platforms = []
        for platform_id in platform_ids:
            try:
                platform, created = Platform.objects.get_or_create(
                    igdb_id=platform_id,
                    defaults={'name': f'Platform {platform_id}'}
                )
                if created or platform.name.startswith('Platform '):
                    platform_info = self.get_platform_info(platform_id)
                    if platform_info:
                        platform.name = platform_info['name']
                        platform.save()
                platforms.append(platform)
            except Exception as e:
                self.stderr.write(f'❌ Ошибка обработки платформы {platform_id}: {e}')
        game.platforms.set(platforms)

    def get_genre_info(self, genre_id):
        try:
            query = f'fields name; where id = {genre_id};'.strip()
            result = make_igdb_request('genres', query)
            return result[0] if result else None
        except Exception as e:
            self.stderr.write(f'❌ Ошибка загрузки жанра {genre_id}: {e}')
            return None

    def get_keyword_info(self, keyword_id):
        try:
            query = f'fields name; where id = {keyword_id};'.strip()
            result = make_igdb_request('keywords', query)
            return result[0] if result else None
        except Exception as e:
            self.stderr.write(f'❌ Ошибка загрузки ключевого слова {keyword_id}: {e}')
            return None

    def get_platform_info(self, platform_id):
        try:
            query = f'fields name; where id = {platform_id};'.strip()
            result = make_igdb_request('platforms', query)
            return result[0] if result else None
        except Exception as e:
            self.stderr.write(f'❌ Ошибка загрузки платформы {platform_id}: {e}')
            return None

    def get_cover_url(self, cover_id):
        try:
            query = f'fields url; where id = {cover_id};'.strip()
            result = make_igdb_request('covers', query)
            if result and 'url' in result[0]:
                return f"https:{result[0]['url'].replace('thumb', 'cover_big')}"
        except Exception as e:
            self.stderr.write(f'❌ Ошибка загрузки обложки {cover_id}: {e}')
        return None