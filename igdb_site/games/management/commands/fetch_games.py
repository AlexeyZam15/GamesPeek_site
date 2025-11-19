from django.core.management.base import BaseCommand
from django.db import transaction
from games.igdb_api import make_igdb_request
from games.models import Game, Genre, Platform, Keyword
from datetime import datetime
from django.utils import timezone


class Command(BaseCommand):
    help = 'Fetch games from IGDB (MAXIMUM SPEED)'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=50, help='Number of games to fetch')

    def handle(self, *args, **options):
        limit = options['limit']
        self.stdout.write(f"🚀 SUPER-FAST fetch for {limit} games...")

        # ОДИН запрос для ВСЕГО
        data = f"""
        fields name, summary, storyline, rating, rating_count, first_release_date, 
               genres.name, platforms.name, cover.image_id, keywords.name;
        sort rating_count desc;
        where rating_count > 5 & rating > 70;
        limit {limit};
        """

        try:
            games_data = make_igdb_request('games', data)
            self.stdout.write(f"📥 Received {len(games_data)} games")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Failed: {e}'))
            return

        # Используем транзакцию для скорости
        with transaction.atomic():
            self.process_games_batch(games_data)

    def process_games_batch(self, games_data):
        """Обрабатывает все игры в одной транзакции"""
        # Собираем ВСЕ уникальные данные
        all_genres = {}
        all_platforms = {}
        all_keywords = {}

        # Проходим по играм и собираем данные
        for game_data in games_data:
            # Жанры
            for genre in game_data.get('genres', []):
                all_genres[genre['id']] = genre['name']
            # Платформы
            for platform in game_data.get('platforms', []):
                all_platforms[platform['id']] = platform['name']
            # Ключевые слова
            for keyword in game_data.get('keywords', []):
                all_keywords[keyword['id']] = keyword['name']

        # МАССОВОЕ создание жанров
        existing_genres = Genre.objects.filter(igdb_id__in=all_genres.keys())
        existing_genre_ids = {g.igdb_id: g for g in existing_genres}

        genres_to_create = [
            Genre(igdb_id=igdb_id, name=name)
            for igdb_id, name in all_genres.items()
            if igdb_id not in existing_genre_ids
        ]
        if genres_to_create:
            Genre.objects.bulk_create(genres_to_create)
            self.stdout.write(f"✅ Created {len(genres_to_create)} genres")

        # Обновляем мапу существующих жанров
        all_genre_objects = {**existing_genre_ids,
                             **{g.igdb_id: g for g in Genre.objects.filter(igdb_id__in=all_genres.keys())}}

        # МАССОВОЕ создание платформ
        existing_platforms = Platform.objects.filter(igdb_id__in=all_platforms.keys())
        existing_platform_ids = {p.igdb_id: p for p in existing_platforms}

        platforms_to_create = [
            Platform(igdb_id=igdb_id, name=name)
            for igdb_id, name in all_platforms.items()
            if igdb_id not in existing_platform_ids
        ]
        if platforms_to_create:
            Platform.objects.bulk_create(platforms_to_create)
            self.stdout.write(f"✅ Created {len(platforms_to_create)} platforms")

        all_platform_objects = {**existing_platform_ids,
                                **{p.igdb_id: p for p in Platform.objects.filter(igdb_id__in=all_platforms.keys())}}

        # МАССОВОЕ создание ключевых слов
        existing_keywords = Keyword.objects.filter(igdb_id__in=all_keywords.keys())
        existing_keyword_ids = {k.igdb_id: k for k in existing_keywords}

        keywords_to_create = [
            Keyword(igdb_id=igdb_id, name=name)
            for igdb_id, name in all_keywords.items()
            if igdb_id not in existing_keyword_ids
        ]
        if keywords_to_create:
            Keyword.objects.bulk_create(keywords_to_create)
            self.stdout.write(f"✅ Created {len(keywords_to_create)} keywords")

        all_keyword_objects = {**existing_keyword_ids,
                               **{k.igdb_id: k for k in Keyword.objects.filter(igdb_id__in=all_keywords.keys())}}

        # Теперь обрабатываем игры
        for i, game_data in enumerate(games_data, 1):
            self.process_single_game_fast(
                game_data, i, len(games_data),
                all_genre_objects, all_platform_objects, all_keyword_objects
            )

    def process_single_game_fast(self, game_data, current, total, genres_map, platforms_map, keywords_map):
        """Обрабатывает одну игру используя предзагруженные данные"""
        try:
            # Дата релиза
            first_release_date = None
            if game_data.get('first_release_date'):
                naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                first_release_date = timezone.make_aware(naive_datetime)

            # Создаем/обновляем игру
            game, created = Game.objects.update_or_create(
                igdb_id=game_data['id'],
                defaults={
                    'name': game_data.get('name', ''),
                    'summary': game_data.get('summary', ''),
                    'storyline': game_data.get('storyline', ''),
                    'rating': game_data.get('rating'),
                    'rating_count': game_data.get('rating_count', 0),
                    'first_release_date': first_release_date,
                    'cover_url': f"https://images.igdb.com/igdb/image/upload/t_cover_big/{game_data.get('cover', {}).get('image_id')}.jpg" if game_data.get(
                        'cover') else None
                }
            )

            # Добавляем связи МАССОВО
            game.genres.set([genres_map[g['id']] for g in game_data.get('genres', []) if g['id'] in genres_map])
            game.platforms.set(
                [platforms_map[p['id']] for p in game_data.get('platforms', []) if p['id'] in platforms_map])
            game.keywords.set([keywords_map[k['id']] for k in game_data.get('keywords', []) if k['id'] in keywords_map])

            status = "🆕" if created else "🔄"
            self.stdout.write(f"{status} [{current}/{total}] {game.name}")

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Error: {game_data.get("name", "Unknown")}: {e}'))