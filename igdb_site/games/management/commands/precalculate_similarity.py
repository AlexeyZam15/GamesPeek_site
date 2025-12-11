# management/commands/precalculate_similarity.py
from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Game, GameCountsCache, GameSimilarityCache
from games.similarity import GameSimilarity
import time
from django.db.models import Count


class Command(BaseCommand):
    help = 'Предварительно рассчитывает подсчеты и схожесть игр'

    def add_arguments(self, parser):
        parser.add_argument('--batch-size', type=int, default=100, help='Размер батча для обработки')
        parser.add_argument('--limit', type=int, default=None, help='Ограничить количество обрабатываемых игр')

    def handle(self, *args, **options):
        batch_size = options['batch_size']
        limit = options['limit']

        similarity_engine = GameSimilarity()

        # 1. Рассчитываем подсчеты для всех игр
        print("1. Расчет подсчетов элементов для игр...")
        games = Game.objects.all()
        if limit:
            games = games[:limit]

        total_games = games.count()
        print(f"Всего игр для обработки: {total_games}")

        processed = 0
        for i in range(0, total_games, batch_size):
            batch_games = games[i:i + batch_size]

            for game in batch_games:
                processed += 1

                # Получаем или создаем кэш подсчетов
                cache, created = GameCountsCache.objects.get_or_create(game=game)

                # Обновляем подсчеты
                cache.genres_count = game.genres.count()
                cache.keywords_count = game.keywords.count()
                cache.themes_count = game.themes.count()
                cache.developers_count = game.developers.count()
                cache.perspectives_count = game.player_perspectives.count()
                cache.game_modes_count = game.game_modes.count()
                cache.save()

                if processed % 100 == 0:
                    print(f"Обработано подсчетов: {processed}/{total_games}")

        print("Подсчеты элементов завершены!")

        # 2. Рассчитываем схожесть для популярных игр
        print("\n2. Расчет схожести для популярных игр...")

        # Берем топ N популярных игр как source
        popular_games = Game.objects.filter(rating_count__gt=100).order_by('-rating_count')[:50]

        source_processed = 0
        for source_game in popular_games:
            source_processed += 1
            print(f"\nОбработка исходной игры {source_processed}/{len(popular_games)}: {source_game.name}")

            # Получаем данные исходной игры
            source_data = similarity_engine._get_cached_game_data(source_game)

            # Берем игры-кандидаты (исключая саму себя)
            candidate_games = Game.objects.exclude(id=source_game.id)[:1000]

            candidate_processed = 0
            for target_game in candidate_games:
                candidate_processed += 1

                # Проверяем, есть ли уже кэш
                if GameSimilarityCache.objects.filter(source_game=source_game, target_game=target_game).exists():
                    continue

                # Подсчитываем общие элементы
                common_genres = len(set(source_data['genres']) & set(target_game.genres.values_list('id', flat=True)))
                common_keywords = len(
                    set(source_data['keywords']) & set(target_game.keywords.values_list('id', flat=True)))
                common_themes = len(set(source_data['themes']) & set(target_game.themes.values_list('id', flat=True)))
                common_developers = len(
                    set(source_data['developers']) & set(target_game.developers.values_list('id', flat=True)))
                common_perspectives = len(set(source_data['perspectives']) & set(
                    target_game.player_perspectives.values_list('id', flat=True)))
                common_game_modes = len(
                    set(source_data['game_modes']) & set(target_game.game_modes.values_list('id', flat=True)))

                # Получаем подсчеты target игры
                try:
                    target_counts = GameCountsCache.objects.get(game=target_game)
                except GameCountsCache.DoesNotExist:
                    # Если нет кэша, считаем на лету
                    target_counts = GameCountsCache(
                        game=target_game,
                        genres_count=target_game.genres.count(),
                        keywords_count=target_game.keywords.count(),
                        themes_count=target_game.themes.count(),
                        developers_count=target_game.developers.count(),
                        perspectives_count=target_game.player_perspectives.count(),
                        game_modes_count=target_game.game_modes.count(),
                    )
                    target_counts.save()

                # Рассчитываем схожесть
                similarity = similarity_engine.calculate_similarity(source_game, target_game)

                # Сохраняем в кэш
                GameSimilarityCache.objects.create(
                    source_game=source_game,
                    target_game=target_game,
                    common_genres=common_genres,
                    common_keywords=common_keywords,
                    common_themes=common_themes,
                    common_developers=common_developers,
                    common_perspectives=common_perspectives,
                    common_game_modes=common_game_modes,
                    target_genres_count=target_counts.genres_count,
                    target_keywords_count=target_counts.keywords_count,
                    target_themes_count=target_counts.themes_count,
                    target_developers_count=target_counts.developers_count,
                    target_perspectives_count=target_counts.perspectives_count,
                    target_game_modes_count=target_counts.game_modes_count,
                    calculated_similarity=similarity
                )

                if candidate_processed % 100 == 0:
                    print(f"  Кандидатов обработано: {candidate_processed}")

            print(f"  Игра {source_game.name} завершена, сохранено {candidate_processed} записей")

        print("\nПредварительный расчет завершен!")