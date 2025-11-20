# games/management/commands/precalculate_similarity.py
from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Game, GameSimilarityCache
from games.similarity import GameSimilarity
from django.utils import timezone


class Command(BaseCommand):
    help = 'Precalculate game similarities for faster search'

    def add_arguments(self, parser):
        parser.add_argument('--batch-size', type=int, default=50, help='Number of games to process per batch')
        parser.add_argument('--top-n', type=int, default=30, help='Number of similar games to cache per game')
        parser.add_argument('--min-similarity', type=int, default=15, help='Minimum similarity score to cache')

    def handle(self, *args, **options):
        batch_size = options['batch_size']
        top_n = options['top_n']
        min_similarity = options['min_similarity']

        # Берем популярные игры
        games = Game.objects.filter(rating_count__gt=5).order_by('-rating_count')[:batch_size]
        total_games = games.count()
        similarity_engine = GameSimilarity()

        self.stdout.write(f"🎮 Precalculating similarities for {total_games} popular games...")

        processed = 0
        for game in games:
            try:
                # Находим похожие игры (возвращает список словарей)
                similar_games_data = similarity_engine.find_similar_games(
                    game,
                    limit=top_n,
                    min_similarity=min_similarity
                )

                self.stdout.write(f"📊 {game.name}: found {len(similar_games_data)} similar games")

                # Сохраняем в кэш
                cache_created = 0
                with transaction.atomic():
                    for similar_data in similar_games_data:
                        # similar_data - это словарь с ключами: 'game', 'similarity', etc.
                        similar_game = similar_data['game']
                        similarity_score = similar_data['similarity']

                        if similarity_score >= min_similarity:
                            GameSimilarityCache.objects.update_or_create(
                                game1=game,
                                game2=similar_game,
                                defaults={'similarity_score': similarity_score}
                            )
                            cache_created += 1

                processed += 1
                self.stdout.write(f"✅ {game.name}: {cache_created} similar games cached")

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Error processing {game.name}: {e}"))
                continue

        self.stdout.write(
            self.style.SUCCESS(f"🎉 Game similarities precalculation completed! Processed {processed} games"))

        # Статистика
        total_cached = GameSimilarityCache.objects.count()
        self.stdout.write(f"📈 Total cached similarities: {total_cached}")