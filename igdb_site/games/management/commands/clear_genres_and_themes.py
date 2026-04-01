# Создайте файл: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\management\commands\clear_genres_and_themes.py

from django.core.management.base import BaseCommand
from django.db import connection, transaction


class Command(BaseCommand):
    """
    Django management command to clear all genres and themes from all games
    with maximum performance using raw SQL and batch processing.
    """

    help = 'Clears all genres and themes from all games (optimized)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=10000,
            help='Batch size for processing games (default: 10000)'
        )

        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be cleared without actually clearing'
        )

    def handle(self, *args, **options):
        batch_size = options['batch_size']
        dry_run = options['dry_run']

        self.stdout.write(self.style.WARNING('Starting to clear genres and themes...'))

        with connection.cursor() as cursor:
            # Get counts
            cursor.execute("""
                SELECT COUNT(DISTINCT game_id) as games_with_genres,
                       COUNT(DISTINCT game_id) as games_with_themes
                FROM (
                    SELECT game_id FROM games_game_genres
                    UNION
                    SELECT game_id FROM games_game_themes
                ) as games_with_relations
            """)

            result = cursor.fetchone()
            games_with_genres = result[0] if result else 0
            games_with_themes = result[1] if result else 0
            total_games = max(games_with_genres, games_with_themes)

            self.stdout.write(f'Games with genres: {games_with_genres}')
            self.stdout.write(f'Games with themes: {games_with_themes}')
            self.stdout.write(f'Total games with relations: {total_games}')

            if dry_run:
                self.stdout.write(self.style.SUCCESS('Dry run completed - no changes were made'))
                return

            if total_games == 0:
                self.stdout.write(self.style.SUCCESS('No games have genres or themes to clear'))
                return

            # Get all game IDs with relations
            cursor.execute("""
                SELECT DISTINCT game_id 
                FROM (
                    SELECT game_id FROM games_game_genres
                    UNION
                    SELECT game_id FROM games_game_themes
                ) as games_with_relations
            """)

            game_ids = [row[0] for row in cursor.fetchall()]

            self.stdout.write(f'Found {len(game_ids)} games to process')

            # Process in batches with raw SQL
            total_cleared = 0
            cleared_genres = 0
            cleared_themes = 0

            for i in range(0, len(game_ids), batch_size):
                batch_ids = game_ids[i:i + batch_size]
                ids_placeholder = ','.join(['%s'] * len(batch_ids))

                with transaction.atomic():
                    # Clear genres many-to-many table
                    cursor.execute(
                        f"""
                        DELETE FROM games_game_genres 
                        WHERE game_id IN ({ids_placeholder})
                        """,
                        batch_ids
                    )
                    cleared_genres += cursor.rowcount

                    # Clear themes many-to-many table
                    cursor.execute(
                        f"""
                        DELETE FROM games_game_themes 
                        WHERE game_id IN ({ids_placeholder})
                        """,
                        batch_ids
                    )
                    cleared_themes += cursor.rowcount

                    # Clear materialized vectors and update cache timestamp
                    cursor.execute(
                        f"""
                        UPDATE games_game 
                        SET genre_ids = '{{}}'::integer[],
                            theme_ids = '{{}}'::integer[],
                            _cached_genre_count = 0,
                            _cache_updated_at = NOW()
                        WHERE id IN ({ids_placeholder})
                        """,
                        batch_ids
                    )

                    total_cleared += len(batch_ids)

                self.stdout.write(f'Processed {total_cleared} of {len(game_ids)} games')

            self.stdout.write(self.style.SUCCESS(
                f'Successfully cleared {cleared_genres} genre relations and '
                f'{cleared_themes} theme relations from {total_cleared} games'
            ))