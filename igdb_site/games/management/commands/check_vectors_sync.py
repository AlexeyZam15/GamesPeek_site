"""Check if materialized vectors are in sync with actual relations."""

from django.core.management.base import BaseCommand
from django.db.models import Count, Prefetch
from games.models import Game
from collections import defaultdict


class Command(BaseCommand):
    help = 'Verify that materialized vectors match actual M2M relations'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            default=100,
            help='Number of games to check'
        )
        parser.add_argument(
            '--fix',
            action='store_true',
            help='Fix desynchronized vectors'
        )

    def handle(self, *args, **options):
        limit = options['limit']
        fix = options['fix']

        games = Game.objects.prefetch_related(
            'genres', 'keywords', 'themes',
            'player_perspectives', 'developers', 'game_modes'
        )[:limit]

        desynced = defaultdict(list)

        for game in games:
            actual_genres = set(game.genres.values_list('id', flat=True))
            actual_keywords = set(game.keywords.values_list('id', flat=True))
            actual_themes = set(game.themes.values_list('id', flat=True))
            actual_perspectives = set(game.player_perspectives.values_list('id', flat=True))
            actual_developers = set(game.developers.values_list('id', flat=True))
            actual_modes = set(game.game_modes.values_list('id', flat=True))

            stored_genres = set(game.genre_ids or [])
            stored_keywords = set(game.keyword_ids or [])
            stored_themes = set(game.theme_ids or [])
            stored_perspectives = set(game.perspective_ids or [])
            stored_developers = set(game.developer_ids or [])
            stored_modes = set(game.game_mode_ids or [])

            if actual_genres != stored_genres:
                desynced['genres'].append((game.id, game.name, stored_genres, actual_genres))

            if actual_keywords != stored_keywords:
                desynced['keywords'].append((game.id, game.name, len(stored_keywords), len(actual_keywords)))

            if actual_themes != stored_themes:
                desynced['themes'].append((game.id, game.name, stored_themes, actual_themes))

            if actual_perspectives != stored_perspectives:
                desynced['perspectives'].append((game.id, game.name, stored_perspectives, actual_perspectives))

            if actual_developers != stored_developers:
                desynced['developers'].append((game.id, game.name, stored_developers, actual_developers))

            if actual_modes != stored_modes:
                desynced['game_modes'].append((game.id, game.name, stored_modes, actual_modes))

        # Report desyncs
        total_desyncs = sum(len(v) for v in desynced.values())

        if total_desyncs == 0:
            self.stdout.write(self.style.SUCCESS("✓ All vectors are in sync!"))
        else:
            self.stdout.write(self.style.WARNING(f"Found {total_desyncs} desynchronized vectors:"))

            for field, items in desynced.items():
                self.stdout.write(f"  {field}: {len(items)} games")
                for item in items[:5]:  # Show first 5
                    self.stdout.write(f"    - Game {item[0]}: {item[1]}")

                if fix:
                    self.stdout.write(f"    → Fixing {field}...")
                    for game_id, _, _, _ in items[:10]:  # Fix first 10
                        game = Game.objects.get(id=game_id)
                        game.update_materialized_vectors(force=True)
                        self.stdout.write(f"      Fixed game {game_id}")

            if fix:
                self.stdout.write(self.style.SUCCESS("✓ Fix completed"))