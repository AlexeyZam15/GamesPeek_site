"""Populate materialized vectors for existing games."""

from django.db import migrations
from django.db.models import Count, Prefetch


def populate_vectors(apps, schema_editor):
    """Populate genre_ids, keyword_ids, etc. for all games."""
    Game = apps.get_model('games', 'Game')

    batch_size = 500
    total = Game.objects.count()
    print(f"Starting population of {total} games...")

    updated_count = 0

    for offset in range(0, total, batch_size):
        # Get batch of games with all related data prefetched
        games = list(Game.objects.prefetch_related(
            'genres',
            'keywords',
            'themes',
            'player_perspectives',
            'developers',
            'game_modes'
        ).only('id')[offset:offset + batch_size])

        batch = []
        for game in games:
            # Get IDs from related objects
            genre_ids = list(game.genres.values_list('id', flat=True))
            keyword_ids = list(game.keywords.values_list('id', flat=True))
            theme_ids = list(game.themes.values_list('id', flat=True))
            perspective_ids = list(game.player_perspectives.values_list('id', flat=True))
            developer_ids = list(game.developers.values_list('id', flat=True))
            game_mode_ids = list(game.game_modes.values_list('id', flat=True))

            # Only update if actually changed
            if (set(genre_ids) != set(game.genre_ids or []) or
                    set(keyword_ids) != set(game.keyword_ids or []) or
                    set(theme_ids) != set(game.theme_ids or []) or
                    set(perspective_ids) != set(game.perspective_ids or []) or
                    set(developer_ids) != set(game.developer_ids or []) or
                    set(game_mode_ids) != set(game.game_mode_ids or [])):
                game.genre_ids = genre_ids
                game.keyword_ids = keyword_ids
                game.theme_ids = theme_ids
                game.perspective_ids = perspective_ids
                game.developer_ids = developer_ids
                game.game_mode_ids = game_mode_ids
                batch.append(game)

        if batch:
            Game.objects.bulk_update(batch, [
                'genre_ids', 'keyword_ids', 'theme_ids',
                'perspective_ids', 'developer_ids', 'game_mode_ids'
            ])
            updated_count += len(batch)

        print(f"Processed {offset + len(games)}/{total} games, updated {updated_count}")

    print(f"Completed! Updated {updated_count} games.")


def reverse_populate(apps, schema_editor):
    """Clear materialized vectors."""
    Game = apps.get_model('games', 'Game')
    updated = Game.objects.update(
        genre_ids=[],
        keyword_ids=[],
        theme_ids=[],
        perspective_ids=[],
        developer_ids=[],
        game_mode_ids=[]
    )
    print(f"Cleared vectors for {updated} games.")


class Migration(migrations.Migration):
    dependencies = [
        ('games', '0015_add_gin_indexes_for_arrays'),
    ]

    operations = [
        migrations.RunPython(populate_vectors, reverse_populate),
    ]