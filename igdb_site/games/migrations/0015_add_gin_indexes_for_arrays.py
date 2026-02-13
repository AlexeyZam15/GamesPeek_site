"""Add GIN indexes for ArrayField similarity search."""

from django.db import migrations
from django.contrib.postgres.operations import CreateExtension


class Migration(migrations.Migration):
    """
    Migration to add GIN indexes for all ArrayField columns used in similarity search.
    Uses CONCURRENTLY to avoid locking the table on large databases.

    This migration must run AFTER the fields are added (0014_*).
    """

    atomic = False  # CRITICAL: CONCURRENTLY cannot run inside a transaction

    dependencies = [
        ('games', '0014_game_developer_ids_game_game_mode_ids_game_genre_ids_and_more'),
    ]

    operations = [
        # Install btree_gin extension if not exists (allows GIN indexes on integer arrays)
        CreateExtension('btree_gin'),

        # === GIN INDEXES FOR ARRAYFIELDS ===
        # genre_ids: for genre overlap searches
        migrations.RunSQL(
            sql="""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS games_game_genre_ids_gin
                    ON games_game USING GIN (genre_ids);
                """,
            reverse_sql="""
                        DROP INDEX CONCURRENTLY IF EXISTS games_game_genre_ids_gin;
                        """
        ),

        # keyword_ids: for keyword overlap searches
        migrations.RunSQL(
            sql="""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS games_game_keyword_ids_gin
                    ON games_game USING GIN (keyword_ids);
                """,
            reverse_sql="""
                        DROP INDEX CONCURRENTLY IF EXISTS games_game_keyword_ids_gin;
                        """
        ),

        # theme_ids: for theme overlap searches
        migrations.RunSQL(
            sql="""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS games_game_theme_ids_gin
                    ON games_game USING GIN (theme_ids);
                """,
            reverse_sql="""
                        DROP INDEX CONCURRENTLY IF EXISTS games_game_theme_ids_gin;
                        """
        ),

        # perspective_ids: for perspective overlap searches
        migrations.RunSQL(
            sql="""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS games_game_perspective_ids_gin
                    ON games_game USING GIN (perspective_ids);
                """,
            reverse_sql="""
                        DROP INDEX CONCURRENTLY IF EXISTS games_game_perspective_ids_gin;
                        """
        ),

        # developer_ids: for developer overlap searches
        migrations.RunSQL(
            sql="""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS games_game_developer_ids_gin
                    ON games_game USING GIN (developer_ids);
                """,
            reverse_sql="""
                        DROP INDEX CONCURRENTLY IF EXISTS games_game_developer_ids_gin;
                        """
        ),

        # game_mode_ids: for game mode overlap searches
        migrations.RunSQL(
            sql="""
                CREATE INDEX CONCURRENTLY IF NOT EXISTS games_game_game_mode_ids_gin
                    ON games_game USING GIN (game_mode_ids);
                """,
            reverse_sql="""
                        DROP INDEX CONCURRENTLY IF EXISTS games_game_game_mode_ids_gin;
                        """
        ),
    ]