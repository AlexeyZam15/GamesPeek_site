# games/migrations/0008_add_indexes_for_similarity.py
from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ('games', '0007_alter_game_date_added'),
    ]

    operations = [
        # === ИНДЕКСЫ ДЛЯ ТАБЛИЦ СВЯЗЕЙ МНОГИЕ-КО-МНОГИМ ===

        # 1. Для таблицы games_game_genres (игры ↔ жанры)
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_genres_game_id
                ON games_game_genres(game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_genres_game_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_genres_genre_id
                ON games_game_genres(genre_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_genres_genre_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_genres_both
                ON games_game_genres(game_id, genre_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_genres_both;"
        ),

        # 2. Для таблицы games_game_keywords (игры ↔ ключевые слова)
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_keywords_game_id
                ON games_game_keywords(game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_keywords_game_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_keywords_keyword_id
                ON games_game_keywords(keyword_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_keywords_keyword_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_keywords_both
                ON games_game_keywords(game_id, keyword_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_keywords_both;"
        ),

        # 3. Для таблицы games_game_platforms (игры ↔ платформы)
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_platforms_game_id
                ON games_game_platforms(game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_platforms_game_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_platforms_platform_id
                ON games_game_platforms(platform_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_platforms_platform_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_platforms_both
                ON games_game_platforms(game_id, platform_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_platforms_both;"
        ),

        # 4. Для таблицы games_game_themes (игры ↔ темы)
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_themes_game_id
                ON games_game_themes(game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_themes_game_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_themes_theme_id
                ON games_game_themes(theme_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_themes_theme_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_themes_both
                ON games_game_themes(game_id, theme_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_themes_both;"
        ),

        # 5. Для таблицы games_game_game_modes (игры ↔ режимы игры)
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_modes_game_id
                ON games_game_game_modes(game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_modes_game_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_modes_mode_id
                ON games_game_game_modes(gamemode_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_modes_mode_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_modes_both
                ON games_game_game_modes(game_id, gamemode_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_modes_both;"
        ),

        # 6. Для таблицы games_game_player_perspectives (игры ↔ перспективы)
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_perspectives_game_id
                ON games_game_player_perspectives(game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_perspectives_game_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_perspectives_perspective_id
                ON games_game_player_perspectives(playerperspective_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_perspectives_perspective_id;"
        ),

        # 7. Для таблицы games_game_developers (игры ↔ разработчики)
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_developers_game_id
                ON games_game_developers(game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_developers_game_id;"
        ),
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_developers_company_id
                ON games_game_developers(company_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_developers_company_id;"
        ),

        # === СОСТАВНЫЕ ИНДЕКСЫ ДЛЯ ОПТИМИЗАЦИИ ПОХОЖЕСТИ ===

        # Индекс для быстрого поиска кандидатов по жанрам
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_genres_for_similarity
                ON games_game_genres(genre_id, game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_genres_for_similarity;"
        ),

        # Индекс для быстрого поиска кандидатов по ключевым словам
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_game_keywords_for_similarity
                ON games_game_keywords(keyword_id, game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_game_keywords_for_similarity;"
        ),

        # Индекс для таблицы keywords по популярности
        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_keywords_popularity
                ON games_keyword(cached_usage_count DESC);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_keywords_popularity;"
        ),

        # === ИНДЕКСЫ ДЛЯ ОСНОВНОЙ ТАБЛИЦЫ GAMES ===

        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_games_rating_combined
                ON games_game(rating_count DESC, rating DESC, game_type);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_games_rating_combined;"
        ),

        migrations.RunSQL(
            """
            CREATE INDEX IF NOT EXISTS idx_games_release_date
                ON games_game(first_release_date DESC, game_type);
            """,
            reverse_sql="DROP INDEX IF EXISTS idx_games_release_date;"
        ),
    ]