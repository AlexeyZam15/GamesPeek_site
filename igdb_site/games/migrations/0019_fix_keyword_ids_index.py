# games/migrations/0019_fix_keyword_ids_index.py
"""
Миграция для исправления индекса на поле keyword_ids модели Game.
Заменяет прямой индекс на массиве на индекс по MD5-хешу массива,
чтобы избежать ошибки превышения размера строки индекса.
"""

from django.db import migrations, models
import django.contrib.postgres.fields


class Migration(migrations.Migration):
    """
    Исправление индекса для поля keyword_ids.
    PostgreSQL не может индексировать слишком большие массивы напрямую,
    поэтому создаем индекс на IMMUTABLE функции, возвращающей хеш массива.
    """

    dependencies = [
        ('games', '0018_game__cached_engine_count_game_engine_ids_gameengine_and_more'),
    ]

    operations = [
        # Создаем IMMUTABLE функцию для хеширования массива
        migrations.RunSQL(
            sql="""
                CREATE
                OR REPLACE FUNCTION immutable_array_md5(arr integer[])
            RETURNS text
            LANGUAGE sql
            IMMUTABLE
            AS $$
                SELECT md5(array_to_string(arr, ','));
                $$;
                """,
            reverse_sql='DROP FUNCTION IF EXISTS immutable_array_md5(integer[]);',
        ),

        # Удаляем старый проблемный индекс
        migrations.RunSQL(
            sql='DROP INDEX IF EXISTS games_game_keyword_ids_c070b389;',
            reverse_sql=migrations.RunSQL.noop,
        ),

        # Создаем новый индекс с использованием IMMUTABLE функции
        migrations.RunSQL(
            sql="""
                CREATE INDEX games_game_keyword_ids_md5_idx
                    ON games_game (immutable_array_md5(keyword_ids));
                """,
            reverse_sql='DROP INDEX IF EXISTS games_game_keyword_ids_md5_idx;',
        ),

        # Создаем GIN индекс для эффективного поиска по содержимому массива
        migrations.RunSQL(
            sql="""
                CREATE INDEX games_game_keyword_ids_gin_idx
                    ON games_game USING GIN (keyword_ids);
                """,
            reverse_sql='DROP INDEX IF EXISTS games_game_keyword_ids_gin_idx;',
        ),
    ]