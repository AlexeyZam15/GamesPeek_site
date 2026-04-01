# games/migrations/0020_add_cascade_delete_to_keywords.py
"""
Миграция для добавления каскадного удаления к внешним ключам keyword_ids.
"""

from django.db import migrations


class Migration(migrations.Migration):
    """
    Добавляет ON DELETE CASCADE к внешнему ключу keyword_id в таблице games_game_keywords.
    """

    atomic = False  # Отключаем транзакцию для возможности использования CONCURRENTLY

    dependencies = [
        ('games', '0019_fix_keyword_ids_index'),
    ]

    operations = [
        # Удаляем старый внешний ключ
        migrations.RunSQL(
            sql="""
            ALTER TABLE games_game_keywords 
            DROP CONSTRAINT IF EXISTS games_game_keywords_keyword_id_f6aebc17_fk_games_keyword_id;
            """,
            reverse_sql=migrations.RunSQL.noop,
        ),

        # Создаем новый с каскадным удалением
        migrations.RunSQL(
            sql="""
            ALTER TABLE games_game_keywords 
            ADD CONSTRAINT games_game_keywords_keyword_id_f6aebc17_fk_games_keyword_id 
            FOREIGN KEY (keyword_id) 
            REFERENCES games_keyword(id) 
            ON DELETE CASCADE 
            DEFERRABLE INITIALLY DEFERRED;
            """,
            reverse_sql="""
            ALTER TABLE games_game_keywords
            DROP CONSTRAINT IF EXISTS games_game_keywords_keyword_id_f6aebc17_fk_games_keyword_id;
            """,
        ),

        # Добавляем индекс (без CONCURRENTLY, так как это не в транзакции)
        migrations.RunSQL(
            sql="""
            CREATE INDEX IF NOT EXISTS games_game_keywords_keyword_game_idx 
            ON games_game_keywords (keyword_id, game_id);
            """,
            reverse_sql="DROP INDEX IF EXISTS games_game_keywords_keyword_game_idx;",
        ),
    ]