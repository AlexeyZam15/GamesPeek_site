# games/migrations/0002_add_similar_and_platform_fields.py
from django.db import migrations, models
import django.contrib.postgres.fields
import django.contrib.postgres.indexes


class Migration(migrations.Migration):
    dependencies = [
        ('games', '0001_initial'),
    ]

    operations = [
        # Добавляем поле platform_ids
        migrations.AddField(
            model_name='game',
            name='platform_ids',
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.IntegerField(),
                blank=True,
                db_index=True,
                default=list,
                help_text='Materialized list of platform IDs for fast similarity search',
                size=None,
            ),
        ),
        # Добавляем поле similar_game_ids
        migrations.AddField(
            model_name='game',
            name='similar_game_ids',
            field=django.contrib.postgres.fields.ArrayField(
                base_field=models.IntegerField(),
                blank=True,
                db_index=True,
                default=list,
                help_text='Список ID игр, отсортированных по убыванию схожести (первые 12 самых похожих)',
                size=None,
            ),
        ),
        # Добавляем GIN индексы для новых полей
        migrations.AddIndex(
            model_name='game',
            index=django.contrib.postgres.indexes.GinIndex(
                fields=['platform_ids'],
                name='game_platform_ids_gin'
            ),
        ),
        migrations.AddIndex(
            model_name='game',
            index=django.contrib.postgres.indexes.GinIndex(
                fields=['similar_game_ids'],
                name='game_similar_game_ids_gin'
            ),
        ),
    ]