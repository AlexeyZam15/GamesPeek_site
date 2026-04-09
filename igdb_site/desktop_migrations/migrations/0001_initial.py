"""
Desktop-specific migration that creates ALL tables based on your actual database structure.
This migration does NOT use btree_gin extension.
Run this ONLY in desktop mode, NOT on your production server.
"""
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        # ============================================
        # МОДЕЛЬ: GamesKeywordcategory
        # ============================================
        migrations.CreateModel(
            name='GamesKeywordcategory',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('name', models.CharField(max_length=100, unique=True)),
                ('description', models.TextField()),
            ],
            options={
                'db_table': 'games_keywordcategory',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesGameengine
        # ============================================
        migrations.CreateModel(
            name='GamesGameengine',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=255)),
                ('description', models.TextField(blank=True, null=True)),
                ('logo_url', models.CharField(blank=True, max_length=500, null=True)),
                ('created_at', models.DateTimeField()),
                ('updated_at', models.DateTimeField()),
            ],
            options={
                'db_table': 'games_gameengine',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesGamemode
        # ============================================
        migrations.CreateModel(
            name='GamesGamemode',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=100)),
            ],
            options={
                'db_table': 'games_gamemode',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesGenre
        # ============================================
        migrations.CreateModel(
            name='GamesGenre',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=100)),
            ],
            options={
                'db_table': 'games_genre',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesPlatform
        # ============================================
        migrations.CreateModel(
            name='GamesPlatform',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=100)),
                ('slug', models.CharField(blank=True, max_length=50, null=True)),
            ],
            options={
                'db_table': 'games_platform',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesPlayerperspective
        # ============================================
        migrations.CreateModel(
            name='GamesPlayerperspective',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=100)),
            ],
            options={
                'db_table': 'games_playerperspective',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesTheme
        # ============================================
        migrations.CreateModel(
            name='GamesTheme',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=100)),
            ],
            options={
                'db_table': 'games_theme',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesKeyword
        # ============================================
        migrations.CreateModel(
            name='GamesKeyword',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=100)),
                ('cached_usage_count', models.IntegerField()),
                ('last_count_update', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField()),
                ('category', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING,
                                               to='desktop_migrations.gameskeywordcategory')),
            ],
            options={
                'db_table': 'games_keyword',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesCompany
        # ============================================
        migrations.CreateModel(
            name='GamesCompany',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=255)),
                ('description', models.TextField()),
                ('website', models.CharField(max_length=500)),
                ('start_date', models.DateTimeField(blank=True, null=True)),
                ('changed_at', models.DateTimeField(blank=True, null=True)),
                ('created_at', models.DateTimeField()),
                ('updated_at', models.DateTimeField()),
            ],
            options={
                'db_table': 'games_company',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesSeries
        # ============================================
        migrations.CreateModel(
            name='GamesSeries',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=255)),
                ('description', models.TextField(blank=True, null=True)),
                ('created_at', models.DateTimeField()),
                ('slug', models.CharField(blank=True, max_length=255, null=True, unique=True)),
                ('is_main_series', models.BooleanField()),
                ('parent_series',
                 models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING,
                                   to='desktop_migrations.gamesseries')),
            ],
            options={
                'db_table': 'games_series',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesGame
        # ============================================
        migrations.CreateModel(
            name='GamesGame',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('igdb_id', models.IntegerField(unique=True)),
                ('name', models.CharField(max_length=255)),
                ('summary', models.TextField(blank=True, null=True)),
                ('game_type', models.IntegerField(blank=True, null=True)),
                ('version_title', models.CharField(blank=True, max_length=255, null=True)),
                ('rawg_description', models.TextField(blank=True, null=True)),
                ('storyline', models.TextField(blank=True, null=True)),
                ('rating', models.FloatField(blank=True, null=True)),
                ('rating_count', models.IntegerField()),
                ('first_release_date', models.DateTimeField(blank=True, null=True)),
                ('series_order', models.IntegerField(blank=True, null=True)),
                ('cover_url', models.CharField(blank=True, max_length=500, null=True)),
                ('wiki_description', models.TextField(blank=True, null=True)),
                ('_cached_genre_count', models.IntegerField(blank=True, null=True)),
                ('_cached_keyword_count', models.IntegerField(blank=True, null=True)),
                ('_cached_platform_count', models.IntegerField(blank=True, null=True)),
                ('_cached_developer_count', models.IntegerField(blank=True, null=True)),
                ('_cache_updated_at', models.DateTimeField(blank=True, null=True)),
                ('last_analyzed_date', models.DateTimeField(blank=True, null=True)),
                ('date_added', models.DateTimeField()),
                ('updated_at', models.DateTimeField()),
                ('developer_ids', models.TextField()),
                ('game_mode_ids', models.TextField()),
                ('genre_ids', models.TextField()),
                ('keyword_ids', models.TextField()),
                ('perspective_ids', models.TextField()),
                ('theme_ids', models.TextField()),
                ('_cached_engine_count', models.IntegerField(blank=True, null=True)),
                ('engine_ids', models.TextField()),
                ('parent_game', models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING,
                                                  to='desktop_migrations.gamesgame')),
                ('version_parent',
                 models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.DO_NOTHING,
                                   related_name='gamesgame_version_parent_set', to='desktop_migrations.gamesgame')),
            ],
            options={
                'db_table': 'games_game',
                'managed': True,
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGameDevelopers
        # ============================================
        migrations.CreateModel(
            name='GamesGameDevelopers',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('company', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                              to='desktop_migrations.gamescompany')),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
            ],
            options={
                'db_table': 'games_game_developers',
                'managed': True,
                'unique_together': {('game', 'company')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGameEngines
        # ============================================
        migrations.CreateModel(
            name='GamesGameEngines',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
                ('gameengine', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                                 to='desktop_migrations.gamesgameengine')),
            ],
            options={
                'db_table': 'games_game_engines',
                'managed': True,
                'unique_together': {('game', 'gameengine')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGameGameModes
        # ============================================
        migrations.CreateModel(
            name='GamesGameGameModes',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
                ('gamemode', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                               to='desktop_migrations.gamesgamemode')),
            ],
            options={
                'db_table': 'games_game_game_modes',
                'managed': True,
                'unique_together': {('game', 'gamemode')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGameGenres
        # ============================================
        migrations.CreateModel(
            name='GamesGameGenres',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
                ('genre',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgenre')),
            ],
            options={
                'db_table': 'games_game_genres',
                'managed': True,
                'unique_together': {('game', 'genre')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGameKeywords
        # ============================================
        migrations.CreateModel(
            name='GamesGameKeywords',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
                ('keyword', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                              to='desktop_migrations.gameskeyword')),
            ],
            options={
                'db_table': 'games_game_keywords',
                'managed': True,
                'unique_together': {('game', 'keyword')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGamePlatforms
        # ============================================
        migrations.CreateModel(
            name='GamesGamePlatforms',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
                ('platform', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                               to='desktop_migrations.gamesplatform')),
            ],
            options={
                'db_table': 'games_game_platforms',
                'managed': True,
                'unique_together': {('game', 'platform')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGamePlayerPerspectives
        # ============================================
        migrations.CreateModel(
            name='GamesGamePlayerPerspectives',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
                ('playerperspective', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                                        to='desktop_migrations.gamesplayerperspective')),
            ],
            options={
                'db_table': 'games_game_player_perspectives',
                'managed': True,
                'unique_together': {('game', 'playerperspective')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGamePublishers
        # ============================================
        migrations.CreateModel(
            name='GamesGamePublishers',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('company', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                              to='desktop_migrations.gamescompany')),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
            ],
            options={
                'db_table': 'games_game_publishers',
                'managed': True,
                'unique_together': {('game', 'company')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGameSeries
        # ============================================
        migrations.CreateModel(
            name='GamesGameSeries',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
                ('series', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                             to='desktop_migrations.gamesseries')),
            ],
            options={
                'db_table': 'games_game_series',
                'managed': True,
                'unique_together': {('game', 'series')},
            },
        ),

        # ============================================
        # СВЯЗЬ: GamesGameThemes
        # ============================================
        migrations.CreateModel(
            name='GamesGameThemes',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
                ('theme',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamestheme')),
            ],
            options={
                'db_table': 'games_game_themes',
                'managed': True,
                'unique_together': {('game', 'theme')},
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesScreenshot
        # ============================================
        migrations.CreateModel(
            name='GamesScreenshot',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('url', models.CharField(max_length=500)),
                ('w', models.SmallIntegerField()),
                ('h', models.SmallIntegerField()),
                ('primary', models.BooleanField()),
                ('game',
                 models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING, to='desktop_migrations.gamesgame')),
            ],
            options={
                'db_table': 'games_screenshot',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesGamecardcache
        # ============================================
        migrations.CreateModel(
            name='GamesGamecardcache',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('rendered_card', models.TextField()),
                ('compressed_card', models.BinaryField(blank=True, null=True)),
                ('game_name', models.CharField(max_length=255)),
                ('game_rating', models.FloatField(blank=True, null=True)),
                ('game_cover_url', models.CharField(blank=True, max_length=500, null=True)),
                ('game_type', models.IntegerField(blank=True, null=True)),
                ('genres_json', models.JSONField()),
                ('platforms_json', models.JSONField()),
                ('perspectives_json', models.JSONField()),
                ('keywords_json', models.JSONField()),
                ('themes_json', models.JSONField()),
                ('game_modes_json', models.JSONField()),
                ('is_active', models.BooleanField()),
                ('cache_key', models.CharField(max_length=64, unique=True)),
                ('card_hash', models.CharField(max_length=64)),
                ('hit_count', models.IntegerField()),
                ('last_accessed', models.DateTimeField()),
                ('created_at', models.DateTimeField()),
                ('updated_at', models.DateTimeField()),
                ('template_version', models.CharField(max_length=10)),
                ('game', models.OneToOneField(on_delete=django.db.models.deletion.DO_NOTHING,
                                              to='desktop_migrations.gamesgame')),
            ],
            options={
                'db_table': 'games_gamecardcache',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesGamecountscache
        # ============================================
        migrations.CreateModel(
            name='GamesGamecountscache',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('genres_count', models.IntegerField()),
                ('keywords_count', models.IntegerField()),
                ('themes_count', models.IntegerField()),
                ('developers_count', models.IntegerField()),
                ('perspectives_count', models.IntegerField()),
                ('game_modes_count', models.IntegerField()),
                ('created_at', models.DateTimeField()),
                ('updated_at', models.DateTimeField()),
                ('game', models.OneToOneField(on_delete=django.db.models.deletion.DO_NOTHING,
                                              to='desktop_migrations.gamesgame')),
            ],
            options={
                'db_table': 'games_gamecountscache',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesFiltersectioncache
        # ============================================
        migrations.CreateModel(
            name='GamesFiltersectioncache',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('section_key', models.CharField(max_length=100, unique=True)),
                ('section_type', models.CharField(max_length=50)),
                ('rendered_html', models.TextField()),
                ('data_hash', models.CharField(max_length=64)),
                ('template_version', models.CharField(max_length=10)),
                ('is_active', models.BooleanField()),
                ('hit_count', models.IntegerField()),
                ('last_accessed', models.DateTimeField()),
                ('created_at', models.DateTimeField()),
                ('updated_at', models.DateTimeField()),
            ],
            options={
                'db_table': 'games_filtersectioncache',
                'managed': True,
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesGamesimilaritycache
        # ============================================
        migrations.CreateModel(
            name='GamesGamesimilaritycache',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('similarity_score', models.FloatField()),
                ('calculated_at', models.DateTimeField()),
                ('game1', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                            related_name='gamesimilaritycache_game1',
                                            to='desktop_migrations.gamesgame')),
                ('game2', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                            related_name='gamesimilaritycache_game2',
                                            to='desktop_migrations.gamesgame')),
            ],
            options={
                'db_table': 'games_gamesimilaritycache',
                'managed': True,
                'unique_together': {('game1', 'game2')},
            },
        ),

        # ============================================
        # МОДЕЛЬ: GamesGamesimilaritydetail
        # ============================================
        migrations.CreateModel(
            name='GamesGamesimilaritydetail',
            fields=[
                ('id', models.BigAutoField(primary_key=True, serialize=False)),
                ('common_genres', models.IntegerField()),
                ('common_keywords', models.IntegerField()),
                ('common_themes', models.IntegerField()),
                ('common_developers', models.IntegerField()),
                ('common_perspectives', models.IntegerField()),
                ('common_game_modes', models.IntegerField()),
                ('calculated_similarity', models.FloatField()),
                ('updated_at', models.DateTimeField()),
                ('source_game', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                                  related_name='similaritydetail_source',
                                                  to='desktop_migrations.gamesgame')),
                ('target_game', models.ForeignKey(on_delete=django.db.models.deletion.DO_NOTHING,
                                                  related_name='similaritydetail_target',
                                                  to='desktop_migrations.gamesgame')),
            ],
            options={
                'db_table': 'games_gamesimilaritydetail',
                'managed': True,
                'unique_together': {('source_game', 'target_game')},
            },
        ),
    ]