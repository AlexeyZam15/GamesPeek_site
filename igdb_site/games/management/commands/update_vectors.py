# games/management/commands/update_vectors.py
from django.core.management.base import BaseCommand
from django.core.cache import cache
from django.db import connection
import time


class Command(BaseCommand):
    help = 'МАКСИМАЛЬНО БЫСТРОЕ обновление векторов одним SQL запросом'

    def handle(self, *args, **options):
        start_time = time.time()

        self.stdout.write(self.style.SUCCESS(f'\n{"=" * 60}'))
        self.stdout.write(self.style.SUCCESS('СУПЕР-БЫСТРОЕ ОБНОВЛЕНИЕ ВЕКТОРОВ'))
        self.stdout.write(self.style.SUCCESS(f'{"=" * 60}\n'))

        # Получаем статистику ДО обновления
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM games_game")
            total_games = cursor.fetchone()[0]

            cursor.execute("""
                           SELECT COUNT(CASE WHEN genre_ids = '{}' THEN 1 END)       as empty_genres,
                                  COUNT(CASE WHEN keyword_ids = '{}' THEN 1 END)     as empty_keywords,
                                  COUNT(CASE WHEN theme_ids = '{}' THEN 1 END)       as empty_themes,
                                  COUNT(CASE WHEN perspective_ids = '{}' THEN 1 END) as empty_perspectives,
                                  COUNT(CASE WHEN developer_ids = '{}' THEN 1 END)   as empty_developers,
                                  COUNT(CASE WHEN game_mode_ids = '{}' THEN 1 END)   as empty_game_modes,
                                  COUNT(CASE WHEN engine_ids = '{}' THEN 1 END)      as empty_engines
                           FROM games_game
                           """)
            empty_stats = cursor.fetchone()

            cursor.execute("""
                           SELECT COUNT(genre_ids)       as non_empty_genres,
                                  COUNT(keyword_ids)     as non_empty_keywords,
                                  COUNT(theme_ids)       as non_empty_themes,
                                  COUNT(perspective_ids) as non_empty_perspectives,
                                  COUNT(developer_ids)   as non_empty_developers,
                                  COUNT(game_mode_ids)   as non_empty_game_modes,
                                  COUNT(engine_ids)      as non_empty_engines
                           FROM games_game
                           WHERE genre_ids != '{}' OR 
                    keyword_ids != '{}' OR 
                    theme_ids != '{}' OR 
                    perspective_ids != '{}' OR 
                    developer_ids != '{}' OR 
                    game_mode_ids != '{}' OR 
                    engine_ids != '{}'
                           """)
            non_empty_stats = cursor.fetchone()

        self.stdout.write(self.style.WARNING(f'📊 Статистика ДО обновления:'))
        self.stdout.write(f'   Всего игр в базе: {total_games}')
        self.stdout.write(f'   Игр с непустыми векторами: {non_empty_stats[0] if non_empty_stats else 0}')
        self.stdout.write(f'   Пустые genre_ids: {empty_stats[0]}')
        self.stdout.write(f'   Пустые keyword_ids: {empty_stats[1]}')
        self.stdout.write(f'   Пустые theme_ids: {empty_stats[2]}')
        self.stdout.write(f'   Пустые perspective_ids: {empty_stats[3]}')
        self.stdout.write(f'   Пустые developer_ids: {empty_stats[4]}')
        self.stdout.write(f'   Пустые game_mode_ids: {empty_stats[5]}')
        self.stdout.write(f'   Пустые engine_ids: {empty_stats[6]}')

        self.stdout.write(self.style.WARNING(f'\n🔄 Начинаю обновление векторов...'))

        with connection.cursor() as cursor:
            # Один гигантский запрос, который обновляет всё сразу
            cursor.execute("""
                           WITH genre_agg AS (SELECT game_id, array_agg(genre_id ORDER BY genre_id) as genre_ids
                                              FROM games_game_genres
                                              GROUP BY game_id),
                                keyword_agg AS (SELECT game_id, array_agg(keyword_id ORDER BY keyword_id) as keyword_ids
                                                FROM games_game_keywords
                                                GROUP BY game_id),
                                theme_agg AS (SELECT game_id, array_agg(theme_id ORDER BY theme_id) as theme_ids
                                              FROM games_game_themes
                                              GROUP BY game_id),
                                perspective_agg
                                    AS (SELECT game_id, array_agg(playerperspective_id ORDER BY playerperspective_id) as perspective_ids
                                        FROM games_game_player_perspectives
                                        GROUP BY game_id),
                                developer_agg
                                    AS (SELECT game_id, array_agg(company_id ORDER BY company_id) as developer_ids
                                        FROM games_game_developers
                                        GROUP BY game_id),
                                gamemode_agg
                                    AS (SELECT game_id, array_agg(gamemode_id ORDER BY gamemode_id) as game_mode_ids
                                        FROM games_game_game_modes
                                        GROUP BY game_id),
                                engine_agg
                                    AS (SELECT game_id, array_agg(gameengine_id ORDER BY gameengine_id) as engine_ids
                                        FROM games_game_engines
                                        GROUP BY game_id)
                           UPDATE games_game g
                           SET genre_ids       = COALESCE(ga.genre_ids, ARRAY[]::integer[]),
                               keyword_ids     = COALESCE(ka.keyword_ids, ARRAY[]::integer[]),
                               theme_ids       = COALESCE(ta.theme_ids, ARRAY[]::integer[]),
                               perspective_ids = COALESCE(pa.perspective_ids, ARRAY[]::integer[]),
                               developer_ids   = COALESCE(da.developer_ids, ARRAY[]::integer[]),
                               game_mode_ids   = COALESCE(gma.game_mode_ids, ARRAY[]::integer[]),
                               engine_ids      = COALESCE(ea.engine_ids, ARRAY[]::integer[]) FROM games_game g2
                LEFT JOIN genre_agg ga
                           ON g2.id = ga.game_id
                               LEFT JOIN keyword_agg ka ON g2.id = ka.game_id
                               LEFT JOIN theme_agg ta ON g2.id = ta.game_id
                               LEFT JOIN perspective_agg pa ON g2.id = pa.game_id
                               LEFT JOIN developer_agg da ON g2.id = da.game_id
                               LEFT JOIN gamemode_agg gma ON g2.id = gma.game_id
                               LEFT JOIN engine_agg ea ON g2.id = ea.game_id
                           WHERE g.id = g2.id
                           """)

            updated = cursor.rowcount

        # Очищаем кэш
        cache.clear()

        # Получаем статистику ПОСЛЕ обновления
        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT COUNT(CASE WHEN genre_ids != '{}' THEN 1 END)       as non_empty_genres,
                                  COUNT(CASE WHEN keyword_ids != '{}' THEN 1 END)     as non_empty_keywords,
                                  COUNT(CASE WHEN theme_ids != '{}' THEN 1 END)       as non_empty_themes,
                                  COUNT(CASE WHEN perspective_ids != '{}' THEN 1 END) as non_empty_perspectives,
                                  COUNT(CASE WHEN developer_ids != '{}' THEN 1 END)   as non_empty_developers,
                                  COUNT(CASE WHEN game_mode_ids != '{}' THEN 1 END)   as non_empty_game_modes,
                                  COUNT(CASE WHEN engine_ids != '{}' THEN 1 END)      as non_empty_engines
                           FROM games_game
                           """)
            after_stats = cursor.fetchone()

        total_time = time.time() - start_time

        self.stdout.write(self.style.SUCCESS(f'\n✅ Обновлено записей: {updated}'))
        self.stdout.write(self.style.SUCCESS(f'✅ Кэш очищен'))
        self.stdout.write(self.style.SUCCESS(f'⏱️  Время выполнения: {total_time:.2f} сек'))

        self.stdout.write(self.style.WARNING(f'\n📊 Статистика ПОСЛЕ обновления:'))
        self.stdout.write(f'   Непустые genre_ids: {after_stats[0]}')
        self.stdout.write(f'   Непустые keyword_ids: {after_stats[1]}')
        self.stdout.write(f'   Непустые theme_ids: {after_stats[2]}')
        self.stdout.write(f'   Непустые perspective_ids: {after_stats[3]}')
        self.stdout.write(f'   Непустые developer_ids: {after_stats[4]}')
        self.stdout.write(f'   Непустые game_mode_ids: {after_stats[5]}')
        self.stdout.write(f'   Непустые engine_ids: {after_stats[6]}')

        # Проверка консистентности
        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT COUNT(*)                                              as games_with_genres,
                                  (SELECT COUNT(*) FROM games_game_genres)              as total_genre_relations,
                                  COUNT(*)                                              as games_with_keywords,
                                  (SELECT COUNT(*) FROM games_game_keywords)            as total_keyword_relations,
                                  COUNT(*)                                              as games_with_themes,
                                  (SELECT COUNT(*) FROM games_game_themes)              as total_theme_relations,
                                  COUNT(*)                                              as games_with_perspectives,
                                  (SELECT COUNT(*) FROM games_game_player_perspectives) as total_perspective_relations,
                                  COUNT(*)                                              as games_with_developers,
                                  (SELECT COUNT(*) FROM games_game_developers)          as total_developer_relations,
                                  COUNT(*)                                              as games_with_game_modes,
                                  (SELECT COUNT(*) FROM games_game_game_modes)          as total_gamemode_relations,
                                  COUNT(*)                                              as games_with_engines,
                                  (SELECT COUNT(*) FROM games_game_engines)             as total_engine_relations
                           FROM games_game
                           WHERE genre_ids != '{}' OR 
                    keyword_ids != '{}' OR 
                    theme_ids != '{}' OR 
                    perspective_ids != '{}' OR 
                    developer_ids != '{}' OR 
                    game_mode_ids != '{}' OR 
                    engine_ids != '{}'
                           """)
            consistency = cursor.fetchone()

        self.stdout.write(self.style.WARNING(f'\n🔍 Проверка консистентности:'))
        self.stdout.write(f'   Жанры: {consistency[0]} игр имеют {consistency[1]} связей')
        self.stdout.write(f'   Ключевые слова: {consistency[2]} игр имеют {consistency[3]} связей')
        self.stdout.write(f'   Темы: {consistency[4]} игр имеют {consistency[5]} связей')
        self.stdout.write(f'   Перспективы: {consistency[6]} игр имеют {consistency[7]} связей')
        self.stdout.write(f'   Разработчики: {consistency[8]} игр имеют {consistency[9]} связей')
        self.stdout.write(f'   Режимы игры: {consistency[10]} игр имеют {consistency[11]} связей')
        self.stdout.write(f'   Движки: {consistency[12]} игр имеют {consistency[13]} связей')

        self.stdout.write(self.style.SUCCESS(f'\n{"=" * 60}'))
        self.stdout.write(self.style.SUCCESS('ОБНОВЛЕНИЕ ЗАВЕРШЕНО'))
        self.stdout.write(self.style.SUCCESS(f'{"=" * 60}\n'))