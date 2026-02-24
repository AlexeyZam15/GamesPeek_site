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
                               engine_ids      = COALESCE(ea.engine_ids, ARRAY[]::integer[]) FROM 
                    games_game g2
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

        total_time = time.time() - start_time

        self.stdout.write(self.style.SUCCESS(f'✅ Обновлено игр: {updated}'))
        self.stdout.write(self.style.SUCCESS(f'✅ Кэш очищен'))
        self.stdout.write(self.style.SUCCESS(f'⏱️  Время: {total_time:.2f} сек'))
        self.stdout.write(self.style.SUCCESS(f'{"=" * 60}\n'))