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

        self._check_and_clear_locks()

        # КРИТИЧЕСКИ ВАЖНО: увеличиваем work_mem для этой сессии
        with connection.cursor() as cursor:
            cursor.execute("SET work_mem = '1GB'")
            cursor.execute("SET maintenance_work_mem = '2GB'")
            self.stdout.write(self.style.WARNING('   ⚙️ work_mem = 1GB, maintenance_work_mem = 2GB'))

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

        self.stdout.write(self.style.WARNING(f'\n📊 Статистика ДО обновления:'))
        self.stdout.write(f'   Всего игр в базе: {total_games:,}')
        self.stdout.write(f'   Игр с непустыми векторами: {non_empty_stats[0] if non_empty_stats else 0:,}')
        self.stdout.write(f'   Пустые genre_ids: {empty_stats[0]:,}')
        self.stdout.write(f'   Пустые keyword_ids: {empty_stats[1]:,}')
        self.stdout.write(f'   Пустые theme_ids: {empty_stats[2]:,}')
        self.stdout.write(f'   Пустые perspective_ids: {empty_stats[3]:,}')
        self.stdout.write(f'   Пустые developer_ids: {empty_stats[4]:,}')
        self.stdout.write(f'   Пустые game_mode_ids: {empty_stats[5]:,}')
        self.stdout.write(f'   Пустые engine_ids: {empty_stats[6]:,}')

        self.stdout.write(self.style.WARNING(f'\n🔄 Начинаю обновление векторов...'))

        # ОПТИМИЗИРОВАННОЕ ОБНОВЛЕНИЕ С БАТЧИНГОМ
        batch_size = 5000
        total_updated = 0
        total_cleaned = 0

        # Получаем список ID игр
        with connection.cursor() as cursor:
            cursor.execute("SELECT id FROM games_game ORDER BY id")
            game_ids = [row[0] for row in cursor.fetchall()]

        self.stdout.write(f'   📦 Обновление батчами по {batch_size} игр...')

        for i in range(0, len(game_ids), batch_size):
            batch_ids = game_ids[i:i + batch_size]
            ids_str = ','.join(str(id) for id in batch_ids)

            with connection.cursor() as cursor:
                # ОБНОВЛЕНИЕ ВЕКТОРОВ (без keyword_ids, так как они уже хранятся в games_game)
                cursor.execute(f"""
                    WITH all_relations AS (
                        SELECT game_id, 'genre' as rel_type, g.igdb_id as rel_id
                        FROM games_game_genres gg
                        JOIN games_genre g ON gg.genre_id = g.id
                        WHERE gg.game_id IN ({ids_str})
                        UNION ALL
                        SELECT game_id, 'theme', t.igdb_id
                        FROM games_game_themes gt
                        JOIN games_theme t ON gt.theme_id = t.id
                        WHERE gt.game_id IN ({ids_str})
                        UNION ALL
                        SELECT game_id, 'perspective', pp.igdb_id
                        FROM games_game_player_perspectives gpp
                        JOIN games_playerperspective pp ON gpp.playerperspective_id = pp.id
                        WHERE gpp.game_id IN ({ids_str})
                        UNION ALL
                        SELECT game_id, 'developer', c.igdb_id
                        FROM games_game_developers gd
                        JOIN games_company c ON gd.company_id = c.id
                        WHERE gd.game_id IN ({ids_str})
                        UNION ALL
                        SELECT game_id, 'gamemode', gm.igdb_id
                        FROM games_game_game_modes ggm
                        JOIN games_gamemode gm ON ggm.gamemode_id = gm.id
                        WHERE ggm.game_id IN ({ids_str})
                        UNION ALL
                        SELECT game_id, 'engine', ge.igdb_id
                        FROM games_game_engines gge
                        JOIN games_gameengine ge ON gge.gameengine_id = ge.id
                        WHERE gge.game_id IN ({ids_str})
                    ),
                    aggregated AS (
                        SELECT game_id,
                               array_agg(DISTINCT rel_id) FILTER (WHERE rel_type = 'genre') as genre_ids,
                               array_agg(DISTINCT rel_id) FILTER (WHERE rel_type = 'theme') as theme_ids,
                               array_agg(DISTINCT rel_id) FILTER (WHERE rel_type = 'perspective') as perspective_ids,
                               array_agg(DISTINCT rel_id) FILTER (WHERE rel_type = 'developer') as developer_ids,
                               array_agg(DISTINCT rel_id) FILTER (WHERE rel_type = 'gamemode') as game_mode_ids,
                               array_agg(DISTINCT rel_id) FILTER (WHERE rel_type = 'engine') as engine_ids
                        FROM all_relations
                        GROUP BY game_id
                    )
                    UPDATE games_game g
                    SET genre_ids = COALESCE(a.genre_ids, ARRAY[]::integer[]),
                        theme_ids = COALESCE(a.theme_ids, ARRAY[]::integer[]),
                        perspective_ids = COALESCE(a.perspective_ids, ARRAY[]::integer[]),
                        developer_ids = COALESCE(a.developer_ids, ARRAY[]::integer[]),
                        game_mode_ids = COALESCE(a.game_mode_ids, ARRAY[]::integer[]),
                        engine_ids = COALESCE(a.engine_ids, ARRAY[]::integer[])
                    FROM aggregated a
                    WHERE g.id = a.game_id
                """)
                total_updated += cursor.rowcount

            # Очистка игр без связей в этом батче
            with connection.cursor() as cursor:
                cursor.execute(f"""
                    UPDATE games_game
                    SET genre_ids = ARRAY[]::integer[],
                        theme_ids = ARRAY[]::integer[],
                        perspective_ids = ARRAY[]::integer[],
                        developer_ids = ARRAY[]::integer[],
                        game_mode_ids = ARRAY[]::integer[],
                        engine_ids = ARRAY[]::integer[]
                    WHERE id IN ({ids_str})
                      AND id NOT IN (
                          SELECT DISTINCT game_id FROM (
                              SELECT game_id FROM games_game_genres WHERE game_id IN ({ids_str}) UNION
                              SELECT game_id FROM games_game_themes WHERE game_id IN ({ids_str}) UNION
                              SELECT game_id FROM games_game_player_perspectives WHERE game_id IN ({ids_str}) UNION
                              SELECT game_id FROM games_game_developers WHERE game_id IN ({ids_str}) UNION
                              SELECT game_id FROM games_game_game_modes WHERE game_id IN ({ids_str}) UNION
                              SELECT game_id FROM games_game_engines WHERE game_id IN ({ids_str})
                          ) t
                      )
                """)
                total_cleaned += cursor.rowcount

            # Прогресс
            progress = (i + len(batch_ids)) / len(game_ids) * 100
            self.stdout.write(
                f'   📍 Батч {i // batch_size + 1}/{(len(game_ids) - 1) // batch_size + 1}: {progress:.1f}% | Обновлено: {total_updated:,} | Очищено: {total_cleaned:,}')

        cache.clear()
        self._clear_game_card_cache()
        self._clear_filter_section_cache()

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

        self.stdout.write(self.style.SUCCESS(f'\n✅ Обновлено записей: {total_updated + total_cleaned:,}'))
        self.stdout.write(self.style.SUCCESS(f'✅ Из них обновлено со связями: {total_updated:,}'))
        self.stdout.write(self.style.SUCCESS(f'✅ Из них очищено без связей: {total_cleaned:,}'))
        self.stdout.write(self.style.SUCCESS(f'✅ Кэш Django очищен'))
        self.stdout.write(self.style.SUCCESS(f'✅ Кэш карточек игр очищен'))
        self.stdout.write(self.style.SUCCESS(f'✅ Кэш секций фильтров очищен'))
        self.stdout.write(self.style.SUCCESS(f'⏱️  Время выполнения: {total_time:.2f} сек'))

        self.stdout.write(self.style.WARNING(f'\n📊 Статистика ПОСЛЕ обновления:'))
        self.stdout.write(f'   Непустые genre_ids: {after_stats[0]:,}')
        self.stdout.write(f'   Непустые keyword_ids: {after_stats[1]:,}')
        self.stdout.write(f'   Непустые theme_ids: {after_stats[2]:,}')
        self.stdout.write(f'   Непустые perspective_ids: {after_stats[3]:,}')
        self.stdout.write(f'   Непустые developer_ids: {after_stats[4]:,}')
        self.stdout.write(f'   Непустые game_mode_ids: {after_stats[5]:,}')
        self.stdout.write(f'   Непустые engine_ids: {after_stats[6]:,}')

        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT COUNT(*)                                              as games_with_genres,
                                  (SELECT COUNT(*) FROM games_game_genres)              as total_genre_relations,
                                  COUNT(*)                                              as games_with_keywords,
                                  (SELECT SUM(array_length(keyword_ids, 1)) FROM games_game WHERE keyword_ids != '{}') as total_keyword_relations,
                                  COUNT(*) as games_with_themes,
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
        self.stdout.write(f'   Жанры: {consistency[0]:,} игр имеют {consistency[1]:,} связей')
        self.stdout.write(f'   Ключевые слова: {consistency[2]:,} игр имеют {consistency[3]:,} связей')
        self.stdout.write(f'   Темы: {consistency[4]:,} игр имеют {consistency[5]:,} связей')
        self.stdout.write(f'   Перспективы: {consistency[6]:,} игр имеют {consistency[7]:,} связей')
        self.stdout.write(f'   Разработчики: {consistency[8]:,} игр имеют {consistency[9]:,} связей')
        self.stdout.write(f'   Режимы игры: {consistency[10]:,} игр имеют {consistency[11]:,} связей')
        self.stdout.write(f'   Движки: {consistency[12]:,} игр имеют {consistency[13]:,} связей')

        self.stdout.write(self.style.SUCCESS(f'\n{"=" * 60}'))
        self.stdout.write(self.style.SUCCESS('ОБНОВЛЕНИЕ ЗАВЕРШЕНО'))
        self.stdout.write(self.style.SUCCESS(f'{"=" * 60}\n'))

    def _ensure_gin_indexes(self):
        """
        Создает GIN индексы для полей-массивов для ускорения поиска похожих игр.
        GIN индексы эффективнее B-tree для операторов && (пересечение) и @> (содержит).
        """
        self.stdout.write(self.style.WARNING('\n🔍 Проверка GIN индексов для массивов...'))

        gin_indexes = [
            ("idx_game_genre_ids_gin", "genre_ids", "gin__int_ops"),
            ("idx_game_keyword_ids_gin", "keyword_ids", "gin__int_ops"),
            ("idx_game_theme_ids_gin", "theme_ids", "gin__int_ops"),
            ("idx_game_perspective_ids_gin", "perspective_ids", "gin__int_ops"),
            ("idx_game_developer_ids_gin", "developer_ids", "gin__int_ops"),
            ("idx_game_game_mode_ids_gin", "game_mode_ids", "gin__int_ops"),
            ("idx_game_engine_ids_gin", "engine_ids", "gin__int_ops"),
        ]

        with connection.cursor() as cursor:
            for index_name, column, opclass in gin_indexes:
                try:
                    cursor.execute(f"""
                        CREATE INDEX CONCURRENTLY IF NOT EXISTS {index_name}
                        ON games_game USING gin ({column} {opclass})
                    """)
                    self.stdout.write(self.style.SUCCESS(f'   ✅ Создан индекс {index_name}'))
                except Exception as e:
                    if 'already exists' not in str(e).lower():
                        self.stdout.write(self.style.WARNING(f'   ⚠️ {index_name}: {e}'))

        self.stdout.write(self.style.SUCCESS('   ✅ Проверка GIN индексов завершена'))

    def _clear_filter_section_cache(self):
        """Очищает кэш секций фильтров."""
        from games.models import FilterSectionCache

        self.stdout.write(self.style.WARNING('\n🗑️  Очистка кэша секций фильтров...'))

        count = FilterSectionCache.objects.all().delete()[0]
        self.stdout.write(self.style.SUCCESS(f'   ✅ Удалено {count} записей FilterSectionCache'))
        return count

    def _clear_game_card_cache(self, dry_run=False):
        """Очищает таблицу кэша карточек игр."""
        from games.models_parts.game_card import GameCardCache

        self.stdout.write(self.style.WARNING('\n🗑️  Очистка кэша карточек игр...'))

        if dry_run:
            count = GameCardCache.objects.count()
            self.stdout.write(f'   [DRY-RUN] Будет очищено {count} записей GameCardCache')
            return count

        count = GameCardCache.objects.all().delete()[0]
        self.stdout.write(self.style.SUCCESS(f'   ✅ Удалено {count} записей GameCardCache'))
        return count

    def _check_and_clear_locks(self):
        """Проверяет наличие блокировок и автоматически снимает их"""
        self.stdout.write(self.style.WARNING('🔍 Проверка блокировок базы данных...'))

        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT a.pid,
                                  l.mode,
                                  l.relation::regclass as table_name, age(now(), a.query_start) as lock_age
                           FROM pg_locks l
                                    JOIN pg_stat_activity a ON l.pid = a.pid
                           WHERE NOT l.granted
                              OR (l.locktype = 'relation' AND l.mode LIKE '%ExclusiveLock%')
                           ORDER BY lock_age DESC NULLS LAST
                           """)

            locks = cursor.fetchall()

            if locks:
                self.stdout.write(self.style.WARNING(f'   ⚠️ Найдено {len(locks)} активных блокировок:'))
                for lock in locks[:5]:
                    pid, mode, table_name, lock_age = lock
                    self.stdout.write(f'      PID: {pid}, {mode}, Таблица: {table_name}, Возраст: {lock_age}')

                self.stdout.write(self.style.WARNING('   🔓 Автоматически снимаю блокировки...'))

                cursor.execute("""
                               SELECT a.pid
                               FROM pg_stat_activity a
                               WHERE a.state = 'active'
                                 AND a.pid != pg_backend_pid()
                      AND (a.query_start < now() - interval '30 seconds' OR
                           a.pid IN (SELECT l.pid FROM pg_locks l WHERE NOT l.granted))
                               """)

                pids = [row[0] for row in cursor.fetchall()]

                if pids:
                    for pid in pids:
                        cursor.execute("SELECT pg_terminate_backend(%s)", [pid])
                        self.stdout.write(f'      Завершен процесс PID: {pid}')

                    self.stdout.write(self.style.SUCCESS(f'   ✅ Завершено {len(pids)} заблокированных процессов'))
                    time.sleep(1)
                else:
                    self.stdout.write(self.style.WARNING('   ⚠️ Не найдено процессов для завершения'))
            else:
                self.stdout.write(self.style.SUCCESS('   ✅ Активных блокировок не найдено'))

            self.stdout.write('')