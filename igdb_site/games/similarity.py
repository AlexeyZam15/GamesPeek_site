from django.db.models import Prefetch, Count
from collections import defaultdict

from .models import Genre, Game, Theme, Company, PlayerPerspective, GameMode, Keyword


class VirtualGame:
    """Виртуальная игра, созданная из выбранных критериев"""

    def __init__(self, genre_ids=None, keyword_ids=None, theme_ids=None,
                 perspective_ids=None, developer_ids=None, series_id=None,
                 game_mode_ids=None, game_type_ids=None, engine_ids=None):
        self.genre_ids = genre_ids or []
        self.keyword_ids = keyword_ids or []
        self.theme_ids = theme_ids or []
        self.perspective_ids = perspective_ids or []
        self.developer_ids = developer_ids or []
        self.series_id = series_id
        self.game_mode_ids = game_mode_ids or []
        self.game_type_ids = game_type_ids or []
        self.engine_ids = engine_ids or []

        self.genres = []
        self.keywords = []
        self.themes = []
        self.player_perspectives = []
        self.developers = []
        self.series = None
        self.game_modes = []
        self.game_types = []
        self.engines = []

        self.name = "Custom Search Criteria"
        self.rating = None
        self.rating_count = 0

    def __str__(self):
        return f"VirtualGame(genres: {len(self.genre_ids)}, keywords: {len(self.keyword_ids)}, game_modes: {len(self.game_mode_ids)}, game_types: {len(self.game_type_ids)}, engines: {len(self.engine_ids)})"

    def load_related(self):
        """Ленивая загрузка связанных объектов"""
        from .models import Genre, Keyword, Theme, PlayerPerspective, Company, Series, GameMode, GameEngine

        if not self.genres and self.genre_ids:
            self.genres = list(Genre.objects.filter(id__in=self.genre_ids).only('id', 'name'))

        if not self.keywords and self.keyword_ids:
            self.keywords = list(Keyword.objects.filter(id__in=self.keyword_ids).only('id', 'name'))

        if not self.themes and self.theme_ids:
            self.themes = list(Theme.objects.filter(id__in=self.theme_ids).only('id', 'name'))

        if not self.player_perspectives and self.perspective_ids:
            self.player_perspectives = list(
                PlayerPerspective.objects.filter(id__in=self.perspective_ids).only('id', 'name'))

        if not self.developers and self.developer_ids:
            self.developers = list(Company.objects.filter(id__in=self.developer_ids).only('id', 'name'))

        if not self.game_modes and self.game_mode_ids:
            self.game_modes = list(GameMode.objects.filter(id__in=self.game_mode_ids).only('id', 'name'))

        if not self.game_types and self.game_type_ids:
            self.game_types = self.game_type_ids

        if not self.engines and self.engine_ids:
            self.engines = list(GameEngine.objects.filter(id__in=self.engine_ids).only('id', 'name'))


class GameSimilarity:
    """
    УНИВЕРСАЛЬНЫЙ алгоритм похожести с динамическими весами
    """
    DEFAULT_SIMILAR_GAMES_LIMIT = 500  # Если установить 0, будут возвращаться все найденные игры без ограничения

    # Базовые константы с распределением весов
    GENRES_WEIGHT = 30.0
    KEYWORDS_WEIGHT = 40.0
    THEMES_WEIGHT = 10.0
    PERSPECTIVES_WEIGHT = 10.0
    GAME_MODES_WEIGHT = 5.0
    DEVELOPERS_WEIGHT = 5.0
    ENGINES_WEIGHT = 0.0

    # Конфигурационные константы с оптимизированными весами
    # НОВАЯ КОНСТАНТА: минимальное количество общих жанров для включения в результат
    MIN_COMMON_GENRES = 2

    # НОВАЯ КОНСТАНТА: минимальный порог похожести по умолчанию
    DEFAULT_MIN_SIMILARITY = 40

    # Вспомогательные константы для расчетов
    KEYWORDS_ADD_PER_MATCH = 0.2

    def __init__(self):
        # Кэш для ускорения повторных расчетов
        self._similarity_cache = {}
        self._game_data_cache = {}

    def get_similarity_formula(self, source, target):
        """
        Возвращает структурированные данные для красивого отображения вклада каждого критерия.
        impact-badge теперь показывает процент соответствия критерия (common/source_total * 100)
        """
        try:
            # Получаем breakdown для этой пары игр
            breakdown = self.get_similarity_breakdown(source, target)

            # Получаем данные исходной игры
            source_data, _ = self._prepare_source_data(source)

            # Формируем структурированные данные для шаблона
            criteria_contributions = []

            # Жанры
            if breakdown['genres']['max_score'] > 0:
                common_count = len(breakdown['genres']['common_elements'])
                source_count = source_data['genre_count']

                # Процент соответствия жанров (common/source_total * 100)
                genre_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0

                criteria_contributions.append({
                    'icon': '🎮',
                    'name': 'Genres',
                    'map_name': 'genres',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['genres']['max_score'],
                    'contribution': breakdown['genres']['score'],
                    'percentage': genre_match_percentage,  # ИЗМЕНЕНО: процент соответствия
                    'color': 'purple'
                })

            # Ключевые слова
            if breakdown['keywords']['max_score'] > 0:
                common_count = len(breakdown['keywords']['common_elements'])
                source_count = source_data['keyword_count']

                # Процент соответствия ключевых слов (common/source_total * 100)
                keyword_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0

                criteria_contributions.append({
                    'icon': '🔑',
                    'name': 'Keywords',
                    'map_name': 'keywords',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['keywords']['max_score'],
                    'contribution': breakdown['keywords']['score'],
                    'percentage': keyword_match_percentage,  # ИЗМЕНЕНО: процент соответствия
                    'color': 'success'
                })

            # Темы
            if breakdown['themes']['max_score'] > 0:
                common_count = len(breakdown['themes']['common_elements'])
                source_count = source_data['theme_count']

                theme_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0

                criteria_contributions.append({
                    'icon': '🎭',
                    'name': 'Themes',
                    'map_name': 'themes',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['themes']['max_score'],
                    'contribution': breakdown['themes']['score'],
                    'percentage': theme_match_percentage,  # ИЗМЕНЕНО
                    'color': 'orange'
                })

            # Перспективы
            if breakdown['perspectives']['max_score'] > 0:
                common_count = len(breakdown['perspectives']['common_elements'])
                source_count = source_data['perspective_count']

                perspective_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0

                criteria_contributions.append({
                    'icon': '👁️',
                    'name': 'Perspectives',
                    'map_name': 'perspectives',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['perspectives']['max_score'],
                    'contribution': breakdown['perspectives']['score'],
                    'percentage': perspective_match_percentage,  # ИЗМЕНЕНО
                    'color': 'info'
                })

            # Режимы игры
            if breakdown['game_modes']['max_score'] > 0:
                common_count = len(breakdown['game_modes']['common_elements'])
                source_count = source_data['game_mode_count']

                gamemode_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0

                criteria_contributions.append({
                    'icon': '🎯',
                    'name': 'Game Modes',
                    'map_name': 'game_modes',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['game_modes']['max_score'],
                    'contribution': breakdown['game_modes']['score'],
                    'percentage': gamemode_match_percentage,  # ИЗМЕНЕНО
                    'color': 'pink'
                })

            # Разработчики
            if breakdown['developers']['max_score'] > 0:
                common_count = len(breakdown['developers']['common_elements'])
                source_count = source_data['developer_count']

                developer_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0

                criteria_contributions.append({
                    'icon': '🏢',
                    'name': 'Developers',
                    'map_name': 'developers',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['developers']['max_score'],
                    'contribution': breakdown['developers']['score'],
                    'percentage': developer_match_percentage,  # ИЗМЕНЕНО
                    'color': 'secondary'
                })

            # Движки
            if breakdown['engines']['max_score'] > 0:
                common_count = len(breakdown['engines']['common_elements'])
                source_count = source_data['engine_count']

                engine_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0

                criteria_contributions.append({
                    'icon': '⚙️',
                    'name': 'Engines',
                    'map_name': 'engines',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['engines']['max_score'],
                    'contribution': breakdown['engines']['score'],
                    'percentage': engine_match_percentage,  # ИЗМЕНЕНО
                    'color': 'warning'
                })

            # Бонус (если есть)
            bonus = breakdown.get('bonus', 0) if breakdown.get('bonus', 0) > 0 else None

            return {
                'criteria': criteria_contributions,
                'bonus': bonus,
                'total': breakdown['total_similarity'],
                'total_from_criteria': breakdown.get('total_without_bonus',
                                                     sum(c['contribution'] for c in criteria_contributions))
            }

        except Exception as e:
            print(f"Error generating similarity formula: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'criteria': [],
                'bonus': None,
                'total': 0,
                'total_from_criteria': 0,
                'error': str(e)
            }

    def _get_candidate_ids_new(self, source_data, single_player_info, min_similarity, search_filters=None):
        """
        МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ поиск кандидатов - БЕЗ ЛИМИТОВ.
        Использует сырой SQL для максимальной скорости.
        """
        import time
        from django.utils import timezone
        from django.db import connection
        from games.views_parts.base_views import _apply_search_filters

        print("МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ поиск кандидатов (без лимитов)...")
        start_time = time.time()

        current_time = timezone.now()

        source_genre_ids = source_data['genre_ids']
        source_theme_ids = source_data['theme_ids']
        source_keyword_ids = source_data['keyword_ids']
        source_perspective_ids = source_data['perspective_ids']
        source_game_mode_ids = source_data['game_mode_ids']
        source_engine_ids = source_data['engine_ids']
        source_game_id = source_data.get('game_id', 0)

        has_single_player = single_player_info['has_single_player']
        single_player_mode_id = single_player_info['single_player_mode_id']
        dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']

        # Формируем список ID для исключения
        exclude_ids = [source_game_id] if source_game_id else []

        # Строим SQL запрос для быстрого получения кандидатов
        sql_parts = []
        params = []

        # Базовый запрос: все вышедшие игры
        base_sql = """
                   SELECT DISTINCT g.id
                   FROM games_game g
                   WHERE g.first_release_date IS NOT NULL
                     AND g.first_release_date <= %s \
                   """
        params.append(current_time)
        sql_parts.append(base_sql)

        # Добавляем поисковые фильтры через JOIN
        if search_filters:
            filter_joins = []

            # Платформы (OR) - используем EXISTS для скорости
            if search_filters.get('platforms'):
                platform_ids = search_filters['platforms']
                platform_ids_str = ','.join(map(str, platform_ids))
                filter_joins.append(f"""
                    AND EXISTS (
                        SELECT 1 FROM games_game_platforms ggp 
                        WHERE ggp.game_id = g.id AND ggp.platform_id IN ({platform_ids_str})
                    )
                """)

            # Жанры (AND) - каждый жанр требует отдельной проверки
            if search_filters.get('genres'):
                for genre_id in search_filters['genres']:
                    filter_joins.append(f"""
                        AND EXISTS (
                            SELECT 1 FROM games_game_genres ggg 
                            WHERE ggg.game_id = g.id AND ggg.genre_id = {genre_id}
                        )
                    """)

            # Ключевые слова (AND)
            if search_filters.get('keywords'):
                for keyword_id in search_filters['keywords']:
                    filter_joins.append(f"""
                        AND EXISTS (
                            SELECT 1 FROM games_game_keywords ggk 
                            WHERE ggk.game_id = g.id AND ggk.keyword_id = {keyword_id}
                        )
                    """)

            # Темы (AND)
            if search_filters.get('themes'):
                for theme_id in search_filters['themes']:
                    filter_joins.append(f"""
                        AND EXISTS (
                            SELECT 1 FROM games_game_themes ggt 
                            WHERE ggt.game_id = g.id AND ggt.theme_id = {theme_id}
                        )
                    """)

            # Перспективы (OR)
            if search_filters.get('perspectives'):
                perspective_ids = search_filters['perspectives']
                perspective_ids_str = ','.join(map(str, perspective_ids))
                filter_joins.append(f"""
                    AND EXISTS (
                        SELECT 1 FROM games_game_player_perspectives gggp 
                        WHERE gggp.game_id = g.id AND gggp.playerperspective_id IN ({perspective_ids_str})
                    )
                """)

            # Режимы игры (OR)
            if search_filters.get('game_modes'):
                game_mode_ids = search_filters['game_modes']
                game_mode_ids_str = ','.join(map(str, game_mode_ids))
                filter_joins.append(f"""
                    AND EXISTS (
                        SELECT 1 FROM games_game_game_modes gggm 
                        WHERE gggm.game_id = g.id AND gggm.gamemode_id IN ({game_mode_ids_str})
                    )
                """)

            # Движки (OR)
            if search_filters.get('engines'):
                engine_ids = search_filters['engines']
                engine_ids_str = ','.join(map(str, engine_ids))
                filter_joins.append(f"""
                    AND EXISTS (
                        SELECT 1 FROM games_game_engines gge 
                        WHERE gge.game_id = g.id AND gge.gameengine_id IN ({engine_ids_str})
                    )
                """)

            # Игровые типы (OR)
            if search_filters.get('game_types'):
                game_type_ids = search_filters['game_types']
                game_type_ids_str = ','.join(map(str, game_type_ids))
                filter_joins.append(f"""
                    AND g.game_type IN ({game_type_ids_str})
                """)

            # Дата (AND)
            year_start = search_filters.get('release_year_start')
            year_end = search_filters.get('release_year_end')
            if year_start:
                filter_joins.append(f" AND EXTRACT(YEAR FROM g.first_release_date) >= {year_start}")
            if year_end:
                filter_joins.append(f" AND EXTRACT(YEAR FROM g.first_release_date) <= {year_end}")

            sql_parts.extend(filter_joins)

        # Исключаем исходную игру
        if exclude_ids:
            exclude_str = ','.join(map(str, exclude_ids))
            sql_parts.append(f" AND g.id NOT IN ({exclude_str})")

        # Если есть жанры - добавляем фильтр по общим жанрам
        if source_genre_ids:
            source_genre_ids_str = ','.join(map(str, source_genre_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_genres ggg 
                    WHERE ggg.game_id = g.id AND ggg.genre_id IN ({source_genre_ids_str})
                    GROUP BY ggg.game_id
                    HAVING COUNT(DISTINCT ggg.genre_id) >= {dynamic_min_common_genres}
                )
            """)

        # Если есть темы - добавляем фильтр по темам
        elif source_theme_ids:
            source_theme_ids_str = ','.join(map(str, source_theme_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_themes ggt 
                    WHERE ggt.game_id = g.id AND ggt.theme_id IN ({source_theme_ids_str})
                )
            """)

        # Если есть движки - добавляем фильтр по движкам
        elif source_engine_ids:
            source_engine_ids_str = ','.join(map(str, source_engine_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_engines gge 
                    WHERE gge.game_id = g.id AND gge.gameengine_id IN ({source_engine_ids_str})
                )
            """)

        # Если есть ключевые слова - добавляем фильтр по ключевым словам
        elif source_keyword_ids:
            source_keyword_ids_str = ','.join(map(str, source_keyword_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_keywords ggk 
                    WHERE ggk.game_id = g.id AND ggk.keyword_id IN ({source_keyword_ids_str})
                )
            """)

        # Если есть перспективы - добавляем фильтр по перспективам
        elif source_perspective_ids:
            source_perspective_ids_str = ','.join(map(str, source_perspective_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_player_perspectives gggp 
                    WHERE gggp.game_id = g.id AND gggp.playerperspective_id IN ({source_perspective_ids_str})
                )
            """)

        # Если есть режимы игры - добавляем фильтр по режимам
        elif source_game_mode_ids:
            source_game_mode_ids_str = ','.join(map(str, source_game_mode_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_game_modes gggm 
                    WHERE gggm.game_id = g.id AND gggm.gamemode_id IN ({source_game_mode_ids_str})
                )
            """)

        # Фильтр по Single Player
        if has_single_player and single_player_mode_id:
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_game_modes gggm 
                    WHERE gggm.game_id = g.id AND gggm.gamemode_id = {single_player_mode_id}
                )
            """)

        # Объединяем SQL
        final_sql = ' '.join(sql_parts)

        # Выполняем запрос
        candidate_ids = []
        with connection.cursor() as cursor:
            cursor.execute(final_sql, params)
            candidate_ids = [row[0] for row in cursor.fetchall()]

        print(f"Найдено {len(candidate_ids)} уникальных кандидатов за {time.time() - start_time:.2f} сек")
        return candidate_ids

    def _game_passes_search_filters(self, game, search_filters):
        """
        Проверяет, проходит ли игра поисковые фильтры.
        Использует ту же логику: AND между группами, OR внутри группы.
        """
        if not search_filters:
            return True

        # Проверка каждой группы фильтров
        # Платформы (OR)
        if search_filters.get('platforms'):
            game_platform_ids = set(game.platforms.values_list('id', flat=True))
            if not (set(search_filters['platforms']) & game_platform_ids):
                return False

        # Игровые типы (OR)
        if search_filters.get('game_types'):
            if game.game_type not in search_filters['game_types']:
                return False

        # Перспективы (OR)
        if search_filters.get('perspectives'):
            game_perspective_ids = set(game.player_perspectives.values_list('id', flat=True))
            if not (set(search_filters['perspectives']) & game_perspective_ids):
                return False

        # Режимы игры (OR)
        if search_filters.get('game_modes'):
            game_mode_ids = set(game.game_modes.values_list('id', flat=True))
            if not (set(search_filters['game_modes']) & game_mode_ids):
                return False

        # Движки (OR)
        if search_filters.get('engines'):
            game_engine_ids = set(game.engines.values_list('id', flat=True))
            if not (set(search_filters['engines']) & game_engine_ids):
                return False

        # Жанры (AND)
        if search_filters.get('genres'):
            game_genre_ids = set(game.genres.values_list('id', flat=True))
            for genre_id in search_filters['genres']:
                if genre_id not in game_genre_ids:
                    return False

        # Ключевые слова (AND)
        if search_filters.get('keywords'):
            game_keyword_ids = set(game.keywords.values_list('id', flat=True))
            for keyword_id in search_filters['keywords']:
                if keyword_id not in game_keyword_ids:
                    return False

        # Темы (AND)
        if search_filters.get('themes'):
            game_theme_ids = set(game.themes.values_list('id', flat=True))
            for theme_id in search_filters['themes']:
                if theme_id not in game_theme_ids:
                    return False

        # Дата (AND)
        year_start = search_filters.get('release_year_start')
        year_end = search_filters.get('release_year_end')

        if year_start or year_end:
            if not game.first_release_date:
                return False
            game_year = game.first_release_date.year
            if year_start and game_year < year_start:
                return False
            if year_end and game_year > year_end:
                return False

        return True

    def _calculate_common_elements_new(self, games_data, source_data, candidate_ids):
        """
        МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ подсчет общих элементов - ОДИН SQL ЗАПРОС.
        Использует агрегацию через COUNT с CASE WHEN для всех типов данных одновременно.
        """
        import time
        from django.db import connection

        print("МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ подсчет общих элементов (один запрос)...")
        start_time = time.time()

        if not candidate_ids:
            return games_data

        source_genre_ids = source_data.get('genre_ids', [])
        source_keyword_ids = source_data.get('keyword_ids', [])
        source_theme_ids = source_data.get('theme_ids', [])
        source_perspective_ids = source_data.get('perspective_ids', [])
        source_game_mode_ids = source_data.get('game_mode_ids', [])
        source_engine_ids = source_data.get('engine_ids', [])
        single_player_mode_id = source_data.get('single_player_mode_id')

        # Подготавливаем строки для IN
        genre_in = ','.join(map(str, source_genre_ids)) if source_genre_ids else 'NULL'
        keyword_in = ','.join(map(str, source_keyword_ids)) if source_keyword_ids else 'NULL'
        theme_in = ','.join(map(str, source_theme_ids)) if source_theme_ids else 'NULL'
        perspective_in = ','.join(map(str, source_perspective_ids)) if source_perspective_ids else 'NULL'
        gamemode_in = ','.join(map(str, source_game_mode_ids)) if source_game_mode_ids else 'NULL'
        engine_in = ','.join(map(str, source_engine_ids)) if source_engine_ids else 'NULL'

        candidate_ids_str = ','.join(map(str, candidate_ids))
        single_player_id = single_player_mode_id or 0

        # Единый SQL запрос с подзапросами
        query = f"""
            SELECT
                g.id as game_id,

                -- Быстрый подсчет общих жанров через JSON
                COALESCE((
                    SELECT COUNT(*)
                    FROM games_game_genres ggg
                    WHERE ggg.game_id = g.id AND ggg.genre_id IN ({genre_in if genre_in != 'NULL' else 'NULL'})
                ), 0) as common_genres,

                -- Быстрый подсчет общих ключевых слов
                COALESCE((
                    SELECT COUNT(*)
                    FROM games_game_keywords ggk
                    WHERE ggk.game_id = g.id AND ggk.keyword_id IN ({keyword_in if keyword_in != 'NULL' else 'NULL'})
                ), 0) as common_keywords,

                -- Быстрый подсчет общих тем
                COALESCE((
                    SELECT COUNT(*)
                    FROM games_game_themes ggt
                    WHERE ggt.game_id = g.id AND ggt.theme_id IN ({theme_in if theme_in != 'NULL' else 'NULL'})
                ), 0) as common_themes,

                -- Быстрый подсчет общих перспектив
                COALESCE((
                    SELECT COUNT(*)
                    FROM games_game_player_perspectives gggp
                    WHERE gggp.game_id = g.id AND gggp.playerperspective_id IN ({perspective_in if perspective_in != 'NULL' else 'NULL'})
                ), 0) as common_perspectives,

                -- Быстрый подсчет общих режимов игры
                COALESCE((
                    SELECT COUNT(*)
                    FROM games_game_game_modes gggm
                    WHERE gggm.game_id = g.id AND gggm.gamemode_id IN ({gamemode_in if gamemode_in != 'NULL' else 'NULL'})
                ), 0) as common_game_modes,

                -- Быстрый подсчет общих движков
                COALESCE((
                    SELECT COUNT(*)
                    FROM games_game_engines gge
                    WHERE gge.game_id = g.id AND gge.gameengine_id IN ({engine_in if engine_in != 'NULL' else 'NULL'})
                ), 0) as common_engines,

                -- Быстрая проверка Single Player
                EXISTS (
                    SELECT 1
                    FROM games_game_game_modes gggm
                    WHERE gggm.game_id = g.id AND gggm.gamemode_id = {single_player_id}
                ) as has_single_player

            FROM games_game g
            WHERE g.id IN ({candidate_ids_str})
        """

        with connection.cursor() as cursor:
            cursor.execute(query)

            for row in cursor.fetchall():
                game_id = row[0]
                if game_id in games_data:
                    games_data[game_id].update({
                        'common_genres': row[1],
                        'common_keywords': row[2],
                        'common_themes': row[3],
                        'common_perspectives': row[4],
                        'common_game_modes': row[5],
                        'common_engines': row[6],
                        'has_single_player': bool(row[7]),
                    })

        print(f"Подсчет с разбивкой завершен за {time.time() - start_time:.2f} сек")
        return games_data

    def _load_full_objects(self, similar_games):
        """
        МАКСИМАЛЬНО ОПТИМИЗИРОВАННАЯ загрузка полных объектов игр.
        Использует bulk запросы и prefetch_related.
        """
        import time

        print("МАКСИМАЛЬНО ОПТИМИЗИРОВАННАЯ загрузка полных объектов...")
        load_time = time.time()

        final_results = []

        if not similar_games:
            return final_results

        try:
            game_ids = [item['game_id'] for item in similar_games]

            # Создаем словарь для быстрого доступа к similarity
            similarity_map = {item['game_id']: item for item in similar_games}

            # Единый запрос с prefetch_related для всех связанных данных
            games = Game.objects.filter(id__in=game_ids).prefetch_related(
                'genres',
                'keywords',
                'themes',
                'game_modes',
                'engines',
                'platforms',
                'player_perspectives',
                'developers'
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            )

            for game in games:
                item = similarity_map.get(game.id)
                if item:
                    final_results.append({
                        'game': game,
                        'similarity': item['similarity'],
                        'common_keywords_count': item['common_keywords'],
                        'common_genres_count': item['common_genres'],
                        'common_themes_count': item['common_themes'],
                        'common_engines_count': item.get('common_engines', 0),
                        'has_single_player': item['has_single_player'],
                        'is_source_game': False
                    })
        except Exception as e:
            print(f"Ошибка при загрузке объектов: {e}")
            import traceback
            traceback.print_exc()
            return []

        print(f"Загрузка объектов завершена за {time.time() - load_time:.2f} сек")
        return final_results

    def _prepare_candidate_data(self, candidate_ids):
        """МАКСИМАЛЬНО ОПТИМИЗИРОВАННАЯ подготовка данных кандидатов - один запрос."""
        import time
        from django.db import connection

        print("МАКСИМАЛЬНО ОПТИМИЗИРОВАННАЯ подготовка данных...")
        prep_time = time.time()

        games_data = {}

        if not candidate_ids:
            return games_data

        candidate_ids_str = ','.join(map(str, candidate_ids))

        with connection.cursor() as cursor:
            # Один запрос для получения всех данных
            query = f"""
                SELECT 
                    g.id,
                    g.name,
                    COALESCE(gc.genre_count, 0) as total_genres,
                    COALESCE(kc.keyword_count, 0) as total_keywords,
                    COALESCE(tc.theme_count, 0) as total_themes,
                    COALESCE(pc.perspective_count, 0) as total_perspectives,
                    COALESCE(gmc.game_mode_count, 0) as total_game_modes,
                    COALESCE(ec.engine_count, 0) as total_engines
                FROM games_game g
                LEFT JOIN (
                    SELECT game_id, COUNT(*) as genre_count
                    FROM games_game_genres
                    GROUP BY game_id
                ) gc ON g.id = gc.game_id
                LEFT JOIN (
                    SELECT game_id, COUNT(*) as keyword_count
                    FROM games_game_keywords
                    GROUP BY game_id
                ) kc ON g.id = kc.game_id
                LEFT JOIN (
                    SELECT game_id, COUNT(*) as theme_count
                    FROM games_game_themes
                    GROUP BY game_id
                ) tc ON g.id = tc.game_id
                LEFT JOIN (
                    SELECT game_id, COUNT(*) as perspective_count
                    FROM games_game_player_perspectives
                    GROUP BY game_id
                ) pc ON g.id = pc.game_id
                LEFT JOIN (
                    SELECT game_id, COUNT(*) as game_mode_count
                    FROM games_game_game_modes
                    GROUP BY game_id
                ) gmc ON g.id = gmc.game_id
                LEFT JOIN (
                    SELECT game_id, COUNT(*) as engine_count
                    FROM games_game_engines
                    GROUP BY game_id
                ) ec ON g.id = ec.game_id
                WHERE g.id IN ({candidate_ids_str})
            """

            cursor.execute(query)

            for row in cursor.fetchall():
                game_id, game_name = row[0], row[1]
                games_data[game_id] = {
                    'id': game_id,
                    'name': game_name,
                    'total_genres': row[2],
                    'total_keywords': row[3],
                    'total_themes': row[4],
                    'total_perspectives': row[5],
                    'total_game_modes': row[6],
                    'total_engines': row[7],
                    'common_keywords': 0,
                    'common_genres': 0,
                    'common_themes': 0,
                    'common_perspectives': 0,
                    'common_game_modes': 0,
                    'common_engines': 0,
                    'has_single_player': False,
                }

        print(f"Подготовлено {len(games_data)} игр за {time.time() - prep_time:.2f} сек")
        return games_data

    def _calculate_game_similarity_new(self, source_genre_count, source_keyword_count, source_theme_count,
                                       source_developer_count, source_perspective_count, source_game_mode_count,
                                       source_engine_count,
                                       target_data, source_data=None):
        """
        НОВЫЙ расчет схожести - с KEYWORDS_ADD_PER_MATCH для ключевых слов.
        Ключевые слова: каждое совпадение дает KEYWORDS_ADD_PER_MATCH %.
        """
        # Используем фиксированные веса из класса, а не динамические
        total_weight = (self.GENRES_WEIGHT + self.KEYWORDS_WEIGHT + self.THEMES_WEIGHT +
                        self.PERSPECTIVES_WEIGHT + self.GAME_MODES_WEIGHT +
                        self.DEVELOPERS_WEIGHT + self.ENGINES_WEIGHT)

        similarity = 0.0

        # 1. ЖАНРЫ
        if self.GENRES_WEIGHT > 0 and source_genre_count > 0:
            if target_data.get('common_genres', 0) > 0:
                genre_match_ratio = target_data['common_genres'] / max(source_genre_count, 1)
                similarity += genre_match_ratio * self.GENRES_WEIGHT

        # 2. КЛЮЧЕВЫЕ СЛОВА - ИСПРАВЛЕНО: каждое совпадение дает KEYWORDS_ADD_PER_MATCH %
        if self.KEYWORDS_WEIGHT > 0 and source_keyword_count > 0:
            common_keywords = target_data.get('common_keywords', 0)
            if common_keywords > 0:
                # Каждое совпадение добавляет KEYWORDS_ADD_PER_MATCH %, но не больше KEYWORDS_WEIGHT
                keyword_score = min(common_keywords * self.KEYWORDS_ADD_PER_MATCH, self.KEYWORDS_WEIGHT)
                similarity += keyword_score

        # 3. ТЕМЫ
        if self.THEMES_WEIGHT > 0 and source_theme_count > 0:
            if target_data.get('common_themes', 0) > 0:
                theme_match_ratio = target_data['common_themes'] / max(source_theme_count, 1)
                similarity += theme_match_ratio * self.THEMES_WEIGHT

        # 4. ПЕРСПЕКТИВЫ
        if self.PERSPECTIVES_WEIGHT > 0 and source_perspective_count > 0:
            if target_data.get('common_perspectives', 0) > 0:
                perspective_match_ratio = target_data['common_perspectives'] / max(source_perspective_count, 1)
                similarity += perspective_match_ratio * self.PERSPECTIVES_WEIGHT

        # 5. РЕЖИМЫ ИГРЫ
        if self.GAME_MODES_WEIGHT > 0 and source_game_mode_count > 0:
            if target_data.get('common_game_modes', 0) > 0:
                game_mode_match_ratio = target_data['common_game_modes'] / max(source_game_mode_count, 1)
                similarity += game_mode_match_ratio * self.GAME_MODES_WEIGHT

        # 6. РАЗРАБОТЧИКИ
        if self.DEVELOPERS_WEIGHT > 0 and source_developer_count > 0:
            if target_data.get('common_developers', 0) > 0:
                developer_match_ratio = target_data.get('common_developers', 0) / max(source_developer_count, 1)
                similarity += developer_match_ratio * self.DEVELOPERS_WEIGHT

        # 7. ДВИЖКИ
        if self.ENGINES_WEIGHT > 0 and source_engine_count > 0:
            if target_data.get('common_engines', 0) > 0:
                engine_match_ratio = target_data.get('common_engines', 0) / max(source_engine_count, 1)
                similarity += engine_match_ratio * self.ENGINES_WEIGHT

        # 8. ДОПОЛНИТЕЛЬНЫЙ БАЛЛ за наличие любых совпадений
        has_any_matches = any([
            target_data.get('common_genres', 0) > 0,
            target_data.get('common_keywords', 0) > 0,
            target_data.get('common_themes', 0) > 0,
            target_data.get('common_perspectives', 0) > 0,
            target_data.get('common_game_modes', 0) > 0,
            target_data.get('common_developers', 0) > 0,
            target_data.get('common_engines', 0) > 0
        ])

        # Добавляем бонус, если есть совпадения и задействовано более одного критерия
        active_criteria_count = sum([
            source_genre_count > 0,
            source_keyword_count > 0,
            source_theme_count > 0,
            source_perspective_count > 0,
            source_game_mode_count > 0,
            source_developer_count > 0,
            source_engine_count > 0
        ])

        if has_any_matches and active_criteria_count > 1:
            similarity += 5.0

        return min(100.0, similarity)

    def calculate_similarity(self, source, target):
        """Основной метод вычисления похожести с ФИКСИРОВАННЫМИ весами."""
        # Проверка на идентичность
        if source == target:
            return 100.0

        # Генерация ключа кэша
        cache_key = self._get_similarity_cache_key(source, target)

        # Проверка кэша
        if cache_key in self._similarity_cache:
            return self._similarity_cache[cache_key]

        # Получаем данные
        source_data = self._get_cached_game_data(source)
        target_data = self._get_cached_game_data(target)

        # Используем фиксированные веса из класса
        total_weight = (self.GENRES_WEIGHT + self.KEYWORDS_WEIGHT + self.THEMES_WEIGHT +
                        self.PERSPECTIVES_WEIGHT + self.GAME_MODES_WEIGHT +
                        self.DEVELOPERS_WEIGHT + self.ENGINES_WEIGHT)

        similarity = 0.0

        # Подсчитываем активные критерии для бонуса
        active_criteria_count = 0

        # 1. ЖАНРЫ
        if self.GENRES_WEIGHT > 0 and source_data.get('genres'):
            common_genres = source_data['genres'] & target_data['genres']
            if common_genres:
                source_count = len(source_data['genres'])
                match_ratio = len(common_genres) / source_count
                similarity += match_ratio * self.GENRES_WEIGHT
                active_criteria_count += 1

        # 2. КЛЮЧЕВЫЕ СЛОВА - процент совпадения
        if self.KEYWORDS_WEIGHT > 0 and source_data.get('keywords'):
            common_keywords = source_data['keywords'] & target_data['keywords']
            if common_keywords:
                source_count = len(source_data['keywords'])
                match_ratio = len(common_keywords) / source_count
                similarity += match_ratio * self.KEYWORDS_WEIGHT
                active_criteria_count += 1

        # 3. ТЕМЫ
        if self.THEMES_WEIGHT > 0 and source_data.get('themes'):
            common_themes = source_data['themes'] & target_data['themes']
            if common_themes:
                source_count = len(source_data['themes'])
                match_ratio = len(common_themes) / source_count
                similarity += match_ratio * self.THEMES_WEIGHT
                active_criteria_count += 1

        # 4. РАЗРАБОТЧИКИ
        if self.DEVELOPERS_WEIGHT > 0 and source_data.get('developers'):
            common_developers = source_data['developers'] & target_data['developers']
            if common_developers:
                source_count = len(source_data['developers'])
                match_ratio = len(common_developers) / source_count
                similarity += match_ratio * self.DEVELOPERS_WEIGHT
                active_criteria_count += 1

        # 5. ПЕРСПЕКТИВЫ
        if self.PERSPECTIVES_WEIGHT > 0 and source_data.get('perspectives'):
            common_perspectives = source_data['perspectives'] & target_data['perspectives']
            if common_perspectives:
                source_count = len(source_data['perspectives'])
                match_ratio = len(common_perspectives) / source_count
                similarity += match_ratio * self.PERSPECTIVES_WEIGHT
                active_criteria_count += 1

        # 6. РЕЖИМЫ ИГРЫ
        if self.GAME_MODES_WEIGHT > 0 and source_data.get('game_modes'):
            common_game_modes = source_data['game_modes'] & target_data['game_modes']
            if common_game_modes:
                source_count = len(source_data['game_modes'])
                match_ratio = len(common_game_modes) / source_count
                similarity += match_ratio * self.GAME_MODES_WEIGHT
                active_criteria_count += 1

        # 7. ДВИЖКИ
        if self.ENGINES_WEIGHT > 0 and source_data.get('engines'):
            common_engines = source_data['engines'] & target_data['engines']
            if common_engines:
                source_count = len(source_data['engines'])
                match_ratio = len(common_engines) / source_count
                similarity += match_ratio * self.ENGINES_WEIGHT
                active_criteria_count += 1

        # Добавляем бонус, если есть любые совпадения и задействовано более одного критерия
        has_any_matches = similarity > 0
        if has_any_matches and active_criteria_count > 1:
            similarity += 5.0

        # Ограничиваем результат
        similarity = max(0.0, min(100.0, similarity))

        # Сохраняем в кэш
        self._similarity_cache[cache_key] = similarity

        return similarity

    def get_similarity_breakdown(self, source, target):
        """
        Детальная разбивка похожести по компонентам с KEYWORDS_ADD_PER_MATCH для ключевых слов.
        Использует ту же логику, что и _calculate_game_similarity_new.
        """
        source_data, single_player_info = self._prepare_source_data(source)
        target_raw = self._get_cached_game_data(target)

        target_data = {
            'common_genres': len(source_data.get('genres', set()) & target_raw.get('genres', set())),
            'common_keywords': len(source_data.get('keywords', set()) & target_raw.get('keywords', set())),
            'common_themes': len(source_data.get('themes', set()) & target_raw.get('themes', set())),
            'common_perspectives': len(source_data.get('perspectives', set()) & target_raw.get('perspectives', set())),
            'common_game_modes': len(source_data.get('game_modes', set()) & target_raw.get('game_modes', set())),
            'common_developers': len(source_data.get('developers', set()) & target_raw.get('developers', set())),
            'common_engines': len(source_data.get('engines', set()) & target_raw.get('engines', set())),
        }

        # Фиксированные веса
        max_scores = {
            'genres': self.GENRES_WEIGHT,
            'keywords': self.KEYWORDS_WEIGHT,
            'themes': self.THEMES_WEIGHT,
            'perspectives': self.PERSPECTIVES_WEIGHT,
            'game_modes': self.GAME_MODES_WEIGHT,
            'developers': self.DEVELOPERS_WEIGHT,
            'engines': self.ENGINES_WEIGHT,
        }

        scores = {}

        # Жанры
        if max_scores['genres'] > 0 and source_data['genre_count'] > 0:
            if target_data['common_genres'] > 0:
                genre_match_ratio = target_data['common_genres'] / max(source_data['genre_count'], 1)
                scores['genres'] = genre_match_ratio * max_scores['genres']
            else:
                scores['genres'] = 0.0
        else:
            scores['genres'] = 0.0

        # Ключевые слова - каждое совпадение дает KEYWORDS_ADD_PER_MATCH %
        if max_scores['keywords'] > 0 and source_data['keyword_count'] > 0:
            if target_data['common_keywords'] > 0:
                keyword_score = min(target_data['common_keywords'] * self.KEYWORDS_ADD_PER_MATCH,
                                    max_scores['keywords'])
                scores['keywords'] = keyword_score
            else:
                scores['keywords'] = 0.0
        else:
            scores['keywords'] = 0.0

        # Темы
        if max_scores['themes'] > 0 and source_data['theme_count'] > 0:
            if target_data['common_themes'] > 0:
                theme_match_ratio = target_data['common_themes'] / max(source_data['theme_count'], 1)
                scores['themes'] = theme_match_ratio * max_scores['themes']
            else:
                scores['themes'] = 0.0
        else:
            scores['themes'] = 0.0

        # Перспективы
        if max_scores['perspectives'] > 0 and source_data['perspective_count'] > 0:
            if target_data['common_perspectives'] > 0:
                perspective_match_ratio = target_data['common_perspectives'] / max(source_data['perspective_count'], 1)
                scores['perspectives'] = perspective_match_ratio * max_scores['perspectives']
            else:
                scores['perspectives'] = 0.0
        else:
            scores['perspectives'] = 0.0

        # Режимы игры
        if max_scores['game_modes'] > 0 and source_data['game_mode_count'] > 0:
            if target_data['common_game_modes'] > 0:
                game_mode_match_ratio = target_data['common_game_modes'] / max(source_data['game_mode_count'], 1)
                scores['game_modes'] = game_mode_match_ratio * max_scores['game_modes']
            else:
                scores['game_modes'] = 0.0
        else:
            scores['game_modes'] = 0.0

        # Разработчики
        if max_scores['developers'] > 0 and source_data['developer_count'] > 0:
            if target_data['common_developers'] > 0:
                developer_match_ratio = target_data['common_developers'] / max(source_data['developer_count'], 1)
                scores['developers'] = developer_match_ratio * max_scores['developers']
            else:
                scores['developers'] = 0.0
        else:
            scores['developers'] = 0.0

        # Движки
        if max_scores['engines'] > 0 and source_data['engine_count'] > 0:
            if target_data['common_engines'] > 0:
                engine_match_ratio = target_data['common_engines'] / max(source_data['engine_count'], 1)
                scores['engines'] = engine_match_ratio * max_scores['engines']
            else:
                scores['engines'] = 0.0
        else:
            scores['engines'] = 0.0

        total_without_bonus = sum(scores.values())

        # Бонус за любые совпадения, если задействовано более одного критерия
        has_any_matches = any(scores.values())
        active_criteria_count = sum([
            source_data['genre_count'] > 0,
            source_data['keyword_count'] > 0,
            source_data['theme_count'] > 0,
            source_data['perspective_count'] > 0,
            source_data['game_mode_count'] > 0,
            source_data['developer_count'] > 0,
            source_data['engine_count'] > 0
        ])

        bonus = 0.0
        if has_any_matches and active_criteria_count > 1:
            bonus = 5.0
            total = total_without_bonus + bonus
        else:
            total = total_without_bonus

        total = min(100.0, total)

        common_elements = {
            'genres': list(source_data.get('genres', set()) & target_raw.get('genres', set())),
            'keywords': list(source_data.get('keywords', set()) & target_raw.get('keywords', set())),
            'themes': list(source_data.get('themes', set()) & target_raw.get('themes', set())),
            'perspectives': list(source_data.get('perspectives', set()) & target_raw.get('perspectives', set())),
            'game_modes': list(source_data.get('game_modes', set()) & target_raw.get('game_modes', set())),
            'developers': list(source_data.get('developers', set()) & target_raw.get('developers', set())),
            'engines': list(source_data.get('engines', set()) & target_raw.get('engines', set())),
        }

        return {
            'genres': {
                'score': scores['genres'],
                'max_score': max_scores['genres'],
                'common_elements': common_elements['genres']
            },
            'keywords': {
                'score': scores['keywords'],
                'max_score': max_scores['keywords'],
                'common_elements': common_elements['keywords']
            },
            'themes': {
                'score': scores['themes'],
                'max_score': max_scores['themes'],
                'common_elements': common_elements['themes']
            },
            'developers': {
                'score': scores['developers'],
                'max_score': max_scores['developers'],
                'common_elements': common_elements['developers']
            },
            'perspectives': {
                'score': scores['perspectives'],
                'max_score': max_scores['perspectives'],
                'common_elements': common_elements['perspectives']
            },
            'game_modes': {
                'score': scores['game_modes'],
                'max_score': max_scores['game_modes'],
                'common_elements': common_elements['game_modes']
            },
            'engines': {
                'score': scores['engines'],
                'max_score': max_scores['engines'],
                'common_elements': common_elements['engines']
            },
            'dynamic_weights': max_scores,
            'total_similarity': total,
            'bonus': bonus,
            'total_without_bonus': total_without_bonus
        }

    def _calculate_similarity_for_candidates(self, games_data, source_data, source_game, single_player_info):
        """
        МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ расчет схожести для всех кандидатов.
        Использует KEYWORDS_ADD_PER_MATCH для ключевых слов.
        """
        import time

        print("МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ расчет схожести для кандидатов...")
        calc_time = time.time()

        similar_games = []
        source_genre_count = source_data['genre_count']
        source_keyword_count = source_data['keyword_count']
        source_theme_count = source_data['theme_count']
        source_developer_count = source_data['developer_count']
        source_perspective_count = source_data['perspective_count']
        source_game_mode_count = source_data['game_mode_count']
        source_engine_count = source_data['engine_count']

        has_genres = source_genre_count > 0
        dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']
        has_single_player = single_player_info['has_single_player']
        min_similarity = self.DEFAULT_MIN_SIMILARITY

        source_game_id = getattr(source_game, 'id', None) if isinstance(source_game, Game) else None
        source_game_name = getattr(source_game, 'name', 'Source Game') if isinstance(source_game,
                                                                                     Game) else 'Source Game'

        # Предварительные вычисления для ускорения
        weights = {
            'genres': self.GENRES_WEIGHT,
            'keywords': self.KEYWORDS_WEIGHT,
            'themes': self.THEMES_WEIGHT,
            'perspectives': self.PERSPECTIVES_WEIGHT,
            'game_modes': self.GAME_MODES_WEIGHT,
            'developers': self.DEVELOPERS_WEIGHT,
            'engines': self.ENGINES_WEIGHT,
        }

        # Сначала добавляем исходную игру, если это реальная игра (не VirtualGame)
        if source_game_id is not None:
            similar_games.append({
                'game_id': source_game_id,
                'game_name': source_game_name,
                'similarity': 100.0,
                'common_keywords': source_data['keyword_count'],
                'common_genres': source_data['genre_count'],
                'common_themes': source_data['theme_count'],
                'common_perspectives': source_data['perspective_count'],
                'common_game_modes': source_data['game_mode_count'],
                'common_engines': source_data['engine_count'],
                'has_single_player': has_single_player,
                'is_source_game': True
            })
            print(f"Добавлена исходная игра '{source_game_name}' с 100% схожести")

        # Обработка кандидатов
        for game_id, data in games_data.items():
            # Пропускаем исходную игру
            if source_game_id and game_id == source_game_id:
                continue

            # Быстрая фильтрация по жанрам
            if has_genres and data['common_genres'] < dynamic_min_common_genres:
                continue

            # Быстрая фильтрация по Single Player
            if has_single_player and not data['has_single_player']:
                continue

            # Расчет схожести
            similarity = 0.0

            # 1. Жанры (процент совпадения от веса)
            if source_genre_count > 0 and data['common_genres'] > 0:
                genre_match_ratio = data['common_genres'] / source_genre_count
                similarity += genre_match_ratio * weights['genres']

            # 2. Ключевые слова - ИСПРАВЛЕНО: каждое совпадение дает KEYWORDS_ADD_PER_MATCH %
            if source_keyword_count > 0 and data['common_keywords'] > 0:
                keyword_score = min(data['common_keywords'] * self.KEYWORDS_ADD_PER_MATCH, weights['keywords'])
                similarity += keyword_score

            # 3. Темы (процент совпадения от веса)
            if source_theme_count > 0 and data['common_themes'] > 0:
                theme_match_ratio = data['common_themes'] / source_theme_count
                similarity += theme_match_ratio * weights['themes']

            # 4. Перспективы (процент совпадения от веса)
            if source_perspective_count > 0 and data['common_perspectives'] > 0:
                perspective_match_ratio = data['common_perspectives'] / source_perspective_count
                similarity += perspective_match_ratio * weights['perspectives']

            # 5. Режимы игры (процент совпадения от веса)
            if source_game_mode_count > 0 and data['common_game_modes'] > 0:
                game_mode_match_ratio = data['common_game_modes'] / source_game_mode_count
                similarity += game_mode_match_ratio * weights['game_modes']

            # 6. Разработчики (процент совпадения от веса)
            if source_developer_count > 0 and data.get('common_developers', 0) > 0:
                developer_match_ratio = data.get('common_developers', 0) / source_developer_count
                similarity += developer_match_ratio * weights['developers']

            # 7. Движки (процент совпадения от веса)
            if source_engine_count > 0 and data.get('common_engines', 0) > 0:
                engine_match_ratio = data.get('common_engines', 0) / source_engine_count
                similarity += engine_match_ratio * weights['engines']

            # Бонус за множественные совпадения
            if similarity > 0:
                active_criteria = sum([
                    source_genre_count > 0 and data['common_genres'] > 0,
                    source_keyword_count > 0 and data['common_keywords'] > 0,
                    source_theme_count > 0 and data['common_themes'] > 0,
                    source_perspective_count > 0 and data['common_perspectives'] > 0,
                    source_game_mode_count > 0 and data['common_game_modes'] > 0,
                    source_developer_count > 0 and data.get('common_developers', 0) > 0,
                    source_engine_count > 0 and data.get('common_engines', 0) > 0,
                ])
                if active_criteria > 1:
                    similarity += 5.0

            similarity = min(100.0, similarity)

            if similarity >= min_similarity:
                similar_games.append({
                    'game_id': game_id,
                    'game_name': data['name'],
                    'similarity': similarity,
                    'common_keywords': data['common_keywords'],
                    'common_genres': data['common_genres'],
                    'common_themes': data['common_themes'],
                    'common_perspectives': data['common_perspectives'],
                    'common_game_modes': data['common_game_modes'],
                    'common_engines': data.get('common_engines', 0),
                    'has_single_player': data['has_single_player'],
                    'is_source_game': False
                })

        print(f"Расчет схожести завершен за {time.time() - calc_time:.2f} сек")
        print(f"Найдено {len(similar_games)} игр выше порога {min_similarity}%")
        return similar_games

    def _get_similarity_cache_key(self, source, target):
        """Генерирует ключ для кэша схожести"""
        if isinstance(source, VirtualGame):
            source_key = f"virtual_{hash(tuple(sorted(source.genre_ids + source.keyword_ids + source.theme_ids + source.game_type_ids + source.engine_ids)))}"
        else:
            source_key = f"game_{source.id}"

        if isinstance(target, VirtualGame):
            target_key = f"virtual_{hash(tuple(sorted(target.genre_ids + target.keyword_ids + target.theme_ids + target.game_type_ids + target.engine_ids)))}"
        else:
            target_key = f"game_{target.id}"

        return f"{source_key}_{target_key}"

    def _get_cached_game_data(self, obj):
        """Получает или кэширует данные игры - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        if isinstance(obj, VirtualGame):
            cache_key = f"virtual_{hash(tuple(sorted(obj.genre_ids + obj.keyword_ids + obj.theme_ids + obj.game_type_ids + obj.engine_ids)))}"
        else:
            cache_key = f"game_{obj.id}"

        if cache_key not in self._game_data_cache:
            # Загружаем данные в первый раз
            self._game_data_cache[cache_key] = {
                'genres': self._get_genres(obj),
                'keywords': self._get_keywords(obj),
                'themes': self._get_themes(obj),
                'developers': self._get_developers(obj),
                'perspectives': self._get_perspectives(obj),
                'game_modes': self._get_game_modes(obj),
                'engines': self._get_engines(obj),
            }

        return self._game_data_cache[cache_key]

    # УНИВЕРСАЛЬНЫЕ МЕТОДЫ ДЛЯ ПОЛУЧЕНИЯ ДАННЫХ
    def _get_genres(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.genre_ids)
        elif hasattr(obj, 'genres'):
            if not hasattr(obj, '_cached_genre_ids'):
                obj._cached_genre_ids = set(obj.genres.values_list('id', flat=True))
            return obj._cached_genre_ids
        return set()

    def _get_keywords(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.keyword_ids)
        elif hasattr(obj, 'keywords'):
            if not hasattr(obj, '_cached_keyword_ids'):
                obj._cached_keyword_ids = set(obj.keywords.values_list('id', flat=True))
            return obj._cached_keyword_ids
        return set()

    def _get_themes(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.theme_ids)
        elif hasattr(obj, 'themes'):
            if not hasattr(obj, '_cached_theme_ids'):
                obj._cached_theme_ids = set(obj.themes.values_list('id', flat=True))
            return obj._cached_theme_ids
        return set()

    def _get_developers(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.developer_ids)
        elif hasattr(obj, 'developers'):
            if not hasattr(obj, '_cached_developer_ids'):
                obj._cached_developer_ids = set(obj.developers.values_list('id', flat=True))
            return obj._cached_developer_ids
        return set()

    def _get_perspectives(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.perspective_ids)
        elif hasattr(obj, 'player_perspectives'):
            if not hasattr(obj, '_cached_perspective_ids'):
                obj._cached_perspectives_ids = set(obj.player_perspectives.values_list('id', flat=True))
            return obj._cached_perspectives_ids
        return set()

    def _get_game_modes(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.game_mode_ids)
        elif hasattr(obj, 'game_modes'):
            if not hasattr(obj, '_cached_game_mode_ids'):
                obj._cached_game_mode_ids = set(obj.game_modes.values_list('id', flat=True))
            return obj._cached_game_mode_ids
        return set()

    # ДОБАВЛЕНО: метод для получения движков
    def _get_engines(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.engine_ids)
        elif hasattr(obj, 'engines'):
            if not hasattr(obj, '_cached_engine_ids'):
                obj._cached_engine_ids = set(obj.engines.values_list('id', flat=True))
            return obj._cached_engine_ids
        return set()

    def find_similar_games(self, source_game, min_similarity=None, limit=None, search_filters=None):
        """
        ОПТИМИЗИРОВАННЫЙ расчет похожих игр - ТОЛЬКО ВЫШЕДШИЕ ИГРЫ - БЕЗ ЛИМИТОВ НА КАНДИДАТЫ

        Args:
            source_game: Исходная игра или VirtualGame
            min_similarity: Минимальный порог схожести
            limit: Лимит результатов
            search_filters: Словарь с поисковыми фильтрами для предварительной фильтрации
        """
        import time
        from django.core.cache import cache
        import json
        import hashlib
        from django.utils import timezone
        from .models import Game

        if limit is None:
            limit = self.DEFAULT_SIMILAR_GAMES_LIMIT

        if min_similarity is None:
            min_similarity = self.DEFAULT_MIN_SIMILARITY

        source_data, single_player_info = self._prepare_source_data(source_game)

        print(f"\n=== SIMILARITY DEBUG ===")
        print(f"Source game: {getattr(source_game, 'id', 'virtual')}")

        # 1. Получаем кандидатов
        candidate_ids = self._get_candidate_ids_new(source_data, single_player_info, min_similarity, search_filters)
        print(f"Candidate IDs found: {len(candidate_ids)}")

        if not candidate_ids:
            print("Нет подходящих кандидатов")
            return []

        # 2. Подготовка данных кандидатов
        games_data = self._prepare_candidate_data(candidate_ids)

        # 3. Подсчет общих элементов
        games_data = self._calculate_common_elements_new(games_data, source_data, candidate_ids)

        # 4. Расчет схожести
        similar_games = self._calculate_similarity_for_candidates(
            games_data, source_data, source_game, single_player_info
        )

        # 5. СОРТИРОВКА: исходная игра (is_source_game=True) всегда первая
        # Затем остальные по убыванию схожести
        similar_games.sort(key=lambda x: (not x.get('is_source_game', False), -x['similarity']))

        # 6. Применяем лимит (если limit > 0)
        if limit > 0:
            similar_games = similar_games[:limit]

        # 7. Загрузка полных объектов
        final_results = self._load_full_objects(similar_games)

        print(f"Найдено {len(final_results)} похожих игр")
        print(f"Исходная игра в результатах: {any(r.get('is_source_game', False) for r in final_results)}")

        return final_results

    # Обновляем _prepare_source_data:
    def _prepare_source_data(self, source_game):
        """Подготовка данных исходной игры - ИСПРАВЛЕННАЯ ВЕРСИЯ"""
        from .models import GameMode

        # Получаем базовые данные из кэша (множества)
        source_data = self._get_cached_game_data(source_game)

        # Получаем ID исходной игры
        source_game_id = None
        if isinstance(source_game, Game):
            source_game_id = source_game.id

        # Преобразуем множества в списки для всех полей
        source_genre_ids = list(source_data.get('genres', set()))
        source_genre_count = len(source_genre_ids)

        source_keyword_ids = list(source_data.get('keywords', set()))
        source_keyword_count = len(source_keyword_ids)

        source_theme_ids = list(source_data.get('themes', set()))
        source_theme_count = len(source_theme_ids)

        source_perspective_ids = list(source_data.get('perspectives', set()))
        source_perspective_count = len(source_perspective_ids)

        source_game_mode_ids = list(source_data.get('game_modes', set()))
        source_game_mode_count = len(source_game_mode_ids)

        source_developer_ids = list(source_data.get('developers', set()))
        source_developer_count = len(source_developer_ids)

        source_engine_ids = list(source_data.get('engines', set()))
        source_engine_count = len(source_engine_ids)

        # Проверяем, есть ли у исходной игры режим Single player
        has_single_player_in_source = False
        single_player_mode_id = None

        if source_game_mode_ids:
            single_player_mode = GameMode.objects.filter(name__iexact='single player').first()
            if single_player_mode:
                has_single_player_in_source = single_player_mode.id in source_game_mode_ids
                single_player_mode_id = single_player_mode.id
            else:
                # Если режим не найден, ищем альтернативные варианты
                alternative_names = ['single', 'singleplayer', 'single-player', '1 player']
                for alt_name in alternative_names:
                    alt_mode = GameMode.objects.filter(name__iexact=alt_name).first()
                    if alt_mode:
                        has_single_player_in_source = alt_mode.id in source_game_mode_ids
                        single_player_mode_id = alt_mode.id
                        break

        # Определяем динамическое минимальное требование по жанрам
        if source_genre_count > 0:
            if source_genre_count >= 2:
                dynamic_min_common_genres = 2
            elif source_genre_count == 1:
                dynamic_min_common_genres = 1
            else:
                dynamic_min_common_genres = 0
        else:
            dynamic_min_common_genres = 0

        # СОЗДАЕМ НОВЫЙ СЛОВАРЬ со всеми нужными полями
        enhanced_source_data = {
            'game_id': source_game_id,
            'genre_ids': source_genre_ids,
            'genre_count': source_genre_count,
            'keyword_ids': source_keyword_ids,
            'keyword_count': source_keyword_count,
            'theme_ids': source_theme_ids,
            'theme_count': source_theme_count,
            'perspective_ids': source_perspective_ids,
            'perspective_count': source_perspective_count,
            'game_mode_ids': source_game_mode_ids,
            'game_mode_count': source_game_mode_count,
            'engine_ids': source_engine_ids,
            'engine_count': source_engine_count,
            'developer_ids': source_developer_ids,
            'developer_count': source_developer_count,
            'single_player_mode_id': single_player_mode_id,
            # Сохраняем оригинальные множества для обратной совместимости
            'genres': source_data.get('genres', set()),
            'keywords': source_data.get('keywords', set()),
            'themes': source_data.get('themes', set()),
            'perspectives': source_data.get('perspectives', set()),
            'game_modes': source_data.get('game_modes', set()),
            'developers': source_data.get('developers', set()),
            'engines': source_data.get('engines', set()),
        }

        single_player_info = {
            'has_single_player': has_single_player_in_source,
            'single_player_mode_id': single_player_mode_id,
            'dynamic_min_common_genres': dynamic_min_common_genres,
            'has_genres': source_genre_count > 0,
            'has_themes': source_theme_count > 0,
            'has_keywords': source_keyword_count > 0
        }

        return enhanced_source_data, single_player_info

    def _calculate_and_filter_similarity(self, games_data, source_game, source_data, min_similarity,
                                         single_player_info):
        """Расчет схожести и фильтрация по минимальному порогу с динамическими весами"""
        import time

        print("Этап 4: Расчет схожести с динамическими весами...")
        calc_time = time.time()

        similar_games = []
        max_similarity = 0
        max_game_name = ""

        for game_id, data in games_data.items():
            similarity = 0.0

            # Если это исходная игра (для реальных игр)
            if isinstance(source_game, Game) and game_id == source_game.id:
                similarity = 100.0
                print(f"Исходная игра '{data['name']}' добавлена с 100% схожести")
            else:
                # Проверяем требование по общих жанрам (ТОЛЬКО если есть исходные жанры)
                dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']
                has_genres = single_player_info['has_genres']

                if has_genres and data['common_genres'] < dynamic_min_common_genres:
                    continue

                # Проверяем требование по Single player
                if single_player_info['has_single_player'] and not data['has_single_player']:
                    continue

                # Расчет схожести с динамическими весами
                similarity = self._calculate_game_similarity_new(
                    source_data['genre_count'],
                    source_data['keyword_count'],
                    source_data['theme_count'],
                    source_data['developer_count'],
                    source_data['perspective_count'],
                    source_data['game_mode_count'],
                    source_data['engine_count'],
                    data,
                    source_data
                )

            # Отслеживаем максимальную схожесть
            if game_id != getattr(source_game, 'id', None) and similarity > max_similarity:
                max_similarity = similarity
                max_game_name = data['name']

            # Добавляем если превышает порог
            if similarity >= min_similarity:
                similar_games.append({
                    'game_id': game_id,
                    'game_name': data['name'],
                    'similarity': similarity,
                    'common_keywords': data['common_keywords'],
                    'common_genres': data['common_genres'],
                    'common_themes': data['common_themes'],
                    'common_engines': data.get('common_engines', 0),
                    'has_single_player': data['has_single_player'],
                    'is_source_game': (isinstance(source_game, Game) and game_id == source_game.id)
                })

        print(f"Расчет схожести завершен за {time.time() - calc_time:.2f} сек")
        print(f"Максимальная найденная схожесть: {max_similarity:.1f}% (игра: {max_game_name})")
        print(f"Найдено {len(similar_games)} игр выше порога {min_similarity}%")

        return similar_games

    # В similarity.py, обновляем метод _get_candidate_ids:
    def _get_candidate_ids(self, source_data, single_player_info):
        """Получение ID кандидатов - ТОЛЬКО ВЫШЕДШИЕ ИГРЫ"""
        import time
        from django.db import connection
        from django.utils import timezone

        print("Поиск кандидатов (только вышедшие игры)...")
        start_time = time.time()

        candidate_ids = []
        dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']
        has_single_player = single_player_info['has_single_player']
        single_player_mode_id = single_player_info['single_player_mode_id']
        source_genre_ids = source_data['genre_ids']
        source_engine_ids = source_data.get('engine_ids', [])
        current_time = timezone.now()

        # ОТСЛЕЖИВАЕМ - используем ли мы логику с жанрами
        use_genre_logic = bool(source_genre_ids)

        print(f"Исходные данные: жанров={len(source_genre_ids)}, используем логику жанров={use_genre_logic}")

        # Получаем ID игр с общими жанрами (ТОЛЬКО если есть исходные жанры)
        if use_genre_logic and source_genre_ids:
            with connection.cursor() as cursor:
                source_genre_ids_str = ','.join(map(str, source_genre_ids))

                # Основной запрос с фильтрацией по дате
                query = f"""
                    SELECT ggg.game_id, COUNT(*) as common_count
                    FROM games_game_genres ggg
                    INNER JOIN games_game g ON ggg.game_id = g.id
                    WHERE ggg.genre_id IN ({source_genre_ids_str})
                    AND g.first_release_date IS NOT NULL
                    AND g.first_release_date <= %s
                """

                query += f"""
                    GROUP BY ggg.game_id
                    HAVING COUNT(*) >= {dynamic_min_common_genres}
                    ORDER BY common_count DESC
                    LIMIT 500
                """

                cursor.execute(query, [current_time])
                candidate_ids = [row[0] for row in cursor.fetchall()]
                print(f"Найдено кандидатов по жанрам: {len(candidate_ids)}")
        elif not use_genre_logic and dynamic_min_common_genres == 0:
            # Если не выбраны жанры, ищем по популярности без ограничений по жанрам
            from .models import Game
            queryset = Game.objects.filter(
                first_release_date__isnull=False,
                first_release_date__lte=current_time
            )

            # Проверяем другие критерии для оптимизации
            other_criteria_count = (
                    len(source_data['keyword_ids']) +
                    len(source_data['theme_ids']) +
                    len(source_data['perspective_ids']) +
                    len(source_data['game_mode_ids']) +
                    len(source_data['engine_ids'])
            )

            if other_criteria_count > 0:
                # Если есть другие критерии, ограничиваем выборку сильнее
                candidate_ids = list(
                    queryset.order_by('-rating_count')
                    .values_list('id', flat=True)[:800]  # Берем больше кандидатов
                )
            else:
                # Если совсем нет критериев, берем популярные игры
                candidate_ids = list(
                    queryset.order_by('-rating_count')
                    .values_list('id', flat=True)[:200]
                )

            print(f"Найдено кандидатов без жанров: {len(candidate_ids)}")

        # Фильтруем по Single player (если требуется)
        if has_single_player and single_player_mode_id and candidate_ids:
            with connection.cursor() as cursor:
                candidate_ids_str = ','.join(map(str, candidate_ids))

                query = f"""
                    SELECT DISTINCT game_id
                    FROM games_game_game_modes 
                    WHERE gamemode_id = {single_player_mode_id}
                    AND game_id IN ({candidate_ids_str})
                """
                cursor.execute(query)
                games_with_single_player = set([row[0] for row in cursor.fetchall()])

                candidate_ids = [game_id for game_id in candidate_ids
                                 if game_id in games_with_single_player]

            print(f"После фильтра Single player: {len(candidate_ids)}")

        print(f"Всего найдено {len(candidate_ids)} кандидатов за {time.time() - start_time:.2f} сек")
        return candidate_ids

    def _generate_cache_key(self, source_game, min_similarity, limit, source_data, single_player_info):
        """Генерация ключа кэша"""
        import json
        import hashlib

        if isinstance(source_game, VirtualGame):
            cache_key_data = {
                'type': 'virtual',
                'genre_ids': sorted(source_game.genre_ids),
                'keyword_ids': sorted(source_game.keyword_ids),
                'theme_ids': sorted(source_game.theme_ids),
                'game_type_ids': sorted(source_game.game_type_ids),
                'engine_ids': sorted(source_game.engine_ids),
                'min_similarity': min_similarity,
                'dynamic_min_common_genres': single_player_info['dynamic_min_common_genres'],
                'has_single_player': single_player_info['has_single_player'],
                'limit': limit,
                'version': 'v_with_game_types_and_engines'
            }
        else:
            cache_key_data = {
                'type': 'game',
                'game_id': source_game.id,
                'min_similarity': min_similarity,
                'dynamic_min_common_genres': single_player_info['dynamic_min_common_genres'],
                'has_single_player': single_player_info['has_single_player'],
                'game_type': getattr(source_game, 'game_type', None),
                'engines': sorted([e.id for e in source_game.engines.all()]),
                'limit': limit,
                'version': 'v_with_game_types_and_engines'
            }

        cache_key_str = json.dumps(cache_key_data, sort_keys=True)
        return f'game_similarity_{hashlib.md5(cache_key_str.encode()).hexdigest()}'

    def _calculate_common_elements(self, games_data, source_data, candidate_ids):
        """Подсчет общих элементов - упрощенный"""
        import time
        from django.db import connection

        print("Подсчет общих элементов...")
        start_time = time.time()

        if not candidate_ids:
            return games_data

        candidate_ids_str = ','.join(map(str, candidate_ids))

        # Простой запрос без фильтра по дате (кандидаты уже отфильтрованы)
        with connection.cursor() as cursor:
            query = f"""
                SELECT 
                    g.id as game_id,

                    -- Общие жанры
                    (
                        SELECT COUNT(DISTINCT genre_id)
                        FROM games_game_genres
                        WHERE game_id = g.id 
                        AND genre_id IN %s
                    ) as common_genres,

                    -- Общие ключевые слова
                    (
                        SELECT COUNT(DISTINCT keyword_id)
                        FROM games_game_keywords
                        WHERE game_id = g.id 
                        AND keyword_id IN %s
                    ) as common_keywords,

                    -- Общие темы
                    (
                        SELECT COUNT(DISTINCT theme_id)
                        FROM games_game_themes
                        WHERE game_id = g.id 
                        AND theme_id IN %s
                    ) as common_themes,

                    -- Single player check
                    CASE WHEN EXISTS (
                        SELECT 1 FROM games_game_game_modes
                        WHERE game_id = g.id 
                        AND gamemode_id = %s
                    ) THEN 1 ELSE 0 END as has_single_player

                FROM games_game g
                WHERE g.id IN ({candidate_ids_str})
            """

            # Подготавливаем параметры
            source_genre_ids = source_data.get('genre_ids', [])
            source_keyword_ids = source_data.get('keyword_ids', [])
            source_theme_ids = source_data.get('theme_ids', [])
            single_player_mode_id = source_data.get('single_player_mode_id')

            cursor.execute(query, (
                tuple(source_genre_ids) if source_genre_ids else (0,),
                tuple(source_keyword_ids) if source_keyword_ids else (0,),
                tuple(source_theme_ids) if source_theme_ids else (0,),
                single_player_mode_id or 0
            ))

            for row in cursor.fetchall():
                game_id = row[0]
                if game_id in games_data:
                    games_data[game_id].update({
                        'common_genres': row[1],
                        'common_keywords': row[2],
                        'common_themes': row[3],
                        'has_single_player': bool(row[4]),
                    })

        print(f"Подсчет завершен за {time.time() - start_time:.2f} сек")
        return games_data

    def _sort_and_limit_results(self, similar_games, source_game, limit):
        """Сортировка результатов и ограничение по лимиту"""
        import time

        print("Этап 5: Сортировка результатов...")
        sort_time = time.time()

        # Разделяем исходную игру и остальные
        source_game_items = [item for item in similar_games if item.get('is_source_game', False)]
        other_game_items = [item for item in similar_games if not item.get('is_source_game', False)]

        # Сортируем остальные по убыванию схожести
        other_game_items.sort(key=lambda x: x['similarity'], reverse=True)

        # Объединяем: исходная игра первая, затем остальные
        similar_games = source_game_items + other_game_items
        similar_games = similar_games[:limit]

        print(f"Сортировка завершена за {time.time() - sort_time:.2f} сек")
        return similar_games

    def _get_single_player_mode_id(self):
        """Получение ID режима Single player"""
        from .models import GameMode

        single_player_mode = GameMode.objects.filter(name__iexact='single player').first()
        if single_player_mode:
            return single_player_mode.id

        # Поиск альтернативных названий
        alternative_names = ['single', 'singleplayer', 'single-player', '1 player']
        for alt_name in alternative_names:
            alt_mode = GameMode.objects.filter(name__iexact=alt_name).first()
            if alt_mode:
                return alt_mode.id

        return None

    def _calculate_game_similarity(self, source_genre_count, source_keyword_count, source_theme_count,
                                   source_developer_count, source_perspective_count, source_game_mode_count,
                                   target_data):
        """Упрощенный и оптимизированный расчет схожести"""
        similarity = 0.0

        # 1. ЖАНРЫ (30%) - упрощенный расчет
        if source_genre_count > 0 and target_data['common_genres'] > 0:
            # Используем коэффициент совпадения
            genre_match_ratio = target_data['common_genres'] / max(source_genre_count, target_data['total_genres'])
            similarity += genre_match_ratio * self.GENRES_WEIGHT

        # 2. КЛЮЧЕВЫЕ СЛОВА (30%) - упрощенный
        if target_data['common_keywords'] > 0:
            # Ограничиваем максимальный вклад
            max_keyword_contrib = min(target_data['common_keywords'] * 2.0, self.KEYWORDS_WEIGHT)
            similarity += max_keyword_contrib

        # 3. ТЕМЫ (20%) - упрощенный
        if source_theme_count > 0 and target_data['common_themes'] > 0:
            theme_match_ratio = target_data['common_themes'] / max(source_theme_count, target_data['total_themes'])
            similarity += theme_match_ratio * self.THEMES_WEIGHT

        # 4-6. Остальные компоненты - фиксированные маленькие вклады если есть совпадения
        if target_data['common_developers'] > 0:
            similarity += self.DEVELOPERS_WEIGHT * 0.5

        if target_data['common_perspectives'] > 0:
            similarity += self.PERSPECTIVES_WEIGHT * 0.7

        if target_data['common_game_modes'] > 0:
            similarity += self.GAME_MODES_WEIGHT * 0.5

        return min(100.0, similarity)  # Ограничиваем 100%

    def _calculate_keyword_similarity_per_match(self, source_keywords, target_keywords):
        """
        Расчет схожести ключевых слов
        - За каждое совпадающее ключевое слово добавляется 1%
        - Максимальный результат ограничен KEYWORDS_WEIGHT (30%)
        """
        if not source_keywords or not target_keywords:
            return 0.0

        # Находим количество совпадающих ключевых слов
        if len(source_keywords) <= len(target_keywords):
            # Перебираем меньшее множество для оптимизации
            common_count = sum(1 for keyword in source_keywords if keyword in target_keywords)
        else:
            common_count = sum(1 for keyword in target_keywords if keyword in source_keywords)

        # Каждое совпадение добавляет 1%, но не больше максимального веса
        similarity = min(common_count * self.KEYWORDS_ADD_PER_MATCH, self.KEYWORDS_WEIGHT)

        return similarity

    def _calculate_set_similarity(self, set1, set2, max_score):
        """
        Расчет схожести для множеств (для тем, разработчиков, перспектив, режимов игры, движков)
        Использует коэффициент Жаккара
        """
        if not set1 and not set2:
            return max_score

        if not set1 or not set2:
            return 0.0

        set1_len = len(set1)
        set2_len = len(set2)

        # Для небольших множеств используем прямое сравнение
        if set1_len < set2_len:
            # Перебираем меньшее множество
            common_count = sum(1 for item in set1 if item in set2)
        else:
            common_count = sum(1 for item in set2 if item in set1)

        total_count = set1_len + set2_len - common_count

        if total_count > 0:
            overlap_ratio = common_count / total_count
            return overlap_ratio * max_score

        return 0.0

    def clear_cache(self):
        """Очищает кэш (полезно при изменении данных)"""
        self._similarity_cache.clear()
        self._game_data_cache.clear()

    def batch_calculate_similarities(self, source_game, target_games):
        """
        Пакетный расчет схожести для списка игр
        Полезно для массовых операций
        """
        results = []
        source_data = self._get_cached_game_data(source_game)

        for target_game in target_games:
            similarity = self.calculate_similarity(source_game, target_game)
            results.append({
                'game': target_game,
                'similarity': similarity
            })

        return results
