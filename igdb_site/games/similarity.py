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
    KEYWORDS_ADD_PER_MATCH = 0.4

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
        ИСПРАВЛЕННЫЙ поиск кандидатов через ArrayField + GIN - БЕЗ ЛИМИТОВ.
        Теперь поддерживает поисковые фильтры (search_filters) с OR/AND логикой.
        """
        import time
        from django.utils import timezone
        from django.db.models import Q
        from games.views_parts.base_views import _apply_search_filters

        print("БЫСТРЫЙ поиск кандидатов через ArrayField + GIN (БЕЗ ЛИМИТОВ)...")
        print(f"DEBUG: search_filters received = {search_filters}")
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

        print(
            f"Критерии: жанры={len(source_genre_ids)}, темы={len(source_theme_ids)}, движки={len(source_engine_ids)}, "
            f"мин. общих жанров={dynamic_min_common_genres}")

        from games.models import Game

        base_qs = Game.objects.filter(
            first_release_date__isnull=False,
            first_release_date__lte=current_time
        )

        print(f"DEBUG: Всего вышедших игр: {base_qs.count()}")

        # === ПРИМЕНЯЕМ ПОИСКОВЫЕ ФИЛЬТРЫ С OR/AND ЛОГИКОЙ ===
        if search_filters:
            print("Применяем поисковые фильтры...")
            print(f"DEBUG: search_filters = {search_filters}")

            # ПРОВЕРКА: сколько игр с выбранным движком вообще есть
            if search_filters.get('engines'):
                engine_ids = search_filters['engines']
                games_with_engine = Game.objects.filter(engines__id__in=engine_ids)
                print(f"DEBUG: Всего игр с выбранными движками {engine_ids}: {games_with_engine.count()}")
                if games_with_engine.count() > 0:
                    sample_engine = list(games_with_engine.values_list('id', flat=True)[:10])
                    print(f"DEBUG: Примеры ID игр с выбранными движками: {sample_engine}")

            filtered_qs = _apply_search_filters(base_qs, search_filters)
            filtered_count = filtered_qs.count()
            print(f"После применения всех поисковых фильтров: {filtered_count} игр")

            # ПРОВЕРКА: сколько из прошедших фильтры имеют выбранный движок
            if search_filters.get('engines'):
                engine_ids = search_filters['engines']
                filtered_with_engine = filtered_qs.filter(engines__id__in=engine_ids)
                print(f"DEBUG: Из прошедших фильтры, с выбранным движком: {filtered_with_engine.count()}")
                if filtered_with_engine.count() > 0:
                    sample_filtered_with_engine = list(filtered_with_engine.values_list('id', flat=True)[:10])
                    print(f"DEBUG: Примеры ID с движком после всех фильтров: {sample_filtered_with_engine}")
        else:
            print("DEBUG: Нет поисковых фильтров")
            filtered_qs = base_qs

        candidate_ids = []

        # Исходная игра
        if source_game_id and source_game_id > 0:
            if search_filters:
                source_game_obj = Game.objects.filter(id=source_game_id).first()
                if source_game_obj and self._game_passes_search_filters(source_game_obj, search_filters):
                    candidate_ids.append(source_game_id)
                    print(f"Добавлена исходная игра ID {source_game_id} (прошла фильтры)")
                else:
                    print(f"Исходная игра ID {source_game_id} НЕ прошла поисковые фильтры")
            else:
                candidate_ids.append(source_game_id)

        # ===== СЛУЧАЙ 1: Есть жанры =====
        if source_genre_ids:
            print(f"\nDEBUG: Ищем игры с общими жанрами")

            # Ищем игры с общими жанрами ТОЛЬКО среди прошедших поисковые фильтры
            games_with_overlap = filtered_qs.filter(
                genre_ids__overlap=source_genre_ids
            ).exclude(id=source_game_id).distinct()

            overlap_count = games_with_overlap.count()
            print(f"DEBUG: Игр с общими жанрами (после поисковых фильтров): {overlap_count}")

            if overlap_count > 0:
                sample_overlap = list(games_with_overlap.values_list('id', flat=True)[:10])
                print(f"DEBUG: Примеры ID с общими жанрами: {sample_overlap}")

                # ПРОВЕРКА: есть ли среди них игры с выбранным движком
                if search_filters and search_filters.get('engines'):
                    engine_ids = search_filters['engines']
                    overlap_with_engine = games_with_overlap.filter(engines__id__in=engine_ids)
                    print(f"DEBUG: Из них с выбранным движком: {overlap_with_engine.count()}")
                    if overlap_with_engine.count() > 0:
                        sample_overlap_engine = list(overlap_with_engine.values_list('id', flat=True)[:10])
                        print(f"DEBUG: Примеры ID с общими жанрами И выбранным движком: {sample_overlap_engine}")

            if overlap_count == 0:
                print("DEBUG: НЕТ игр с общими жанрами, прошедших поисковые фильтры")
                return candidate_ids

            # Загружаем genre_ids
            candidates_data = list(games_with_overlap.values('id', 'genre_ids'))

            # Фильтруем по количеству общих жанров
            source_set = set(source_genre_ids)
            filtered_ids = []

            for item in candidates_data:
                game_genres = set(item['genre_ids'])
                common_count = len(source_set & game_genres)
                if common_count >= dynamic_min_common_genres:
                    filtered_ids.append(item['id'])

            print(f"DEBUG: После фильтрации по min_common_genres={dynamic_min_common_genres}: {len(filtered_ids)} игр")

            if filtered_ids:
                popular_games = Game.objects.filter(
                    id__in=filtered_ids
                ).order_by('-rating_count').values_list('id', flat=True)
                candidate_ids.extend(list(popular_games))

        # ===== СЛУЧАЙ 2: Нет жанров =====
        elif (source_theme_ids or source_engine_ids) and not source_genre_ids:
            filter_condition = Q()
            if source_theme_ids:
                filter_condition |= Q(theme_ids__overlap=source_theme_ids)
            if source_engine_ids:
                filter_condition |= Q(engine_ids__overlap=source_engine_ids)

            candidates = filtered_qs.filter(filter_condition).exclude(id=source_game_id).distinct()
            other_candidates = list(candidates.order_by('-rating_count').values_list('id', flat=True))
            candidate_ids.extend(other_candidates)
            print(f"Найдено кандидатов по темам/движкам: {len(other_candidates)}")

        # ===== СЛУЧАЙ 3: Другие критерии =====
        elif source_keyword_ids or source_perspective_ids or source_game_mode_ids:
            filter_condition = Q()
            if source_keyword_ids:
                filter_condition |= Q(keyword_ids__overlap=source_keyword_ids)
            if source_perspective_ids:
                filter_condition |= Q(perspective_ids__overlap=source_perspective_ids)
            if source_game_mode_ids:
                filter_condition |= Q(game_mode_ids__overlap=source_game_mode_ids)

            candidates = filtered_qs.filter(filter_condition).exclude(id=source_game_id).distinct()
            if has_single_player and single_player_mode_id:
                candidates = candidates.filter(game_mode_ids__contains=[single_player_mode_id])
            other_candidates = list(candidates.order_by('-rating_count').values_list('id', flat=True))
            candidate_ids.extend(other_candidates)

        # ===== СЛУЧАЙ 4: Нет критериев =====
        else:
            other_candidates = list(
                filtered_qs.exclude(id=source_game_id)
                .order_by('-rating_count')
                .values_list('id', flat=True)
            )
            candidate_ids.extend(other_candidates)

        # Убираем дубликаты
        seen = set()
        unique_candidates = []
        for game_id in candidate_ids:
            if game_id not in seen:
                seen.add(game_id)
                unique_candidates.append(game_id)

        print(f"\nИТОГО найдено {len(unique_candidates)} уникальных кандидатов за {time.time() - start_time:.2f} сек")
        if unique_candidates:
            print(f"Первые 10 ID: {unique_candidates[:10]}")

        return unique_candidates

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
        """ОПТИМИЗИРОВАННЫЙ подсчет общих элементов - с разбивкой по частям для скорости"""
        import time
        from django.db import connection
        from collections import defaultdict

        print("ОПТИМИЗИРОВАННЫЙ подсчет общих элементов (с разбивкой)...")
        start_time = time.time()

        if not candidate_ids:
            return games_data

        candidate_ids_str = ','.join(map(str, candidate_ids))

        source_genre_ids = source_data.get('genre_ids', [])
        source_keyword_ids = source_data.get('keyword_ids', [])
        source_theme_ids = source_data.get('theme_ids', [])
        source_perspective_ids = source_data.get('perspective_ids', [])
        source_game_mode_ids = source_data.get('game_mode_ids', [])
        source_engine_ids = source_data.get('engine_ids', [])
        single_player_mode_id = source_data.get('single_player_mode_id')

        # Инициализируем счетчики для всех игр
        for game_id in games_data:
            games_data[game_id].update({
                'common_genres': 0,
                'common_keywords': 0,
                'common_themes': 0,
                'common_perspectives': 0,
                'common_game_modes': 0,
                'common_engines': 0,
                'has_single_player': False,
            })

        with connection.cursor() as cursor:
            # 1. ЖАНРЫ - отдельный быстрый запрос
            if source_genre_ids:
                genre_query = f"""
                    SELECT game_id, COUNT(DISTINCT genre_id) as cnt
                    FROM games_game_genres
                    WHERE game_id IN ({candidate_ids_str})
                    AND genre_id IN %s
                    GROUP BY game_id
                """
                cursor.execute(genre_query, (tuple(source_genre_ids),))
                for game_id, cnt in cursor.fetchall():
                    if game_id in games_data:
                        games_data[game_id]['common_genres'] = cnt

            # 2. КЛЮЧЕВЫЕ СЛОВА - отдельный запрос
            if source_keyword_ids:
                keyword_query = f"""
                    SELECT game_id, COUNT(DISTINCT keyword_id) as cnt
                    FROM games_game_keywords
                    WHERE game_id IN ({candidate_ids_str})
                    AND keyword_id IN %s
                    GROUP BY game_id
                """
                cursor.execute(keyword_query, (tuple(source_keyword_ids),))
                for game_id, cnt in cursor.fetchall():
                    if game_id in games_data:
                        games_data[game_id]['common_keywords'] = cnt

            # 3. ТЕМЫ - отдельный запрос
            if source_theme_ids:
                theme_query = f"""
                    SELECT game_id, COUNT(DISTINCT theme_id) as cnt
                    FROM games_game_themes
                    WHERE game_id IN ({candidate_ids_str})
                    AND theme_id IN %s
                    GROUP BY game_id
                """
                cursor.execute(theme_query, (tuple(source_theme_ids),))
                for game_id, cnt in cursor.fetchall():
                    if game_id in games_data:
                        games_data[game_id]['common_themes'] = cnt

            # 4. ПЕРСПЕКТИВЫ - отдельный запрос
            if source_perspective_ids:
                perspective_query = f"""
                    SELECT game_id, COUNT(DISTINCT playerperspective_id) as cnt
                    FROM games_game_player_perspectives
                    WHERE game_id IN ({candidate_ids_str})
                    AND playerperspective_id IN %s
                    GROUP BY game_id
                """
                cursor.execute(perspective_query, (tuple(source_perspective_ids),))
                for game_id, cnt in cursor.fetchall():
                    if game_id in games_data:
                        games_data[game_id]['common_perspectives'] = cnt

            # 5. РЕЖИМЫ ИГРЫ - отдельный запрос
            if source_game_mode_ids:
                gamemode_query = f"""
                    SELECT game_id, COUNT(DISTINCT gamemode_id) as cnt
                    FROM games_game_game_modes
                    WHERE game_id IN ({candidate_ids_str})
                    AND gamemode_id IN %s
                    GROUP BY game_id
                """
                cursor.execute(gamemode_query, (tuple(source_game_mode_ids),))
                for game_id, cnt in cursor.fetchall():
                    if game_id in games_data:
                        games_data[game_id]['common_game_modes'] = cnt

            # 6. ДВИЖКИ - отдельный запрос
            if source_engine_ids:
                engine_query = f"""
                    SELECT game_id, COUNT(DISTINCT gameengine_id) as cnt
                    FROM games_game_engines
                    WHERE game_id IN ({candidate_ids_str})
                    AND gameengine_id IN %s
                    GROUP BY game_id
                """
                cursor.execute(engine_query, (tuple(source_engine_ids),))
                for game_id, cnt in cursor.fetchall():
                    if game_id in games_data:
                        games_data[game_id]['common_engines'] = cnt

            # 7. SINGLE PLAYER - отдельный запрос
            if single_player_mode_id:
                sp_query = f"""
                    SELECT DISTINCT game_id
                    FROM games_game_game_modes
                    WHERE game_id IN ({candidate_ids_str})
                    AND gamemode_id = %s
                """
                cursor.execute(sp_query, (single_player_mode_id,))
                for row in cursor.fetchall():
                    game_id = row[0]
                    if game_id in games_data:
                        games_data[game_id]['has_single_player'] = True

        print(f"Подсчет с разбивкой завершен за {time.time() - start_time:.2f} сек")
        return games_data

    def _prepare_candidate_data(self, candidate_ids):
        """ОПТИМИЗИРОВАННАЯ подготовка данных кандидатов"""
        import time
        from .models import Game
        from django.db import connection

        print("ОПТИМИЗИРОВАННАЯ подготовка данных...")
        prep_time = time.time()

        games_data = {}

        if not candidate_ids:
            return games_data

        candidate_ids_str = ','.join(map(str, candidate_ids))

        with connection.cursor() as cursor:
            # Один запрос для получения имен игр
            query = f"""
                SELECT id, name 
                FROM games_game 
                WHERE id IN ({candidate_ids_str})
            """
            cursor.execute(query)

            for row in cursor.fetchall():
                game_id, game_name = row
                games_data[game_id] = {
                    'id': game_id,
                    'name': game_name,
                    'common_keywords': 0,
                    'common_genres': 0,
                    'common_themes': 0,
                    'common_developers': 0,
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
        НОВЫЙ расчет схожести - с ФИКСИРОВАННЫМИ весами.
        Ключевые слова считаются по проценту совпадения (common/source_total * 100%).
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

        # 2. КЛЮЧЕВЫЕ СЛОВА - ИСПРАВЛЕНО: рассчитываем как процент совпадения
        if self.KEYWORDS_WEIGHT > 0 and source_keyword_count > 0:
            if target_data.get('common_keywords', 0) > 0:
                keyword_match_ratio = target_data['common_keywords'] / max(source_keyword_count, 1)
                similarity += keyword_match_ratio * self.KEYWORDS_WEIGHT

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
        Детальная разбивка похожести по компонентам с ФИКСИРОВАННЫМИ весами.
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

        # Фиксированные веса, а не динамические
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

        # Ключевые слова - процент совпадения
        if max_scores['keywords'] > 0 and source_data['keyword_count'] > 0:
            if target_data['common_keywords'] > 0:
                keyword_match_ratio = target_data['common_keywords'] / max(source_data['keyword_count'], 1)
                scores['keywords'] = keyword_match_ratio * max_scores['keywords']
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
            'dynamic_weights': max_scores,  # Переименовано для совместимости, но теперь это просто фиксированные веса
            'total_similarity': total,
            'bonus': bonus,
            'total_without_bonus': total_without_bonus
        }

    def _calculate_similarity_for_candidates(self, games_data, source_data, source_game, single_player_info):
        """Расчет схожести для всех кандидатов с ФИКСИРОВАННЫМИ весами."""
        import time

        print("Расчет схожести для кандидатов с фиксированными весами...")
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

        # Используем константу класса
        min_similarity = self.DEFAULT_MIN_SIMILARITY

        # Выводим информацию о весах (теперь они фиксированные)
        print(f"Фиксированные веса:")
        print(f"  - Жанры: {self.GENRES_WEIGHT:.1f}%")
        print(f"  - Ключевые слова: {self.KEYWORDS_WEIGHT:.1f}%")
        print(f"  - Темы: {self.THEMES_WEIGHT:.1f}%")
        print(f"  - Перспективы: {self.PERSPECTIVES_WEIGHT:.1f}%")
        print(f"  - Режимы игры: {self.GAME_MODES_WEIGHT:.1f}%")
        print(f"  - Разработчики: {self.DEVELOPERS_WEIGHT:.1f}%")
        print(f"  - Движки: {self.ENGINES_WEIGHT:.1f}%")

        print(f"Условия фильтрации:")
        print(f"  - Есть жанры: {has_genres}")
        print(f"  - Требуется общих жанров: {dynamic_min_common_genres} (только если есть жанры)")
        print(f"  - Требуется Single player: {has_single_player}")
        print(f"  - Минимальный порог схожести: {min_similarity}% (константа DEFAULT_MIN_SIMILARITY)")

        for game_id, data in games_data.items():
            # Если это исходная игра
            if isinstance(source_game, Game) and game_id == source_game.id:
                similar_games.append({
                    'game_id': game_id,
                    'game_name': data['name'],
                    'similarity': 100.0,
                    'common_keywords': data['common_keywords'],
                    'common_genres': data['common_genres'],
                    'common_themes': data['common_themes'],
                    'common_perspectives': data['common_perspectives'],
                    'common_game_modes': data['common_game_modes'],
                    'common_engines': data.get('common_engines', 0),
                    'has_single_player': data['has_single_player'],
                    'is_source_game': True
                })
                continue

            # Проверяем требование по общим жанрам (ТОЛЬКО если есть исходные жанры)
            if has_genres and data['common_genres'] < dynamic_min_common_genres:
                continue

            # Проверяем требование по Single player
            if has_single_player and not data['has_single_player']:
                continue

            # Расчет схожести с фиксированными весами (вызов изменённого метода)
            similarity = self._calculate_game_similarity_new(
                source_genre_count, source_keyword_count, source_theme_count,
                source_developer_count, source_perspective_count, source_game_mode_count,
                source_engine_count,
                data, source_data
            )

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
        from django.db import connection
        from django.core.cache import cache
        import json
        import hashlib
        from django.utils import timezone
        from .models import Game, GameMode

        if limit is None:
            limit = self.DEFAULT_SIMILAR_GAMES_LIMIT

        # Используем константу, если не передан порог
        if min_similarity is None:
            min_similarity = self.DEFAULT_MIN_SIMILARITY

        # 1. Подготовка данных исходной игры
        source_data, single_player_info = self._prepare_source_data(source_game)

        print(f"\n=== SIMILARITY DEBUG ===")
        print(f"Source game: {getattr(source_game, 'id', 'virtual')}")
        print(f"Source data - genres: {source_data['genre_count']}, keywords: {source_data['keyword_count']}")
        print(f"Source data - themes: {source_data['theme_count']}, perspectives: {source_data['perspective_count']}")
        print(f"Source data - game_modes: {source_data['game_mode_count']}, engines: {source_data['engine_count']}")

        if search_filters:
            print(f"Search filters applied:")
            if search_filters.get('platforms'): print(f"  - platforms: {search_filters['platforms']}")
            if search_filters.get('game_modes'): print(f"  - game_modes: {search_filters['game_modes']}")
            if search_filters.get('genres'): print(f"  - genres: {search_filters['genres']}")
            if search_filters.get('keywords'): print(f"  - keywords: {search_filters['keywords']}")
            if search_filters.get('themes'): print(f"  - themes: {search_filters['themes']}")
            if search_filters.get('perspectives'): print(f"  - perspectives: {search_filters['perspectives']}")
            if search_filters.get('game_types'): print(f"  - game_types: {search_filters['game_types']}")
            if search_filters.get('engines'): print(f"  - engines: {search_filters['engines']}")
            if search_filters.get('release_year_start') or search_filters.get('release_year_end'):
                print(f"  - years: {search_filters.get('release_year_start')}-{search_filters.get('release_year_end')}")

        # 2. Генерация ключа кэша (включаем search_filters в ключ)
        cache_key_data = {
            'type': 'game' if isinstance(source_game, Game) else 'virtual',
            'id': getattr(source_game, 'id', 'virtual'),
            'genres': sorted(source_data['genre_ids']),
            'keywords': sorted(source_data['keyword_ids']),
            'themes': sorted(source_data['theme_ids']),
            'perspectives': sorted(source_data['perspective_ids']),
            'game_modes': sorted(source_data['game_mode_ids']),
            'engines': sorted(source_data['engine_ids']),
            'min_similarity': min_similarity,
            'has_single_player': single_player_info['has_single_player'],
            'only_released': True,
            'limit': limit,
            'search_filters': search_filters,  # Добавляем поисковые фильтры в ключ кэша
            'version': 'v16_with_search_filters'
        }

        cache_key = f'game_similarity_{hashlib.md5(json.dumps(cache_key_data, sort_keys=True).encode()).hexdigest()}'

        # Проверяем кэш
        cached_result = cache.get(cache_key)
        if cached_result and time.time() - cached_result.get('timestamp', 0) < 43200:
            print(f"RETURNING CACHED RESULT: {len(cached_result['games'])} games")
            return cached_result['games']

        print(f"РАСЧЕТ для {getattr(source_game, 'id', 'virtual')}...")
        start_time = time.time()

        # 3. Получаем кандидатов (с учетом поисковых фильтров)
        candidate_ids = self._get_candidate_ids_new(source_data, single_player_info, min_similarity, search_filters)
        print(f"Candidate IDs found: {len(candidate_ids)}")

        if not candidate_ids:
            print("Нет подходящих кандидатов")
            cache.set(cache_key, {'games': [], 'timestamp': time.time()}, 3600)
            return []

        # УБРАНО: ограничение кандидатов
        print(f"Все кандидаты: {len(candidate_ids)}")

        # 4. Подготовка данных кандидатов
        games_data = self._prepare_candidate_data(candidate_ids)
        print(f"Games data prepared: {len(games_data)}")

        # 5. Подсчет общих элементов
        games_data = self._calculate_common_elements_new(games_data, source_data, candidate_ids)
        print(f"Common elements calculated")

        # 6. Расчет схожести
        similar_games = self._calculate_similarity_for_candidates(
            games_data, source_data, source_game, single_player_info
        )
        print(f"Similar games calculated: {len(similar_games)}")

        # Выведем первые несколько similarity для проверки
        for i, game in enumerate(similar_games[:5]):
            print(f"  Game {i + 1}: ID {game['game_id']} - {game['game_name']} - similarity: {game['similarity']}")

        # 7. Сортировка
        similar_games.sort(key=lambda x: x['similarity'], reverse=True)

        # 8. Загрузка полных объектов (применяем лимит ТОЛЬКО в конце для финальных результатов)
        if limit == 0:
            final_results = self._load_full_objects(similar_games)
            print(f"Final results (no limit): {len(final_results)}")
        else:
            final_results = self._load_full_objects(similar_games[:limit])
            print(f"Final results (limited to {limit}): {len(final_results)}")

        # Выведем первые несколько final_results для проверки
        for i, result in enumerate(final_results[:5]):
            game_obj = result.get('game')
            similarity = result.get('similarity')
            print(
                f"  Final {i + 1}: ID {getattr(game_obj, 'id', 'unknown')} - {getattr(game_obj, 'name', 'unknown')} - similarity: {similarity}")

        print(f"Найдено {len(final_results)} похожих ВЫШЕДШИХ игр за {time.time() - start_time:.2f} сек")
        print("=== END SIMILARITY DEBUG ===\n")

        # 10. Кэшируем
        cache.set(cache_key, {
            'games': final_results,
            'timestamp': time.time(),
            'count': len(final_results)
        }, 43200)

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

    def _load_full_objects(self, similar_games):
        """Загрузка полных объектов игр"""
        import time
        from .models import Game

        print("Этап 6: Загрузка полных объектов...")
        load_time = time.time()

        final_results = []

        if not similar_games:
            return final_results

        try:
            game_ids = [item['game_id'] for item in similar_games]

            games = Game.objects.filter(id__in=game_ids).prefetch_related(
                'genres', 'keywords', 'themes', 'game_modes', 'engines'
            )

            games_dict = {game.id: game for game in games}

            for item in similar_games:
                game_id = item['game_id']
                if game_id in games_dict:
                    final_results.append({
                        'game': games_dict[game_id],
                        'similarity': item['similarity'],
                        'common_keywords_count': item['common_keywords'],
                        'common_genres_count': item['common_genres'],
                        'common_themes_count': item['common_themes'],
                        'common_engines_count': item.get('common_engines', 0),
                        'has_single_player': item['has_single_player'],
                        'is_source_game': item.get('is_source_game', False)
                    })
                else:
                    game = Game(
                        id=game_id,
                        name=item['game_name']
                    )
                    final_results.append({
                        'game': game,
                        'similarity': item['similarity'],
                        'common_keywords_count': item['common_keywords'],
                        'common_genres_count': item['common_genres'],
                        'common_themes_count': item['common_themes'],
                        'common_engines_count': item.get('common_engines', 0),
                        'has_single_player': item['has_single_player'],
                        'is_source_game': item.get('is_source_game', False)
                    })
        except Exception as e:
            print(f"Ошибка при загрузке объектов: {e}")
            return []

        print(f"Загрузка объектов завершена за {time.time() - load_time:.2f} сек")
        return final_results

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
