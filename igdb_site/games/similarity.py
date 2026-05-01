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
    # ВЕРСИЯ АЛГОРИТМА - УВЕЛИЧИВАТЬ ПРИ ЛЮБЫХ ИЗМЕНЕНИЯХ В ЛОГИКЕ РАСЧЕТА
    # Это гарантирует, что старый кэш не будет использоваться после обновления
    ALGORITHM_VERSION = 7

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
    MIN_COMMON_GENRES = 2  # минимальное количество общих жанров для включения в результат

    DEFAULT_MIN_SIMILARITY = 40  # минимальный порог похожести по умолчанию

    # Вспомогательные константы для расчетов
    KEYWORDS_ADD_PER_MATCH = 0.2  # каждое совпадающее ключевое слово добавляет 0.2% к схожести
    EXTRA_GENRE_PENALTY = 2.0  # штраф за каждый лишний жанр у целевой игры (в процентах)

    def __init__(self):
        # Кэш для ускорения повторных расчетов
        self._similarity_cache = {}
        self._game_data_cache = {}

    def clear_cache(self, force=False):
        """
        Очищает кэш.
        Если force=True, полностью очищает все кэши.
        """
        if force:
            self._similarity_cache.clear()
            self._game_data_cache.clear()
            print(f"GameSimilarity: Cache cleared completely")
        else:
            self._similarity_cache.clear()
            self._game_data_cache.clear()
            print(f"GameSimilarity: Cache cleared")

    def get_similarity_formula(self, source, target):
        """
        Возвращает структурированные данные для красивого отображения вклада каждого критерия.
        Процент соответствия показывает common/source_total * 100.
        """
        try:
            # Получаем breakdown для этой пары игр (используем прямой расчет, без рекурсии)
            breakdown = self._calculate_breakdown_direct(source, target)

            # Получаем данные исходной игры
            source_data, _ = self._prepare_source_data(source)

            # Формируем структурированные данные для шаблона
            criteria_contributions = []

            # Жанры
            if breakdown['genres']['max_score'] > 0:
                common_count = len(breakdown['genres']['common_elements'])
                source_count = source_data['genre_count']
                genre_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0
                criteria_contributions.append({
                    'icon': '🎮',
                    'name': 'Genres',
                    'map_name': 'genres',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['genres']['max_score'],
                    'contribution': breakdown['genres']['score'],
                    'percentage': genre_match_percentage,
                    'color': 'purple'
                })

            # Ключевые слова
            if breakdown['keywords']['max_score'] > 0:
                common_count = len(breakdown['keywords']['common_elements'])
                source_count = source_data['keyword_count']
                keyword_match_percentage = (common_count / source_count * 100) if source_count > 0 else 0
                criteria_contributions.append({
                    'icon': '🔑',
                    'name': 'Keywords',
                    'map_name': 'keywords',
                    'common': common_count,
                    'total': source_count,
                    'weight': breakdown['keywords']['max_score'],
                    'contribution': breakdown['keywords']['score'],
                    'percentage': keyword_match_percentage,
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
                    'percentage': theme_match_percentage,
                    'color': 'orange'
                })

            # Перспективы - ДОБАВЛЕНО
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
                    'percentage': perspective_match_percentage,
                    'color': 'info'
                })

            # Режимы игры - ДОБАВЛЕНО
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
                    'percentage': gamemode_match_percentage,
                    'color': 'pink'
                })

            # Разработчики - ДОБАВЛЕНО
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
                    'percentage': developer_match_percentage,
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
                    'percentage': engine_match_percentage,
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

    def get_similarity_breakdown(self, source, target):
        """
        Детальная разбивка похожести по компонентам.
        Использует унифицированный алгоритм через _calculate_breakdown_direct.
        """
        return self._calculate_breakdown_direct(source, target)

    def _calculate_breakdown_direct(self, source, target):
        """
        Прямой расчет детальной разбивки похожести без рекурсии.
        """
        source_data, single_player_info = self._prepare_source_data(source)
        target_raw = self._get_cached_game_data(target)

        common_genres = len(source_data.get('genres', set()) & target_raw.get('genres', set()))
        common_keywords = len(source_data.get('keywords', set()) & target_raw.get('keywords', set()))
        common_themes = len(source_data.get('themes', set()) & target_raw.get('themes', set()))
        common_perspectives = len(source_data.get('perspectives', set()) & target_raw.get('perspectives', set()))
        common_game_modes = len(source_data.get('game_modes', set()) & target_raw.get('game_modes', set()))
        common_developers = len(source_data.get('developers', set()) & target_raw.get('developers', set()))
        common_engines = len(source_data.get('engines', set()) & target_raw.get('engines', set()))

        target_data = {
            'common_genres': common_genres,
            'common_keywords': common_keywords,
            'common_themes': common_themes,
            'common_perspectives': common_perspectives,
            'common_game_modes': common_game_modes,
            'common_developers': common_developers,
            'common_engines': common_engines,
            'total_genres': len(target_raw.get('genres', set())),
            'total_keywords': len(target_raw.get('keywords', set())),
            'total_themes': len(target_raw.get('themes', set())),
            'total_perspectives': len(target_raw.get('perspectives', set())),
            'total_game_modes': len(target_raw.get('game_modes', set())),
            'total_developers': len(target_raw.get('developers', set())),
            'total_engines': len(target_raw.get('engines', set())),
        }

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
            common = target_data['common_genres']
            source_total = source_data['genre_count']
            target_total = target_data['total_genres']

            # Каждый жанр исходной игры стоит (30 / количество жанров) процентов
            percent_per_genre = max_scores['genres'] / source_total

            # Базовый процент за совпавшие жанры
            genre_score = common * percent_per_genre

            # Штраф за лишние жанры у целевой игры
            extra_genres_in_target = target_total - common
            penalty = extra_genres_in_target * self.EXTRA_GENRE_PENALTY

            scores['genres'] = max(0.0, genre_score - penalty)
        else:
            scores['genres'] = 0.0

        # Ключевые слова
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

        has_any_matches = any([
            target_data['common_genres'] > 0,
            target_data['common_keywords'] > 0,
            target_data['common_themes'] > 0,
            target_data['common_perspectives'] > 0,
            target_data['common_game_modes'] > 0,
            target_data['common_developers'] > 0,
            target_data['common_engines'] > 0
        ])

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

        final_total = min(100.0, total_without_bonus + bonus)

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
            'total_similarity': final_total,
            'bonus': bonus,
            'total_without_bonus': total_without_bonus
        }

    def _calculate_unified_similarity(self, source_genre_count, source_keyword_count, source_theme_count,
                                      source_developer_count, source_perspective_count, source_game_mode_count,
                                      source_engine_count, target_data):
        """
        УНИФИЦИРОВАННЫЙ расчет схожести для всех мест использования.
        Жанры: 30% распределяются пропорционально совпавшим жанрам исходной игры.
        Каждый совпавший жанр дает (30 / source_genre_count) %.
        За каждый лишний жанр у целевой игры вычитается EXTRA_GENRE_PENALTY %.
        Ключевые слова: каждое совпадение дает KEYWORDS_ADD_PER_MATCH %.
        """
        similarity = 0.0

        # 1. ЖАНРЫ - пропорционально совпавшим жанрам исходной игры
        if self.GENRES_WEIGHT > 0 and source_genre_count > 0:
            common_genres = target_data.get('common_genres', 0)
            source_total = source_genre_count
            target_total = target_data.get('total_genres', 0)

            # Каждый жанр исходной игры стоит (30 / количество жанров) процентов
            percent_per_genre = self.GENRES_WEIGHT / source_total

            # Базовый процент за совпавшие жанры
            genre_score = common_genres * percent_per_genre

            # Штраф за лишние жанры у целевой игры (жанры, которых нет в исходной)
            extra_genres_in_target = target_total - common_genres
            penalty = extra_genres_in_target * self.EXTRA_GENRE_PENALTY

            genre_score = max(0.0, genre_score - penalty)
            similarity += genre_score

        # 2. КЛЮЧЕВЫЕ СЛОВА - каждое совпадение дает KEYWORDS_ADD_PER_MATCH %
        if self.KEYWORDS_WEIGHT > 0 and source_keyword_count > 0:
            common_keywords = target_data.get('common_keywords', 0)
            if common_keywords > 0:
                keyword_score = min(common_keywords * self.KEYWORDS_ADD_PER_MATCH, self.KEYWORDS_WEIGHT)
                similarity += keyword_score

        # 3. ТЕМЫ - процент совпадения от веса
        if self.THEMES_WEIGHT > 0 and source_theme_count > 0:
            if target_data.get('common_themes', 0) > 0:
                theme_match_ratio = target_data['common_themes'] / max(source_theme_count, 1)
                similarity += theme_match_ratio * self.THEMES_WEIGHT

        # 4. ПЕРСПЕКТИВЫ - процент совпадения от веса
        if self.PERSPECTIVES_WEIGHT > 0 and source_perspective_count > 0:
            if target_data.get('common_perspectives', 0) > 0:
                perspective_match_ratio = target_data['common_perspectives'] / max(source_perspective_count, 1)
                similarity += perspective_match_ratio * self.PERSPECTIVES_WEIGHT

        # 5. РЕЖИМЫ ИГРЫ - процент совпадения от веса
        if self.GAME_MODES_WEIGHT > 0 and source_game_mode_count > 0:
            if target_data.get('common_game_modes', 0) > 0:
                game_mode_match_ratio = target_data['common_game_modes'] / max(source_game_mode_count, 1)
                similarity += game_mode_match_ratio * self.GAME_MODES_WEIGHT

        # 6. РАЗРАБОТЧИКИ - процент совпадения от веса
        if self.DEVELOPERS_WEIGHT > 0 and source_developer_count > 0:
            if target_data.get('common_developers', 0) > 0:
                developer_match_ratio = target_data.get('common_developers', 0) / max(source_developer_count, 1)
                similarity += developer_match_ratio * self.DEVELOPERS_WEIGHT

        # 7. ДВИЖКИ - процент совпадения от веса
        if self.ENGINES_WEIGHT > 0 and source_engine_count > 0:
            if target_data.get('common_engines', 0) > 0:
                engine_match_ratio = target_data.get('common_engines', 0) / max(source_engine_count, 1)
                similarity += engine_match_ratio * self.ENGINES_WEIGHT

        # 8. ДОПОЛНИТЕЛЬНЫЙ БОНУС за любые совпадения, если задействовано более одного критерия
        has_any_matches = any([
            target_data.get('common_genres', 0) > 0,
            target_data.get('common_keywords', 0) > 0,
            target_data.get('common_themes', 0) > 0,
            target_data.get('common_perspectives', 0) > 0,
            target_data.get('common_game_modes', 0) > 0,
            target_data.get('common_developers', 0) > 0,
            target_data.get('common_engines', 0) > 0
        ])

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

        return round(min(100.0, similarity), 2)

    def _get_candidate_ids_new(self, source_data, single_player_info, min_similarity, search_filters=None):
        """
        МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ поиск кандидатов - БЕЗ ЛИМИТОВ.
        Применяет поисковые фильтры ДО расчёта похожести.
        Использует сырой SQL для максимальной скорости.
        """
        import time
        from django.utils import timezone
        from django.db import connection

        print("МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ поиск кандидатов (без лимитов)...")
        print(f"Search filters received: {search_filters}")
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
                     AND g.first_release_date <= %s
                   """
        params.append(current_time)
        sql_parts.append(base_sql)

        # ========== ПРИМЕНЯЕМ ПОИСКОВЫЕ ФИЛЬТРЫ (search_filters) ДО РАСЧЁТА ПОХОЖЕСТИ ==========
        if search_filters:
            print(f"Applying search filters before similarity calculation: {search_filters}")

            # Платформы (OR)
            if search_filters.get('platforms'):
                platform_ids = search_filters['platforms']
                platform_ids_str = ','.join(map(str, platform_ids))
                sql_parts.append(f"""
                    AND EXISTS (
                        SELECT 1 FROM games_game_platforms ggp 
                        WHERE ggp.game_id = g.id AND ggp.platform_id IN ({platform_ids_str})
                    )
                """)
                print(f"  - Added platforms filter: {platform_ids}")

            # Жанры (AND - игра должна содержать ВСЕ выбранные жанры)
            if search_filters.get('genres'):
                for genre_id in search_filters['genres']:
                    sql_parts.append(f"""
                        AND EXISTS (
                            SELECT 1 FROM games_game_genres ggg 
                            WHERE ggg.game_id = g.id AND ggg.genre_id = {genre_id}
                        )
                    """)
                print(f"  - Added genres AND filter: {search_filters['genres']}")

            # Ключевые слова (AND - игра должна содержать ВСЕ выбранные ключевые слова)
            if search_filters.get('keywords'):
                for keyword_id in search_filters['keywords']:
                    sql_parts.append(f"""
                        AND EXISTS (
                            SELECT 1 FROM games_game_keywords ggk 
                            WHERE ggk.game_id = g.id AND ggk.keyword_id = {keyword_id}
                        )
                    """)
                print(f"  - Added keywords AND filter: {search_filters['keywords']}")

            # Темы (AND - игра должна содержать ВСЕ выбранные темы)
            if search_filters.get('themes'):
                for theme_id in search_filters['themes']:
                    sql_parts.append(f"""
                        AND EXISTS (
                            SELECT 1 FROM games_game_themes ggt 
                            WHERE ggt.game_id = g.id AND ggt.theme_id = {theme_id}
                        )
                    """)
                print(f"  - Added themes AND filter: {search_filters['themes']}")

            # Перспективы (OR)
            if search_filters.get('perspectives'):
                perspective_ids = search_filters['perspectives']
                perspective_ids_str = ','.join(map(str, perspective_ids))
                sql_parts.append(f"""
                    AND EXISTS (
                        SELECT 1 FROM games_game_player_perspectives gggp 
                        WHERE gggp.game_id = g.id AND gggp.playerperspective_id IN ({perspective_ids_str})
                    )
                """)
                print(f"  - Added perspectives OR filter: {perspective_ids}")

            # Режимы игры (OR)
            if search_filters.get('game_modes'):
                game_mode_ids = search_filters['game_modes']
                game_mode_ids_str = ','.join(map(str, game_mode_ids))
                sql_parts.append(f"""
                    AND EXISTS (
                        SELECT 1 FROM games_game_game_modes gggm 
                        WHERE gggm.game_id = g.id AND gggm.gamemode_id IN ({game_mode_ids_str})
                    )
                """)
                print(f"  - Added game_modes OR filter: {game_mode_ids}")

            # Движки (OR)
            if search_filters.get('engines'):
                engine_ids = search_filters['engines']
                engine_ids_str = ','.join(map(str, engine_ids))
                sql_parts.append(f"""
                    AND EXISTS (
                        SELECT 1 FROM games_game_engines gge 
                        WHERE gge.game_id = g.id AND gge.gameengine_id IN ({engine_ids_str})
                    )
                """)
                print(f"  - Added engines OR filter: {engine_ids}")

            # Игровые типы (OR)
            if search_filters.get('game_types'):
                game_type_ids = search_filters['game_types']
                game_type_ids_str = ','.join(map(str, game_type_ids))
                sql_parts.append(f"""
                    AND g.game_type IN ({game_type_ids_str})
                """)
                print(f"  - Added game_types OR filter: {game_type_ids}")

            # Дата (AND)
            year_start = search_filters.get('release_year_start')
            year_end = search_filters.get('release_year_end')
            if year_start:
                sql_parts.append(f" AND EXTRACT(YEAR FROM g.first_release_date) >= {year_start}")
                print(f"  - Added year_start filter: {year_start}")
            if year_end:
                sql_parts.append(f" AND EXTRACT(YEAR FROM g.first_release_date) <= {year_end}")
                print(f"  - Added year_end filter: {year_end}")

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
            print(f"Added source genre filter: min {dynamic_min_common_genres} common genres")

        # Если есть темы - добавляем фильтр по темам
        elif source_theme_ids:
            source_theme_ids_str = ','.join(map(str, source_theme_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_themes ggt 
                    WHERE ggt.game_id = g.id AND ggt.theme_id IN ({source_theme_ids_str})
                )
            """)
            print("Added source theme filter")

        # Если есть движки - добавляем фильтр по движкам
        elif source_engine_ids:
            source_engine_ids_str = ','.join(map(str, source_engine_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_engines gge 
                    WHERE gge.game_id = g.id AND gge.gameengine_id IN ({source_engine_ids_str})
                )
            """)
            print("Added source engine filter")

        # Если есть ключевые слова - добавляем фильтр по ключевым словам
        elif source_keyword_ids:
            source_keyword_ids_str = ','.join(map(str, source_keyword_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_keywords ggk 
                    WHERE ggk.game_id = g.id AND ggk.keyword_id IN ({source_keyword_ids_str})
                )
            """)
            print("Added source keyword filter")

        # Если есть перспективы - добавляем фильтр по перспективам
        elif source_perspective_ids:
            source_perspective_ids_str = ','.join(map(str, source_perspective_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_player_perspectives gggp 
                    WHERE gggp.game_id = g.id AND gggp.playerperspective_id IN ({source_perspective_ids_str})
                )
            """)
            print("Added source perspective filter")

        # Если есть режимы игры - добавляем фильтр по режимам
        elif source_game_mode_ids:
            source_game_mode_ids_str = ','.join(map(str, source_game_mode_ids))
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_game_modes gggm 
                    WHERE gggm.game_id = g.id AND gggm.gamemode_id IN ({source_game_mode_ids_str})
                )
            """)
            print("Added source game_mode filter")

        # Фильтр по Single Player
        if has_single_player and single_player_mode_id:
            sql_parts.append(f"""
                AND EXISTS (
                    SELECT 1 FROM games_game_game_modes gggm 
                    WHERE gggm.game_id = g.id AND gggm.gamemode_id = {single_player_mode_id}
                )
            """)
            print("Added single player filter")

        # Объединяем SQL
        final_sql = ' '.join(sql_parts)
        print(f"Final SQL query: {final_sql}")

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
        source_developer_ids = source_data.get('developer_ids', [])  # ДОБАВЛЕНО
        single_player_mode_id = source_data.get('single_player_mode_id')

        # Подготавливаем строки для IN
        genre_in = ','.join(map(str, source_genre_ids)) if source_genre_ids else 'NULL'
        keyword_in = ','.join(map(str, source_keyword_ids)) if source_keyword_ids else 'NULL'
        theme_in = ','.join(map(str, source_theme_ids)) if source_theme_ids else 'NULL'
        perspective_in = ','.join(map(str, source_perspective_ids)) if source_perspective_ids else 'NULL'
        gamemode_in = ','.join(map(str, source_game_mode_ids)) if source_game_mode_ids else 'NULL'
        engine_in = ','.join(map(str, source_engine_ids)) if source_engine_ids else 'NULL'
        developer_in = ','.join(map(str, source_developer_ids)) if source_developer_ids else 'NULL'  # ДОБАВЛЕНО

        candidate_ids_str = ','.join(map(str, candidate_ids))
        single_player_id = single_player_mode_id or 0

        # Единый SQL запрос с подзапросами
        query = f"""
            SELECT
                g.id as game_id,

                -- Быстрый подсчет общих жанров
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

                -- Быстрый подсчет общих разработчиков (ДОБАВЛЕНО)
                COALESCE((
                    SELECT COUNT(*)
                    FROM games_game_developers ggd
                    WHERE ggd.game_id = g.id AND ggd.company_id IN ({developer_in if developer_in != 'NULL' else 'NULL'})
                ), 0) as common_developers,

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
                        'common_developers': row[7],  # ДОБАВЛЕНО
                        'has_single_player': bool(row[8]),
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
                    COALESCE(ec.engine_count, 0) as total_engines,
                    COALESCE(dc.developer_count, 0) as total_developers
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
                LEFT JOIN (
                    SELECT game_id, COUNT(*) as developer_count
                    FROM games_game_developers
                    GROUP BY game_id
                ) dc ON g.id = dc.game_id
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
                    'total_developers': row[8],
                    'common_keywords': 0,
                    'common_genres': 0,
                    'common_themes': 0,
                    'common_perspectives': 0,
                    'common_game_modes': 0,
                    'common_engines': 0,
                    'common_developers': 0,
                    'has_single_player': False,
                }

        print(f"Подготовлено {len(games_data)} игр за {time.time() - prep_time:.2f} сек")
        return games_data

    def _calculate_game_similarity_new(self, source_genre_count, source_keyword_count, source_theme_count,
                                       source_developer_count, source_perspective_count, source_game_mode_count,
                                       source_engine_count, target_data, source_data=None):
        """
        Расчет схожести - с KEYWORDS_ADD_PER_MATCH для ключевых слов.
        Жанры: 30% распределяются пропорционально совпавшим жанрам исходной игры.
        За каждый лишний жанр у целевой игры вычитается 2%.
        """
        similarity = 0.0

        # 1. ЖАНРЫ - пропорционально совпавшим жанрам исходной игры
        if self.GENRES_WEIGHT > 0 and source_genre_count > 0:
            common_genres = target_data.get('common_genres', 0)
            source_total = source_genre_count
            target_total = target_data.get('total_genres', 0)

            # Каждый жанр исходной игры стоит (30 / количество жанров) процентов
            percent_per_genre = self.GENRES_WEIGHT / source_total

            # Базовый процент за совпавшие жанры
            genre_score = common_genres * percent_per_genre

            # Штраф за лишние жанры у целевой игры
            extra_genres_in_target = target_total - common_genres
            penalty = extra_genres_in_target * self.EXTRA_GENRE_PENALTY

            similarity += max(0.0, genre_score - penalty)

        # 2. КЛЮЧЕВЫЕ СЛОВА
        if self.KEYWORDS_WEIGHT > 0 and source_keyword_count > 0:
            common_keywords = target_data.get('common_keywords', 0)
            if common_keywords > 0:
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

        # 8. БОНУС
        has_any_matches = any([
            target_data.get('common_genres', 0) > 0,
            target_data.get('common_keywords', 0) > 0,
            target_data.get('common_themes', 0) > 0,
            target_data.get('common_perspectives', 0) > 0,
            target_data.get('common_game_modes', 0) > 0,
            target_data.get('common_developers', 0) > 0,
            target_data.get('common_engines', 0) > 0
        ])

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
        """
        Основной метод вычисления похожести.
        Использует унифицированный алгоритм с KEYWORDS_ADD_PER_MATCH.
        """
        # Проверка на идентичность
        if source == target:
            return 100.0

        # Генерация ключа кэша
        cache_key = self._get_similarity_cache_key(source, target)

        # Проверка кэша
        if cache_key in self._similarity_cache:
            return self._similarity_cache[cache_key]

        # Получаем данные для source и target
        source_data, single_player_info = self._prepare_source_data(source)
        target_raw = self._get_cached_game_data(target)

        # Подготавливаем данные target в формате, понятном унифицированному алгоритму
        target_data = {
            'common_genres': len(source_data.get('genres', set()) & target_raw.get('genres', set())),
            'common_keywords': len(source_data.get('keywords', set()) & target_raw.get('keywords', set())),
            'common_themes': len(source_data.get('themes', set()) & target_raw.get('themes', set())),
            'common_perspectives': len(source_data.get('perspectives', set()) & target_raw.get('perspectives', set())),
            'common_game_modes': len(source_data.get('game_modes', set()) & target_raw.get('game_modes', set())),
            'common_developers': len(source_data.get('developers', set()) & target_raw.get('developers', set())),
            'common_engines': len(source_data.get('engines', set()) & target_raw.get('engines', set())),
        }

        # Используем унифицированный расчет схожести
        similarity = self._calculate_unified_similarity(
            source_data['genre_count'],
            source_data['keyword_count'],
            source_data['theme_count'],
            source_data['developer_count'],
            source_data['perspective_count'],
            source_data['game_mode_count'],
            source_data['engine_count'],
            target_data
        )

        # Сохраняем в кэш
        self._similarity_cache[cache_key] = similarity

        return similarity

    def _calculate_similarity_for_candidates(self, games_data, source_data, source_game, single_player_info):
        """
        МАКСИМАЛЬНО ОПТИМИЗИРОВАННЫЙ расчет схожести для всех кандидатов.
        Использует унифицированный метод _calculate_unified_similarity.
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

        # Обработка кандидатов
        for game_id, data in games_data.items():
            # Пропускаем исходную игру
            if source_game_id and game_id == source_game_id:
                continue

            # СПЕЦИАЛЬНАЯ ОТЛАДКА ДЛЯ XCOM 2 (ID: 840)
            if game_id == 840:
                print(f"\n=== DEBUG FOR XCOM 2 (ID: 840) ===")
                print(f"Game name: {data['name']}")
                print(f"Source genre_count: {source_genre_count}")
                print(f"common_genres: {data['common_genres']}")
                print(f"Source keyword_count: {source_keyword_count}")
                print(f"common_keywords: {data['common_keywords']}")
                print(f"Source theme_count: {source_theme_count}")
                print(f"common_themes: {data['common_themes']}")
                print(f"Source perspective_count: {source_perspective_count}")
                print(f"common_perspectives: {data['common_perspectives']}")
                print(f"Source game_mode_count: {source_game_mode_count}")
                print(f"common_game_modes: {data['common_game_modes']}")
                print(f"Source developer_count: {source_developer_count}")
                print(f"common_developers: {data.get('common_developers', 0)}")
                print(f"Source engine_count: {source_engine_count}")
                print(f"common_engines: {data.get('common_engines', 0)}")

                # Пошаговый расчет
                similarity = 0.0

                # Жанры
                if self.GENRES_WEIGHT > 0 and source_genre_count > 0:
                    if data['common_genres'] > 0:
                        genre_match_ratio = data['common_genres'] / max(source_genre_count, 1)
                        genre_score = genre_match_ratio * self.GENRES_WEIGHT
                        similarity += genre_score
                        print(
                            f"Genres: {data['common_genres']}/{source_genre_count} = {genre_match_ratio:.4f} × {self.GENRES_WEIGHT} = {genre_score:.2f}")

                # Ключевые слова
                if self.KEYWORDS_WEIGHT > 0 and source_keyword_count > 0:
                    common_keywords = data['common_keywords']
                    if common_keywords > 0:
                        keyword_score = min(common_keywords * self.KEYWORDS_ADD_PER_MATCH, self.KEYWORDS_WEIGHT)
                        similarity += keyword_score
                        print(
                            f"Keywords: {common_keywords} × {self.KEYWORDS_ADD_PER_MATCH} = {common_keywords * self.KEYWORDS_ADD_PER_MATCH:.2f} (capped at {self.KEYWORDS_WEIGHT}) = {keyword_score:.2f}")

                # Темы
                if self.THEMES_WEIGHT > 0 and source_theme_count > 0:
                    if data['common_themes'] > 0:
                        theme_match_ratio = data['common_themes'] / max(source_theme_count, 1)
                        theme_score = theme_match_ratio * self.THEMES_WEIGHT
                        similarity += theme_score
                        print(
                            f"Themes: {data['common_themes']}/{source_theme_count} = {theme_match_ratio:.4f} × {self.THEMES_WEIGHT} = {theme_score:.2f}")

                # Перспективы
                if self.PERSPECTIVES_WEIGHT > 0 and source_perspective_count > 0:
                    if data['common_perspectives'] > 0:
                        perspective_match_ratio = data['common_perspectives'] / max(source_perspective_count, 1)
                        perspective_score = perspective_match_ratio * self.PERSPECTIVES_WEIGHT
                        similarity += perspective_score
                        print(
                            f"Perspectives: {data['common_perspectives']}/{source_perspective_count} = {perspective_match_ratio:.4f} × {self.PERSPECTIVES_WEIGHT} = {perspective_score:.2f}")

                # Режимы игры
                if self.GAME_MODES_WEIGHT > 0 and source_game_mode_count > 0:
                    if data['common_game_modes'] > 0:
                        game_mode_match_ratio = data['common_game_modes'] / max(source_game_mode_count, 1)
                        game_mode_score = game_mode_match_ratio * self.GAME_MODES_WEIGHT
                        similarity += game_mode_score
                        print(
                            f"Game Modes: {data['common_game_modes']}/{source_game_mode_count} = {game_mode_match_ratio:.4f} × {self.GAME_MODES_WEIGHT} = {game_mode_score:.2f}")

                # Разработчики
                if self.DEVELOPERS_WEIGHT > 0 and source_developer_count > 0:
                    common_developers = data.get('common_developers', 0)
                    if common_developers > 0:
                        developer_match_ratio = common_developers / max(source_developer_count, 1)
                        developer_score = developer_match_ratio * self.DEVELOPERS_WEIGHT
                        similarity += developer_score
                        print(
                            f"Developers: {common_developers}/{source_developer_count} = {developer_match_ratio:.4f} × {self.DEVELOPERS_WEIGHT} = {developer_score:.2f}")

                # Движки
                if self.ENGINES_WEIGHT > 0 and source_engine_count > 0:
                    common_engines = data.get('common_engines', 0)
                    if common_engines > 0:
                        engine_match_ratio = common_engines / max(source_engine_count, 1)
                        engine_score = engine_match_ratio * self.ENGINES_WEIGHT
                        similarity += engine_score
                        print(
                            f"Engines: {common_engines}/{source_engine_count} = {engine_match_ratio:.4f} × {self.ENGINES_WEIGHT} = {engine_score:.2f}")

                # Бонус
                has_any_matches = any([
                    data.get('common_genres', 0) > 0,
                    data.get('common_keywords', 0) > 0,
                    data.get('common_themes', 0) > 0,
                    data.get('common_perspectives', 0) > 0,
                    data.get('common_game_modes', 0) > 0,
                    data.get('common_developers', 0) > 0,
                    data.get('common_engines', 0) > 0
                ])

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
                    print(
                        f"Bonus: +5.0 (has_any_matches={has_any_matches}, active_criteria_count={active_criteria_count})")

                final_similarity = min(100.0, similarity)
                print(f"FINAL SIMILARITY: {final_similarity}")
                print(f"=====================================\n")

                # Используем уже рассчитанную similarity
                pass
            else:
                # Обычный расчет
                similarity = self._calculate_unified_similarity(
                    source_genre_count,
                    source_keyword_count,
                    source_theme_count,
                    source_developer_count,
                    source_perspective_count,
                    source_game_mode_count,
                    source_engine_count,
                    data
                )

            # Быстрая фильтрация по жанрам
            if has_genres and data['common_genres'] < dynamic_min_common_genres:
                continue

            # Быстрая фильтрация по Single Player
            if has_single_player and not data['has_single_player']:
                continue

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
        """Генерирует ключ для кэша схожести с учетом версии алгоритма"""
        if isinstance(source, VirtualGame):
            source_key = f"virtual_{hash(tuple(sorted(source.genre_ids + source.keyword_ids + source.theme_ids + source.game_type_ids + source.engine_ids)))}"
        else:
            source_key = f"game_{source.id}"

        if isinstance(target, VirtualGame):
            target_key = f"virtual_{hash(tuple(sorted(target.genre_ids + target.keyword_ids + target.theme_ids + target.game_type_ids + target.engine_ids)))}"
        else:
            target_key = f"game_{target.id}"

        # Добавляем версию алгоритма в ключ кэша для автоматической инвалидации
        return f"sim_v{self.ALGORITHM_VERSION}_{source_key}_{target_key}"

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
        ОПТИМИЗИРОВАННЫЙ расчет похожих игр - ПОЛНЫЙ ПЕРЕРАСЧЁТ с поисковыми фильтрами.

        Search filters применяются на этапе получения кандидатов (SQL уровень),
        чтобы фильтровать игры ДО расчёта похожести.
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

        print(f"\n=== FIND SIMILAR GAMES ===")
        print(f"Source: {getattr(source_game, 'id', 'virtual')}")
        print(f"Search filters: {search_filters}")
        print(f"Min similarity: {min_similarity}")

        source_data, single_player_info = self._prepare_source_data(source_game)

        # 1. Получаем кандидатов с применением поисковых фильтров
        candidate_ids = self._get_candidate_ids_new(source_data, single_player_info, min_similarity, search_filters)
        print(f"Candidate IDs found: {len(candidate_ids)}")

        if not candidate_ids:
            print("Нет подходящих кандидатов")
            return []

        # 2. Подготовка данных кандидатов
        games_data = self._prepare_candidate_data(candidate_ids)

        # 3. Подсчет общих элементов
        games_data = self._calculate_common_elements_new(games_data, source_data, candidate_ids)

        # 4. Расчет схожести (только для кандидатов, уже отфильтрованных search_filters)
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

    def _generate_cache_key(self, source_game, min_similarity, limit, source_data, single_player_info):
        """Генерация ключа кэша с учетом версии алгоритма"""
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
                'algorithm_version': self.ALGORITHM_VERSION,  # Добавляем версию алгоритма
                'version': 'v19_with_algorithm_version'
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
                'algorithm_version': self.ALGORITHM_VERSION,  # Добавляем версию алгоритма
                'version': 'v19_with_algorithm_version'
            }

        cache_key_str = json.dumps(cache_key_data, sort_keys=True)
        return f'game_similarity_{hashlib.md5(cache_key_str.encode()).hexdigest()}'

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
