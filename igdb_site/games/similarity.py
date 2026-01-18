from django.db.models import Prefetch, Count
from collections import defaultdict

from .models import Genre, Game, Theme, Company, PlayerPerspective, GameMode, Keyword


class VirtualGame:
    """Виртуальная игра, созданная из выбранных критериев"""

    def __init__(self, genre_ids=None, keyword_ids=None, theme_ids=None,
                 perspective_ids=None, developer_ids=None, series_id=None,
                 game_mode_ids=None, game_type_ids=None):  # ДОБАВЛЕНО game_type_ids
        self.genre_ids = genre_ids or []
        self.keyword_ids = keyword_ids or []
        self.theme_ids = theme_ids or []
        self.perspective_ids = perspective_ids or []
        self.developer_ids = developer_ids or []
        self.series_id = series_id
        self.game_mode_ids = game_mode_ids or []
        self.game_type_ids = game_type_ids or []  # ДОБАВЛЕНО

        self.genres = []
        self.keywords = []
        self.themes = []
        self.player_perspectives = []
        self.developers = []
        self.series = None
        self.game_modes = []
        self.game_types = []  # ДОБАВЛЕНО

        self.name = "Custom Search Criteria"
        self.rating = None
        self.rating_count = 0

    def __str__(self):
        return f"VirtualGame(genres: {len(self.genre_ids)}, keywords: {len(self.keyword_ids)}, game_modes: {len(self.game_mode_ids)}, game_types: {len(self.game_type_ids)})"

    def load_related(self):
        """Ленивая загрузка связанных объектов"""
        from .models import Genre, Keyword, Theme, PlayerPerspective, Company, Series, GameMode

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

        # ДОБАВЛЕНО: загрузка типов игр
        if not self.game_types and self.game_type_ids:
            # Типы игр хранятся как ID, не нужно загружать из базы
            self.game_types = self.game_type_ids

        if self.series_id and not self.series:
            self.series = Series.objects.filter(id=self.series_id).first()


class GameSimilarity:
    """
    УНИВЕРСАЛЬНЫЙ алгоритм похожести с динамическими весами
    """

    # Базовые константы с распределением весов
    GENRES_WEIGHT = 30.0
    KEYWORDS_WEIGHT = 30.0
    THEMES_WEIGHT = 20.0
    PERSPECTIVES_WEIGHT = 10.0
    GAME_MODES_WEIGHT = 5.0
    DEVELOPERS_WEIGHT = 5.0

    # Конфигурационные константы с оптимизированными весами
    GENRES_TOTAL_WEIGHT = 30.0
    GENRES_EXACT_MATCH_WEIGHT = 10.0
    GENRES_PARTIAL_MATCH_WEIGHT = 20.0
    # НОВАЯ КОНСТАНТА: минимальное количество общих жанров для включения в результат
    MIN_COMMON_GENRES = 2  # Ищем игры только с 2+ общими жанрами

    # Вспомогательные константы для расчетов
    KEYWORDS_ADD_PER_MATCH = 0.5  # Базовое значение для ключевых слов

    def __init__(self):
        # Кэш для ускорения повторных расчетов
        self._similarity_cache = {}
        self._game_data_cache = {}

    def _calculate_dynamic_weights(self, source_data):
        """
        Рассчитывает динамические веса на основе выбранных критериев
        Если выбран только один тип критериев - ему присваивается 100% веса
        Если выбраны несколько - сохраняем пропорции базовых весов
        """
        # Определяем, какие критерии используются
        used_criteria = {
            'genres': len(source_data.get('genre_ids', [])) > 0,
            'keywords': len(source_data.get('keyword_ids', [])) > 0,
            'themes': len(source_data.get('theme_ids', [])) > 0,
            'perspectives': len(source_data.get('perspective_ids', [])) > 0,
            'game_modes': len(source_data.get('game_mode_ids', [])) > 0,
            'developers': len(source_data.get('developer_ids', [])) > 0,
        }

        # Считаем количество активных критериев
        active_criteria_count = sum(used_criteria.values())

        # Если нет критериев вообще
        if active_criteria_count == 0:
            return {
                'genres': 0.0,
                'keywords': 0.0,
                'themes': 0.0,
                'perspectives': 0.0,
                'game_modes': 0.0,
                'developers': 0.0,
                'active_criteria_count': 0,
                'is_single_criterion': False
            }

        # Если только один критерий - ему 100%
        elif active_criteria_count == 1:
            weights = {
                'genres': 100.0 if used_criteria['genres'] else 0.0,
                'keywords': 100.0 if used_criteria['keywords'] else 0.0,
                'themes': 100.0 if used_criteria['themes'] else 0.0,
                'perspectives': 100.0 if used_criteria['perspectives'] else 0.0,
                'game_modes': 100.0 if used_criteria['game_modes'] else 0.0,
                'developers': 100.0 if used_criteria['developers'] else 0.0,
                'active_criteria_count': 1,
                'is_single_criterion': True
            }

            # Логирование для отладки
            for criterion, is_used in used_criteria.items():
                if is_used:
                    print(f"Динамические веса: один критерий '{criterion}' получает 100% веса")
                    break

            return weights

        # Если несколько критериев - распределяем базовые веса пропорционально
        else:
            # Сумма базовых весов используемых критериев
            total_base_weight = 0.0
            if used_criteria['genres']:
                total_base_weight += self.GENRES_WEIGHT
            if used_criteria['keywords']:
                total_base_weight += self.KEYWORDS_WEIGHT
            if used_criteria['themes']:
                total_base_weight += self.THEMES_WEIGHT
            if used_criteria['perspectives']:
                total_base_weight += self.PERSPECTIVES_WEIGHT
            if used_criteria['game_modes']:
                total_base_weight += self.GAME_MODES_WEIGHT
            if used_criteria['developers']:
                total_base_weight += self.DEVELOPERS_WEIGHT

            # Рассчитываем динамические веса
            weights = {
                'genres': (self.GENRES_WEIGHT / total_base_weight * 100.0) if used_criteria['genres'] else 0.0,
                'keywords': (self.KEYWORDS_WEIGHT / total_base_weight * 100.0) if used_criteria['keywords'] else 0.0,
                'themes': (self.THEMES_WEIGHT / total_base_weight * 100.0) if used_criteria['themes'] else 0.0,
                'perspectives': (self.PERSPECTIVES_WEIGHT / total_base_weight * 100.0) if used_criteria[
                    'perspectives'] else 0.0,
                'game_modes': (self.GAME_MODES_WEIGHT / total_base_weight * 100.0) if used_criteria[
                    'game_modes'] else 0.0,
                'developers': (self.DEVELOPERS_WEIGHT / total_base_weight * 100.0) if used_criteria[
                    'developers'] else 0.0,
                'active_criteria_count': active_criteria_count,
                'is_single_criterion': False
            }

            # Логирование для отладки
            print(f"Динамические веса ({active_criteria_count} критериев):")
            for criterion, weight in weights.items():
                if criterion not in ['active_criteria_count', 'is_single_criterion'] and weight > 0:
                    print(f"  - {criterion}: {weight:.1f}%")

            return weights

    def _calculate_game_similarity_new(self, source_genre_count, source_keyword_count, source_theme_count,
                                       source_developer_count, source_perspective_count, source_game_mode_count,
                                       target_data, source_data=None):
        """НОВЫЙ расчет схожести - с динамическими весами"""
        similarity = 0.0

        # Получаем динамические веса
        if source_data:
            dynamic_weights = self._calculate_dynamic_weights(source_data)
        else:
            # Создаем упрощенную source_data для расчета весов
            simplified_source = {
                'genre_ids': [1] if source_genre_count > 0 else [],
                'keyword_ids': [1] if source_keyword_count > 0 else [],
                'theme_ids': [1] if source_theme_count > 0 else [],
                'perspective_ids': [1] if source_perspective_count > 0 else [],
                'game_mode_ids': [1] if source_game_mode_count > 0 else [],
                'developer_ids': [1] if source_developer_count > 0 else [],
            }
            dynamic_weights = self._calculate_dynamic_weights(simplified_source)

        # Если вообще нет критериев - возвращаем минимальную схожесть
        if dynamic_weights['active_criteria_count'] == 0:
            return 15.0  # Базовая схожесть для популярных игр

        # 1. ЖАНРЫ
        if dynamic_weights['genres'] > 0 and source_genre_count > 0 and target_data['common_genres'] > 0:
            if dynamic_weights['is_single_criterion']:
                # Для одного критерия - любое совпадение дает 100%
                similarity = 100.0
                return similarity
            else:
                genre_match_ratio = target_data['common_genres'] / max(source_genre_count, 1)
                similarity += genre_match_ratio * dynamic_weights['genres']

        # 2. КЛЮЧЕВЫЕ СЛОВА
        if dynamic_weights['keywords'] > 0 and target_data['common_keywords'] > 0:
            if dynamic_weights['is_single_criterion']:
                # Для одного критерия - любое совпадение дает 100%
                similarity = 100.0
                return similarity
            else:
                keyword_match_ratio = min(target_data['common_keywords'] / max(source_keyword_count, 1), 1.0)
                similarity += keyword_match_ratio * dynamic_weights['keywords']

        # 3. ТЕМЫ
        if dynamic_weights['themes'] > 0 and source_theme_count > 0 and target_data['common_themes'] > 0:
            if dynamic_weights['is_single_criterion']:
                similarity = 100.0
                return similarity
            else:
                theme_match_ratio = target_data['common_themes'] / max(source_theme_count, 1)
                similarity += theme_match_ratio * dynamic_weights['themes']

        # 4. ПЕРСПЕКТИВЫ
        if dynamic_weights['perspectives'] > 0 and source_perspective_count > 0 and target_data[
            'common_perspectives'] > 0:
            if dynamic_weights['is_single_criterion']:
                similarity = 100.0
                return similarity
            else:
                perspective_match_ratio = target_data['common_perspectives'] / max(source_perspective_count, 1)
                similarity += perspective_match_ratio * dynamic_weights['perspectives']

        # 5. РЕЖИМЫ ИГРЫ
        if dynamic_weights['game_modes'] > 0 and source_game_mode_count > 0 and target_data['common_game_modes'] > 0:
            if dynamic_weights['is_single_criterion']:
                similarity = 100.0
                return similarity
            else:
                game_mode_match_ratio = target_data['common_game_modes'] / max(source_game_mode_count, 1)
                similarity += game_mode_match_ratio * dynamic_weights['game_modes']

        # 6. РАЗРАБОТЧИКИ
        if dynamic_weights['developers'] > 0 and source_developer_count > 0 and target_data.get('common_developers',
                                                                                                0) > 0:
            if dynamic_weights['is_single_criterion']:
                similarity = 100.0
                return similarity
            else:
                developer_match_ratio = target_data.get('common_developers', 0) / max(source_developer_count, 1)
                similarity += developer_match_ratio * dynamic_weights['developers']

        # 7. ДОПОЛНИТЕЛЬНЫЙ БАЛЛ за наличие любых совпадений
        has_any_matches = any([
            target_data['common_genres'] > 0,
            target_data['common_keywords'] > 0,
            target_data['common_themes'] > 0,
            target_data['common_perspectives'] > 0,
            target_data['common_game_modes'] > 0,
            target_data.get('common_developers', 0) > 0
        ])

        if has_any_matches and dynamic_weights['active_criteria_count'] > 1:
            similarity += 5.0  # Дополнительный базовый балл только для нескольких критериев

        return min(100.0, similarity)

    def calculate_similarity(self, source, target):
        """Основной метод вычисления похожести с динамическими весами"""
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

        # Рассчитываем динамические веса
        dynamic_weights = self._calculate_dynamic_weights(source_data)

        similarity = 0.0

        # 1. ЖАНРЫ
        if dynamic_weights['genres'] > 0:
            common_genres = source_data['genres'] & target_data['genres']
            if common_genres:
                if dynamic_weights['is_single_criterion']:
                    similarity = 100.0
                    self._similarity_cache[cache_key] = similarity
                    return similarity
                else:
                    union = source_data['genres'] | target_data['genres']
                    jaccard = len(common_genres) / len(union)
                    similarity += jaccard * dynamic_weights['genres']

        # 2. КЛЮЧЕВЫЕ СЛОВА
        if dynamic_weights['keywords'] > 0:
            common_keywords = source_data['keywords'] & target_data['keywords']
            if common_keywords:
                if dynamic_weights['is_single_criterion']:
                    similarity = 100.0
                    self._similarity_cache[cache_key] = similarity
                    return similarity
                else:
                    source_count = len(source_data['keywords'])
                    match_percentage = len(common_keywords) / source_count
                    similarity += match_percentage * dynamic_weights['keywords']

        # 3. ТЕМЫ
        if dynamic_weights['themes'] > 0:
            common_themes = source_data['themes'] & target_data['themes']
            if common_themes:
                if dynamic_weights['is_single_criterion']:
                    similarity = 100.0
                    self._similarity_cache[cache_key] = similarity
                    return similarity
                else:
                    union = source_data['themes'] | target_data['themes']
                    jaccard = len(common_themes) / len(union)
                    similarity += jaccard * dynamic_weights['themes']

        # 4. РАЗРАБОТЧИКИ
        if dynamic_weights['developers'] > 0:
            common_developers = source_data['developers'] & target_data['developers']
            if common_developers:
                if dynamic_weights['is_single_criterion']:
                    similarity = 100.0
                    self._similarity_cache[cache_key] = similarity
                    return similarity
                else:
                    union = source_data['developers'] | target_data['developers']
                    jaccard = len(common_developers) / len(union)
                    similarity += jaccard * dynamic_weights['developers']

        # 5. ПЕРСПЕКТИВЫ
        if dynamic_weights['perspectives'] > 0:
            common_perspectives = source_data['perspectives'] & target_data['perspectives']
            if common_perspectives:
                if dynamic_weights['is_single_criterion']:
                    similarity = 100.0
                    self._similarity_cache[cache_key] = similarity
                    return similarity
                else:
                    union = source_data['perspectives'] | target_data['perspectives']
                    jaccard = len(common_perspectives) / len(union)
                    similarity += jaccard * dynamic_weights['perspectives']

        # 6. РЕЖИМЫ ИГРЫ
        if dynamic_weights['game_modes'] > 0:
            common_game_modes = source_data['game_modes'] & target_data['game_modes']
            if common_game_modes:
                if dynamic_weights['is_single_criterion']:
                    similarity = 100.0
                    self._similarity_cache[cache_key] = similarity
                    return similarity
                else:
                    union = source_data['game_modes'] | target_data['game_modes']
                    jaccard = len(common_game_modes) / len(union)
                    similarity += jaccard * dynamic_weights['game_modes']

        # Ограничиваем результат
        similarity = max(0.0, min(100.0, similarity))

        # Сохраняем в кэш
        self._similarity_cache[cache_key] = similarity

        return similarity

    def get_similarity_breakdown(self, source, target):
        """
        Детальная разбивка похожести по компонентам с динамическими весами
        """
        # Получаем данные
        source_data = self._get_cached_game_data(source)
        target_data = self._get_cached_game_data(target)

        # Рассчитываем динамические веса
        dynamic_weights = self._calculate_dynamic_weights(source_data)

        breakdown = {
            'genres': {'score': 0.0, 'max_score': dynamic_weights['genres'], 'common_elements': []},
            'keywords': {'score': 0.0, 'max_score': dynamic_weights['keywords'], 'common_elements': []},
            'themes': {'score': 0.0, 'max_score': dynamic_weights['themes'], 'common_elements': []},
            'developers': {'score': 0.0, 'max_score': dynamic_weights['developers'], 'common_elements': []},
            'perspectives': {'score': 0.0, 'max_score': dynamic_weights['perspectives'], 'common_elements': []},
            'game_modes': {'score': 0.0, 'max_score': dynamic_weights['game_modes'], 'common_elements': []},
            'dynamic_weights': dynamic_weights,
            'total_similarity': 0.0
        }

        # Расчет для каждого компонента
        if dynamic_weights['genres'] > 0:
            common_genres = source_data['genres'] & target_data['genres']
            if common_genres:
                if dynamic_weights['is_single_criterion']:
                    breakdown['genres']['score'] = 100.0
                else:
                    union = source_data['genres'] | target_data['genres']
                    jaccard = len(common_genres) / len(union)
                    breakdown['genres']['score'] = jaccard * dynamic_weights['genres']
                breakdown['genres']['common_elements'] = list(common_genres)

        if dynamic_weights['keywords'] > 0:
            common_keywords = source_data['keywords'] & target_data['keywords']
            if common_keywords:
                if dynamic_weights['is_single_criterion']:
                    breakdown['keywords']['score'] = 100.0
                else:
                    source_count = len(source_data['keywords'])
                    match_percentage = len(common_keywords) / source_count
                    breakdown['keywords']['score'] = match_percentage * dynamic_weights['keywords']
                breakdown['keywords']['common_elements'] = list(common_keywords)

        if dynamic_weights['themes'] > 0:
            common_themes = source_data['themes'] & target_data['themes']
            if common_themes:
                if dynamic_weights['is_single_criterion']:
                    breakdown['themes']['score'] = 100.0
                else:
                    union = source_data['themes'] | target_data['themes']
                    jaccard = len(common_themes) / len(union)
                    breakdown['themes']['score'] = jaccard * dynamic_weights['themes']
                breakdown['themes']['common_elements'] = list(common_themes)

        if dynamic_weights['developers'] > 0:
            common_developers = source_data['developers'] & target_data['developers']
            if common_developers:
                if dynamic_weights['is_single_criterion']:
                    breakdown['developers']['score'] = 100.0
                else:
                    union = source_data['developers'] | target_data['developers']
                    jaccard = len(common_developers) / len(union)
                    breakdown['developers']['score'] = jaccard * dynamic_weights['developers']
                breakdown['developers']['common_elements'] = list(common_developers)

        if dynamic_weights['perspectives'] > 0:
            common_perspectives = source_data['perspectives'] & target_data['perspectives']
            if common_perspectives:
                if dynamic_weights['is_single_criterion']:
                    breakdown['perspectives']['score'] = 100.0
                else:
                    union = source_data['perspectives'] | target_data['perspectives']
                    jaccard = len(common_perspectives) / len(union)
                    breakdown['perspectives']['score'] = jaccard * dynamic_weights['perspectives']
                breakdown['perspectives']['common_elements'] = list(common_perspectives)

        if dynamic_weights['game_modes'] > 0:
            common_game_modes = source_data['game_modes'] & target_data['game_modes']
            if common_game_modes:
                if dynamic_weights['is_single_criterion']:
                    breakdown['game_modes']['score'] = 100.0
                else:
                    union = source_data['game_modes'] | target_data['game_modes']
                    jaccard = len(common_game_modes) / len(union)
                    breakdown['game_modes']['score'] = jaccard * dynamic_weights['game_modes']
                breakdown['game_modes']['common_elements'] = list(common_game_modes)

        # Суммируем общую схожесть
        total = sum([
            breakdown['genres']['score'],
            breakdown['keywords']['score'],
            breakdown['themes']['score'],
            breakdown['developers']['score'],
            breakdown['perspectives']['score'],
            breakdown['game_modes']['score']
        ])

        breakdown['total_similarity'] = min(100.0, total)

        return breakdown

    def _calculate_similarity_for_candidates(self, games_data, source_data, source_game, min_similarity,
                                             single_player_info):
        """Расчет схожести для всех кандидатов с динамическими весами"""
        import time

        print("Расчет схожести для кандидатов с динамическими весами...")
        calc_time = time.time()

        similar_games = []
        source_genre_count = source_data['genre_count']
        source_keyword_count = source_data['keyword_count']
        source_theme_count = source_data['theme_count']
        source_developer_count = source_data['developer_count']
        source_perspective_count = source_data['perspective_count']
        source_game_mode_count = source_data['game_mode_count']

        has_genres = source_genre_count > 0
        dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']
        has_single_player = single_player_info['has_single_player']

        print(f"Динамические веса:")
        dynamic_weights = self._calculate_dynamic_weights(source_data)
        for criterion, weight in dynamic_weights.items():
            if criterion not in ['active_criteria_count', 'is_single_criterion'] and weight > 0:
                print(f"  - {criterion}: {weight:.1f}%")

        print(f"Условия фильтрации:")
        print(f"  - Есть жанры: {has_genres}")
        print(f"  - Требуется общих жанров: {dynamic_min_common_genres} (только если есть жанры)")
        print(f"  - Требуется Single player: {has_single_player}")

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

            # Расчет схожести с динамическими весами
            similarity = self._calculate_game_similarity_new(
                source_genre_count, source_keyword_count, source_theme_count,
                source_developer_count, source_perspective_count, source_game_mode_count,
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
                    'has_single_player': data['has_single_player'],
                    'is_source_game': False
                })

        print(f"Расчет схожести завершен за {time.time() - calc_time:.2f} сек")
        print(f"Найдено {len(similar_games)} игр выше порога {min_similarity}%")
        return similar_games

    def _get_similarity_cache_key(self, source, target):
        """Генерирует ключ для кэша схожести"""
        if isinstance(source, VirtualGame):
            source_key = f"virtual_{hash(tuple(sorted(source.genre_ids + source.keyword_ids + source.theme_ids + source.game_type_ids)))}"
        else:
            source_key = f"game_{source.id}"

        if isinstance(target, VirtualGame):
            target_key = f"virtual_{hash(tuple(sorted(target.genre_ids + target.keyword_ids + target.theme_ids + target.game_type_ids)))}"
        else:
            target_key = f"game_{target.id}"

        return f"{source_key}_{target_key}"

    def _get_cached_game_data(self, obj):
        """Получает или кэширует данные игры"""
        if isinstance(obj, VirtualGame):
            cache_key = f"virtual_{hash(tuple(sorted(obj.genre_ids + obj.keyword_ids + obj.theme_ids + obj.game_type_ids)))}"
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



    # В similarity.py, обновляем метод find_similar_games:
    def find_similar_games(self, source_game, min_similarity=20, limit=1000):
        """ОПТИМИЗИРОВАННЫЙ расчет похожих игр - ТОЛЬКО ВЫШЕДШИЕ ИГРЫ"""
        import time
        from django.db import connection
        from django.core.cache import cache
        import json
        import hashlib
        from django.utils import timezone
        from .models import Game, GameMode

        if limit is None:
            limit = 500

        # 1. Подготовка данных исходной игры
        source_data, single_player_info = self._prepare_source_data(source_game)

        # 2. Генерация ключа кэша
        cache_key_data = {
            'type': 'game' if isinstance(source_game, Game) else 'virtual',
            'id': getattr(source_game, 'id', 'virtual'),
            'genres': sorted(source_data['genre_ids']),
            'keywords': sorted(source_data['keyword_ids']),
            'themes': sorted(source_data['theme_ids']),
            'perspectives': sorted(source_data['perspective_ids']),
            'game_modes': sorted(source_data['game_mode_ids']),
            'min_similarity': min_similarity,
            'has_single_player': single_player_info['has_single_player'],
            'only_released': True,
            'limit': limit,
            'version': 'v14_similar_no_genres_required'
        }

        cache_key = f'game_similarity_{hashlib.md5(json.dumps(cache_key_data, sort_keys=True).encode()).hexdigest()}'

        # Проверяем кэш
        cached_result = cache.get(cache_key)
        if cached_result and time.time() - cached_result.get('timestamp', 0) < 43200:
            return cached_result['games']

        print(f"РАСЧЕТ для {getattr(source_game, 'id', 'virtual')}...")
        start_time = time.time()

        # 3. Получаем кандидатов - НОВАЯ ЛОГИКА
        candidate_ids = self._get_candidate_ids_new(source_data, single_player_info, min_similarity)

        if not candidate_ids:
            print("Нет подходящих кандидатов")
            cache.set(cache_key, {'games': [], 'timestamp': time.time()}, 3600)
            return []

        # 4. Берем только нужное количество кандидатов
        candidate_ids = candidate_ids[:limit * 3]  # Берем больше для фильтрации

        print(f"Кандидатов: {len(candidate_ids)}")

        # 5. Подготовка данных кандидатов
        games_data = self._prepare_candidate_data(candidate_ids)

        # 6. Подсчет общих элементов
        games_data = self._calculate_common_elements_new(games_data, source_data, candidate_ids)

        # 7. Расчет схожести
        similar_games = self._calculate_similarity_for_candidates(
            games_data, source_data, source_game, min_similarity, single_player_info
        )

        # 8. Сортировка
        similar_games.sort(key=lambda x: x['similarity'], reverse=True)

        # 9. Загрузка полных объектов
        final_results = self._load_full_objects(similar_games[:limit])

        print(f"Найдено {len(final_results)} похожих ВЫШЕДШИХ игр за {time.time() - start_time:.2f} сек")

        # 10. Кэшируем
        cache.set(cache_key, {
            'games': final_results,
            'timestamp': time.time(),
            'count': len(final_results)
        }, 43200)

        return final_results

    def _get_candidate_ids_new(self, source_data, single_player_info, min_similarity):
        """НОВАЯ логика получения кандидатов - без обязательных общих жанров"""
        import time
        from django.db import connection
        from django.utils import timezone
        from .models import Game

        print("НОВЫЙ поиск кандидатов (без обязательных жанров)...")
        start_time = time.time()

        current_time = timezone.now()
        candidate_ids = []

        source_genre_ids = source_data['genre_ids']
        source_keyword_ids = source_data['keyword_ids']
        source_theme_ids = source_data['theme_ids']
        source_perspective_ids = source_data['perspective_ids']
        source_game_mode_ids = source_data['game_mode_ids']

        has_single_player = single_player_info['has_single_player']
        single_player_mode_id = single_player_info['single_player_mode_id']

        print(f"Критерии поиска:")
        print(f"  - Жанры: {len(source_genre_ids)}")
        print(f"  - Ключевые слова: {len(source_keyword_ids)}")
        print(f"  - Темы: {len(source_theme_ids)}")
        print(f"  - Перспективы: {len(source_perspective_ids)}")
        print(f"  - Режимы игры: {len(source_game_mode_ids)}")
        print(f"  - Single player требуется: {has_single_player}")

        # Определяем стратегию поиска
        has_genres = len(source_genre_ids) > 0
        has_other_criteria = any([
            len(source_keyword_ids) > 0,
            len(source_theme_ids) > 0,
            len(source_perspective_ids) > 0,
            len(source_game_mode_ids) > 0
        ])

        if has_genres:
            # Стратегия 1: Есть жанры - ищем по жанрам
            print("Стратегия: Поиск по жанрам")
            with connection.cursor() as cursor:
                source_genre_ids_str = ','.join(map(str, source_genre_ids))

                # Определяем минимальное количество общих жанров
                dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']

                query = f"""
                    SELECT ggg.game_id, COUNT(*) as common_count
                    FROM games_game_genres ggg
                    INNER JOIN games_game g ON ggg.game_id = g.id
                    WHERE ggg.genre_id IN ({source_genre_ids_str})
                    AND g.first_release_date IS NOT NULL
                    AND g.first_release_date <= %s
                    GROUP BY ggg.game_id
                    HAVING COUNT(*) >= {dynamic_min_common_genres}
                    ORDER BY common_count DESC
                    LIMIT 1000
                """

                cursor.execute(query, [current_time])
                candidate_ids = [row[0] for row in cursor.fetchall()]
                print(f"Найдено кандидатов по жанрам: {len(candidate_ids)}")

        elif has_other_criteria:
            # Стратегия 2: Нет жанров, но есть другие критерии
            print("Стратегия: Поиск по другим критериям (без жанров)")

            # Начинаем с популярных игр
            queryset = Game.objects.filter(
                first_release_date__isnull=False,
                first_release_date__lte=current_time
            )

            # Если есть критерии Single player, применяем их сразу
            if has_single_player and single_player_mode_id:
                queryset = queryset.filter(game_modes__id=single_player_mode_id)
                print("  - Применен фильтр Single player")

            # Берем больше игр для лучшего покрытия
            candidate_ids = list(
                queryset.order_by('-rating_count')
                .values_list('id', flat=True)[:2000]
            )
            print(f"Найдено кандидатов (популярные игры): {len(candidate_ids)}")

        else:
            # Стратегия 3: Нет критериев вообще (только платформы/разработчики)
            print("Стратегия: Нет критериев похожести")
            candidate_ids = list(
                Game.objects.filter(
                    first_release_date__isnull=False,
                    first_release_date__lte=current_time
                )
                .order_by('-rating_count')
                .values_list('id', flat=True)[:500]
            )
            print(f"Найдено кандидатов (только вышедшие): {len(candidate_ids)}")

        print(f"Всего кандидатов: {len(candidate_ids)} за {time.time() - start_time:.2f} сек")
        return candidate_ids

    def _calculate_common_elements_new(self, games_data, source_data, candidate_ids):
        """Новый подсчет общих элементов для всех критериев"""
        import time
        from django.db import connection

        print("Подсчет общих элементов (все критерии)...")
        start_time = time.time()

        if not candidate_ids:
            return games_data

        candidate_ids_str = ','.join(map(str, candidate_ids))

        source_genre_ids = source_data.get('genre_ids', [])
        source_keyword_ids = source_data.get('keyword_ids', [])
        source_theme_ids = source_data.get('theme_ids', [])
        source_perspective_ids = source_data.get('perspective_ids', [])
        source_game_mode_ids = source_data.get('game_mode_ids', [])
        single_player_mode_id = source_data.get('single_player_mode_id')

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

                    -- Общие перспективы
                    (
                        SELECT COUNT(DISTINCT playerperspective_id)
                        FROM games_game_player_perspectives
                        WHERE game_id = g.id 
                        AND playerperspective_id IN %s
                    ) as common_perspectives,

                    -- Общие режимы игры
                    (
                        SELECT COUNT(DISTINCT gamemode_id)
                        FROM games_game_game_modes
                        WHERE game_id = g.id 
                        AND gamemode_id IN %s
                    ) as common_game_modes,

                    -- Single player check
                    CASE WHEN EXISTS (
                        SELECT 1 FROM games_game_game_modes
                        WHERE game_id = g.id 
                        AND gamemode_id = %s
                    ) THEN 1 ELSE 0 END as has_single_player

                FROM games_game g
                WHERE g.id IN ({candidate_ids_str})
            """

            cursor.execute(query, (
                tuple(source_genre_ids) if source_genre_ids else (0,),
                tuple(source_keyword_ids) if source_keyword_ids else (0,),
                tuple(source_theme_ids) if source_theme_ids else (0,),
                tuple(source_perspective_ids) if source_perspective_ids else (0,),
                tuple(source_game_mode_ids) if source_game_mode_ids else (0,),
                single_player_mode_id or 0
            ))

            for row in cursor.fetchall():
                game_id = row[0]
                if game_id in games_data:
                    games_data[game_id].update({
                        'common_genres': row[1],
                        'common_keywords': row[2],
                        'common_themes': row[3],
                        'common_perspectives': row[4],
                        'common_game_modes': row[5],
                        'has_single_player': bool(row[6]),
                    })

        print(f"Подсчет завершен за {time.time() - start_time:.2f} сек")
        return games_data

    # Обновляем _prepare_source_data:
    def _prepare_source_data(self, source_game):
        """Подготовка данных исходной игры - ОБНОВЛЕНО"""
        from .models import GameMode

        source_data = self._get_cached_game_data(source_game)
        source_genre_ids = list(source_data['genres'])
        source_genre_count = len(source_genre_ids)
        source_keyword_ids = list(source_data['keywords'])
        source_keyword_count = len(source_keyword_ids)
        source_theme_ids = list(source_data['themes'])
        source_theme_count = len(source_theme_ids)
        source_perspective_ids = list(source_data['perspectives'])
        source_perspective_count = len(source_perspective_ids)
        source_game_mode_ids = list(source_data['game_modes'])
        source_game_mode_count = len(source_game_mode_ids)

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
        # ТОЛЬКО если есть исходные жанры
        if source_genre_count > 0:
            if source_genre_count >= 2:
                dynamic_min_common_genres = 2
            elif source_genre_count == 1:
                dynamic_min_common_genres = 1
            else:
                dynamic_min_common_genres = 0
        else:
            # Если нет исходных жанров - не требуем общих жанров
            dynamic_min_common_genres = 0

        # Обновляем source_data
        source_data.update({
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
            'single_player_mode_id': single_player_mode_id,
            'developer_ids': list(source_data['developers']),
            'developer_count': len(source_data['developers']),
        })

        single_player_info = {
            'has_single_player': has_single_player_in_source,
            'single_player_mode_id': single_player_mode_id,
            'dynamic_min_common_genres': dynamic_min_common_genres,
            'has_genres': source_genre_count > 0
        }

        print(f"Данные исходной игры:")
        print(f"  - Жанры: {source_genre_count} (мин. общих: {dynamic_min_common_genres})")
        print(f"  - Ключевые слова: {source_keyword_count}")
        print(f"  - Темы: {source_theme_count}")
        print(f"  - Перспективы: {source_perspective_count}")
        print(f"  - Режимы игры: {source_game_mode_count}")
        print(f"  - Single player: {has_single_player_in_source}")

        return source_data, single_player_info

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
        # Убрано: source_game_type_ids = source_data.get('game_type_ids', [])
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
                    len(source_data['game_mode_ids'])
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
                'game_type_ids': sorted(source_game.game_type_ids),  # ДОБАВЛЕНО
                'min_similarity': min_similarity,
                'dynamic_min_common_genres': single_player_info['dynamic_min_common_genres'],
                'has_single_player': single_player_info['has_single_player'],
                'limit': limit,
                'version': 'v_with_game_types'
            }
        else:
            cache_key_data = {
                'type': 'game',
                'game_id': source_game.id,
                'min_similarity': min_similarity,
                'dynamic_min_common_genres': single_player_info['dynamic_min_common_genres'],
                'has_single_player': single_player_info['has_single_player'],
                'game_type': getattr(source_game, 'game_type', None),  # ДОБАВЛЕНО
                'limit': limit,
                'version': 'v_with_game_types'
            }

        cache_key_str = json.dumps(cache_key_data, sort_keys=True)
        return f'game_similarity_{hashlib.md5(cache_key_str.encode()).hexdigest()}'

    def _prepare_candidate_data(self, candidate_ids):
        """Подготовка данных кандидатов"""
        import time
        from .models import Game

        print("Этап 2: Подготовка данных для расчета...")
        prep_time = time.time()

        games_data = {}

        if not candidate_ids:
            return games_data

        # Получаем игры-кандидаты только с нужными полями
        candidate_games = Game.objects.filter(id__in=candidate_ids).only('id', 'name')

        for game in candidate_games:
            games_data[game.id] = {
                'id': game.id,
                'name': game.name,
                'common_keywords': 0,
                'common_genres': 0,
                'common_themes': 0,
                'common_developers': 0,
                'common_perspectives': 0,
                'common_game_modes': 0,
                'total_genres': 0,
                'total_keywords': 0,
                'total_themes': 0,
                'total_developers': 0,
                'total_perspectives': 0,
                'total_game_modes': 0,
                'has_single_player': False,
            }

        print(f"Подготовлено {len(games_data)} игр за {time.time() - prep_time:.2f} сек")
        return games_data

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
                'genres', 'keywords', 'themes', 'game_modes'
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
            similarity += genre_match_ratio * self.GENRES_TOTAL_WEIGHT

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

    def _calculate_genre_similarity(self, source_genres, target_genres):
        """
        Расчет схожести жанров
        """
        if not source_genres and not target_genres:
            return self.GENRES_TOTAL_WEIGHT

        if not source_genres or not target_genres:
            return 0.0

        total_score = 0.0

        # Быстрая проверка точного совпадения
        if source_genres == target_genres:
            total_score += self.GENRES_EXACT_MATCH_WEIGHT
            # Если точное совпадение, сразу возвращаем полный балл
            return total_score + self.GENRES_PARTIAL_MATCH_WEIGHT

        # Частичное совпадение
        source_len = len(source_genres)
        target_len = len(target_genres)

        # Прямое сравнение
        common = source_genres.intersection(target_genres)
        common_count = len(common)

        total_count = source_len + target_len - common_count

        if total_count > 0:
            genre_overlap_ratio = common_count / total_count
            total_score += genre_overlap_ratio * self.GENRES_PARTIAL_MATCH_WEIGHT

        return total_score

    def _calculate_set_similarity(self, set1, set2, max_score):
        """
        Расчет схожести для множеств (для тем, разработчиков, перспектив, режимов игры)
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