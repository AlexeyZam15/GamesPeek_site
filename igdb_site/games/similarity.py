from django.db.models import Q
from .models import Game, Keyword, Genre, Series, Company, Theme, PlayerPerspective, GameMode


class VirtualGame:
    """Виртуальная игра, созданная из выбранных критериев"""

    def __init__(self, genre_ids=None, keyword_ids=None, theme_ids=None,
                 perspective_ids=None, developer_ids=None, series_id=None):
        self.genre_ids = genre_ids or []
        self.keyword_ids = keyword_ids or []
        self.theme_ids = theme_ids or []
        self.perspective_ids = perspective_ids or []
        self.developer_ids = developer_ids or []
        self.series_id = series_id

        self.genres = Genre.objects.filter(id__in=genre_ids) if genre_ids else []
        self.keywords = Keyword.objects.filter(id__in=keyword_ids) if keyword_ids else []
        self.themes = Theme.objects.filter(id__in=theme_ids) if theme_ids else []
        self.player_perspectives = PlayerPerspective.objects.filter(id__in=perspective_ids) if perspective_ids else []
        self.developers = Company.objects.filter(id__in=developer_ids) if developer_ids else []
        self.series = Series.objects.filter(id=series_id).first() if series_id else None

        self.name = "Custom Search Criteria"
        self.rating = None
        self.rating_count = 0

    def __str__(self):
        return f"VirtualGame(genres: {len(self.genres)}, keywords: {len(self.keywords)})"


class GameSimilarity:
    """
    УНИВЕРСАЛЬНЫЙ алгоритм похожести с учетом ОСНОВНЫХ критериев:

    ВЕСА КОМПОНЕНТОВ:
    - Жанры: 30%
    - Ключевые слова: 25%
    - Темы: 20%
    - Разработчики: 15%
    - Перспективы: 10%
    """

    # Конфигурационные константы с оптимизированными весами
    GENRES_WEIGHT = 30.0
    KEYWORDS_WEIGHT = 25.0
    THEMES_WEIGHT = 20.0
    DEVELOPERS_WEIGHT = 15.0
    PERSPECTIVES_WEIGHT = 10.0
    GENRES_EXACT_MATCH_WEIGHT = 10.0

    def calculate_similarity(self, source, target):
        """
        УНИВЕРСАЛЬНЫЙ метод вычисления похожести
        Поддерживает:
        - Game vs Game
        - VirtualGame vs Game
        - Game vs VirtualGame
        - VirtualGame vs VirtualGame
        """
        if source == target:
            return 100.0

        similarity = 0.0

        # 1. ЖАНРЫ (30%)
        genre_score = self._calculate_set_similarity(
            self._get_genres(source),
            self._get_genres(target),
            self.GENRES_WEIGHT
        )
        similarity += genre_score

        # 2. КЛЮЧЕВЫЕ СЛОВА (25%)
        keyword_score = self._calculate_set_similarity(
            self._get_keywords(source),
            self._get_keywords(target),
            self.KEYWORDS_WEIGHT
        )
        similarity += keyword_score

        # 3. ТЕМЫ (20%)
        theme_score = self._calculate_set_similarity(
            self._get_themes(source),
            self._get_themes(target),
            self.THEMES_WEIGHT
        )
        similarity += theme_score

        # 4. РАЗРАБОТЧИКИ (15%)
        developer_score = self._calculate_set_similarity(
            self._get_developers(source),
            self._get_developers(target),
            self.DEVELOPERS_WEIGHT
        )
        similarity += developer_score

        # 5. ПЕРСПЕКТИВЫ (10%)
        perspective_score = self._calculate_set_similarity(
            self._get_perspectives(source),
            self._get_perspectives(target),
            self.PERSPECTIVES_WEIGHT
        )
        similarity += perspective_score

        return max(0.0, min(100.0, similarity))

    def _calculate_set_similarity(self, set1, set2, max_score):
        """
        Универсальный расчет схожести для любых множеств
        """
        if not set1 and not set2:
            return max_score  # Оба пустые - полное совпадение

        if not set1 or not set2:
            return 0.0  # Один пустой - нет совпадения

        common_elements = set1.intersection(set2)
        union_elements = set1.union(set2)

        if union_elements:
            overlap_ratio = len(common_elements) / len(union_elements)
            return overlap_ratio * max_score

        return 0.0

    # УНИВЕРСАЛЬНЫЕ МЕТОДЫ ДЛЯ ПОЛУЧЕНИЯ ДАННЫХ
    def _get_genres(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.genres)
        elif hasattr(obj, 'genres'):
            return set(obj.genres.all())
        return set()

    def _get_keywords(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.keywords)
        elif hasattr(obj, 'keywords'):
            return set(obj.keywords.all())
        return set()

    def _get_themes(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.themes)
        elif hasattr(obj, 'themes'):
            return set(obj.themes.all())
        return set()

    def _get_developers(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.developers)
        elif hasattr(obj, 'developers'):
            return set(obj.developers.all())
        return set()

    def _get_perspectives(self, obj):
        if isinstance(obj, VirtualGame):
            return set(obj.player_perspectives)
        elif hasattr(obj, 'player_perspectives'):
            return set(obj.player_perspectives.all())
        return set()

    def _get_series(self, obj):
        if isinstance(obj, VirtualGame):
            return obj.series
        elif hasattr(obj, 'series'):
            return obj.series
        return None

    def find_similar_games(self, source_game, limit=20, min_similarity=15):
        """
        Находит похожие игры для указанной игры или критериев
        """
        similar_games = []

        # Определяем какие игры исключать
        if isinstance(source_game, Game):
            all_games = Game.objects.exclude(pk=source_game.pk)
        else:
            all_games = Game.objects.all()

        all_games = all_games.prefetch_related(
            'genres', 'keywords', 'themes', 'developers', 'player_perspectives'
        )

        for candidate_game in all_games:
            similarity = self.calculate_similarity(source_game, candidate_game)

            if similarity >= min_similarity:
                # Собираем общие элементы для детальной информации
                common_data = {
                    'game': candidate_game,
                    'similarity': similarity,
                    'common_genres': list(self._get_genres(source_game).intersection(self._get_genres(candidate_game))),
                    'common_keywords': list(
                        self._get_keywords(source_game).intersection(self._get_keywords(candidate_game))),
                    'common_themes': list(self._get_themes(source_game).intersection(self._get_themes(candidate_game))),
                    'common_developers': list(
                        self._get_developers(source_game).intersection(self._get_developers(candidate_game))),
                    'common_perspectives': list(
                        self._get_perspectives(source_game).intersection(self._get_perspectives(candidate_game))),
                }

                similar_games.append(common_data)

        similar_games.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_games[:limit]

    def get_similarity_breakdown(self, source, target):
        """
        Детальная разбивка похожести по компонентам
        """
        genre_score = self._calculate_set_similarity(
            self._get_genres(source), self._get_genres(target), self.GENRES_WEIGHT
        )
        keyword_score = self._calculate_set_similarity(
            self._get_keywords(source), self._get_keywords(target), self.KEYWORDS_WEIGHT
        )
        theme_score = self._calculate_set_similarity(
            self._get_themes(source), self._get_themes(target), self.THEMES_WEIGHT
        )
        developer_score = self._calculate_set_similarity(
            self._get_developers(source), self._get_developers(target), self.DEVELOPERS_WEIGHT
        )
        perspective_score = self._calculate_set_similarity(
            self._get_perspectives(source), self._get_perspectives(target), self.PERSPECTIVES_WEIGHT
        )

        breakdown = {
            'genres': {
                'score': genre_score,
                'max_score': self.GENRES_WEIGHT,
                'common_elements': list(self._get_genres(source).intersection(self._get_genres(target))),
                'source_count': len(self._get_genres(source)),
                'target_count': len(self._get_genres(target))
            },
            'keywords': {
                'score': keyword_score,
                'max_score': self.KEYWORDS_WEIGHT,
                'common_elements': list(self._get_keywords(source).intersection(self._get_keywords(target))),
                'source_count': len(self._get_keywords(source)),
                'target_count': len(self._get_keywords(target))
            },
            'themes': {
                'score': theme_score,
                'max_score': self.THEMES_WEIGHT,
                'common_elements': list(self._get_themes(source).intersection(self._get_themes(target))),
                'source_count': len(self._get_themes(source)),
                'target_count': len(self._get_themes(target))
            },
            'developers': {
                'score': developer_score,
                'max_score': self.DEVELOPERS_WEIGHT,
                'common_elements': list(self._get_developers(source).intersection(self._get_developers(target))),
                'source_count': len(self._get_developers(source)),
                'target_count': len(self._get_developers(target))
            },
            'perspectives': {
                'score': perspective_score,
                'max_score': self.PERSPECTIVES_WEIGHT,
                'common_elements': list(self._get_perspectives(source).intersection(self._get_perspectives(target))),
                'source_count': len(self._get_perspectives(source)),
                'target_count': len(self._get_perspectives(target))
            },
            'total_similarity': genre_score + keyword_score + theme_score + developer_score + perspective_score
        }

        return breakdown