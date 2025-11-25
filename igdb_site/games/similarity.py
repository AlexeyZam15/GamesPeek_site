from django.db.models import Q
from .models import Game, Keyword, Genre


class VirtualGame:
    """Виртуальная игра, созданная из выбранных критериев"""

    def __init__(self, genre_ids=None, keyword_ids=None):
        self.genre_ids = genre_ids or []
        self.keyword_ids = keyword_ids or []
        self.genres = Genre.objects.filter(id__in=genre_ids) if genre_ids else []
        self.keywords = Keyword.objects.filter(id__in=keyword_ids) if keyword_ids else []
        self.platforms = []
        self.name = "Custom Search Criteria"
        self.rating = None
        self.rating_count = 0

    def __str__(self):
        return f"VirtualGame(genres: {len(self.genres)}, keywords: {len(self.keywords)})"


class GameSimilarity:
    """
    Унифицированный алгоритм похожести:
    - 70% за жанры (10% за точное совпадение + 60% за частичное совпадение)
    - 30% за ключевые слова
    """

    # Конфигурационные константы
    GENRES_TOTAL_WEIGHT = 70.0
    GENRES_EXACT_MATCH_WEIGHT = 10.0
    GENRES_PARTIAL_MATCH_WEIGHT = 60.0
    KEYWORDS_TOTAL_WEIGHT = 30.0

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

        # 1. ЖАНРЫ (70% всего)
        genre_score = self._calculate_genre_similarity(source, target)
        similarity += genre_score

        # 2. КЛЮЧЕВЫЕ СЛОВА (30% всего)
        keyword_score = self._calculate_keyword_similarity(source, target)
        similarity += keyword_score

        return max(0.0, min(100.0, similarity))

    def _calculate_genre_similarity(self, source, target):
        """
        Универсальный расчет жанров
        """
        source_genres = self._get_genres(source)
        target_genres = self._get_genres(target)

        if not source_genres and not target_genres:
            return self.GENRES_TOTAL_WEIGHT

        if not source_genres or not target_genres:
            return 0.0

        total_score = 0.0

        # 1. Точное совпадение жанров (10%)
        if source_genres == target_genres:
            total_score += self.GENRES_EXACT_MATCH_WEIGHT

        # 2. Частичное совпадение жанров (до 60%)
        common_genres = source_genres.intersection(target_genres)
        union_genres = source_genres.union(target_genres)

        if union_genres:
            genre_overlap_ratio = len(common_genres) / len(union_genres)
            total_score += genre_overlap_ratio * self.GENRES_PARTIAL_MATCH_WEIGHT

        return total_score

    def _calculate_keyword_similarity(self, source, target):
        """
        Универсальный расчет ключевых слов
        """
        source_keywords = self._get_keywords(source)
        target_keywords = self._get_keywords(target)

        if not source_keywords and not target_keywords:
            return self.KEYWORDS_TOTAL_WEIGHT

        if not source_keywords or not target_keywords:
            return 0.0

        common_keywords = source_keywords.intersection(target_keywords)
        union_keywords = source_keywords.union(target_keywords)

        if union_keywords:
            keyword_overlap_ratio = len(common_keywords) / len(union_keywords)
            return keyword_overlap_ratio * self.KEYWORDS_TOTAL_WEIGHT

        return 0.0

    def _get_genres(self, obj):
        """Универсальное получение жанров"""
        if isinstance(obj, VirtualGame):
            return set(obj.genres)
        elif hasattr(obj, 'genres'):
            return set(obj.genres.all())
        return set()

    def _get_keywords(self, obj):
        """Универсальное получение ключевых слов"""
        if isinstance(obj, VirtualGame):
            return set(obj.keywords)
        elif hasattr(obj, 'keywords'):
            return set(obj.keywords.all())
        return set()

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

        all_games = all_games.prefetch_related('genres', 'keywords', 'platforms')

        for candidate_game in all_games:
            similarity = self.calculate_similarity(source_game, candidate_game)

            if similarity >= min_similarity:
                source_genres = self._get_genres(source_game)
                target_genres = self._get_genres(candidate_game)
                source_keywords = self._get_keywords(source_game)
                target_keywords = self._get_keywords(candidate_game)

                common_genres = source_genres.intersection(target_genres)
                common_keywords = source_keywords.intersection(target_keywords)

                similar_games.append({
                    'game': candidate_game,
                    'similarity': similarity,
                    'common_genres': list(common_genres),
                    'common_keywords': list(common_keywords)
                })

        similar_games.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_games[:limit]

    def get_similarity_breakdown(self, source, target):
        """
        Детальная разбивка похожести
        """
        genre_score = self._calculate_genre_similarity(source, target)
        keyword_score = self._calculate_keyword_similarity(source, target)

        source_genres = self._get_genres(source)
        target_genres = self._get_genres(target)

        breakdown = {
            'genres': {
                'score': genre_score,
                'max_score': self.GENRES_TOTAL_WEIGHT,
                'components': {
                    'exact_match': self.GENRES_EXACT_MATCH_WEIGHT if source_genres == target_genres else 0.0,
                    'partial_match': genre_score - (
                        self.GENRES_EXACT_MATCH_WEIGHT if source_genres == target_genres else 0.0)
                }
            },
            'keywords': {
                'score': keyword_score,
                'max_score': self.KEYWORDS_TOTAL_WEIGHT
            },
            'total_similarity': genre_score + keyword_score
        }

        return breakdown
