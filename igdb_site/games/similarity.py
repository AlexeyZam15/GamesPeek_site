import math
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
    Обновленный алгоритм похожести: штраф за несоответствие жанров + штраф за недостающие жанры
    """

    # Конфигурационные константы
    GENRES_TOTAL_WEIGHT = 70.0  # Общий вес жанров
    GENRES_EXACT_MATCH_WEIGHT = 10.0  # Штраф если жанры не одинаковые
    GENRES_MISSING_PENALTY_WEIGHT = 60.0  # Штраф за недостающие жанры
    KEYWORDS_TOTAL_WEIGHT = 30.0  # Общий вес ключевых слов

    def calculate_similarity(self, game1, game2):
        """
        Вычисляет похожесть между двумя играми
        """
        if game1 == game2:
            return 100.0

        # Начинаем с 100%
        similarity = 100.0

        # 1. ШТРАФ ЕСЛИ ЖАНРЫ НЕ ОДИНАКОВЫЕ
        if not self._are_genres_exactly_same(game1, game2):
            similarity -= self.GENRES_EXACT_MATCH_WEIGHT

        # 2. ШТРАФ ЗА НЕДОСТАЮЩИЕ ЖАНРЫ
        genre_missing_penalty = self._calculate_genre_missing_penalty(game1, game2)
        similarity -= genre_missing_penalty

        # 3. ШТРАФ ЗА НЕДОСТАЮЩИЕ КЛЮЧЕВЫЕ СЛОВА
        keyword_penalty = self._calculate_keyword_penalty(game1, game2)
        similarity -= keyword_penalty

        return max(0.0, similarity)

    def _are_genres_exactly_same(self, game1, game2):
        """
        Проверяет, совпадают ли жанры ровно
        """
        genres1 = set(game1.genres.all())
        genres2 = set(game2.genres.all())
        return genres1 == genres2

    def _calculate_genre_missing_penalty(self, game1, game2):
        """
        Рассчитывает штраф за недостающие жанры (0-GENRES_MISSING_PENALTY_WEIGHT%)
        """
        genres1 = set(game1.genres.all())
        genres2 = set(game2.genres.all())

        if not genres1:
            return 0.0

        missing_genres = genres1 - genres2

        if not missing_genres:
            return 0.0

        penalty = (len(missing_genres) / len(genres1)) * self.GENRES_MISSING_PENALTY_WEIGHT
        return penalty

    def _calculate_keyword_penalty(self, game1, game2):
        """
        Рассчитывает штраф за различия в ключевых словах (0-KEYWORDS_TOTAL_WEIGHT%)
        """
        keywords1 = set(game1.keywords.all())
        keywords2 = set(game2.keywords.all())

        if not keywords1:
            return 0.0

        missing_keywords = keywords1 - keywords2

        if not missing_keywords:
            return 0.0

        penalty = (len(missing_keywords) / len(keywords1)) * self.KEYWORDS_TOTAL_WEIGHT
        return penalty

    def calculate_similarity_to_virtual(self, virtual_game, real_game):
        """
        Вычисляет похожесть между виртуальной игрой (критериями) и реальной игрой
        """
        similarity = 100.0

        # 1. ШТРАФ ЕСЛИ ЖАНРЫ НЕ ОДИНАКОВЫЕ
        if not self._are_virtual_genres_exactly_same(virtual_game, real_game):
            similarity -= self.GENRES_EXACT_MATCH_WEIGHT

        # 2. ШТРАФ ЗА ОТСУТСТВИЕ ЖАНРОВ
        genre_missing_penalty = self._calculate_virtual_genre_missing_penalty(virtual_game, real_game)
        similarity -= genre_missing_penalty

        # 3. ШТРАФ ЗА ОТСУТСТВИЕ КЛЮЧЕВЫХ СЛОВ
        keyword_penalty = self._calculate_virtual_keyword_penalty(virtual_game, real_game)
        similarity -= keyword_penalty

        return max(0.0, similarity)

    def _are_virtual_genres_exactly_same(self, virtual_game, real_game):
        """
        Проверяет, совпадают ли жанры виртуальной игры с реальной ровно
        """
        virtual_genres = set(virtual_game.genres)
        real_genres = set(real_game.genres.all())
        return virtual_genres == real_genres

    def _calculate_virtual_genre_missing_penalty(self, virtual_game, real_game):
        """
        Рассчитывает штраф за отсутствие жанров виртуальной игры в реальной игре
        """
        virtual_genres = set(virtual_game.genres)
        real_genres = set(real_game.genres.all())

        if not virtual_genres:
            return 0.0

        missing_genres = virtual_genres - real_genres

        if not missing_genres:
            return 0.0

        penalty = (len(missing_genres) / len(virtual_genres)) * self.GENRES_MISSING_PENALTY_WEIGHT
        return penalty

    def _calculate_virtual_keyword_penalty(self, virtual_game, real_game):
        """
        Рассчитывает штраф за отсутствие ключевых слов виртуальной игры в реальной игре
        """
        virtual_keywords = set(virtual_game.keywords)
        real_keywords = set(real_game.keywords.all())

        if not virtual_keywords:
            return 0.0

        missing_keywords = virtual_keywords - real_keywords

        if not missing_keywords:
            return 0.0

        penalty = (len(missing_keywords) / len(virtual_keywords)) * self.KEYWORDS_TOTAL_WEIGHT
        return penalty

    # Остальные методы остаются без изменений
    def find_similar_games(self, game, limit=20, min_similarity=15):
        """
        Находит похожие игры для указанной игры
        """
        similar_games = []

        all_games = Game.objects.exclude(pk=game.pk).prefetch_related(
            'genres', 'keywords', 'platforms'
        )

        for candidate_game in all_games:
            similarity = self.calculate_similarity(game, candidate_game)

            if similarity >= min_similarity:
                common_genres = set(game.genres.all()).intersection(set(candidate_game.genres.all()))
                common_keywords = set(game.keywords.all()).intersection(set(candidate_game.keywords.all()))

                similar_games.append({
                    'game': candidate_game,
                    'similarity': similarity,
                    'common_genres': list(common_genres),
                    'common_keywords': list(common_keywords)
                })

        similar_games.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_games[:limit]

    def find_similar_games_to_criteria(self, genre_ids=None, keyword_ids=None, limit=50, min_similarity=15):
        """
        Находит игры, похожие на виртуальную игру из выбранных критериев
        """
        virtual_game = VirtualGame(genre_ids=genre_ids, keyword_ids=keyword_ids)
        similar_games = []

        all_games = Game.objects.all().prefetch_related(
            'genres', 'keywords', 'platforms'
        )

        for candidate_game in all_games:
            similarity = self.calculate_similarity_to_virtual(virtual_game, candidate_game)

            if similarity >= min_similarity:
                common_genres = set(virtual_game.genres).intersection(set(candidate_game.genres.all()))
                common_keywords = set(virtual_game.keywords).intersection(set(candidate_game.keywords.all()))

                similar_games.append({
                    'game': candidate_game,
                    'similarity': similarity,
                    'common_genres': list(common_genres),
                    'common_keywords': list(common_keywords)
                })

        similar_games.sort(key=lambda x: x['similarity'], reverse=True)
        return similar_games[:limit]

    def get_similarity_breakdown(self, game1, game2):
        """
        Детальная разбивка похожести по категориям
        """
        breakdown = {}

        # Штраф за жанры
        genre_penalty = self._calculate_genre_penalty(game1, game2)
        breakdown['genres'] = {
            'penalty': genre_penalty,
            'max_penalty': 70.0,
            'score': 70.0 - genre_penalty
        }

        # Штраф за ключевые слова
        keyword_penalty = self._calculate_keyword_penalty(game1, game2)
        breakdown['keywords'] = {
            'penalty': keyword_penalty,
            'max_penalty': 30.0,
            'score': 30.0 - keyword_penalty
        }

        breakdown['total_similarity'] = 100.0 - genre_penalty - keyword_penalty

        return breakdown

    def get_common_keywords_by_category(self, game1, game2):
        """
        Возвращает общие ключевые слова по категориям (для обратной совместимости)
        """
        keywords1 = game1.keywords.select_related('category').all()
        keywords2 = game2.keywords.select_related('category').all()

        common_keywords = {}

        for kw1 in keywords1:
            for kw2 in keywords2:
                if kw1.name == kw2.name:
                    category_name = kw1.category.name if kw1.category else 'Other'
                    if category_name not in common_keywords:
                        common_keywords[category_name] = []
                    common_keywords[category_name].append(kw1.name)
                    break

        return common_keywords

    def get_matching_criteria(self, virtual_game, real_game):
        """
        Возвращает информацию о совпавших критериях (для обратной совместимости)
        """
        matching = {
            'genres': [],
            'keywords_by_category': {}
        }

        # Совпавшие жанры
        virtual_genres = set(virtual_game.genres)
        real_genres = set(real_game.genres.all())
        matching['genres'] = list(virtual_genres.intersection(real_genres))

        # Совпавшие ключевые слова по категориям
        virtual_keywords = set(virtual_game.keywords)
        real_keywords = set(real_game.keywords.all())

        common_keywords = virtual_keywords.intersection(real_keywords)
        for keyword in common_keywords:
            category_name = keyword.category.name if keyword.category else 'Other'
            if category_name not in matching['keywords_by_category']:
                matching['keywords_by_category'][category_name] = []
            matching['keywords_by_category'][category_name].append(keyword.name)

        return matching
