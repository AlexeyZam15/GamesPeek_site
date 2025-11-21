import math
from django.db.models import Q
from .models import Game, Keyword, Genre


class GameSimilarity:
    """
    Алгоритм поиска похожих игр с приоритетом: Жанры > Ключ.слова жанров > Геймплей
    """

    # Веса с приоритетом: Жанры > Ключ.слова жанров > Геймплей
    WEIGHTS = {
        'genres': 25.0,  # МАКСИМАЛЬНЫЙ вес для ЖАНРОВ (модель Genre)
        'Genre': 8.0,  # Высокий вес для ключевых слов категории Genre
        'Gameplay': 6.0,  # Средний вес для геймплейных ключевых слов
        'Setting': 4.0,
        'Narrative': 3.5,
        'Characters': 3.0,
        'Technical': 2.5,
        'Graphics': 2.0,
        'Multiplayer': 1.8,
        'Audio': 1.5,
        'Platform': 1.2,
        'Context': 1.0,
        'Achievements': 0.8,
        'Development': 0.5,
        'Other': 0.3,
    }

    def calculate_similarity(self, game1, game2):
        """
        Вычисляет похожесть между двумя играми с приоритетом жанров
        """
        if game1 == game2:
            return 100.0

        total_score = 0
        max_possible_score = 0

        # 1. СХОДСТВО ПО ЖАНРАМ (МОДЕЛЬ GENRE) - МАКСИМАЛЬНЫЙ ПРИОРИТЕТ
        genres1 = set(game1.genres.all())
        genres2 = set(game2.genres.all())

        if genres1 and genres2:
            common_genres = genres1.intersection(genres2)
            if common_genres:
                # Базовая схожесть по жанрам
                genre_similarity = len(common_genres) / max(len(genres1), len(genres2))

                # ДОПОЛНИТЕЛЬНЫЙ БУСТ за совпадение жанров
                genre_multiplier = 1.0
                if len(common_genres) >= 3:
                    genre_multiplier = 2.0  # Очень большой буст для 3+ общих жанров
                elif len(common_genres) == 2:
                    genre_multiplier = 1.6  # Большой буст для 2 общих жанров
                elif len(common_genres) == 1:
                    genre_multiplier = 1.3  # Средний буст для 1 общего жанра

                genre_score = genre_similarity * self.WEIGHTS['genres'] * genre_multiplier
                total_score += genre_score
                max_possible_score += self.WEIGHTS['genres'] * genre_multiplier

        # 2. СХОДСТВО ПО КЛЮЧЕВЫМ СЛОВАМ (по всем категориям)
        keywords1 = game1.keywords.select_related('category').all()
        keywords2 = game2.keywords.select_related('category').all()

        if keywords1 and keywords2:
            # Группируем ключевые слова по категориям
            keyword_categories = {}

            for keyword in keywords1:
                category_name = keyword.category.name if keyword.category else 'Other'
                if category_name not in keyword_categories:
                    keyword_categories[category_name] = {'game1': set(), 'game2': set()}
                keyword_categories[category_name]['game1'].add(keyword.name)

            for keyword in keywords2:
                category_name = keyword.category.name if keyword.category else 'Other'
                if category_name not in keyword_categories:
                    keyword_categories[category_name] = {'game1': set(), 'game2': set()}
                keyword_categories[category_name]['game2'].add(keyword.name)

            # Считаем похожесть для каждой категории
            for category_name, keywords in keyword_categories.items():
                weight = self.WEIGHTS.get(category_name, self.WEIGHTS['Other'])

                keywords1_set = keywords['game1']
                keywords2_set = keywords['game2']

                if keywords1_set and keywords2_set:
                    common_keywords = keywords1_set.intersection(keywords2_set)
                    if common_keywords:
                        category_similarity = len(common_keywords) / max(len(keywords1_set), len(keywords2_set))

                        # Дополнительный буст для категории Genre (ключевые слова жанров)
                        category_multiplier = 1.0
                        if category_name == 'Genre':
                            category_multiplier = 1.4  # Буст для ключевых слов жанров
                        elif category_name == 'Gameplay':
                            category_multiplier = 1.2  # Небольшой буст для геймплея

                        category_score = category_similarity * weight * category_multiplier
                        total_score += category_score
                        max_possible_score += weight * category_multiplier

        # 3. ДОПОЛНИТЕЛЬНЫЕ ФАКТОРЫ
        additional_score = 0

        # Общие платформы
        platforms1 = set(game1.platforms.all())
        platforms2 = set(game2.platforms.all())
        if platforms1 and platforms2:
            common_platforms = platforms1.intersection(platforms2)
            if common_platforms:
                platform_similarity = len(common_platforms) / max(len(platforms1), len(platforms2))
                additional_score += platform_similarity * 1.0

        # Похожий рейтинг
        if game1.rating and game2.rating:
            rating_diff = abs(game1.rating - game2.rating)
            if rating_diff <= 0.5:  # Разница менее 0.5 балла
                additional_score += 0.8
            elif rating_diff <= 1.0:  # Разница менее 1 балла
                additional_score += 0.4

        total_score += additional_score
        max_possible_score += 1.8  # Максимальный дополнительный балл

        # Нормализуем результат (0-100%)
        if max_possible_score == 0:
            return 0.0

        base_similarity = (total_score / max_possible_score) * 100

        # Увеличиваем итоговый процент для лучшего отображения
        final_similarity = self._boost_similarity_score(base_similarity)

        return min(final_similarity, 100.0)

    def _boost_similarity_score(self, base_score):
        """
        Увеличивает базовый процент схожести
        """
        if base_score <= 0:
            return 0
        elif base_score <= 20:
            return base_score * 1.6
        elif base_score <= 50:
            return base_score * 1.3
        else:
            return base_score * 1.15

    def find_similar_games(self, game, limit=20, min_similarity=15):
        """
        Находит похожие игры для указанной игры
        """
        similar_games = []

        # Ищем все игры кроме текущей
        all_games = Game.objects.exclude(pk=game.pk).prefetch_related(
            'genres', 'keywords__category', 'platforms'
        )

        for candidate_game in all_games:
            similarity = self.calculate_similarity(game, candidate_game)

            if similarity >= min_similarity:
                # Получаем общие жанры и ключевые слова
                common_genres = set(game.genres.all()).intersection(set(candidate_game.genres.all()))
                common_keywords_by_category = self.get_common_keywords_by_category(game, candidate_game)

                # Получаем названия категорий с наибольшим количеством совпадений
                matching_categories = []
                for category_name, keywords in common_keywords_by_category.items():
                    if keywords:
                        matching_categories.append(category_name)

                similar_games.append({
                    'game': candidate_game,
                    'similarity': similarity,
                    'common_genres': list(common_genres),
                    'common_keywords': common_keywords_by_category,
                    'matching_categories': matching_categories[:3]
                })

        # Сортируем по похожести (от большей к меньшей)
        similar_games.sort(key=lambda x: x['similarity'], reverse=True)

        return similar_games[:limit]

    def get_common_keywords_by_category(self, game1, game2):
        """
        Возвращает общие ключевые слова по категориям
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

    def get_similarity_breakdown(self, game1, game2):
        """
        Детальная разбивка похожести по всем категориям
        """
        breakdown = {}

        # 1. Жанры (модель Genre)
        genres1 = set(game1.genres.all())
        genres2 = set(game2.genres.all())

        if genres1 and genres2:
            common_genres = genres1.intersection(genres2)
            genre_similarity = len(common_genres) / max(len(genres1), len(genres2)) * 100
            breakdown['Genres'] = {
                'similarity_percent': genre_similarity,
                'common_items': [genre.name for genre in common_genres],
                'weight': self.WEIGHTS['genres']
            }

        # 2. Ключевые слова по всем категориям
        keywords1 = game1.keywords.select_related('category').all()
        keywords2 = game2.keywords.select_related('category').all()

        if keywords1 and keywords2:
            # Группируем ключевые слова по категориям для обеих игр
            categories_data = {}

            for keyword in keywords1:
                category_name = keyword.category.name if keyword.category else 'Other'
                if category_name not in categories_data:
                    categories_data[category_name] = {'game1': set(), 'game2': set()}
                categories_data[category_name]['game1'].add(keyword.name)

            for keyword in keywords2:
                category_name = keyword.category.name if keyword.category else 'Other'
                if category_name not in categories_data:
                    categories_data[category_name] = {'game1': set(), 'game2': set()}
                categories_data[category_name]['game2'].add(keyword.name)

            # Считаем похожесть для каждой категории
            for category_name, data in categories_data.items():
                keywords1_set = data['game1']
                keywords2_set = data['game2']

                if keywords1_set and keywords2_set:
                    common_keywords = keywords1_set.intersection(keywords2_set)
                    category_similarity = len(common_keywords) / max(len(keywords1_set), len(keywords2_set)) * 100
                    weight = self.WEIGHTS.get(category_name, self.WEIGHTS['Other'])

                    breakdown[category_name] = {
                        'similarity_percent': category_similarity,
                        'common_items': list(common_keywords),
                        'weight': weight
                    }

        return breakdown