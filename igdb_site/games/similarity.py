import math
from django.db.models import Q
from .models import Game, Keyword


class GameSimilarity:
    """
    Алгоритм поиска похожих игр на основе жанров и ключевых слов
    """

    # Обновленные веса для всех категорий
    WEIGHTS = {
        'Genre': 5.0,  # Самый важный - жанры
        'Gameplay': 4.0,  # Очень важны - геймплей
        'Setting': 3.5,  # Очень важны - сеттинг
        'Narrative': 3.0,  # Важны - нарратив
        'Characters': 2.5,  # Важны - персонажи
        'Technical': 2.0,  # Умеренно важны - технические аспекты
        'Graphics': 1.5,  # Умеренно важны - графика
        'Multiplayer': 1.5,  # Умеренно важны - мультиплеер
        'Audio': 1.0,  # Менее важны - аудио
        'Platform': 1.0,  # Менее важны - платформы
        'Context': 0.8,  # Мало важны - контекст
        'Achievements': 0.5,  # Минимально важны - достижения
        'Development': 0.3,  # Минимально важны - разработка
    }

    def calculate_similarity(self, game1, game2):
        """
        Вычисляет похожесть между двумя играми (0-100%)
        """
        if game1 == game2:
            return 100.0

        total_score = 0
        max_possible_score = 0

        # 1. СХОДСТВО ПО ЖАНРАМ (самое важное)
        genres1 = set(game1.genres.all())
        genres2 = set(game2.genres.all())

        if genres1 and genres2:
            common_genres = genres1.intersection(genres2)
            genre_similarity = len(common_genres) / max(len(genres1), len(genres2))
            total_score += genre_similarity * self.WEIGHTS['Genre']
            max_possible_score += self.WEIGHTS['Genre']

        # 2. СХОДСТВО ПО КЛЮЧЕВЫМ СЛОВАМ (по всем категориям)
        keywords1 = game1.keywords.select_related('category').all()
        keywords2 = game2.keywords.select_related('category').all()

        if keywords1 and keywords2:
            # Группируем ключевые слова по всем категориям
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
                weight = self.WEIGHTS.get(category_name, 0.5)  # По умолчанию 0.5 для неизвестных категорий

                keywords1_set = keywords['game1']
                keywords2_set = keywords['game2']

                if keywords1_set and keywords2_set:
                    common_keywords = keywords1_set.intersection(keywords2_set)
                    category_similarity = len(common_keywords) / max(len(keywords1_set), len(keywords2_set))
                    total_score += category_similarity * weight
                    max_possible_score += weight

        # Нормализуем результат (0-100%)
        if max_possible_score == 0:
            return 0.0

        similarity = (total_score / max_possible_score) * 100
        return min(similarity, 100.0)

    def find_similar_games(self, game, limit=10, min_similarity=20):
        """
        Находит похожие игры для указанной игры
        """
        similar_games = []

        # Ищем все игры кроме текущей (с жанрами и ключевыми словами)
        all_games = Game.objects.exclude(pk=game.pk).prefetch_related(
            'genres', 'keywords__category'
        )

        for candidate_game in all_games:
            similarity = self.calculate_similarity(game, candidate_game)

            if similarity >= min_similarity:
                # Получаем общие жанры и ключевые слова по категориям
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
                    'matching_categories': matching_categories[:3]  # Топ-3 категории
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

        # 1. Жанры
        genres1 = set(game1.genres.all())
        genres2 = set(game2.genres.all())

        if genres1 and genres2:
            common_genres = genres1.intersection(genres2)
            genre_similarity = len(common_genres) / max(len(genres1), len(genres2)) * 100
            breakdown['Genre'] = {
                'similarity_percent': genre_similarity,
                'common_items': [genre.name for genre in common_genres],
                'weight': self.WEIGHTS['Genre']
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
                    weight = self.WEIGHTS.get(category_name, 0.5)

                    breakdown[category_name] = {
                        'similarity_percent': category_similarity,
                        'common_items': list(common_keywords),
                        'weight': weight
                    }

        return breakdown