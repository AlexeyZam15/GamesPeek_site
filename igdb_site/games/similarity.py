from django.db.models import Prefetch, Count
from collections import defaultdict

from .models import Genre, Game, Theme, Company, PlayerPerspective, GameMode, Keyword


class VirtualGame:
    """Виртуальная игра, созданная из выбранных критериев"""

    def __init__(self, genre_ids=None, keyword_ids=None, theme_ids=None,
                 perspective_ids=None, developer_ids=None, series_id=None, game_mode_ids=None):
        self.genre_ids = genre_ids or []
        self.keyword_ids = keyword_ids or []
        self.theme_ids = theme_ids or []
        self.perspective_ids = perspective_ids or []
        self.developer_ids = developer_ids or []
        self.series_id = series_id
        self.game_mode_ids = game_mode_ids or []

        self.genres = []
        self.keywords = []
        self.themes = []
        self.player_perspectives = []
        self.developers = []
        self.series = None
        self.game_modes = []

        self.name = "Custom Search Criteria"
        self.rating = None
        self.rating_count = 0

    def __str__(self):
        return f"VirtualGame(genres: {len(self.genre_ids)}, keywords: {len(self.keyword_ids)}, game_modes: {len(self.game_mode_ids)})"

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

        if self.series_id and not self.series:
            self.series = Series.objects.filter(id=self.series_id).first()


class GameSimilarity:
    """
    УНИВЕРСАЛЬНЫЙ алгоритм похожести с оптимизациями для производительности

    ВЕСА КОМПОНЕНТОВ:
    - Жанры: 30% (10% за точное совпадение + 20% за частичное)
    - Ключевые слова: 30% (1% за каждое совпадающее ключевое слово)
    - Темы: 20%
    - Разработчики: 5%
    - Перспективы: 10%
    - Режимы игры: 5%
    """

    # Конфигурационные константы с оптимизированными весами
    GENRES_TOTAL_WEIGHT = 30.0
    GENRES_EXACT_MATCH_WEIGHT = 10.0
    GENRES_PARTIAL_MATCH_WEIGHT = 20.0
    KEYWORDS_WEIGHT = 30.0
    KEYWORDS_ADD_PER_MATCH = 0.5  # 1% за каждое совпадающее ключевое слово
    THEMES_WEIGHT = 30.0
    DEVELOPERS_WEIGHT = 5.0
    PERSPECTIVES_WEIGHT = 10.0
    GAME_MODES_WEIGHT = 5.0

    # НОВАЯ КОНСТАНТА: минимальное количество общих жанров для включения в результат
    MIN_COMMON_GENRES = 2  # Ищем игры только с 2+ общими жанрами

    def __init__(self):
        # Кэш для ускорения повторных расчетов
        self._similarity_cache = {}
        self._game_data_cache = {}

    def find_similar_games(self, source_game, min_similarity=20, limit=1000):
        """
        УПРОЩЕННЫЙ расчет БЕЗ ОГРАНИЧЕНИЙ
        Исходная игра включается с 100% схожести
        Минимальная схожесть: 20%
        """
        import time
        from django.db import connection
        from django.core.cache import cache
        import json
        import hashlib
        from .models import Game, GameMode

        if limit is None:
            limit = 1000

        # 1. Подготовка данных исходной игры и проверка Single player
        source_data, single_player_info = self._prepare_source_data(source_game)

        # 2. Генерация ключа кэша
        cache_key = self._generate_cache_key(source_game, min_similarity, limit,
                                             source_data, single_player_info)

        # Проверяем кэш
        cached_result = cache.get(cache_key)
        if cached_result:
            return cached_result

        print(f"РАСЧЕТ для {getattr(source_game, 'id', 'virtual')}...")
        start_time = time.time()

        # 3. Получение кандидатов по жанрам и Single player
        candidate_ids = self._get_candidate_ids(source_data, single_player_info)

        if not candidate_ids:
            print("Нет подходящих кандидатов")
            return []

        # 4. Подготовка данных кандидатов
        games_data = self._prepare_candidate_data(candidate_ids)

        # 5. Подсчет общих элементов
        games_data = self._calculate_common_elements(games_data, source_data, candidate_ids)

        # 6. Расчет схожести и фильтрация
        similar_games = self._calculate_and_filter_similarity(
            games_data, source_game, source_data, min_similarity, single_player_info
        )

        # 7. Сортировка и ограничение
        similar_games = self._sort_and_limit_results(similar_games, source_game, limit)

        # 8. Загрузка полных объектов
        final_results = self._load_full_objects(similar_games)

        print(f"Всего найдено {len(final_results)} похожих игр за {time.time() - start_time:.2f} сек")

        # 9. Кэшируем результат
        cache.set(cache_key, final_results, 86400)

        return final_results

    def _prepare_source_data(self, source_game):
        """Подготовка данных исходной игры и проверка Single player"""
        from .models import GameMode

        source_data = self._get_cached_game_data(source_game)
        source_genre_ids = list(source_data['genres'])
        source_genre_count = len(source_genre_ids)

        # Проверяем, есть ли у исходной игры режим Single player
        source_game_mode_ids = list(source_data['game_modes'])
        has_single_player_in_source = False
        single_player_mode_id = None

        # Получаем ID режима "Single player" из базы данных
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
        if source_genre_count >= 2:
            dynamic_min_common_genres = 2
        elif source_genre_count == 1:
            dynamic_min_common_genres = 1
        else:
            dynamic_min_common_genres = 0

        source_data.update({
            'genre_ids': source_genre_ids,
            'genre_count': source_genre_count,
            'keyword_ids': list(source_data['keywords']),
            'theme_ids': list(source_data['themes']),
            'developer_ids': list(source_data['developers']),
            'perspective_ids': list(source_data['perspectives']),
            'game_mode_ids': source_game_mode_ids,
            'keyword_count': len(source_data['keywords']),
            'theme_count': len(source_data['themes']),
            'developer_count': len(source_data['developers']),
            'perspective_count': len(source_data['perspectives']),
            'game_mode_count': len(source_game_mode_ids),
        })

        single_player_info = {
            'has_single_player': has_single_player_in_source,
            'single_player_mode_id': single_player_mode_id,
            'dynamic_min_common_genres': dynamic_min_common_genres
        }

        print(f"Данные исходной игры:")
        print(f"  - Жанры: {source_genre_count}")
        print(f"  - Требуется общих жанров: {dynamic_min_common_genres}")
        print(f"  - Имеет режим Single player: {has_single_player_in_source}")

        return source_data, single_player_info

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
                'min_similarity': min_similarity,
                'dynamic_min_common_genres': single_player_info['dynamic_min_common_genres'],
                'has_single_player': single_player_info['has_single_player'],
                'limit': limit,
                'version': 'v_single_player_check'
            }
        else:
            cache_key_data = {
                'type': 'game',
                'game_id': source_game.id,
                'min_similarity': min_similarity,
                'dynamic_min_common_genres': single_player_info['dynamic_min_common_genres'],
                'has_single_player': single_player_info['has_single_player'],
                'limit': limit,
                'version': 'v_single_player_check'
            }

        cache_key_str = json.dumps(cache_key_data, sort_keys=True)
        return f'game_similarity_{hashlib.md5(cache_key_str.encode()).hexdigest()}'

    def _get_candidate_ids(self, source_data, single_player_info):
        """Получение ID кандидатов по жанрам и Single player"""
        import time
        from django.db import connection
        from .models import Game

        print("Этап 1: Получение кандидатов с общими жанрами...")
        filter_time = time.time()

        candidate_ids = []
        dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']
        has_single_player = single_player_info['has_single_player']
        single_player_mode_id = single_player_info['single_player_mode_id']
        source_genre_ids = source_data['genre_ids']

        # Получаем ID игр с общими жанрами
        if source_genre_ids:
            with connection.cursor() as cursor:
                source_genre_ids_str = ','.join(map(str, source_genre_ids))
                query = f"""
                    SELECT game_id, COUNT(*) as common_genres_count
                    FROM games_game_genres 
                    WHERE genre_id IN ({source_genre_ids_str})
                    GROUP BY game_id
                    HAVING COUNT(*) >= {dynamic_min_common_genres}
                """
                cursor.execute(query)
                candidate_ids = [row[0] for row in cursor.fetchall()]
        elif dynamic_min_common_genres == 0:
            # Если не требуется общих жанров, берем все игры
            candidate_ids = list(Game.objects.values_list('id', flat=True))

        # Фильтруем по Single player если требуется
        if has_single_player and single_player_mode_id and candidate_ids:
            with connection.cursor() as cursor:
                # Создаем строку с ID кандидатов
                candidate_ids_str = ','.join(map(str, candidate_ids))

                # Получаем игры, которые имеют режим Single player
                query = f"""
                    SELECT DISTINCT game_id
                    FROM games_game_game_modes 
                    WHERE gamemode_id = {single_player_mode_id}
                    AND game_id IN ({candidate_ids_str})
                """
                cursor.execute(query)
                games_with_single_player = set([row[0] for row in cursor.fetchall()])

                # Фильтруем кандидатов
                candidate_ids = [game_id for game_id in candidate_ids
                                 if game_id in games_with_single_player]

            print(f"Отфильтровано по режиму Single player: осталось {len(candidate_ids)} кандидатов")

        print(f"Найдено {len(candidate_ids)} кандидатов за {time.time() - filter_time:.2f} сек")
        return candidate_ids

    def _prepare_candidate_data(self, candidate_ids):
        """Подготовка данных кандидатов"""
        import time
        from .models import Game

        print("Этап 2: Подготовка данных для расчета...")
        prep_time = time.time()

        games_data = {}

        if not candidate_ids:
            return games_data

        # Получаем игры-кандидаты
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
        """Подсчет общих элементов между исходной игрой и кандидатами"""
        import time
        from django.db import connection

        print("Этап 3: Подсчет общих элементов...")
        count_time = time.time()

        if not candidate_ids:
            return games_data

        candidate_ids_str = ','.join(map(str, candidate_ids))

        with connection.cursor() as cursor:
            # Проверяем наличие режима Single player у кандидатов
            single_player_mode_id = self._get_single_player_mode_id()
            if single_player_mode_id:
                query = f"""
                    SELECT game_id
                    FROM games_game_game_modes 
                    WHERE game_id IN ({candidate_ids_str}) 
                    AND gamemode_id = {single_player_mode_id}
                """
                cursor.execute(query)
                for row in cursor.fetchall():
                    game_id = row[0]
                    if game_id in games_data:
                        games_data[game_id]['has_single_player'] = True

            # Подсчет общих элементов для каждой категории
            categories = [
                ('genres', source_data['genre_ids'], 'games_game_genres', 'genre_id'),
                ('keywords', source_data['keyword_ids'], 'games_game_keywords', 'keyword_id'),
                ('themes', source_data['theme_ids'], 'games_game_themes', 'theme_id'),
                ('developers', source_data['developer_ids'], 'games_game_developers', 'company_id'),
                ('perspectives', source_data['perspective_ids'], 'games_game_player_perspectives',
                 'playerperspective_id'),
                ('game_modes', source_data['game_mode_ids'], 'games_game_game_modes', 'gamemode_id'),
            ]

            for category, source_ids, table, field in categories:
                if source_ids:
                    source_ids_str = ','.join(map(str, source_ids))
                    query = f"""
                        SELECT game_id, COUNT(*) as common_count
                        FROM {table} 
                        WHERE game_id IN ({candidate_ids_str}) 
                        AND {field} IN ({source_ids_str})
                        GROUP BY game_id
                    """
                    cursor.execute(query)
                    for game_id, common_count in cursor.fetchall():
                        if game_id in games_data:
                            games_data[game_id][f'common_{category}'] = common_count

            # Подсчет общего количества элементов
            tables = [
                ('genres', 'games_game_genres', 'genre_id'),
                ('keywords', 'games_game_keywords', 'keyword_id'),
                ('themes', 'games_game_themes', 'theme_id'),
                ('developers', 'games_game_developers', 'company_id'),
                ('perspectives', 'games_game_player_perspectives', 'playerperspective_id'),
                ('game_modes', 'games_game_game_modes', 'gamemode_id'),
            ]

            for category, table, field in tables:
                query = f"""
                    SELECT game_id, COUNT(*) as total_count
                    FROM {table} 
                    WHERE game_id IN ({candidate_ids_str})
                    GROUP BY game_id
                """
                cursor.execute(query)
                for game_id, total_count in cursor.fetchall():
                    if game_id in games_data:
                        games_data[game_id][f'total_{category}'] = total_count

        print(f"Подсчет элементов завершен за {time.time() - count_time:.2f} сек")
        return games_data

    def _calculate_and_filter_similarity(self, games_data, source_game, source_data, min_similarity,
                                         single_player_info):
        """Расчет схожести и фильтрация по минимальному порогу"""
        import time

        print("Этап 4: Расчет схожести...")
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
                # Проверяем требование по общим жанрам
                dynamic_min_common_genres = single_player_info['dynamic_min_common_genres']
                if data['common_genres'] < dynamic_min_common_genres:
                    continue

                # Проверяем требование по Single player
                if single_player_info['has_single_player'] and not data['has_single_player']:
                    continue

                # Расчет схожести
                similarity = self._calculate_game_similarity(
                    source_data['genre_count'],
                    source_data['keyword_count'],
                    source_data['theme_count'],
                    source_data['developer_count'],
                    source_data['perspective_count'],
                    source_data['game_mode_count'],
                    data
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
        """Вспомогательный метод для расчета схожести из данных"""
        similarity = 0.0

        # 1. ЖАНРЫ (30%)
        if source_genre_count == 0 and target_data['total_genres'] == 0:
            similarity += self.GENRES_TOTAL_WEIGHT
        elif source_genre_count > 0 or target_data['total_genres'] > 0:
            if source_genre_count == target_data['total_genres'] and target_data['common_genres'] == source_genre_count:
                similarity += self.GENRES_EXACT_MATCH_WEIGHT

            total_genres_sum = source_genre_count + target_data['total_genres'] - target_data['common_genres']
            if total_genres_sum > 0:
                genre_overlap_ratio = target_data['common_genres'] / total_genres_sum
                similarity += genre_overlap_ratio * self.GENRES_PARTIAL_MATCH_WEIGHT
            elif source_genre_count == 0 or target_data['total_genres'] == 0:
                similarity += self.GENRES_PARTIAL_MATCH_WEIGHT * 0.1

        # 2. КЛЮЧЕВЫЕ СЛОВА (30%)
        if target_data['common_keywords'] > 0:
            keyword_score = min(target_data['common_keywords'] * self.KEYWORDS_ADD_PER_MATCH, self.KEYWORDS_WEIGHT)
            similarity += keyword_score

        # 3. ТЕМЫ (20%)
        if source_theme_count == 0 and target_data['total_themes'] == 0:
            similarity += self.THEMES_WEIGHT
        elif source_theme_count > 0 or target_data['total_themes'] > 0:
            total_themes_sum = source_theme_count + target_data['total_themes'] - target_data['common_themes']
            if total_themes_sum > 0:
                theme_overlap_ratio = target_data['common_themes'] / total_themes_sum
                similarity += theme_overlap_ratio * self.THEMES_WEIGHT
            elif source_theme_count == 0 or target_data['total_themes'] == 0:
                similarity += self.THEMES_WEIGHT * 0.1

        # 4. РАЗРАБОТЧИКИ (5%)
        if source_developer_count == 0 and target_data['total_developers'] == 0:
            similarity += self.DEVELOPERS_WEIGHT
        elif source_developer_count > 0 or target_data['total_developers'] > 0:
            total_developers_sum = source_developer_count + target_data['total_developers'] - target_data[
                'common_developers']
            if total_developers_sum > 0:
                developer_overlap_ratio = target_data['common_developers'] / total_developers_sum
                similarity += developer_overlap_ratio * self.DEVELOPERS_WEIGHT
            elif source_developer_count == 0 or target_data['total_developers'] == 0:
                similarity += self.DEVELOPERS_WEIGHT * 0.1

        # 5. ПЕРСПЕКТИВЫ (10%)
        if source_perspective_count == 0 and target_data['total_perspectives'] == 0:
            similarity += self.PERSPECTIVES_WEIGHT
        elif source_perspective_count > 0 or target_data['total_perspectives'] > 0:
            total_perspectives_sum = source_perspective_count + target_data['total_perspectives'] - target_data[
                'common_perspectives']
            if total_perspectives_sum > 0:
                perspective_overlap_ratio = target_data['common_perspectives'] / total_perspectives_sum
                similarity += perspective_overlap_ratio * self.PERSPECTIVES_WEIGHT
            elif source_perspective_count == 0 or target_data['total_perspectives'] == 0:
                similarity += self.PERSPECTIVES_WEIGHT * 0.1

        # 6. РЕЖИМЫ ИГРЫ (5%)
        if source_game_mode_count == 0 and target_data['total_game_modes'] == 0:
            similarity += self.GAME_MODES_WEIGHT
        elif source_game_mode_count > 0 or target_data['total_game_modes'] > 0:
            total_game_modes_sum = source_game_mode_count + target_data['total_game_modes'] - target_data[
                'common_game_modes']
            if total_game_modes_sum > 0:
                game_mode_overlap_ratio = target_data['common_game_modes'] / total_game_modes_sum
                similarity += game_mode_overlap_ratio * self.GAME_MODES_WEIGHT
            elif source_game_mode_count == 0 or target_data['total_game_modes'] == 0:
                similarity += self.GAME_MODES_WEIGHT * 0.1

        return similarity

    def calculate_similarity(self, source, target):
        """Основной метод вычисления похожести со всеми компонентами"""
        # Проверка на идентичность
        if source == target:
            return 100.0

        # Генерация ключа кэша
        cache_key = self._get_similarity_cache_key(source, target)

        # Проверка кэша
        if cache_key in self._similarity_cache:
            return self._similarity_cache[cache_key]

        similarity = 0.0

        # Получаем кэшированные данные
        source_data = self._get_cached_game_data(source)
        target_data = self._get_cached_game_data(target)

        # 1. ЖАНРЫ (30%)
        genre_score = self._calculate_genre_similarity(
            source_data['genres'],
            target_data['genres']
        )
        similarity += genre_score

        # 2. КЛЮЧЕВЫЕ СЛОВА (30%) - 1% за каждое совпадение
        keyword_score = self._calculate_keyword_similarity_per_match(
            source_data['keywords'],
            target_data['keywords']
        )
        similarity += keyword_score

        # 3. ТЕМЫ (20%)
        theme_score = self._calculate_set_similarity(
            source_data['themes'],
            target_data['themes'],
            self.THEMES_WEIGHT
        )
        similarity += theme_score

        # 4. РАЗРАБОТЧИКИ (5%)
        developer_score = self._calculate_set_similarity(
            source_data['developers'],
            target_data['developers'],
            self.DEVELOPERS_WEIGHT
        )
        similarity += developer_score

        # 5. ПЕРСПЕКТИВЫ (10%)
        perspective_score = self._calculate_set_similarity(
            source_data['perspectives'],
            target_data['perspectives'],
            self.PERSPECTIVES_WEIGHT
        )
        similarity += perspective_score

        # 6. РЕЖИМЫ ИГРЫ (5%)
        game_mode_score = self._calculate_set_similarity(
            source_data['game_modes'],
            target_data['game_modes'],
            self.GAME_MODES_WEIGHT
        )
        similarity += game_mode_score

        # Ограничиваем результат
        similarity = max(0.0, min(100.0, similarity))

        # Сохраняем в кэш
        self._similarity_cache[cache_key] = similarity

        return similarity

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

    def _get_similarity_cache_key(self, source, target):
        """Генерирует ключ для кэша схожести"""
        if isinstance(source, VirtualGame):
            source_key = f"virtual_{hash(tuple(sorted(source.genre_ids + source.keyword_ids + source.theme_ids)))}"
        else:
            source_key = f"game_{source.id}"

        if isinstance(target, VirtualGame):
            target_key = f"virtual_{hash(tuple(sorted(target.genre_ids + target.keyword_ids + target.theme_ids)))}"
        else:
            target_key = f"game_{target.id}"

        return f"{source_key}_{target_key}"

    def _get_cached_game_data(self, obj):
        """Получает или кэширует данные игры"""
        if isinstance(obj, VirtualGame):
            cache_key = f"virtual_{hash(tuple(sorted(obj.genre_ids + obj.keyword_ids + obj.theme_ids)))}"
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

    def get_similarity_breakdown(self, source, target):
        """
        Детальная разбивка похожести по компонентам
        """
        # Получаем кэшированные данные
        source_data = self._get_cached_game_data(source)
        target_data = self._get_cached_game_data(target)

        # Расчет схожести жанров
        source_genres = source_data['genres']
        target_genres = target_data['genres']
        genre_exact_match = source_genres == target_genres
        genre_score = self._calculate_genre_similarity(source_genres, target_genres)

        # Расчет схожести ключевых слов
        keyword_score = self._calculate_keyword_similarity_per_match(
            source_data['keywords'], target_data['keywords']
        )
        common_keywords = source_data['keywords'].intersection(target_data['keywords'])
        common_keyword_count = len(common_keywords)

        # Расчет остальных компонентов
        theme_score = self._calculate_set_similarity(
            source_data['themes'], target_data['themes'], self.THEMES_WEIGHT
        )
        developer_score = self._calculate_set_similarity(
            source_data['developers'], target_data['developers'], self.DEVELOPERS_WEIGHT
        )
        perspective_score = self._calculate_set_similarity(
            source_data['perspectives'], target_data['perspectives'], self.PERSPECTIVES_WEIGHT
        )
        game_mode_score = self._calculate_set_similarity(
            source_data['game_modes'], target_data['game_modes'], self.GAME_MODES_WEIGHT
        )

        # Создаем разбивку
        breakdown = {
            'genres': {
                'score': genre_score,
                'max_score': self.GENRES_TOTAL_WEIGHT,
                'exact_match': genre_exact_match,
                'exact_match_bonus': self.GENRES_EXACT_MATCH_WEIGHT if genre_exact_match else 0.0,
                'partial_match_score': genre_score - (self.GENRES_EXACT_MATCH_WEIGHT if genre_exact_match else 0.0),
                'common_elements': list(source_genres.intersection(target_genres)),
                'source_count': len(source_genres),
                'target_count': len(target_genres)
            },
            'keywords': {
                'score': keyword_score,
                'max_score': self.KEYWORDS_WEIGHT,
                'added_per_match': self.KEYWORDS_ADD_PER_MATCH,
                'common_elements': list(common_keywords),
                'common_count': common_keyword_count,
                'source_count': len(source_data['keywords']),
                'target_count': len(target_data['keywords']),
                'calculated_as': f"{common_keyword_count} × {self.KEYWORDS_ADD_PER_MATCH}% = {common_keyword_count * self.KEYWORDS_ADD_PER_MATCH}% (ограничено {self.KEYWORDS_WEIGHT}%)"
            },
            'themes': {
                'score': theme_score,
                'max_score': self.THEMES_WEIGHT,
                'common_elements': list(source_data['themes'].intersection(target_data['themes'])),
                'source_count': len(source_data['themes']),
                'target_count': len(target_data['themes'])
            },
            'developers': {
                'score': developer_score,
                'max_score': self.DEVELOPERS_WEIGHT,
                'common_elements': list(source_data['developers'].intersection(target_data['developers'])),
                'source_count': len(source_data['developers']),
                'target_count': len(target_data['developers'])
            },
            'perspectives': {
                'score': perspective_score,
                'max_score': self.PERSPECTIVES_WEIGHT,
                'common_elements': list(source_data['perspectives'].intersection(target_data['perspectives'])),
                'source_count': len(source_data['perspectives']),
                'target_count': len(target_data['perspectives'])
            },
            'game_modes': {
                'score': game_mode_score,
                'max_score': self.GAME_MODES_WEIGHT,
                'common_elements': list(source_data['game_modes'].intersection(target_data['game_modes'])),
                'source_count': len(source_data['game_modes']),
                'target_count': len(target_data['game_modes'])
            },
            'total_similarity': genre_score + keyword_score + theme_score + developer_score + perspective_score + game_mode_score
        }

        return breakdown

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
