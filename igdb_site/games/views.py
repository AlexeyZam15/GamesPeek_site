from django.db import models

# ===== СТАНДАРТНЫЕ ИМПОРТЫ =====
import time
import json
import hashlib
from urllib.parse import urlencode

# ===== DJANGO ИМПОРТЫ =====
from django.shortcuts import render, get_object_or_404
from django.db.models import Count, Prefetch, Q
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.core.cache import cache
from django.urls import reverse

# ===== ЛОКАЛЬНЫЕ ИМПОРТЫ =====
from .similarity import GameSimilarity, VirtualGame
from .models import (
    Game, Genre, Keyword, KeywordCategory, Platform,
    Theme, PlayerPerspective, Company, Series, GameMode
)

# Импортируем вспомогательные функции
from .helpers import (
    generate_compact_url_params,
    get_compact_game_list_url,
)


def convert_params_to_lists(params):
    """Конвертирует строковые параметры в списки чисел"""

    def to_int_list(param_str):
        if not param_str:
            return []
        try:
            # Удаляем пробелы и разбиваем по запятым
            return [int(x.strip()) for x in param_str.split(',') if x.strip()]
        except (ValueError, TypeError) as e:
            print(f"Error converting param string '{param_str}' to int list: {e}")
            return []

    result = {
        'genres': to_int_list(params.get('g', '')),
        'keywords': to_int_list(params.get('k', '')),
        'platforms': to_int_list(params.get('p', '')),
        'themes': to_int_list(params.get('t', '')),
        'perspectives': to_int_list(params.get('pp', '')),
        'developers': to_int_list(params.get('d', '')),
        'game_modes': to_int_list(params.get('gm', '')),
    }

    print(f"Converted params to lists: {result}")
    return result

def get_filter_data():
    """Получает данные для фильтров с кэшированием"""
    cache_key = 'game_list_filters_data_v2'  # Изменил ключ, чтобы сбросить кэш
    filter_data = cache.get(cache_key)

    if not filter_data:
        print("Fetching fresh filter data from DB...")
        filter_data = fetch_filter_data_from_db()
        cache.set(cache_key, filter_data, 600)
        print(f"Filter data cached. Keys: {list(filter_data.keys())}")
    else:
        print("Using cached filter data")

    return filter_data


def fetch_filter_data_from_db():
    """Получает данные фильтров из базы"""
    print("Fetching filter data from database...")

    try:
        # Platforms
        platforms = Platform.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('-game_count', 'name')[:50]
        print(f"Fetched {platforms.count()} platforms")

        # Keywords
        popular_keywords = Keyword.objects.filter(
            cached_usage_count__gt=0
        ).select_related('category').order_by('-cached_usage_count')[:50]
        print(f"Fetched {popular_keywords.count()} keywords")

        # Game Modes
        game_modes = GameMode.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('name')[:30]
        print(f"Fetched {game_modes.count()} game modes")

        # Themes
        themes = Theme.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('name')[:30]
        print(f"Fetched {themes.count()} themes")

        # Perspectives
        perspectives = PlayerPerspective.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('name')[:20]
        print(f"Fetched {perspectives.count()} perspectives")

        # Developers
        developers = Company.objects.annotate(
            developed_game_count=Count('developed_games')
        ).filter(developed_game_count__gt=0).order_by('name')[:30]
        print(f"Fetched {developers.count()} developers")

        return {
            'platforms': list(platforms),
            'popular_keywords': list(popular_keywords),
            'game_modes': list(game_modes),
            'themes': list(themes),
            'perspectives': list(perspectives),
            'developers': list(developers),
        }

    except Exception as e:
        print(f"Error fetching filter data: {e}")
        # Возвращаем пустые структуры в случае ошибки
        return {
            'platforms': [],
            'popular_keywords': [],
            'game_modes': [],
            'themes': [],
            'perspectives': [],
            'developers': [],
        }


def precalculate_similar_games():
    """Фоновая задача для предварительного расчета похожих игр"""
    from .models import Game
    from .similarity import GameSimilarity
    from django.core.cache import cache
    import json
    import hashlib

    similarity_engine = GameSimilarity()

    # Берем только популярные игры (например, с rating_count > 100)
    popular_games = Game.objects.filter(rating_count__gt=100)[:500]  # Топ 500 популярных

    for game in popular_games:
        # Рассчитываем похожие игры
        similar_games = similarity_engine.find_similar_games(
            source_game=game,
            min_similarity=15,
            limit=50
        )

        # Сохраняем в кэш
        cache_key_data = {
            'game_id': game.id,
            'min_similarity': 15,
            'version': 'v5_keywords_per_match'
        }
        cache_key_str = json.dumps(cache_key_data, sort_keys=True)
        cache_key = f'precalc_similar_{hashlib.md5(cache_key_str.encode()).hexdigest()}'

        cache.set(cache_key, similar_games, 86400 * 7)  # Храним неделю
        print(f"Предварительно рассчитаны похожие игры для {game.name}")

    print("Предварительный расчет завершен!")


def get_similar_games_for_game(game_obj, selected_platforms):
    """Получает похожие игры для конкретной игры БЕЗ ОГРАНИЧЕНИЙ"""
    # Ключ кэша
    cache_key_data = {
        'game_id': game_obj.id,
        'platforms': sorted(selected_platforms) if selected_platforms else [],
        'version': 'v_no_limits'
    }

    cache_key_str = json.dumps(cache_key_data, sort_keys=True)
    cache_key = f'similar_for_game_{hashlib.md5(cache_key_str.encode()).hexdigest()}'

    cached_data = cache.get(cache_key)

    if cached_data:
        similar_games = cached_data['games']
        total_count = cached_data['count']
        print(f"Using cached similar games for game {game_obj.id}")
    else:
        similarity_engine = GameSimilarity()
        # ИСПОЛЬЗУЕМ БЕЗ ОГРАНИЧЕНИЙ
        similar_games = similarity_engine.find_similar_games(
            source_game=game_obj,
            min_similarity=0,  # ← 0% вместо 15%
            limit=None
        )
        total_count = len(similar_games)
        print(f"Generated similar games for game {game_obj.id}")

        cache.set(cache_key, {
            'games': similar_games,
            'count': total_count,
            'timestamp': time.time()
        }, 86400)

    # Фильтрация по платформам
    if selected_platforms:
        similar_games = filter_by_platforms(similar_games, selected_platforms)
        total_count = len(similar_games)

    return similar_games, total_count


def get_similar_games_for_criteria(selected_criteria):
    """Получает похожие игры для критериев с OR-фильтрацией"""
    # Создаем ключ кэша
    cache_key_data = {
        'g': sorted(selected_criteria['genres']),
        'k': sorted(selected_criteria['keywords']),
        't': sorted(selected_criteria['themes']),
        'pp': sorted(selected_criteria['perspectives']),
        'd': sorted(selected_criteria['developers']),
        'gm': sorted(selected_criteria['game_modes']),
        'p': sorted(selected_criteria['platforms']),  # Добавляем платформы
        'version': 'v5_keywords_per_match'
    }

    cache_key_str = json.dumps(cache_key_data, sort_keys=True)
    cache_key = f'virtual_search_{hashlib.md5(cache_key_str.encode()).hexdigest()}'

    cached_data = cache.get(cache_key)

    if cached_data:
        similar_games = cached_data['games']
        total_count = cached_data['count']
        print(f"Using cached virtual search")
    else:
        virtual_game = VirtualGame(
            genre_ids=selected_criteria['genres'],
            keyword_ids=selected_criteria['keywords'],
            theme_ids=selected_criteria['themes'],
            perspective_ids=selected_criteria['perspectives'],
            developer_ids=selected_criteria['developers'],
            game_mode_ids=selected_criteria['game_modes']
        )

        similarity_engine = GameSimilarity()
        similar_games = similarity_engine.find_similar_games(
            source_game=virtual_game,
            min_similarity=15,
            limit=None
        )
        total_count = len(similar_games)
        print(f"Generated virtual search")

        cache.set(cache_key, {
            'games': similar_games,
            'count': total_count,
            'timestamp': time.time()
        }, 86400)  # 24 часа

    # Фильтрация по платформам (если не учли в основном кэше)
    if selected_criteria['platforms']:
        similar_games = filter_by_platforms(similar_games, selected_criteria['platforms'])
        total_count = len(similar_games)

    return similar_games, total_count


def filter_by_platforms(games_data, platform_ids):
    """Фильтрует игры по платформам"""
    filtered = []
    for item in games_data:
        game = item.get('game') if isinstance(item, dict) else item
        game_platform_ids = [p.id for p in game.platforms.all()]
        if any(pid in platform_ids for pid in game_platform_ids):
            filtered.append(item)
    return filtered


def game_detail(request, pk):
    """Детальная страница игры"""
    game = get_object_or_404(
        Game.objects.prefetch_related(
            'keywords', 'genres', 'platforms', 'themes',
            'developers', 'player_perspectives', 'game_modes', 'publishers'
        ),
        pk=pk
    )

    context = {
        'game': game,
    }
    return render(request, 'games/game_detail.html', context)


def game_comparison(request, pk2):
    """Универсальное сравнение: игра-игра или критерии-игра"""
    try:
        game2 = get_object_or_404(
            Game.objects.prefetch_related(
                'keywords', 'genres', 'platforms', 'themes',
                'developers', 'player_perspectives', 'game_modes'
            ),
            pk=pk2
        )

        source_game_id = request.GET.get('source_game')
        genres_param = request.GET.get('g', '')
        keywords_param = request.GET.get('k', '')
        themes_param = request.GET.get('t', '')
        perspectives_param = request.GET.get('pp', '')
        developers_param = request.GET.get('d', '')
        game_modes_param = request.GET.get('gm', '')

        game1 = None
        criteria_genres = []
        criteria_keywords = []
        criteria_themes = []
        criteria_perspectives = []
        criteria_developers = []
        criteria_game_modes = []

        # Определяем источник сравнения
        if source_game_id:
            try:
                game1 = Game.objects.get(pk=source_game_id)
                is_criteria_comparison = False
            except Game.DoesNotExist:
                game1 = None
                is_criteria_comparison = True
        else:
            is_criteria_comparison = True

        # Если это сравнение с критериями
        if is_criteria_comparison and (
                genres_param or keywords_param or themes_param or
                perspectives_param or developers_param or game_modes_param):
            # Конвертируем параметры
            selected_genres = [int(g) for g in genres_param.split(',') if g.strip()] if genres_param else []
            selected_keywords = [int(k) for k in keywords_param.split(',') if k.strip()] if keywords_param else []
            selected_themes = [int(t) for t in themes_param.split(',') if t.strip()] if themes_param else []
            selected_perspectives = [int(pp) for pp in perspectives_param.split(',') if
                                     pp.strip()] if perspectives_param else []
            selected_developers = [int(d) for d in developers_param.split(',') if d.strip()] if developers_param else []
            selected_game_modes = [int(gm) for gm in game_modes_param.split(',') if
                                   gm.strip()] if game_modes_param else []

            # Получаем объекты
            criteria_genres = Genre.objects.filter(id__in=selected_genres)
            criteria_keywords = Keyword.objects.filter(id__in=selected_keywords)
            criteria_themes = Theme.objects.filter(id__in=selected_themes)
            criteria_perspectives = PlayerPerspective.objects.filter(id__in=selected_perspectives)
            criteria_developers = Company.objects.filter(id__in=selected_developers)
            criteria_game_modes = GameMode.objects.filter(id__in=selected_game_modes)

        # Используем оптимизированный алгоритм
        similarity_engine = GameSimilarity()

        if is_criteria_comparison and not game1:
            source = VirtualGame(
                genre_ids=[g.id for g in criteria_genres],
                keyword_ids=[k.id for k in criteria_keywords],
                theme_ids=[t.id for t in criteria_themes],
                perspective_ids=[pp.id for pp in criteria_perspectives],
                developer_ids=[d.id for d in criteria_developers],
                game_mode_ids=[gm.id for gm in criteria_game_modes]
            )
        else:
            source = game1

        # Рассчитываем схожесть
        similarity_score = similarity_engine.calculate_similarity(source, game2)
        breakdown = similarity_engine.get_similarity_breakdown(source, game2)

        # Рассчитываем общие элементы
        if is_criteria_comparison and not game1:
            # Сравниваем критерии с игрой
            shared_genres = game2.genres.all() & criteria_genres
            shared_keywords = game2.keywords.all() & criteria_keywords
            shared_themes = game2.themes.all() & criteria_themes
            shared_perspectives = game2.player_perspectives.all() & criteria_perspectives
            shared_developers = game2.developers.all() & criteria_developers
            shared_game_modes = game2.game_modes.all() & criteria_game_modes

            source_genres = set(criteria_genres)
            source_keywords = set(criteria_keywords)
            source_themes = set(criteria_themes)
            source_perspectives = set(criteria_perspectives)
            source_developers = set(criteria_developers)
            source_game_modes = set(criteria_game_modes)
        else:
            # Сравниваем две игры
            shared_genres = game1.genres.all() & game2.genres.all()
            shared_keywords = game1.keywords.all() & game2.keywords.all()
            shared_themes = game1.themes.all() & game2.themes.all()
            shared_perspectives = game1.player_perspectives.all() & game2.player_perspectives.all()
            shared_developers = game1.developers.all() & game2.developers.all()
            shared_game_modes = game1.game_modes.all() & game2.game_modes.all()

            source_genres = set(game1.genres.all())
            source_keywords = set(game1.keywords.all())
            source_themes = set(game1.themes.all())
            source_perspectives = set(game1.player_perspectives.all())
            source_developers = set(game1.developers.all())
            source_game_modes = set(game1.game_modes.all())

        target_genres = set(game2.genres.all())
        target_keywords = set(game2.keywords.all())
        target_themes = set(game2.themes.all())
        target_perspectives = set(game2.player_perspectives.all())
        target_developers = set(game2.developers.all())
        target_game_modes = set(game2.game_modes.all())

        # Группируем общие ключевые слова по категориям
        keyword_categories = KeywordCategory.objects.all()
        shared_keywords_by_category = {}

        for category in keyword_categories:
            category_keywords = shared_keywords.filter(category=category)
            if category_keywords.exists():
                shared_keywords_by_category[category.name] = category_keywords

        context = {
            'game1': game1,
            'game2': game2,
            'criteria_genres': criteria_genres,
            'criteria_keywords': criteria_keywords,
            'criteria_themes': criteria_themes,
            'criteria_perspectives': criteria_perspectives,
            'criteria_developers': criteria_developers,
            'criteria_game_modes': criteria_game_modes,
            'similarity_score': similarity_score,
            'shared_genres': shared_genres,
            'shared_keywords': shared_keywords,
            'shared_themes': shared_themes,
            'shared_perspectives': shared_perspectives,
            'shared_developers': shared_developers,
            'shared_game_modes': shared_game_modes,
            'shared_genres_count': shared_genres.count(),
            'shared_keywords_count': shared_keywords.count(),
            'shared_themes_count': shared_themes.count(),
            'shared_perspectives_count': shared_perspectives.count(),
            'shared_developers_count': shared_developers.count(),
            'shared_game_modes_count': shared_game_modes.count(),
            'shared_keywords_by_category': shared_keywords_by_category,
            'is_criteria_comparison': is_criteria_comparison and not game1,
            'breakdown': breakdown,
            'genres_weight': int(similarity_engine.GENRES_TOTAL_WEIGHT),
            'keywords_weight': int(similarity_engine.KEYWORDS_WEIGHT),
            'keywords_add_per_match': int(similarity_engine.KEYWORDS_ADD_PER_MATCH),
            'themes_weight': int(similarity_engine.THEMES_WEIGHT),
            'developers_weight': int(similarity_engine.DEVELOPERS_WEIGHT),
            'perspectives_weight': int(similarity_engine.PERSPECTIVES_WEIGHT),
            'game_modes_weight': int(similarity_engine.GAME_MODES_WEIGHT),
            'genres_exact_match_weight': int(similarity_engine.GENRES_EXACT_MATCH_WEIGHT),
        }

        return render(request, 'games/game_comparison.html', context)

    except Exception as e:
        from django.http import HttpResponseServerError
        return HttpResponseServerError(f"Error in comparison: {str(e)}")


def extract_request_params(request):
    """Извлекает параметры из запроса"""
    return {
        'find_similar': request.GET.get('find_similar', ''),
        'g': request.GET.get('g', ''),
        'k': request.GET.get('k', ''),
        'p': request.GET.get('p', ''),
        't': request.GET.get('t', ''),
        'pp': request.GET.get('pp', ''),
        'd': request.GET.get('d', ''),
        'gm': request.GET.get('gm', ''),
        'source_game': request.GET.get('source_game', ''),
        'sort': request.GET.get('sort', ''),
        'page': request.GET.get('page', '1'),
    }


def generate_cache_key(params):
    """Генерирует ключ кэша на основе параметров"""
    cache_key_str = json.dumps(params, sort_keys=True)
    return f'game_list_full_page_{hashlib.md5(cache_key_str.encode()).hexdigest()}'



def should_find_similar(params, selected_criteria):
    """Определяет, нужно ли искать похожие игры"""
    find_similar = params.get('find_similar') == '1'

    # Автоматически включаем если выбраны критерии похожести
    if not find_similar:
        criteria = selected_criteria
        if (criteria['genres'] or criteria['keywords'] or criteria['themes'] or
                criteria['developers'] or criteria['perspectives'] or criteria['game_modes']):
            find_similar = True

    return find_similar


def get_source_game(source_game_id):
    """Получает объект исходной игры"""
    if source_game_id:
        try:
            return Game.objects.get(pk=source_game_id)
        except Game.DoesNotExist:
            return None
    return None


def has_similarity_criteria(selected_criteria):
    """Проверяет, есть ли критерии для поиска похожих"""
    criteria = selected_criteria
    return bool(criteria['genres'] or criteria['keywords'] or criteria['themes'] or
                criteria['developers'] or criteria['perspectives'] or criteria['game_modes'])


def handle_similar_games_mode(request, params, selected_criteria, source_game_obj, filter_data):
    """Обрабатывает режим поиска похожих игр"""
    # Получаем данные
    current_sort = params.get('sort', '-similarity')
    page_number = params.get('page', 1)
    find_similar = True  # В этом режиме всегда поиск похожих

    # Определяем источник для поиска
    if source_game_obj:
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, selected_criteria['platforms']
        )
        source_game = create_source_game_object(game_obj=source_game_obj)
    else:
        similar_games_data, total_count = get_similar_games_for_criteria(
            selected_criteria
        )
        source_game = create_source_game_object(
            source_game_id=params.get('source_game'),
            selected_criteria=selected_criteria
        )

    # Форматируем данные
    games_with_similarity = format_similar_games_data(similar_games_data)

    # Сортируем
    sort_similar_games(games_with_similarity, current_sort)

    # Пагинация
    page_obj, paginator, is_paginated = paginate_results(
        games_with_similarity, page_number, items_per_page=12
    )

    # Собираем контекст
    return build_context(
        mode='similar',
        page_obj=page_obj,
        paginator=paginator,
        is_paginated=is_paginated,
        total_count=total_count,
        selected_criteria=selected_criteria,
        filter_data=filter_data,  # КРИТИЧЕСКО ВАЖНО: передаем filter_data
        params=params,
        source_game=source_game,
        source_game_obj=source_game_obj,
        current_sort=current_sort,
        find_similar=find_similar
    )


def create_source_game_object(game_obj=None, source_game_id=None, selected_criteria=None):
    """Создает объект source_game для шаблона"""

    class SimpleSourceGame:
        def __init__(self):
            if game_obj:
                self.id = game_obj.id
                self.name = game_obj.name
                self.source_game_id = game_obj.id
                self.genres_ids = list(game_obj.genres.values_list('id', flat=True))
            else:
                self.id = source_game_id
                self.name = "Search Criteria" if not source_game_id else f"Game #{source_game_id}"
                self.source_game_id = source_game_id
                self.genres_ids = selected_criteria['genres'] if selected_criteria else []

    return SimpleSourceGame()


def handle_regular_mode(request, params, selected_criteria, filter_data):
    """Обрабатывает обычный режим фильтрации"""
    current_sort = params.get('sort', '-rating_count')
    page_number = params.get('page', 1)
    find_similar = False  # В обычном режиме не ищем похожие

    # Получаем отфильтрованные игры
    games, total_count = get_filtered_games(selected_criteria, current_sort)

    # Пагинация
    page_obj, paginator, is_paginated = paginate_results(
        games, page_number, items_per_page=16
    )

    # Собираем контекст
    return build_context(
        mode='regular',
        page_obj=page_obj,
        paginator=paginator,
        is_paginated=is_paginated,
        total_count=total_count,
        selected_criteria=selected_criteria,
        filter_data=filter_data,  # КРИТИЧЕСКО ВАЖНО: передаем filter_data
        params=params,
        source_game=None,
        source_game_obj=None,
        current_sort=current_sort,
        find_similar=find_similar
    )


def get_filtered_games(selected_criteria, sort_field):
    """Получает отфильтрованные игры"""
    # Создаем ключ кэша
    filter_key = create_filter_cache_key(selected_criteria, sort_field)

    cached_data = cache.get(filter_key)

    if cached_data:
        games = cached_data['queryset']
        total_count = cached_data['count']
        print(f"Using cached filtered games")
    else:
        games = Game.objects.all()

        # Применяем фильтры
        games = apply_filters(games, selected_criteria)

        # Сортировка
        if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count',
                          '-first_release_date']:
            games = games.order_by(sort_field)
        else:
            games = games.order_by('-rating_count')

        total_count = games.count()

        cache.set(filter_key, {
            'queryset': games,
            'count': total_count,
            'timestamp': time.time()
        }, 300)

    return games, total_count


def apply_filters(queryset, selected_criteria):
    """Применяет фильтры к queryset"""
    if selected_criteria['genres']:
        queryset = queryset.filter(genres__id__in=selected_criteria['genres'])
    if selected_criteria['keywords']:
        queryset = queryset.filter(keywords__id__in=selected_criteria['keywords'])
    if selected_criteria['platforms']:
        queryset = queryset.filter(platforms__id__in=selected_criteria['platforms'])
    if selected_criteria['themes']:
        queryset = queryset.filter(themes__id__in=selected_criteria['themes'])
    if selected_criteria['perspectives']:
        queryset = queryset.filter(player_perspectives__id__in=selected_criteria['perspectives'])
    if selected_criteria['developers']:
        queryset = queryset.filter(developers__id__in=selected_criteria['developers'])
    if selected_criteria['game_modes']:
        queryset = queryset.filter(game_modes__id__in=selected_criteria['game_modes'])

    return queryset.distinct()


def format_similar_games_data(similar_games_data):
    """Форматирует данные похожих игр"""
    return [
        {
            'game': item['game'],
            'similarity': item['similarity'],
        }
        for item in similar_games_data
    ]


def sort_similar_games(games_with_similarity, current_sort):
    """Сортирует похожие игры"""

    if current_sort == '-rating_count':
        games_with_similarity.sort(key=lambda x: x['game'].rating_count or 0, reverse=True)
    elif current_sort == '-rating':
        games_with_similarity.sort(key=lambda x: x['game'].rating or 0, reverse=True)
    elif current_sort == 'name':
        games_with_similarity.sort(key=lambda x: x['game'].name.lower())
    elif current_sort == '-name':
        games_with_similarity.sort(key=lambda x: x['game'].name.lower(), reverse=True)
    elif current_sort == '-first_release_date':
        games_with_similarity.sort(key=lambda x: x['game'].first_release_date or '', reverse=True)
    elif current_sort == '-similarity':
        # ДОЛЖНО БЫТЬ ТАК: сортировка по проценту похожести
        games_with_similarity.sort(key=lambda x: x['similarity'], reverse=True)
    else:
        # По умолчанию сортируем по похожести
        games_with_similarity.sort(key=lambda x: x['similarity'], reverse=True)


def paginate_results(data, page_number, items_per_page):
    """Пагинирует данные"""
    paginator = Paginator(data, items_per_page)

    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    return page_obj, paginator, paginator.num_pages > 1


def create_filter_cache_key(selected_criteria, sort_field):
    """Создает ключ кэша для фильтров"""
    parts = []

    for key, value in selected_criteria.items():
        if value:
            parts.append(f"{key}_{'-'.join(map(str, sorted(value)))}")

    parts.append(f"sort_{sort_field}")

    return f'filtered_games_{"_".join(parts)}' if parts else 'filtered_games_all'


def build_context(mode, **kwargs):
    """Собирает контекст для шаблона"""
    # Получаем жанры из кэша
    genres_list = cache.get('genres_list')
    if not genres_list:
        genres_list = list(Genre.objects.all().only('id', 'name').order_by('name'))
        cache.set('genres_list', genres_list, 600)

    # Генерируем URL параметры
    compact_url_params = generate_compact_url_params(
        find_similar=(mode == 'similar'),
        genres=kwargs['selected_criteria']['genres'],
        keywords=kwargs['selected_criteria']['keywords'],
        platforms=kwargs['selected_criteria']['platforms'],
        themes=kwargs['selected_criteria']['themes'],
        perspectives=kwargs['selected_criteria']['perspectives'],
        developers=kwargs['selected_criteria']['developers'],
        game_modes=kwargs['selected_criteria']['game_modes'],
        sort=kwargs.get('current_sort', '')
    )

    # Получаем данные фильтров
    filter_data = kwargs.get('filter_data', {})

    base_context = {
        'genres': genres_list,
        'keyword_categories': KeywordCategory.objects.all().only('id', 'name'),
        'current_sort': kwargs.get('current_sort', ''),
        'find_similar': kwargs.get('find_similar', False),
        'compact_url_params': compact_url_params,

        # Выбранные критерии
        'selected_genres': kwargs['selected_criteria']['genres'],
        'selected_keywords': kwargs['selected_criteria']['keywords'],
        'selected_platforms': kwargs['selected_criteria']['platforms'],
        'selected_themes': kwargs['selected_criteria']['themes'],
        'selected_perspectives': kwargs['selected_criteria']['perspectives'],
        'selected_developers': kwargs['selected_criteria']['developers'],
        'selected_game_modes': kwargs['selected_criteria']['game_modes'],

        # Данные для фильтров (с проверкой на существование)
        'popular_keywords': filter_data.get('popular_keywords', []),
        'platforms': filter_data.get('platforms', []),
        'themes': filter_data.get('themes', []),
        'perspectives': filter_data.get('perspectives', []),
        'developers': filter_data.get('developers', []),
        'game_modes': filter_data.get('game_modes', []),

        # Передаем основные параметры
        'page_obj': kwargs.get('page_obj'),
        'paginator': kwargs.get('paginator'),
        'is_paginated': kwargs.get('is_paginated', False),
        'total_count': kwargs.get('total_count', 0),
        'source_game': kwargs.get('source_game'),
        'source_game_obj': kwargs.get('source_game_obj'),
        'params': kwargs.get('params', {}),
        'selected_criteria': kwargs.get('selected_criteria', {}),
    }

    # Добавляем специфичные для режима поля
    if mode == 'similar':
        base_context.update({
            'games_with_similarity': kwargs.get('page_obj').object_list if kwargs.get('page_obj') else [],
            'games': [],
            'show_similarity': True,
        })
    else:
        base_context.update({
            'games': kwargs.get('page_obj').object_list if kwargs.get('page_obj') else [],
            'games_with_similarity': [],
            'show_similarity': False,
        })

    # Проверяем наличие данных фильтров
    if not filter_data:
        print("WARNING: filter_data is empty in build_context")

    # Для отладки
    debug_info = {
        'mode': mode,
        'genres_count': len(base_context['genres']),
        'platforms_count': len(base_context['platforms']),
        'themes_count': len(base_context['themes']),
        'perspectives_count': len(base_context['perspectives']),
        'developers_count': len(base_context['developers']),
        'game_modes_count': len(base_context['game_modes']),
        'selected_criteria_counts': {
            'genres': len(base_context['selected_genres']),
            'keywords': len(base_context['selected_keywords']),
            'platforms': len(base_context['selected_platforms']),
            'themes': len(base_context['selected_themes']),
            'perspectives': len(base_context['selected_perspectives']),
            'developers': len(base_context['selected_developers']),
            'game_modes': len(base_context['selected_game_modes']),
        }
    }
    print(f"DEBUG build_context: {debug_info}")

    return base_context


def game_list(request):
    """Главная функция списка игр - координатор"""
    # Получаем параметры запроса
    params = extract_request_params(request)

    # Генерируем ключ кэша
    cache_key = generate_cache_key(params)

    # Проверяем кэш полной страницы
    cached_page = cache.get(cache_key)
    if cached_page:
        return cached_page

    start_time = time.time()

    # Получаем данные фильтров
    filter_data = get_filter_data()

    # Конвертируем параметры
    selected_criteria = convert_params_to_lists(params)

    # Определяем режим работы
    find_similar = should_find_similar(params, selected_criteria)

    # Получаем исходную игру если есть
    source_game_obj = get_source_game(params.get('source_game'))

    # Обрабатываем в зависимости от режима
    if find_similar and has_similarity_criteria(selected_criteria):
        result = handle_similar_games_mode(
            request, params, selected_criteria,
            source_game_obj, filter_data
        )
    else:
        result = handle_regular_mode(
            request, params, selected_criteria, filter_data
        )

    # Добавляем время выполнения
    result['execution_time'] = round(time.time() - start_time, 3)

    # Обновляем: добавляем выбранные критерии в контекст
    result.update({
        'selected_genres': selected_criteria['genres'],
        'selected_keywords': selected_criteria['keywords'],
        'selected_platforms': selected_criteria['platforms'],
        'selected_themes': selected_criteria['themes'],
        'selected_perspectives': selected_criteria['perspectives'],
        'selected_developers': selected_criteria['developers'],
        'selected_game_modes': selected_criteria['game_modes'],
    })

    # Рендерим и кэшируем
    response = render(request, 'games/game_list.html', result)
    cache.set(cache_key, response, 300)

    return response


def home(request):
    """Главная страница с популярными играми"""
    popular_games = Game.objects.filter(
        rating_count__gt=10
    ).prefetch_related('genres', 'platforms', 'themes', 'developers').order_by('-rating_count')[:12]

    recent_games = Game.objects.filter(
        first_release_date__isnull=False
    ).prefetch_related('genres', 'platforms', 'themes').order_by('-first_release_date')[:12]

    # Популярные теги
    popular_keywords = Keyword.objects.filter(
        cached_usage_count__gt=0
    ).select_related('category').order_by('-cached_usage_count')[:30]

    context = {
        'popular_games': popular_games,
        'recent_games': recent_games,
        'popular_keywords': popular_keywords,
    }
    return render(request, 'games/home.html', context)


def keyword_category_view(request, category_id):
    """Просмотр игр по категории ключевых слов"""
    category = get_object_or_404(KeywordCategory, id=category_id)

    games = Game.objects.filter(
        keywords__category=category
    ).prefetch_related('genres', 'platforms', 'keywords').distinct()

    # Популярные ключевые слова в этой категории
    popular_keywords = Keyword.objects.filter(
        category=category,
        game__isnull=False
    ).annotate(
        game_count=models.Count('game')
    ).order_by('-game_count')[:20]

    context = {
        'category': category,
        'games': games,
        'popular_keywords': popular_keywords,
    }
    return render(request, 'games/keyword_category.html', context)


def game_search(request):
    """Простой поиск игр по названию"""
    search_query = request.GET.get('q', '')

    games = Game.objects.all().prefetch_related('genres', 'platforms', 'themes', 'developers')

    if search_query:
        games = games.filter(name__icontains=search_query)

    # Сортировка по популярности
    games = games.order_by('-rating_count', '-rating')

    context = {
        'games': games,
        'search_query': search_query,
        'total_results': games.count(),
    }
    return render(request, 'games/game_search.html', context)


def platform_list(request):
    """Страница со списком всех платформ"""
    platforms = Platform.objects.annotate(
        game_count=Count('game')
    ).filter(game_count__gt=0).order_by('-game_count', 'name')

    context = {
        'platforms': platforms,
    }
    return render(request, 'games/platform_list.html', context)


def platform_games(request, platform_id):
    """Список игр для конкретной платформы"""
    platform = get_object_or_404(Platform, id=platform_id)
    games = Game.objects.filter(platforms=platform).prefetch_related(
        'genres', 'platforms', 'keywords', 'themes', 'developers'
    ).order_by('-rating_count', '-rating')

    # Пагинация
    paginator = Paginator(games, 20)
    page = request.GET.get('page', 1)

    try:
        page_obj = paginator.page(page)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    context = {
        'platform': platform,
        'games': page_obj,
        'total_games': games.count(),
        'page_obj': page_obj,
        'is_paginated': paginator.num_pages > 1,
    }
    return render(request, 'games/platform_games.html', context)
