"""Views for game list page."""

from django.utils import timezone
from typing import Dict, List, Tuple, Any, Optional
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.db.models import Prefetch
from django.template.loader import render_to_string
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
import time

from ..models import GameCardCache

from .base_views import (
    logger, CACHE_TIMES, get_cache_key, cache_get_or_set,
    extract_request_params, convert_params_to_lists, _apply_filters,
    _get_cached_years_range, _get_optimized_filter_data, _get_cached_game,
    _get_cached_genres_list, _get_cached_filter_data,
    _format_similar_games_data, _sort_similar_games,
    SimpleSourceGame, GameSimilarity, VirtualGame,
    Game, Genre, Platform, PlayerPerspective, Keyword, Theme, Company, GameMode,
    GameTypeEnum, cache, GameEngine  # Добавлен GameEngine
)
from ..utils.filter_renderer import FilterRenderer
from django.http import JsonResponse


# Константа для количества игр на страницу - теперь используется сервером
ITEMS_PER_PAGE = 16


def ajax_load_keywords(request: HttpRequest) -> HttpResponse:
    """
    AJAX endpoint for loading keywords with pagination and search.
    Supports mobile mode with fewer items per page.
    """
    from ..models import Keyword
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
    from django.template.loader import render_to_string
    from django.http import JsonResponse

    page_num = request.GET.get('page', '1')
    search_term = request.GET.get('search', '').strip()
    selected_ids = request.GET.get('selected', '')
    selected_id_list = [int(x) for x in selected_ids.split(',') if x.isdigit()]
    container_type = request.GET.get('type', 'search')
    input_name = request.GET.get('input_name', 'keywords')
    container_id = request.GET.get('container_id', 'keywords-container')
    is_mobile = request.GET.get('mobile', '0') == '1'

    # Определяем количество элементов на странице
    items_per_page = 8 if is_mobile else 18

    # Базовый запрос
    keywords_qs = Keyword.objects.select_related('category').only(
        'id', 'name', 'category__id', 'category__name', 'cached_usage_count'
    )

    # Поиск по названию
    if search_term:
        keywords_qs = keywords_qs.filter(name__icontains=search_term)

    # Сортировка по популярности
    keywords_qs = keywords_qs.order_by('-cached_usage_count', 'name')

    # Пагинация
    paginator = Paginator(keywords_qs, items_per_page)

    try:
        page_obj = paginator.page(page_num)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    # Рендерим HTML
    html = render_to_string('games/game_list/partials/_keyword_list_items.html', {
        'keywords': page_obj.object_list,
        'page_obj': page_obj,
        'paginator': paginator,
        'selected_ids': selected_id_list,
        'container_id': container_id,
        'input_name': input_name,
        'container_type': container_type,
        'search_term': search_term,
        'is_mobile': is_mobile,
    })

    return JsonResponse({
        'html': html,
        'has_next': page_obj.has_next(),
        'has_previous': page_obj.has_previous(),
        'current_page': page_obj.number,
        'total_pages': paginator.num_pages,
        'total_count': paginator.count,
        'items_per_page': items_per_page,
    })

def search_results(request: HttpRequest) -> HttpResponse:
    """
    Отдельная страница для поиска игр через панель поиска вверху.
    """
    start_time = time.time()

    print(f"GET params: {dict(request.GET)}")
    search_query = request.GET.get('q', '').strip()
    page_num = request.GET.get('page', '1')
    sort_field = request.GET.get('sort', '-rating_count')

    print(f"Search query: '{search_query}'")
    print(f"Page: {page_num}, Sort: {sort_field}")

    try:
        page_num = int(page_num)
    except (ValueError, TypeError):
        page_num = 1

    # Если есть поисковой запрос
    if search_query:
        games_qs = Game.objects.all().only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        )

        from django.db.models import Q

        # Разбиваем запрос на слова
        words = search_query.lower().split()

        # Создаём фильтр для поиска по всем словам в названии
        name_filter = Q()
        for word in words:
            name_filter &= Q(name__icontains=word)

        games_qs = games_qs.filter(name_filter)

        # Сортировка
        if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
            games_qs = games_qs.order_by(sort_field)
        else:
            games_qs = games_qs.order_by('-rating_count')

        paginator = Paginator(games_qs, ITEMS_PER_PAGE)

        try:
            page_obj = paginator.page(page_num)
        except PageNotAnInteger:
            page_obj = paginator.page(1)
        except EmptyPage:
            page_obj = paginator.page(paginator.num_pages)

        total_count = paginator.count
        total_pages = paginator.num_pages
        games = list(page_obj.object_list)

        # Обновляем игры с кэшированными карточками
        games = _update_games_with_cached_cards(
            games,
            {
                'show_similarity': False,
                'current_page': page_num,
            }
        )

        start_index = (page_num - 1) * ITEMS_PER_PAGE + 1
        end_index = min(page_num * ITEMS_PER_PAGE, total_count)

    else:
        # Нет поискового запроса
        games = []
        page_obj = None
        paginator = None
        total_count = 0
        total_pages = 1
        start_index = 0
        end_index = 0

    years_range = _get_cached_years_range()

    context = {
        'search_query': search_query,
        'games': games,
        'page_obj': page_obj,
        'paginator': paginator,
        'is_paginated': paginator and paginator.num_pages > 1 if paginator else False,
        'total_count': total_count,
        'total_pages': total_pages,
        'current_page': page_num,
        'start_index': start_index,
        'end_index': end_index,
        'items_per_page': ITEMS_PER_PAGE,
        'years_range': years_range,
        'current_sort': sort_field,
        'execution_time': round(time.time() - start_time, 3),
        'show_similarity': False,
        'find_similar': False,
        'source_game': None,
        'source_game_obj': None,
    }

    return render(request, 'games/search_results.html', context)


def game_list(request: HttpRequest) -> HttpResponse:
    """
    Main game list function - renders filters on server with aggressive caching.
    Filters are rendered server-side, not via AJAX.
    """
    start_time = time.time()

    params = extract_request_params(request)
    selected_criteria = convert_params_to_lists(params)

    years_range = _get_cached_years_range()

    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = _get_cached_game(params['source_game'])
        except (Game.DoesNotExist, ValueError):
            pass

    filter_data = _get_optimized_filter_data()

    # Извлекаем search_* параметры из запроса
    search_genres = request.GET.get('search_g', '')
    search_keywords = request.GET.get('search_k', '')
    search_platforms = request.GET.get('search_p', '')
    search_themes = request.GET.get('search_t', '')
    search_perspectives = request.GET.get('search_pp', '')
    search_game_modes = request.GET.get('search_gm', '')
    search_game_types = request.GET.get('search_gt', '')
    search_engines = request.GET.get('search_e', '')
    search_year_start = request.GET.get('search_ys', '')
    search_year_end = request.GET.get('search_ye', '')

    # Преобразуем строки в списки ID
    search_selected = {
        'genres': [int(x) for x in search_genres.split(',') if x.isdigit()] if search_genres else [],
        'keywords': [int(x) for x in search_keywords.split(',') if x.isdigit()] if search_keywords else [],
        'platforms': [int(x) for x in search_platforms.split(',') if x.isdigit()] if search_platforms else [],
        'themes': [int(x) for x in search_themes.split(',') if x.isdigit()] if search_themes else [],
        'perspectives': [int(x) for x in search_perspectives.split(',') if x.isdigit()] if search_perspectives else [],
        'game_modes': [int(x) for x in search_game_modes.split(',') if x.isdigit()] if search_game_modes else [],
        'game_types': [int(x) for x in search_game_types.split(',') if x.isdigit()] if search_game_types else [],
        'engines': [int(x) for x in search_engines.split(',') if x.isdigit()] if search_engines else [],
        'release_year_start': int(search_year_start) if search_year_start and search_year_start.isdigit() else None,
        'release_year_end': int(search_year_end) if search_year_end and search_year_end.isdigit() else None,
    }

    search_selected_objects = _get_selected_criteria_objects(search_selected)

    # similarity_selected использует selected_criteria (из URL параметров без search_ префикса)
    similarity_selected = {
        'genres': selected_criteria.get('genres', []),
        'keywords': selected_criteria.get('keywords', []),
        'platforms': selected_criteria.get('platforms', []),
        'themes': selected_criteria.get('themes', []),
        'perspectives': selected_criteria.get('perspectives', []),
        'game_modes': selected_criteria.get('game_modes', []),
        'game_types': selected_criteria.get('game_types', []),
        'engines': selected_criteria.get('engines', []),
        'developers': selected_criteria.get('developers', []),
    }

    if find_similar and source_game_obj:
        if not any(v for k, v in similarity_selected.items() if k != 'developers'):
            similarity_selected['genres'] = [g.id for g in source_game_obj.genres.all()]
            similarity_selected['keywords'] = [k.id for k in source_game_obj.keywords.all()]
            similarity_selected['themes'] = [t.id for t in source_game_obj.themes.all()]
            similarity_selected['perspectives'] = [p.id for p in source_game_obj.player_perspectives.all()]
            similarity_selected['engines'] = [e.id for e in source_game_obj.engines.all()]

    similarity_selected_objects = _get_selected_criteria_objects(similarity_selected)

    # Генерируем хеш параметров для кэширования фильтров
    params_hash = _get_params_hash_from_request(request)

    # Рендерим фильтры на сервере с кэшированием - передаем similarity_selected с engines
    cached_filter_sections = _render_filters_with_cache(
        params_hash,
        filter_data,
        years_range,
        search_selected,
        similarity_selected,
        params.get('sort', '')
    )

    context = {
        'games': [],
        'games_with_similarity': [],
        'page_obj': None,
        'paginator': None,
        'is_paginated': False,
        'total_count': 0,
        'total_pages': 1,
        'current_page': 1,
        'start_index': 0,
        'end_index': 0,
        'items_per_page': ITEMS_PER_PAGE,

        'find_similar': find_similar,
        'show_similarity': find_similar,
        'source_game': None,
        'source_game_obj': source_game_obj,

        'genres': _get_cached_genres_list(),
        'themes': filter_data['themes'],
        'perspectives': filter_data['perspectives'],
        'game_modes': filter_data['game_modes'],
        'keywords': filter_data['keywords'],
        'platforms': filter_data['platforms'],
        'popular_keywords': filter_data['popular_keywords'],
        'game_types': GameTypeEnum.CHOICES,
        'engines': filter_data.get('engines', []),

        'years_range': years_range,
        'current_year': timezone.now().year,

        'cached_filter_sections': cached_filter_sections,

        'search_selected_genres': search_selected['genres'],
        'search_selected_keywords': search_selected['keywords'],
        'search_selected_platforms': search_selected['platforms'],
        'search_selected_themes': search_selected['themes'],
        'search_selected_perspectives': search_selected['perspectives'],
        'search_selected_game_modes': search_selected['game_modes'],
        'search_selected_game_types': search_selected['game_types'],
        'search_selected_engines': search_selected['engines'],
        'search_selected_release_year_start': search_selected['release_year_start'],
        'search_selected_release_year_end': search_selected['release_year_end'],

        'search_selected_genres_objects': search_selected_objects.get('genres', []),
        'search_selected_keywords_objects': search_selected_objects.get('keywords', []),
        'search_selected_platforms_objects': search_selected_objects.get('platforms', []),
        'search_selected_themes_objects': search_selected_objects.get('themes', []),
        'search_selected_perspectives_objects': search_selected_objects.get('perspectives', []),
        'search_selected_game_modes_objects': search_selected_objects.get('game_modes', []),
        'search_selected_engines_objects': search_selected_objects.get('engines', []),

        'similarity_selected_genres': similarity_selected['genres'],
        'similarity_selected_keywords': similarity_selected['keywords'],
        'similarity_selected_platforms': similarity_selected['platforms'],
        'similarity_selected_themes': similarity_selected['themes'],
        'similarity_selected_perspectives': similarity_selected['perspectives'],
        'similarity_selected_game_modes': similarity_selected['game_modes'],
        'similarity_selected_game_types': similarity_selected['game_types'],
        'similarity_selected_engines': similarity_selected.get('engines', []),
        'similarity_selected_developers': similarity_selected.get('developers', []),

        'similarity_selected_genres_objects': similarity_selected_objects.get('genres', []),
        'similarity_selected_keywords_objects': similarity_selected_objects.get('keywords', []),
        'similarity_selected_platforms_objects': similarity_selected_objects.get('platforms', []),
        'similarity_selected_themes_objects': similarity_selected_objects.get('themes', []),
        'similarity_selected_perspectives_objects': similarity_selected_objects.get('perspectives', []),
        'similarity_selected_game_modes_objects': similarity_selected_objects.get('game_modes', []),
        'similarity_selected_engines_objects': similarity_selected_objects.get('engines', []),
        'similarity_selected_developers_objects': similarity_selected_objects.get('developers', []),

        'selected_genres': search_selected['genres'],
        'selected_keywords': search_selected['keywords'],
        'selected_platforms': search_selected['platforms'],
        'selected_themes': search_selected['themes'],
        'selected_perspectives': search_selected['perspectives'],
        'selected_game_modes': search_selected['game_modes'],
        'selected_game_types': search_selected['game_types'],
        'selected_release_year_start': search_selected['release_year_start'],
        'selected_release_year_end': search_selected['release_year_end'],
        'selected_developers': similarity_selected.get('developers', []),
        'selected_engines': similarity_selected.get('engines', []),

        'selected_genres_objects': search_selected_objects.get('genres', []),
        'selected_keywords_objects': search_selected_objects.get('keywords', []),
        'selected_platforms_objects': search_selected_objects.get('platforms', []),
        'selected_themes_objects': search_selected_objects.get('themes', []),
        'selected_perspectives_objects': search_selected_objects.get('perspectives', []),
        'selected_game_modes_objects': search_selected_objects.get('game_modes', []),
        'selected_developers_objects': similarity_selected_objects.get('developers', []),
        'selected_engines_objects': similarity_selected_objects.get('engines', []),

        'current_sort': params.get('sort', ''),

        'execution_time': round(time.time() - start_time, 3),

        'debug_info': {
            'mode': 'server_rendered_filters',
            'message': 'Filters rendered on server with caching',
            'find_similar': find_similar,
            'has_source_game': source_game_obj is not None,
            'engines_count': len(filter_data.get('engines', [])),
        }
    }

    if source_game_obj:
        from .base_views import SimpleSourceGame

        game_criteria = {
            'genres': similarity_selected['genres'],
            'keywords': similarity_selected['keywords'],
            'themes': similarity_selected['themes'],
            'perspectives': similarity_selected['perspectives'],
            'developers': similarity_selected.get('developers', []),
            'game_modes': similarity_selected['game_modes'],
            'engines': similarity_selected.get('engines', []),
        }

        context['source_game'] = SimpleSourceGame(
            game_obj=source_game_obj,
            criteria=game_criteria,
            display_name=source_game_obj.name
        )

    return render(request, 'games/game_list.html', context)


def _get_params_hash_from_request(request: HttpRequest) -> str:
    """
    Генерирует хеш параметров запроса для инвалидации кэша фильтров.

    Args:
        request: HTTP request object

    Returns:
        MD5 хеш параметров, влияющих на отображение фильтров
    """
    import hashlib
    import json

    filter_params = {}
    param_keys = ['g', 'k', 'p', 't', 'pp', 'gm', 'gt', 'e', 'ys', 'ye', 'source_game', 'find_similar', 'sort']

    for key in param_keys:
        value = request.GET.get(key, '')
        if value:
            filter_params[key] = value

    param_string = json.dumps(filter_params, sort_keys=True)
    return hashlib.md5(param_string.encode()).hexdigest()


def _render_filters_with_cache(
        params_hash: str,
        filter_data: Dict,
        years_range: Dict,
        search_selected: Dict,
        similarity_selected: Dict,
        current_sort: str
) -> Dict[str, str]:
    """
    Рендерит все секции фильтров с использованием модели FilterSectionCache.
    """
    from ..models_parts.filter_cache import FilterSectionCache
    from ..utils.filter_renderer import FilterRenderer
    from ..models import Genre, GameTypeEnum

    print("=" * 60)
    print("DEBUG _render_filters_with_cache:")
    print(f"filter_data keys: {list(filter_data.keys())}")
    print(f"filter_data has 'genres': {'genres' in filter_data}")
    print(f"filter_data genres count: {len(filter_data.get('genres', []))}")
    print(f"filter_data has 'game_types': {'game_types' in filter_data}")
    print(f"filter_data has 'platforms': {'platforms' in filter_data}")
    print(f"filter_data has 'keywords': {'keywords' in filter_data}")
    print(f"filter_data has 'themes': {'themes' in filter_data}")
    print(f"filter_data has 'perspectives': {'perspectives' in filter_data}")
    print(f"filter_data has 'game_modes': {'game_modes' in filter_data}")
    print(f"filter_data has 'engines': {'engines' in filter_data}")
    print("=" * 60)

    result = {}
    renderer = FilterRenderer()

    # Получаем списки напрямую из базы данных
    genres_list = list(Genre.objects.all().only('id', 'name').order_by('name'))
    game_types_list = GameTypeEnum.CHOICES

    print(f"Direct DB query - genres count: {len(genres_list)}")
    print(f"Direct DB query - game types count: {len(game_types_list)}")
    print(f"search_selected genres: {search_selected.get('genres', [])}")
    print(f"similarity_selected genres: {similarity_selected.get('genres', [])}")
    print("=" * 60)

    # Получаем данные фильтров с учётом выбранных ключевых слов
    from .base_views import _get_optimized_filter_data

    similarity_keyword_ids = similarity_selected.get('keywords', [])
    search_keyword_ids = search_selected.get('keywords', [])
    all_selected_keywords = list(set(similarity_keyword_ids + search_keyword_ids))

    # Получаем оптимизированные данные фильтров с включением выбранных ключевых слов
    optimized_filter_data = _get_optimized_filter_data(selected_keyword_ids=all_selected_keywords)

    # Используем оптимизированные данные для ключевых слов
    keywords_for_render = optimized_filter_data.get('keywords', [])
    popular_keywords_for_render = optimized_filter_data.get('popular_keywords', [])

    # Search Filters
    result['search_platforms'] = FilterSectionCache.get_or_render(
        'search_platforms',
        params_hash,
        lambda: renderer.render_search_platforms(
            filter_data.get('platforms', []),
            search_selected.get('platforms', [])
        ),
        selected_ids=search_selected.get('platforms', [])
    )

    result['search_game_types'] = FilterSectionCache.get_or_render(
        'search_game_types',
        params_hash,
        lambda: renderer.render_search_game_types(
            game_types_list,
            search_selected.get('game_types', [])
        ),
        selected_ids=search_selected.get('game_types', [])
    )

    result['search_genres'] = FilterSectionCache.get_or_render(
        'search_genres',
        params_hash,
        lambda: renderer.render_search_genres(
            genres_list,
            search_selected.get('genres', [])
        ),
        selected_ids=search_selected.get('genres', [])
    )

    result['search_keywords'] = FilterSectionCache.get_or_render(
        'search_keywords',
        params_hash,
        lambda: renderer.render_search_keywords(
            keywords_for_render,
            search_selected.get('keywords', [])
        ),
        selected_ids=search_selected.get('keywords', [])
    )

    result['search_themes'] = FilterSectionCache.get_or_render(
        'search_themes',
        params_hash,
        lambda: renderer.render_search_themes(
            filter_data.get('themes', []),
            search_selected.get('themes', [])
        ),
        selected_ids=search_selected.get('themes', [])
    )

    result['search_perspectives'] = FilterSectionCache.get_or_render(
        'search_perspectives',
        params_hash,
        lambda: renderer.render_search_perspectives(
            filter_data.get('perspectives', []),
            search_selected.get('perspectives', [])
        ),
        selected_ids=search_selected.get('perspectives', [])
    )

    result['search_game_modes'] = FilterSectionCache.get_or_render(
        'search_game_modes',
        params_hash,
        lambda: renderer.render_search_game_modes(
            filter_data.get('game_modes', []),
            search_selected.get('game_modes', [])
        ),
        selected_ids=search_selected.get('game_modes', [])
    )

    result['search_engines'] = FilterSectionCache.get_or_render(
        'search_engines',
        params_hash,
        lambda: renderer.render_search_engines(
            filter_data.get('engines', []),
            search_selected.get('engines', [])
        ),
        selected_ids=search_selected.get('engines', [])
    )

    # Date filter
    date_context = {
        'release_year_start': search_selected.get('release_year_start'),
        'release_year_end': search_selected.get('release_year_end'),
    }
    result['search_date'] = FilterSectionCache.get_or_render(
        'search_date',
        params_hash,
        lambda: renderer.render_search_date_filter(
            search_selected.get('release_year_start'),
            search_selected.get('release_year_end'),
            years_range.get('min_year', 1970),
            years_range.get('max_year', 2024),
            years_range.get('current_year', 2024)
        ),
        context_data=date_context
    )

    # Similarity Filters
    result['similarity_genres'] = FilterSectionCache.get_or_render(
        'similarity_genres',
        params_hash,
        lambda: renderer.render_similarity_genres(
            genres_list,
            similarity_selected.get('genres', [])
        ),
        selected_ids=similarity_selected.get('genres', [])
    )

    result['similarity_keywords'] = FilterSectionCache.get_or_render(
        'similarity_keywords',
        params_hash,
        lambda: renderer.render_similarity_keywords(
            keywords_for_render,
            similarity_selected.get('keywords', [])
        ),
        selected_ids=similarity_selected.get('keywords', [])
    )

    result['similarity_themes'] = FilterSectionCache.get_or_render(
        'similarity_themes',
        params_hash,
        lambda: renderer.render_similarity_themes(
            filter_data.get('themes', []),
            similarity_selected.get('themes', [])
        ),
        selected_ids=similarity_selected.get('themes', [])
    )

    result['similarity_perspectives'] = FilterSectionCache.get_or_render(
        'similarity_perspectives',
        params_hash,
        lambda: renderer.render_similarity_perspectives(
            filter_data.get('perspectives', []),
            similarity_selected.get('perspectives', [])
        ),
        selected_ids=similarity_selected.get('perspectives', [])
    )

    result['similarity_game_modes'] = FilterSectionCache.get_or_render(
        'similarity_game_modes',
        params_hash,
        lambda: renderer.render_similarity_game_modes(
            filter_data.get('game_modes', []),
            similarity_selected.get('game_modes', [])
        ),
        selected_ids=similarity_selected.get('game_modes', [])
    )

    result['similarity_engines'] = FilterSectionCache.get_or_render(
        'similarity_engines',
        params_hash,
        lambda: renderer.render_similarity_engines(
            filter_data.get('engines', []),
            similarity_selected.get('engines', [])
        ),
        selected_ids=similarity_selected.get('engines', [])
    )

    print(f"DEBUG _render_filters_with_cache: result keys = {list(result.keys())}")
    print("=" * 60)

    return result


def _get_or_render_section(cache_key: str, render_func: callable, timeout: int) -> str:
    """
    Получает секцию фильтра из кэша или рендерит и сохраняет.

    Args:
        cache_key: Ключ кэша
        render_func: Функция для рендеринга
        timeout: Время жизни кэша в секундах

    Returns:
        HTML секции
    """
    cached_html = cache.get(cache_key)
    if cached_html:
        return cached_html

    html = render_func()
    cache.set(cache_key, html, timeout)
    return html


def ajax_load_filters(request: HttpRequest) -> HttpResponse:
    """
    Load filters HTML via AJAX - используется только для динамических обновлений.
    При первом рендеринге фильтры уже есть на странице.
    """
    from django.template.loader import render_to_string

    params_hash = _get_params_hash_from_request(request)
    params = extract_request_params(request)
    selected_criteria = convert_params_to_lists(params)
    years_range = _get_cached_years_range()
    filter_data = _get_optimized_filter_data()

    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = _get_cached_game(params['source_game'])
        except (Game.DoesNotExist, ValueError):
            pass

    search_selected = {
        'genres': [],
        'keywords': [],
        'platforms': [],
        'themes': [],
        'perspectives': [],
        'game_modes': [],
        'game_types': [],
        'engines': [],
        'release_year_start': selected_criteria['release_year_start'],
        'release_year_end': selected_criteria['release_year_end'],
    }

    similarity_selected = {
        'genres': selected_criteria['genres'],
        'keywords': selected_criteria['keywords'],
        'platforms': selected_criteria['platforms'],
        'themes': selected_criteria['themes'],
        'perspectives': selected_criteria['perspectives'],
        'game_modes': selected_criteria['game_modes'],
        'game_types': selected_criteria['game_types'],
        'engines': selected_criteria.get('engines', []),
        'developers': selected_criteria.get('developers', []),
    }

    # Используем кэшированный рендеринг
    cached_sections = _render_filters_with_cache(
        params_hash,
        filter_data,
        years_range,
        search_selected,
        similarity_selected,
        params.get('sort', '')
    )

    html = render_to_string('games/game_list/_real_filters.html', {
        'cached_filter_sections': cached_sections,
        'genres': _get_cached_genres_list(),
        'themes': filter_data['themes'],
        'perspectives': filter_data['perspectives'],
        'game_modes': filter_data['game_modes'],
        'keywords': filter_data['keywords'],
        'platforms': filter_data['platforms'],
        'engines': filter_data.get('engines', []),
        'game_types': GameTypeEnum.CHOICES,
        'years_range': years_range,
        'current_year': timezone.now().year,
        'search_selected_genres': search_selected['genres'],
        'search_selected_keywords': search_selected['keywords'],
        'search_selected_platforms': search_selected['platforms'],
        'search_selected_themes': search_selected['themes'],
        'search_selected_perspectives': search_selected['perspectives'],
        'search_selected_game_modes': search_selected['game_modes'],
        'search_selected_game_types': search_selected['game_types'],
        'search_selected_engines': search_selected['engines'],
        'search_selected_release_year_start': search_selected['release_year_start'],
        'search_selected_release_year_end': search_selected['release_year_end'],
        'similarity_selected_genres': similarity_selected['genres'],
        'similarity_selected_keywords': similarity_selected['keywords'],
        'similarity_selected_platforms': similarity_selected['platforms'],
        'similarity_selected_themes': similarity_selected['themes'],
        'similarity_selected_perspectives': similarity_selected['perspectives'],
        'similarity_selected_game_modes': similarity_selected['game_modes'],
        'similarity_selected_game_types': similarity_selected['game_types'],
        'similarity_selected_engines': similarity_selected.get('engines', []),
        'current_sort': params.get('sort', ''),
        'find_similar': find_similar,
        'source_game_obj': source_game_obj,
    })

    response = HttpResponse(html)
    response['Content-Type'] = 'text/html; charset=utf-8'
    return response


def _get_cached_card_html(game: Game, show_similarity: bool = False,
                          similarity_percent: float = None) -> Optional[str]:
    """
    Получает HTML карточки из кэша модели.
    Возвращает None если карточка не найдена - вызывающий код должен отрендерить.

    Args:
        game: Объект игры
        show_similarity: Показывать ли процент схожести
        similarity_percent: Процент схожести

    Returns:
        HTML карточки или None если не найден в кэше
    """
    try:
        # Передаём game объект для проверки актуальности данных
        card_cache = GameCardCache.get_card_for_game(game.id, game=game)

        if card_cache:
            # Инкрементируем счетчик использования
            card_cache.increment_hit()
            return card_cache.rendered_card

    except Exception as e:
        logger.debug(f"Cache miss for game {game.id}: {str(e)}")

    return None


def _render_and_cache_card(game: Game, context: Dict, show_similarity: bool = False,
                           similarity_percent: float = None) -> str:
    """
    Рендерит карточку и немедленно сохраняет в кэш модели.
    Используется когда карточка нужна прямо сейчас и должна быть сохранена.

    Args:
        game: Объект игры
        context: Контекст для рендеринга
        show_similarity: Показывать ли процент схожести
        similarity_percent: Процент схожести

    Returns:
        HTML карточки
    """
    from games.utils.game_card_utils import GameCardCreator

    # Рендерим карточку
    rendered_card = render_to_string('games/partials/_game_card.html', context)

    try:
        # Извлекаем связанные данные
        related_data = GameCardCreator._extract_related_data(game)

        # Создаем или обновляем карточку в БД
        card_cache, created = GameCardCache.get_or_create_card(
            game=game,
            rendered_card=rendered_card,
            show_similarity=show_similarity,
            similarity_percent=similarity_percent,
            card_size='normal',
            **related_data
        )

        logger.info(f"{'Created' if created else 'Updated'} card cache for game {game.id} ({game.name})")
        return card_cache.rendered_card

    except Exception as e:
        logger.error(f"Failed to cache card for game {game.id}: {str(e)}", exc_info=True)
        # В случае ошибки возвращаем свежеотрендеренный HTML без сохранения
        return rendered_card


def _render_game_card_with_caching(game: Game, context: Dict) -> str:
    """
    Рендерит карточку игры с использованием кэша модели.
    """
    show_similarity = context.get('show_similarity', False)
    similarity_percent = None

    if show_similarity and hasattr(game, 'similarity'):
        similarity_percent = game.similarity

    # Пытаемся получить из кэша
    cached_html = _get_cached_card_html(game, show_similarity, similarity_percent)

    if cached_html:
        return cached_html

    # Если нет в кэше - рендерим и кэшируем
    return _render_and_cache_card(game, context, show_similarity, similarity_percent)


def _update_games_with_cached_cards(games_list: List, context: Dict) -> List:
    """
    Обновляет список игр объектами с кэшированными карточками из БД.
    ОПТИМИЗИРОВАНО: пакетная загрузка карточек одним запросом.
    """

    show_similarity = context.get('show_similarity', False)
    current_page = context.get('current_page', 1)

    if not games_list:
        return games_list

    game_ids = []
    game_items = []

    for idx, item in enumerate(games_list):
        if isinstance(item, dict) and 'game' in item:
            game_obj = item['game']
            similarity = item.get('similarity')
            game_ids.append(game_obj.id)
            game_items.append((item, game_obj, similarity, True, idx))
        else:
            game_obj = item
            similarity = getattr(game_obj, 'similarity', None)
            game_ids.append(game_obj.id)
            game_items.append((item, game_obj, similarity, False, idx))

    from games.models import GameCardCache
    current_cache_version = GameCardCache.CARD_CACHE_VERSION

    cards_in_db = {}
    try:
        all_cards = GameCardCache.objects.filter(
            game_id__in=game_ids,
            is_active=True
        ).select_related('game')

        for card in all_cards:
            cards_in_db[card.game_id] = card
    except Exception as e:
        logger.error(f"Error batch loading card caches: {str(e)}")
        cards_in_db = {}

    processed_items = []

    for item, game_obj, similarity, is_similar_mode, idx in game_items:
        existing_card = cards_in_db.get(game_obj.id)

        if existing_card and existing_card.template_version == current_cache_version:
            try:
                existing_card.increment_hit()
                card_html = existing_card.rendered_card

                if show_similarity and similarity is not None and similarity > 0:
                    import re

                    pattern = r'(<div[^>]*class="[^"]*game-card-container[^"]*"[^>]*)>'
                    replacement = r'\1 data-similarity="' + str(similarity) + r'">'
                    card_html = re.sub(pattern, replacement, card_html, count=1)

                    source_game = context.get('source_game')
                    if source_game and hasattr(source_game, 'id'):
                        pattern = r'(<div[^>]*class="[^"]*game-card-container[^"]*"[^>]*)>'
                        replacement = r'\1 data-source-game-id="' + str(source_game.id) + r'">'
                        card_html = re.sub(pattern, replacement, card_html, count=1)

                if show_similarity and similarity is not None:
                    game_obj.similarity = similarity

                if isinstance(item, dict):
                    item['cached_card'] = card_html
                else:
                    item.cached_card = card_html

                processed_items.append(item)
                continue
            except Exception as e:
                logger.error(f"Error using cached card: {str(e)}")

        items_per_page = context.get('items_per_page', 16)
        game_index_offset = (current_page - 1) * items_per_page + idx

        card_context = {
            'game': game_obj,
            'current_page': current_page,
            'game_index': idx,
            'game_index_offset': game_index_offset,
            'forloop': {'counter0': idx, 'counter': idx + 1},
            'show_similarity': show_similarity,
        }

        if show_similarity:
            card_context['source_game'] = context.get('source_game')
            if similarity is not None:
                card_context['similarity_percent'] = similarity

        card_html = render_to_string('games/partials/_game_card.html', card_context)

        if show_similarity and similarity is not None and similarity > 0:
            import re

            pattern = r'(<div[^>]*class="[^"]*game-card-container[^"]*"[^>]*)>'
            replacement = r'\1 data-similarity="' + str(similarity) + r'">'
            card_html = re.sub(pattern, replacement, card_html, count=1)

            source_game = context.get('source_game')
            if source_game and hasattr(source_game, 'id'):
                pattern = r'(<div[^>]*class="[^"]*game-card-container[^"]*"[^>]*)>'
                replacement = r'\1 data-source-game-id="' + str(source_game.id) + r'">'
                card_html = re.sub(pattern, replacement, card_html, count=1)

        if show_similarity and similarity is not None:
            game_obj.similarity = similarity

        if isinstance(item, dict):
            item['cached_card'] = card_html
        else:
            item.cached_card = card_html
        processed_items.append(item)

    return processed_items


def _paginate_games(games_list, page_number, items_per_page=ITEMS_PER_PAGE):
    """Создает пагинатор для списка игр."""
    paginator = Paginator(games_list, items_per_page)

    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    return page_obj


def _get_all_games_mode_with_pagination(
        selected_criteria: Dict[str, List[int]],
        sort_field: str,
        page_num: int
) -> Dict[str, Any]:
    """Режим отображения ВСЕХ игр с СЕРВЕРНОЙ пагинацией - ОПТИМИЗИРОВАН."""
    # УБИРАЕМ prefetch - они не нужны, карточки берутся из кэша
    games_qs = Game.objects.all().only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url', 'game_type'
    )

    if any(selected_criteria.values()):
        games_qs = _apply_filters(games_qs, selected_criteria)

    if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
        games_qs = games_qs.order_by(sort_field)
    else:
        games_qs = games_qs.order_by('-rating_count')

    # Используем пагинатор Django
    paginator = Paginator(games_qs, ITEMS_PER_PAGE)

    try:
        page_obj = paginator.page(page_num)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    total_count = paginator.count

    return {
        'page_obj': page_obj,
        'paginator': paginator,
        'is_paginated': paginator.num_pages > 1,
        'total_count': total_count,
        'current_page': page_obj.number,
        'show_similarity': False,
        'find_similar': False,
        'source_game': None,
    }


def _get_similar_games_mode_with_pagination(
        params: Dict[str, str],
        selected_criteria: Dict[str, List[int]],
        source_game_obj: Optional[Game],
        page_num: int,
        search_genres_list: List[int] = None,
        search_keywords_list: List[int] = None,
        search_themes_list: List[int] = None,
        search_perspectives_list: List[int] = None,
        search_game_modes_list: List[int] = None,
        search_engines_list: List[int] = None,
        search_platforms_list: List[int] = None,
        search_game_types_list: List[int] = None,
        search_year_start_int: int = None,
        search_year_end_int: int = None,
) -> Dict[str, Any]:
    """
    Режим похожих игр с СЕРВЕРНОЙ пагинацией.
    """
    timers = {
        'source_game_creation': 0,
        'formatting': 0,
        'search_filters': 0,
        'sorting': 0,
        'pagination': 0,
        'total': 0
    }
    total_start = time.time()

    current_sort = params.get('sort', '-similarity')

    # Собираем поисковые фильтры в словарь
    search_filters = {}

    if search_platforms_list:
        search_filters['platforms'] = search_platforms_list
        print(f"DEBUG: search_filters platforms = {search_platforms_list}")

    if search_game_modes_list:
        search_filters['game_modes'] = search_game_modes_list
        print(f"DEBUG: search_filters game_modes = {search_game_modes_list}")

    if search_genres_list:
        search_filters['genres'] = search_genres_list
        print(f"DEBUG: search_filters genres = {search_genres_list}")

    if search_keywords_list:
        search_filters['keywords'] = search_keywords_list
        print(f"DEBUG: search_filters keywords = {search_keywords_list}")

    if search_themes_list:
        search_filters['themes'] = search_themes_list
        print(f"DEBUG: search_filters themes = {search_themes_list}")

    if search_perspectives_list:
        search_filters['perspectives'] = search_perspectives_list
        print(f"DEBUG: search_filters perspectives = {search_perspectives_list}")

    if search_game_types_list:
        search_filters['game_types'] = search_game_types_list
        print(f"DEBUG: search_filters game_types = {search_game_types_list}")

    if search_engines_list:
        search_filters['engines'] = search_engines_list
        print(f"DEBUG: search_filters engines = {search_engines_list}")

    if search_year_start_int:
        search_filters['release_year_start'] = search_year_start_int
        print(f"DEBUG: search_filters release_year_start = {search_year_start_int}")

    if search_year_end_int:
        search_filters['release_year_end'] = search_year_end_int
        print(f"DEBUG: search_filters release_year_end = {search_year_end_int}")

    print(f"DEBUG: Final search_filters = {search_filters}")

    stage_start = time.time()
    if source_game_obj:
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, [], search_filters  # Передаем поисковые фильтры
        )
        print(f"get_similar_games_for_game took: {time.time() - stage_start:.3f}s")
        print(f"Found {total_count} games with search_filters")

        source_display = source_game_obj.name

        game_criteria = {
            'genres': [g.id for g in source_game_obj.genres.all()] if hasattr(source_game_obj, 'genres') else [],
            'keywords': [k.id for k in source_game_obj.keywords.all()] if hasattr(source_game_obj, 'keywords') else [],
            'themes': [t.id for t in source_game_obj.themes.all()] if hasattr(source_game_obj, 'themes') else [],
            'perspectives': [p.id for p in source_game_obj.player_perspectives.all()] if hasattr(source_game_obj,
                                                                                                 'player_perspectives') else [],
            'game_modes': [gm.id for gm in source_game_obj.game_modes.all()] if hasattr(source_game_obj,
                                                                                        'game_modes') else [],
            'engines': [e.id for e in source_game_obj.engines.all()] if hasattr(source_game_obj, 'engines') else [],
        }

        source_game = SimpleSourceGame(
            game_obj=source_game_obj,
            criteria=game_criteria,
            display_name=source_display
        )
    else:
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria, search_filters)
        print(f"get_similar_games_for_criteria took: {time.time() - stage_start:.3f}s")
        source_display = "Search Criteria"

        source_game = SimpleSourceGame(
            game_obj=None,
            criteria=selected_criteria,
            display_name=source_display
        )
    timers['source_game_creation'] = round(time.time() - stage_start, 3)

    stage_start = time.time()
    games_with_similarity = _format_similar_games_data(similar_games_data)

    for item in games_with_similarity:
        if 'game' in item and item.get('similarity') is not None:
            item['game'].similarity = item['similarity']
    timers['formatting'] = round(time.time() - stage_start, 3)

    timers['search_filters'] = 0

    stage_start = time.time()
    _sort_similar_games(games_with_similarity, current_sort)
    timers['sorting'] = round(time.time() - stage_start, 3)

    stage_start = time.time()
    paginator = Paginator(games_with_similarity, ITEMS_PER_PAGE)

    try:
        page_obj = paginator.page(page_num)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    current_games_with_similarity = list(page_obj.object_list)
    timers['pagination'] = round(time.time() - stage_start, 3)

    timers['total'] = round(time.time() - total_start, 3)

    print("\n=== TIMERS: _get_similar_games_mode_with_pagination ===")
    print(f"Source game creation: {timers['source_game_creation']}s")
    print(f"Formatting data: {timers['formatting']}s")
    print(f"Sorting: {timers['sorting']}s")
    print(f"Pagination: {timers['pagination']}s")
    print(f"TOTAL: {timers['total']}s")
    print("======================================================\n")

    return {
        'page_obj': page_obj,
        'paginator': paginator,
        'is_paginated': paginator.num_pages > 1,
        'total_count': total_count,
        'current_page': page_obj.number,
        'games_with_similarity': current_games_with_similarity,
        'show_similarity': True,
        'find_similar': True,
        'source_game': source_game,
        'source_game_obj': source_game_obj,
        'timers': timers,
    }


def ajax_load_games_page(request: HttpRequest) -> HttpResponse:
    """Load games for specific page via AJAX with card caching and text search support."""
    start_time = time.time()
    timers = {
        'params_extraction': 0,
        'mode_determination': 0,
        'data_loading': 0,
        'card_caching': 0,
        'template_rendering': 0,
        'total': 0
    }
    total_start = time.time()

    print("\n=== AJAX LOAD GAMES PAGE ===")
    print(f"GET params: {dict(request.GET)}")

    stage_start = time.time()
    page_num = request.GET.get('page', '1')
    try:
        page_num = int(page_num)
    except (ValueError, TypeError):
        page_num = 1

    params = extract_request_params(request)
    selected_criteria = convert_params_to_lists(params)

    # Extract search parameters from GET
    search_genres = request.GET.get('search_g', '')
    search_keywords = request.GET.get('search_k', '')
    search_themes = request.GET.get('search_t', '')
    search_perspectives = request.GET.get('search_pp', '')
    search_game_modes = request.GET.get('search_gm', '')
    search_engines = request.GET.get('search_e', '')
    search_platforms = request.GET.get('search_p', '')
    search_game_types = request.GET.get('search_gt', '')
    search_year_start = request.GET.get('search_ys', '')
    search_year_end = request.GET.get('search_ye', '')

    # Text search query from separate parameter 'q' (not from search_k)
    search_text_query = request.GET.get('q', '').strip()

    search_genres_list = [int(x) for x in search_genres.split(',') if x.isdigit()] if search_genres else []
    search_keywords_list = [int(x) for x in search_keywords.split(',') if x.isdigit()] if search_keywords else []
    search_themes_list = [int(x) for x in search_themes.split(',') if x.isdigit()] if search_themes else []
    search_perspectives_list = [int(x) for x in search_perspectives.split(',') if
                                x.isdigit()] if search_perspectives else []
    search_game_modes_list = [int(x) for x in search_game_modes.split(',') if x.isdigit()] if search_game_modes else []
    search_engines_list = [int(x) for x in search_engines.split(',') if x.isdigit()] if search_engines else []
    search_platforms_list = [int(x) for x in search_platforms.split(',') if x.isdigit()] if search_platforms else []
    search_game_types_list = [int(x) for x in search_game_types.split(',') if x.isdigit()] if search_game_types else []

    try:
        search_year_start_int = int(search_year_start) if search_year_start else None
    except ValueError:
        search_year_start_int = None

    try:
        search_year_end_int = int(search_year_end) if search_year_end else None
    except ValueError:
        search_year_end_int = None

    print(f"Search text query: '{search_text_query}'")
    print(f"Search genres: {search_genres_list}")
    print(f"Search platforms: {search_platforms_list}")
    print(f"Search keywords: {search_keywords_list}")

    sort_field = params.get('sort', '-rating_count')
    find_similar = params.get('find_similar') == '1'

    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = Game.objects.only('id', 'name').get(pk=int(params['source_game']))
            print(f"Source game: {source_game_obj.id} - {source_game_obj.name}")
        except (Game.DoesNotExist, ValueError):
            pass
    timers['params_extraction'] = round(time.time() - stage_start, 3)

    stage_start = time.time()

    # Check if there are any search parameters
    has_search_params = any([
        search_genres_list, search_keywords_list, search_themes_list,
        search_perspectives_list, search_game_modes_list, search_engines_list,
        search_platforms_list, search_game_types_list,
        search_year_start_int, search_year_end_int, search_text_query
    ])

    # CRITICAL: When in similar mode, we should use similar mode with search filters,
    # NOT fall back to regular mode
    use_regular_mode = not find_similar

    if use_regular_mode:
        print("Mode: regular games with search filters")

        # Build search_filters dictionary for _apply_search_filters
        search_filters = {}
        if search_platforms_list:
            search_filters['platforms'] = search_platforms_list
        if search_game_types_list:
            search_filters['game_types'] = search_game_types_list
        if search_genres_list:
            search_filters['genres'] = search_genres_list
        if search_keywords_list:
            search_filters['keywords'] = search_keywords_list
        if search_themes_list:
            search_filters['themes'] = search_themes_list
        if search_perspectives_list:
            search_filters['perspectives'] = search_perspectives_list
        if search_game_modes_list:
            search_filters['game_modes'] = search_game_modes_list
        if search_engines_list:
            search_filters['engines'] = search_engines_list
        if search_year_start_int:
            search_filters['release_year_start'] = search_year_start_int
        if search_year_end_int:
            search_filters['release_year_end'] = search_year_end_int
        if search_text_query:
            search_filters['text_query'] = search_text_query

        # Get queryset with search filters applied
        games_qs = Game.objects.all().only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        )

        if search_filters:
            from .base_views import _apply_search_filters
            games_qs = _apply_search_filters(games_qs, search_filters)

        if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
            games_qs = games_qs.order_by(sort_field)
        else:
            games_qs = games_qs.order_by('-rating_count')

        paginator = Paginator(games_qs, ITEMS_PER_PAGE)

        try:
            page_obj = paginator.page(page_num)
        except PageNotAnInteger:
            page_obj = paginator.page(1)
        except EmptyPage:
            page_obj = paginator.page(paginator.num_pages)

        total_count = paginator.count
        total_pages = paginator.num_pages

        games = list(page_obj.object_list)
        games_with_similarity = []
        show_similarity = False
        source_game = None

    else:
        print("Mode: similar games with search filters")
        # Pass search filters to the similar games function
        mode_result = _get_similar_games_mode_with_pagination(
            params,
            selected_criteria,
            source_game_obj,
            page_num,
            search_genres_list=search_genres_list,
            search_keywords_list=search_keywords_list,
            search_themes_list=search_themes_list,
            search_perspectives_list=search_perspectives_list,
            search_game_modes_list=search_game_modes_list,
            search_engines_list=search_engines_list,
            search_platforms_list=search_platforms_list,
            search_game_types_list=search_game_types_list,
            search_year_start_int=search_year_start_int,
            search_year_end_int=search_year_end_int,
        )

        games_with_similarity = mode_result.get('games_with_similarity', [])
        games = []
        source_game = mode_result.get('source_game')
        paginator = mode_result.get('paginator')
        total_pages = paginator.num_pages if paginator else 1
        total_count = mode_result.get('total_count', 0)
        show_similarity = True

    timers['mode_determination'] = round(time.time() - stage_start, 3)

    stage_start = time.time()

    if use_regular_mode:
        games = _update_games_with_cached_cards(
            games,
            {
                'show_similarity': False,
                'current_page': page_num,
            }
        )
        template_context = {
            'games': games,
            'current_page': page_num,
            'page_obj': page_obj,
            'paginator': paginator,
            'is_paginated': paginator.num_pages > 1,
            'total_count': total_count,
            'total_pages': total_pages,
            'start_index': (page_num - 1) * ITEMS_PER_PAGE + 1,
            'end_index': min(page_num * ITEMS_PER_PAGE, total_count),
            'items_per_page': ITEMS_PER_PAGE,
            'current_sort': sort_field,
            'request': request,
            'show_similarity': False,
        }
    else:
        games_with_similarity = _update_games_with_cached_cards(
            games_with_similarity,
            {
                'show_similarity': True,
                'source_game': source_game,
                'current_page': page_num,
            }
        )
        template_context = {
            'games': games_with_similarity,
            'show_similarity': True,
            'source_game': source_game,
            'source_game_obj': source_game_obj,
            'current_page': page_num,
            'page_obj': mode_result.get('page_obj'),
            'paginator': paginator,
            'is_paginated': mode_result.get('is_paginated', False),
            'total_count': total_count,
            'total_pages': total_pages,
            'start_index': (page_num - 1) * ITEMS_PER_PAGE + 1,
            'end_index': min(page_num * ITEMS_PER_PAGE, total_count),
            'items_per_page': ITEMS_PER_PAGE,
            'current_sort': sort_field,
            'request': request,
        }

    timers['card_caching'] = round(time.time() - stage_start, 3)

    template_context['debug_total_pages'] = template_context['total_pages']
    template_context['debug_current_page'] = page_num
    template_context['debug_params'] = dict(request.GET)

    stage_start = time.time()
    html = render_to_string('games/game_list/_games_results.html', template_context)
    timers['template_rendering'] = round(time.time() - stage_start, 3)

    timers['total'] = round(time.time() - total_start, 3)

    print("\n=== TIMERS: ajax_load_games_page ===")
    print(f"Params extraction: {timers['params_extraction']}s")
    print(f"Mode determination: {timers['mode_determination']}s")
    print(f"Card caching: {timers['card_caching']}s")
    print(f"Template rendering: {timers['template_rendering']}s")
    print(f"TOTAL AJAX: {timers['total']}s")
    print("==================================\n")

    response = HttpResponse(html)
    response['Content-Type'] = 'text/html; charset=utf-8'
    response['X-AJAX-Page'] = str(page_num)
    response['X-Total-Pages'] = str(template_context['total_pages'])
    response['X-Response-Time'] = f"{time.time() - start_time:.3f}s"

    print(f"Response prepared in {time.time() - start_time:.3f}s")
    print("=== END AJAX LOAD ===\n")

    return response


def get_similar_games_for_criteria(selected_criteria: Dict[str, List[int]], search_filters: Dict = None) -> Tuple[
    List, int]:
    """Get similar games for criteria - с поддержкой поиска без жанров."""
    import json
    import hashlib
    from ..similarity import GameSimilarity

    start_total = time.time()
    print(f"\n=== get_similar_games_for_criteria START ===")
    print(
        f"Criteria: genres={len(selected_criteria['genres'])}, keywords={len(selected_criteria['keywords'])}, themes={len(selected_criteria['themes'])}, engines={len(selected_criteria.get('engines', []))}")

    stage_start = time.time()

    # Получаем текущую версию алгоритма
    similarity_engine = GameSimilarity()
    algorithm_version = similarity_engine.ALGORITHM_VERSION

    cache_data = json.dumps({
        'g': selected_criteria['genres'],
        'k': selected_criteria['keywords'],
        't': selected_criteria['themes'],
        'pp': selected_criteria['perspectives'],
        'd': selected_criteria['developers'],
        'gm': selected_criteria['game_modes'],
        'e': selected_criteria['engines'],
        'search_filters': search_filters,
        'algorithm_version': algorithm_version,
    }, sort_keys=True)

    cache_key = f'virtual_search_full_{hashlib.md5(cache_data.encode()).hexdigest()}'
    cached_data = cache.get(cache_key)
    print(f"Cache check: {time.time() - stage_start:.3f}s")

    if cached_data:
        logger.debug(f"Cache HIT for criteria search: {len(cached_data['games'])} games")
        print(f"Cache HIT - returning {len(cached_data['games'])} games")
        print(f"Total time: {time.time() - start_total:.3f}s")
        print("=== get_similar_games_for_criteria END (CACHE HIT) ===\n")
        return cached_data['games'], cached_data['count']

    print(f"Cache MISS - calculating similarity...")

    stage_start = time.time()
    virtual_game = VirtualGame(
        genre_ids=selected_criteria['genres'],
        keyword_ids=selected_criteria['keywords'],
        theme_ids=selected_criteria['themes'],
        perspective_ids=selected_criteria['perspectives'],
        developer_ids=selected_criteria['developers'],
        game_mode_ids=selected_criteria['game_modes'],
        engine_ids=selected_criteria['engines'],
    )
    print(f"VirtualGame created: {time.time() - stage_start:.3f}s")

    stage_start = time.time()
    similar_games = similarity_engine.find_similar_games(
        source_game=virtual_game,
        search_filters=search_filters
    )
    print(f"find_similar_games executed: {time.time() - stage_start:.3f}s, found {len(similar_games)} games")

    total_count = len(similar_games)

    stage_start = time.time()
    cache_time = 10800
    if selected_criteria['genres']:
        cache_time = 7200

    cache.set(cache_key, {
        'games': similar_games,
        'count': total_count,
        'timestamp': time.time()
    }, cache_time)
    print(f"Cache save: {time.time() - stage_start:.3f}s")

    criteria_count = sum(len(v) for key, v in selected_criteria.items()
                         if key not in ['release_years', 'release_year_start', 'release_year_end'])

    logger.info(f"Similar games search took: {time.time() - start_total:.2f}s, "
                f"criteria: {criteria_count}, "
                f"results: {total_count}")

    print(f"TOTAL time for get_similar_games_for_criteria: {time.time() - start_total:.3f}s")
    print("=== get_similar_games_for_criteria END ===\n")

    return similar_games, total_count


def get_similar_games_for_game(game_obj: Game, selected_platforms: List[int], search_filters: Dict = None) -> Tuple[
    List, int]:
    """
    Get similar games for a specific game with database caching.
    Использует модель GameSimilarityCache для хранения результатов в БД.
    """
    from ..models_parts.similarity import GameSimilarityCache
    from .base_views import _generate_cache_key, CACHE_TIMES
    from ..similarity import GameSimilarity
    import hashlib
    import json

    start_total = time.time()
    print(f"\n=== get_similar_games_for_game START for game {game_obj.id} - {game_obj.name} ===")

    stage_start = time.time()

    # Получаем текущую версию алгоритма
    similarity_engine = GameSimilarity()
    algorithm_version = similarity_engine.ALGORITHM_VERSION

    # Создаем ключ для поиска в БД
    cache_key_data = {
        'game_id': game_obj.id,
        'platforms': sorted(selected_platforms) if selected_platforms else [],
        'search_filters': search_filters,
        'algorithm_version': algorithm_version,
    }

    cache_key = hashlib.md5(json.dumps(cache_key_data, sort_keys=True).encode()).hexdigest()

    # Пытаемся найти в базе данных
    try:
        cached_entry = GameSimilarityCache.objects.filter(
            game1_id=game_obj.id,
            # Используем поле similarity_score как хранилище для версии и данных
        ).select_related('game2').first()

        # Проверяем актуальность кэша (по дате или версии)
        if cached_entry:
            # Если кэш свежий (менее 7 дней), используем его
            from django.utils import timezone
            from datetime import timedelta

            if cached_entry.calculated_at > timezone.now() - timedelta(days=7):
                # Получаем все похожие игры для этой исходной игры
                all_similar = list(GameSimilarityCache.objects.filter(
                    game1_id=game_obj.id
                ).select_related('game2').order_by('-similarity_score'))

                similar_games = []
                for item in all_similar:
                    similar_games.append({
                        'game': item.game2,
                        'similarity': item.similarity_score,
                        'common_keywords': getattr(item, 'common_keywords', 0),
                        'common_genres': getattr(item, 'common_genres', 0),
                        'common_themes': getattr(item, 'common_themes', 0),
                    })

                total_count = len(similar_games)
                print(f"Cache HIT for game {game_obj.id} - found {total_count} games")
                print(f"Total time: {time.time() - start_total:.3f}s")
                return similar_games, total_count
    except Exception as e:
        print(f"Cache read error: {e}")

    print(f"Cache MISS for game {game_obj.id} - calculating similarity...")

    stage_start = time.time()
    similar_games = similarity_engine.find_similar_games(
        source_game=game_obj,
        min_similarity=0,
        search_filters=search_filters
    )
    print(f"find_similar_games executed: {time.time() - stage_start:.3f}s, found {len(similar_games)} games")

    total_count = len(similar_games)

    # Сохраняем результаты в базу данных
    stage_start = time.time()
    try:
        # Удаляем старые записи для этой игры
        GameSimilarityCache.objects.filter(game1_id=game_obj.id).delete()

        # Создаем новые записи
        cache_entries = []
        for item in similar_games:
            if isinstance(item, dict):
                target_game = item.get('game')
                similarity = item.get('similarity', 0)
            else:
                target_game = item
                similarity = getattr(item, 'similarity', 0)

            if target_game and hasattr(target_game, 'id'):
                cache_entries.append(GameSimilarityCache(
                    game1_id=game_obj.id,
                    game2_id=target_game.id,
                    similarity_score=similarity,
                ))

        # Массовое создание записей
        if cache_entries:
            GameSimilarityCache.objects.bulk_create(cache_entries, batch_size=500)
            print(f"Saved {len(cache_entries)} similarity records to database")
    except Exception as e:
        print(f"Failed to save to database: {e}")

    print(f"TOTAL time for get_similar_games_for_game: {time.time() - start_total:.3f}s")
    print("=== get_similar_games_for_game END ===\n")

    return similar_games, total_count


def get_source_game(source_game_id: Optional[str]) -> Optional[Game]:
    """Get source game object with optimized query."""
    if not source_game_id:
        return None

    try:
        return Game.objects.only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        ).get(pk=int(source_game_id))
    except (Game.DoesNotExist, ValueError):
        return None


def clear_game_list_cache_specific(params_pattern=None):
    """
    Очищает кэш страниц игр.
    params_pattern: словарь с параметрами для выборочной очистки
    """
    from django.core.cache import cache

    # Для django.core.cache без поддержки паттернов
    # Самый простой способ: изменить версию кэша
    global CACHE_VERSION
    old_version = CACHE_VERSION
    CACHE_VERSION = f"v{int(CACHE_VERSION[1:]) + 1}"
    return f"Cache version updated from {old_version} to {CACHE_VERSION}"


def _build_context_from_cached_data(cached_data: Dict, params: Dict, requested_page_num: int) -> Dict:
    """Build context from cached data with page number support."""
    filter_data = cached_data['filter_data']
    years_range = cached_data['years_range']
    mode_result = cached_data['mode_result']

    current_page = requested_page_num
    total_count = mode_result.get('total_count', 0)
    total_pages = (total_count + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE if total_count > 0 else 1

    if current_page > total_pages:
        current_page = total_pages
    if current_page < 1:
        current_page = 1

    start_index = (current_page - 1) * ITEMS_PER_PAGE + 1
    end_index = min(current_page * ITEMS_PER_PAGE, total_count)

    selected_criteria = convert_params_to_lists(params)

    context = {
        'games': mode_result.get('games', []),
        'games_with_similarity': mode_result.get('games_with_similarity', []),
        'page_obj': mode_result.get('page_obj'),
        'is_paginated': mode_result.get('is_paginated', False),
        'total_count': total_count,
        'total_pages': total_pages,
        'current_page': current_page,
        'start_index': start_index,
        'end_index': end_index,

        'find_similar': mode_result.get('find_similar', False),
        'show_similarity': mode_result.get('show_similarity', False),
        'source_game': mode_result.get('source_game'),

        'similarity_map': mode_result.get('similarity_map', {}),

        'genres': _get_cached_genres_list(),
        'themes': filter_data['themes'],
        'perspectives': filter_data['perspectives'],
        'game_modes': filter_data['game_modes'],
        'keywords': filter_data['keywords'],
        'platforms': filter_data['platforms'],
        'popular_keywords': filter_data['popular_keywords'],
        'game_types': GameTypeEnum.CHOICES,

        'years_range': years_range,
        'current_year': timezone.now().year,

        'selected_criteria_objects': cached_data.get('selected_criteria_objects', {}),
        'current_sort': params.get('sort', ''),

        'debug_info': {
            'mode': cached_data['mode'],
            'from_cache': True,
            'cache_timestamp': cached_data.get('timestamp', 0),
            'requested_page': requested_page_num
        }
    }

    context.update({
        'selected_genres': selected_criteria['genres'],
        'selected_keywords': selected_criteria['keywords'],
        'selected_platforms': selected_criteria['platforms'],
        'selected_themes': selected_criteria['themes'],
        'selected_perspectives': selected_criteria['perspectives'],
        'selected_game_modes': selected_criteria['game_modes'],
        'selected_game_types': selected_criteria['game_types'],
        'selected_release_year_start': selected_criteria['release_year_start'],
        'selected_release_year_end': selected_criteria['release_year_end'],
        'selected_developers': selected_criteria['developers'],

        'selected_genres_objects': cached_data.get('selected_criteria_objects', {}).get('genres', []),
        'selected_keywords_objects': cached_data.get('selected_criteria_objects', {}).get('keywords', []),
        'selected_platforms_objects': cached_data.get('selected_criteria_objects', {}).get('platforms', []),
        'selected_themes_objects': cached_data.get('selected_criteria_objects', {}).get('themes', []),
        'selected_perspectives_objects': cached_data.get('selected_criteria_objects', {}).get('perspectives', []),
        'selected_game_modes_objects': cached_data.get('selected_criteria_objects', {}).get('game_modes', []),
        'selected_developers_objects': cached_data.get('selected_criteria_objects', {}).get('developers', []),
    })

    return context


def _handle_ajax_game_page(request: HttpRequest) -> HttpResponse:
    """Handle AJAX requests for game pages - optimized version."""
    start_time = time.time()

    params = extract_request_params(request)
    requested_page = params.get('page', '1')

    try:
        requested_page_num = int(requested_page)
    except (ValueError, TypeError):
        requested_page_num = 1

    selected_criteria = convert_params_to_lists(params)

    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = _get_cached_game(params['source_game'])
        except (Game.DoesNotExist, ValueError):
            pass

    should_use_similar_mode = _should_use_similar_mode(
        find_similar,
        source_game_obj,
        selected_criteria
    )

    if should_use_similar_mode:
        mode_result = _get_similar_games_mode_with_pagination(
            params, selected_criteria, source_game_obj, requested_page_num
        )
        games = mode_result.get('games_with_similarity', [])
        template_context = {
            'games': games,
            'show_similarity': True,
            'source_game': mode_result.get('source_game'),
            'current_page': requested_page_num,
            'similarity_map': mode_result.get('similarity_map', {}),
        }
    else:
        mode_result = _get_all_games_mode_with_pagination(
            selected_criteria, params.get('sort', '-rating_count'), requested_page_num
        )
        games = list(mode_result.get('page_obj', {}).object_list) if mode_result.get('page_obj') else []
        template_context = {
            'games': games,
            'current_page': requested_page_num,
        }

    html = render_to_string('games/game_list/_games_grid.html', template_context)

    response = HttpResponse(html)
    response['Content-Type'] = 'text/html; charset=utf-8'
    response['X-AJAX'] = 'true'
    response['X-Page'] = str(requested_page_num)
    response['X-Total-Games'] = str(len(games))
    response['X-Response-Time'] = f"{time.time() - start_time:.3f}s"

    return response


def _build_optimized_context(
        mode_result: Dict[str, Any],
        mode: str,
        filter_data: Dict[str, List],
        selected_criteria: Dict[str, List[int]],
        selected_criteria_objects: Dict[str, List],
        params: Dict[str, str],
        source_game_obj: Optional[Game],
        find_similar: bool,
        years_range: Dict,
        execution_time: float,
        requested_page_num: int,
        items_per_page: int = ITEMS_PER_PAGE,
        offset: int = 0
) -> Dict[str, Any]:
    """
    Build optimized template context with minimal overhead.
    """
    genres_list = _get_cached_genres_list()

    current_page = requested_page_num
    total_count = mode_result.get('total_count', 0)
    total_pages = mode_result.get('paginator', {}).num_pages if mode_result.get('paginator') else 1

    if mode == 'similar':
        games_with_similarity = mode_result.get('games_with_similarity', [])
        games = []
        show_similarity = True
        source_game = mode_result.get('source_game')
    else:
        games = list(mode_result.get('page_obj', {}).object_list) if mode_result.get('page_obj') else []
        games_with_similarity = []
        show_similarity = False
        source_game = None

    start_index = (current_page - 1) * items_per_page + 1
    end_index = min(current_page * items_per_page, total_count)

    context = {
        'games': games,
        'games_with_similarity': games_with_similarity,
        'page_obj': mode_result.get('page_obj'),
        'is_paginated': mode_result.get('is_paginated', False),
        'total_count': total_count,
        'total_pages': total_pages,
        'current_page': current_page,
        'start_index': start_index,
        'end_index': end_index,
        'items_per_page': items_per_page,

        'find_similar': find_similar,
        'show_similarity': show_similarity,
        'source_game': source_game,
        'source_game_obj': source_game_obj,

        'genres': genres_list,
        'themes': filter_data['themes'],
        'perspectives': filter_data['perspectives'],
        'game_modes': filter_data['game_modes'],
        'keywords': filter_data['keywords'],
        'platforms': filter_data['platforms'],
        'popular_keywords': filter_data['popular_keywords'],
        'game_types': GameTypeEnum.CHOICES,

        'years_range': years_range,
        'current_year': timezone.now().year,

        'selected_genres': selected_criteria['genres'],
        'selected_keywords': selected_criteria['keywords'],
        'selected_platforms': selected_criteria['platforms'],
        'selected_themes': selected_criteria['themes'],
        'selected_perspectives': selected_criteria['perspectives'],
        'selected_game_modes': selected_criteria['game_modes'],
        'selected_game_types': selected_criteria['game_types'],
        'selected_release_year_start': selected_criteria['release_year_start'],
        'selected_release_year_end': selected_criteria['release_year_end'],
        'selected_developers': selected_criteria['developers'],

        'selected_genres_objects': selected_criteria_objects.get('genres', []),
        'selected_keywords_objects': selected_criteria_objects.get('keywords', []),
        'selected_platforms_objects': selected_criteria_objects.get('platforms', []),
        'selected_themes_objects': selected_criteria_objects.get('themes', []),
        'selected_perspectives_objects': selected_criteria_objects.get('perspectives', []),
        'selected_game_modes_objects': selected_criteria_objects.get('game_modes', []),
        'selected_developers_objects': selected_criteria_objects.get('developers', []),

        'current_sort': params.get('sort', ''),

        'execution_time': round(execution_time, 3),

        'debug_info': {
            'mode': mode,
            'requested_page': current_page,
            'mode_result_page': mode_result.get('current_page', 1),
            'has_genres': bool(selected_criteria['genres']),
            'genre_count': len(selected_criteria['genres']),
            'has_keywords': bool(selected_criteria['keywords']),
            'keyword_count': len(selected_criteria['keywords']),
            'keywords_total': len(filter_data['keywords']),
            'popular_keywords_total': len(filter_data['popular_keywords']),
            'has_themes': bool(selected_criteria['themes']),
            'theme_count': len(selected_criteria['themes']),
            'has_perspectives': bool(selected_criteria['perspectives']),
            'perspective_count': len(selected_criteria['perspectives']),
            'has_game_modes': bool(selected_criteria['game_modes']),
            'game_mode_count': len(selected_criteria['game_modes']),
            'find_similar_param': find_similar,
            'has_source_game': bool(source_game_obj),
            'genres_total': len(genres_list),
            'themes_total': len(filter_data['themes']),
            'perspectives_total': len(filter_data['perspectives']),
            'game_modes_total': len(filter_data['game_modes']),
            'from_cache': False,
            'total_pages': total_pages,
            'games_count': len(games) if games else len(games_with_similarity),
        }
    }

    if selected_criteria['release_year_start'] or selected_criteria['release_year_end']:
        context['has_date_filter'] = True
    else:
        context['has_date_filter'] = False

    return context


def _should_use_similar_mode(
        find_similar_param: bool,
        source_game_obj: Optional[Game],
        selected_criteria: Dict[str, List[int]]
) -> bool:
    """
    Determine if similar games mode should be used.

    Args:
        find_similar_param: find_similar parameter from request
        source_game_obj: Source game object (if any)
        selected_criteria: Selected criteria dictionaries

    Returns:
        True if similar games mode should be used
    """
    if find_similar_param:
        logger.debug("Similar mode: explicitly requested (find_similar=1)")
        return True

    if source_game_obj:
        logger.debug(f"Similar mode: source game {source_game_obj.id} provided")
        return True

    has_similarity_criteria = _has_similarity_criteria(selected_criteria)

    if has_similarity_criteria:
        logger.debug("Similar mode: similarity criteria detected")
        return True

    logger.debug("Similar mode: no criteria detected, using regular mode")
    return False


def _has_similarity_criteria(selected_criteria: Dict[str, List[int]]) -> bool:
    """
    Check if there are criteria for similarity search.
    """
    similarity_criteria = [
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes'],
        selected_criteria['engines']  # ДОБАВЛЕНО
    ]

    return any(similarity_criteria)


def _get_selected_criteria_objects(selected_criteria: Dict[str, List[int]]) -> Dict[str, List]:
    """Получаем объекты для всех выбранных критериев."""
    selected_objects = {}

    if selected_criteria.get('genres'):
        selected_objects['genres'] = list(Genre.objects.filter(
            id__in=selected_criteria['genres']
        ).only('id', 'name'))

    if selected_criteria.get('keywords'):
        selected_objects['keywords'] = list(Keyword.objects.filter(
            id__in=selected_criteria['keywords']
        ).only('id', 'name'))

    if selected_criteria.get('platforms'):
        selected_objects['platforms'] = list(Platform.objects.filter(
            id__in=selected_criteria['platforms']
        ).only('id', 'name'))

    if selected_criteria.get('themes'):
        selected_objects['themes'] = list(Theme.objects.filter(
            id__in=selected_criteria['themes']
        ).only('id', 'name'))

    if selected_criteria.get('perspectives'):
        selected_objects['perspectives'] = list(PlayerPerspective.objects.filter(
            id__in=selected_criteria['perspectives']
        ).only('id', 'name'))

    if selected_criteria.get('game_modes'):
        selected_objects['game_modes'] = list(GameMode.objects.filter(
            id__in=selected_criteria['game_modes']
        ).only('id', 'name'))

    if selected_criteria.get('engines'):
        selected_objects['engines'] = list(GameEngine.objects.filter(
            id__in=selected_criteria['engines']
        ).only('id', 'name'))

    return selected_objects


def _get_all_games_mode(selected_criteria: Dict[str, List[int]], sort_field: str, page_number: str) -> Dict[str, Any]:
    """Режим отображения ВСЕХ игр с фильтрами для серверной пагинации."""
    games_qs = Game.objects.all().prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
        Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url', 'game_type'
    )

    if any(selected_criteria.values()):
        games_qs = _apply_filters(games_qs, selected_criteria)

    if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
        games_qs = games_qs.order_by(sort_field)
    else:
        games_qs = games_qs.order_by('-rating_count')

    # Используем пагинатор Django
    paginator = Paginator(games_qs, ITEMS_PER_PAGE)

    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    return {
        'page_obj': page_obj,
        'paginator': paginator,
        'is_paginated': paginator.num_pages > 1,
        'total_count': paginator.count,
        'show_similarity': False,
        'find_similar': False,
        'source_game': None,
    }


def _get_similar_games_mode(params: Dict[str, str], selected_criteria: Dict[str, List[int]],
                            source_game_obj: Optional[Game]) -> Dict[str, Any]:
    """Режим похожих игр с поддержкой поиска без жанров для серверной пагинации."""
    current_sort = params.get('sort', '-similarity')
    page_number = params.get('page', '1')

    logger.info(f"Режим похожих игр запущен. Критерии: "
                f"жанры={len(selected_criteria['genres'])}, "
                f"ключевые слова={len(selected_criteria['keywords'])}, "
                f"темы={len(selected_criteria['themes'])}, "
                f"перспективы={len(selected_criteria['perspectives'])}, "
                f"режимы игры={len(selected_criteria['game_modes'])}, "
                f"движки={len(selected_criteria['engines'])}")  # ИЗМЕНЕНО

    if source_game_obj:
        logger.info(f"Поиск похожих игр для игры: {source_game_obj.name} (ID: {source_game_obj.id})")
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, selected_criteria['platforms']
        )
        source_display = source_game_obj.name

        game_criteria = {}

        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'genres' in source_game_obj._prefetched_objects_cache:
            game_criteria['genres'] = [g.id for g in source_game_obj.genres.all()]
        elif hasattr(source_game_obj, 'genres') and hasattr(source_game_obj.genres, 'all'):
            game_criteria['genres'] = [g.id for g in source_game_obj.genres.all()]
        else:
            game_criteria['genres'] = []

        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'keywords' in source_game_obj._prefetched_objects_cache:
            game_criteria['keywords'] = [k.id for k in source_game_obj.keywords.all()]
        elif hasattr(source_game_obj, 'keywords') and hasattr(source_game_obj.keywords, 'all'):
            game_criteria['keywords'] = [k.id for k in source_game_obj.keywords.all()]
        else:
            game_criteria['keywords'] = []

        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'themes' in source_game_obj._prefetched_objects_cache:
            game_criteria['themes'] = [t.id for t in source_game_obj.themes.all()]
        elif hasattr(source_game_obj, 'themes') and hasattr(source_game_obj.themes, 'all'):
            game_criteria['themes'] = [t.id for t in source_game_obj.themes.all()]
        else:
            game_criteria['themes'] = []

        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'player_perspectives' in source_game_obj._prefetched_objects_cache:
            game_criteria['perspectives'] = [p.id for p in source_game_obj.player_perspectives.all()]
        elif hasattr(source_game_obj, 'player_perspectives') and hasattr(source_game_obj.player_perspectives, 'all'):
            game_criteria['perspectives'] = [p.id for p in source_game_obj.player_perspectives.all()]
        else:
            game_criteria['perspectives'] = []

        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'developers' in source_game_obj._prefetched_objects_cache:
            game_criteria['developers'] = [d.id for d in source_game_obj.developers.all()]
        elif hasattr(source_game_obj, 'developers') and hasattr(source_game_obj.developers, 'all'):
            game_criteria['developers'] = [d.id for d in source_game_obj.developers.all()]
        else:
            game_criteria['developers'] = []

        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'game_modes' in source_game_obj._prefetched_objects_cache:
            game_criteria['game_modes'] = [gm.id for gm in source_game_obj.game_modes.all()]
        elif hasattr(source_game_obj, 'game_modes') and hasattr(source_game_obj.game_modes, 'all'):
            game_criteria['game_modes'] = [gm.id for gm in source_game_obj.game_modes.all()]
        else:
            game_criteria['game_modes'] = []

        # ДОБАВЛЕНО: сбор движков
        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'engines' in source_game_obj._prefetched_objects_cache:
            game_criteria['engines'] = [e.id for e in source_game_obj.engines.all()]
        elif hasattr(source_game_obj, 'engines') and hasattr(source_game_obj.engines, 'all'):
            game_criteria['engines'] = [e.id for e in source_game_obj.engines.all()]
        else:
            game_criteria['engines'] = []

        source_game = SimpleSourceGame(
            game_obj=source_game_obj,
            criteria=game_criteria,
            display_name=source_display
        )
    else:
        logger.info("Поиск похожих игр по критериям")
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)
        source_display = "Search Criteria"

        source_game = SimpleSourceGame(
            game_obj=None,
            criteria=selected_criteria,
            display_name=source_display
        )


# Экспортируем все публичные функции
__all__ = [
    'ajax_load_games_page',
    'game_list',
    'get_similar_games_for_criteria',
    'get_similar_games_for_game',
    'get_source_game',
    'clear_game_list_cache_specific',
    '_build_context_from_cached_data',
    '_handle_ajax_game_page',
    '_get_all_games_mode_with_pagination',
    '_get_similar_games_mode_with_pagination',
    '_build_optimized_context',
    '_should_use_similar_mode',
    '_has_similarity_criteria',
    '_get_selected_criteria_objects',
    '_get_all_games_mode',
    '_get_similar_games_mode',
]
