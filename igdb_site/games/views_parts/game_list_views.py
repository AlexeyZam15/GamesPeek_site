# games/views_parts/game_list_views.py
"""Views for game list page."""

import os
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
    GameTypeEnum, cache, GameEngine
)
from ..utils.filter_renderer import FilterRenderer
from django.http import JsonResponse
from django.views.decorators.cache import cache_page
from django.views.decorators.vary import vary_on_headers

import redis
import pickle
from ..breadcrumb import generate_similar_games_breadcrumb

redis_client = redis.Redis(host='127.0.0.1', port=6379, db=1, decode_responses=False)

ITEMS_PER_PAGE = 16

# ===== ОПЦИЯ ДЛЯ ОТЛАДКИ =====
# Установите в True для вывода отладочной информации
DEBUG_SIMILARITY = os.environ.get('DEBUG_SIMILARITY', 'False') == 'True'

# Создаем экземпляр GameSimilarity с verbose=DEBUG_SIMILARITY
similarity = GameSimilarity(verbose=DEBUG_SIMILARITY)


def _debug_print(*args, **kwargs):
    """Условный print для отладки."""
    if DEBUG_SIMILARITY:
        print(*args, **kwargs)


def ajax_load_keywords(request: HttpRequest) -> HttpResponse:
    """AJAX endpoint for loading keywords with pagination and search."""
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

    items_per_page = 8 if is_mobile else 18

    keywords_qs = Keyword.objects.select_related('category').only(
        'id', 'name', 'category__id', 'category__name', 'cached_usage_count'
    )

    if search_term:
        keywords_qs = keywords_qs.filter(name__icontains=search_term)

    keywords_qs = keywords_qs.order_by('-cached_usage_count', 'name')

    paginator = Paginator(keywords_qs, items_per_page)

    try:
        page_obj = paginator.page(page_num)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

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
    """Отдельная страница для поиска игр через панель поиска вверху."""
    start_time = time.time()

    _debug_print(f"GET params: {dict(request.GET)}")
    search_query = request.GET.get('q', '').strip()
    page_num = request.GET.get('page', '1')
    sort_field = request.GET.get('sort', '-rating_count')

    _debug_print(f"Search query: '{search_query}'")
    _debug_print(f"Page: {page_num}, Sort: {sort_field}")

    try:
        page_num = int(page_num)
    except (ValueError, TypeError):
        page_num = 1

    if search_query:
        games_qs = Game.objects.all().only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        )

        from django.db.models import Q

        words = search_query.lower().split()
        name_filter = Q()
        for word in words:
            name_filter &= Q(name__icontains=word)

        games_qs = games_qs.filter(name_filter)

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


@cache_page(60 * 60)
@vary_on_headers('Cookie')
def game_list(request: HttpRequest) -> HttpResponse:
    """
    Main game list function - renders filters on server with aggressive caching.
    Filters are rendered server-side, not via AJAX.

    Первая страница (page=1 или без параметра page) загружается на сервере
    полностью, чтобы поисковые роботы видели контент.
    """
    from ..models import Game  # <--- ДОБАВЛЕН ИМПОРТ (УБРАТЬ КОНФЛИКТ)

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

    params_hash = _get_params_hash_from_request(request)

    cached_filter_sections = _render_filters_with_cache(
        params_hash,
        filter_data,
        years_range,
        search_selected,
        similarity_selected,
        params.get('sort', '')
    )

    page_param = params.get('page', '1')
    is_first_page = page_param == '1' or page_param == ''

    games_data = []
    games_with_similarity_data = []
    page_obj = None
    paginator = None
    total_count = 0
    total_pages = 1
    current_page = 1
    show_similarity_flag = find_similar
    source_game_for_template = None

    if is_first_page:
        _debug_print(f"[SERVER RENDER] First page - loading games on server")
        page_num = 1
        current_page = 1

        has_search_params = any([
            search_selected['genres'], search_selected['keywords'], search_selected['themes'],
            search_selected['perspectives'], search_selected['game_modes'], search_selected['engines'],
            search_selected['platforms'], search_selected['game_types'],
            search_selected['release_year_start'], search_selected['release_year_end']
        ])

        if find_similar and (source_game_obj or any(similarity_selected.values())):
            mode_result = _get_similar_games_mode_with_pagination(
                params,
                similarity_selected,
                source_game_obj,
                page_num,
                search_genres_list=search_selected['genres'],
                search_keywords_list=search_selected['keywords'],
                search_themes_list=search_selected['themes'],
                search_perspectives_list=search_selected['perspectives'],
                search_game_modes_list=search_selected['game_modes'],
                search_engines_list=search_selected['engines'],
                search_platforms_list=search_selected['platforms'],
                search_game_types_list=search_selected['game_types'],
                search_year_start_int=search_selected['release_year_start'],
                search_year_end_int=search_selected['release_year_end'],
            )
            games_with_similarity_data = _update_games_with_cached_cards(
                mode_result.get('games_with_similarity', []),
                {'show_similarity': True, 'source_game': mode_result.get('source_game'), 'current_page': page_num}
            )
            page_obj = mode_result.get('page_obj')
            paginator = mode_result.get('paginator')
            total_count = mode_result.get('total_count', 0)
            total_pages = paginator.num_pages if paginator else 1
            show_similarity_flag = True
            source_game_for_template = mode_result.get('source_game')
        else:
            games_qs = Game.objects.all().only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            )

            if has_search_params:
                search_filters = {}
                if search_selected['platforms']:
                    search_filters['platforms'] = search_selected['platforms']
                if search_selected['game_types']:
                    search_filters['game_types'] = search_selected['game_types']
                if search_selected['genres']:
                    search_filters['genres'] = search_selected['genres']
                if search_selected['keywords']:
                    search_filters['keywords'] = search_selected['keywords']
                if search_selected['themes']:
                    search_filters['themes'] = search_selected['themes']
                if search_selected['perspectives']:
                    search_filters['perspectives'] = search_selected['perspectives']
                if search_selected['game_modes']:
                    search_filters['game_modes'] = search_selected['game_modes']
                if search_selected['engines']:
                    search_filters['engines'] = search_selected['engines']
                if search_selected['release_year_start']:
                    search_filters['release_year_start'] = search_selected['release_year_start']
                if search_selected['release_year_end']:
                    search_filters['release_year_end'] = search_selected['release_year_end']

                from .base_views import _apply_search_filters
                games_qs = _apply_search_filters(games_qs, search_filters)

            sort_field = params.get('sort', '-rating_count')
            if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count',
                              '-first_release_date']:
                games_qs = games_qs.order_by(sort_field)
            else:
                games_qs = games_qs.order_by('-rating_count')

            paginator = Paginator(games_qs, ITEMS_PER_PAGE)
            page_obj = paginator.page(1)
            games_list_data = list(page_obj.object_list)
            games_data = _update_games_with_cached_cards(
                games_list_data,
                {'show_similarity': False, 'current_page': page_num}
            )
            total_count = paginator.count
            total_pages = paginator.num_pages
            show_similarity_flag = False

    # Генерируем навигационную цепочку для страницы похожих игр
    breadcrumb_json_ld = ''
    if find_similar and source_game_obj:
        breadcrumb_json_ld = generate_similar_games_breadcrumb(
            game_title=source_game_obj.name,
            base_url="https://gamespeek.dpdns.org"
        )
    elif find_similar and params.get('source_game'):
        try:
            from ..models import Game
            source_game_temp = Game.objects.only('name').get(pk=int(params['source_game']))
            breadcrumb_json_ld = generate_similar_games_breadcrumb(
                game_title=source_game_temp.name,
                base_url="https://gamespeek.dpdns.org"
            )
        except (Game.DoesNotExist, ValueError):
            pass

    context = {
        'games': games_data,
        'games_with_similarity': games_with_similarity_data,
        'page_obj': page_obj,
        'paginator': paginator,
        'is_paginated': total_pages > 1,
        'total_count': total_count,
        'total_pages': total_pages,
        'current_page': current_page,
        'start_index': 0 if total_count == 0 else 1,
        'end_index': min(ITEMS_PER_PAGE, total_count) if total_count > 0 else 0,
        'items_per_page': ITEMS_PER_PAGE,
        'find_similar': find_similar,
        'show_similarity': show_similarity_flag,
        'source_game': source_game_for_template,
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
            'server_rendered_first_page': is_first_page,
        },

        'breadcrumb_json_ld': breadcrumb_json_ld,
    }

    if source_game_obj and not source_game_for_template:
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
    """Генерирует хеш параметров запроса для инвалидации кэша фильтров."""
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
    """Renders filter sections with aggressive caching using Redis."""
    import hashlib
    import json
    import pickle
    from django.conf import settings
    from ..utils.filter_renderer import FilterRenderer

    redis_client = getattr(settings, 'redis_client', None)
    if redis_client is None:
        import redis
        redis_client = redis.Redis(host='127.0.0.1', port=6379, db=1, decode_responses=False)

    cache_data = {
        'params_hash': params_hash,
        'search_selected': {k: sorted(v) for k, v in search_selected.items() if isinstance(v, list)},
        'similarity_selected': {k: sorted(v) for k, v in similarity_selected.items() if isinstance(v, list)},
        'current_sort': current_sort,
        'years_range': years_range,
        'version': 'v3'
    }
    cache_key_str = json.dumps(cache_data, sort_keys=True)
    cache_key_hash = hashlib.md5(cache_key_str.encode()).hexdigest()
    cache_key = f'rendered_filters_{cache_key_hash}'

    cached_data = redis_client.get(cache_key)
    if cached_data is not None:
        _debug_print(f"[CACHE HIT] rendered_filters for key {cache_key_hash[:8]}")
        return pickle.loads(cached_data)

    _debug_print(f"[CACHE MISS] rendered_filters for key {cache_key_hash[:8]}")

    renderer = FilterRenderer()

    from ..models import Genre
    genres_list = list(Genre.objects.all().only('id', 'name').order_by('name'))
    game_types_list = GameTypeEnum.CHOICES

    result = {
        'search_platforms': renderer.render_search_platforms(
            filter_data.get('platforms', []),
            search_selected.get('platforms', [])
        ),
        'search_game_types': renderer.render_search_game_types(
            game_types_list,
            search_selected.get('game_types', [])
        ),
        'search_genres': renderer.render_search_genres(
            genres_list,
            search_selected.get('genres', [])
        ),
        'search_keywords': renderer.render_search_keywords(
            filter_data.get('keywords', []),
            search_selected.get('keywords', [])
        ),
        'search_themes': renderer.render_search_themes(
            filter_data.get('themes', []),
            search_selected.get('themes', [])
        ),
        'search_perspectives': renderer.render_search_perspectives(
            filter_data.get('perspectives', []),
            search_selected.get('perspectives', [])
        ),
        'search_game_modes': renderer.render_search_game_modes(
            filter_data.get('game_modes', []),
            search_selected.get('game_modes', [])
        ),
        'search_engines': renderer.render_search_engines(
            filter_data.get('engines', []),
            search_selected.get('engines', [])
        ),
        'search_date': renderer.render_search_date_filter(
            search_selected.get('release_year_start'),
            search_selected.get('release_year_end'),
            years_range.get('min_year', 1970),
            years_range.get('max_year', 2024),
            years_range.get('current_year', 2024)
        ),
        'similarity_genres': renderer.render_similarity_genres(
            genres_list,
            similarity_selected.get('genres', [])
        ),
        'similarity_keywords': renderer.render_similarity_keywords(
            filter_data.get('keywords', []),
            similarity_selected.get('keywords', [])
        ),
        'similarity_themes': renderer.render_similarity_themes(
            filter_data.get('themes', []),
            similarity_selected.get('themes', [])
        ),
        'similarity_perspectives': renderer.render_similarity_perspectives(
            filter_data.get('perspectives', []),
            similarity_selected.get('perspectives', [])
        ),
        'similarity_game_modes': renderer.render_similarity_game_modes(
            filter_data.get('game_modes', []),
            similarity_selected.get('game_modes', [])
        ),
        'similarity_engines': renderer.render_similarity_engines(
            filter_data.get('engines', []),
            similarity_selected.get('engines', [])
        ),
    }

    redis_client.setex(cache_key, 1800, pickle.dumps(result))
    _debug_print(f"[CACHE SAVE] rendered_filters for key {cache_key_hash[:8]} saved to Redis")

    return result


def ajax_load_filters(request: HttpRequest) -> HttpResponse:
    """Load filters HTML via AJAX."""
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
    """Получает HTML карточки из кэша модели."""
    try:
        card_cache = GameCardCache.get_card_for_game(game.id, game=game)
        if card_cache:
            card_cache.increment_hit()
            return card_cache.rendered_card
    except Exception as e:
        logger.debug(f"Cache miss for game {game.id}: {str(e)}")

    return None


def _render_and_cache_card(game: Game, context: Dict, show_similarity: bool = False,
                           similarity_percent: float = None) -> str:
    """Рендерит карточку и немедленно сохраняет в кэш модели."""
    from games.utils.game_card_utils import GameCardCreator

    rendered_card = render_to_string('games/partials/_game_card.html', context)

    try:
        related_data = GameCardCreator._extract_related_data(game)
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
        return rendered_card


def _render_game_card_with_caching(game: Game, context: Dict) -> str:
    """Рендерит карточку игры с использованием кэша модели."""
    show_similarity = context.get('show_similarity', False)
    similarity_percent = None

    if show_similarity and hasattr(game, 'similarity'):
        similarity_percent = game.similarity

    cached_html = _get_cached_card_html(game, show_similarity, similarity_percent)

    if cached_html:
        return cached_html

    return _render_and_cache_card(game, context, show_similarity, similarity_percent)


def _update_games_with_cached_cards(games_list: List, context: Dict) -> List:
    """Обновляет список игр объектами с кэшированными карточками из БД."""
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
    """Режим отображения ВСЕХ игр с СЕРВЕРНОЙ пагинацией."""
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

    Для ПЕРВОЙ СТРАНИЦЫ (page=1) использует готовый список similar_game_ids
    из модели Game - МГНОВЕННАЯ ЗАГРУЗКА без тяжелых вычислений.

    Для остальных страниц выполняет полный расчет через GameSimilarity.
    """
    import time

    total_start = time.time()
    current_sort = params.get('sort', '-similarity')

    # Собираем search_filters из параметров
    search_filters = {}
    if search_platforms_list:
        search_filters['platforms'] = search_platforms_list
    if search_game_modes_list:
        search_filters['game_modes'] = search_game_modes_list
    if search_genres_list:
        search_filters['genres'] = search_genres_list
    if search_keywords_list:
        search_filters['keywords'] = search_keywords_list
    if search_themes_list:
        search_filters['themes'] = search_themes_list
    if search_perspectives_list:
        search_filters['perspectives'] = search_perspectives_list
    if search_game_types_list:
        search_filters['game_types'] = search_game_types_list
    if search_engines_list:
        search_filters['engines'] = search_engines_list
    if search_year_start_int:
        search_filters['release_year_start'] = search_year_start_int
    if search_year_end_int:
        search_filters['release_year_end'] = search_year_end_int

    # Проверяем, есть ли изменения в фильтрах
    has_filter_changes = False
    if source_game_obj:
        default_genres = [g.id for g in source_game_obj.genres.all()]
        default_keywords = [k.id for k in source_game_obj.keywords.all()]
        default_themes = [t.id for t in source_game_obj.themes.all()]
        default_perspectives = [p.id for p in source_game_obj.player_perspectives.all()]
        default_game_modes = [gm.id for gm in source_game_obj.game_modes.all()]
        default_engines = [e.id for e in source_game_obj.engines.all()]

        if (set(search_filters.get('genres', [])) != set(default_genres) or
                set(search_filters.get('keywords', [])) != set(default_keywords) or
                set(search_filters.get('themes', [])) != set(default_themes) or
                set(search_filters.get('perspectives', [])) != set(default_perspectives) or
                set(search_filters.get('game_modes', [])) != set(default_game_modes) or
                set(search_filters.get('engines', [])) != set(default_engines)):
            has_filter_changes = True

        if (search_filters.get('platforms') or search_filters.get('game_types') or
                search_filters.get('release_year_start') or search_filters.get('release_year_end')):
            has_filter_changes = True

    # ЕСЛИ ПЕРВАЯ СТРАНИЦА И НЕТ ИЗМЕНЕНИЙ В ФИЛЬТРАХ - ИСПОЛЬЗУЕМ ГОТОВЫЙ СПИСОК
    is_first_page = page_num == 1
    use_cached_list = is_first_page and source_game_obj and not has_filter_changes

    if use_cached_list:
        _debug_print(f"[SIMILAR MODE] FIRST PAGE - using cached similar_game_ids (INSTANT)")

        # Получаем ID из готового списка
        similar_ids = source_game_obj.similar_game_ids or []

        if similar_ids:
            # Загружаем игры по ID (быстро, без тяжелых вычислений)
            games = Game.objects.filter(id__in=similar_ids).prefetch_related(
                'genres', 'themes', 'game_modes', 'engines',
                'platforms', 'player_perspectives', 'developers'
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            )

            # Создаем словарь для быстрого доступа
            games_dict = {game.id: game for game in games}

            # Формируем результат в правильном порядке
            games_with_similarity = []
            # Для первой страницы используем примерную схожесть из порядка списка
            # (список уже отсортирован по убыванию схожести)
            total_similarity = 100.0
            step = (100.0 - 40.0) / max(len(similar_ids), 1)

            for idx, game_id in enumerate(similar_ids[:12]):  # Только первые 12
                if game_id in games_dict:
                    game = games_dict[game_id]
                    # Приблизительная схожесть на основе позиции в списке
                    similarity_score = max(40.0, total_similarity - idx * step)
                    games_with_similarity.append({
                        'game': game,
                        'similarity': round(similarity_score, 2),
                        'is_source_game': False
                    })

            total_count = len(similar_ids)

            # Создаем пагинатор
            paginator = Paginator(games_with_similarity, ITEMS_PER_PAGE)
            page_obj = paginator.page(1)

            # Формируем source_game
            source_display = source_game_obj.name
            game_criteria = {
                'genres': [g.id for g in source_game_obj.genres.all()],
                'keywords': [k.id for k in source_game_obj.keywords.all()],
                'themes': [t.id for t in source_game_obj.themes.all()],
                'perspectives': [p.id for p in source_game_obj.player_perspectives.all()],
                'game_modes': [gm.id for gm in source_game_obj.game_modes.all()],
                'engines': [e.id for e in source_game_obj.engines.all()],
            }
            source_game = SimpleSourceGame(
                game_obj=source_game_obj,
                criteria=game_criteria,
                display_name=source_display
            )

            _debug_print(
                f"[FIRST PAGE] Loaded {len(games_with_similarity)} games from cached list in {time.time() - total_start:.3f}s")

            return {
                'page_obj': page_obj,
                'paginator': paginator,
                'is_paginated': paginator.num_pages > 1,
                'total_count': total_count,
                'current_page': 1,
                'games_with_similarity': list(page_obj.object_list),
                'show_similarity': True,
                'find_similar': True,
                'source_game': source_game,
                'source_game_obj': source_game_obj,
                'timers': {'total': time.time() - total_start},
            }
        else:
            _debug_print(f"[FIRST PAGE] No similar_game_ids, falling back to full calculation")

    # ДЛЯ ОСТАЛЬНЫХ СТРАНИЦ ИЛИ ПРИ ИЗМЕНЕНИИ ФИЛЬТРОВ - ПОЛНЫЙ РАСЧЕТ
    _debug_print(f"[SIMILAR MODE] FULL calculation (page={page_num}, has_changes={has_filter_changes})")

    if source_game_obj:
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, [], search_filters
        )

        source_display = source_game_obj.name
        game_criteria = {
            'genres': [g.id for g in source_game_obj.genres.all()],
            'keywords': [k.id for k in source_game_obj.keywords.all()],
            'themes': [t.id for t in source_game_obj.themes.all()],
            'perspectives': [p.id for p in source_game_obj.player_perspectives.all()],
            'game_modes': [gm.id for gm in source_game_obj.game_modes.all()],
            'engines': [e.id for e in source_game_obj.engines.all()],
        }
        source_game = SimpleSourceGame(
            game_obj=source_game_obj,
            criteria=game_criteria,
            display_name=source_display
        )
    else:
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria, search_filters)
        source_display = "Search Criteria"
        source_game = SimpleSourceGame(
            game_obj=None,
            criteria=selected_criteria,
            display_name=source_display
        )

    # Форматируем данные
    games_with_similarity = _format_similar_games_data(similar_games_data)

    for item in games_with_similarity:
        if 'game' in item and item.get('similarity') is not None:
            item['game'].similarity = item['similarity']

    # Сортируем
    _sort_similar_games(games_with_similarity, current_sort)

    # Пагинация
    paginator = Paginator(games_with_similarity, ITEMS_PER_PAGE)
    try:
        page_obj = paginator.page(page_num)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    current_games_with_similarity = list(page_obj.object_list)

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
        'timers': {'total': time.time() - total_start},
    }


@cache_page(60 * 15)
@vary_on_headers('*')
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

    _debug_print("\n=== AJAX LOAD GAMES PAGE ===")
    _debug_print(f"GET params: {dict(request.GET)}")

    stage_start = time.time()
    page_num = request.GET.get('page', '1')
    try:
        page_num = int(page_num)
    except (ValueError, TypeError):
        page_num = 1

    params = extract_request_params(request)
    selected_criteria = convert_params_to_lists(params)

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

    _debug_print(f"Search year start: {search_year_start} -> {search_year_start_int}")
    _debug_print(f"Search year end: {search_year_end} -> {search_year_end_int}")
    _debug_print(f"Search text query: '{search_text_query}'")
    _debug_print(f"Search genres: {search_genres_list}")
    _debug_print(f"Search platforms: {search_platforms_list}")
    _debug_print(f"Search keywords: {search_keywords_list}")

    sort_field = params.get('sort', '-rating_count')
    find_similar = params.get('find_similar') == '1'

    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = Game.objects.only('id', 'name').get(pk=int(params['source_game']))
            _debug_print(f"Source game: {source_game_obj.id} - {source_game_obj.name}")
        except (Game.DoesNotExist, ValueError):
            pass
    timers['params_extraction'] = round(time.time() - stage_start, 3)

    stage_start = time.time()

    has_search_params = any([
        search_genres_list, search_keywords_list, search_themes_list,
        search_perspectives_list, search_game_modes_list, search_engines_list,
        search_platforms_list, search_game_types_list,
        search_year_start_int, search_year_end_int, search_text_query
    ])

    use_regular_mode = not find_similar

    if use_regular_mode:
        _debug_print("Mode: regular games with search filters")

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
        _debug_print("Mode: similar games with search filters")
        _debug_print(
            f"Passing to _get_similar_games_mode_with_pagination: year_start={search_year_start_int}, year_end={search_year_end_int}")

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

    if DEBUG_SIMILARITY:
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

    _debug_print(f"Response prepared in {time.time() - start_time:.3f}s")
    _debug_print("=== END AJAX LOAD ===\n")

    return response


def get_similar_games_for_criteria(selected_criteria: Dict[str, List[int]], search_filters: Dict = None) -> Tuple[
    List, int]:
    """
    Get similar games for criteria without any caching.

    Args:
        selected_criteria: Выбранные критерии для поиска
        search_filters: Словарь с фильтрами (включая release_year_start, release_year_end)

    Returns:
        Tuple (список игр с процентами схожести, общее количество)
    """
    from ..similarity import GameSimilarity, VirtualGame
    import time

    start_total = time.time()

    print(f"\n=== get_similar_games_for_criteria START ===")
    print(
        f"Criteria: genres={len(selected_criteria['genres'])}, keywords={len(selected_criteria['keywords'])}, themes={len(selected_criteria['themes'])}, engines={len(selected_criteria.get('engines', []))}")
    print(f"Search filters: {search_filters}")

    similarity_engine = GameSimilarity()

    virtual_game = VirtualGame(
        genre_ids=selected_criteria['genres'],
        keyword_ids=selected_criteria['keywords'],
        theme_ids=selected_criteria['themes'],
        perspective_ids=selected_criteria['perspectives'],
        developer_ids=selected_criteria['developers'],
        game_mode_ids=selected_criteria['game_modes'],
        engine_ids=selected_criteria['engines'],
    )

    similar_games = similarity_engine.find_similar_games(
        source_game=virtual_game,
        search_filters=search_filters
    )

    total_count = len(similar_games)

    print(f"find_similar_games executed: {time.time() - start_total:.3f}s, found {total_count} games")

    criteria_count = sum(len(v) for key, v in selected_criteria.items()
                         if key not in ['release_years', 'release_year_start', 'release_year_end'])

    print(f"TOTAL time: {time.time() - start_total:.2f}s, criteria: {criteria_count}, results: {total_count}")
    print("=== get_similar_games_for_criteria END ===\n")

    return similar_games, total_count


def get_similar_games_for_game(game_obj: Game, selected_platforms: List[int], search_filters: Dict = None) -> Tuple[
    List, int]:
    """
    Get similar games for a specific game without any caching.

    Args:
        game_obj: Объект исходной игры
        selected_platforms: Список ID выбранных платформ
        search_filters: Словарь с фильтрами (включая release_year_start, release_year_end)

    Returns:
        Tuple (список игр с процентами схожести, общее количество)
    """
    from ..similarity import GameSimilarity
    import time

    start_total = time.time()
    print(f"\n=== get_similar_games_for_game START for game {game_obj.id} - {game_obj.name} ===")
    print(f"Search filters: {search_filters}")

    similarity_engine = GameSimilarity()

    similar_games = similarity_engine.find_similar_games(
        source_game=game_obj,
        min_similarity=0,
        limit=500,
        search_filters=search_filters
    )

    print(f"find_similar_games executed: {time.time() - start_total:.3f}s, found {len(similar_games)} games")

    total_count = len(similar_games)

    print(f"TOTAL time: {time.time() - start_total:.3f}s")
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
    """Очищает кэш страниц игр."""
    from django.core.cache import cache

    global CACHE_VERSION
    old_version = CACHE_VERSION
    CACHE_VERSION = f"v{int(CACHE_VERSION[1:]) + 1}"
    return f"Cache version updated from {old_version} to {CACHE_VERSION}"


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
