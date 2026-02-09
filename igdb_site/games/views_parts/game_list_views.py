"""Views for game list page."""

from django.utils import timezone
from typing import Dict, List, Tuple, Any, Optional
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render
from django.db.models import Prefetch
from django.template.loader import render_to_string
import time

from .base_views import (
    logger, CACHE_TIMES, get_cache_key, cache_get_or_set,
    extract_request_params, convert_params_to_lists, _apply_filters,
    _get_cached_years_range, _get_optimized_filter_data, _get_cached_game,
    _get_cached_genres_list, _get_cached_filter_data,
    _format_similar_games_data, _sort_similar_games,
    SimpleSourceGame, GameSimilarity, VirtualGame,
    Game, Genre, Platform, PlayerPerspective, Keyword, Theme, Company, GameMode,
    GameTypeEnum, cache
)

# Константа для количества игр на страницу
ITEMS_PER_PAGE_CLIENT = 16

# Конфигурация кэширования страниц
PAGE_CACHE_TIMEOUT = 300  # 5 минут для страниц с фильтрами
STATIC_CACHE_TIMEOUT = 1800  # 30 минут для статических страниц
CACHE_VERSION = 'v2'  # Увеличиваем при изменении шаблонов


def _generate_page_cache_key(request_params, page_number=1):
    """Генерирует уникальный ключ кэша для страницы"""
    import hashlib
    import json

    # Сортируем параметры для консистентности
    sorted_params = dict(sorted(request_params.items()))
    sorted_params['page'] = page_number
    sorted_params['cache_version'] = CACHE_VERSION

    # Создаем хэш
    params_json = json.dumps(sorted_params, sort_keys=True)
    key_hash = hashlib.md5(params_json.encode()).hexdigest()

    return f'game_list_page_{key_hash}'


def _get_cached_page_or_render(request, cache_key, render_func, timeout):
    """Получает страницу из кэша или рендерит новую"""
    from django.core.cache import cache
    from django.http import HttpResponse

    # Пробуем получить из кэша
    cached_content = cache.get(cache_key)
    if cached_content:
        response = HttpResponse(cached_content)
        response['X-Cache-Hit'] = 'Page'
        response['X-Cache-Key'] = cache_key
        return response

    # Рендерим новую страницу
    response = render_func()

    # Кэшируем если статус 200
    if response.status_code == 200:
        cache.set(cache_key, response.content, timeout)
        response['X-Cache-Hit'] = 'Miss'
        response['X-Cache-Key'] = cache_key

    return response

def ajax_load_games_page(request: HttpRequest) -> HttpResponse:
    """Load games for specific page via AJAX - CORRECTED VERSION."""
    start_time = time.time()

    page_num = request.GET.get('page', '1')
    try:
        page_num = int(page_num)
    except (ValueError, TypeError):
        page_num = 1

    params = extract_request_params(request)
    selected_criteria = convert_params_to_lists(params)

    sort_field = params.get('sort', '-rating_count')

    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = Game.objects.get(pk=int(params['source_game']))
        except (Game.DoesNotExist, ValueError):
            pass

    print(f"AJAX LOAD: Page {page_num}, find_similar: {find_similar}, sort: {sort_field}")

    items_per_page = 16
    offset = (page_num - 1) * items_per_page

    if find_similar and (source_game_obj or any([
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes']
    ])):
        if source_game_obj:
            similar_games_data, total_count = get_similar_games_for_game(
                source_game_obj, selected_criteria['platforms']
            )
        else:
            similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)

        games_with_similarity = _format_similar_games_data(similar_games_data, limit=total_count)
        _sort_similar_games(games_with_similarity, sort_field)

        current_page_games = games_with_similarity[offset:offset + items_per_page]

        for i, game_item in enumerate(current_page_games):
            game_item['game_index'] = offset + i
            game_item['page_number'] = page_num
            # ВАЖНО: Убеждаемся, что у объекта game есть свойство similarity
            if isinstance(game_item, dict):
                game_obj = game_item.get('game')
                similarity = game_item.get('similarity', 0)
                if game_obj and not hasattr(game_obj, 'similarity'):
                    game_obj.similarity = similarity

        context = {
            'games': current_page_games,
            'show_similarity': True,
            'source_game': SimpleSourceGame(
                game_obj=source_game_obj,
                criteria=selected_criteria,
                display_name=source_game_obj.name if source_game_obj else "Search Criteria"
            ),
            'current_page': page_num,
            'game_index_offset': offset,
        }
    else:
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

        current_games = list(games_qs[offset:offset + items_per_page])

        for i, game in enumerate(current_games):
            game.game_index = offset + i
            game.page_number = page_num

        context = {
            'games': current_games,
            'show_similarity': False,
            'current_page': page_num,
        }

    html = render_to_string('games/game_list/_games_grid.html', context)

    response = HttpResponse(html)
    response['Content-Type'] = 'text/html; charset=utf-8'
    response['X-AJAX-Page'] = str(page_num)
    response['X-AJAX-Count'] = str(len(context.get('games', [])))
    response['X-AJAX-Offset'] = str(offset)
    response['X-Response-Time'] = f"{time.time() - start_time:.3f}s"

    return response


def game_list(request: HttpRequest) -> HttpResponse:
    """Main game list function with page-level caching."""

    # Проверяем AJAX запросы - не кэшируем
    is_ajax = request.GET.get('_ajax') == '1'
    if is_ajax:
        return _handle_ajax_game_page(request)

    # Определяем таймаут кэша
    params = extract_request_params(request)

    # Проверяем, статичная ли это страница (без фильтров)
    has_filters = any([
        params.get('g'), params.get('k'), params.get('p'), params.get('t'),
        params.get('pp'), params.get('d'), params.get('gm'), params.get('gt'),
        params.get('yr'), params.get('ys'), params.get('ye'), params.get('find_similar'),
        params.get('source_game')
    ])

    timeout = STATIC_CACHE_TIMEOUT if not has_filters else PAGE_CACHE_TIMEOUT

    # Генерируем ключ кэша
    cache_key = _generate_page_cache_key(params)

    # Функция для рендеринга
    def render_game_list():
        start_time = time.time()

        requested_page = request.GET.get('page', '1')
        print(f"MAIN VIEW: Rendering page {requested_page}")

        try:
            requested_page_num = int(requested_page)
        except (ValueError, TypeError):
            requested_page_num = 1

        selected_criteria = convert_params_to_lists(params)
        selected_criteria_objects = _get_selected_criteria_objects(selected_criteria)
        years_range = _get_cached_years_range()

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

        items_per_page = 16
        offset = (requested_page_num - 1) * items_per_page

        if should_use_similar_mode:
            mode_result = _get_similar_games_mode_paginated(
                params, selected_criteria, source_game_obj, requested_page_num
            )
            mode = 'similar'
        else:
            mode_result = _get_all_games_mode_paginated(
                selected_criteria, params.get('sort', '-rating_count'), requested_page_num
            )
            mode = 'regular'

        print(
            f"RENDERING: Mode {mode}, page {requested_page_num}, games: {len(mode_result.get('games', mode_result.get('games_with_similarity', [])))}")

        filter_data = _get_optimized_filter_data()

        context = _build_optimized_context(
            mode_result=mode_result,
            mode=mode,
            filter_data=filter_data,
            selected_criteria=selected_criteria,
            selected_criteria_objects=selected_criteria_objects,
            params=params,
            source_game_obj=source_game_obj,
            find_similar=find_similar,
            years_range=years_range,
            execution_time=time.time() - start_time,
            requested_page_num=requested_page_num,
            items_per_page=items_per_page,
            offset=offset
        )

        context['debug_info']['requested_page'] = requested_page_num
        context['debug_info']['mode_result_page'] = mode_result.get('current_page', 1)
        context['debug_info']['offset'] = offset
        context['debug_info']['page_cached'] = False

        response = render(request, 'games/game_list.html', context)
        response['X-Cache-Hit'] = 'Render'
        response['X-Response-Time'] = f"{context['execution_time']:.3f}s"
        response['X-Mode'] = mode
        response['X-Requested-Page'] = str(requested_page_num)
        response['X-ModeResult-Page'] = str(mode_result.get('current_page', 1))
        response['X-Offset'] = str(offset)

        return response

    # Используем кэшированную или рендерим новую страницу
    return _get_cached_page_or_render(request, cache_key, render_game_list, timeout)


def _build_context_from_cached_data(cached_data: Dict, params: Dict, requested_page_num: int) -> Dict:
    """Build context from cached data with page number support."""
    filter_data = cached_data['filter_data']
    years_range = cached_data['years_range']
    mode_result = cached_data['mode_result']

    current_page = requested_page_num
    total_count = mode_result.get('total_count', 0)
    total_pages = (total_count + ITEMS_PER_PAGE_CLIENT - 1) // ITEMS_PER_PAGE_CLIENT if total_count > 0 else 1

    if current_page > total_pages:
        current_page = total_pages
    if current_page < 1:
        current_page = 1

    start_index = (current_page - 1) * ITEMS_PER_PAGE_CLIENT + 1
    end_index = min(current_page * ITEMS_PER_PAGE_CLIENT, total_count)

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
        mode_result = _get_similar_games_mode_paginated(
            params, selected_criteria, source_game_obj, requested_page_num
        )
        games = mode_result.get('games_with_similarity', [])
        print(f"AJAX: Loading similar games page {requested_page_num}, found {len(games)} games")
        template_context = {
            'games': games,
            'show_similarity': True,
            'source_game': mode_result.get('source_game'),
            'current_page': requested_page_num,
            'similarity_map': mode_result.get('similarity_map', {}),
        }
    else:
        mode_result = _get_all_games_mode_paginated(
            selected_criteria, params.get('sort', '-rating_count'), requested_page_num
        )
        games = mode_result.get('games', [])
        print(f"AJAX: Loading regular games page {requested_page_num}, found {len(games)} games")
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


def _get_all_games_mode_paginated(
        selected_criteria: Dict[str, List[int]],
        sort_field: str,
        page_num: int
) -> Dict[str, Any]:
    """Режим отображения игр с клиентской пагинацией."""
    print(f"SERVER: Loading ALL games mode for page {page_num}")

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

    total_count = games_qs.count()
    items_per_page = 16
    offset = (page_num - 1) * items_per_page

    print(f"SERVER DEBUG: Page {page_num}, offset: {offset}, limit: {items_per_page}")
    print(f"SERVER DEBUG: Total count: {total_count}")

    games = list(games_qs[offset:offset + items_per_page])

    print(f"SERVER DEBUG: Loaded {len(games)} games for page {page_num}")

    return {
        'games': games,
        'total_count': total_count,
        'current_page': page_num,
        'show_similarity': False,
        'find_similar': False,
        'source_game': None,
        'similarity_map': {},
    }


def _get_similar_games_mode_paginated(
        params: Dict[str, str],
        selected_criteria: Dict[str, List[int]],
        source_game_obj: Optional[Game],
        page_num: int
) -> Dict[str, Any]:
    """Режим похожих игр с клиентской пагинацией."""
    current_sort = params.get('sort', '-similarity')

    print(f"AJAX: Similar games mode for page {page_num}")

    if source_game_obj:
        print(f"AJAX: Finding similar games for game: {source_game_obj.name} (ID: {source_game_obj.id})")
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, selected_criteria['platforms']
        )
        source_display = source_game_obj.name

        game_criteria = {}

        if hasattr(source_game_obj, 'genres') and hasattr(source_game_obj.genres, 'all'):
            game_criteria['genres'] = [g.id for g in source_game_obj.genres.all()]
        else:
            game_criteria['genres'] = []

        if hasattr(source_game_obj, 'keywords') and hasattr(source_game_obj.keywords, 'all'):
            game_criteria['keywords'] = [k.id for k in source_game_obj.keywords.all()]
        else:
            game_criteria['keywords'] = []

        if hasattr(source_game_obj, 'themes') and hasattr(source_game_obj.themes, 'all'):
            game_criteria['themes'] = [t.id for t in source_game_obj.themes.all()]
        else:
            game_criteria['themes'] = []

        if hasattr(source_game_obj, 'player_perspectives') and hasattr(source_game_obj.player_perspectives, 'all'):
            game_criteria['perspectives'] = [p.id for p in source_game_obj.player_perspectives.all()]
        else:
            game_criteria['perspectives'] = []

        if hasattr(source_game_obj, 'game_modes') and hasattr(source_game_obj.game_modes, 'all'):
            game_criteria['game_modes'] = [gm.id for gm in source_game_obj.game_modes.all()]
        else:
            game_criteria['game_modes'] = []

        source_game = SimpleSourceGame(
            game_obj=source_game_obj,
            criteria=game_criteria,
            display_name=source_display
        )
    else:
        print(f"AJAX: Finding similar games by criteria")
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)
        source_display = "Search Criteria"

        source_game = SimpleSourceGame(
            game_obj=None,
            criteria=selected_criteria,
            display_name=source_display
        )

    print(f"AJAX: Found {len(similar_games_data)} similar games total")

    games_with_similarity = _format_similar_games_data(similar_games_data, limit=total_count)
    _sort_similar_games(games_with_similarity, current_sort)

    items_per_page = 16
    offset = (page_num - 1) * items_per_page
    current_page_games = games_with_similarity[offset:offset + items_per_page]

    print(f"AJAX: Returning {len(current_page_games)} games for page {page_num} (offset {offset})")

    return {
        'games_with_similarity': current_page_games,
        'total_count': total_count,
        'current_page': page_num,
        'show_similarity': True,
        'find_similar': True,
        'source_game': source_game,
        'source_game_obj': source_game_obj,
    }


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
        items_per_page: int = 16,
        offset: int = 0
) -> Dict[str, Any]:
    """
    Build optimized template context with minimal overhead.
    """
    genres_list = _get_cached_genres_list()

    current_page = requested_page_num
    total_count = mode_result.get('total_count', 0)
    total_pages = 0

    if mode == 'similar':
        games_with_similarity = mode_result.get('games_with_similarity', [])
        games = []
        show_similarity = True
        source_game = mode_result.get('source_game')

        if games_with_similarity:
            start_idx = offset
            end_idx = min(offset + items_per_page, len(games_with_similarity))
            current_page_games = games_with_similarity[start_idx:end_idx]

            for i, game_item in enumerate(current_page_games):
                # ВАЖНО: Убеждаемся, что у объекта game есть свойство similarity
                if isinstance(game_item, dict):
                    game_obj = game_item.get('game')
                    similarity = game_item.get('similarity', 0)
                    if game_obj and not hasattr(game_obj, 'similarity'):
                        game_obj.similarity = similarity

                game_item['game_index'] = offset + i
                game_item['page_number'] = current_page

            games_with_similarity = current_page_games
    else:
        games = mode_result.get('games', [])
        games_with_similarity = []
        show_similarity = False
        source_game = None

        if games:
            start_idx = offset
            end_idx = min(offset + items_per_page, len(games))
            current_page_games = games[start_idx:end_idx]

            for i, game in enumerate(current_page_games):
                game.game_index = offset + i
                game.page_number = current_page

            games = current_page_games

    start_index = offset + 1
    end_index = min(offset + items_per_page, total_count)

    print(
        f"CONTEXT: Building for REQUESTED page {current_page}, offset: {offset}, "
        f"games count: {len(games) if games else len(games_with_similarity)}, "
        f"total_count: {total_count}")

    context = {
        'games': games,
        'games_with_similarity': games_with_similarity,
        'page_obj': None,
        'is_paginated': False,
        'total_count': total_count,
        'total_pages': total_pages,
        'current_page': current_page,
        'start_index': start_index,
        'end_index': end_index,
        'items_per_page': items_per_page,
        'game_index_offset': offset,

        'find_similar': find_similar,
        'show_similarity': show_similarity,
        'source_game': source_game,
        'source_game_obj': source_game_obj,

        'similarity_map': mode_result.get('similarity_map', {}),

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
            'offset': offset,
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
            'total_pages': 0,
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
        selected_criteria['game_modes']
    ]

    return any(similarity_criteria)


def _get_selected_criteria_objects(selected_criteria: Dict[str, List[int]]) -> Dict[str, List]:
    """Получаем объекты для всех выбранных критериев."""
    selected_objects = {}

    if selected_criteria['genres']:
        selected_objects['genres'] = list(Genre.objects.filter(
            id__in=selected_criteria['genres']
        ).only('id', 'name'))

    if selected_criteria['keywords']:
        selected_objects['keywords'] = list(Keyword.objects.filter(
            id__in=selected_criteria['keywords']
        ).only('id', 'name'))

    if selected_criteria['platforms']:
        selected_objects['platforms'] = list(Platform.objects.filter(
            id__in=selected_criteria['platforms']
        ).only('id', 'name'))

    if selected_criteria['themes']:
        selected_objects['themes'] = list(Theme.objects.filter(
            id__in=selected_criteria['themes']
        ).only('id', 'name'))

    if selected_criteria['perspectives']:
        selected_objects['perspectives'] = list(PlayerPerspective.objects.filter(
            id__in=selected_criteria['perspectives']
        ).only('id', 'name'))

    if selected_criteria['game_modes']:
        selected_objects['game_modes'] = list(GameMode.objects.filter(
            id__in=selected_criteria['game_modes']
        ).only('id', 'name'))

    return selected_objects


def _get_all_games_mode(selected_criteria: Dict[str, List[int]], sort_field: str, page_number: str) -> Dict[str, Any]:
    """Режим отображения ВСЕХ игр с фильтрами для клиентской пагинации."""
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

    all_games = list(games_qs[:500])
    total_count = min(games_qs.count(), 500)

    return {
        'games': all_games,
        'page_obj': None,
        'paginator': None,
        'is_paginated': False,
        'total_count': total_count,
        'show_similarity': False,
        'find_similar': False,
        'source_game': None,
    }


def _get_similar_games_mode(params: Dict[str, str], selected_criteria: Dict[str, List[int]],
                            source_game_obj: Optional[Game]) -> Dict[str, Any]:
    """Режим похожих игр с поддержкой поиска без жанров для клиентской пагинации."""
    current_sort = params.get('sort', '-similarity')
    page_number = params.get('page', '1')

    logger.info(f"Режим похожих игр запущен. Критерии: "
                f"жанры={len(selected_criteria['genres'])}, "
                f"ключевые слова={len(selected_criteria['keywords'])}, "
                f"темы={len(selected_criteria['themes'])}, "
                f"перспективы={len(selected_criteria['perspectives'])}, "
                f"режимы игры={len(selected_criteria['game_modes'])}")

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

    logger.info(f"Найдено {len(similar_games_data)} игр до форматирования")

    games_with_similarity = _format_similar_games_data(similar_games_data, limit=500)

    logger.info(f"После форматирования: {len(games_with_similarity)} игр")

    _sort_similar_games(games_with_similarity, current_sort)

    return {
        'games_with_similarity': games_with_similarity,
        'page_obj': None,
        'paginator': None,
        'is_paginated': False,
        'total_count': len(games_with_similarity),
        'show_similarity': True,
        'find_similar': True,
        'source_game': source_game,
        'source_game_obj': source_game_obj,
    }


def get_similar_games_for_criteria(selected_criteria: Dict[str, List[int]]) -> Tuple[List, int]:
    """Get similar games for criteria - с поддержкой поиска без жанров."""
    import json
    import hashlib

    cache_data = json.dumps({
        'g': selected_criteria['genres'],
        'k': selected_criteria['keywords'],
        't': selected_criteria['themes'],
        'pp': selected_criteria['perspectives'],
        'd': selected_criteria['developers'],
        'gm': selected_criteria['game_modes'],
        'version': 'v16_clientside_pagination'
    }, sort_keys=True)

    cache_key = f'virtual_search_full_{hashlib.md5(cache_data.encode()).hexdigest()}'
    cached_data = cache.get(cache_key)

    if cached_data:
        logger.debug(f"Cache HIT for criteria search: {len(cached_data['games'])} games")
        return cached_data['games'], cached_data['count']

    start_time = time.time()

    virtual_game = VirtualGame(
        genre_ids=selected_criteria['genres'],
        keyword_ids=selected_criteria['keywords'],
        theme_ids=selected_criteria['themes'],
        perspective_ids=selected_criteria['perspectives'],
        developer_ids=selected_criteria['developers'],
        game_mode_ids=selected_criteria['game_modes'],
    )

    similarity_engine = GameSimilarity()

    min_similarity = 10

    has_genres = bool(selected_criteria['genres'])
    has_keywords = bool(selected_criteria['keywords'])
    has_themes = bool(selected_criteria['themes'])
    has_perspectives = bool(selected_criteria['perspectives'])
    has_game_modes = bool(selected_criteria['game_modes'])

    logger.info(f"Поиск похожих игр по критериям: "
                f"жанры={has_genres}({len(selected_criteria['genres'])}), "
                f"ключевые слова={has_keywords}({len(selected_criteria['keywords'])}), "
                f"темы={has_themes}({len(selected_criteria['themes'])}), "
                f"перспективы={has_perspectives}({len(selected_criteria['perspectives'])}), "
                f"режимы={has_game_modes}({len(selected_criteria['game_modes'])})")

    if has_game_modes and not has_genres and not has_keywords and not has_themes and not has_perspectives:
        min_similarity = 1
        logger.info(f"Только режимы игры, порог схожести: {min_similarity}%")

    elif not has_genres and any([has_keywords, has_themes, has_perspectives, has_game_modes]):
        min_similarity = 3
        logger.info(f"Поиск без жанров, порог схожести: {min_similarity}%")

    elif not any([has_genres, has_keywords, has_themes, has_perspectives, has_game_modes]):
        min_similarity = 0
        logger.info(f"Нет критериев похожести, порог схожести: {min_similarity}%")

    similar_games = similarity_engine.find_similar_games(
        source_game=virtual_game,
        min_similarity=min_similarity,
        limit=500
    )

    total_count = len(similar_games)

    cache_time = 10800
    if has_genres:
        cache_time = 7200

    cache.set(cache_key, {
        'games': similar_games,
        'count': total_count,
        'timestamp': time.time(),
        'min_similarity': min_similarity
    }, cache_time)

    criteria_count = sum(len(v) for key, v in selected_criteria.items()
                         if key not in ['release_years', 'release_year_start', 'release_year_end'])

    logger.info(f"Similar games search took: {time.time() - start_time:.2f}s, "
                f"criteria: {criteria_count}, "
                f"results: {total_count}, "
                f"genres_used: {has_genres}, "
                f"min_similarity: {min_similarity}")

    return similar_games, total_count


def get_similar_games_for_game(game_obj: Game, selected_platforms: List[int]) -> Tuple[List, int]:
    """Get similar games for a specific game without limits."""
    from .base_views import _generate_cache_key, _filter_by_platforms, _prefetch_similar_games, CACHE_TIMES
    import hashlib

    cache_key_data = {
        'game_id': game_obj.id,
        'platforms': sorted(selected_platforms) if selected_platforms else [],
        'version': 'v_clientside_pagination',
        'game_cached_counts': {
            'genres': game_obj.cached_genre_count,
            'keywords': game_obj.cached_keyword_count,
            'platforms': game_obj.cached_platform_count,
            'developers': game_obj.cached_developer_count,
        }
    }

    cache_key = f'similar_for_game_{_generate_cache_key(cache_key_data)}'
    cached_data = cache.get(cache_key)

    if cached_data:
        similar_games = cached_data['games']
        total_count = cached_data['count']
    else:
        similarity_engine = GameSimilarity()
        similar_games = similarity_engine.find_similar_games(
            source_game=game_obj,
            min_similarity=0,
            limit=500
        )
        total_count = len(similar_games)

        cache.set(cache_key, {
            'games': similar_games,
            'count': total_count,
            'timestamp': time.time()
        }, CACHE_TIMES['aggressive']['similar_for_game'])

    if selected_platforms:
        similar_games = _filter_by_platforms(similar_games, selected_platforms)
        total_count = len(similar_games)

    if similar_games:
        similar_games = _prefetch_similar_games(similar_games)

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
    '_get_all_games_mode_paginated',
    '_get_similar_games_mode_paginated',
    '_build_optimized_context',
    '_should_use_similar_mode',
    '_has_similarity_criteria',
    '_get_selected_criteria_objects',
    '_get_all_games_mode',
    '_get_similar_games_mode',
]