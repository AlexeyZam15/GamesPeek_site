"""Optimized views for game similarity search with updated models."""

# ===== STANDARD IMPORTS =====
import time
import json
import hashlib
import logging
from functools import lru_cache
from typing import Dict, List, Tuple, Any, Optional, Union
from urllib.parse import urlencode

# ===== DJANGO IMPORTS =====
from django.shortcuts import render, get_object_or_404
from django.db.models import Count, Prefetch, Q
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.core.cache import cache
from django.http import HttpRequest, HttpResponse, HttpResponseServerError
from django.db import models
from django.utils import timezone
from datetime import timedelta
from django.template.loader import render_to_string

# ===== LOCAL IMPORTS =====
from .similarity import GameSimilarity, VirtualGame
from .models import (
    Game, Genre, Keyword, KeywordCategory, Platform,
    Theme, PlayerPerspective, Company, Series, GameMode,
    GameTypeEnum
)
from .helpers import generate_compact_url_params

# ===== CONSTANTS =====
CACHE_TIMES = {
    'filter_data': 3600,
    'similar_games': 172800,  # 48 часов для похожих игр
    'filtered_games': 1800,
    'full_page': 900,
    'genres_list': 7200,
    'static_pages': 3600,
}

ITEMS_PER_PAGE = {
    'similar': 20,
    'regular': 20,
    'platform': 20,
}

# Pre-compiled empty results
_EMPTY_RESULT = {
    'genres': [],
    'keywords': [],
    'platforms': [],
    'themes': [],
    'perspectives': [],
    'developers': [],
    'game_modes': [],
}

# Добавьте настройки логов
logger = logging.getLogger('game_similarity')
logger.setLevel(logging.DEBUG)

# Или для консоли
import sys

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# ===== HELPER CLASSES =====

class SimpleSourceGame:
    """Simple wrapper for source game data."""

    def __init__(self, game_obj=None, criteria=None, display_name=None):
        if game_obj:
            self.id = game_obj.id
            self.name = game_obj.name
            self.display_name = display_name or game_obj.name
            self.is_game = True
        else:
            self.id = None
            self.name = display_name or "Search Criteria"
            self.display_name = display_name or "Search Criteria"
            self.is_game = False
        self.genres_ids = criteria['genres'] if criteria else []


# ===== CORE UTILITY FUNCTIONS =====

def get_objects_by_ids(model_class, ids: List[int], only_fields: List[str] = None) -> List:
    """Get model objects by their IDs efficiently."""
    if not ids:
        return []

    queryset = model_class.objects.filter(id__in=ids)
    if only_fields:
        queryset = queryset.only(*only_fields)

    # Сохраняем порядок как в списке IDs
    id_to_obj = {obj.id: obj for obj in queryset}
    return [id_to_obj[id] for id in ids if id in id_to_obj]


@lru_cache(maxsize=1024)
def _cached_string_to_int_list(param_str: str) -> List[int]:
    """Cached conversion of parameter strings to integer lists."""
    if not param_str:
        return []

    result = []
    for part in param_str.split(','):
        part = part.strip()
        if part and part.isdigit():
            result.append(int(part))
    return result


def convert_params_to_lists(params_dict: Dict[str, str]) -> Dict[str, List[int]]:
    """Convert query parameters to lists of integers."""
    # Quick check for empty params
    has_params = False
    for key in ['g', 'k', 'p', 't', 'pp', 'd', 'gm']:
        if params_dict.get(key):
            has_params = True
            break

    if not has_params:
        return _EMPTY_RESULT.copy()

    return {
        'genres': _cached_string_to_int_list(params_dict.get('g', '')),
        'keywords': _cached_string_to_int_list(params_dict.get('k', '')),
        'platforms': _cached_string_to_int_list(params_dict.get('p', '')),
        'themes': _cached_string_to_int_list(params_dict.get('t', '')),
        'perspectives': _cached_string_to_int_list(params_dict.get('pp', '')),
        'developers': _cached_string_to_int_list(params_dict.get('d', '')),
        'game_modes': _cached_string_to_int_list(params_dict.get('gm', '')),
    }


def _generate_cache_key(data: Dict) -> str:
    """Generate cache key from data."""
    cache_key_str = ''.join(f"{k}:{v}" for k, v in sorted(data.items()))
    return f"cache_{hashlib.md5(cache_key_str.encode()).hexdigest()}"


def extract_request_params(request: HttpRequest) -> Dict[str, str]:
    """Extract parameters from request efficiently."""
    get_params = request.GET
    return {
        'find_similar': get_params.get('find_similar', ''),
        'g': get_params.get('g', ''),
        'k': get_params.get('k', ''),
        'p': get_params.get('p', ''),
        't': get_params.get('t', ''),
        'pp': get_params.get('pp', ''),
        'd': get_params.get('d', ''),
        'gm': get_params.get('gm', ''),
        'source_game': get_params.get('source_game', ''),
        'sort': get_params.get('sort', ''),
        'page': get_params.get('page', '1'),
    }


# ===== FILTER DATA FUNCTIONS =====

@lru_cache(maxsize=1)
def get_filter_data() -> Dict[str, List]:
    """Get filter data with caching."""
    cache_key = 'game_list_filters_data_v3'
    filter_data = cache.get(cache_key)

    if filter_data:
        return filter_data

    filter_data = _fetch_filter_data_from_db()
    cache.set(cache_key, filter_data, CACHE_TIMES['filter_data'])
    return filter_data


def _fetch_filter_data_from_db() -> Dict[str, List]:
    """Fetch filter data from database with optimized queries."""
    platforms = Platform.objects.annotate(
        game_count=Count('game', distinct=True)  # ← добавил distinct=True
    ).filter(game_count__gt=0).order_by('-game_count', 'name')

    popular_keywords = Keyword.objects.filter(
        cached_usage_count__gt=0
    ).select_related('category').order_by('-cached_usage_count')

    game_modes = GameMode.objects.annotate(
        game_count=Count('game', distinct=True)  # ← добавил distinct=True
    ).filter(game_count__gt=0).order_by('name')

    themes = Theme.objects.annotate(
        game_count=Count('game', distinct=True)  # ← добавил distinct=True
    ).filter(game_count__gt=0).order_by('name')

    perspectives = PlayerPerspective.objects.annotate(
        game_count=Count('game', distinct=True)  # ← добавил distinct=True
    ).filter(game_count__gt=0).order_by('name')

    developers = Company.objects.annotate(
        developed_game_count=Count('developed_games', distinct=True)  # ← добавил distinct=True
    ).filter(developed_game_count__gt=0).order_by('name')

    return {
        'platforms': list(platforms),
        'popular_keywords': list(popular_keywords),
        'game_modes': list(game_modes),
        'themes': list(themes),
        'perspectives': list(perspectives),
        'developers': list(developers),
    }


# ===== SIMILARITY FUNCTIONS =====

def get_similar_games_for_game(game_obj: Game, selected_platforms: List[int]) -> Tuple[List, int]:
    """Get similar games for a specific game without limits."""
    cache_key_data = {
        'game_id': game_obj.id,
        'platforms': sorted(selected_platforms) if selected_platforms else [],
        'version': 'v_no_limits_prefetch',
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
            limit=None
        )
        total_count = len(similar_games)

        cache.set(cache_key, {
            'games': similar_games,
            'count': total_count,
            'timestamp': time.time()
        }, CACHE_TIMES['similar_games'])

    # Filter by platforms
    if selected_platforms:
        similar_games = _filter_by_platforms(similar_games, selected_platforms)
        total_count = len(similar_games)

    # Prefetch for remaining games
    if similar_games:
        similar_games = _prefetch_similar_games(similar_games)

    return similar_games, total_count


def _get_cached_similar_games(params: Dict[str, str], selected_criteria: Dict[str, List[int]]) -> Optional[
    Tuple[List, int]]:
    """Быстрая проверка кэша для похожих игр."""
    # Простой быстрый ключ
    import json
    cache_data = json.dumps({
        'find_similar': params.get('find_similar'),
        'source_game': params.get('source_game'),
        'g': selected_criteria['genres'],
        'k': selected_criteria['keywords'][:50],  # Проверяем по первым 50 keywords
        't': selected_criteria['themes'],
        'pp': selected_criteria['perspectives'],
        'gm': selected_criteria['game_modes'],
        'sort': params.get('sort'),
        'version': 'v9_quick_check'
    }, sort_keys=True)

    cache_key = f'quick_similar_{hashlib.md5(cache_data.encode()).hexdigest()}'
    return cache.get(cache_key)


def get_similar_games_for_criteria(selected_criteria: Dict[str, List[int]]) -> Tuple[List, int]:
    """Get similar games for criteria - OPTIMIZED for 1000+ results."""
    import json

    # Быстрый хеш для кэша
    cache_data = json.dumps({
        'g': selected_criteria['genres'],
        'k': selected_criteria['keywords'],
        't': selected_criteria['themes'],
        'pp': selected_criteria['perspectives'],
        'd': selected_criteria['developers'],
        'gm': selected_criteria['game_modes'],
        'version': 'v9_full_1000'
    }, sort_keys=True)

    cache_key = f'virtual_search_full_{hashlib.md5(cache_data.encode()).hexdigest()}'
    cached_data = cache.get(cache_key)

    if cached_data:
        logging.debug(f"Cache HIT for criteria search: {len(cached_data['games'])} games")
        return cached_data['games'], cached_data['count']

    start_time = time.time()

    virtual_game = VirtualGame(
        genre_ids=selected_criteria['genres'],
        keyword_ids=selected_criteria['keywords'],
        theme_ids=selected_criteria['themes'],
        perspective_ids=selected_criteria['perspectives'],
        developer_ids=selected_criteria['developers'],
        game_mode_ids=selected_criteria['game_modes']
    )

    similarity_engine = GameSimilarity()

    # УВЕЛИЧИВАЕМ до 1000 игр
    similar_games = similarity_engine.find_similar_games(
        source_game=virtual_game,
        min_similarity=10,  # Понижаем для большего количества результатов
        limit=1000  # УВЕЛИЧИЛИ ДО 1000
    )

    total_count = len(similar_games)

    # Кэшируем дольше для частых запросов
    cache_time = 7200  # 2 часа
    cache.set(cache_key, {
        'games': similar_games,
        'count': total_count,
        'timestamp': time.time()
    }, cache_time)

    logging.info(f"Similar games search took: {time.time() - start_time:.2f}s, "
                 f"criteria: {sum(len(v) for v in selected_criteria.values())}, "
                 f"results: {total_count}")

    return similar_games, total_count


def _filter_by_platforms(games_data: List, platform_ids: List[int]) -> List:
    """Filter games by platforms with optimization."""
    if not platform_ids or not games_data:
        return games_data

    filtered = []
    platform_ids_set = set(platform_ids)

    for item in games_data:
        game = item.get('game') if isinstance(item, dict) else item

        if hasattr(game, 'cached_platform_count') and game.cached_platform_count == 0:
            continue

        if hasattr(game, '_cached_platform_ids'):
            game_platform_ids = game._cached_platform_ids
        else:
            game_platform_ids = {p.id for p in game.platforms.all()}

        if platform_ids_set & game_platform_ids:
            filtered.append(item)

    return filtered


def _prefetch_similar_games(similar_games: List) -> List:
    """Prefetch related data for similar games."""
    game_ids = []
    for item in similar_games:
        game = item.get('game') if isinstance(item, dict) else item
        if hasattr(game, 'id'):
            game_ids.append(game.id)

    if not game_ids:
        return similar_games

    games_dict = {}
    games_with_prefetch = Game.objects.filter(id__in=game_ids).prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
        Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
        Prefetch('keywords', queryset=Keyword.objects.select_related('category').only(
            'id', 'name', 'category__id', 'category__name'
        )),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url', 'game_type'
    )

    for game in games_with_prefetch:
        games_dict[game.id] = game

    # Replace games in results with prefetched versions
    updated_games = []
    for item in similar_games:
        if isinstance(item, dict):
            game = item.get('game')
            if hasattr(game, 'id') and game.id in games_dict:
                item['game'] = games_dict[game.id]
            updated_games.append(item)
        elif hasattr(item, 'id') and item.id in games_dict:
            updated_games.append(games_dict[item.id])
        else:
            updated_games.append(item)

    return updated_games


# ===== VIEW HANDLERS =====

def should_find_similar(params: Dict[str, str], selected_criteria: Dict[str, List[int]]) -> bool:
    """Determine if similar games search should be performed."""
    if params.get('find_similar') == '1':
        return True

    # Проверяем только критерии похожести, исключая платформы и разработчиков
    similarity_criteria = [
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes']
    ]

    # Если есть хотя бы один критерий похожести
    return any(similarity_criteria)


def has_similarity_criteria(selected_criteria: Dict[str, List[int]]) -> bool:
    """Check if there are criteria for similarity search."""
    # Исключаем платформы и разработчиков из критериев похожести
    similarity_criteria = [
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes']
    ]

    return any(similarity_criteria)


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


def _format_similar_games_data(similar_games_data: List, limit: int = 500) -> List[Dict[str, Any]]:
    """Format similar games data - OPTIMIZED for large datasets."""
    if not similar_games_data:
        return []

    # Ограничиваем для начальной загрузки
    if len(similar_games_data) > limit:
        similar_games_data = similar_games_data[:limit]

    game_ids = []
    similarity_map = {}

    # Собираем ID игр и similarity
    for item in similar_games_data:
        if isinstance(item, dict):
            game = item.get('game')
            similarity = item.get('similarity', 0)
        else:
            game = item
            similarity = 0

        if hasattr(game, 'id'):
            game_ids.append(game.id)
            similarity_map[game.id] = similarity

    games_dict = {}
    if game_ids:
        # ЗАГРУЖАЕМ ВСЕ 1000 игр одним запросом
        games_with_prefetch = Game.objects.filter(id__in=game_ids).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url'
        )

        for game in games_with_prefetch:
            games_dict[game.id] = game

    # Форматируем
    formatted = []
    for item in similar_games_data:
        if isinstance(item, dict):
            game = item.get('game')
            similarity = item.get('similarity', 0)
        else:
            game = item
            similarity = similarity_map.get(getattr(game, 'id', None), 0)

        if hasattr(game, 'id') and game.id in games_dict:
            game = games_dict[game.id]

        formatted.append({
            'game': game,
            'similarity': similarity,
        })

    return formatted


def _sort_similar_games(games_with_similarity: List[Dict[str, Any]], current_sort: str) -> None:
    """Sort similar games."""
    if current_sort == '-rating_count':
        games_with_similarity.sort(key=lambda x: x['game'].rating_count or 0, reverse=True)
    elif current_sort == '-rating':
        games_with_similarity.sort(key=lambda x: x['game'].rating or 0, reverse=True)
    elif current_sort == 'name':
        games_with_similarity.sort(key=lambda x: x['game'].name.lower())
    elif current_sort == '-name':
        games_with_similarity.sort(key=lambda x: x['game'].name.lower(), reverse=True)
    elif current_sort == '-first_release_date':
        games_with_similarity.sort(
            key=lambda x: x['game'].first_release_date or '',
            reverse=True
        )
    else:  # Default to similarity
        games_with_similarity.sort(key=lambda x: x['similarity'], reverse=True)


def _paginate_results(data: List, page_number: str, items_per_page: int) -> Tuple:
    """Paginate data - OPTIMIZED for large lists."""
    if not data:
        empty_paginator = Paginator([], items_per_page)
        return empty_paginator.page(1), empty_paginator, False

    # Используем оптимизированный пагинатор для больших списков
    from django.core.paginator import Paginator

    paginator = Paginator(data, items_per_page)

    try:
        page_obj = paginator.page(int(page_number))
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    return page_obj, paginator, paginator.num_pages > 1


def _create_filter_cache_key(selected_criteria: Dict[str, List[int]], sort_field: str) -> str:
    """Create cache key for filters."""
    parts = []

    for key, value in selected_criteria.items():
        if value:
            parts.append(f"{key}_{'-'.join(map(str, sorted(value)))}")

    parts.append(f"sort_{sort_field}")
    parts.append(f"version_v2")

    return f'filtered_games_{"_".join(parts)}' if parts else 'filtered_games_all'


def _apply_filters(queryset: models.QuerySet, selected_criteria: Dict[str, List[int]]) -> models.QuerySet:
    """Apply filters to queryset with OR logic for platforms."""
    # Основной фильтр для всех полей кроме платформ
    main_filters = Q()
    has_main_filters = False

    # Фильтр для платформ (OR логика)
    platform_filter = Q()
    has_platform_filter = False

    # Обрабатываем платформы отдельно
    if selected_criteria['platforms']:
        platform_filter = Q(platforms__id__in=selected_criteria['platforms'])
        has_platform_filter = True

    # Обрабатываем остальные поля (AND логика)
    other_fields = [
        ('genres', 'genres__id__in'),
        ('keywords', 'keywords__id__in'),
        ('themes', 'themes__id__in'),
        ('perspectives', 'player_perspectives__id__in'),
        ('developers', 'developers__id__in'),
        ('game_modes', 'game_modes__id__in')
    ]

    for field, model_field in other_fields:
        if selected_criteria[field]:
            main_filters &= Q(**{model_field: selected_criteria[field]})
            has_main_filters = True

    # Применяем фильтры
    if has_platform_filter and has_main_filters:
        # Платформы OR + остальные AND
        queryset = queryset.filter(platform_filter & main_filters).distinct()
    elif has_platform_filter:
        # Только платформы (OR)
        queryset = queryset.filter(platform_filter).distinct()
    elif has_main_filters:
        # Только остальные фильтры (AND)
        queryset = queryset.filter(main_filters).distinct()

    return queryset


def _get_filtered_games(selected_criteria: Dict[str, List[int]], sort_field: str) -> Tuple[List, int]:
    """Get filtered games with caching."""
    cache_key = _create_filter_cache_key(selected_criteria, sort_field)
    cached_data = cache.get(cache_key)

    if cached_data and 'game_ids' in cached_data:
        game_ids = cached_data['game_ids']
        total_count = cached_data['count']

        games = Game.objects.filter(id__in=game_ids).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            Prefetch('keywords', queryset=Keyword.objects.select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            )),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        )

        # Sort in Python for cached data
        if sort_field == '-rating_count':
            games = sorted(games, key=lambda x: x.rating_count or 0, reverse=True)
        elif sort_field == '-rating':
            games = sorted(games, key=lambda x: x.rating or 0, reverse=True)
        elif sort_field == '-first_release_date':
            games = sorted(games, key=lambda x: x.first_release_date or '', reverse=True)
        elif sort_field == 'name':
            games = sorted(games, key=lambda x: x.name.lower())
        elif sort_field == '-name':
            games = sorted(games, key=lambda x: x.name.lower(), reverse=True)
    else:
        games = Game.objects.all().prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            Prefetch('keywords', queryset=Keyword.objects.select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            )),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        )

        games = _apply_filters(games, selected_criteria)

        if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
            games = games.order_by(sort_field)
        else:
            games = games.order_by('-rating_count')

        total_count = games.count()
        game_ids = list(games.values_list('id', flat=True)[:200])

        cache.set(cache_key, {
            'game_ids': game_ids,
            'count': total_count,
            'sort_field': sort_field,
            'timestamp': time.time()
        }, CACHE_TIMES['filtered_games'])

        games = list(games[:200])

    return games, total_count


def _build_context(mode: str, **kwargs) -> Dict[str, Any]:
    """Build template context."""
    # Get genres from cache
    genres_list = cache.get('genres_list')
    if not genres_list:
        genres_list = list(Genre.objects.all().only('id', 'name').order_by('name'))
        cache.set('genres_list', genres_list, CACHE_TIMES['genres_list'])

    # Generate URL parameters
    selected_criteria = kwargs['selected_criteria']
    compact_url_params = generate_compact_url_params(
        find_similar=(mode == 'similar'),
        genres=selected_criteria['genres'],
        keywords=selected_criteria['keywords'],
        platforms=selected_criteria['platforms'],
        themes=selected_criteria['themes'],
        perspectives=selected_criteria['perspectives'],
        developers=selected_criteria['developers'],
        game_modes=selected_criteria['game_modes'],
        sort=kwargs.get('current_sort', '')
    )

    # Get filter data
    filter_data = kwargs.get('filter_data', {})

    # Get selected criteria objects if available
    selected_criteria_objects = kwargs.get('selected_criteria_objects', {})

    # Build context
    context = {
        'genres': genres_list,
        'keyword_categories': list(KeywordCategory.objects.all().only('id', 'name')),
        'current_sort': kwargs.get('current_sort', ''),
        'find_similar': kwargs.get('find_similar', False),
        'compact_url_params': compact_url_params,

        # Selected criteria IDs
        'selected_genres': selected_criteria['genres'],
        'selected_keywords': selected_criteria['keywords'],
        'selected_platforms': selected_criteria['platforms'],
        'selected_themes': selected_criteria['themes'],
        'selected_perspectives': selected_criteria['perspectives'],
        'selected_developers': selected_criteria['developers'],
        'selected_game_modes': selected_criteria['game_modes'],

        # Selected criteria OBJECTS (для отображения бейджей)
        'selected_genres_objects': selected_criteria_objects.get('genres', []),
        'selected_keywords_objects': selected_criteria_objects.get('keywords', []),
        'selected_platforms_objects': selected_criteria_objects.get('platforms', []),
        'selected_themes_objects': selected_criteria_objects.get('themes', []),
        'selected_perspectives_objects': selected_criteria_objects.get('perspectives', []),
        'selected_developers_objects': selected_criteria_objects.get('developers', []),
        'selected_game_modes_objects': selected_criteria_objects.get('game_modes', []),

        # Filter data
        'popular_keywords': filter_data.get('popular_keywords', []),
        'platforms': filter_data.get('platforms', []),
        'themes': filter_data.get('themes', []),
        'perspectives': filter_data.get('perspectives', []),
        'developers': filter_data.get('developers', []),
        'game_modes': filter_data.get('game_modes', []),

        # Pagination
        'page_obj': kwargs.get('page_obj'),
        'paginator': kwargs.get('paginator'),
        'is_paginated': kwargs.get('is_paginated', False),
        'total_count': kwargs.get('total_count', 0),

        # Source
        'source_game': kwargs.get('source_game'),
        'source_game_obj': kwargs.get('source_game_obj'),
        'selected_criteria': selected_criteria,

        # Mode specific
        'debug_mode': mode,
    }

    # Add mode-specific fields
    if mode == 'similar':
        context.update({
            'games_with_similarity': kwargs['page_obj'].object_list if kwargs.get('page_obj') else [],
            'games': [],
            'show_similarity': True,
        })
    else:
        context.update({
            'games': kwargs['page_obj'].object_list if kwargs.get('page_obj') else [],
            'games_with_similarity': [],
            'show_similarity': False,
        })

    return context


def handle_similar_games_mode(
        request: HttpRequest,
        params: Dict[str, str],
        selected_criteria: Dict[str, List[int]],
        source_game_obj: Optional[Game],
        filter_data: Dict[str, List]
) -> Dict[str, Any]:
    """Handle similar games mode."""
    current_sort = params.get('sort', '-similarity')
    page_number = params.get('page', 1)

    # Get similar games
    if source_game_obj:
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, selected_criteria['platforms']
        )
        source_display = source_game_obj.name
    else:
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)
        source_display = "Search Criteria"

    # Format data
    games_with_similarity = _format_similar_games_data(similar_games_data)

    # Sort
    _sort_similar_games(games_with_similarity, current_sort)

    # Paginate - ИСПРАВЛЕНО: 16 игр на страницу
    page_obj, paginator, is_paginated = _paginate_results(
        games_with_similarity, page_number, 16  # ← БЫЛО: ITEMS_PER_PAGE['similar']
    )

    # Create source game object
    source_game = SimpleSourceGame(source_game_obj, selected_criteria, source_display)

    # Получаем объекты для отображения активных бейджей
    selected_criteria_objects = {}

    # Жанры
    if selected_criteria['genres']:
        selected_criteria_objects['genres'] = get_objects_by_ids(
            Genre, selected_criteria['genres'], ['id', 'name']
        )

    # Ключевые слова
    if selected_criteria['keywords']:
        selected_criteria_objects['keywords'] = get_objects_by_ids(
            Keyword, selected_criteria['keywords'], ['id', 'name']
        )

    # Платформы
    if selected_criteria['platforms']:
        selected_criteria_objects['platforms'] = get_objects_by_ids(
            Platform, selected_criteria['platforms'], ['id', 'name', 'display_name']
        )

    # Темы
    if selected_criteria['themes']:
        selected_criteria_objects['themes'] = get_objects_by_ids(
            Theme, selected_criteria['themes'], ['id', 'name']
        )

    # Перспективы
    if selected_criteria['perspectives']:
        selected_criteria_objects['perspectives'] = get_objects_by_ids(
            PlayerPerspective, selected_criteria['perspectives'], ['id', 'name']
        )

    # Режимы игры
    if selected_criteria['game_modes']:
        selected_criteria_objects['game_modes'] = get_objects_by_ids(
            GameMode, selected_criteria['game_modes'], ['id', 'name']
        )

    # Build context using _build_context
    context = _build_context(
        mode='similar',
        page_obj=page_obj,
        paginator=paginator,
        is_paginated=is_paginated,
        total_count=total_count,
        selected_criteria=selected_criteria,
        filter_data=filter_data,
        params=params,
        source_game=source_game,
        source_game_obj=source_game_obj,
        current_sort=current_sort,
        find_similar=True,
        selected_criteria_objects=selected_criteria_objects  # Добавляем объекты
    )

    return context


def handle_regular_mode(
        request: HttpRequest,
        params: Dict[str, str],
        selected_criteria: Dict[str, List[int]],
        filter_data: Dict[str, List]
) -> Dict[str, Any]:
    """Handle regular filtering mode."""
    current_sort = params.get('sort', '-rating_count')
    page_number = params.get('page', 1)

    # Get filtered games
    games, total_count = _get_filtered_games(selected_criteria, current_sort)

    # Paginate
    page_obj, paginator, is_paginated = _paginate_results(
        list(games), page_number, ITEMS_PER_PAGE['regular']
    )

    # Получаем объекты для отображения активных бейджей и чекбоксов
    selected_criteria_objects = {}

    # Жанры
    if selected_criteria['genres']:
        selected_criteria_objects['genres'] = get_objects_by_ids(
            Genre, selected_criteria['genres'], ['id', 'name']
        )

    # Ключевые слова
    if selected_criteria['keywords']:
        selected_criteria_objects['keywords'] = get_objects_by_ids(
            Keyword, selected_criteria['keywords'], ['id', 'name']
        )

    # Платформы
    if selected_criteria['platforms']:
        selected_criteria_objects['platforms'] = get_objects_by_ids(
            Platform, selected_criteria['platforms'], ['id', 'name', 'display_name']
        )

    # Темы
    if selected_criteria['themes']:
        selected_criteria_objects['themes'] = get_objects_by_ids(
            Theme, selected_criteria['themes'], ['id', 'name']
        )

    # Перспективы
    if selected_criteria['perspectives']:
        selected_criteria_objects['perspectives'] = get_objects_by_ids(
            PlayerPerspective, selected_criteria['perspectives'], ['id', 'name']
        )

    # Режимы игры
    if selected_criteria['game_modes']:
        selected_criteria_objects['game_modes'] = get_objects_by_ids(
            GameMode, selected_criteria['game_modes'], ['id', 'name']
        )

    # Build context
    return _build_context(
        mode='regular',
        page_obj=page_obj,
        paginator=paginator,
        is_paginated=is_paginated,
        total_count=total_count,
        selected_criteria=selected_criteria,
        filter_data=filter_data,
        params=params,
        source_game=None,
        source_game_obj=None,
        current_sort=current_sort,
        find_similar=False,
        selected_criteria_objects=selected_criteria_objects  # Добавляем объекты
    )


# ===== MAIN VIEWS =====
def _get_all_games_mode(selected_criteria: Dict[str, List[int]], sort_field: str, page_number: str) -> Dict[str, Any]:
    """Режим отображения ВСЕХ игр с фильтрами."""
    # Оптимизированный запрос для всех игр
    games_qs = Game.objects.all().prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
        Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url'
    )

    # Применяем фильтры если есть
    if any(selected_criteria.values()):
        games_qs = _apply_filters(games_qs, selected_criteria)

    # Сортировка
    if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
        games_qs = games_qs.order_by(sort_field)
    else:
        games_qs = games_qs.order_by('-rating_count')

    # Пагинация
    paginator = Paginator(games_qs, ITEMS_PER_PAGE['regular'])

    try:
        page_obj = paginator.page(int(page_number))
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    return {
        'games': page_obj.object_list,
        'page_obj': page_obj,
        'paginator': paginator,
        'is_paginated': paginator.num_pages > 1,
        'total_count': paginator.count,
        'show_similarity': False,
        'find_similar': False,
    }


def _get_similar_games_mode(params: Dict[str, str], selected_criteria: Dict[str, List[int]],
                            source_game_obj: Optional[Game]) -> Dict[str, Any]:
    """Режим похожих игр."""
    current_sort = params.get('sort', '-similarity')
    page_number = params.get('page', '1')

    # Получаем похожие игры
    if source_game_obj:
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, selected_criteria['platforms']
        )
    else:
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)

    # Форматируем
    games_with_similarity = _format_similar_games_data(similar_games_data)
    _sort_similar_games(games_with_similarity, current_sort)

    # Пагинация - ИСПРАВЛЕНО: 16 игр на страницу
    page_obj, paginator, is_paginated = _paginate_results(
        games_with_similarity, page_number, 16  # ← БЫЛО: ITEMS_PER_PAGE['similar']
    )

    # Создаем source game объект
    source_game = SimpleSourceGame(
        source_game_obj,
        selected_criteria,
        source_game_obj.name if source_game_obj else "Search Criteria"
    )

    return {
        'games_with_similarity': page_obj.object_list,
        'page_obj': page_obj,
        'paginator': paginator,
        'is_paginated': is_paginated,
        'total_count': total_count,
        'show_similarity': True,
        'find_similar': True,
        'source_game': source_game,
        'source_game_obj': source_game_obj,
    }

def _get_cached_filter_data() -> Dict[str, List]:
    """Получаем кэшированные данные для фильтров."""
    filter_data = cache.get('optimized_filter_data_v3')

    if not filter_data:
        filter_data = {
            'platforms': list(Platform.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name', 'slug')
                              .order_by('-game_count', 'name')),

            'popular_keywords': list(Keyword.objects.filter(
                cached_usage_count__gt=0
            ).select_related('category').only(
                'id', 'name', 'category__id', 'category__name', 'cached_usage_count'
            ).order_by('-cached_usage_count')[:50]),

            'game_modes': list(GameMode.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name').order_by('name')),

            'themes': list(Theme.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name').order_by('name')),

            'perspectives': list(PlayerPerspective.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name').order_by('name')),

            'developers': list(Company.objects.annotate(
                developed_game_count=Count('developed_games', distinct=True)
            ).filter(developed_game_count__gt=0).only('id', 'name').order_by('name')),
        }
        cache.set('optimized_filter_data_v3', filter_data, 7200)

    return filter_data


def _get_selected_criteria_objects(selected_criteria: Dict[str, List[int]]) -> Dict[str, List]:
    """Получаем объекты для выбранных критериев."""
    selected_objects = {}

    # Список полей для загрузки
    fields_to_load = [
        ('genres', Genre, ['id', 'name']),
        ('keywords', Keyword, ['id', 'name']),
        ('platforms', Platform, ['id', 'name', 'slug']),
        ('themes', Theme, ['id', 'name']),
        ('perspectives', PlayerPerspective, ['id', 'name']),
        ('game_modes', GameMode, ['id', 'name']),
        ('developers', Company, ['id', 'name']),
    ]

    for field_name, model_class, fields in fields_to_load:
        if selected_criteria[field_name]:
            selected_objects[field_name] = list(
                model_class.objects.filter(id__in=selected_criteria[field_name])
                .only(*fields)
            )

    return selected_objects


def _get_optimized_filtered_games(selected_criteria: Dict[str, List[int]], sort_field: str) -> Tuple[List, int]:
    """Get filtered games with maximum optimization."""
    # Кэшируем ID игр
    cache_key_data = {
        'g': sorted(selected_criteria['genres']),
        'p': sorted(selected_criteria['platforms']),
        't': sorted(selected_criteria['themes']),
        'pp': sorted(selected_criteria['perspectives']),
        'd': sorted(selected_criteria['developers']),
        'gm': sorted(selected_criteria['game_modes']),
        'sort': sort_field,
        'version': 'v3_optimized'
    }

    cache_key = f'filtered_ids_{hashlib.md5(str(cache_key_data).encode()).hexdigest()}'
    cached = cache.get(cache_key)

    if cached:
        game_ids = cached['ids']
        total_count = cached['count']

        # Загружаем игры с правильным prefetch
        games = Game.objects.filter(id__in=game_ids).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url'
        )

        # Сортируем в Python по тому же принципу
        sort_map = {
            '-rating_count': lambda x: x.rating_count or 0,
            '-rating': lambda x: x.rating or 0,
            'name': lambda x: x.name.lower(),
            '-name': lambda x: x.name.lower(),
            '-first_release_date': lambda x: x.first_release_date or '',
        }

        if sort_field in sort_map:
            games = sorted(games, key=sort_map[sort_field], reverse=sort_field.startswith('-'))

        return list(games), total_count

    # Если нет в кэше - делаем запрос
    games = Game.objects.all().prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
        Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url'
    )

    # Применяем фильтры
    games = _apply_filters(games, selected_criteria)

    # Сортируем
    if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
        games = games.order_by(sort_field)
    else:
        games = games.order_by('-rating_count')

    total_count = games.count()
    game_ids = list(games.values_list('id', flat=True)[:200])  # Лимит для производительности

    # Кэшируем
    cache.set(cache_key, {
        'ids': game_ids,
        'count': total_count,
        'timestamp': time.time()
    }, 1800)  # 30 минут

    return list(games[:200]), total_count


def game_list(request: HttpRequest) -> HttpResponse:
    """Main game list function - с приоритетом на кэширование."""
    # 1. СУПЕР БЫСТРЫЙ КЭШ - проверяем даже без хеширования
    cache_key_simple = f'game_list_{request.GET.urlencode()}'
    cached_response = cache.get(cache_key_simple)

    if cached_response:
        cached_response['X-Cache-Hit'] = 'True-Simple'
        return cached_response

    start_time = time.time()

    # 2. Быстрая обработка параметров
    params = extract_request_params(request)

    # 3. Если очень много параметров - используем агрессивный кэш
    total_params = sum(len(str(v)) for v in params.values() if v)
    if total_params > 1000:  # Очень длинный URL
        # Специальный быстрый ключ для длинных запросов
        cache_key_long = f'game_list_long_{hashlib.md5(request.GET.urlencode()[:500].encode()).hexdigest()}'
        cached_long = cache.get(cache_key_long)

        if cached_long:
            cached_long['X-Cache-Hit'] = 'True-Long'
            cached_long['X-Params-Length'] = str(total_params)
            return cached_long

    selected_criteria = convert_params_to_lists(params)

    # 4. Определяем режим
    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = Game.objects.only('id', 'name').get(pk=int(params['source_game']))
        except (Game.DoesNotExist, ValueError):
            pass

    # 5. ВЫБОР РЕЖИМА с приоритетом на кэширование
    if find_similar or source_game_obj or has_similarity_criteria(selected_criteria):
        mode_result = _get_similar_games_mode(params, selected_criteria, source_game_obj)
        mode = 'similar'
    else:
        mode_result = _get_all_games_mode(
            selected_criteria,
            params.get('sort', '-rating_count'),
            params.get('page', '1')
        )
        mode = 'regular'

    # 6. БЫСТРАЯ загрузка фильтров из кэша
    filter_data = _get_cached_filter_data()
    genres_list = cache.get('genres_list_v3') or list(Genre.objects.only('id', 'name').order_by('name'))

    # 7. Минимальный контекст для скорости
    context = {
        'games': mode_result.get('games', []),
        'games_with_similarity': mode_result.get('games_with_similarity', []),
        'page_obj': mode_result.get('page_obj'),
        'is_paginated': mode_result.get('is_paginated', False),
        'total_count': mode_result.get('total_count', 0),

        'find_similar': mode_result.get('find_similar', False),
        'show_similarity': mode_result.get('show_similarity', False),
        'source_game': mode_result.get('source_game'),

        'genres': genres_list,
        'platforms': filter_data['platforms'],
        'selected_genres': selected_criteria['genres'],
        'selected_platforms': selected_criteria['platforms'],
        'current_sort': params.get('sort', ''),

        'execution_time': round(time.time() - start_time, 3),
    }

    # 8. Рендерим
    response = render(request, 'games/game_list.html', context)

    # 9. АГРЕССИВНОЕ КЭШИРОВАНИЕ для медленных запросов
    cache_time = 300  # 5 минут по умолчанию

    if context['execution_time'] > 1.0:
        # Медленные запросы кэшируем дольше
        cache_time = 600  # 10 минут
        response['X-Cache-Reason'] = 'Slow-Query'

    cache.set(cache_key_simple, response, cache_time)

    if total_params > 1000:
        # Длинные запросы тоже кэшируем
        cache.set(cache_key_long, response, cache_time)

    response['X-Cache-Hit'] = 'False'
    response['X-Response-Time'] = f"{context['execution_time']:.3f}s"
    response['X-Mode'] = mode

    return response


def game_detail(request: HttpRequest, pk: int) -> HttpResponse:
    """Game detail page with optimized queries."""
    prefetch_keywords = Prefetch('keywords', queryset=Keyword.objects.select_related('category'))

    game = get_object_or_404(
        Game.objects.prefetch_related(
            prefetch_keywords,
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
            Prefetch('developers', queryset=Company.objects.only('id', 'name')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
            Prefetch('publishers', queryset=Company.objects.only('id', 'name')),
        ),
        pk=pk
    )

    return render(request, 'games/game_detail.html', {'game': game})


def game_comparison(request: HttpRequest, pk2: int) -> HttpResponse:
    """Universal comparison: game-game or criteria-game."""
    try:
        # Get second game
        keyword_prefetch = Prefetch('keywords', queryset=Keyword.objects.select_related('category'))

        game2 = get_object_or_404(
            Game.objects.prefetch_related(
                keyword_prefetch,
                Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
                Prefetch('platforms', queryset=Platform.objects.only('id', 'name')),
                Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
                Prefetch('developers', queryset=Company.objects.only('id', 'name')),
                Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
                Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
            ),
            pk=pk2
        )

        # Get source game
        source_game_id = request.GET.get('source_game')
        game1 = None

        if source_game_id and source_game_id.strip() and source_game_id.strip().lower() != 'none':
            try:
                game1 = Game.objects.only(
                    'id', 'name', 'game_type'
                ).prefetch_related(
                    'genres', 'keywords', 'themes',
                    'developers', 'player_perspectives', 'game_modes'
                ).get(pk=int(source_game_id))
            except (Game.DoesNotExist, ValueError):
                game1 = None

        # Convert parameters
        selected_criteria = convert_params_to_lists(request.GET)

        # Получаем объекты для отображения критериев на карточке
        criteria_genres_objs = Genre.objects.filter(id__in=selected_criteria['genres']).only('id', 'name') if \
            selected_criteria['genres'] else []
        criteria_keywords_objs = Keyword.objects.filter(id__in=selected_criteria['keywords']).only('id', 'name') if \
            selected_criteria['keywords'] else []
        criteria_themes_objs = Theme.objects.filter(id__in=selected_criteria['themes']).only('id', 'name') if \
            selected_criteria['themes'] else []
        criteria_perspectives_objs = PlayerPerspective.objects.filter(id__in=selected_criteria['perspectives']).only(
            'id', 'name') if selected_criteria['perspectives'] else []
        criteria_developers_objs = Company.objects.filter(id__in=selected_criteria['developers']).only('id', 'name') if \
            selected_criteria['developers'] else []
        criteria_game_modes_objs = GameMode.objects.filter(id__in=selected_criteria['game_modes']).only('id', 'name') if \
            selected_criteria['game_modes'] else []

        # Determine comparison type
        if game1:
            # Game-game comparison
            is_criteria_comparison = False
            source = game1
        else:
            # Always criteria comparison if no source game
            is_criteria_comparison = True
            source = VirtualGame(
                genre_ids=selected_criteria['genres'],
                keyword_ids=selected_criteria['keywords'],
                theme_ids=selected_criteria['themes'],
                perspective_ids=selected_criteria['perspectives'],
                developer_ids=selected_criteria['developers'],
                game_mode_ids=selected_criteria['game_modes']
            )

        # Calculate similarity
        similarity_engine = GameSimilarity()
        similarity_score = similarity_engine.calculate_similarity(source, game2)
        breakdown = similarity_engine.get_similarity_breakdown(source, game2)

        # Calculate shared items
        shared_items = {}
        fields_to_compare = ['genres', 'keywords', 'themes', 'perspectives', 'developers', 'game_modes']

        if is_criteria_comparison:
            # Criteria vs Game
            for field in fields_to_compare:
                # Get game field
                if field == 'perspectives':
                    game_field = game2.player_perspectives.all()
                else:
                    game_field = getattr(game2, field).all()

                # Get criteria
                criteria_ids = selected_criteria[field]
                if criteria_ids:
                    model_map = {
                        'genres': Genre,
                        'keywords': Keyword,
                        'themes': Theme,
                        'perspectives': PlayerPerspective,
                        'developers': Company,
                        'game_modes': GameMode
                    }
                    model = model_map[field]
                    criteria_objects = model.objects.filter(id__in=criteria_ids)
                    shared_items[field] = list(game_field & criteria_objects)
                else:
                    shared_items[field] = []
        else:
            # Game vs Game
            for field in fields_to_compare:
                if field == 'perspectives':
                    field1 = game1.player_perspectives.all()
                    field2 = game2.player_perspectives.all()
                else:
                    field1 = getattr(game1, field).all()
                    field2 = getattr(game2, field).all()

                shared_items[field] = list(field1 & field2)

        # Создаем виртуальную игру для тегов
        class CriteriaVirtualGame:
            """Обертка для критериев поиска, чтобы использовать с тегами."""

            def __init__(self, criteria_dict):
                self.id = None  # Нет реального ID
                self.genres = criteria_dict['genres']
                self.keywords = criteria_dict['keywords']
                self.themes = criteria_dict['themes']
                self.player_perspectives = criteria_dict['perspectives']
                self.developers = criteria_dict['developers']
                self.game_modes = criteria_dict['game_modes']

            # Методы для совместимости с тегами
            @property
            def all(self):
                return []

            def get(self, *args, **kwargs):
                return None

            # Методы, которые могут вызываться в тегах
            def genres_list(self):
                """Возвращает список ID жанров для тегов."""
                return self.genres

            def keywords_list(self):
                """Возвращает список ID ключевых слов для тегов."""
                return self.keywords

            def themes_list(self):
                """Возвращает список ID тем для тегов."""
                return self.themes

            def perspectives_list(self):
                """Возвращает список ID перспектив для тегов."""
                return self.player_perspectives

            def developers_list(self):
                """Возвращает список ID разработчиков для тегов."""
                return self.developers

            def game_modes_list(self):
                """Возвращает список ID режимов игры для тегов."""
                return self.game_modes

        # Создаем виртуальную игру для контекста
        if is_criteria_comparison:
            criteria_virtual_game = CriteriaVirtualGame(selected_criteria)
        else:
            # Для сравнения игра-игра используем game1
            criteria_virtual_game = game1

        # Prepare context
        context = {
            'game1': game1,
            'game2': game2,
            'similarity_score': similarity_score,
            'is_criteria_comparison': is_criteria_comparison,
            'breakdown': breakdown,
            'selected_criteria': selected_criteria,
            'selected_criteria_count': sum(len(v) for v in selected_criteria.values()),

            # Критерии для отображения на карточке
            'criteria_genres': list(criteria_genres_objs),
            'criteria_keywords': list(criteria_keywords_objs),
            'criteria_themes': list(criteria_themes_objs),
            'criteria_perspectives': list(criteria_perspectives_objs),
            'criteria_developers': list(criteria_developers_objs),
            'criteria_game_modes': list(criteria_game_modes_objs),

            # Виртуальная игра для тегов
            'criteria_virtual_game': criteria_virtual_game,

            # ID критериев для ссылок (альтернатива)
            'criteria_genres_ids': selected_criteria['genres'],
            'criteria_keywords_ids': selected_criteria['keywords'],
            'criteria_themes_ids': selected_criteria['themes'],
            'criteria_perspectives_ids': selected_criteria['perspectives'],
            'criteria_developers_ids': selected_criteria['developers'],
            'criteria_game_modes_ids': selected_criteria['game_modes'],

            # Algorithm weights
            'genres_weight': int(similarity_engine.GENRES_TOTAL_WEIGHT),
            'keywords_weight': int(similarity_engine.KEYWORDS_WEIGHT),
            'keywords_add_per_match': int(similarity_engine.KEYWORDS_ADD_PER_MATCH),
            'themes_weight': int(similarity_engine.THEMES_WEIGHT),
            'developers_weight': int(similarity_engine.DEVELOPERS_WEIGHT),
            'perspectives_weight': int(similarity_engine.PERSPECTIVES_WEIGHT),
            'game_modes_weight': int(similarity_engine.GAME_MODES_WEIGHT),
            'genres_exact_match_weight': int(similarity_engine.GENRES_EXACT_MATCH_WEIGHT),
        }

        # Add shared items
        for field, items in shared_items.items():
            context[f'shared_{field}'] = items
            context[f'shared_{field}_count'] = len(items)

        # Для совместимости с шаблоном
        context.update({
            'selected_genres': selected_criteria['genres'],
            'selected_keywords': selected_criteria['keywords'],
            'selected_themes': selected_criteria['themes'],
            'selected_perspectives': selected_criteria['perspectives'],
            'selected_developers': selected_criteria['developers'],
            'selected_game_modes': selected_criteria['game_modes'],

            # Объекты для выбранных критериев
            'selected_genres_objects': list(criteria_genres_objs),
            'selected_keywords_objects': list(criteria_keywords_objs),
            'selected_themes_objects': list(criteria_themes_objs),
            'selected_perspectives_objects': list(criteria_perspectives_objs),
            'selected_developers_objects': list(criteria_developers_objs),
            'selected_game_modes_objects': list(criteria_game_modes_objs),
        })

        # Рассчитываем среднюю схожесть для виртуальной карточки
        if is_criteria_comparison:
            # Для критериев можно использовать общую схожесть
            context['average_similarity'] = similarity_score
        else:
            # Для игры можно оставить None или рассчитать как-то иначе
            context['average_similarity'] = None

        return render(request, 'games/game_comparison.html', context)

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in comparison: {str(e)}")
        print(f"Details: {error_details}")
        return HttpResponseServerError(f"Error in comparison: {str(e)}")


def home(request: HttpRequest) -> HttpResponse:
    """Optimized home page with minimal queries."""
    cache_key = 'optimized_home_v10_final'
    cached_context = cache.get(cache_key)

    if cached_context:
        response = render(request, 'games/home.html', cached_context)
        response['X-Cache-Hit'] = 'True'
        return response

    start_time = time.time()

    try:
        # ОДИН запрос для всех игр с правильным prefetch_related
        popular_games = Game.objects.filter(
            rating_count__gt=10,
            rating__gte=3.0
        ).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives',
                     queryset=PlayerPerspective.objects.only('id', 'name')),
            # Убрал keywords и themes - они не используются в шаблоне карточки на главной!
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url'
        ).order_by('-rating_count', '-rating')[:12]

        # Второй запрос для недавних игр
        two_years_ago = timezone.now() - timedelta(days=730)
        recent_games = Game.objects.filter(
            first_release_date__gte=two_years_ago,
            first_release_date__lte=timezone.now()
        ).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives',
                     queryset=PlayerPerspective.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url'
        ).order_by('-first_release_date')[:12]

        # Keywords - только поля, нужные для отображения
        popular_keywords = Keyword.objects.filter(
            cached_usage_count__gt=0
        ).only('id', 'name').order_by('-cached_usage_count')[:20]  # Вернул лимит!

        from django.db import connection
        query_count = len(connection.queries)

        context = {
            'popular_games': list(popular_games),  # Преобразуем в list для кэша
            'recent_games': list(recent_games),
            'popular_keywords': list(popular_keywords),
            'execution_time': round(time.time() - start_time, 3),
            'query_count': query_count,
        }

        cache.set(cache_key, context, 300)  # 5 минут кэша

        response = render(request, 'games/home.html', context)
        response['X-Cache-Hit'] = 'False'
        response['X-DB-Queries'] = str(query_count)
        response['X-Response-Time'] = f"{context['execution_time']:.3f}s"

        return response

    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Home page error: {str(e)}")

        # Fallback контекст
        context = {
            'popular_games': [],
            'recent_games': [],
            'popular_keywords': [],
        }
        return render(request, 'games/home.html', context)


def keyword_category_view(request: HttpRequest, category_id: int) -> HttpResponse:
    """View games by keyword category."""
    category = get_object_or_404(KeywordCategory.objects.only('id', 'name'), id=category_id)

    games = Game.objects.filter(
        keywords__category=category
    ).prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url'
    ).distinct()

    popular_keywords = Keyword.objects.filter(
        category=category,
        game__isnull=False
    ).annotate(game_count=Count('game')).only(
        'id', 'name', 'category__id'
    ).order_by('-game_count')  # УДАЛИЛ ограничение [:20]

    return render(request, 'games/keyword_category.html', {
        'category': category,
        'games': list(games),
        'popular_keywords': list(popular_keywords),
    })


def game_search(request: HttpRequest) -> HttpResponse:
    """Simple game search by name."""
    search_query = request.GET.get('q', '')

    games = Game.objects.all().prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
        Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
        Prefetch('developers', queryset=Company.objects.only('id', 'name')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url', 'game_type'
    )

    if search_query:
        games = games.filter(name__icontains=search_query)

    games = games.order_by('-rating_count', '-rating')

    return render(request, 'games/game_search.html', {
        'games': list(games),
        'search_query': search_query,
        'total_results': games.count(),
    })


def platform_list(request: HttpRequest) -> HttpResponse:
    """Platform list page."""
    platforms = Platform.objects.annotate(
        game_count=Count('game')
    ).filter(game_count__gt=0).only(
        'id', 'name', 'slug'
    ).order_by('-game_count', 'name')

    return render(request, 'games/platform_list.html', {
        'platforms': list(platforms),
    })


def platform_games(request: HttpRequest, platform_id: int) -> HttpResponse:
    """Games for specific platform."""
    platform = get_object_or_404(Platform.objects.only('id', 'name', 'slug'), id=platform_id)

    games = Game.objects.filter(platforms=platform).prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('keywords', queryset=Keyword.objects.select_related('category')),
        Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
        Prefetch('developers', queryset=Company.objects.only('id', 'name')),
        Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url', 'game_type'
    ).order_by('-rating_count', '-rating')

    # Pagination
    paginator = Paginator(list(games), ITEMS_PER_PAGE['platform'])
    page = request.GET.get('page', 1)

    try:
        page_obj = paginator.page(int(page))
    except (PageNotAnInteger, EmptyPage):
        page_obj = paginator.page(1)

    return render(request, 'games/platform_games.html', {
        'platform': platform,
        'games': page_obj,
        'total_games': games.count(),
        'page_obj': page_obj,
        'is_paginated': paginator.num_pages > 1,
    })
