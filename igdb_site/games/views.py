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
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger  # Убедитесь, что есть эта строка
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
from functools import lru_cache, wraps

# ===== CACHE CONFIGURATION =====
CACHE_TIMES = {
    # Краткосрочный кэш (горячие данные)
    'short': {
        'filter_data': 1800,  # 30 минут
        'years_range': 3600,  # 1 час
        'genres_list': 7200,  # 2 часа
        'static_pages': 3600,  # 1 час
    },
    # Среднесрочный кэш
    'medium': {
        'similar_games': 43200,  # 12 часов (увеличили)
        'filtered_games': 7200,  # 2 часа
        'popular_keywords': 14400,  # 4 часа
        'platforms': 21600,  # 6 часов
    },
    # Долгосрочный кэш (редко меняющиеся данные)
    'long': {
        'game_types': 86400 * 7,  # 7 дней
        'themes': 86400 * 3,  # 3 дня
        'perspectives': 86400 * 3,
        'game_modes': 86400 * 3,
    },
    # Агрессивный кэш для тяжелых запросов
    'aggressive': {
        'full_page': 900,  # 15 минут
        'virtual_search': 1800,  # 30 минут
        'similar_for_game': 10800,  # 3 часа
    }
}

# Versioned cache keys для инвалидации
CACHE_VERSION = 'v20'

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
    """Simple wrapper for source game data with compatibility for template tags."""

    def __init__(self, game_obj=None, criteria=None, display_name=None):
        """
        Initialize SimpleSourceGame.

        Args:
            game_obj: Real Game object (optional)
            criteria: Dictionary of criteria (genres, keywords, etc.)
            display_name: Display name for the source
        """
        self.game_obj = game_obj
        self.criteria = criteria or {}
        self.display_name = display_name

        if game_obj:
            # Real game object
            self.id = game_obj.id
            self.name = game_obj.name
            self.is_game = True
            self.display_name = display_name or game_obj.name

            # Store all IDs from game object
            self._cache_game_ids()
        else:
            # Virtual game (criteria only)
            self.id = None
            self.name = display_name or "Search Criteria"
            self.is_game = False
            self.display_name = display_name or "Search Criteria"

            # Initialize empty ID caches
            self._genre_ids = criteria.get('genres', []) if criteria else []
            self._keyword_ids = criteria.get('keywords', []) if criteria else []
            self._theme_ids = criteria.get('themes', []) if criteria else []
            self._perspective_ids = criteria.get('perspectives', []) if criteria else []
            self._developer_ids = criteria.get('developers', []) if criteria else []
            self._game_mode_ids = criteria.get('game_modes', []) if criteria else []

    def _cache_game_ids(self):
        """Cache IDs from game object for faster access."""
        if not self.game_obj:
            return

        # Cache genre IDs
        if hasattr(self.game_obj, '_cached_genre_ids'):
            self._genre_ids = self.game_obj._cached_genre_ids
        elif hasattr(self.game_obj, 'genres') and hasattr(self.game_obj.genres, 'all'):
            self._genre_ids = [g.id for g in self.game_obj.genres.all()]
        else:
            self._genre_ids = []

        # Cache keyword IDs
        if hasattr(self.game_obj, '_cached_keyword_ids'):
            self._keyword_ids = self.game_obj._cached_keyword_ids
        elif hasattr(self.game_obj, 'keywords') and hasattr(self.game_obj.keywords, 'all'):
            self._keyword_ids = [k.id for k in self.game_obj.keywords.all()]
        else:
            self._keyword_ids = []

        # Cache theme IDs
        if hasattr(self.game_obj, '_cached_theme_ids'):
            self._theme_ids = self.game_obj._cached_theme_ids
        elif hasattr(self.game_obj, 'themes') and hasattr(self.game_obj.themes, 'all'):
            self._theme_ids = [t.id for t in self.game_obj.themes.all()]
        else:
            self._theme_ids = []

        # Cache perspective IDs
        if hasattr(self.game_obj, '_cached_perspective_ids'):
            self._perspective_ids = self.game_obj._cached_perspective_ids
        elif hasattr(self.game_obj, 'player_perspectives') and hasattr(self.game_obj.player_perspectives, 'all'):
            self._perspective_ids = [p.id for p in self.game_obj.player_perspectives.all()]
        else:
            self._perspective_ids = []

        # Cache developer IDs
        if hasattr(self.game_obj, '_cached_developer_ids'):
            self._developer_ids = self.game_obj._cached_developer_ids
        elif hasattr(self.game_obj, 'developers') and hasattr(self.game_obj.developers, 'all'):
            self._developer_ids = [d.id for d in self.game_obj.developers.all()]
        else:
            self._developer_ids = []

        # Cache game mode IDs
        if hasattr(self.game_obj, '_cached_game_mode_ids'):
            self._game_mode_ids = self.game_obj._cached_game_mode_ids
        elif hasattr(self.game_obj, 'game_modes') and hasattr(self.game_obj.game_modes, 'all'):
            self._game_mode_ids = [gm.id for gm in self.game_obj.game_modes.all()]
        else:
            self._game_mode_ids = []

    # ===== METHODS FOR TEMPLATE TAGS COMPATIBILITY =====

    def genres_list(self):
        """Return list of genre IDs for template tags."""
        return self._genre_ids

    def keywords_list(self):
        """Return list of keyword IDs for template tags."""
        return self._keyword_ids

    def themes_list(self):
        """Return list of theme IDs for template tags."""
        return self._theme_ids

    def perspectives_list(self):
        """Return list of perspective IDs for template tags."""
        return self._perspective_ids

    def developers_list(self):
        """Return list of developer IDs for template tags."""
        return self._developer_ids

    def game_modes_list(self):
        """Return list of game mode IDs for template tags."""
        return self._game_mode_ids

    # ===== PROPERTIES FOR DIRECT ACCESS =====

    @property
    def genres_ids(self):
        """Get genre IDs (alias for compatibility)."""
        return self._genre_ids

    @property
    def keywords_ids(self):
        """Get keyword IDs (alias for compatibility)."""
        return self._keyword_ids

    @property
    def themes_ids(self):
        """Get theme IDs (alias for compatibility)."""
        return self._theme_ids

    @property
    def perspectives_ids(self):
        """Get perspective IDs (alias for compatibility)."""
        return self._perspective_ids

    @property
    def developers_ids(self):
        """Get developer IDs (alias for compatibility)."""
        return self._developer_ids

    @property
    def game_modes_ids(self):
        """Get game mode IDs (alias for compatibility)."""
        return self._game_mode_ids

    # ===== DUMMY METHODS FOR TEMPLATE COMPATIBILITY =====

    def all(self):
        """Dummy method for template compatibility."""
        return []

    def get(self, *args, **kwargs):
        """Dummy method for template compatibility."""
        return None

    def __str__(self):
        return self.display_name or self.name

    def __repr__(self):
        if self.is_game:
            return f"<SimpleSourceGame: {self.name} (ID: {self.id})>"
        else:
            return f"<SimpleSourceGame: {self.name} (Criteria)>"


# Добавьте константу для количества игр на страницу
ITEMS_PER_PAGE_CLIENT = 16  # Количество игр на страницу для клиентской пагинации


def ajax_load_games_page(request: HttpRequest) -> HttpResponse:
    """Load games for specific page via AJAX - CORRECTED VERSION."""
    start_time = time.time()

    # Получаем номер страницы
    page_num = request.GET.get('page', '1')
    try:
        page_num = int(page_num)
    except (ValueError, TypeError):
        page_num = 1

    # Получаем параметры фильтров
    params = extract_request_params(request)
    selected_criteria = convert_params_to_lists(params)

    # Сортировка
    sort_field = params.get('sort', '-rating_count')

    # Режим поиска
    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = Game.objects.get(pk=int(params['source_game']))
        except (Game.DoesNotExist, ValueError):
            pass

    print(f"AJAX LOAD: Page {page_num}, find_similar: {find_similar}, sort: {sort_field}")

    # ВАЖНО: Рассчитываем правильный индекс для каждой страницы
    items_per_page = 16  # Должно совпадать с ITEMS_PER_PAGE_CLIENT
    offset = (page_num - 1) * items_per_page

    # Загружаем игры для запрошенной страницы
    if find_similar and (source_game_obj or any([
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes']
    ])):
        # Похожие игры
        if source_game_obj:
            similar_games_data, total_count = get_similar_games_for_game(
                source_game_obj, selected_criteria['platforms']
            )
        else:
            similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)

        # Форматируем
        games_with_similarity = _format_similar_games_data(similar_games_data, limit=total_count)
        _sort_similar_games(games_with_similarity, sort_field)

        # Берем игры для текущей страницы
        current_page_games = games_with_similarity[offset:offset + items_per_page]

        # ВАЖНО: Рассчитываем правильные индексы для каждой игры
        for i, game_item in enumerate(current_page_games):
            game_item['game_index'] = offset + i
            game_item['page_number'] = page_num

        context = {
            'games': current_page_games,
            'show_similarity': True,
            'source_game': SimpleSourceGame(
                game_obj=source_game_obj,
                criteria=selected_criteria,
                display_name=source_game_obj.name if source_game_obj else "Search Criteria"
            ),
            'current_page': page_num,
            'game_index_offset': offset,  # Добавляем смещение
        }
    else:
        # Обычные игры
        games_qs = Game.objects.all().prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        )

        # Применяем фильтры
        if any(selected_criteria.values()):
            games_qs = _apply_filters(games_qs, selected_criteria)

        # Сортировка
        if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
            games_qs = games_qs.order_by(sort_field)
        else:
            games_qs = games_qs.order_by('-rating_count')

        # Берем игры для текущей страницы
        current_games = list(games_qs[offset:offset + items_per_page])

        # ВАЖНО: Добавляем правильные индексы
        for i, game in enumerate(current_games):
            game.game_index = offset + i
            game.page_number = page_num

        context = {
            'games': current_games,
            'show_similarity': False,
            'current_page': page_num,
        }

    # Рендерим только игры с правильными data-атрибутами
    html = render_to_string('games/game_list/_games_grid.html', context)

    response = HttpResponse(html)
    response['Content-Type'] = 'text/html; charset=utf-8'
    response['X-AJAX-Page'] = str(page_num)
    response['X-AJAX-Count'] = str(len(context.get('games', context.get('games_with_similarity', []))))
    response['X-AJAX-Offset'] = str(offset)
    response['X-Response-Time'] = f"{time.time() - start_time:.3f}s"

    return response


def game_list(request: HttpRequest) -> HttpResponse:
    """Main game list function with enhanced caching."""
    # Проверяем AJAX запрос для загрузки страниц
    is_ajax = request.GET.get('_ajax') == '1'

    if is_ajax:
        return _handle_ajax_game_page(request)

    start_time = time.time()

    # Fast parameter processing
    params = extract_request_params(request)

    # ВАЖНО: Получаем номер страницы ИЗ URL ЗАПРОСА
    requested_page = request.GET.get('page', '1')
    print(f"MAIN VIEW: URL requested page: {requested_page}")

    try:
        requested_page_num = int(requested_page)
    except (ValueError, TypeError):
        requested_page_num = 1

    # Process fresh if no cache
    selected_criteria = convert_params_to_lists(params)

    # Get objects for all selected criteria
    selected_criteria_objects = _get_selected_criteria_objects(selected_criteria)

    # Get years range (heavily cached)
    years_range = _get_cached_years_range()

    # Determine mode
    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = _get_cached_game(params['source_game'])
        except (Game.DoesNotExist, ValueError):
            pass

    # Select mode
    should_use_similar_mode = _should_use_similar_mode(
        find_similar,
        source_game_obj,
        selected_criteria
    )

    # Process mode with REQUESTED page number
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
        f"MAIN VIEW: Mode {mode}, REQUESTED page {requested_page_num}, games count: {len(mode_result.get('games', mode_result.get('games_with_similarity', [])))}")

    # Fast loading of filter data from cache
    filter_data = _get_optimized_filter_data()

    # Build context with REQUESTED page number
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

    # Add debug info
    context['debug_info']['requested_page'] = requested_page_num
    context['debug_info']['mode_result_page'] = mode_result.get('current_page', 1)
    context['debug_info']['offset'] = offset

    # Render response
    response = render(request, 'games/game_list.html', context)

    response['X-Cache-Hit'] = 'Miss'
    response['X-Response-Time'] = f"{context['execution_time']:.3f}s"
    response['X-Mode'] = mode
    response['X-Requested-Page'] = str(requested_page_num)
    response['X-ModeResult-Page'] = str(mode_result.get('current_page', 1))
    response['X-Offset'] = str(offset)

    return response


def _build_context_from_cached_data(cached_data: Dict, params: Dict, requested_page_num: int) -> Dict:
    """Build context from cached data with page number support."""
    # Get filter data
    filter_data = cached_data['filter_data']

    # Get years range
    years_range = cached_data['years_range']

    # Get mode_result
    mode_result = cached_data['mode_result']

    # ВАЖНО: Используем запрошенную страницу, а не всегда первую
    current_page = requested_page_num
    total_count = mode_result.get('total_count', 0)

    # Рассчитываем total_pages на основе общего количества
    total_pages = (total_count + ITEMS_PER_PAGE_CLIENT - 1) // ITEMS_PER_PAGE_CLIENT if total_count > 0 else 1

    # Корректируем current_page если он выходит за пределы
    if current_page > total_pages:
        current_page = total_pages
    if current_page < 1:
        current_page = 1

    # Рассчитываем start и end индексы для текущей страницы
    start_index = (current_page - 1) * ITEMS_PER_PAGE_CLIENT + 1
    end_index = min(current_page * ITEMS_PER_PAGE_CLIENT, total_count)

    # Build basic context
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

        # Filter data
        'genres': _get_cached_genres_list(),
        'themes': filter_data['themes'],
        'perspectives': filter_data['perspectives'],
        'game_modes': filter_data['game_modes'],
        'keywords': filter_data['keywords'],
        'platforms': filter_data['platforms'],
        'popular_keywords': filter_data['popular_keywords'],
        'game_types': GameTypeEnum.CHOICES,

        # Years
        'years_range': years_range,
        'current_year': timezone.now().year,

        # Selected criteria
        'selected_criteria_objects': cached_data.get('selected_criteria_objects', {}),
        'current_sort': params.get('sort', ''),

        # Debug info
        'debug_info': {
            'mode': cached_data['mode'],
            'from_cache': True,
            'cache_timestamp': cached_data.get('timestamp', 0),
            'requested_page': requested_page_num
        }
    }

    # Добавляем selected criteria IDs (нужно получить из params)
    selected_criteria = convert_params_to_lists(params)

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

        # Selected criteria OBJECTS (из кэша)
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

    # Извлекаем параметры
    params = extract_request_params(request)
    requested_page = params.get('page', '1')

    try:
        requested_page_num = int(requested_page)
    except (ValueError, TypeError):
        requested_page_num = 1

    selected_criteria = convert_params_to_lists(params)

    # ВАЖНО: Получаем игры для конкретной страницы
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

    # ВАЖНО: Загружаем игры ДЛЯ ЗАПРОШЕННОЙ СТРАНИЦЫ, а не все
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
            'current_page': requested_page_num,  # ВАЖНО: передаем номер страницы
            'similarity_map': mode_result.get('similarity_map', {}),  # ВАЖНО: передаем карту схожести
        }
    else:
        mode_result = _get_all_games_mode_paginated(
            selected_criteria, params.get('sort', '-rating_count'), requested_page_num
        )
        games = mode_result.get('games', [])
        print(f"AJAX: Loading regular games page {requested_page_num}, found {len(games)} games")
        template_context = {
            'games': games,
            'current_page': requested_page_num,  # ВАЖНО: передаем номер страницы
        }

    # ВАЖНО: Рендерим ТОЛЬКО HTML игр для ЗАПРОШЕННОЙ страницы
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

    # Оптимизированный запрос для игр
    games_qs = Game.objects.all().prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
        Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url', 'game_type'
    )

    # Применяем фильтры если есть
    if any(selected_criteria.values()):
        games_qs = _apply_filters(games_qs, selected_criteria)

    # Сортировка
    if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
        games_qs = games_qs.order_by(sort_field)
    else:
        games_qs = games_qs.order_by('-rating_count')

    # Подсчет общего количества для клиентской пагинации
    total_count = games_qs.count()

    # Вычисляем смещение для запроса
    items_per_page = 16
    offset = (page_num - 1) * items_per_page

    print(f"SERVER DEBUG: Page {page_num}, offset: {offset}, limit: {items_per_page}")
    print(f"SERVER DEBUG: Total count: {total_count}")

    # Берем только игры для текущей страницы
    games = list(games_qs[offset:offset + items_per_page])

    print(f"SERVER DEBUG: Loaded {len(games)} games for page {page_num}")

    return {
        'games': games,
        'total_count': total_count,  # ВАЖНО: Передаем общее количество
        'current_page': page_num,
        'show_similarity': False,
        'find_similar': False,
        'source_game': None,
        'similarity_map': {},
    }


def test_pagination(request: HttpRequest) -> HttpResponse:
    """Test endpoint to check pagination."""
    page_num = request.GET.get('page', '1')
    try:
        page_num = int(page_num)
    except (ValueError, TypeError):
        page_num = 1

    # Простой запрос без фильтров
    games_qs = Game.objects.all().order_by('id')
    total_count = games_qs.count()

    offset = (page_num - 1) * ITEMS_PER_PAGE_CLIENT
    games = list(games_qs[offset:offset + ITEMS_PER_PAGE_CLIENT])

    context = {
        'games': games,
        'page_num': page_num,
        'total_count': total_count,
        'offset': offset,
        'game_ids': [g.id for g in games]
    }

    return render(request, 'games/test_pagination.html', context)


def _get_similar_games_mode_paginated(
        params: Dict[str, str],
        selected_criteria: Dict[str, List[int]],
        source_game_obj: Optional[Game],
        page_num: int
) -> Dict[str, Any]:
    """Режим похожих игр с клиентской пагинацией."""
    current_sort = params.get('sort', '-similarity')

    print(f"AJAX: Similar games mode for page {page_num}")

    # Получаем похожие игры (ВСЕ похожие игры)
    if source_game_obj:
        print(f"AJAX: Finding similar games for game: {source_game_obj.name} (ID: {source_game_obj.id})")
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, selected_criteria['platforms']
        )
        source_display = source_game_obj.name

        # Создаем SimpleSourceGame с критериями из игры
        game_criteria = {}

        # Заполняем критерии
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

        # Создаем SimpleSourceGame только с критериями
        source_game = SimpleSourceGame(
            game_obj=None,
            criteria=selected_criteria,
            display_name=source_display
        )

    print(f"AJAX: Found {len(similar_games_data)} similar games total")

    # Форматируем ВСЕ похожие игры
    games_with_similarity = _format_similar_games_data(similar_games_data, limit=total_count)

    # Сортируем ВСЕ игры
    _sort_similar_games(games_with_similarity, current_sort)

    # Вычисляем смещение для запроса
    items_per_page = 16
    offset = (page_num - 1) * items_per_page

    # Берем только игры для ЗАПРОШЕННОЙ страницы
    current_page_games = games_with_similarity[offset:offset + items_per_page]

    print(f"AJAX: Returning {len(current_page_games)} games for page {page_num} (offset {offset})")

    return {
        'games_with_similarity': current_page_games,
        'total_count': total_count,  # ВАЖНО: Передаем общее количество
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
    # Get genres list from cache
    genres_list = _get_cached_genres_list()

    # ВАЖНО: Для КЛИЕНТСКОЙ пагинации используем total_count из mode_result
    # но current_page и total_pages рассчитываем на клиенте

    current_page = requested_page_num
    total_count = mode_result.get('total_count', 0)

    # ВАЖНО: Для клиентской пагинации НЕ используем серверную пагинацию
    # total_pages рассчитывается на клиенте JavaScript'ом
    total_pages = 0  # Будет рассчитано на клиенте

    # Если запрошенная страница выходит за пределы - корректируем
    if total_pages > 0 and current_page > total_pages:
        current_page = total_pages

    # Если это похожие игры
    if mode == 'similar':
        games_with_similarity = mode_result.get('games_with_similarity', [])
        games = []
        show_similarity = True
        source_game = mode_result.get('source_game')

        # ВАЖНО: Для клиентской пагинации нужно ограничить количество игр
        # для текущей страницы
        if games_with_similarity:
            start_idx = offset
            end_idx = min(offset + items_per_page, len(games_with_similarity))
            current_page_games = games_with_similarity[start_idx:end_idx]

            # Добавляем индексы для каждой игры
            for i, game_item in enumerate(current_page_games):
                game_item['game_index'] = offset + i
                game_item['page_number'] = current_page

            games_with_similarity = current_page_games
    else:
        games = mode_result.get('games', [])
        games_with_similarity = []
        show_similarity = False
        source_game = None

        # ВАЖНО: Для клиентской пагинации нужно ограничить количество игр
        # для текущей страницы
        if games:
            start_idx = offset
            end_idx = min(offset + items_per_page, len(games))
            current_page_games = games[start_idx:end_idx]

            # Добавляем индексы для каждой игры
            for i, game in enumerate(current_page_games):
                game.game_index = offset + i
                game.page_number = current_page

            games = current_page_games

    # Рассчитываем start и end индексы для ЗАПРОШЕННОЙ страницы
    start_index = offset + 1
    end_index = min(offset + items_per_page, total_count)

    print(
        f"CONTEXT: Building for REQUESTED page {current_page}, offset: {offset}, "
        f"games count: {len(games) if games else len(games_with_similarity)}, "
        f"total_count: {total_count}")

    # Prepare context
    context = {
        # Games data - для ЗАПРОШЕННОЙ страницы
        'games': games,
        'games_with_similarity': games_with_similarity,
        'page_obj': None,  # ВАЖНО: Не используем серверную пагинацию
        'is_paginated': False,  # ВАЖНО: Отключаем серверную пагинацию
        'total_count': total_count,  # ВАЖНО: Передаем общее количество для клиентской пагинации
        'total_pages': 0,  # Будет рассчитано на клиенте
        'current_page': current_page,
        'start_index': start_index,
        'end_index': end_index,
        'items_per_page': items_per_page,
        'game_index_offset': offset,  # Добавляем смещение

        # Mode flags
        'find_similar': find_similar,
        'show_similarity': show_similarity,
        'source_game': source_game,
        'source_game_obj': source_game_obj,

        # Similarity data
        'similarity_map': mode_result.get('similarity_map', {}),

        # Filter data for checkboxes (ALL items)
        'genres': genres_list,
        'themes': filter_data['themes'],
        'perspectives': filter_data['perspectives'],
        'game_modes': filter_data['game_modes'],
        'keywords': filter_data['keywords'],

        # Filter data for display
        'platforms': filter_data['platforms'],
        'popular_keywords': filter_data['popular_keywords'],
        'game_types': GameTypeEnum.CHOICES,

        # Date filter
        'years_range': years_range,
        'current_year': timezone.now().year,

        # Selected criteria IDs
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

        # Selected criteria OBJECTS (for badges)
        'selected_genres_objects': selected_criteria_objects.get('genres', []),
        'selected_keywords_objects': selected_criteria_objects.get('keywords', []),
        'selected_platforms_objects': selected_criteria_objects.get('platforms', []),
        'selected_themes_objects': selected_criteria_objects.get('themes', []),
        'selected_perspectives_objects': selected_criteria_objects.get('perspectives', []),
        'selected_game_modes_objects': selected_criteria_objects.get('game_modes', []),
        'selected_developers_objects': selected_criteria_objects.get('developers', []),

        # Sorting
        'current_sort': params.get('sort', ''),

        # Performance info
        'execution_time': round(execution_time, 3),

        # Debug info
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
            'total_pages': 0,  # Для клиентской пагинации
            'games_count': len(games) if games else len(games_with_similarity),
        }
    }

    # Add date filter badge flag
    if selected_criteria['release_year_start'] or selected_criteria['release_year_end']:
        context['has_date_filter'] = True
    else:
        context['has_date_filter'] = False

    return context

# ===== MODE DETECTION FUNCTIONS =====

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
    # If explicitly requested
    if find_similar_param:
        logger.debug("Similar mode: explicitly requested (find_similar=1)")
        return True

    # If we have a source game
    if source_game_obj:
        logger.debug(f"Similar mode: source game {source_game_obj.id} provided")
        return True

    # Check for similarity criteria
    has_similarity_criteria = _has_similarity_criteria(selected_criteria)

    if has_similarity_criteria:
        logger.debug("Similar mode: similarity criteria detected")
        return True

    # Default to regular mode
    logger.debug("Similar mode: no criteria detected, using regular mode")
    return False


def _has_similarity_criteria(selected_criteria: Dict[str, List[int]]) -> bool:
    """
    Check if there are criteria for similarity search.

    Now includes all criteria that can be used for similarity search:
    - Genres
    - Keywords
    - Themes
    - Perspectives
    - Game modes

    Excludes:
    - Platforms (search filter only)
    - Game types (search filter only)
    - Developers (search filter only)
    - Release date (search filter only)
    """
    similarity_criteria = [
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes']
    ]

    return any(similarity_criteria)


def extract_request_params(request: HttpRequest) -> Dict[str, str]:
    """Extract parameters from request efficiently."""
    get_params = request.GET

    params = {}
    for key in ['find_similar', 'g', 'k', 'p', 't', 'pp', 'd', 'gm', 'gt', 'yr', 'ys', 'ye', 'source_game', 'sort',
                'page']:
        value = get_params.get(key, '')
        params[key] = value

    return params


# И обновить convert_params_to_lists для обработки None:
def convert_params_to_lists(params_dict: Dict[str, str]) -> Dict[str, List[int]]:
    """Convert query parameters to lists of integers."""

    # Helper function to parse string to int list
    def parse_int_list(param_value):
        if not param_value or param_value is None:
            return []

        result = []
        try:
            for part in str(param_value).split(','):
                part = part.strip()
                if part and part.isdigit():
                    result.append(int(part))
        except (ValueError, AttributeError):
            return []
        return result

    # Process year range
    release_year_start = None
    release_year_end = None

    year_range = params_dict.get('yr')
    if year_range and '-' in year_range:
        try:
            parts = year_range.split('-')
            if parts[0].strip():
                release_year_start = int(parts[0].strip())
            if len(parts) > 1 and parts[1].strip():
                release_year_end = int(parts[1].strip())
        except (ValueError, IndexError):
            pass

    # Alternative format: separate parameters
    if not release_year_start and params_dict.get('ys'):
        try:
            release_year_start = int(params_dict['ys'])
        except ValueError:
            pass

    if not release_year_end and params_dict.get('ye'):
        try:
            release_year_end = int(params_dict['ye'])
        except ValueError:
            pass

    return {
        'genres': parse_int_list(params_dict.get('g')),
        'keywords': parse_int_list(params_dict.get('k')),
        'platforms': parse_int_list(params_dict.get('p')),
        'themes': parse_int_list(params_dict.get('t')),
        'perspectives': parse_int_list(params_dict.get('pp')),
        'developers': parse_int_list(params_dict.get('d')),
        'game_modes': parse_int_list(params_dict.get('gm')),
        'game_types': parse_int_list(params_dict.get('gt')),
        'release_years': [],  # Legacy, not used
        'release_year_start': release_year_start,
        'release_year_end': release_year_end,
    }


# ===== CACHE MANAGEMENT =====

def warm_cache_for_home_page() -> Dict:
    """Warm cache for home page data."""
    logger.info("Warming cache for home page...")

    # Warm common data
    data_to_warm = [
        ('years_range', lambda: get_release_years_range()),
        ('optimized_filter_data', _get_optimized_filter_data),
        ('genres_list', lambda: list(Genre.objects.all().only('id', 'name').order_by('name'))),
    ]

    results = {}
    for key, func in data_to_warm:
        try:
            cache_key = get_cache_key(key)
            data = func()
            cache.set(cache_key, data, CACHE_TIMES['short'].get(key, 3600))
            results[key] = 'warmed'
        except Exception as e:
            results[key] = f'error: {str(e)}'

    # Warm popular queries
    popular_queries = [
        {'sort': '-rating_count'},  # Most popular
        {'sort': '-rating'},  # Highest rated
        {'sort': '-first_release_date'},  # Newest
        {'g': '5,12,31'},  # Common genres (RPG, Adventure, Indie)
    ]

    from django.test import RequestFactory
    factory = RequestFactory()

    for i, query in enumerate(popular_queries):
        try:
            request = factory.get('/games/', query)

            # Get cache key for this query
            params = extract_request_params(request)
            cache_key = get_cache_key(
                'game_list_data',
                {
                    'params': params,
                    'selected_criteria': convert_params_to_lists(params),
                }
            )

            # Pre-calculate data
            selected_criteria = convert_params_to_lists(params)
            mode_result = _get_all_games_mode(selected_criteria, params.get('sort', '-rating_count'), '1')

            cache_data = {
                'mode_result': mode_result,
                'filter_data': _get_optimized_filter_data(),
                'years_range': get_release_years_range(),
                'timestamp': time.time(),
                'mode': 'regular'
            }

            cache.set(cache_key, cache_data, CACHE_TIMES['medium']['filtered_games'])
            results[f'query_{i}'] = 'warmed'

        except Exception as e:
            results[f'query_{i}'] = f'error: {str(e)}'

    return results


def clear_game_list_cache() -> Dict:
    """Clear all game list related cache."""
    logger.info("Clearing game list cache...")

    groups_to_clear = [
        'game_list_pages',
        'game_list_data',
        'game_list_data_first_page',  # Добавили новый ключ
        'filter_data',
        'similar_games',
        'all_games',
        'static_data',
    ]

    results = {}
    for group in groups_to_clear:
        try:
            deleted = invalidate_cache_group(group)
            results[group] = f'deleted {deleted} keys'
        except Exception as e:
            results[group] = f'error: {str(e)}'

    return results


def get_cache_stats() -> Dict:
    """Get cache statistics."""
    # This is a simplified version - actual implementation depends on cache backend
    stats = {
        'total_keys_estimated': 'N/A',
        'memory_usage': 'N/A',
        'hit_rate': 'N/A',
    }

    # Try to get some stats from cache
    try:
        # Test keys from different groups
        test_keys = [
            get_cache_key('years_range'),
            get_cache_key('optimized_filter_data'),
            get_cache_key('genres_list'),
        ]

        cached = cache.get_many(test_keys)
        stats['test_keys_cached'] = f"{len(cached)}/{len(test_keys)}"

    except Exception as e:
        stats['error'] = str(e)

    return stats


# ===== CACHE DECORATORS =====

def cache_view(timeout: int = 300, vary_on: List[str] = None,
               cache_group: str = None):
    """
    Decorator to cache view responses.

    Args:
        timeout: Cache timeout in seconds
        vary_on: List of GET parameters to vary cache on
        cache_group: Cache group for invalidation
    """

    def decorator(view_func):
        @wraps(view_func)
        def _wrapped_view(request, *args, **kwargs):
            # Build cache key
            key_parts = [
                request.path,
                request.GET.urlencode() if vary_on is None else
                '&'.join(f"{k}={request.GET.get(k, '')}" for k in vary_on if k in request.GET)
            ]

            cache_key = get_cache_key('view', {'parts': key_parts})

            # Try to get from cache
            cached_response = cache.get(cache_key)
            if cached_response and isinstance(cached_response, HttpResponse):
                cached_response['X-Cache-Hit'] = 'View-Decorator'
                return cached_response

            # Call the view
            response = view_func(request, *args, **kwargs)

            # Cache the response if it's successful
            if response.status_code == 200:
                cache.set(cache_key, response, timeout)

                if cache_group:
                    group_key = f"cache_group_{cache_group}"
                    group_members = cache.get(group_key, [])
                    if cache_key not in group_members:
                        group_members.append(cache_key)
                        cache.set(group_key, group_members, timeout + 3600)

            response['X-Cache-Hit'] = 'Miss'
            return response

        return _wrapped_view

    return decorator


def cache_method(timeout: int = 300, key_prefix: str = None):
    """
    Decorator to cache method results.

    Args:
        timeout: Cache timeout in seconds
        key_prefix: Custom key prefix
    """

    def decorator(method):
        @wraps(method)
        def _wrapped_method(self, *args, **kwargs):
            # Build cache key
            prefix = key_prefix or f"{self.__class__.__name__}_{method.__name__}"
            key_data = {
                'args': args,
                'kwargs': kwargs,
                'self_id': id(self) if hasattr(self, 'id') else None
            }

            cache_key = get_cache_key(prefix, key_data)

            # Try cache
            cached = cache.get(cache_key)
            if cached is not None:
                return cached

            # Call method
            result = method(self, *args, **kwargs)

            # Cache result
            cache.set(cache_key, result, timeout)

            return result

        return _wrapped_method

    return decorator


def _get_cached_years_range() -> Dict:
    """Get years range with multi-layer caching."""
    cache_key = get_cache_key('years_range')

    return cache_get_or_set(
        cache_key,
        lambda: get_release_years_range(),
        CACHE_TIMES['short']['years_range'],
        cache_group='static_data'
    )


def _get_optimized_filter_data() -> Dict[str, List]:
    """Get all filter data with intelligent caching."""
    cache_key = get_cache_key('optimized_filter_data')

    def fetch_filter_data():
        # Получаем все данные одним запросом где возможно
        from django.db.models import Prefetch

        # Platform data
        platforms = list(Platform.objects.annotate(
            game_count=Count('game', distinct=True)
        ).filter(game_count__gt=0).only('id', 'name', 'slug')
                         .order_by('-game_count', 'name'))

        # Keywords - все без ограничений
        keywords = list(Keyword.objects.all()
                        .select_related('category').only(
            'id', 'name', 'category__id', 'category__name'
        ).order_by('name'))

        # Popular keywords (top 50)
        popular_keywords = list(Keyword.objects.filter(
            cached_usage_count__gt=0
        ).select_related('category').only(
            'id', 'name', 'category__id', 'category__name', 'cached_usage_count'
        ).order_by('-cached_usage_count')[:50])

        # All other data
        themes = list(Theme.objects.annotate(
            game_count=Count('game', distinct=True)
        ).filter(game_count__gt=0).only('id', 'name').order_by('name'))

        perspectives = list(PlayerPerspective.objects.annotate(
            game_count=Count('game', distinct=True)
        ).filter(game_count__gt=0).only('id', 'name').order_by('name'))

        game_modes = list(GameMode.objects.annotate(
            game_count=Count('game', distinct=True)
        ).filter(game_count__gt=0).only('id', 'name').order_by('name'))

        developers = list(Company.objects.annotate(
            developed_game_count=Count('developed_games', distinct=True)
        ).filter(developed_game_count__gt=0).only('id', 'name').order_by('name'))

        return {
            'platforms': platforms,
            'keywords': keywords,
            'popular_keywords': popular_keywords,
            'themes': themes,
            'perspectives': perspectives,
            'game_modes': game_modes,
            'developers': developers,
        }

    return cache_get_or_set(
        cache_key,
        fetch_filter_data,
        CACHE_TIMES['short']['filter_data'],
        cache_group='filter_data'
    )


def _get_cached_game(game_id: str) -> Optional[Game]:
    """Get game from cache or database."""
    cache_key = get_cache_key('game', {'id': game_id})

    def fetch_game():
        try:
            return Game.objects.only('id', 'name', 'game_type').get(pk=int(game_id))
        except (Game.DoesNotExist, ValueError):
            return None

    return cache_get_or_set(
        cache_key,
        fetch_game,
        3600,  # 1 hour cache for games
        cache_group='games'
    )


def _get_similar_games_mode_cached(params: Dict[str, str],
                                   selected_criteria: Dict[str, List[int]],
                                   source_game_obj: Optional[Game]) -> Dict[str, Any]:
    """Cached version of similar games mode."""
    cache_key_data = {
        'params': params,
        'selected_criteria': selected_criteria,
        'source_game_id': source_game_obj.id if source_game_obj else None,
        'type': 'similar_games_mode'
    }

    cache_key = get_cache_key('similar_games_mode', cache_key_data)

    def fetch_similar_games():
        return _get_similar_games_mode(params, selected_criteria, source_game_obj)

    return cache_get_or_set(
        cache_key,
        fetch_similar_games,
        CACHE_TIMES['aggressive']['virtual_search'],
        cache_group='similar_games'
    )


def _get_all_games_mode_cached(selected_criteria: Dict[str, List[int]],
                               sort_field: str) -> Dict[str, Any]:
    """Cached version of all games mode."""
    cache_key_data = {
        'selected_criteria': selected_criteria,
        'sort_field': sort_field,
        'type': 'all_games_mode'
    }

    cache_key = get_cache_key('all_games_mode', cache_key_data)

    def fetch_all_games():
        return _get_all_games_mode(selected_criteria, sort_field, '1')

    return cache_get_or_set(
        cache_key,
        fetch_all_games,
        CACHE_TIMES['medium']['filtered_games'],
        cache_group='all_games'
    )


def _should_cache_full_page(params: Dict, selected_criteria: Dict) -> bool:
    """Determine if full page should be cached."""
    # Don't cache pages with complex filters
    total_criteria = 0
    for v in selected_criteria.values():
        if v is not None:
            if isinstance(v, (list, tuple, set)):
                total_criteria += len(v)
            elif isinstance(v, (int, float)):
                total_criteria += 1
            # Игнорируем другие типы

    # Cache simple queries only
    if total_criteria > 5:
        return False

    # Don't cache pages with certain parameters
    excluded_params = ['page', 'sort', '_scroll']
    has_excluded = any(params.get(p) for p in excluded_params if p in params)

    if has_excluded:
        return False

    return True


def _get_cached_genres_list() -> List:
    """Get genres list with caching."""
    cache_key = get_cache_key('genres_list')

    return cache_get_or_set(
        cache_key,
        lambda: list(Genre.objects.all().only('id', 'name').order_by('name')),
        CACHE_TIMES['short']['genres_list'],
        cache_group='static_data'
    )


# ===== CACHE HELPER FUNCTIONS =====

def get_cache_key(prefix: str, data: Dict = None, version: str = None) -> str:
    """Generate versioned cache key."""
    version = version or CACHE_VERSION

    if data:
        import json
        data_str = json.dumps(data, sort_keys=True)
        key_hash = hashlib.md5(data_str.encode()).hexdigest()
        return f"{prefix}_{version}_{key_hash}"

    return f"{prefix}_{version}"


def cache_get_or_set(key: str, func: callable, timeout: int,
                     cache_group: str = None) -> Any:
    """
    Get from cache or calculate and set with timeout.

    Args:
        key: Cache key
        func: Function to call if cache miss
        timeout: Cache timeout in seconds
        cache_group: Optional group for bulk invalidation

    Returns:
        Cached or calculated value
    """
    cached = cache.get(key)

    if cached is not None:
        # Check if it's our special cache wrapper
        if isinstance(cached, dict) and cached.get('__cache_wrapper') == True:
            return cached['data']
        return cached

    # Calculate fresh value
    result = func()

    # Store with wrapper for type safety
    if not isinstance(result, (str, int, float, list, dict, tuple)):
        # Complex objects need wrapper
        cache_value = {
            '__cache_wrapper': True,
            'data': result,
            'timestamp': time.time()
        }
    else:
        cache_value = result

    cache.set(key, cache_value, timeout)

    # Register in cache group if specified
    if cache_group:
        group_key = f"cache_group_{cache_group}"
        group_members = cache.get(group_key, [])
        if key not in group_members:
            group_members.append(key)
            cache.set(group_key, group_members, timeout + 3600)

    return result


def invalidate_cache_group(cache_group: str) -> int:
    """Invalidate all cache keys in a group."""
    group_key = f"cache_group_{cache_group}"
    group_members = cache.get(group_key, [])

    deleted_count = 0
    for key in group_members:
        if cache.delete(key):
            deleted_count += 1

    cache.delete(group_key)
    return deleted_count


def cache_multi_get(keys: List[str]) -> Dict[str, Any]:
    """Multi-get from cache."""
    if not keys:
        return {}

    cached = cache.get_many(keys)
    result = {}

    for key, value in cached.items():
        if isinstance(value, dict) and value.get('__cache_wrapper'):
            result[key] = value['data']
        else:
            result[key] = value

    return result


def cache_multi_set(items: Dict[str, Any], timeout: int) -> None:
    """Multi-set to cache."""
    if not items:
        return

    cache_items = {}
    for key, value in items.items():
        if not isinstance(value, (str, int, float, list, dict, tuple)):
            cache_items[key] = {
                '__cache_wrapper': True,
                'data': value,
                'timestamp': time.time()
            }
        else:
            cache_items[key] = value

    cache.set_many(cache_items, timeout)


# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ CONTEXT =====

def _get_selected_criteria_objects(selected_criteria: Dict[str, List[int]]) -> Dict[str, List]:
    """Получаем объекты для всех выбранных критериев."""
    selected_objects = {}

    # Жанры
    if selected_criteria['genres']:
        selected_objects['genres'] = list(Genre.objects.filter(
            id__in=selected_criteria['genres']
        ).only('id', 'name'))

    # Ключевые слова
    if selected_criteria['keywords']:
        selected_objects['keywords'] = list(Keyword.objects.filter(
            id__in=selected_criteria['keywords']
        ).only('id', 'name'))

    # Платформы
    if selected_criteria['platforms']:
        selected_objects['platforms'] = list(Platform.objects.filter(
            id__in=selected_criteria['platforms']
        ).only('id', 'name'))

    # Темы
    if selected_criteria['themes']:
        selected_objects['themes'] = list(Theme.objects.filter(
            id__in=selected_criteria['themes']
        ).only('id', 'name'))

    # Перспективы
    if selected_criteria['perspectives']:
        selected_objects['perspectives'] = list(PlayerPerspective.objects.filter(
            id__in=selected_criteria['perspectives']
        ).only('id', 'name'))

    # Режимы игры
    if selected_criteria['game_modes']:
        selected_objects['game_modes'] = list(GameMode.objects.filter(
            id__in=selected_criteria['game_modes']
        ).only('id', 'name'))

    return selected_objects


def _get_all_games_mode(selected_criteria: Dict[str, List[int]], sort_field: str, page_number: str) -> Dict[str, Any]:
    """Режим отображения ВСЕХ игр с фильтрами для клиентской пагинации."""
    # Оптимизированный запрос для ВСЕХ игр (без лимита)
    games_qs = Game.objects.all().prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
        Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url', 'game_type'
    )

    # Применяем фильтры если есть
    if any(selected_criteria.values()):
        games_qs = _apply_filters(games_qs, selected_criteria)

    # Сортировка
    if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
        games_qs = games_qs.order_by(sort_field)
    else:
        games_qs = games_qs.order_by('-rating_count')

    # Загружаем ВСЕ игры для клиентской пагинации
    # Но ограничим для производительности (например, 500 игр)
    all_games = list(games_qs[:500])  # Ограничиваем 500 для производительности
    total_count = min(games_qs.count(), 500)

    # Возвращаем все игры (клиентская пагинация будет разбивать их)
    return {
        'games': all_games,  # ВСЕ игры, не только одна страница
        'page_obj': None,  # Не используем серверную пагинацию
        'paginator': None,
        'is_paginated': False,  # Отключаем серверную пагинацию
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

    # Получаем похожие игры
    if source_game_obj:
        logger.info(f"Поиск похожих игр для игры: {source_game_obj.name} (ID: {source_game_obj.id})")
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, selected_criteria['platforms']
        )
        source_display = source_game_obj.name

        # Собираем критерии из игры для SimpleSourceGame
        # Оптимизированно - используем prefetched данные
        game_criteria = {}

        # Жанры
        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'genres' in source_game_obj._prefetched_objects_cache:
            game_criteria['genres'] = [g.id for g in source_game_obj.genres.all()]
        elif hasattr(source_game_obj, 'genres') and hasattr(source_game_obj.genres, 'all'):
            game_criteria['genres'] = [g.id for g in source_game_obj.genres.all()]
        else:
            game_criteria['genres'] = []

        # Ключевые слова
        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'keywords' in source_game_obj._prefetched_objects_cache:
            game_criteria['keywords'] = [k.id for k in source_game_obj.keywords.all()]
        elif hasattr(source_game_obj, 'keywords') and hasattr(source_game_obj.keywords, 'all'):
            game_criteria['keywords'] = [k.id for k in source_game_obj.keywords.all()]
        else:
            game_criteria['keywords'] = []

        # Темы
        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'themes' in source_game_obj._prefetched_objects_cache:
            game_criteria['themes'] = [t.id for t in source_game_obj.themes.all()]
        elif hasattr(source_game_obj, 'themes') and hasattr(source_game_obj.themes, 'all'):
            game_criteria['themes'] = [t.id for t in source_game_obj.themes.all()]
        else:
            game_criteria['themes'] = []

        # Перспективы
        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'player_perspectives' in source_game_obj._prefetched_objects_cache:
            game_criteria['perspectives'] = [p.id for p in source_game_obj.player_perspectives.all()]
        elif hasattr(source_game_obj, 'player_perspectives') and hasattr(source_game_obj.player_perspectives, 'all'):
            game_criteria['perspectives'] = [p.id for p in source_game_obj.player_perspectives.all()]
        else:
            game_criteria['perspectives'] = []

        # Разработчики
        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'developers' in source_game_obj._prefetched_objects_cache:
            game_criteria['developers'] = [d.id for d in source_game_obj.developers.all()]
        elif hasattr(source_game_obj, 'developers') and hasattr(source_game_obj.developers, 'all'):
            game_criteria['developers'] = [d.id for d in source_game_obj.developers.all()]
        else:
            game_criteria['developers'] = []

        # Режимы игры
        if hasattr(source_game_obj,
                   '_prefetched_objects_cache') and 'game_modes' in source_game_obj._prefetched_objects_cache:
            game_criteria['game_modes'] = [gm.id for gm in source_game_obj.game_modes.all()]
        elif hasattr(source_game_obj, 'game_modes') and hasattr(source_game_obj.game_modes, 'all'):
            game_criteria['game_modes'] = [gm.id for gm in source_game_obj.game_modes.all()]
        else:
            game_criteria['game_modes'] = []

        # Создаем SimpleSourceGame с критериями из игры
        source_game = SimpleSourceGame(
            game_obj=source_game_obj,
            criteria=game_criteria,
            display_name=source_display
        )
    else:
        logger.info("Поиск похожих игр по критериям")
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)
        source_display = "Search Criteria"

        # Создаем SimpleSourceGame только с критериями
        source_game = SimpleSourceGame(
            game_obj=None,
            criteria=selected_criteria,
            display_name=source_display
        )

    logger.info(f"Найдено {len(similar_games_data)} игр до форматирования")

    # Форматируем (но не ограничиваем 500, а все)
    games_with_similarity = _format_similar_games_data(similar_games_data, limit=500)  # Увеличили лимит

    logger.info(f"После форматирования: {len(games_with_similarity)} игр")

    # Сортируем
    _sort_similar_games(games_with_similarity, current_sort)

    # Возвращаем ВСЕ игры для клиентской пагинации
    return {
        'games_with_similarity': games_with_similarity,  # ВСЕ игры
        'page_obj': None,  # Не используем серверную пагинацию
        'paginator': None,
        'is_paginated': False,  # Отключаем серверную пагинацию
        'total_count': len(games_with_similarity),
        'show_similarity': True,
        'find_similar': True,
        'source_game': source_game,
        'source_game_obj': source_game_obj,
    }


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

        # Получаем сохраненный процент схожести из параметров (если есть)
        saved_similarity = request.GET.get('similarity')
        if saved_similarity:
            try:
                similarity_score = float(saved_similarity)
            except (ValueError, TypeError):
                similarity_score = 0
        else:
            similarity_score = 0

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

        # Если процент схожести не был сохранен в параметрах, рассчитываем его
        if similarity_score == 0:
            similarity_engine = GameSimilarity()

            if is_criteria_comparison:
                # Для критериев vs игры - используем метод find_similar_games
                virtual_game = VirtualGame(
                    genre_ids=selected_criteria['genres'],
                    keyword_ids=selected_criteria['keywords'],
                    theme_ids=selected_criteria['themes'],
                    perspective_ids=selected_criteria['perspectives'],
                    developer_ids=selected_criteria['developers'],
                    game_mode_ids=selected_criteria['game_modes']
                )

                # Находим похожие игры (как на странице списка)
                similar_games = similarity_engine.find_similar_games(
                    source_game=virtual_game,
                    min_similarity=0,  # Минимальный порог
                    limit=1000
                )

                # Ищем game2 в результатах
                for game_data in similar_games:
                    if isinstance(game_data, dict) and game_data.get('game') and game_data['game'].id == game2.id:
                        similarity_score = game_data.get('similarity', 0)
                        break
                    elif hasattr(game_data, 'id') and game_data.id == game2.id:
                        similarity_score = getattr(game_data, 'similarity', 0)
                        break

                # Если не нашли в результатах, рассчитываем напрямую
                if similarity_score == 0:
                    similarity_score = similarity_engine.calculate_similarity(virtual_game, game2)
            else:
                # Для game1 vs game2 - находим через поиск похожих игр для game1
                similar_games = similarity_engine.find_similar_games(
                    source_game=game1,
                    min_similarity=0,
                    limit=1000
                )

                # Ищем game2 в результатах
                for game_data in similar_games:
                    if isinstance(game_data, dict) and game_data.get('game') and game_data['game'].id == game2.id:
                        similarity_score = game_data.get('similarity', 0)
                        break
                    elif hasattr(game_data, 'id') and game_data.id == game2.id:
                        similarity_score = getattr(game_data, 'similarity', 0)
                        break

                # Если не нашли в результатах, рассчитываем напрямую
                if similarity_score == 0:
                    similarity_score = similarity_engine.calculate_similarity(game1, game2)

        # Получаем breakdown только если нужно его показывать
        breakdown = None
        if similarity_score > 0:  # Получаем breakdown только если есть схожесть
            similarity_engine = GameSimilarity()
            breakdown = similarity_engine.get_similarity_breakdown(source if is_criteria_comparison else game1, game2)

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

        # Подготовка контекста
        context = {
            'game1': game1,
            'game2': game2,
            'similarity_score': similarity_score,
            'is_criteria_comparison': is_criteria_comparison,
            'breakdown': breakdown,
            'selected_criteria': selected_criteria,

            # Критерии для отображения на карточке
            'criteria_genres': list(criteria_genres_objs),
            'criteria_keywords': list(criteria_keywords_objs),
            'criteria_themes': list(criteria_themes_objs),
            'criteria_perspectives': list(criteria_perspectives_objs),
            'criteria_developers': list(criteria_developers_objs),
            'criteria_game_modes': list(criteria_game_modes_objs),

            # ID критериев
            'criteria_genres_ids': selected_criteria['genres'],
            'criteria_keywords_ids': selected_criteria['keywords'],
            'criteria_themes_ids': selected_criteria['themes'],
            'criteria_perspectives_ids': selected_criteria['perspectives'],
            'criteria_developers_ids': selected_criteria['developers'],
            'criteria_game_modes_ids': selected_criteria['game_modes'],
        }

        # Для совместимости с тегами создаем виртуальную игру
        if is_criteria_comparison:
            # Создаем класс-обертку для критериев
            class CriteriaWrapper:
                def __init__(self, criteria):
                    self.id = None
                    self.name = "Search Criteria"
                    self.genres = criteria['genres']
                    self.keywords = criteria['keywords']
                    self.themes = criteria['themes']
                    self.player_perspectives = criteria['perspectives']
                    self.developers = criteria['developers']
                    self.game_modes = criteria['game_modes']

                def genres_list(self):
                    return self.genres

                def keywords_list(self):
                    return self.keywords

                def themes_list(self):
                    return self.themes

                def perspectives_list(self):
                    return self.player_perspectives

                def developers_list(self):
                    return self.developers

                def game_modes_list(self):
                    return self.game_modes

            context['criteria_virtual_game'] = CriteriaWrapper(selected_criteria)
        else:
            context['criteria_virtual_game'] = game1

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

        # Средняя схожесть
        if is_criteria_comparison:
            context['average_similarity'] = similarity_score
        else:
            context['average_similarity'] = None

        return render(request, 'games/game_comparison.html', context)

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Error in comparison: {str(e)}")
        logger.error(f"Details: {error_details}")
        return HttpResponseServerError(f"Error in comparison: {str(e)}")


def get_similar_games_for_criteria(selected_criteria: Dict[str, List[int]]) -> Tuple[List, int]:
    """Get similar games for criteria - с поддержкой поиска без жанров."""
    import json

    # Быстрый хеш для кэша
    cache_data = json.dumps({
        'g': selected_criteria['genres'],
        'k': selected_criteria['keywords'],
        't': selected_criteria['themes'],
        'pp': selected_criteria['perspectives'],
        'd': selected_criteria['developers'],
        'gm': selected_criteria['game_modes'],
        'version': 'v16_clientside_pagination'  # Обновляем версию
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

    # Адаптируем минимальную схожесть в зависимости от критериев
    min_similarity = 10  # Базовый порог

    # Логирование информации о критериях
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

    # Если только режимы игры (без жанров)
    if has_game_modes and not has_genres and not has_keywords and not has_themes and not has_perspectives:
        min_similarity = 1  # Очень низкий порог для поиска по режимам
        logger.info(f"Только режимы игры, порог схожести: {min_similarity}%")

    # Если нет жанров, но есть другие критерии
    elif not has_genres and any([has_keywords, has_themes, has_perspectives, has_game_modes]):
        min_similarity = 3  # Низкий порог для поиска по другим критериям
        logger.info(f"Поиск без жанров, порог схожести: {min_similarity}%")

    # Если вообще нет критериев похожести
    elif not any([has_genres, has_keywords, has_themes, has_perspectives, has_game_modes]):
        min_similarity = 0  # Минимальный порог
        logger.info(f"Нет критериев похожести, порог схожести: {min_similarity}%")

    # Увеличиваем лимит для клиентской пагинации
    similar_games = similarity_engine.find_similar_games(
        source_game=virtual_game,
        min_similarity=min_similarity,
        limit=500  # Увеличили лимит с 1000 до 500 (для производительности)
    )

    total_count = len(similar_games)

    # Кэшируем
    cache_time = 10800  # 3 часа для запросов без жанров
    if has_genres:
        cache_time = 7200  # 2 часа для запросов с жанрами

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


@lru_cache(maxsize=1)
def get_release_years_range():
    """Get min and max release years from database."""
    cache_key = 'release_years_range_v1'
    cached_data = cache.get(cache_key)

    if cached_data:
        return cached_data

    try:
        from django.db.models import Min, Max

        # Получаем минимальный и максимальный год из игр с непустой датой выхода
        years_range = Game.objects.filter(
            first_release_date__isnull=False
        ).aggregate(
            min_year=Min('first_release_date__year'),
            max_year=Max('first_release_date__year')
        )

        min_year = years_range['min_year'] or 1970
        max_year = years_range['max_year'] or timezone.now().year

        # Убедимся что это целые числа
        min_year = int(min_year)
        max_year = int(max_year)

        result = {
            'min_year': min_year,
            'max_year': max_year,
        }

        cache.set(cache_key, result, CACHE_TIMES['years_range'])
        return result

    except Exception as e:
        logger.error(f"Error getting release years range: {e}")
        current_year = timezone.now().year
        return {'min_year': 1970, 'max_year': current_year}


# Обновите _apply_filters для поддержки фильтра по дате:
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
        ('game_modes', 'game_modes__id__in'),
        ('game_types', 'game_type__in')
    ]

    for field, model_field in other_fields:
        if selected_criteria[field]:
            main_filters &= Q(**{model_field: selected_criteria[field]})
            has_main_filters = True

    # Фильтр по диапазону годов
    year_start = selected_criteria.get('release_year_start')
    year_end = selected_criteria.get('release_year_end')

    if year_start or year_end:
        # Создаем фильтр по дате
        date_filter = Q()

        if year_start:
            # Начало года (1 января year_start)
            start_date = f"{year_start}-01-01"
            date_filter &= Q(first_release_date__gte=start_date)

        if year_end:
            # Конец года (31 декабря year_end)
            end_date = f"{year_end}-12-31"
            date_filter &= Q(first_release_date__lte=end_date)

        main_filters &= date_filter
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


def _generate_cache_key(data: Dict) -> str:
    """Generate cache key from data."""
    cache_key_str = ''.join(f"{k}:{v}" for k, v in sorted(data.items()))
    return f"cache_{hashlib.md5(cache_key_str.encode()).hexdigest()}"


# ===== SIMILARITY FUNCTIONS =====

def get_similar_games_for_game(game_obj: Game, selected_platforms: List[int]) -> Tuple[List, int]:
    """Get similar games for a specific game without limits."""
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
            limit=500  # Увеличили лимит
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
        # Загружаем все игры одним запросом
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


# ===== MAIN VIEWS =====


def _get_cached_filter_data() -> Dict[str, List]:
    """Получаем кэшированные данные для всех фильтров."""
    filter_data = cache.get('optimized_filter_data_v6')  # Увеличиваем версию кэша до v6

    if not filter_data:
        # Получаем ВСЕ данные, не только популярные
        filter_data = {
            # Платформы - с количеством игр для сортировки
            'platforms': list(Platform.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name', 'slug')
                              .order_by('-game_count', 'name')),

            # Ключевые слова - ВСЕ без ограничений
            'keywords': list(Keyword.objects.all()
                             .select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            ).order_by('name')),  # УБРАЛИ [:200] - теперь ВСЕ ключевые слова

            # Популярные ключевые слова для отображения вверху
            'popular_keywords': list(Keyword.objects.filter(
                cached_usage_count__gt=0
            ).select_related('category').only(
                'id', 'name', 'category__id', 'category__name', 'cached_usage_count'
            ).order_by('-cached_usage_count')[:50]),

            # ВСЕ режимы игры
            'game_modes': list(GameMode.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name').order_by('name')),

            # ВСЕ темы
            'themes': list(Theme.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name').order_by('name')),

            # ВСЕ перспективы
            'perspectives': list(PlayerPerspective.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name').order_by('name')),

            'developers': list(Company.objects.annotate(
                developed_game_count=Count('developed_games', distinct=True)
            ).filter(developed_game_count__gt=0).only('id', 'name').order_by('name')),
        }
        cache.set('optimized_filter_data_v6', filter_data, 7200)  # 2 часа кэша

    return filter_data


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
