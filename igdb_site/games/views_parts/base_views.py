"""Base views with common functions and configurations."""

# ===== STANDARD IMPORTS =====
import time
import json
import hashlib
import logging
from functools import lru_cache, wraps
from typing import Dict, List, Tuple, Any, Optional, Union
from urllib.parse import urlencode

# ===== DJANGO IMPORTS =====
from django.shortcuts import render, get_object_or_404
from django.db.models import Count, Prefetch, Q
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.core.cache import cache
from django.http import HttpRequest, HttpResponse, HttpResponseServerError, JsonResponse
from django.db import models
from django.utils import timezone
from datetime import timedelta
from django.template.loader import render_to_string

# ===== LOCAL IMPORTS =====
from ..similarity import GameSimilarity, VirtualGame
from ..models import Game, Genre, Keyword, KeywordCategory, Platform, Theme, PlayerPerspective, Company, Series, \
    GameMode, GameTypeEnum, GameEngine

from ..helpers import generate_compact_url_params

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
CACHE_VERSION = 'v21'

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
            self._engine_ids = criteria.get('engines', []) if criteria else []  # ДОБАВЛЕНО

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

        # Cache engine IDs
        if hasattr(self.game_obj, '_cached_engine_ids'):
            self._engine_ids = self.game_obj._cached_engine_ids
        elif hasattr(self.game_obj, 'engines') and hasattr(self.game_obj.engines, 'all'):
            self._engine_ids = [e.id for e in self.game_obj.engines.all()]
        else:
            self._engine_ids = []

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

    def engines_list(self):
        """Return list of engine IDs for template tags."""
        return self._engine_ids

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

    @property
    def engines_ids(self):
        """Get engine IDs (alias for compatibility)."""
        return self._engine_ids

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


# ===== HELPER FUNCTIONS =====

def extract_request_params(request: HttpRequest) -> Dict[str, str]:
    """Extract parameters from request efficiently."""
    get_params = request.GET

    params = {}
    # Добавьте 'e' в список параметров
    for key in ['find_similar', 'g', 'k', 'p', 't', 'pp', 'd', 'gm', 'gt', 'yr', 'ys', 'ye', 'source_game', 'sort',
                'page', 'e']:
        value = get_params.get(key, '')
        params[key] = value

    return params


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

    # Parse all parameters
    result = {
        'genres': parse_int_list(params_dict.get('g')),
        'keywords': parse_int_list(params_dict.get('k')),
        'platforms': parse_int_list(params_dict.get('p')),
        'themes': parse_int_list(params_dict.get('t')),
        'perspectives': parse_int_list(params_dict.get('pp')),
        'developers': parse_int_list(params_dict.get('d')),
        'game_modes': parse_int_list(params_dict.get('gm')),
        'game_types': parse_int_list(params_dict.get('gt')),
        'engines': parse_int_list(params_dict.get('e')),  # Параметр 'e' для движков
        'release_years': [],
        'release_year_start': release_year_start,
        'release_year_end': release_year_end,
    }

    # Отладка
    print(f"DEBUG convert_params_to_lists: e = '{params_dict.get('e')}'")
    print(f"DEBUG convert_params_to_lists: engines result = {result['engines']}")

    return result


def _apply_filters(queryset: models.QuerySet, selected_criteria: Dict[str, List[int]]) -> models.QuerySet:
    """
    Apply filters to queryset with OR logic for platforms.
    """
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
        ('game_types', 'game_type__in'),
        ('engines', 'engines__id__in'),
    ]

    for field, model_field in other_fields:
        field_value = selected_criteria.get(field, [])
        if field_value:
            print(f"DEBUG _apply_filters: applying filter {field} = {field_value}")
            main_filters &= Q(**{model_field: field_value})
            has_main_filters = True

    # Фильтр по диапазону годов
    year_start = selected_criteria.get('release_year_start')
    year_end = selected_criteria.get('release_year_end')

    if year_start or year_end:
        date_filter = Q()
        if year_start:
            start_date = f"{year_start}-01-01"
            date_filter &= Q(first_release_date__gte=start_date)
        if year_end:
            end_date = f"{year_end}-12-31"
            date_filter &= Q(first_release_date__lte=end_date)
        main_filters &= date_filter
        has_main_filters = True

    # Применяем фильтры
    if has_platform_filter and has_main_filters:
        queryset = queryset.filter(platform_filter & main_filters).distinct()
    elif has_platform_filter:
        queryset = queryset.filter(platform_filter).distinct()
    elif has_main_filters:
        queryset = queryset.filter(main_filters).distinct()

    return queryset


def _apply_search_filters(queryset: models.QuerySet, search_filters: Dict[str, List[int]]) -> models.QuerySet:
    """
    Apply search filters with logic:
    - BETWEEN different filter groups: AND (must satisfy ALL groups)
    - WITHIN a single group with multiple values: OR (any of selected)
    - TEXT search: name__icontains for text_query

    Example: platforms OR, engines AND, genres AND, text search
    Result: (platforms) AND (engines) AND (genres) AND (name contains text)
    """

    # Собираем Q объекты для каждой группы фильтров
    filter_groups = []

    # ========== ТЕКСТОВЫЙ ПОИСК ПО НАЗВАНИЮ ==========
    text_query = search_filters.get('text_query')
    if text_query:
        text_filter = Q(name__icontains=text_query)
        filter_groups.append(text_filter)
        print(f"DEBUG _apply_search_filters: text search filter: '{text_query}'")

    # ========== ГРУППА: Платформы (OR внутри группы) ==========
    if search_filters.get('platforms'):
        platforms_filter = Q(platforms__id__in=search_filters['platforms'])
        filter_groups.append(platforms_filter)
        print(f"DEBUG _apply_search_filters: platforms OR filter: {search_filters['platforms']}")

    # ========== ГРУППА: Игровые типы (OR внутри группы) ==========
    if search_filters.get('game_types'):
        game_types_filter = Q(game_type__in=search_filters['game_types'])
        filter_groups.append(game_types_filter)
        print(f"DEBUG _apply_search_filters: game_types OR filter: {search_filters['game_types']}")

    # ========== ГРУППА: Перспективы (OR внутри группы) ==========
    if search_filters.get('perspectives'):
        perspectives_filter = Q(player_perspectives__id__in=search_filters['perspectives'])
        filter_groups.append(perspectives_filter)
        print(f"DEBUG _apply_search_filters: perspectives OR filter: {search_filters['perspectives']}")

    # ========== ГРУППА: Режимы игры (OR внутри группы) ==========
    if search_filters.get('game_modes'):
        game_modes_filter = Q(game_modes__id__in=search_filters['game_modes'])
        filter_groups.append(game_modes_filter)
        print(f"DEBUG _apply_search_filters: game_modes OR filter: {search_filters['game_modes']}")

    # ========== ГРУППА: Движки (OR внутри группы) ==========
    if search_filters.get('engines'):
        engines_filter = Q(engines__id__in=search_filters['engines'])
        filter_groups.append(engines_filter)
        print(f"DEBUG _apply_search_filters: engines OR filter: {search_filters['engines']}")

    # ========== ГРУППА: Жанры (AND внутри группы) ==========
    if search_filters.get('genres'):
        genres_filter = Q()
        for genre_id in search_filters['genres']:
            genres_filter &= Q(genres__id=genre_id)
        filter_groups.append(genres_filter)
        print(f"DEBUG _apply_search_filters: genres AND filter: {search_filters['genres']}")

    # ========== ГРУППА: Ключевые слова (AND внутри группы) ==========
    if search_filters.get('keywords'):
        keywords_filter = Q()
        for keyword_id in search_filters['keywords']:
            keywords_filter &= Q(keywords__id=keyword_id)
        filter_groups.append(keywords_filter)
        print(f"DEBUG _apply_search_filters: keywords AND filter: {search_filters['keywords']}")

    # ========== ГРУППА: Темы (AND внутри группы) ==========
    if search_filters.get('themes'):
        themes_filter = Q()
        for theme_id in search_filters['themes']:
            themes_filter &= Q(themes__id=theme_id)
        filter_groups.append(themes_filter)
        print(f"DEBUG _apply_search_filters: themes AND filter: {search_filters['themes']}")

    # ========== ГРУППА: Дата (AND) ==========
    year_start = search_filters.get('release_year_start')
    year_end = search_filters.get('release_year_end')

    if year_start or year_end:
        date_filter = Q()
        if year_start:
            start_date = f"{year_start}-01-01"
            date_filter &= Q(first_release_date__gte=start_date)
        if year_end:
            end_date = f"{year_end}-12-31"
            date_filter &= Q(first_release_date__lte=end_date)
        filter_groups.append(date_filter)
        print(f"DEBUG _apply_search_filters: date filter: {year_start}-{year_end}")

    # ========== ПРИМЕНЯЕМ ВСЕ ГРУППЫ С ЛОГИКОЙ AND ==========
    if filter_groups:
        # Объединяем все группы через AND
        combined_filter = Q()
        for group_filter in filter_groups:
            combined_filter &= group_filter

        queryset = queryset.filter(combined_filter).distinct()
        print(f"DEBUG _apply_search_filters: applied {len(filter_groups)} filter groups with AND logic")
    else:
        print(f"DEBUG _apply_search_filters: no filters to apply")

    print(f"DEBUG _apply_search_filters: final count = {queryset.count()}")

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


def _format_similar_games_data(similar_games_data: List) -> List[Dict[str, Any]]:
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

    # Форматируем в структуру, ожидаемую шаблоном
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

        # ВАЖНО: добавляем similarity непосредственно к объекту игры
        if similarity:
            game.similarity = similarity

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


# ===== CACHE MANAGEMENT FUNCTIONS =====

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
        'game_list_data_first_page',
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
    stats = {
        'total_keys_estimated': 'N/A',
        'memory_usage': 'N/A',
        'hit_rate': 'N/A',
    }

    try:
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


@lru_cache(maxsize=1)
def get_release_years_range():
    """Get min and max release years from database."""
    cache_key = 'release_years_range_v1'
    cached_data = cache.get(cache_key)

    if cached_data:
        return cached_data

    try:
        from django.db.models import Min, Max

        years_range = Game.objects.filter(
            first_release_date__isnull=False
        ).aggregate(
            min_year=Min('first_release_date__year'),
            max_year=Max('first_release_date__year')
        )

        min_year = years_range['min_year'] or 1970
        max_year = years_range['max_year'] or timezone.now().year

        min_year = int(min_year)
        max_year = int(max_year)

        result = {
            'min_year': min_year,
            'max_year': max_year,
        }

        cache.set(cache_key, result, CACHE_TIMES['short']['years_range'])
        return result

    except Exception as e:
        logger.error(f"Error getting release years range: {e}")
        current_year = timezone.now().year
        return {'min_year': 1970, 'max_year': current_year}


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
    cache_key = get_cache_key('optimized_filter_data_v7')

    def fetch_filter_data():
        from django.db.models import Prefetch, Count
        from ..models import GameEngine

        platforms = list(Platform.objects.annotate(
            game_count=Count('game', distinct=True)
        ).filter(game_count__gt=0).only('id', 'name', 'slug')
                         .order_by('-game_count', 'name'))

        keywords = list(Keyword.objects.all()
                        .select_related('category').only(
            'id', 'name', 'category__id', 'category__name'
        ).order_by('name'))

        popular_keywords = list(Keyword.objects.filter(
            cached_usage_count__gt=0
        ).select_related('category').only(
            'id', 'name', 'category__id', 'category__name', 'cached_usage_count'
        ).order_by('-cached_usage_count')[:50])

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

        engines = list(GameEngine.objects.annotate(
            game_count=Count('games', distinct=True)
        ).filter(game_count__gt=0).only('id', 'name').order_by('-game_count', 'name'))

        return {
            'platforms': platforms,
            'keywords': keywords,
            'popular_keywords': popular_keywords,
            'themes': themes,
            'perspectives': perspectives,
            'game_modes': game_modes,
            'developers': developers,
            'engines': engines,
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
        3600,
        cache_group='games'
    )


def _get_cached_genres_list() -> List:
    """Get genres list with caching."""
    cache_key = get_cache_key('genres_list')

    return cache_get_or_set(
        cache_key,
        lambda: list(Genre.objects.all().only('id', 'name').order_by('name')),
        CACHE_TIMES['short']['genres_list'],
        cache_group='static_data'
    )


def _get_cached_filter_data() -> Dict[str, List]:
    """Получаем кэшированные данные для всех фильтров."""
    filter_data = cache.get('optimized_filter_data_v6')

    if not filter_data:
        from ..models import GameEngine

        filter_data = {
            'platforms': list(Platform.objects.annotate(
                game_count=Count('game', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name', 'slug')
                              .order_by('-game_count', 'name')),

            'keywords': list(Keyword.objects.all()
                             .select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            ).order_by('name')),

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

            'engines': list(GameEngine.objects.annotate(
                game_count=Count('games', distinct=True)
            ).filter(game_count__gt=0).only('id', 'name').order_by('-game_count', 'name')),
        }
        cache.set('optimized_filter_data_v6', filter_data, 7200)

    return filter_data
