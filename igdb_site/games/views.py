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

CACHE_TIMES = {
    'filter_data': 3600,
    'similar_games': 172800,
    'filtered_games': 1800,
    'full_page': 900,
    'genres_list': 7200,
    'static_pages': 3600,
    'years_range': 86400,  # 24 часа для диапазона годов
}

_EMPTY_RESULT = {
    'genres': [],
    'keywords': [],
    'platforms': [],
    'themes': [],
    'perspectives': [],
    'developers': [],
    'game_modes': [],
    'game_types': [],
    'release_years': [],  # ДОБАВЛЕНО
    'release_year_start': None,  # ДОБАВЛЕНО
    'release_year_end': None,  # ДОБАВЛЕНО
}

ITEMS_PER_PAGE = {
    'similar': 20,
    'regular': 20,
    'platform': 20,
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


# ===== CORE UTILITY FUNCTIONS =====

# В views.py, обновите функцию game_list:

# В views.py, обновляем функцию should_find_similar:

# В views.py, обновляем get_similar_games_for_criteria:

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
    """Режим отображения ВСЕХ игр с фильтрами."""
    # Оптимизированный запрос для всех игр
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
        'source_game': None,
    }


def _get_similar_games_mode(params: Dict[str, str], selected_criteria: Dict[str, List[int]],
                            source_game_obj: Optional[Game]) -> Dict[str, Any]:
    """Режим похожих игр с поддержкой поиска без жанров."""
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

    # Форматируем
    games_with_similarity = _format_similar_games_data(similar_games_data)
    logger.info(f"После форматирования: {len(games_with_similarity)} игр")

    # Сортируем
    _sort_similar_games(games_with_similarity, current_sort)

    # Пагинация
    page_obj, paginator, is_paginated = _paginate_results(
        games_with_similarity, page_number, 16
    )

    logger.info(f"Режим похожих игр завершен. Результатов: {total_count}, показано: {len(page_obj.object_list)}")

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


def game_list(request: HttpRequest) -> HttpResponse:
    """Main game list function - с приоритетом на кэширование и поддержкой поиска без жанров."""
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
        cache_key_long = f'game_list_long_{hashlib.md5(request.GET.urlencode()[:500].encode()).hexdigest()}'
        cached_long = cache.get(cache_key_long)

        if cached_long:
            cached_long['X-Cache-Hit'] = 'True-Long'
            cached_long['X-Params-Length'] = str(total_params)
            return cached_long

    selected_criteria = convert_params_to_lists(params)

    # 4. Получаем объекты для всех выбранных критериев
    selected_criteria_objects = _get_selected_criteria_objects(selected_criteria)

    # 5. Получаем диапазон годов выпуска
    years_range = get_release_years_range()

    # 6. Определяем режим
    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = Game.objects.only('id', 'name', 'game_type').get(pk=int(params['source_game']))
        except (Game.DoesNotExist, ValueError):
            pass

    # 7. ВЫБОР РЕЖИМА с приоритетом на кэширование
    should_use_similar_mode = False

    # Если явно запрошен поиск похожих
    if find_similar:
        should_use_similar_mode = True
        logger.debug("Режим похожих игр: явно запрошен (find_similar=1)")

    # Если есть исходная игра
    elif source_game_obj:
        should_use_similar_mode = True
        logger.debug(f"Режим похожих игр: есть исходная игра {source_game_obj.id}")

    # Если есть критерии похожести (жанры ИЛИ другие критерии)
    else:
        has_genres = bool(selected_criteria['genres'])
        has_other_criteria = any([
            bool(selected_criteria['keywords']),
            bool(selected_criteria['themes']),
            bool(selected_criteria['perspectives']),
            bool(selected_criteria['game_modes'])
        ])

        should_use_similar_mode = has_genres or has_other_criteria

        logger.debug(
            f"Режим похожих игр проверка: жанры={has_genres}, другие критерии={has_other_criteria}, результат={should_use_similar_mode}")

    # Выбираем режим
    if should_use_similar_mode:
        mode_result = _get_similar_games_mode(params, selected_criteria, source_game_obj)
        mode = 'similar'

        # ДОБАВЛЯЕМ: Создаем словарь со схожестями для всех игр на странице
        similarity_map = {}
        if mode_result.get('games_with_similarity'):
            for game_data in mode_result['games_with_similarity']:
                game = game_data.get('game')
                similarity = game_data.get('similarity', 0)
                if game and hasattr(game, 'id'):
                    similarity_map[game.id] = similarity
        mode_result['similarity_map'] = similarity_map

    else:
        mode_result = _get_all_games_mode(
            selected_criteria,
            params.get('sort', '-rating_count'),
            params.get('page', '1')
        )
        mode = 'regular'
        mode_result['similarity_map'] = {}  # Пустой словарь для обычного режима

    # 8. БЫСТРАЯ загрузка фильтров из кэша
    filter_data = _get_cached_filter_data()

    # ВАЖНОЕ ИСПРАВЛЕНИЕ: Получаем полные списки для чекбоксов
    genres_list = cache.get('genres_list_full_v1')
    if not genres_list:
        # Получаем ВСЕ жанры, не только популярные
        genres_list = list(Genre.objects.all().only('id', 'name').order_by('name'))
        cache.set('genres_list_full_v1', genres_list, CACHE_TIMES['genres_list'])

    # Получаем все темы
    themes_list = cache.get('themes_list_full_v1')
    if not themes_list:
        themes_list = list(Theme.objects.all().only('id', 'name').order_by('name'))
        cache.set('themes_list_full_v1', themes_list, CACHE_TIMES['genres_list'])

    # Получаем все перспективы
    perspectives_list = cache.get('perspectives_list_full_v1')
    if not perspectives_list:
        perspectives_list = list(PlayerPerspective.objects.all().only('id', 'name').order_by('name'))
        cache.set('perspectives_list_full_v1', perspectives_list, CACHE_TIMES['genres_list'])

    # Получаем все режимы игры
    game_modes_list = cache.get('game_modes_list_full_v1')
    if not game_modes_list:
        game_modes_list = list(GameMode.objects.all().only('id', 'name').order_by('name'))
        cache.set('game_modes_list_full_v1', game_modes_list, CACHE_TIMES['genres_list'])

    # Получаем все ключевые слова (для чекбоксов)
    # Используем уже загруженные из filter_data['keywords'] которые теперь ВСЕ
    keywords_list = filter_data['keywords']

    # 9. Минимальный контекст для скорости
    context = {
        'games': mode_result.get('games', []),
        'games_with_similarity': mode_result.get('games_with_similarity', []),
        'page_obj': mode_result.get('page_obj'),
        'is_paginated': mode_result.get('is_paginated', False),
        'total_count': mode_result.get('total_count', 0),

        'find_similar': mode_result.get('find_similar', False),
        'show_similarity': mode_result.get('show_similarity', False),
        'source_game': mode_result.get('source_game'),
        'source_game_obj': mode_result.get('source_game_obj'),

        # ДОБАВЛЯЕМ: словарь схожестей в контекст
        'similarity_map': mode_result.get('similarity_map', {}),

        # ВАЖНОЕ ИСПРАВЛЕНИЕ: Используем полные списки для чекбоксов
        'genres': genres_list,  # ВСЕ жанры
        'themes': themes_list,  # ВСЕ темы
        'perspectives': perspectives_list,  # ВСЕ перспективы
        'game_modes': game_modes_list,  # ВСЕ режимы игры

        # ВСЕ ключевые слова (для чекбоксов)
        'keywords': keywords_list,  # ВСЕ ключевые слова

        # Платформы и популярные ключевые слова (для отображения вверху)
        'platforms': filter_data['platforms'],
        'popular_keywords': filter_data['popular_keywords'],  # Для бейджей или другого отображения
        'game_types': GameTypeEnum.CHOICES,

        # Диапазон годов
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

        # Selected criteria OBJECTS
        'selected_genres_objects': selected_criteria_objects.get('genres', []),
        'selected_keywords_objects': selected_criteria_objects.get('keywords', []),
        'selected_platforms_objects': selected_criteria_objects.get('platforms', []),
        'selected_themes_objects': selected_criteria_objects.get('themes', []),
        'selected_perspectives_objects': selected_criteria_objects.get('perspectives', []),
        'selected_game_modes_objects': selected_criteria_objects.get('game_modes', []),

        'current_sort': params.get('sort', ''),
        'execution_time': round(time.time() - start_time, 3),

        # Дополнительная информация для отладки
        'debug_info': {
            'mode': mode,
            'has_genres': bool(selected_criteria['genres']),
            'genre_count': len(selected_criteria['genres']),
            'has_keywords': bool(selected_criteria['keywords']),
            'keyword_count': len(selected_criteria['keywords']),
            'keywords_total': len(keywords_list),  # Исправлено: используем keywords_list
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
            'themes_total': len(themes_list),
            'perspectives_total': len(perspectives_list),
            'game_modes_total': len(game_modes_list),
        }
    }

    # Добавляем developers если они нужны в шаблоне
    context['selected_developers'] = selected_criteria['developers']
    context['selected_developers_objects'] = selected_criteria_objects.get('developers', [])

    # Добавляем активный бейдж фильтра даты если выбраны годы
    if selected_criteria['release_year_start'] or selected_criteria['release_year_end']:
        context['has_date_filter'] = True
    else:
        context['has_date_filter'] = False

    # 10. Рендерим
    response = render(request, 'games/game_list.html', context)

    # 11. АГРЕССИВНОЕ КЭШИРОВАНИЕ
    cache_time = 300  # 5 минут по умолчанию

    if context['execution_time'] > 1.0:
        cache_time = 600  # 10 минут
        response['X-Cache-Reason'] = 'Slow-Query'

    if mode == 'similar' and not selected_criteria['genres']:
        cache_time = 1800  # 30 минут для поиска без жанров
        response['X-Cache-Reason'] = 'Similar-No-Genres'

    cache.set(cache_key_simple, response, cache_time)

    if total_params > 1000:
        cache.set(cache_key_long, response, cache_time)

    response['X-Cache-Hit'] = 'False'
    response['X-Response-Time'] = f"{context['execution_time']:.3f}s"
    response['X-Mode'] = mode
    response['X-Similarity-Criteria'] = str(should_use_similar_mode)

    return response


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
        'version': 'v15_similar_no_genres_final'  # Обновляем версию
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

    similar_games = similarity_engine.find_similar_games(
        source_game=virtual_game,
        min_similarity=min_similarity,
        limit=1000
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

def should_find_similar(params: Dict[str, str], selected_criteria: Dict[str, List[int]]) -> bool:
    """Determine if similar games search should be performed."""
    if params.get('find_similar') == '1':
        return True

    # Проверяем наличие исходной игры
    if params.get('source_game'):
        return True

    # Проверяем критерии похожести
    # Теперь жанры не обязательны - достаточно других критериев
    similarity_criteria = [
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes'],
        selected_criteria['genres']  # Жанры тоже считаются критерием похожести
    ]

    # Если есть хотя бы один критерий похожести
    return any(similarity_criteria)


def has_similarity_criteria(selected_criteria: Dict[str, List[int]]) -> bool:
    """Check if there are criteria for similarity search."""
    # Включаем все критерии похожести, включая жанры
    similarity_criteria = [
        selected_criteria['genres'],  # Теперь жанры тоже считаются
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes']
    ]

    return any(similarity_criteria)


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


# Обновите extract_request_params:
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
        'gt': get_params.get('gt', ''),
        'yr': get_params.get('yr', ''),  # ДОБАВЛЕНО: диапазон годов (например: "1990-2020")
        'ys': get_params.get('ys', ''),  # ДОБАВЛЕНО: год начала
        'ye': get_params.get('ye', ''),  # ДОБАВЛЕНО: год окончания
        'source_game': get_params.get('source_game', ''),
        'sort': get_params.get('sort', ''),
        'page': get_params.get('page', '1'),
    }


# Обновите convert_params_to_lists:
def convert_params_to_lists(params_dict: Dict[str, str]) -> Dict[str, List[int]]:
    """Convert query parameters to lists of integers."""
    # Quick check for empty params
    has_params = False
    for key in ['g', 'k', 'p', 't', 'pp', 'd', 'gm', 'gt', 'yr', 'ys', 'ye']:  # ДОБАВЛЕНО yr, ys, ye
        if params_dict.get(key):
            has_params = True
            break

    if not has_params:
        return _EMPTY_RESULT.copy()

    # Обработка диапазона годов
    release_years = []
    release_year_start = None
    release_year_end = None

    year_range = params_dict.get('yr', '')
    if year_range and '-' in year_range:
        try:
            parts = year_range.split('-')
            release_year_start = int(parts[0].strip()) if parts[0].strip() else None
            release_year_end = int(parts[1].strip()) if parts[1].strip() else None
        except (ValueError, IndexError):
            pass

    # Альтернативный формат: отдельные параметры
    if not release_year_start and params_dict.get('ys'):
        try:
            release_year_start = int(params_dict.get('ys'))
        except ValueError:
            pass

    if not release_year_end and params_dict.get('ye'):
        try:
            release_year_end = int(params_dict.get('ye'))
        except ValueError:
            pass

    return {
        'genres': _cached_string_to_int_list(params_dict.get('g', '')),
        'keywords': _cached_string_to_int_list(params_dict.get('k', '')),
        'platforms': _cached_string_to_int_list(params_dict.get('p', '')),
        'themes': _cached_string_to_int_list(params_dict.get('t', '')),
        'perspectives': _cached_string_to_int_list(params_dict.get('pp', '')),
        'developers': _cached_string_to_int_list(params_dict.get('d', '')),
        'game_modes': _cached_string_to_int_list(params_dict.get('gm', '')),
        'game_types': _cached_string_to_int_list(params_dict.get('gt', '')),
        'release_years': release_years,
        'release_year_start': release_year_start,
        'release_year_end': release_year_end,
    }


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


# Обновите _build_context для добавления информации о годах:
def _build_context(mode: str, **kwargs) -> Dict[str, Any]:
    """Build template context."""
    # Get genres from cache
    genres_list = cache.get('genres_list')
    if not genres_list:
        genres_list = list(Genre.objects.all().only('id', 'name').order_by('name'))
        cache.set('genres_list', genres_list, CACHE_TIMES['genres_list'])

    # Get years range
    years_range = get_release_years_range()

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
        game_types=selected_criteria['game_types'],
        release_year_start=selected_criteria['release_year_start'],
        release_year_end=selected_criteria['release_year_end'],
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

        # Years range for date filter
        'years_range': years_range,
        'current_year': timezone.now().year,

        # Selected criteria IDs
        'selected_genres': selected_criteria['genres'],
        'selected_keywords': selected_criteria['keywords'],
        'selected_platforms': selected_criteria['platforms'],
        'selected_themes': selected_criteria['themes'],
        'selected_perspectives': selected_criteria['perspectives'],
        'selected_developers': selected_criteria['developers'],
        'selected_game_modes': selected_criteria['game_modes'],
        'selected_game_types': selected_criteria['game_types'],
        'selected_release_year_start': selected_criteria['release_year_start'],
        'selected_release_year_end': selected_criteria['release_year_end'],

        # Selected criteria OBJECTS
        'selected_genres_objects': selected_criteria_objects.get('genres', []),
        'selected_keywords_objects': selected_criteria_objects.get('keywords', []),
        'selected_platforms_objects': selected_criteria_objects.get('platforms', []),
        'selected_themes_objects': selected_criteria_objects.get('themes', []),
        'selected_perspectives_objects': selected_criteria_objects.get('perspectives', []),
        'selected_developers_objects': selected_criteria_objects.get('developers', []),
        'selected_game_modes_objects': selected_criteria_objects.get('game_modes', []),
        'selected_game_types_objects': selected_criteria_objects.get('game_types', []),

        # Search Filters данные
        'platforms': filter_data.get('platforms', []),
        'game_types': GameTypeEnum.CHOICES,

        # Similarity Filters данные
        'popular_keywords': filter_data.get('popular_keywords', []),
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
        return []  # Всегда возвращаем пустой список, а не None

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

    return formatted  # Всегда возвращаем список (может быть пустым)


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
    from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

    if not data:
        empty_paginator = Paginator([], items_per_page)
        return empty_paginator.page(1), empty_paginator, False

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


# ===== FILTER DATA FUNCTIONS =====

@lru_cache(maxsize=1)
def get_filter_data() -> Dict[str, List]:
    """Get filter data with caching."""
    cache_key = 'game_list_filters_data_v4'  # Обновлена версия
    filter_data = cache.get(cache_key)

    if filter_data:
        return filter_data

    filter_data = _fetch_filter_data_from_db()
    cache.set(cache_key, filter_data, CACHE_TIMES['filter_data'])
    return filter_data


def _fetch_filter_data_from_db() -> Dict[str, List]:
    """Fetch filter data from database with optimized queries."""
    platforms = Platform.objects.annotate(
        game_count=Count('game', distinct=True)
    ).filter(game_count__gt=0).order_by('-game_count', 'name')

    # ВСЕ ключевые слова без ограничений
    keywords = Keyword.objects.all().select_related('category').order_by('name')

    popular_keywords = Keyword.objects.filter(
        cached_usage_count__gt=0
    ).select_related('category').order_by('-cached_usage_count')

    game_modes = GameMode.objects.annotate(
        game_count=Count('game', distinct=True)
    ).filter(game_count__gt=0).order_by('name')

    themes = Theme.objects.annotate(
        game_count=Count('game', distinct=True)
    ).filter(game_count__gt=0).order_by('name')

    perspectives = PlayerPerspective.objects.annotate(
        game_count=Count('game', distinct=True)
    ).filter(game_count__gt=0).order_by('name')

    developers = Company.objects.annotate(
        developed_game_count=Count('developed_games', distinct=True)
    ).filter(developed_game_count__gt=0).order_by('name')

    return {
        'platforms': list(platforms),
        'keywords': list(keywords),  # ВСЕ ключевые слова
        'popular_keywords': list(popular_keywords),
        'game_modes': list(game_modes),
        'themes': list(themes),
        'perspectives': list(perspectives),
        'developers': list(developers),
    }


# ===== SIMILARITY FUNCTIONS =====
