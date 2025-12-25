"""Optimized views for game similarity search with updated models."""

# ===== СТАНДАРТНЫЕ ИМПОРТЫ =====
import time
import json
import hashlib
import logging  # ДОБАВЛЯЕМ ЭТОТ ИМПОРТ
from functools import lru_cache
from typing import Dict, List, Tuple, Any, Optional, Union
from urllib.parse import urlencode

# ===== DJANGO ИМПОРТЫ =====
from django.shortcuts import render, get_object_or_404
from django.db.models import Count, Prefetch, Q
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.core.cache import cache
from django.http import HttpRequest, HttpResponse, HttpResponseServerError
from django.db import models
from django.utils import timezone

# ===== ЛОКАЛЬНЫЕ ИМПОРТЫ =====
from .similarity import GameSimilarity, VirtualGame
from .models import (
    Game, Genre, Keyword, KeywordCategory, Platform,
    Theme, PlayerPerspective, Company, Series, GameMode,
    GameTypeEnum
)
from .helpers import generate_compact_url_params, get_compact_game_list_url

# ===== КОНСТАНТЫ =====
CACHE_TIMES = {
    'filter_data': 3600,  # 1 час
    'similar_games': 172800,  # 48 часов
    'filtered_games': 1800,  # 30 минут
    'full_page': 900,  # 15 минут
    'genres_list': 7200,  # 2 часа
    'precalc_similar': 604800,  # 7 дней
    'static_pages': 3600,  # 1 час
}

ITEMS_PER_PAGE = {
    'similar': 12,
    'regular': 16,
    'platform': 20,
}

"""
Оптимизированные функции для работы с параметрами запросов.
"""

from functools import lru_cache
from typing import Dict, List, Tuple
import json

# Кэш для пре-компилированных пустых результатов
_EMPTY_RESULT = {
    'genres': [],
    'keywords': [],
    'platforms': [],
    'themes': [],
    'perspectives': [],
    'developers': [],
    'game_modes': [],
}


@lru_cache(maxsize=1024)
def _cached_string_to_int_list(param_str: str) -> List[int]:
    """
    Кэширует конвертацию отдельных строк параметров.
    Это эффективнее чем кэшировать весь словарь.
    """
    if not param_str:
        return []

    # Оптимизированная конвертация
    result = []
    for part in param_str.split(','):
        part = part.strip()
        if part and part.isdigit():
            result.append(int(part))

    return result


def convert_params_to_lists(params_dict: Dict[str, str]) -> Dict[str, List[int]]:
    """
    Конвертирует параметры запроса в списки чисел.
    Использует интеллектуальное кэширование на уровне отдельных параметров.
    """
    # Быстрая проверка на пустые параметры
    has_params = False
    for key in ['g', 'k', 'p', 't', 'pp', 'd', 'gm']:
        if params_dict.get(key):
            has_params = True
            break

    if not has_params:
        return _EMPTY_RESULT.copy()

    # Используем кэшированные функции для каждого параметра
    return {
        'genres': _cached_string_to_int_list(params_dict.get('g', '')),
        'keywords': _cached_string_to_int_list(params_dict.get('k', '')),
        'platforms': _cached_string_to_int_list(params_dict.get('p', '')),
        'themes': _cached_string_to_int_list(params_dict.get('t', '')),
        'perspectives': _cached_string_to_int_list(params_dict.get('pp', '')),
        'developers': _cached_string_to_int_list(params_dict.get('d', '')),
        'game_modes': _cached_string_to_int_list(params_dict.get('gm', '')),
    }


# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====

# get_filter_data с @lru_cache работает корректно, так как не принимает аргументов
@lru_cache(maxsize=1)
def get_filter_data() -> Dict[str, List]:
    """Получает данные для фильтров с кэшированием."""
    cache_key = 'game_list_filters_data_v3'
    filter_data = cache.get(cache_key)

    if filter_data:
        return filter_data

    filter_data = _fetch_filter_data_from_db()
    cache.set(cache_key, filter_data, CACHE_TIMES['filter_data'])
    return filter_data


# _generate_cache_key тоже можно оптимизировать
def _generate_cache_key(data: Dict) -> str:
    """Генерирует ключ кэша на основе данных."""
    # Используем отсортированные строковые представления
    cache_key_str = ''.join(f"{k}:{v}" for k, v in sorted(data.items()))
    return f"cache_{hashlib.md5(cache_key_str.encode()).hexdigest()}"


# Оптимизируем extract_request_params для быстрого извлечения
def extract_request_params(request: HttpRequest) -> Dict[str, str]:
    """Извлекает параметры из запроса с минимальными накладками."""
    # Используем локальную переменную для быстрого доступа
    get_params = request.GET

    # Предварительно вычисляем часто используемые параметры
    params = {
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

    return params


def _fetch_filter_data_from_db() -> Dict[str, List]:
    """Получает данные фильтров из базы с оптимизированными запросами."""
    # Используем оптимизированные методы из GameManager
    platforms = Platform.objects.annotate(
        game_count=Count('game')
    ).filter(game_count__gt=0).order_by('-game_count', 'name')[:50]

    # Используем cached_usage_count из оптимизированной модели Keyword
    popular_keywords = Keyword.objects.filter(
        cached_usage_count__gt=0
    ).select_related('category').order_by('-cached_usage_count')[:50]

    game_modes = GameMode.objects.annotate(
        game_count=Count('game')
    ).filter(game_count__gt=0).order_by('name')[:30]

    themes = Theme.objects.annotate(
        game_count=Count('game')
    ).filter(game_count__gt=0).order_by('name')[:30]

    perspectives = PlayerPerspective.objects.annotate(
        game_count=Count('game')
    ).filter(game_count__gt=0).order_by('name')[:20]

    developers = Company.objects.annotate(
        developed_game_count=Count('developed_games')
    ).filter(developed_game_count__gt=0).order_by('name')[:30]

    return {
        'platforms': list(platforms),
        'popular_keywords': list(popular_keywords),
        'game_modes': list(game_modes),
        'themes': list(themes),
        'perspectives': list(perspectives),
        'developers': list(developers),
    }


def get_similar_games_for_game(game_obj: Game, selected_platforms: List[int]) -> Tuple[List, int]:
    """Получает похожие игры для конкретной игры БЕЗ ОГРАНИЧЕНИЙ с prefetch."""
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

    # Фильтрация по платформам
    if selected_platforms:
        similar_games = _filter_by_platforms(similar_games, selected_platforms)
        total_count = len(similar_games)

    # После фильтрации, prefetch для оставшихся игр
    if similar_games:
        # Извлекаем ID игр
        game_ids = []
        for item in similar_games:
            game = item.get('game') if isinstance(item, dict) else item
            if hasattr(game, 'id'):
                game_ids.append(game.id)

        if game_ids:
            # Batch prefetch
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

            # Заменяем игры в результатах на prefetched версии
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

            similar_games = updated_games

    return similar_games, total_count


def get_similar_games_for_criteria(selected_criteria: Dict[str, List[int]]) -> Tuple[List, int]:
    """Получает похожие игры для критериев с OR-фильтрацией."""
    cache_key_data = {
        'g': sorted(selected_criteria['genres']),
        'k': sorted(selected_criteria['keywords']),
        't': sorted(selected_criteria['themes']),
        'pp': sorted(selected_criteria['perspectives']),
        'd': sorted(selected_criteria['developers']),
        'gm': sorted(selected_criteria['game_modes']),
        'p': sorted(selected_criteria['platforms']),
        'version': 'v6_optimized'
    }

    cache_key = f'virtual_search_{_generate_cache_key(cache_key_data)}'
    cached_data = cache.get(cache_key)

    if cached_data:
        similar_games = cached_data['games']
        total_count = cached_data['count']
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

        cache.set(cache_key, {
            'games': similar_games,
            'count': total_count,
            'timestamp': time.time()
        }, CACHE_TIMES['similar_games'])

    # Фильтрация по платформам
    if selected_criteria['platforms']:
        similar_games = _filter_by_platforms(similar_games, selected_criteria['platforms'])
        total_count = len(similar_games)

    return similar_games, total_count


def _filter_by_platforms(games_data: List, platform_ids: List[int]) -> List:
    """Фильтрует игры по платформам с оптимизацией."""
    if not platform_ids or not games_data:
        return games_data

    filtered = []
    platform_ids_set = set(platform_ids)

    # Для объектов словаря (сходство)
    for item in games_data:
        game = item.get('game') if isinstance(item, dict) else item

        # Быстрая проверка через cached_platform_count
        if hasattr(game, 'cached_platform_count'):
            if game.cached_platform_count == 0:
                continue
            # Если есть кэшированные платформы, используем их
            if hasattr(game, '_cached_platform_ids'):
                game_platform_ids = game._cached_platform_ids
            else:
                # Батчинговый запрос для всех игр сразу
                game_platform_ids = set()
        else:
            # Стандартная проверка
            game_platform_ids = {p.id for p in game.platforms.all()}

        if platform_ids_set & game_platform_ids:
            filtered.append(item)

    return filtered


def game_detail(request: HttpRequest, pk: int) -> HttpResponse:
    """Детальная страница игры с оптимизированными запросами."""
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
    """Универсальное сравнение: игра-игра или критерии-игра."""
    try:
        # Оптимизированный запрос
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

        source_game_id = request.GET.get('source_game')
        game1 = None

        # ПРАВИЛЬНОЕ ОПРЕДЕЛЕНИЕ: есть ли исходная игра
        if source_game_id and source_game_id.strip() and source_game_id.isdigit():
            try:
                game1 = Game.objects.only(
                    'id', 'name', 'game_type'
                ).prefetch_related(
                    'genres', 'keywords', 'themes',
                    'developers', 'player_perspectives', 'game_modes'
                ).get(pk=int(source_game_id))
            except (Game.DoesNotExist, ValueError):
                game1 = None

        # Конвертируем параметры для использования
        selected_criteria = convert_params_to_lists(request.GET)

        # Определяем тип сравнения
        if game1:
            # Сравнение игра-игра
            is_criteria_comparison = False
            source = game1
        else:
            # Проверяем, есть ли критерии для сравнения
            has_criteria = any(selected_criteria.values())
            if has_criteria:
                # Сравнение критерии-игра
                is_criteria_comparison = True
                source = VirtualGame(
                    genre_ids=selected_criteria['genres'],
                    keyword_ids=selected_criteria['keywords'],
                    theme_ids=selected_criteria['themes'],
                    perspective_ids=selected_criteria['perspectives'],
                    developer_ids=selected_criteria['developers'],
                    game_mode_ids=selected_criteria['game_modes']
                )
            else:
                # Просто показываем игру без сравнения
                return render(request, 'games/game_comparison_simple.html', {
                    'game2': game2,
                    'no_comparison': True,
                })

        # Рассчитываем схожесть
        similarity_engine = GameSimilarity()
        similarity_score = similarity_engine.calculate_similarity(source, game2)
        breakdown = similarity_engine.get_similarity_breakdown(source, game2)

        # Рассчитываем общие элементы
        shared_items = {}
        fields_to_compare = ['genres', 'keywords', 'themes', 'perspectives', 'developers', 'game_modes']

        if is_criteria_comparison:
            # Критерии vs Игра
            for field in fields_to_compare:
                # Получаем поле игры
                if field == 'perspectives':
                    game_field = game2.player_perspectives.all()
                else:
                    game_field = getattr(game2, field).all()

                # Получаем критерии
                criteria_ids = selected_criteria[field]
                if criteria_ids:
                    # Получаем объекты критериев
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
            # Игра vs Игра
            for field in fields_to_compare:
                if field == 'perspectives':
                    field1 = game1.player_perspectives.all()
                    field2 = game2.player_perspectives.all()
                else:
                    field1 = getattr(game1, field).all()
                    field2 = getattr(game2, field).all()

                # Находим пересечение
                shared_items[field] = list(field1 & field2)

        # Подготавливаем контекст
        context = {
            'game1': game1,
            'game2': game2,
            'similarity_score': similarity_score,
            'is_criteria_comparison': is_criteria_comparison,
            'breakdown': breakdown,

            # Добавляем selected_criteria для шаблона
            'selected_criteria': selected_criteria,

            # Добавляем счетчик критериев
            'selected_criteria_count': sum(len(v) for v in selected_criteria.values()),

            # Веса алгоритма
            'genres_weight': int(similarity_engine.GENRES_TOTAL_WEIGHT),
            'keywords_weight': int(similarity_engine.KEYWORDS_WEIGHT),
            'keywords_add_per_match': int(similarity_engine.KEYWORDS_ADD_PER_MATCH),
            'themes_weight': int(similarity_engine.THEMES_WEIGHT),
            'developers_weight': int(similarity_engine.DEVELOPERS_WEIGHT),
            'perspectives_weight': int(similarity_engine.PERSPECTIVES_WEIGHT),
            'game_modes_weight': int(similarity_engine.GAME_MODES_WEIGHT),
            'genres_exact_match_weight': int(similarity_engine.GENRES_EXACT_MATCH_WEIGHT),
        }

        # Добавляем shared items в контекст
        for field, items in shared_items.items():
            context[f'shared_{field}'] = items
            context[f'shared_{field}_count'] = len(items)

        # Для обратной совместимости со старым шаблоном
        context.update({
            'selected_genres': selected_criteria['genres'],
            'selected_keywords': selected_criteria['keywords'],
            'selected_themes': selected_criteria['themes'],
            'selected_perspectives': selected_criteria['perspectives'],
            'selected_developers': selected_criteria['developers'],
            'selected_game_modes': selected_criteria['game_modes'],
        })

        return render(request, 'games/game_comparison.html', context)

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in comparison: {str(e)}")
        print(f"Details: {error_details}")
        return HttpResponseServerError(f"Error in comparison: {str(e)}")


def should_find_similar(params: Dict[str, str], selected_criteria: Dict[str, List[int]]) -> bool:
    """Определяет, нужно ли искать похожие игры."""
    find_similar = params.get('find_similar') == '1'

    if not find_similar:
        # Используем any() для быстрой проверки
        criteria = selected_criteria
        if any(criteria.values()):
            find_similar = True

    return find_similar


def get_source_game(source_game_id: Optional[str]) -> Optional[Game]:
    """Получает объект исходной игры с оптимизированным запросом."""
    if source_game_id:
        try:
            return Game.objects.only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            ).get(pk=source_game_id)
        except Game.DoesNotExist:
            return None
    return None


def has_similarity_criteria(selected_criteria: Dict[str, List[int]]) -> bool:
    """Проверяет, есть ли критерии для поиска похожих."""
    return any(selected_criteria.values())


def handle_similar_games_mode(
        request: HttpRequest,
        params: Dict[str, str],
        selected_criteria: Dict[str, List[int]],
        source_game_obj: Optional[Game],
        filter_data: Dict[str, List]
) -> Dict[str, Any]:
    """Обрабатывает режим поиска похожих игр."""
    current_sort = params.get('sort', '-similarity')
    page_number = params.get('page', 1)

    # Определяем источник для поиска
    if source_game_obj:
        similar_games_data, total_count = get_similar_games_for_game(
            source_game_obj, selected_criteria['platforms']
        )
        source_display = source_game_obj.get_full_title if hasattr(source_game_obj,
                                                                   'get_full_title') else source_game_obj.name
    else:
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)
        source_display = "Search Criteria"

    # Форматируем данные
    games_with_similarity = _format_similar_games_data(similar_games_data)

    # Сортируем
    _sort_similar_games(games_with_similarity, current_sort)

    # Пагинация
    page_obj, paginator, is_paginated = _paginate_results(
        games_with_similarity, page_number, ITEMS_PER_PAGE['similar']
    )

    # Создаем source_game объект для шаблона
    class SimpleSourceGame:
        def __init__(self, game_obj=None, criteria=None):
            if game_obj:
                self.id = game_obj.id
                self.name = game_obj.name
                self.display_name = source_display
                self.is_game = True
            else:
                self.id = None
                self.name = source_display
                self.display_name = source_display
                self.is_game = False
            self.genres_ids = criteria['genres'] if criteria else []

    source_game = SimpleSourceGame(source_game_obj, selected_criteria)

    # Собираем контекст
    return _build_context(
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
        find_similar=True
    )


def handle_regular_mode(
        request: HttpRequest,
        params: Dict[str, str],
        selected_criteria: Dict[str, List[int]],
        filter_data: Dict[str, List]
) -> Dict[str, Any]:
    """Обрабатывает обычный режим фильтрации."""
    current_sort = params.get('sort', '-rating_count')
    page_number = params.get('page', 1)

    # Получаем отфильтрованные игры
    games, total_count = _get_filtered_games(selected_criteria, current_sort)

    # Пагинация
    page_obj, paginator, is_paginated = _paginate_results(
        list(games), page_number, ITEMS_PER_PAGE['regular']
    )

    # Собираем контекст
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
        find_similar=False
    )


def _get_filtered_games(selected_criteria: Dict[str, List[int]], sort_field: str) -> Tuple[models.QuerySet, int]:
    """Получает отфильтрованные игры с улучшенным кэшированием и prefetch."""
    cache_key = _create_filter_cache_key(selected_criteria, sort_field)
    cached_data = cache.get(cache_key)

    if cached_data and 'game_ids' in cached_data:
        # Восстанавливаем только ID из кэша
        game_ids = cached_data['game_ids']
        total_count = cached_data['count']

        # Используем bulk_prefetch для оптимизации с prefetch_related
        games = Game.objects.filter(id__in=game_ids).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            Prefetch('keywords', queryset=Keyword.objects.select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            )),
            Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
            Prefetch('developers', queryset=Company.objects.only('id', 'name')),
            Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type',
            '_cached_genre_count', '_cached_keyword_count',
            '_cached_platform_count', '_cached_developer_count'
        )

        # Применяем сортировку в Python для кэшированных данных
        if cached_data.get('sort_data'):
            # Сортируем уже загруженные игры
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
        # Новый запрос с prefetch_related
        games = Game.objects.all().prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            Prefetch('keywords', queryset=Keyword.objects.select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            )),
            Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
            Prefetch('developers', queryset=Company.objects.only('id', 'name')),
            Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type',
            '_cached_genre_count', '_cached_keyword_count',
            '_cached_platform_count', '_cached_developer_count'
        )

        # Применяем фильтры
        games = _apply_filters(games, selected_criteria)

        # Оптимизированная сортировка
        if sort_field in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count', '-first_release_date']:
            games = games.order_by(sort_field)
        else:
            games = games.order_by('-rating_count')

        total_count = games.count()

        # Берем только ID для кэша и ограничиваем размер
        game_ids = list(games.values_list('id', flat=True)[:200])  # Ограничиваем

        # Дополнительные данные для сортировки
        sort_data = {}
        if sort_field == '-rating_count':
            sort_data = dict(games.values_list('id', 'rating_count')[:200])
        elif sort_field == '-rating':
            sort_data = dict(games.values_list('id', 'rating')[:200])
        elif sort_field == '-first_release_date':
            sort_data = dict(games.values_list('id', 'first_release_date')[:200])

        cache.set(cache_key, {
            'game_ids': game_ids,
            'count': total_count,
            'sort_data': sort_data,
            'sort_field': sort_field,
            'timestamp': time.time()
        }, CACHE_TIMES['filtered_games'])

        # Если нужно ограничить для пагинации
        if len(game_ids) < total_count:
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

            # Применяем сортировку к ограниченному набору
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

    return games, total_count


def _apply_filters(queryset: models.QuerySet, selected_criteria: Dict[str, List[int]]) -> models.QuerySet:
    """Применяет фильтры к queryset с оптимизацией."""
    # Сначала проверяем кэшированные счетчики для быстрого отсечения
    if selected_criteria['genres'] and hasattr(queryset.model, '_cached_genre_count'):
        # Можно добавить дополнительные оптимизации
        pass

    filters = []

    # Строим фильтры с использованием Q объектов
    for field, model_field in [
        ('genres', 'genres__id__in'),
        ('keywords', 'keywords__id__in'),
        ('platforms', 'platforms__id__in'),
        ('themes', 'themes__id__in'),
        ('perspectives', 'player_perspectives__id__in'),
        ('developers', 'developers__id__in'),
        ('game_modes', 'game_modes__id__in')
    ]:
        if selected_criteria[field]:
            filters.append(Q(**{model_field: selected_criteria[field]}))

    if filters:
        # Объединяем все фильтры AND условием
        combined_filter = filters[0]
        for q in filters[1:]:
            combined_filter &= q
        queryset = queryset.filter(combined_filter).distinct()

    return queryset


def _format_similar_games_data(similar_games_data: List) -> List[Dict[str, Any]]:
    """Форматирует данные похожих игр с предварительной загрузкой связей."""
    if not similar_games_data:
        return []

    # Извлекаем ID игр для batch prefetch
    game_ids = []
    for item in similar_games_data:
        game = item.get('game') if isinstance(item, dict) else item
        if hasattr(game, 'id'):
            game_ids.append(game.id)

    # Batch prefetch для всех игр сразу
    if game_ids:
        games_dict = {}
        games_with_prefetch = Game.objects.filter(id__in=game_ids).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            Prefetch('keywords', queryset=Keyword.objects.select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            )),
            Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
            Prefetch('developers', queryset=Company.objects.only('id', 'name')),
            Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type',
            '_cached_genre_count', '_cached_keyword_count',
            '_cached_platform_count', '_cached_developer_count'
        )

        for game in games_with_prefetch:
            games_dict[game.id] = game

    formatted = []
    for item in similar_games_data:
        game = item.get('game') if isinstance(item, dict) else item
        similarity = item.get('similarity', 0) if isinstance(item, dict) else 0

        # Используем prefetched игру если доступна
        if hasattr(game, 'id') and game_ids and game.id in games_dict:
            game = games_dict[game.id]

        # Используем кэшированные свойства если доступны
        if hasattr(game, 'cached_genre_count'):
            formatted.append({
                'game': game,
                'similarity': similarity,
                '_cached_rating': game.rating or 0,
                '_cached_rating_count': game.rating_count or 0,
                '_cached_name': game.name.lower(),
            })
        else:
            formatted.append({
                'game': game,
                'similarity': similarity,
            })

    return formatted


def _sort_similar_games(games_with_similarity: List[Dict[str, Any]], current_sort: str) -> None:
    """Сортирует похожие игры с оптимизацией."""
    if current_sort == '-rating_count':
        # Используем кэшированные значения если доступны
        if '_cached_rating_count' in games_with_similarity[0]:
            games_with_similarity.sort(key=lambda x: x['_cached_rating_count'], reverse=True)
        else:
            games_with_similarity.sort(key=lambda x: x['game'].rating_count or 0, reverse=True)
    elif current_sort == '-rating':
        if '_cached_rating' in games_with_similarity[0]:
            games_with_similarity.sort(key=lambda x: x['_cached_rating'], reverse=True)
        else:
            games_with_similarity.sort(key=lambda x: x['game'].rating or 0, reverse=True)
    elif current_sort == 'name':
        if '_cached_name' in games_with_similarity[0]:
            games_with_similarity.sort(key=lambda x: x['_cached_name'])
        else:
            games_with_similarity.sort(key=lambda x: x['game'].name.lower())
    elif current_sort == '-name':
        if '_cached_name' in games_with_similarity[0]:
            games_with_similarity.sort(key=lambda x: x['_cached_name'], reverse=True)
        else:
            games_with_similarity.sort(key=lambda x: x['game'].name.lower(), reverse=True)
    elif current_sort == '-first_release_date':
        games_with_similarity.sort(
            key=lambda x: x['game'].first_release_date or '',
            reverse=True
        )
    elif current_sort == '-similarity':
        games_with_similarity.sort(key=lambda x: x['similarity'], reverse=True)
    else:
        games_with_similarity.sort(key=lambda x: x['similarity'], reverse=True)


def _paginate_results(data: List, page_number: str, items_per_page: int) -> Tuple:
    """Пагинирует данные."""
    paginator = Paginator(data, items_per_page)

    try:
        page_obj = paginator.page(int(page_number))
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    return page_obj, paginator, paginator.num_pages > 1


def _create_filter_cache_key(selected_criteria: Dict[str, List[int]], sort_field: str) -> str:
    """Создает ключ кэша для фильтров."""
    parts = []

    for key, value in selected_criteria.items():
        if value:
            parts.append(f"{key}_{'-'.join(map(str, sorted(value)))}")

    parts.append(f"sort_{sort_field}")
    parts.append(f"version_v2")

    return f'filtered_games_{"_".join(parts)}' if parts else 'filtered_games_all'


def _build_context(mode: str, **kwargs) -> Dict[str, Any]:
    """Собирает контекст для шаблона."""
    # Получаем жанры из кэша с использованием only()
    genres_list = cache.get('genres_list')
    if not genres_list:
        genres_list = list(Genre.objects.all().only('id', 'name').order_by('name'))
        cache.set('genres_list', genres_list, CACHE_TIMES['genres_list'])

    # Генерируем URL параметры
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

    # Получаем данные фильтров
    filter_data = kwargs.get('filter_data', {})

    # Создаем базовый контекст
    context = {
        'genres': genres_list,
        'keyword_categories': list(KeywordCategory.objects.all().only('id', 'name')),
        'current_sort': kwargs.get('current_sort', ''),
        'find_similar': kwargs.get('find_similar', False),
        'compact_url_params': compact_url_params,

        # Выбранные критерии
        'selected_genres': selected_criteria['genres'],
        'selected_keywords': selected_criteria['keywords'],
        'selected_platforms': selected_criteria['platforms'],
        'selected_themes': selected_criteria['themes'],
        'selected_perspectives': selected_criteria['perspectives'],
        'selected_developers': selected_criteria['developers'],
        'selected_game_modes': selected_criteria['game_modes'],

        # Данные для фильтров
        'popular_keywords': filter_data.get('popular_keywords', []),
        'platforms': filter_data.get('platforms', []),
        'themes': filter_data.get('themes', []),
        'perspectives': filter_data.get('perspectives', []),
        'developers': filter_data.get('developers', []),
        'game_modes': filter_data.get('game_modes', []),

        # Пагинация
        'page_obj': kwargs.get('page_obj'),
        'paginator': kwargs.get('paginator'),
        'is_paginated': kwargs.get('is_paginated', False),
        'total_count': kwargs.get('total_count', 0),

        # Источник
        'source_game': kwargs.get('source_game'),
        'source_game_obj': kwargs.get('source_game_obj'),
        'selected_criteria': selected_criteria,

        # Для отладки
        'debug_mode': mode,
    }

    # Добавляем специфичные для режима поля
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


def game_list(request: HttpRequest) -> HttpResponse:
    """Главная функция списка игр с батчинговой оптимизацией."""
    # 1. Быстро извлекаем параметры
    params = extract_request_params(request)

    # 2. Генерируем ключ кэша
    cache_key_data = {}
    for key in ['g', 'k', 'p', 't', 'pp', 'd', 'gm', 'find_similar', 'source_game', 'sort', 'page']:
        if key in params and params[key]:
            cache_key_data[key] = params[key]

    if cache_key_data:
        cache_key = f'game_list_{_generate_cache_key(cache_key_data)}'
    else:
        cache_key = 'game_list_default'

    # 3. Проверяем кэш полной страницы (включая шаблон)
    cached_page = cache.get(cache_key)
    if cached_page:
        return cached_page

    start_time = time.time()

    # 4. Получаем данные фильтров
    filter_data = get_filter_data()

    # 5. Конвертируем параметры
    selected_criteria = convert_params_to_lists(params)

    # 6. Определяем режим работы
    find_similar = should_find_similar(params, selected_criteria)
    source_game_obj = get_source_game(params.get('source_game'))

    # 7. Обрабатываем в зависимости от режима
    if find_similar and has_similarity_criteria(selected_criteria):
        context = handle_similar_games_mode(
            request, params, selected_criteria, source_game_obj, filter_data
        )
    else:
        context = handle_regular_mode(request, params, selected_criteria, filter_data)

    # 8. Добавляем время выполнения
    context['execution_time'] = round(time.time() - start_time, 3)

    # 9. Рендерим
    from django.template.loader import render_to_string
    content = render_to_string('games/game_list.html', context, request=request)

    # 10. Создаем ответ и кэшируем
    response = HttpResponse(content)
    cache.set(cache_key, response, CACHE_TIMES['full_page'])

    return response


def home(request: HttpRequest) -> HttpResponse:
    """Оптимизированная главная страница с минимальными запросами."""

    cache_key = 'optimized_home_final_v8'
    cached_context = cache.get(cache_key)

    if cached_context:
        response = render(request, 'games/home.html', cached_context)
        response['X-Cache-Hit'] = 'True'
        return response

    start_time = time.time()

    try:
        from django.db.models import Prefetch
        from django.utils import timezone
        from datetime import timedelta

        # ===== МИНИМАЛЬНЫЕ PREFETCH ДЛЯ КАРТОЧЕК ИГР =====
        # Карточка в _game_card.html использует только:
        # 1. game.genres.all() - жанры
        # 2. game.platforms.all() - платформы
        # 3. game.player_perspectives.all() - перспективы
        # 4. game.keywords.all() - НЕ используется на главной в карточках!

        # Используем только необходимые prefetch
        genre_prefetch = Prefetch('genres', queryset=Genre.objects.only('id', 'name'))
        platform_prefetch = Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug'))
        perspective_prefetch = Prefetch('player_perspectives',
                                        queryset=PlayerPerspective.objects.only('id', 'name'))

        # ===== ПОЛУЧАЕМ ID ИГР =====
        # Популярные игры - с высоким рейтингом и количеством оценок
        popular_ids = list(Game.objects.filter(
            rating_count__gt=10,
            rating__gte=3.0  # Добавляем минимальный рейтинг
        ).order_by('-rating_count', '-rating').values_list('id', flat=True)[:12])

        # Недавние игры - за последние 2 года
        two_years_ago = timezone.now() - timedelta(days=730)
        recent_ids = list(Game.objects.filter(
            first_release_date__gte=two_years_ago,
            first_release_date__lte=timezone.now()
        ).order_by('-first_release_date').values_list('id', flat=True)[:12])

        # Объединяем ID
        all_game_ids = list(set(popular_ids + recent_ids))

        if all_game_ids:
            # МИНИМАЛЬНЫЙ ЗАПРОС - только необходимые поля и prefetch
            all_games = Game.objects.filter(id__in=all_game_ids).prefetch_related(
                genre_prefetch,  # только для карточек
                platform_prefetch,  # только для карточек
                perspective_prefetch  # только для карточек
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url'
            )  # Убираем game_type - не используется на главной

            # Создаем словарь для быстрого доступа
            games_dict = {game.id: game for game in all_games}

            # Формируем списки в правильном порядке
            popular_games = [games_dict[game_id] for game_id in popular_ids
                             if game_id in games_dict]
            recent_games = [games_dict[game_id] for game_id in recent_ids
                            if game_id in games_dict]

            # Сохраняем порядок сортировки
            popular_games.sort(key=lambda x: popular_ids.index(x.id))
            recent_games.sort(key=lambda x: recent_ids.index(x.id))
        else:
            popular_games = []
            recent_games = []

        # ===== КЛЮЧЕВЫЕ СЛОВА - МИНИМАЛЬНЫЙ ЗАПРОС =====
        # Только кэшированные значения, без обновления
        popular_keywords = list(Keyword.objects.filter(
            cached_usage_count__gt=0
        ).only(
            'id', 'name', 'cached_usage_count'
        ).order_by('-cached_usage_count')[:20])

        # ===== ПОДСЧЕТ И КЭШИРОВАНИЕ =====
        from django.db import connection
        query_count = len(connection.queries)

        context = {
            'popular_games': popular_games,
            'recent_games': recent_games,
            'popular_keywords': popular_keywords,
            'execution_time': round(time.time() - start_time, 3),
            'query_count': query_count,
        }

        # Кэшируем на 5 минут
        cache.set(cache_key, context, 300)

        response = render(request, 'games/home.html', context)
        response['X-Cache-Hit'] = 'False'
        response['X-DB-Queries'] = str(query_count)
        response['X-Response-Time'] = f"{context['execution_time']:.3f}s"

        return response

    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Home page error: {str(e)}")

        # Fallback с минимальными данными
        context = {
            'popular_games': [],
            'recent_games': [],
            'popular_keywords': [],
        }
        return render(request, 'games/home.html', context)


def keyword_category_view(request: HttpRequest, category_id: int) -> HttpResponse:
    """Просмотр игр по категории ключевых слов."""
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

    # Популярные ключевые слова в этой категории
    popular_keywords = Keyword.objects.filter(
        category=category,
        game__isnull=False
    ).annotate(game_count=Count('game')).only(
        'id', 'name', 'category__id'
    ).order_by('-game_count')[:20]

    return render(request, 'games/keyword_category.html', {
        'category': category,
        'games': list(games),
        'popular_keywords': list(popular_keywords),
    })


def game_search(request: HttpRequest) -> HttpResponse:
    """Простой поиск игр по названию."""
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
        # Используем icontains с индексом
        games = games.filter(name__icontains=search_query)

    # Сортировка по популярности
    games = games.order_by('-rating_count', '-rating')

    return render(request, 'games/game_search.html', {
        'games': list(games),
        'search_query': search_query,
        'total_results': games.count(),
    })


def platform_list(request: HttpRequest) -> HttpResponse:
    """Страница со списком всех платформ."""
    platforms = Platform.objects.annotate(
        game_count=Count('game')
    ).filter(game_count__gt=0).only(
        'id', 'name', 'slug'
    ).order_by('-game_count', 'name')

    return render(request, 'games/platform_list.html', {
        'platforms': list(platforms),
    })


def platform_games(request: HttpRequest, platform_id: int) -> HttpResponse:
    """Список игр для конкретной платформы с оптимизированными запросами."""
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

    # Пагинация
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
