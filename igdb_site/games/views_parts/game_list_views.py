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
    GameTypeEnum, cache
)

# Константа для количества игр на страницу - теперь используется сервером
ITEMS_PER_PAGE = 16


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

    if not games_list:
        return games_list

    print(f"\n=== UPDATE GAMES WITH CACHED CARDS DEBUG ===")
    print(f"games_list length: {len(games_list)}")
    print(f"show_similarity: {show_similarity}")

    # Собираем все ID игр
    game_ids = []
    game_items = []

    for item in games_list:
        if isinstance(item, dict) and 'game' in item:
            game_obj = item['game']
            similarity = item.get('similarity')
            game_ids.append(game_obj.id)
            game_items.append((item, game_obj, similarity, True))
        else:
            game_obj = item
            game_ids.append(game_obj.id)
            game_items.append((item, game_obj, None, False))

    # Получаем текущую версию кэша из модели
    from games.models import GameCardCache
    from games.utils.game_card_utils import GameCardCreator
    current_cache_version = GameCardCache.CARD_CACHE_VERSION

    # Загружаем ВСЕ карточки из БД ОДНИМ ЗАПРОСОМ
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

    # Обрабатываем каждый элемент
    processed_items = []

    for item, game_obj, similarity, is_similar_mode in game_items:
        existing_card = cards_in_db.get(game_obj.id)

        if existing_card and existing_card.template_version == current_cache_version:
            # Карточка актуальна - используем её HTML
            try:
                existing_card.increment_hit()
                card_html = existing_card.rendered_card

                # Добавляем data-атрибуты с процентом и source_game, если нужно
                if show_similarity and similarity is not None and similarity > 0:
                    import re

                    # Сначала добавляем data-similarity
                    pattern = r'(<div[^>]*class="[^"]*game-card-container[^"]*"[^>]*)>'
                    replacement = r'\1 data-similarity="' + str(similarity) + r'">'
                    card_html = re.sub(pattern, replacement, card_html, count=1)

                    # Затем добавляем data-source-game-id если есть
                    source_game = context.get('source_game')
                    if source_game and hasattr(source_game, 'id'):
                        pattern = r'(<div[^>]*class="[^"]*game-card-container[^"]*"[^>]*)>'
                        replacement = r'\1 data-source-game-id="' + str(source_game.id) + r'">'
                        card_html = re.sub(pattern, replacement, card_html, count=1)

                if isinstance(item, dict):
                    item['cached_card'] = card_html
                else:
                    item.cached_card = card_html

                processed_items.append(item)
                continue
            except Exception as e:
                logger.error(f"Error using cached card: {str(e)}")

        # Нет карточки в кэше или она устарела - создаём простую карточку без сохранения в БД
        # (для скорости, не сохраняем в БД при пагинации)
        card_context = {'game': game_obj}
        card_html = render_to_string('games/partials/_game_card.html', card_context)

        # Добавляем data-атрибуты с процентом и source_game, если нужно
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


def game_list(request: HttpRequest) -> HttpResponse:
    """
    Main game list function - returns empty container for AJAX to populate.
    No data loading happens here - all loading is done via AJAX.
    """
    start_time = time.time()

    params = extract_request_params(request)
    selected_criteria = convert_params_to_lists(params)
    # Получаем объекты для всех выбранных критериев (используются для отображения)
    selected_criteria_objects = _get_selected_criteria_objects(selected_criteria)
    years_range = _get_cached_years_range()

    find_similar = params.get('find_similar') == '1'
    source_game_obj = None
    if params.get('source_game'):
        try:
            source_game_obj = _get_cached_game(params['source_game'])
        except (Game.DoesNotExist, ValueError):
            pass

    filter_data = _get_optimized_filter_data()

    # === НОВАЯ ЛОГИКА: Отделяем фильтры для поиска от фильтров для похожести ===
    # 1. Фильтры для поиска (Search Filters) - будут заполнены из исходной игры
    search_selected = {
        'genres': [],
        'keywords': [],
        'platforms': [],
        'themes': [],
        'perspectives': [],
        'game_modes': [],
        'game_types': [],
        'engines': [],
        'release_year_start': selected_criteria['release_year_start'],  # Дата может применяться к обоим
        'release_year_end': selected_criteria['release_year_end'],
    }
    search_selected_objects = {}

    # 2. Фильтры для похожести (Similarity Filters)
    # Они берутся из параметров запроса
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
        # Даты не используем для похожести
    }

    # Если есть source_game и это режим find_similar
    if find_similar and source_game_obj:
        print("DEBUG game_list: Source game detected, populating search filters from source game")

        # Получаем платформы исходной игры
        source_platforms = list(source_game_obj.platforms.values_list('id', flat=True))
        print(f"DEBUG: Source game platforms: {source_platforms}")

        # Получаем режимы игры исходной игры
        source_game_modes = list(source_game_obj.game_modes.values_list('id', flat=True))
        print(f"DEBUG: Source game modes: {source_game_modes}")

        # Заполняем SEARCH FILTERS платформами и режимами игры из исходной игры
        if source_platforms:
            search_selected['platforms'] = source_platforms
            print(f"DEBUG: Search platforms set to: {source_platforms}")

        if source_game_modes:
            search_selected['game_modes'] = source_game_modes
            print(f"DEBUG: Search game modes set to: {source_game_modes}")

        # Получаем объекты для поисковых фильтров
        search_selected_objects = _get_selected_criteria_objects(search_selected)

        # Если критерии похожести не переданы, заполняем их из игры
        if not any(similarity_selected.values()):
            print("DEBUG game_list: Populating similarity filters from source game")
            similarity_selected['genres'] = [g.id for g in source_game_obj.genres.all()]
            similarity_selected['keywords'] = [k.id for k in source_game_obj.keywords.all()]
            similarity_selected['themes'] = [t.id for t in source_game_obj.themes.all()]
            similarity_selected['perspectives'] = [p.id for p in source_game_obj.player_perspectives.all()]
            similarity_selected['engines'] = [e.id for e in source_game_obj.engines.all()]

    # Получаем объекты для выбранных фильтров похожести (для отображения)
    similarity_selected_objects = _get_selected_criteria_objects(similarity_selected)
    # ============================================================

    # ОТЛАДКА: проверяем, что пришло из filter_data
    print(f"DEBUG game_list: filter_data keys = {filter_data.keys()}")
    print(f"DEBUG game_list: engines count = {len(filter_data.get('engines', []))}")
    if filter_data.get('engines'):
        print(f"DEBUG game_list: first engine = {filter_data['engines'][0].name if filter_data['engines'] else 'None'}")
    else:
        print("DEBUG game_list: engines list is EMPTY in filter_data!")

    # Подготавливаем контекст с пустыми данными
    context = {
        # Пустые данные для игр - они будут загружены через AJAX
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
        'show_similarity': find_similar,  # Показываем similarity если включен режим
        'source_game': None,  # Будет заполнено через SimpleSourceGame если нужно
        'source_game_obj': source_game_obj,

        # Данные для фильтров
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

        # === ИЗМЕНЕНО: Передаем отдельные наборы для разных панелей ===
        # Значения для панели поиска (Search Filters)
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

        # Значения для панели похожести (Similarity Filters)
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
        # =============================================================

        # Эти переменные пока оставляем для обратной совместимости, но в шаблонах они больше не используются
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
            'mode': 'ajax_only',
            'message': 'Initial page load - data will be loaded via AJAX',
            'find_similar': find_similar,
            'has_source_game': source_game_obj is not None,
            'engines_count': len(filter_data.get('engines', [])),
        }
    }

    # Если есть source_game, создаем SimpleSourceGame для шаблонов
    if source_game_obj:
        from .base_views import SimpleSourceGame

        # Собираем критерии из игры (используем similarity_selected, так как там уже данные из игры)
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


def ajax_load_games_page(request: HttpRequest) -> HttpResponse:
    """Load games for specific page via AJAX with card caching."""
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

    # Извлекаем поисковые фильтры
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

    search_genres_list = [int(x) for x in search_genres.split(',') if x.isdigit()] if search_genres else []
    search_keywords_list = [int(x) for x in search_keywords.split(',') if x.isdigit()] if search_keywords else []
    search_themes_list = [int(x) for x in search_themes.split(',') if x.isdigit()] if search_themes else []
    search_perspectives_list = [int(x) for x in search_perspectives.split(',') if
                                x.isdigit()] if search_perspectives else []
    search_game_modes_list = [int(x) for x in search_game_modes.split(',') if x.isdigit()] if search_game_modes else []
    search_engines_list = [int(x) for x in search_engines.split(',') if x.isdigit()] if search_engines else []
    search_platforms_list = [int(x) for x in search_platforms.split(',') if x.isdigit()] if search_platforms else []
    search_game_types_list = [int(x) for x in search_game_types.split(',') if x.isdigit()] if search_game_types else []

    print(f"DEBUG: search_engines_list = {search_engines_list}")
    print(f"DEBUG: search_platforms_list = {search_platforms_list}")
    print(f"DEBUG: search_game_modes_list = {search_game_modes_list}")

    try:
        search_year_start_int = int(search_year_start) if search_year_start else None
    except ValueError:
        search_year_start_int = None

    try:
        search_year_end_int = int(search_year_end) if search_year_end else None
    except ValueError:
        search_year_end_int = None

    print(f"Search genres: {search_genres_list}")
    print(f"Search platforms: {search_platforms_list}")
    print(f"Search engines: {search_engines_list}")
    print(f"Search years: {search_year_start_int}-{search_year_end_int}")

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
    if find_similar and (source_game_obj or any([
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes'],
        selected_criteria['engines']
    ])):
        print("Mode: similar games")
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
        source_game = mode_result.get('source_game')
        paginator = mode_result.get('paginator')
        total_pages = paginator.num_pages if paginator else 1
        total_count = mode_result.get('total_count', 0)

        mode_timers = mode_result.get('timers', {})
    else:
        print("Mode: regular games")
        mode_result = _get_all_games_mode_with_pagination(
            selected_criteria, sort_field, page_num
        )

        games_with_similarity = []
        source_game = None
        paginator = mode_result.get('paginator')
        total_pages = paginator.num_pages if paginator else 1
        total_count = mode_result.get('total_count', 0)
        games = list(mode_result.get('page_obj', {}).object_list) if mode_result.get('page_obj') else []

        mode_timers = {}
    timers['mode_determination'] = round(time.time() - stage_start, 3)

    stage_start = time.time()
    if find_similar and (source_game_obj or any([
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes'],
        selected_criteria['engines']
    ])):
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
            'current_sort': params.get('sort', ''),
            'request': request,
        }
    else:
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
            'page_obj': mode_result.get('page_obj'),
            'paginator': paginator,
            'is_paginated': mode_result.get('is_paginated', False),
            'total_count': total_count,
            'total_pages': total_pages,
            'start_index': (page_num - 1) * ITEMS_PER_PAGE + 1,
            'end_index': min(page_num * ITEMS_PER_PAGE, total_count),
            'items_per_page': ITEMS_PER_PAGE,
            'current_sort': params.get('sort', ''),
            'request': request,
        }
    timers['card_caching'] = round(time.time() - stage_start, 3)

    template_context['debug_total_pages'] = template_context['total_pages']
    template_context['debug_current_page'] = page_num
    template_context['debug_params'] = dict(request.GET)
    template_context['debug_search_genres'] = search_genres_list

    stage_start = time.time()
    html = render_to_string('games/game_list/_games_results.html', template_context)
    timers['template_rendering'] = round(time.time() - stage_start, 3)

    timers['total'] = round(time.time() - total_start, 3)

    print("\n=== TIMERS: ajax_load_games_page ===")
    print(f"Params extraction: {timers['params_extraction']}s")
    print(f"Mode determination: {timers['mode_determination']}s")
    if mode_timers:
        print(f"  - Source game creation: {mode_timers.get('source_game_creation', 0)}s")
        print(f"  - Formatting: {mode_timers.get('formatting', 0)}s")
        print(f"  - Search filters: {mode_timers.get('search_filters', 0)}s")
        print(f"  - Sorting: {mode_timers.get('sorting', 0)}s")
        print(f"  - Pagination: {mode_timers.get('pagination', 0)}s")
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

    start_total = time.time()
    print(f"\n=== get_similar_games_for_criteria START ===")
    print(
        f"Criteria: genres={len(selected_criteria['genres'])}, keywords={len(selected_criteria['keywords'])}, themes={len(selected_criteria['themes'])}, engines={len(selected_criteria.get('engines', []))}")

    stage_start = time.time()
    cache_data = json.dumps({
        'g': selected_criteria['genres'],
        'k': selected_criteria['keywords'],
        't': selected_criteria['themes'],
        'pp': selected_criteria['perspectives'],
        'd': selected_criteria['developers'],
        'gm': selected_criteria['game_modes'],
        'e': selected_criteria['engines'],
        'search_filters': search_filters,  # Добавляем поисковые фильтры в ключ кэша
        'version': 'v18_with_search_filters'
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
    similarity_engine = GameSimilarity()
    print(f"GameSimilarity engine created: {time.time() - stage_start:.3f}s")

    stage_start = time.time()
    similar_games = similarity_engine.find_similar_games(
        source_game=virtual_game,
        search_filters=search_filters  # Передаем поисковые фильтры
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
    """Get similar games for a specific game without limits - ОПТИМИЗИРОВАНО."""
    from .base_views import _generate_cache_key, CACHE_TIMES
    import hashlib

    start_total = time.time()
    print(f"\n=== get_similar_games_for_game START for game {game_obj.id} - {game_obj.name} ===")

    stage_start = time.time()
    cache_key_data = {
        'game_id': game_obj.id,
        'platforms': sorted(selected_platforms) if selected_platforms else [],
        'search_filters': search_filters,  # Добавляем поисковые фильтры в ключ кэша
        'version': 'v18_with_search_filters',
        'game_cached_counts': {
            'genres': game_obj.cached_genre_count,
            'keywords': game_obj.cached_keyword_count,
            'platforms': game_obj.cached_platform_count,
            'developers': game_obj.cached_developer_count,
            'engines': getattr(game_obj, 'cached_engine_count', 0),
        }
    }

    cache_key = f'similar_for_game_{_generate_cache_key(cache_key_data)}'
    cached_data = cache.get(cache_key)
    print(f"Cache check: {time.time() - stage_start:.3f}s")

    if cached_data:
        print(f"Cache HIT for game {game_obj.id}")
        similar_games = cached_data['games']
        total_count = cached_data['count']
        print(f"Total time: {time.time() - start_total:.3f}s")
        print("=== get_similar_games_for_game END (CACHE HIT) ===\n")
        return similar_games, total_count

    print(f"Cache MISS for game {game_obj.id} - calculating similarity...")

    stage_start = time.time()
    similarity_engine = GameSimilarity()
    print(f"GameSimilarity engine created: {time.time() - stage_start:.3f}s")

    stage_start = time.time()
    similar_games = similarity_engine.find_similar_games(
        source_game=game_obj,
        min_similarity=0,
        search_filters=search_filters  # Передаем поисковые фильтры
    )
    print(f"find_similar_games executed: {time.time() - stage_start:.3f}s, found {len(similar_games)} games")

    total_count = len(similar_games)

    stage_start = time.time()
    cache.set(cache_key, {
        'games': similar_games,
        'count': total_count,
        'timestamp': time.time()
    }, CACHE_TIMES['aggressive']['similar_for_game'])
    print(f"Cache save: {time.time() - stage_start:.3f}s")

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

    if selected_criteria['engines']:
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