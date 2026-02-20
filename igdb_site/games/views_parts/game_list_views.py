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
        cache_key = GameCardCache._generate_key(
            game.id, show_similarity, similarity_percent, 'normal'
        )

        # Получаем самую свежую активную карточку
        card_cache = GameCardCache.objects.filter(
            cache_key=cache_key,
            is_active=True
        ).order_by('-updated_at', '-created_at').first()

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
    Процент схожести передается отдельно, не кэшируется в карточке.
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
            print(f"  Similar mode item - game ID: {game_obj.id}, similarity from item: {similarity}")
        else:
            game_obj = item
            game_ids.append(game_obj.id)
            game_items.append((item, game_obj, None, False))
            print(f"  Regular mode item - game ID: {game_obj.id}")

    # Получаем текущую версию кэша из модели
    from games.models import GameCardCache
    current_cache_version = GameCardCache.CARD_CACHE_VERSION

    print(f"\nCurrent template version: {current_cache_version}")

    # Загружаем ВСЕ карточки из БД (активные и неактивные)
    all_cards_in_db = set()
    valid_cards = {}

    try:
        # Сначала проверим, есть ли вообще карточки в БД (даже неактивные)
        all_cards = GameCardCache.objects.filter(
            game_id__in=game_ids
        ).values('game_id', 'is_active', 'template_version')

        print(f"\nCards in DB (any status): {list(all_cards)}")

        for card_info in all_cards:
            all_cards_in_db.add(card_info['game_id'])

        # Теперь загружаем только активные для использования
        card_objects = GameCardCache.objects.filter(
            game_id__in=game_ids,
            is_active=True
        ).select_related('game')

        print(f"\nFound {card_objects.count()} active cards in DB for these games")

        for card in card_objects:
            if card.template_version == current_cache_version:
                valid_cards[card.game_id] = card
                print(f"  ✅ Valid card for game {card.game_id}")
            else:
                print(
                    f"  ❌ Outdated card for game {card.game_id} (v{card.template_version} vs current v{current_cache_version})")
                card.is_active = False
                card.save(update_fields=['is_active'])

        print(f"\nFound {len(valid_cards)} valid cached cards")
        print(f"Games with ANY card in DB: {len(all_cards_in_db)}")
        print(f"Games with NO card in DB: {set(game_ids) - all_cards_in_db}")

    except Exception as e:
        logger.error(f"Error batch loading card caches: {str(e)}")
        print(f"ERROR loading cards: {str(e)}")

    # Обрабатываем каждый элемент
    cards_to_create = []
    processed_items = []

    for item, game_obj, similarity, is_similar_mode in game_items:
        cached_card = valid_cards.get(game_obj.id)

        print(f"\nProcessing game {game_obj.id} ({game_obj.name}):")
        print(f"  has valid cached_card: {cached_card is not None}")
        print(f"  has ANY card in DB: {game_obj.id in all_cards_in_db}")

        # Устанавливаем similarity если нужно
        if is_similar_mode:
            if similarity is not None and similarity > 0:
                game_obj.similarity = float(similarity)
                if isinstance(item, dict):
                    item['similarity'] = similarity
            else:
                if hasattr(game_obj, 'similarity'):
                    delattr(game_obj, 'similarity')
                if isinstance(item, dict) and 'similarity' in item:
                    del item['similarity']
        else:
            if hasattr(game_obj, 'similarity'):
                delattr(game_obj, 'similarity')

        if cached_card:
            # Используем готовую карточку
            try:
                cached_card.increment_hit()
                if isinstance(item, dict):
                    item['cached_card'] = cached_card.rendered_card
                else:
                    item.cached_card = cached_card.rendered_card
                processed_items.append(item)
                print(f"  ✅ Using cached card")
                continue
            except Exception as e:
                logger.error(f"Error using cached card for game {game_obj.id}: {str(e)}")
                print(f"  ERROR using cached card: {str(e)}")

        # Карточки нет или она невалидна
        print(f"  Rendering new card")

        card_context = {
            'game': game_obj,
            'show_similarity': show_similarity,
            'source_game': context.get('source_game')
        }

        if is_similar_mode and similarity is not None and similarity > 0:
            card_context['similarity'] = similarity

        rendered_card = render_to_string('games/partials/_game_card.html', card_context)

        # Добавляем в список на создание ТОЛЬКО если карточки вообще нет в БД
        if game_obj.id not in all_cards_in_db:
            from games.utils.game_card_utils import GameCardCreator
            related_data = GameCardCreator._extract_related_data(game_obj)
            cards_to_create.append((
                game_obj,
                rendered_card,
                related_data
            ))
            print(f"  ✨ Will save to DB (new card)")
        else:
            print(f"  ⏭️ Not saving - card exists but outdated (will be updated on next run)")

        # Сразу используем отрендеренный HTML
        if isinstance(item, dict):
            item['cached_card'] = rendered_card
        else:
            item.cached_card = rendered_card

        processed_items.append(item)

    print(f"\nProcessed {len(processed_items)} items")
    print(f"Cards to create (really new, never in DB): {len(cards_to_create)}")

    # Сохраняем ТОЛЬКО действительно новые карточки
    if cards_to_create:
        try:
            print(f"\n=== SAVING {len(cards_to_create)} NEW CARDS TO DB ===")
            from games.models import GameCardCache
            stats = GameCardCache.bulk_create_or_update_cards(cards_to_create, batch_size=50)
            print(f"SAVE RESULT: {stats}")
        except Exception as e:
            logger.error(f"Failed to save cards: {str(e)}")
            print(f"ERROR saving cards: {str(e)}")
    else:
        print(f"\n✅ No new cards to save")

    print("=== END UPDATE DEBUG ===\n")

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
        page_num: int
) -> Dict[str, Any]:
    """Режим похожих игр с СЕРВЕРНОЙ пагинацией."""
    current_sort = params.get('sort', '-similarity')

    if source_game_obj:
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
        similar_games_data, total_count = get_similar_games_for_criteria(selected_criteria)
        source_display = "Search Criteria"

        source_game = SimpleSourceGame(
            game_obj=None,
            criteria=selected_criteria,
            display_name=source_display
        )

    games_with_similarity = _format_similar_games_data(similar_games_data, limit=total_count)
    _sort_similar_games(games_with_similarity, current_sort)

    # Используем пагинатор Django
    paginator = Paginator(games_with_similarity, ITEMS_PER_PAGE)

    try:
        page_obj = paginator.page(page_num)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    # Получаем games_with_similarity для текущей страницы
    current_games_with_similarity = list(page_obj.object_list)

    return {
        'page_obj': page_obj,
        'paginator': paginator,
        'is_paginated': paginator.num_pages > 1,
        'total_count': paginator.count,
        'current_page': page_obj.number,
        'games_with_similarity': current_games_with_similarity,
        'show_similarity': True,
        'find_similar': True,
        'source_game': source_game,
        'source_game_obj': source_game_obj,
    }


def game_list(request: HttpRequest) -> HttpResponse:
    """
    Main game list function - returns empty container for AJAX to populate.
    No data loading happens here - all loading is done via AJAX.
    """
    start_time = time.time()

    params = extract_request_params(request)
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

    filter_data = _get_optimized_filter_data()

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

        'execution_time': round(time.time() - start_time, 3),

        'debug_info': {
            'mode': 'ajax_only',
            'message': 'Initial page load - data will be loaded via AJAX',
            'find_similar': find_similar,
            'has_source_game': source_game_obj is not None
        }
    }

    # Если есть source_game, создаем SimpleSourceGame для шаблонов
    if source_game_obj:
        from .base_views import SimpleSourceGame

        # Собираем критерии из игры
        game_criteria = {
            'genres': [g.id for g in source_game_obj.genres.all()] if hasattr(source_game_obj, 'genres') else [],
            'keywords': [k.id for k in source_game_obj.keywords.all()] if hasattr(source_game_obj, 'keywords') else [],
            'themes': [t.id for t in source_game_obj.themes.all()] if hasattr(source_game_obj, 'themes') else [],
            'perspectives': [p.id for p in source_game_obj.player_perspectives.all()] if hasattr(source_game_obj,
                                                                                                 'player_perspectives') else [],
            'developers': [d.id for d in source_game_obj.developers.all()] if hasattr(source_game_obj,
                                                                                      'developers') else [],
            'game_modes': [gm.id for gm in source_game_obj.game_modes.all()] if hasattr(source_game_obj,
                                                                                        'game_modes') else [],
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

    # Определяем режим
    if find_similar and (source_game_obj or any([
        selected_criteria['genres'],
        selected_criteria['keywords'],
        selected_criteria['themes'],
        selected_criteria['perspectives'],
        selected_criteria['game_modes']
    ])):
        # Режим похожих игр
        mode_result = _get_similar_games_mode_with_pagination(
            params, selected_criteria, source_game_obj, page_num
        )

        games_with_similarity = mode_result.get('games_with_similarity', [])
        source_game = mode_result.get('source_game')
        paginator = mode_result.get('paginator')

        # Получаем total_pages из пагинатора
        total_pages = paginator.num_pages if paginator else 1
        total_count = mode_result.get('total_count', 0)

        # Добавляем кэшированные карточки
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
            # Явно передаем параметры для пагинации
            'request': request,  # Передаем request для доступа к GET параметрам
        }
    else:
        # Обычный режим
        mode_result = _get_all_games_mode_with_pagination(
            selected_criteria, sort_field, page_num
        )

        games = list(mode_result.get('page_obj', {}).object_list) if mode_result.get('page_obj') else []
        paginator = mode_result.get('paginator')

        # Получаем total_pages из пагинатора
        total_pages = paginator.num_pages if paginator else 1
        total_count = mode_result.get('total_count', 0)

        # Добавляем кэшированные карточки
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
            # Явно передаем параметры для пагинации
            'request': request,  # Передаем request для доступа к GET параметрам
        }

    # Добавляем отладочную информацию в скрытые поля
    template_context['debug_total_pages'] = template_context['total_pages']
    template_context['debug_current_page'] = page_num

    # ВАЖНО: Передаем также все необходимые данные для фильтров
    template_context['genres'] = _get_cached_genres_list()
    filter_data = _get_optimized_filter_data()
    template_context['themes'] = filter_data['themes']
    template_context['perspectives'] = filter_data['perspectives']
    template_context['game_modes'] = filter_data['game_modes']
    template_context['keywords'] = filter_data['keywords']
    template_context['platforms'] = filter_data['platforms']
    template_context['popular_keywords'] = filter_data['popular_keywords']
    template_context['game_types'] = GameTypeEnum.CHOICES
    template_context['years_range'] = _get_cached_years_range()
    template_context['current_year'] = timezone.now().year

    # Рендерим полный шаблон
    html = render_to_string('games/game_list/_games_results.html', template_context)

    response = HttpResponse(html)
    response['Content-Type'] = 'text/html; charset=utf-8'
    response['X-AJAX-Page'] = str(page_num)
    response['X-Total-Pages'] = str(template_context['total_pages'])
    response['X-Response-Time'] = f"{time.time() - start_time:.3f}s"

    return response


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

    # УБРАНО: вся логика установки min_similarity в зависимости от критериев
    # Теперь просто вызываем find_similar_games без указания порога,
    # и он использует DEFAULT_MIN_SIMILARITY из класса GameSimilarity

    similar_games = similarity_engine.find_similar_games(
        source_game=virtual_game,
        limit=500
        # min_similarity больше не передаем
    )

    total_count = len(similar_games)

    cache_time = 10800
    if selected_criteria['genres']:
        cache_time = 7200

    cache.set(cache_key, {
        'games': similar_games,
        'count': total_count,
        'timestamp': time.time()
        # min_similarity больше не сохраняем в кэш
    }, cache_time)

    criteria_count = sum(len(v) for key, v in selected_criteria.items()
                         if key not in ['release_years', 'release_year_start', 'release_year_end'])

    logger.info(f"Similar games search took: {time.time() - start_time:.2f}s, "
                f"criteria: {criteria_count}, "
                f"results: {total_count}")

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

    # Используем пагинатор Django
    paginator = Paginator(games_with_similarity, ITEMS_PER_PAGE)

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
        'games_with_similarity': list(page_obj.object_list),
        'show_similarity': True,
        'find_similar': True,
        'source_game': source_game,
        'source_game_obj': source_game_obj,
    }


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