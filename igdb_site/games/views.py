from .similarity import GameSimilarity, VirtualGame
from django.db import models
from django.shortcuts import render, get_object_or_404
from .models import Game, Genre, Keyword, KeywordCategory, Platform, Theme, PlayerPerspective, Company, Series, GameMode

from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger


def game_list(request):
    """Список всех игр с поиском похожих игр по выбранным критериям"""
    games = Game.objects.all().prefetch_related(
        'genres', 'platforms', 'keywords', 'themes', 'developers', 'player_perspectives', 'game_modes'
    )

    # Получаем параметры для ВСЕХ критериев (ДОБАВИТЬ Game Modes)
    find_similar = request.GET.get('find_similar') == '1'
    genres_param = request.GET.get('g', '')
    keywords_param = request.GET.get('k', '')
    platforms_param = request.GET.get('p', '')
    themes_param = request.GET.get('t', '')
    perspectives_param = request.GET.get('pp', '')
    developers_param = request.GET.get('d', '')
    game_modes_param = request.GET.get('gm', '')  # ← ДОБАВИТЬ
    source_game_id = request.GET.get('source_game')

    # Конвертируем строки в списки integers (ДОБАВИТЬ Game Modes)
    selected_genres_int = [int(g) for g in genres_param.split(',') if g.strip()] if genres_param else []
    selected_keywords_int = [int(k) for k in keywords_param.split(',') if k.strip()] if keywords_param else []
    selected_platforms_int = [int(p) for p in platforms_param.split(',') if p.strip()] if platforms_param else []
    selected_themes_int = [int(t) for t in themes_param.split(',') if t.strip()] if themes_param else []
    selected_perspectives_int = [int(pp) for pp in perspectives_param.split(',') if pp.strip()] if perspectives_param else []
    selected_developers_int = [int(d) for d in developers_param.split(',') if d.strip()] if developers_param else []
    selected_game_modes_int = [int(gm) for gm in game_modes_param.split(',') if gm.strip()] if game_modes_param else []  # ← ДОБАВИТЬ

    # Автоматически включаем режим похожих игр если переданы критерии (ОБНОВИТЬ условие)
    if not find_similar and (
            selected_genres_int or selected_keywords_int or selected_themes_int or
            selected_developers_int or selected_perspectives_int or selected_game_modes_int):  # ← ДОБАВИТЬ
        find_similar = True

    # Сортировка
    default_sort = '-similarity' if find_similar else '-rating_count'
    current_sort = request.GET.get('sort', default_sort)

    popular_keywords = Keyword.objects.filter(cached_usage_count__gt=0).order_by('-cached_usage_count')
    platforms = Platform.objects.annotate(
        game_count=models.Count('game')
    ).filter(game_count__gt=0).order_by('-game_count', 'name')

    # Новые данные для фильтров
    game_modes = GameMode.objects.annotate(
        game_count=models.Count('game')
    ).filter(game_count__gt=0).order_by('name')

    themes = Theme.objects.annotate(
        game_count=models.Count('game')
    ).filter(game_count__gt=0).order_by('name')

    perspectives = PlayerPerspective.objects.annotate(
        game_count=models.Count('game')
    ).filter(game_count__gt=0).order_by('name')

    developers = Company.objects.annotate(
        developed_game_count=models.Count('developed_games')
    ).filter(developed_game_count__gt=0).order_by('name')[:50]

    show_similarity = False
    games_with_similarity = []
    source_game = None
    total_count = 0

    # РЕЖИМ ПОИСКА ПОХОЖИХ ИГР ПО КРИТЕРИЯМ
    if find_similar and (
            selected_genres_int or selected_keywords_int or selected_themes_int or
            selected_developers_int or selected_perspectives_int or selected_game_modes_int):  # ← ДОБАВИТЬ

        show_similarity = True

        # Создаем виртуальную игру для критериев (БЕЗ game_mode_ids для схожести)
        virtual_game = VirtualGame(
            genre_ids=selected_genres_int,
            keyword_ids=selected_keywords_int,
            theme_ids=selected_themes_int,
            perspective_ids=selected_perspectives_int,
            developer_ids=selected_developers_int,
            game_mode_ids=selected_game_modes_int
        )

        # ИСПОЛЬЗУЕМ НОВЫЙ АЛГОРИТМ
        similarity_engine = GameSimilarity()
        similar_games_data = similarity_engine.find_similar_games(
            source_game=virtual_game,
            limit=200,
            min_similarity=15
        )

        # ФИЛЬТРУЕМ ПО ПЛАТФОРМАМ
        if selected_platforms_int:
            similar_games_data = [
                item for item in similar_games_data
                if any(platform.id in selected_platforms_int for platform in item['game'].platforms.all())
            ]

        # Формируем структуру данных
        games_with_similarity = [
            {
                'game': item['game'],
                'similarity': item['similarity'],
                'common_genres': item['common_genres'],
                'common_keywords': item['common_keywords'],
                'common_themes': item['common_themes'],
                'common_developers': item['common_developers'],
                'common_perspectives': item['common_perspectives'],
                'common_game_modes': item['common_game_modes']
            }
            for item in similar_games_data
        ]

        total_count = len(games_with_similarity)

        # Создаем простой source_game для передачи в шаблон
        class SimpleSourceGame:
            def __init__(self, source_game_id=None, genres=None, keywords=None, themes=None, developers=None,
                         perspectives=None, game_modes=None):  # ← ДОБАВИТЬ
                self.id = source_game_id
                self.name = "Search Criteria"
                self.source_game_id = source_game_id
                self.genres_ids = genres or []
                self.keywords_ids = keywords or []
                self.themes_ids = themes or []
                self.developers_ids = developers or []
                self.perspectives_ids = perspectives or []
                self.game_modes_ids = game_modes or []  # ← ДОБАВИТЬ

        source_game = SimpleSourceGame(
            source_game_id=source_game_id,
            genres=selected_genres_int,
            keywords=selected_keywords_int,
            themes=selected_themes_int,
            developers=selected_developers_int,
            perspectives=selected_perspectives_int
        )

        source_game = SimpleSourceGame(
            source_game_id=source_game_id,
            genres=selected_genres_int,
            keywords=selected_keywords_int,
            themes=selected_themes_int,
            developers=selected_developers_int,
            perspectives=selected_perspectives_int
        )

        source_game = SimpleSourceGame(
            source_game_id=source_game_id,
            genres=selected_genres_int,
            keywords=selected_keywords_int,
            themes=selected_themes_int,
            developers=selected_developers_int,
            perspectives=selected_perspectives_int
        )

        # Сортировка
        if current_sort == '-rating_count':
            games_with_similarity.sort(key=lambda x: x['game'].rating_count or 0, reverse=True)
        elif current_sort == '-rating':
            games_with_similarity.sort(key=lambda x: x['game'].rating or 0, reverse=True)
        elif current_sort == 'name':
            games_with_similarity.sort(key=lambda x: x['game'].name.lower())
        elif current_sort == '-name':
            games_with_similarity.sort(key=lambda x: x['game'].name.lower(), reverse=True)
        elif current_sort == '-first_release_date':
            games_with_similarity.sort(key=lambda x: x['game'].first_release_date or '', reverse=True)

    # ОБЫЧНЫЙ РЕЖИМ ФИЛЬТРАЦИИ
    else:
        if selected_genres_int:
            for genre_id in selected_genres_int:
                games = games.filter(genres__id=genre_id)

        if selected_keywords_int:
            for keyword_id in selected_keywords_int:
                games = games.filter(keywords__id=keyword_id)

        if selected_platforms_int:
            for platform_id in selected_platforms_int:
                games = games.filter(platforms__id=platform_id)

        # Новые фильтры
        if selected_themes_int:
            for theme_id in selected_themes_int:
                games = games.filter(themes__id=theme_id)

        if selected_perspectives_int:
            for perspective_id in selected_perspectives_int:
                games = games.filter(player_perspectives__id=perspective_id)

        if selected_developers_int:
            for developer_id in selected_developers_int:
                games = games.filter(developers__id=developer_id)

        # ДОБАВИТЬ фильтр по Game Modes
        if selected_game_modes_int:
            for game_mode_id in selected_game_modes_int:
                games = games.filter(game_modes__id=game_mode_id)

        if current_sort in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count',
                            '-first_release_date']:
            games = games.order_by(current_sort)

        total_count = games.count()

    # ПАГИНАЦИЯ
    page = request.GET.get('page', 1)

    if show_similarity:
        data_to_paginate = games_with_similarity
        total_count = len(games_with_similarity)
    else:
        data_to_paginate = games
        total_count = games.count()

    paginator = Paginator(data_to_paginate, 16)

    try:
        page_obj = paginator.page(page)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    # Генерируем компактный URL для шаблона с ВСЕМИ параметрами
    compact_url_params = generate_compact_url_params(
        find_similar=find_similar,
        genres=selected_genres_int,
        keywords=selected_keywords_int,
        platforms=selected_platforms_int,
        themes=selected_themes_int,
        perspectives=selected_perspectives_int,
        developers=selected_developers_int,
        game_modes=selected_game_modes_int,  # ← ДОБАВИТЬ
        sort=current_sort
    )

    context = {
        'games': page_obj if not show_similarity else [],
        'games_with_similarity': page_obj.object_list if show_similarity else [],
        'genres': Genre.objects.all().order_by('name'),
        'platforms': platforms,
        'themes': themes,
        'perspectives': perspectives,
        'developers': developers,
        'game_modes': game_modes,  # ← ДОБАВИТЬ
        'keyword_categories': KeywordCategory.objects.all(),
        'popular_keywords': popular_keywords,
        'current_sort': current_sort,
        'show_similarity': show_similarity,
        'selected_genres': selected_genres_int,
        'selected_keywords': selected_keywords_int,
        'selected_themes': selected_themes_int,
        'selected_perspectives': selected_perspectives_int,
        'selected_developers': selected_developers_int,
        'selected_game_modes': selected_game_modes_int,  # ← ДОБАВИТЬ
        'source_game': source_game,
        'selected_platforms': selected_platforms_int,
        'find_similar': find_similar,
        'compact_url_params': compact_url_params,
        'total_count': total_count,
        'page_obj': page_obj,
        'is_paginated': paginator.num_pages > 1,
        'paginator': paginator,
    }

    return render(request, 'games/game_list.html', context)

def generate_compact_url_params(find_similar=False, genres=None, keywords=None, platforms=None,
                                themes=None, perspectives=None, developers=None, game_modes=None, sort=None):
    """
    Генерирует компактные параметры URL с ВСЕМИ критериями
    """
    params = {}

    if find_similar:
        params['find_similar'] = '1'

    if genres:
        params['g'] = ','.join(str(g) for g in genres)

    if keywords:
        params['k'] = ','.join(str(k) for k in keywords)

    if platforms:
        params['p'] = ','.join(str(p) for p in platforms)

    # Новые параметры
    if themes:
        params['t'] = ','.join(str(t) for t in themes)

    if perspectives:
        params['pp'] = ','.join(str(pp) for pp in perspectives)

    if developers:
        params['d'] = ','.join(str(d) for d in developers)

    # ДОБАВИТЬ Game Modes
    if game_modes:
        params['gm'] = ','.join(str(gm) for gm in game_modes)

    if sort:
        params['sort'] = sort

    return params


def get_compact_game_list_url(find_similar=False, genres=None, keywords=None, platforms=None,
                              themes=None, perspectives=None, developers=None, sort=None):
    """
    Вспомогательная функция для генерации полного URL с компактными параметрами
    """
    params = generate_compact_url_params(
        find_similar=find_similar,
        genres=genres,
        keywords=keywords,
        platforms=platforms,
        themes=themes,
        perspectives=perspectives,
        developers=developers,
        sort=sort
    )

    from django.urls import reverse
    from urllib.parse import urlencode

    base_url = reverse('game_list')
    if params:
        return f"{base_url}?{urlencode(params)}"
    return base_url


def game_detail(request, pk):
    """Детальная страница игры с похожими играми"""
    game = get_object_or_404(
        Game.objects.prefetch_related(
            'keywords', 'genres', 'platforms', 'themes',
            'developers', 'player_perspectives', 'game_modes', 'publishers'  # ← ДОБАВИТЬ
        ),
        pk=pk
    )

    # ИСПОЛЬЗУЕМ НОВЫЙ АЛГОРИТМ
    similarity_engine = GameSimilarity()
    similar_games_data = similarity_engine.find_similar_games(game, limit=6, min_similarity=15)

    # Форматируем данные для шаблона
    similar_games = []
    for item in similar_games_data:
        similar_games.append({
            'game': item['game'],
            'similarity': item['similarity'],
            'common_genres': item['common_genres'],
            'common_themes': item['common_themes'],
            'common_developers': item['common_developers'],
            'common_perspectives': item['common_perspectives']
        })

    context = {
        'game': game,
        'similar_games': similar_games,
    }
    return render(request, 'games/game_detail.html', context)


def home(request):
    """Главная страница с популярными играми"""
    popular_games = Game.objects.filter(
        rating_count__gt=10
    ).prefetch_related('genres', 'platforms', 'themes', 'developers').order_by('-rating_count')[:12]

    recent_games = Game.objects.filter(
        first_release_date__isnull=False
    ).prefetch_related('genres', 'platforms', 'themes').order_by('-first_release_date')[:12]

    # Популярные теги
    popular_keywords = Keyword.objects.filter(
        cached_usage_count__gt=0
    ).select_related('category').order_by('-cached_usage_count')[:30]

    context = {
        'popular_games': popular_games,
        'recent_games': recent_games,
        'popular_keywords': popular_keywords,
    }
    return render(request, 'games/home.html', context)


def game_comparison(request, pk2):
    """Универсальное сравнение: игра-игра или критерии-игра с НОВЫМИ критериями"""
    try:
        game2 = get_object_or_404(
            Game.objects.prefetch_related('keywords', 'genres', 'platforms', 'themes', 'developers',
                                          'player_perspectives', 'game_modes'),
            pk=pk2
        )

        source_game_id = request.GET.get('source_game')
        genres_param = request.GET.get('g', '')
        keywords_param = request.GET.get('k', '')
        themes_param = request.GET.get('t', '')
        perspectives_param = request.GET.get('pp', '')
        developers_param = request.GET.get('d', '')
        game_modes_param = request.GET.get('gm', '')

        game1 = None
        criteria_genres = []
        criteria_keywords = []
        criteria_themes = []
        criteria_perspectives = []
        criteria_developers = []
        criteria_game_modes = []

        # Инициализируем переменные для выбранных критериев
        selected_genres = []
        selected_keywords = []
        selected_themes = []
        selected_perspectives = []
        selected_developers = []
        selected_game_modes = []

        # ВСЕГДА показываем исходную игру, если есть source_game_id
        if source_game_id:
            try:
                game1 = Game.objects.get(pk=source_game_id)
                # Даже если переданы критерии, показываем исходную игру
                is_criteria_comparison = False
            except Game.DoesNotExist:
                game1 = None
                is_criteria_comparison = True
        else:
            is_criteria_comparison = True

        # Если нет исходной игры, но есть критерии - используем критерии
        if is_criteria_comparison and (
                genres_param or keywords_param or themes_param or perspectives_param or developers_param or game_modes_param):
            selected_genres = [int(g) for g in genres_param.split(',') if g.strip()] if genres_param else []
            selected_keywords = [int(k) for k in keywords_param.split(',') if k.strip()] if keywords_param else []
            selected_themes = [int(t) for t in themes_param.split(',') if t.strip()] if themes_param else []
            selected_perspectives = [int(pp) for pp in perspectives_param.split(',') if
                                     pp.strip()] if perspectives_param else []
            selected_developers = [int(d) for d in developers_param.split(',') if d.strip()] if developers_param else []
            selected_game_modes = [int(gm) for gm in game_modes_param.split(',') if
                                   gm.strip()] if game_modes_param else []

            criteria_genres = Genre.objects.filter(id__in=selected_genres)
            criteria_keywords = Keyword.objects.filter(id__in=selected_keywords)
            criteria_themes = Theme.objects.filter(id__in=selected_themes)
            criteria_perspectives = PlayerPerspective.objects.filter(id__in=selected_perspectives)
            criteria_developers = Company.objects.filter(id__in=selected_developers)
            criteria_game_modes = GameMode.objects.filter(id__in=selected_game_modes)

        # ИСПОЛЬЗУЕМ НОВЫЙ АЛГОРИТМ ДЛЯ РАСЧЕТА
        similarity_engine = GameSimilarity()

        if is_criteria_comparison and not game1:
            source = VirtualGame(
                genre_ids=[g.id for g in criteria_genres],
                keyword_ids=[k.id for k in criteria_keywords],
                theme_ids=[t.id for t in criteria_themes],
                perspective_ids=[pp.id for pp in criteria_perspectives],
                developer_ids=[d.id for d in criteria_developers],
                game_mode_ids=[gm.id for gm in criteria_game_modes]
            )
        else:
            source = game1

        # РАССЧИТЫВАЕМ СХОЖЕСТЬ
        similarity_score = similarity_engine.calculate_similarity(source, game2)
        breakdown = similarity_engine.get_similarity_breakdown(source, game2)

        # Рассчитываем общие элементы для ВСЕХ критериев
        if is_criteria_comparison and not game1:
            # Сравниваем критерии с игрой
            shared_genres = game2.genres.all() & criteria_genres
            shared_keywords = game2.keywords.all() & criteria_keywords
            shared_themes = game2.themes.all() & criteria_themes
            shared_perspectives = game2.player_perspectives.all() & criteria_perspectives
            shared_developers = game2.developers.all() & criteria_developers
            shared_game_modes = game2.game_modes.all() & criteria_game_modes

            source_genres = set(criteria_genres)
            source_keywords = set(criteria_keywords)
            source_themes = set(criteria_themes)
            source_perspectives = set(criteria_perspectives)
            source_developers = set(criteria_developers)
            source_game_modes = set(criteria_game_modes)
        else:
            # Сравниваем две игры
            shared_genres = game1.genres.all() & game2.genres.all()
            shared_keywords = game1.keywords.all() & game2.keywords.all()
            shared_themes = game1.themes.all() & game2.themes.all()
            shared_perspectives = game1.player_perspectives.all() & game2.player_perspectives.all()
            shared_developers = game1.developers.all() & game2.developers.all()
            shared_game_modes = game1.game_modes.all() & game2.game_modes.all()

            source_genres = set(game1.genres.all())
            source_keywords = set(game1.keywords.all())
            source_themes = set(game1.themes.all())
            source_perspectives = set(game1.player_perspectives.all())
            source_developers = set(game1.developers.all())
            source_game_modes = set(game1.game_modes.all())

        target_genres = set(game2.genres.all())
        target_keywords = set(game2.keywords.all())
        target_themes = set(game2.themes.all())
        target_perspectives = set(game2.player_perspectives.all())
        target_developers = set(game2.developers.all())
        target_game_modes = set(game2.game_modes.all())

        # Группируем общие ключевые слова по категориям
        keyword_categories = KeywordCategory.objects.all()
        shared_keywords_by_category = {}

        for category in keyword_categories:
            category_keywords = shared_keywords.filter(category=category)
            if category_keywords.exists():
                shared_keywords_by_category[category.name] = category_keywords

        # Вычисляем среднюю схожесть для отображения на карточке критериев
        # (если это сравнение с критериями, а не с игрой)
        average_similarity = similarity_score if is_criteria_comparison and not game1 else None

        context = {
            'game1': game1,
            'game2': game2,
            'criteria_genres': criteria_genres,
            'criteria_keywords': criteria_keywords,
            'criteria_themes': criteria_themes,
            'criteria_perspectives': criteria_perspectives,
            'criteria_developers': criteria_developers,
            'criteria_game_modes': criteria_game_modes,
            'similarity_score': similarity_score,
            'shared_genres': shared_genres,
            'shared_keywords': shared_keywords,
            'shared_themes': shared_themes,
            'shared_perspectives': shared_perspectives,
            'shared_developers': shared_developers,
            'shared_game_modes': shared_game_modes,
            'shared_genres_count': shared_genres.count(),
            'shared_keywords_count': shared_keywords.count(),
            'shared_themes_count': shared_themes.count(),
            'shared_perspectives_count': shared_perspectives.count(),
            'shared_developers_count': shared_developers.count(),
            'shared_game_modes_count': shared_game_modes.count(),
            'shared_keywords_by_category': shared_keywords_by_category,
            'is_criteria_comparison': is_criteria_comparison and not game1,  # Только если нет game1
            # Breakdown data из нового алгоритма
            'breakdown': breakdown,
            # Константы для отображения в шаблоне
            'genres_weight': int(similarity_engine.GENRES_TOTAL_WEIGHT),
            'keywords_weight': int(similarity_engine.KEYWORDS_WEIGHT),
            'themes_weight': int(similarity_engine.THEMES_WEIGHT),
            'developers_weight': int(similarity_engine.DEVELOPERS_WEIGHT),
            'perspectives_weight': int(similarity_engine.PERSPECTIVES_WEIGHT),
            'game_modes_weight': int(similarity_engine.GAME_MODES_WEIGHT),
            'genres_exact_match_weight': int(similarity_engine.GENRES_EXACT_MATCH_WEIGHT),
            # Для кнопки "Back to Similar Games"
            'selected_genres': selected_genres,
            'selected_keywords': selected_keywords,
            'selected_themes': selected_themes,
            'selected_perspectives': selected_perspectives,
            'selected_developers': selected_developers,
            'selected_game_modes': selected_game_modes,
            # Для SVG иконки на карточке критериев
            'average_similarity': average_similarity,
        }

        return render(request, 'games/game_comparison.html', context)

    except Exception as e:
        from django.http import HttpResponseServerError
        return HttpResponseServerError(f"Error in comparison: {str(e)}")


def keyword_category_view(request, category_id):
    """Просмотр игр по категории ключевых слов"""
    category = get_object_or_404(KeywordCategory, id=category_id)

    games = Game.objects.filter(
        keywords__category=category
    ).prefetch_related('genres', 'platforms', 'keywords').distinct()

    # Популярные ключевые слова в этой категории
    popular_keywords = Keyword.objects.filter(
        category=category,
        game__isnull=False
    ).annotate(
        game_count=models.Count('game')
    ).order_by('-game_count')[:20]

    context = {
        'category': category,
        'games': games,
        'popular_keywords': popular_keywords,
    }
    return render(request, 'games/keyword_category.html', context)


def game_search(request):
    """Простой поиск игр по названию"""
    search_query = request.GET.get('q', '')

    games = Game.objects.all().prefetch_related('genres', 'platforms', 'themes', 'developers')

    if search_query:
        games = games.filter(name__icontains=search_query)

    # Сортировка по популярности
    games = games.order_by('-rating_count', '-rating')

    context = {
        'games': games,
        'search_query': search_query,
        'total_results': games.count(),
    }
    return render(request, 'games/game_search.html', context)


def platform_list(request):
    """Страница со списком всех платформ"""
    platforms = Platform.objects.annotate(
        game_count=models.Count('game')
    ).filter(game_count__gt=0).order_by('-game_count', 'name')

    context = {
        'platforms': platforms,
    }
    return render(request, 'games/platform_list.html', context)


def platform_games(request, platform_id):
    """Список игр для конкретной платформы"""
    platform = get_object_or_404(Platform, id=platform_id)
    games = Game.objects.filter(platforms=platform).prefetch_related(
        'genres', 'platforms', 'keywords', 'themes', 'developers'
    ).order_by('-rating_count', '-rating')

    # Пагинация
    paginator = Paginator(games, 20)
    page = request.GET.get('page', 1)

    try:
        page_obj = paginator.page(page)
    except PageNotAnInteger:
        page_obj = paginator.page(1)
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages)

    context = {
        'platform': platform,
        'games': page_obj,
        'total_games': games.count(),
        'page_obj': page_obj,
        'is_paginated': paginator.num_pages > 1,
    }
    return render(request, 'games/platform_games.html', context)
