from .similarity import GameSimilarity, VirtualGame
from django.db import models
from django.shortcuts import render, get_object_or_404
from .models import Game, Genre, Keyword, KeywordCategory, Platform

from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger


def game_list(request):
    """Список всех игр с поиском похожих игр по выбранным критериям"""
    games = Game.objects.all().prefetch_related('genres', 'platforms', 'keywords')

    # Получаем параметры
    find_similar = request.GET.get('find_similar') == '1'
    genres_param = request.GET.get('g', '')
    keywords_param = request.GET.get('k', '')
    platforms_param = request.GET.get('p', '')

    # Конвертируем строки в списки integers
    selected_genres_int = [int(g) for g in genres_param.split(',') if g.strip()] if genres_param else []
    selected_keywords_int = [int(k) for k in keywords_param.split(',') if k.strip()] if keywords_param else []
    selected_platforms_int = [int(p) for p in platforms_param.split(',') if p.strip()] if platforms_param else []

    # Старый формат (для обратной совместимости)
    if not selected_genres_int:
        selected_genres = request.GET.getlist('genre')
        selected_genres_int = [int(g) for g in selected_genres if g]

    if not selected_keywords_int:
        selected_keywords = request.GET.getlist('keyword')
        selected_keywords_int = [int(k) for k in selected_keywords if k]

    if not selected_platforms_int:
        selected_platforms = request.GET.getlist('platform')
        selected_platforms_int = [int(p) for p in selected_platforms if p]

    # Автоматически включаем режим похожих игр если переданы критерии
    if not find_similar and (selected_genres_int or selected_keywords_int):
        find_similar = True

    # Сортировка
    default_sort = '-similarity' if find_similar else '-rating_count'
    current_sort = request.GET.get('sort', default_sort)

    popular_keywords = Keyword.objects.filter(usage_count__gt=0).order_by('-usage_count')[:100]
    platforms = Platform.objects.annotate(
        game_count=models.Count('game')
    ).filter(game_count__gt=0).order_by('-game_count', 'name')

    show_similarity = False
    games_with_similarity = []
    source_game = None
    total_count = 0

    # РЕЖИМ ПОИСКА ПОХОЖИХ ИГР ПО КРИТЕРИЯМ
    if find_similar and (selected_genres_int or selected_keywords_int):
        show_similarity = True

        # Создаем виртуальную игру для критериев
        virtual_game = VirtualGame(
            genre_ids=selected_genres_int,
            keyword_ids=selected_keywords_int
        )

        # ИСПОЛЬЗУЕМ ЕДИНЫЙ МЕТОД
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
                'similarity': item['similarity']
            }
            for item in similar_games_data
        ]

        total_count = len(games_with_similarity)

        # Создаем простой source_game для передачи в шаблон
        class SimpleSourceGame:
            def __init__(self, source_game_id=None, genres=None, keywords=None):
                self.id = source_game_id
                self.name = "Search Criteria"
                self.source_game_id = source_game_id
                self.genres = genres or []
                self.keywords = keywords or []

        source_game_id = request.GET.get('source_game')
        source_game = SimpleSourceGame(
            source_game_id=source_game_id,
            genres=selected_genres_int,
            keywords=selected_keywords_int
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

    # Генерируем компактный URL для шаблона
    compact_url_params = generate_compact_url_params(
        find_similar=find_similar,
        genres=selected_genres_int,
        keywords=selected_keywords_int,
        platforms=selected_platforms_int,
        sort=current_sort
    )

    context = {
        'games': page_obj if not show_similarity else [],
        'games_with_similarity': page_obj.object_list if show_similarity else [],
        'genres': Genre.objects.all().order_by('name'),
        'platforms': platforms,
        'keyword_categories': KeywordCategory.objects.all(),
        'popular_keywords': popular_keywords,
        'current_sort': current_sort,
        'show_similarity': show_similarity,
        'selected_genres': selected_genres_int,
        'selected_keywords': selected_keywords_int,
        'selected_platforms': selected_platforms_int,
        'find_similar': find_similar,
        'compact_url_params': compact_url_params,
        'source_game': source_game,
        'total_count': total_count,
        'page_obj': page_obj,
        'is_paginated': paginator.num_pages > 1,
        'paginator': paginator,
    }

    return render(request, 'games/game_list.html', context)

def generate_compact_url_params(find_similar=False, genres=None, keywords=None, platforms=None, sort=None):
    """
    Генерирует компактные параметры URL
    """
    params = {}

    if find_similar:
        params['find_similar'] = '1'

    if genres:
        params['g'] = ','.join(str(g) for g in genres)

    if keywords:
        params['k'] = ','.join(str(k) for k in keywords)

    if platforms:  # ДОБАВЛЕНО: параметр platforms
        params['p'] = ','.join(str(p) for p in platforms)

    if sort:
        params['sort'] = sort

    return params


def get_compact_game_list_url(find_similar=False, genres=None, keywords=None, platforms=None, sort=None):
    """
    Вспомогательная функция для генерации полного URL с компактными параметрами
    """
    params = generate_compact_url_params(
        find_similar=find_similar,
        genres=genres,
        keywords=keywords,
        platforms=platforms,  # ДОБАВЛЕНО
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
        Game.objects.prefetch_related('keywords', 'genres', 'platforms'),
        pk=pk
    )

    # ИСПОЛЬЗУЕМ ЕДИНЫЙ МЕТОД
    similarity_engine = GameSimilarity()
    similar_games_data = similarity_engine.find_similar_games(game, limit=6, min_similarity=15)

    context = {
        'game': game,
        'similar_games': similar_games_data,
    }
    return render(request, 'games/game_detail.html', context)


def home(request):
    """Главная страница с популярными играми"""
    popular_games = Game.objects.filter(
        rating_count__gt=10
    ).prefetch_related('genres', 'platforms').order_by('-rating_count')[:12]

    recent_games = Game.objects.filter(
        first_release_date__isnull=False
    ).prefetch_related('genres', 'platforms').order_by('-first_release_date')[:12]

    # Популярные теги
    popular_keywords = Keyword.objects.filter(
        usage_count__gt=0
    ).select_related('category').order_by('-usage_count')[:30]

    context = {
        'popular_games': popular_games,
        'recent_games': recent_games,
        'popular_keywords': popular_keywords,
    }
    return render(request, 'games/home.html', context)


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


def game_comparison(request, pk2):
    """Универсальное сравнение: игра-игра или критерии-игра"""
    try:
        game2 = get_object_or_404(
            Game.objects.prefetch_related('keywords', 'genres', 'platforms'),
            pk=pk2
        )

        source_game_id = request.GET.get('source_game')
        genres_param = request.GET.get('genres', '')
        keywords_param = request.GET.get('keywords', '')

        game1 = None
        criteria_genres = []
        criteria_keywords = []
        is_criteria_comparison = True

        # Определяем source (игра или критерии)
        if source_game_id:
            try:
                game1 = Game.objects.get(pk=source_game_id)
                source_genres = set(game1.genres.values_list('id', flat=True))
                source_keywords = set(game1.keywords.values_list('id', flat=True))

                current_genres = set([int(g) for g in genres_param.split(',') if g.strip()]) if genres_param else set()
                current_keywords = set(
                    [int(k) for k in keywords_param.split(',') if k.strip()]) if keywords_param else set()

                # Если критерии совпадают с исходной игрой - сравниваем игры
                if source_genres == current_genres and source_keywords == current_keywords:
                    is_criteria_comparison = False
                else:
                    # Критерии изменились - сравниваем с критериями
                    criteria_genres = Genre.objects.filter(id__in=current_genres)
                    criteria_keywords = Keyword.objects.filter(id__in=current_keywords)

            except Game.DoesNotExist:
                selected_genres = [int(g) for g in genres_param.split(',') if g.strip()] if genres_param else []
                selected_keywords = [int(k) for k in keywords_param.split(',') if k.strip()] if keywords_param else []
                criteria_genres = Genre.objects.filter(id__in=selected_genres)
                criteria_keywords = Keyword.objects.filter(id__in=selected_keywords)
        else:
            selected_genres = [int(g) for g in genres_param.split(',') if g.strip()] if genres_param else []
            selected_keywords = [int(k) for k in keywords_param.split(',') if k.strip()] if keywords_param else []
            criteria_genres = Genre.objects.filter(id__in=selected_genres)
            criteria_keywords = Keyword.objects.filter(id__in=selected_keywords)

        # ИСПОЛЬЗУЕМ ЕДИНЫЙ МЕТОД ДЛЯ РАСЧЕТА
        similarity_engine = GameSimilarity()

        if is_criteria_comparison:
            source = VirtualGame(
                genre_ids=[g.id for g in criteria_genres],
                keyword_ids=[k.id for k in criteria_keywords]
            )
        else:
            source = game1

        similarity_score = similarity_engine.calculate_similarity(source, game2)
        breakdown = similarity_engine.get_similarity_breakdown(source, game2)

        # Рассчитываем общие элементы
        if is_criteria_comparison:
            shared_genres = game2.genres.all() & criteria_genres
            shared_keywords = game2.keywords.all() & criteria_keywords
            source_genres = set(criteria_genres)
            source_keywords = set(criteria_keywords)
        else:
            shared_genres = game1.genres.all() & game2.genres.all()
            shared_keywords = game1.keywords.all() & game2.keywords.all()
            source_genres = set(game1.genres.all())
            source_keywords = set(game1.keywords.all())

        target_genres = set(game2.genres.all())
        target_keywords = set(game2.keywords.all())

        # Расчет breakdown данных для отображения
        are_genres_exactly_same = source_genres == target_genres
        missing_genres = source_genres - target_genres

        # Извлекаем данные из breakdown
        genres_score = breakdown['genres']['score']
        keywords_score = breakdown['keywords']['score']
        genre_overlap_score = breakdown['genres']['components']['partial_match']

        # Группируем общие ключевые слова по категориям
        keyword_categories = KeywordCategory.objects.all()
        shared_keywords_by_category = {}

        for category in keyword_categories:
            category_keywords = shared_keywords.filter(category=category)
            if category_keywords.exists():
                shared_keywords_by_category[category.name] = category_keywords

        context = {
            'game1': game1,
            'game2': game2,
            'criteria_genres': criteria_genres,
            'criteria_keywords': criteria_keywords,
            'similarity_score': similarity_score,
            'shared_genres': shared_genres,
            'shared_keywords': shared_keywords,
            'shared_genres_count': shared_genres.count(),
            'shared_keywords_count': shared_keywords.count(),
            'shared_keywords_by_category': shared_keywords_by_category,
            'is_criteria_comparison': is_criteria_comparison,
            # Breakdown data
            'are_genres_exactly_same': are_genres_exactly_same,
            'genre_overlap_score': genre_overlap_score,
            'keyword_overlap_score': keywords_score,
            'genres_score': genres_score,
            'keywords_score': keywords_score,
            'missing_genres_count': len(missing_genres),
            'total_source_genres': len(source_genres),
            'missing_genres': [genre.name for genre in missing_genres],
            # Константы для отображения в шаблоне
            'genres_total_weight': int(similarity_engine.GENRES_TOTAL_WEIGHT),
            'genres_exact_match_weight': int(similarity_engine.GENRES_EXACT_MATCH_WEIGHT),
            'genres_partial_match_weight': int(similarity_engine.GENRES_PARTIAL_MATCH_WEIGHT),
            'keywords_total_weight': int(similarity_engine.KEYWORDS_TOTAL_WEIGHT),
        }

        return render(request, 'games/game_comparison.html', context)

    except Exception as e:
        from django.http import HttpResponseServerError
        return HttpResponseServerError(f"Error in comparison: {str(e)}")


def game_search(request):
    """Простой поиск игр по названию"""
    search_query = request.GET.get('q', '')

    games = Game.objects.all().prefetch_related('genres', 'platforms')

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
        'genres', 'platforms', 'keywords'
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
