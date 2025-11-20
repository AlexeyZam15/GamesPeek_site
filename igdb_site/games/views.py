from .similarity import GameSimilarity
from django.db import models
from django.shortcuts import render, get_object_or_404
from .models import Game, Genre, Keyword, KeywordCategory, Platform, GameSimilarityCache


def game_list(request):
    """Список всех игр с фильтрацией, поиском и поиском похожих игр"""
    games = Game.objects.all().prefetch_related('genres', 'platforms', 'keywords__category')

    similar_game = None
    show_similarity = False

    # Поиск похожих игр
    similar_to_id = request.GET.get('similar_to')
    if similar_to_id:
        similar_game = get_object_or_404(Game, id=similar_to_id)
        show_similarity = True

        # БЫСТРЫЙ ПОИСК ИЗ КЭША
        similar_games_cache = GameSimilarityCache.objects.filter(
            game1=similar_game
        ).select_related('game2').order_by('-similarity_score')[:100]

        # Получаем ID игр для предзагрузки связанных данных
        game_ids = [cache_item.game2_id for cache_item in similar_games_cache]

        # Предзагружаем связанные данные для всех игр одним запросом
        games_with_relations = Game.objects.filter(
            id__in=game_ids
        ).prefetch_related('genres', 'platforms')

        # Создаем словарь для быстрого доступа
        games_dict = {game.id: game for game in games_with_relations}

        # Формируем список игр с процентами схожести
        games_with_similarity = []
        for cache_item in similar_games_cache:
            game_obj = games_dict.get(cache_item.game2_id)
            if game_obj:
                games_with_similarity.append({
                    'game': game_obj,
                    'similarity_percent': cache_item.similarity_score
                })

        # Применяем дополнительные фильтры если есть
        search_query = request.GET.get('search')
        if search_query:
            games_with_similarity = [item for item in games_with_similarity
                                     if search_query.lower() in item['game'].name.lower()]

        genre_filter = request.GET.get('genre')
        if genre_filter:
            games_with_similarity = [item for item in games_with_similarity
                                     if item['game'].genres.filter(id=genre_filter).exists()]

        platform_filter = request.GET.get('platform')
        if platform_filter:
            games_with_similarity = [item for item in games_with_similarity
                                     if item['game'].platforms.filter(id=platform_filter).exists()]

        keyword_category_filter = request.GET.get('keyword_category')
        if keyword_category_filter:
            games_with_similarity = [item for item in games_with_similarity
                                     if item['game'].keywords.filter(category__id=keyword_category_filter).exists()]

    # Обычный поиск (без похожих игр)
    else:
        search_query = request.GET.get('search')
        if search_query:
            games = games.filter(name__icontains=search_query)

        genre_filter = request.GET.get('genre')
        if genre_filter:
            games = games.filter(genres__id=genre_filter)

        platform_filter = request.GET.get('platform')
        if platform_filter:
            games = games.filter(platforms__id=platform_filter)

        keyword_category_filter = request.GET.get('keyword_category')
        if keyword_category_filter:
            games = games.filter(keywords__category__id=keyword_category_filter)

        # Сортировка
        sort = request.GET.get('sort', '-rating_count')
        if sort in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count']:
            games = games.order_by(sort)

    # Получаем популярные ключевые слова для фильтров
    popular_keywords = Keyword.objects.filter(usage_count__gt=0).order_by('-usage_count')[:20]

    # Подготавливаем контекст
    if show_similarity:
        context = {
            'games_with_similarity': games_with_similarity,
            'genres': Genre.objects.all(),
            'platforms': Platform.objects.all(),
            'keyword_categories': KeywordCategory.objects.all(),
            'popular_keywords': popular_keywords,
            'search_query': search_query or '',
            'current_sort': request.GET.get('sort', '-rating_count'),
            'similar_game': similar_game,
            'show_similarity': True,
        }
    else:
        context = {
            'games': games,
            'genres': Genre.objects.all(),
            'platforms': Platform.objects.all(),
            'keyword_categories': KeywordCategory.objects.all(),
            'popular_keywords': popular_keywords,
            'search_query': search_query or '',
            'current_sort': sort,
            'similar_game': None,
            'show_similarity': False,
        }

    return render(request, 'games/game_list.html', context)


def game_detail(request, pk):
    """Детальная страница игры с похожими играми"""
    game = get_object_or_404(
        Game.objects.prefetch_related('keywords__category', 'genres', 'platforms'),
        pk=pk
    )

    # Используем алгоритм похожести
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
    ).select_related('category').order_by('-usage_count')[:15]

    context = {
        'popular_games': popular_games,
        'recent_games': recent_games,
        'popular_keywords': popular_keywords,
    }
    return render(request, 'games/home.html', context)


def game_comparison(request, pk1, pk2):
    """Детальное сравнение двух игр"""
    game1 = get_object_or_404(
        Game.objects.prefetch_related('keywords__category', 'genres', 'platforms'),
        pk=pk1
    )
    game2 = get_object_or_404(
        Game.objects.prefetch_related('keywords__category', 'genres', 'platforms'),
        pk=pk2
    )

    similarity_engine = GameSimilarity()
    similarity_score = similarity_engine.calculate_similarity(game1, game2)
    breakdown = similarity_engine.get_similarity_breakdown(game1, game2)

    context = {
        'game1': game1,
        'game2': game2,
        'similarity_score': similarity_score,
        'breakdown': breakdown,
    }
    return render(request, 'games/game_comparison.html', context)


def keyword_category_view(request, category_id):
    """Просмотр игр по категории ключевых слов"""
    category = get_object_or_404(KeywordCategory, id=category_id)

    games = Game.objects.filter(
        keywords__category=category
    ).prefetch_related('genres', 'platforms', 'keywords__category').distinct()

    # Получаем популярные ключевые слова в этой категории
    popular_keywords = Keyword.objects.filter(
        category=category,
        game__isnull=False
    ).annotate(
        game_count=models.Count('game')
    ).order_by('-game_count')[:10]

    context = {
        'category': category,
        'games': games,
        'popular_keywords': popular_keywords,
    }
    return render(request, 'games/keyword_category.html', context)
