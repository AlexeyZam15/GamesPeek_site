from django.shortcuts import render, get_object_or_404
from django.db.models import Q
from .models import Game, Genre, Platform, Keyword, KeywordCategory
from django.db import models


def game_list(request):
    """Список всех игр с фильтрацией и поиском"""
    games = Game.objects.all().prefetch_related('genres', 'platforms', 'keywords__category')

    # Поиск по названию
    search_query = request.GET.get('search')
    if search_query:
        games = games.filter(name__icontains=search_query)

    # Фильтрация по жанру
    genre_filter = request.GET.get('genre')
    if genre_filter:
        games = games.filter(genres__id=genre_filter)

    # Фильтрация по платформе
    platform_filter = request.GET.get('platform')
    if platform_filter:
        games = games.filter(platforms__id=platform_filter)

    # Фильтрация по категории ключевых слов
    keyword_category_filter = request.GET.get('keyword_category')
    if keyword_category_filter:
        games = games.filter(keywords__category__id=keyword_category_filter)

    # Сортировка
    sort = request.GET.get('sort', '-rating_count')
    if sort in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count']:
        games = games.order_by(sort)

    context = {
        'games': games,
        'genres': Genre.objects.all(),
        'platforms': Platform.objects.all(),
        'keyword_categories': KeywordCategory.objects.all(),  # Добавили категории для фильтрации
        'search_query': search_query or '',
        'current_sort': sort,
    }
    return render(request, 'games/game_list.html', context)


from .similarity import GameSimilarity


def game_detail(request, pk):
    """Детальная страница игры с похожими играми"""
    game = get_object_or_404(
        Game.objects.prefetch_related('keywords__category', 'genres', 'platforms'),
        pk=pk
    )

    # Используем алгоритм похожести
    similarity_engine = GameSimilarity()
    similar_games_data = similarity_engine.find_similar_games(game, limit=6, min_similarity=15)

    # УБИРАЕМ старую логику группировки ключевых слов - теперь используем property из модели
    # В шаблоне будут использоваться game.genre_keywords, game.gameplay_keywords и т.д.

    context = {
        'game': game,
        'similar_games': similar_games_data,
        # Убрали 'keywords_by_category' - теперь используем property модели
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

    # УБИРАЕМ старую логику common_keywords_by_category - теперь breakdown содержит всю информацию

    context = {
        'game1': game1,
        'game2': game2,
        'similarity_score': similarity_score,
        'breakdown': breakdown,
        # Убрали 'common_keywords_by_category' - теперь breakdown содержит всю информацию
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