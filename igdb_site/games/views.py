from django.shortcuts import render, get_object_or_404
from django.db.models import Q
from .models import Game, Genre, Platform, Keyword


def game_list(request):
    """Список всех игр с фильтрацией и поиском"""
    games = Game.objects.all().prefetch_related('genres', 'platforms', 'keywords')

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

    # Сортировка
    sort = request.GET.get('sort', '-rating_count')
    if sort in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count']:
        games = games.order_by(sort)

    context = {
        'games': games,
        'genres': Genre.objects.all(),
        'platforms': Platform.objects.all(),
        'search_query': search_query or '',
        'current_sort': sort,
    }
    return render(request, 'games/game_list.html', context)


def game_detail(request, pk):
    """Детальная страница игры"""
    game = get_object_or_404(
        Game.objects.prefetch_related('genres', 'platforms', 'keywords'),
        pk=pk
    )

    # Простые похожие игры (по жанрам)
    similar_games = Game.objects.filter(
        genres__in=game.genres.all()
    ).exclude(
        pk=game.pk
    ).distinct()[:6]

    # Разделяем ключевые слова по категориям
    gameplay_keywords = game.keywords.filter(category__name='Gameplay')
    setting_keywords = game.keywords.filter(category__name='Setting')
    other_keywords = game.keywords.exclude(category__name__in=['Gameplay', 'Setting'])

    context = {
        'game': game,
        'similar_games': similar_games,
        'gameplay_keywords': gameplay_keywords,
        'setting_keywords': setting_keywords,
        'other_keywords': other_keywords,
    }
    return render(request, 'games/game_detail.html', context)


def home(request):
    """Главная страница с популярными играми"""
    popular_games = Game.objects.filter(
        rating_count__gt=10
    ).order_by('-rating_count')[:12]

    recent_games = Game.objects.filter(
        first_release_date__isnull=False
    ).order_by('-first_release_date')[:12]

    context = {
        'popular_games': popular_games,
        'recent_games': recent_games,
    }
    return render(request, 'games/home.html', context)