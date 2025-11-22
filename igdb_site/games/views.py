from .similarity import GameSimilarity
from django.db import models
from django.shortcuts import render, get_object_or_404
from .models import Game, Genre, Keyword, KeywordCategory, Platform, GameSimilarityCache
import json
import base64


def game_list(request):
    """Список всех игр с поиском похожих игр по выбранным критериям"""
    games = Game.objects.all().prefetch_related('genres', 'platforms', 'keywords')

    # Получаем параметры в новом компактном формате
    find_similar = request.GET.get('find_similar') == '1'

    # Новый компактный формат параметров
    genres_param = request.GET.get('g', '')
    keywords_param = request.GET.get('k', '')

    # Конвертируем строки в списки integers
    selected_genres_int = [int(g) for g in genres_param.split(',') if g.strip()] if genres_param else []
    selected_keywords_int = [int(k) for k in keywords_param.split(',') if k.strip()] if keywords_param else []

    # Старый формат (для обратной совместимости)
    if not selected_genres_int:
        selected_genres = request.GET.getlist('genre')
        selected_genres_int = [int(g) for g in selected_genres if g]

    if not selected_keywords_int:
        selected_keywords = request.GET.getlist('keyword')
        selected_keywords_int = [int(k) for k in selected_keywords if k]

    # Автоматически включаем режим похожих игр если переданы критерии
    if not find_similar and (selected_genres_int or selected_keywords_int):
        find_similar = True

    # Сортировка
    default_sort = '-similarity' if find_similar else '-rating_count'
    current_sort = request.GET.get('sort', default_sort)

    popular_keywords = Keyword.objects.filter(usage_count__gt=0).order_by('-usage_count')[:100]

    show_similarity = False
    games_with_similarity = []
    source_game = None

    # РЕЖИМ ПОИСКА ПОХОЖИХ ИГР ПО КРИТЕРИЯМ
    if find_similar and (selected_genres_int or selected_keywords_int):
        show_similarity = True

        # Определяем исходную игру для сравнения
        source_game_id = request.GET.get('source_game')
        if source_game_id:
            try:
                source_game = Game.objects.get(id=source_game_id)
            except Game.DoesNotExist:
                source_game = None

        # Если нет исходной игры, создаем виртуальную на основе критериев
        if not source_game:
            class VirtualGame:
                def __init__(self, genres, keywords):
                    self.id = 0  # Специальный ID
                    self.name = "Your Search Criteria"
                    self.genres = Genre.objects.filter(id__in=genres)
                    self.keywords = Keyword.objects.filter(id__in=keywords)
                    self.cover_url = None

            source_game = VirtualGame(selected_genres_int, selected_keywords_int)

        # Получаем похожие игры (теперь без фильтрации исходной игры)
        similarity_engine = GameSimilarity()
        similar_games_data = similarity_engine.find_similar_games_to_criteria(
            genre_ids=selected_genres_int,
            keyword_ids=selected_keywords_int,
            limit=50,
            min_similarity=15
        )

        # Формируем структуру данных (без фильтрации)
        games_with_similarity = [
            {
                'game': item['game'],
                'similarity': item['similarity']
            }
            for item in similar_games_data
        ]

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
        # Для '-similarity' уже отсортировано

    # ОБЫЧНЫЙ РЕЖИМ ФИЛЬТРАЦИИ
    else:
        if selected_genres_int:
            for genre_id in selected_genres_int:
                games = games.filter(genres__id=genre_id)

        if selected_keywords_int:
            for keyword_id in selected_keywords_int:
                games = games.filter(keywords__id=keyword_id)

        if current_sort in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count',
                            '-first_release_date']:
            games = games.order_by(current_sort)

    # Генерируем компактный URL для шаблона
    compact_url_params = generate_compact_url_params(
        find_similar=find_similar,
        genres=selected_genres_int,
        keywords=selected_keywords_int,
        sort=current_sort
    )

    context = {
        'games': games if not show_similarity else [],
        'games_with_similarity': games_with_similarity if show_similarity else [],
        'genres': Genre.objects.all(),
        'platforms': Platform.objects.all(),
        'keyword_categories': KeywordCategory.objects.all(),
        'popular_keywords': popular_keywords,
        'current_sort': current_sort,
        'show_similarity': show_similarity,
        'selected_genres': selected_genres_int,
        'selected_keywords': selected_keywords_int,
        'find_similar': find_similar,
        'compact_url_params': compact_url_params,
        'source_game': source_game,
    }

    return render(request, 'games/game_list.html', context)


def generate_compact_url_params(find_similar=False, genres=None, keywords=None, sort=None):
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

    if sort:
        params['sort'] = sort

    return params


def get_compact_game_list_url(find_similar=False, genres=None, keywords=None, sort=None):
    """
    Вспомогательная функция для генерации полного URL с компактными параметрами
    """
    params = generate_compact_url_params(
        find_similar=find_similar,
        genres=genres,
        keywords=keywords,
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

    # Используем ОБНОВЛЕННЫЙ алгоритм похожести
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


def game_comparison(request, pk1, pk2):
    """Детальное сравнение двух игр"""
    game1 = get_object_or_404(
        Game.objects.prefetch_related('keywords', 'genres', 'platforms'),
        pk=pk1
    )
    game2 = get_object_or_404(
        Game.objects.prefetch_related('keywords', 'genres', 'platforms'),
        pk=pk2
    )

    # Пробуем взять процент из URL, если передан
    url_similarity = request.GET.get('similarity')
    if url_similarity:
        try:
            similarity_score = float(url_similarity)
        except (ValueError, TypeError):
            similarity_score = 0
    else:
        # Используем обычный расчет как fallback
        similarity_engine = GameSimilarity()
        similarity_score = similarity_engine.calculate_similarity(game1, game2)

    # Рассчитываем общие элементы
    shared_genres = game1.genres.all() & game2.genres.all()
    shared_keywords = game1.keywords.all() & game2.keywords.all()

    shared_genres_count = shared_genres.count()
    shared_keywords_count = shared_keywords.count()

    # Группируем ключевые слова по категориям
    keyword_categories = KeywordCategory.objects.all()
    shared_keywords_by_category = {}

    for category in keyword_categories:
        category_keywords = shared_keywords.filter(category=category)
        if category_keywords.exists():
            shared_keywords_by_category[category.name] = category_keywords

    context = {
        'game1': game1,
        'game2': game2,
        'similarity_score': similarity_score,
        'shared_genres': shared_genres,
        'shared_keywords': shared_keywords,
        'shared_genres_count': shared_genres_count,
        'shared_keywords_count': shared_keywords_count,
        'shared_keywords_by_category': shared_keywords_by_category,
    }
    return render(request, 'games/game_comparison.html', context)


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
