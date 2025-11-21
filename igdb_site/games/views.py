from .similarity import GameSimilarity
from django.db import models
from django.shortcuts import render, get_object_or_404
from .models import Game, Genre, Keyword, KeywordCategory, Platform, GameSimilarityCache


def game_list(request):
    """Список всех игр с фильтрацией, поиском и поиском похожих игр"""
    games = Game.objects.all().prefetch_related('genres', 'platforms', 'keywords')

    similar_game = None
    show_similarity = False

    # Получаем множественные параметры фильтров - ИСПРАВЛЕНО ИМЯ
    selected_genres = request.GET.getlist('genre')
    selected_keywords = request.GET.getlist('keyword')  # ИСПРАВЛЕНО: 'keyword' вместо 'keyword_category'

    # ПО УМОЛЧАНИЮ: если ищем похожие игры, сортируем по похожести
    default_sort = '-similarity' if 'similar_to' in request.GET else '-rating_count'
    current_sort = request.GET.get('sort', default_sort)

    # Получаем популярные ключевые слова для фильтров
    popular_keywords = Keyword.objects.filter(usage_count__gt=0).order_by('-usage_count')[:20]

    # Поиск похожих игр
    similar_to_id = request.GET.get('similar_to')
    if similar_to_id:
        similar_game = get_object_or_404(Game, id=similar_to_id)
        show_similarity = True

        # Автоматические критерии от похожей игры - ИСПРАВЛЕНО
        auto_genres = [str(genre.id) for genre in similar_game.genres.all()]
        auto_keywords = [str(keyword.id) for keyword in similar_game.keywords.all()]  # ИСПРАВЛЕНО: keyword.id

        # Объединяем автоматические и пользовательские фильтры для поиска
        all_genres = list(set(selected_genres + auto_genres))
        all_keywords = list(set(selected_keywords + auto_keywords))  # ИСПРАВЛЕНО

        # БЫСТРЫЙ ПОИСК ИЗ КЭША
        similar_games_cache = GameSimilarityCache.objects.filter(
            game1=similar_game
        ).select_related('game2').order_by('-similarity_score')[:100]

        # Получаем ID игр для предзагрузки связанных данных
        game_ids = [cache_item.game2_id for cache_item in similar_games_cache]

        # Предзагружаем связанные данные для всех игр одним запросом
        games_with_relations = Game.objects.filter(
            id__in=game_ids
        ).prefetch_related('genres', 'platforms', 'keywords')  # Добавлено keywords

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

        # Применяем дополнительные фильтры если есть - ИСПРАВЛЕНО
        if all_genres:
            games_with_similarity = [item for item in games_with_similarity
                                     if any(str(genre.id) in all_genres
                                            for genre in item['game'].genres.all())]

        if all_keywords:
            games_with_similarity = [item for item in games_with_similarity
                                     if any(str(keyword.id) in all_keywords  # ИСПРАВЛЕНО: keyword.id
                                            for keyword in item['game'].keywords.all())]

        # СОРТИРОВКА для похожих игр
        if current_sort == '-similarity':
            # Уже отсортировано по similarity_score из кэша
            pass
        elif current_sort == '-rating_count':
            games_with_similarity.sort(key=lambda x: x['game'].rating_count or 0, reverse=True)
        elif current_sort == '-rating':
            games_with_similarity.sort(key=lambda x: x['game'].rating or 0, reverse=True)
        elif current_sort == 'name':
            games_with_similarity.sort(key=lambda x: x['game'].name.lower())
        elif current_sort == '-name':
            games_with_similarity.sort(key=lambda x: x['game'].name.lower(), reverse=True)
        elif current_sort == '-first_release_date':
            games_with_similarity.sort(key=lambda x: x['game'].first_release_date or '', reverse=True)

        context = {
            'games_with_similarity': games_with_similarity,
            'genres': Genre.objects.all(),
            'platforms': Platform.objects.all(),
            'keyword_categories': KeywordCategory.objects.all(),
            'popular_keywords': popular_keywords,
            'current_sort': current_sort,
            'similar_game': similar_game,
            'show_similarity': True,
            'selected_genres': [int(g) for g in selected_genres],
            'selected_keywords': [int(k) for k in selected_keywords],  # ИСПРАВЛЕНО
            'auto_genres': [int(g) for g in auto_genres],
            'auto_keywords': [int(k) for k in auto_keywords],  # ИСПРАВЛЕНО
        }

    # Обычный поиск (без похожих игр)
    else:
        # Применяем множественные фильтры жанров
        if selected_genres:
            for genre_id in selected_genres:
                games = games.filter(genres__id=genre_id)

        # Применяем множественные фильтры ключевых слов - ИСПРАВЛЕНО
        if selected_keywords:
            for keyword_id in selected_keywords:
                games = games.filter(keywords__id=keyword_id)  # ИСПРАВЛЕНО: keywords__id

        # Сортировка
        if current_sort in ['name', '-name', 'rating', '-rating', 'rating_count', '-rating_count',
                            '-first_release_date']:
            games = games.order_by(current_sort)

        context = {
            'games': games,
            'genres': Genre.objects.all(),
            'platforms': Platform.objects.all(),
            'keyword_categories': KeywordCategory.objects.all(),
            'popular_keywords': popular_keywords,
            'current_sort': current_sort,
            'similar_game': None,
            'show_similarity': False,
            'selected_genres': [int(g) for g in selected_genres],
            'selected_keywords': [int(k) for k in selected_keywords],  # ИСПРАВЛЕНО
            'auto_genres': [],
            'auto_keywords': [],
        }

    return render(request, 'games/game_list.html', context)


def game_detail(request, pk):
    """Детальная страница игры с похожими играми"""
    game = get_object_or_404(
        Game.objects.prefetch_related('keywords', 'genres', 'platforms'),  # Убрано __category
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
        Game.objects.prefetch_related('keywords', 'genres', 'platforms'),  # Убрано __category
        pk=pk1
    )
    game2 = get_object_or_404(
        Game.objects.prefetch_related('keywords', 'genres', 'platforms'),  # Убрано __category
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
    ).prefetch_related('genres', 'platforms', 'keywords').distinct()  # Убрано __category

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