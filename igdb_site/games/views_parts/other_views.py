"""Other views (home, search, platforms, etc.)."""

import time
from datetime import timedelta
from typing import Dict, List
from django.shortcuts import render, get_object_or_404
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.db.models import Count, Prefetch
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.utils import timezone
from django.contrib.auth import login
from django.contrib.auth.models import User
from django.conf import settings

from ..models import (
    Game, Genre, Platform, Keyword, KeywordCategory,
    Theme, Company, PlayerPerspective, GameMode
)
from .base_views import cache_get_or_set, get_cache_key, CACHE_TIMES


def home(request: HttpRequest) -> HttpResponse:
    """Optimized home page with minimal queries."""
    cache_key = 'optimized_home_v10_final'
    cached_context = cache_get_or_set(cache_key, lambda: _get_home_context(), 300)

    response = render(request, 'games/home.html', cached_context)
    response['X-Cache-Hit'] = 'True' if 'cached' in cached_context else 'False'

    if 'query_count' in cached_context:
        response['X-DB-Queries'] = str(cached_context['query_count'])
    if 'execution_time' in cached_context:
        response['X-Response-Time'] = f"{cached_context['execution_time']:.3f}s"

    return response


def _get_home_context() -> Dict:
    """Get context for home page."""
    start_time = time.time()

    try:
        from django.db import connection

        popular_games = Game.objects.filter(
            rating_count__gt=10,
            rating__gte=3.0
        ).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url'
        ).order_by('-rating_count', '-rating')[:12]

        two_years_ago = timezone.now() - timedelta(days=730)
        recent_games = Game.objects.filter(
            first_release_date__gte=two_years_ago,
            first_release_date__lte=timezone.now()
        ).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url'
        ).order_by('-first_release_date')[:12]

        popular_keywords = Keyword.objects.filter(
            cached_usage_count__gt=0
        ).only('id', 'name').order_by('-cached_usage_count')[:20]

        query_count = len(connection.queries)

        context = {
            'popular_games': list(popular_games),
            'recent_games': list(recent_games),
            'popular_keywords': list(popular_keywords),
            'execution_time': round(time.time() - start_time, 3),
            'query_count': query_count,
            'cached': False,
        }

        return context

    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Home page error: {str(e)}")

        return {
            'popular_games': [],
            'recent_games': [],
            'popular_keywords': [],
            'cached': False,
        }


def keyword_category_view(request: HttpRequest, category_id: int) -> HttpResponse:
    """View games by keyword category."""
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

    popular_keywords = Keyword.objects.filter(
        category=category,
        game__isnull=False
    ).annotate(game_count=Count('game')).only(
        'id', 'name', 'category__id'
    ).order_by('-game_count')

    return render(request, 'games/keyword_category.html', {
        'category': category,
        'games': list(games),
        'popular_keywords': list(popular_keywords),
    })


def game_search(request: HttpRequest) -> HttpResponse:
    """Simple game search by name."""
    search_query = request.GET.get('q', '')

    games = Game.objects.all().prefetch_related(
        Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
        Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
        Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
        Prefetch('developers', queryset=Company.objects.only('id', 'name')),
        Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
        Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
    ).only(
        'id', 'name', 'rating', 'rating_count',
        'first_release_date', 'cover_url', 'game_type'
    )

    if search_query:
        games = games.filter(name__icontains=search_query)

    games = games.order_by('-rating_count', '-rating')

    return render(request, 'games/game_search.html', {
        'games': list(games),
        'search_query': search_query,
        'total_results': games.count(),
        # Добавляем параметры для совместимости с карточками
        'show_similarity': False,
        'source_game': None,
        'selected_genres': [],
        'selected_keywords': [],
        'selected_themes': [],
        'selected_perspectives': [],
        'selected_developers': [],
        'selected_game_modes': [],
        'current_page': 1,
    })


def platform_list(request: HttpRequest) -> HttpResponse:
    """Platform list page."""
    platforms = Platform.objects.annotate(
        game_count=Count('game')
    ).filter(game_count__gt=0).only(
        'id', 'name', 'slug'
    ).order_by('-game_count', 'name')

    return render(request, 'games/platform_list.html', {
        'platforms': list(platforms),
    })


def platform_games(request: HttpRequest, platform_id: int) -> HttpResponse:
    """Games for specific platform."""
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

    ITEMS_PER_PAGE = {'platform': 20}

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


def auto_login_admin(request: HttpRequest) -> JsonResponse:
    """Автоматическая авторизация в админке."""
    print(f"DEBUG: Auto login endpoint called, DEBUG={settings.DEBUG}")

    if not settings.DEBUG:
        print("DEBUG: Auto login blocked - not in DEBUG mode")
        return JsonResponse({
            'status': 'error',
            'message': 'Auto login only available in DEBUG mode'
        }, status=403)

    if request.GET.get('test'):
        return JsonResponse({
            'status': 'test',
            'message': 'Test endpoint working'
        })

    try:
        username = 'admin'
        password = 'admin'

        user, created = User.objects.get_or_create(
            username=username,
            defaults={
                'email': 'admin@example.com',
                'is_staff': True,
                'is_superuser': True,
                'is_active': True
            }
        )

        if created:
            user.set_password(password)
            user.save()
            print(f"DEBUG: Created admin user")
        elif not user.check_password(password):
            user.set_password(password)
            user.save()
            print(f"DEBUG: Reset admin password")

        user.backend = 'django.contrib.auth.backends.ModelBackend'

        login(request, user)
        print(f"DEBUG: Successfully logged in as {username}")

        return JsonResponse({
            'status': 'success',
            'message': f'Successfully logged in as {username}',
            'admin_url': '/admin/',
            'username': username,
            'created': created
        })

    except Exception as e:
        print(f"DEBUG: Error in auto_login_admin: {str(e)}")
        import traceback
        traceback.print_exc()

        return JsonResponse({
            'status': 'error',
            'message': f'Error: {str(e)}'
        }, status=500)