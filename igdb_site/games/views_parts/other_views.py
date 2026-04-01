"""Other views (home, search, platforms, etc.)."""

"""Other views (home, search, platforms, etc.)."""

import time
from datetime import timedelta
from typing import Dict, List
from django.shortcuts import render, get_object_or_404, redirect
from django.http import HttpRequest, HttpResponse, JsonResponse
from django.db.models import Count, Prefetch
from django.utils import timezone
from django.contrib.auth import login
from django.contrib.auth.models import User
from django.conf import settings
from django.contrib import messages
from django.urls import reverse
from django.contrib.admin.views.decorators import staff_member_required

from ..models import (
    Game, Genre, Platform, Keyword, KeywordCategory,
    Theme, Company, PlayerPerspective, GameMode
)
from ..models_parts.game_card import GameCardCache
from ..utils.game_card_utils import GameCardCreator
from .base_views import cache_get_or_set, get_cache_key, CACHE_TIMES


@staff_member_required
def remove_theme_from_game(request, game_id, theme_id):
    """
    Удаляет тему из игры и возвращает обратно в админку.

    Args:
        request: HTTP request object
        game_id: ID игры, из которой удаляем тему
        theme_id: ID темы, которую удаляем

    Returns:
        Redirect to admin game list
    """
    game = get_object_or_404(Game, id=game_id)
    theme = get_object_or_404(Theme, id=theme_id)

    game.themes.remove(theme)

    messages.success(
        request,
        f'Тема "{theme.name}" успешно удалена из игры "{game.name}"'
    )

    return redirect(reverse('admin:games_game_changelist'))

def home(request: HttpRequest) -> HttpResponse:
    """Optimized home page with cached game cards."""
    cache_key = 'optimized_home_with_cards_v1'
    cached_context = cache_get_or_set(cache_key, lambda: _get_home_context(), 300)

    response = render(request, 'games/home.html', cached_context)
    response['X-Cache-Hit'] = 'True' if cached_context.get('cached', False) else 'False'

    if 'query_count' in cached_context:
        response['X-DB-Queries'] = str(cached_context['query_count'])
    if 'execution_time' in cached_context:
        response['X-Response-Time'] = f"{cached_context['execution_time']:.3f}s"

    return response


def _get_home_context() -> Dict:
    """Get context for home page with cached game cards."""
    start_time = time.time()

    try:
        from django.db import connection

        # Получаем ID популярных игр (СОРТИРОВКА ВАЖНА!)
        popular_games_ids = list(Game.objects.filter(
            rating_count__gt=10,
            rating__gte=3.0
        ).only('id').order_by('-rating_count', '-rating')[:20].values_list('id', flat=True))

        # Получаем ID недавних игр
        two_years_ago = timezone.now() - timedelta(days=730)
        recent_games_ids = list(Game.objects.filter(
            first_release_date__gte=two_years_ago,
            first_release_date__lte=timezone.now()
        ).only('id').order_by('-first_release_date')[:20].values_list('id', flat=True))

        # Загружаем полные объекты игр для популярных игр
        popular_games = []
        if popular_games_ids:
            popular_games = list(Game.objects.filter(
                id__in=popular_games_ids
            ).prefetch_related(
                'genres', 'platforms', 'player_perspectives'
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            ))

            # Сортируем в том же порядке, что и popular_games_ids
            game_dict = {game.id: game for game in popular_games}
            popular_games = [game_dict[game_id] for game_id in popular_games_ids if game_id in game_dict]

        # Загружаем полные объекты игр для недавних игр
        recent_games = []
        if recent_games_ids:
            recent_games = list(Game.objects.filter(
                id__in=recent_games_ids
            ).prefetch_related(
                'genres', 'platforms', 'player_perspectives'
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            ))

            # Сортируем в том же порядке, что и recent_games_ids
            game_dict = {game.id: game for game in recent_games}
            recent_games = [game_dict[game_id] for game_id in recent_games_ids if game_id in game_dict]

        # Объединяем все ID игр для массового создания/обновления карточек
        all_game_ids = list(set(popular_games_ids + recent_games_ids))

        # Массово создаем/обновляем карточки для всех игр на главной
        if all_game_ids:
            from ..utils.game_card_utils import GameCardCreator

            GameCardCreator.create_cards_for_games(
                game_ids=all_game_ids,
                show_similarity=False,
                batch_size=100,
                force=False
            )

        # Загружаем готовые карточки из кэша с сохранением порядка
        popular_cards = []
        if popular_games:
            from ..models_parts.game_card import GameCardCache

            # Пакетно загружаем существующие карточки из БД
            cards = {}
            try:
                card_objects = GameCardCache.objects.filter(
                    game_id__in=popular_games_ids,
                    is_active=True
                )

                # Фильтруем только те карточки, у которых ключ соответствует текущей версии
                for card in card_objects:
                    # ИСПРАВЛЕНО: generate_cache_key_for_game принимает только game_id
                    expected_key = GameCardCache.generate_cache_key_for_game(card.game_id)
                    if card.cache_key == expected_key:
                        cards[card.game_id] = card
                    else:
                        # Карточка с устаревшей версией - деактивируем
                        card.is_active = False
                        card.save(update_fields=['is_active'])

            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error batch loading card caches: {str(e)}")

            # Формируем список карточек в правильном порядке
            for game in popular_games:
                if game.id in cards:
                    # Используем готовую карточку из кэша
                    popular_cards.append(cards[game.id])
                else:
                    # Если карточки нет в кэше, но игра есть - рендерим сейчас
                    from django.template.loader import render_to_string
                    card_context = {
                        'game': game,
                        'show_similarity': False,
                        'source_game': None
                    }
                    rendered_card = render_to_string('games/partials/_game_card.html', card_context)

                    # Извлекаем связанные данные
                    related_data = GameCardCreator._extract_related_data(game)

                    # Создаем и сохраняем карточку немедленно
                    try:
                        card_cache, created = GameCardCache.get_or_create_card(
                            game=game,
                            rendered_card=rendered_card,
                            show_similarity=False,
                            similarity_percent=None,
                            card_size='normal',
                            **related_data
                        )
                        popular_cards.append(card_cache)
                    except Exception as e:
                        # Если не удалось сохранить, используем простой объект с rendered_card
                        class SimpleCard:
                            def __init__(self, rendered):
                                self.rendered_card = rendered

                        popular_cards.append(SimpleCard(rendered_card))

        # Загружаем готовые карточки для недавних игр с сохранением порядка
        recent_cards = []
        if recent_games:
            from ..models_parts.game_card import GameCardCache

            # Пакетно загружаем существующие карточки из БД
            cards = {}
            try:
                card_objects = GameCardCache.objects.filter(
                    game_id__in=recent_games_ids,
                    is_active=True
                )

                # Фильтруем только те карточки, у которых ключ соответствует текущей версии
                for card in card_objects:
                    # ИСПРАВЛЕНО: generate_cache_key_for_game принимает только game_id
                    expected_key = GameCardCache.generate_cache_key_for_game(card.game_id)
                    if card.cache_key == expected_key:
                        cards[card.game_id] = card
                    else:
                        # Карточка с устаревшей версией - деактивируем
                        card.is_active = False
                        card.save(update_fields=['is_active'])

            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error batch loading card caches: {str(e)}")

            # Формируем список карточек в правильном порядке
            for game in recent_games:
                if game.id in cards:
                    # Используем готовую карточку из кэша
                    recent_cards.append(cards[game.id])
                else:
                    # Если карточки нет в кэше, но игра есть - рендерим сейчас
                    from django.template.loader import render_to_string
                    card_context = {
                        'game': game,
                        'show_similarity': False,
                        'source_game': None
                    }
                    rendered_card = render_to_string('games/partials/_game_card.html', card_context)

                    # Извлекаем связанные данные
                    related_data = GameCardCreator._extract_related_data(game)

                    # Создаем и сохраняем карточку немедленно
                    try:
                        card_cache, created = GameCardCache.get_or_create_card(
                            game=game,
                            rendered_card=rendered_card,
                            show_similarity=False,
                            similarity_percent=None,
                            card_size='normal',
                            **related_data
                        )
                        recent_cards.append(card_cache)
                    except Exception as e:
                        # Если не удалось сохранить, используем простой объект с rendered_card
                        class SimpleCard:
                            def __init__(self, rendered):
                                self.rendered_card = rendered

                        recent_cards.append(SimpleCard(rendered_card))

        # Популярные ключевые слова
        popular_keywords = list(Keyword.objects.filter(
            cached_usage_count__gt=0
        ).only('id', 'name').order_by('-cached_usage_count')[:20])

        query_count = len(connection.queries)

        context = {
            'popular_cards': popular_cards,
            'recent_cards': recent_cards,
            'popular_keywords': popular_keywords,
            'execution_time': round(time.time() - start_time, 3),
            'query_count': query_count,
            'cached': False,
            'show_similarity': False,
            'source_game': None,
            'selected_genres': [],
            'selected_keywords': [],
            'selected_themes': [],
            'selected_perspectives': [],
            'selected_developers': [],
            'selected_game_modes': [],
        }

        return context

    except Exception as e:
        import logging
        logger = logging.getLogger(__name__)
        logger.error(f"Home page error: {str(e)}", exc_info=True)

        return {
            'popular_cards': [],
            'recent_cards': [],
            'popular_keywords': [],
            'show_similarity': False,
            'source_game': None,
            'selected_genres': [],
            'selected_keywords': [],
            'selected_themes': [],
            'selected_perspectives': [],
            'selected_developers': [],
            'selected_game_modes': [],
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

    # Получаем ID игр по поисковому запросу
    games_queryset = Game.objects.all().only('id')

    if search_query:
        games_queryset = games_queryset.filter(name__icontains=search_query)

    games_queryset = games_queryset.order_by('-rating_count', '-rating')
    game_ids = list(games_queryset[:100].values_list('id', flat=True))

    # Загружаем полные объекты игр
    games = []
    if game_ids:
        games = list(Game.objects.filter(
            id__in=game_ids
        ).prefetch_related(
            'genres', 'platforms', 'player_perspectives'
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        ))

        # Сортируем в том же порядке, что и game_ids
        game_dict = {game.id: game for game in games}
        games = [game_dict[game_id] for game_id in game_ids if game_id in game_dict]

        # Массово создаем карточки для результатов поиска
        if games:
            GameCardCreator.create_cards_for_games(
                game_ids=game_ids,
                show_similarity=False,
                batch_size=100,
                force=False
            )

    return render(request, 'games/game_search.html', {
        'games': games,  # Передаем список игр, а не карточек
        'search_query': search_query,
        'total_results': games_queryset.count(),
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