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
from django.core.mail import send_mail
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
import json
import logging

from ..models import (
    Game, Genre, Platform, Keyword, KeywordCategory,
    Theme, Company, PlayerPerspective, GameMode
)
from ..models_parts.game_card import GameCardCache
from ..utils.game_card_utils import GameCardCreator
from .base_views import cache_get_or_set, get_cache_key, CACHE_TIMES

logger = logging.getLogger(__name__)


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
    context = _get_home_context()

    response = render(request, 'games/home.html', context)

    if 'query_count' in context:
        response['X-DB-Queries'] = str(context['query_count'])
    if 'execution_time' in context:
        response['X-Response-Time'] = f"{context['execution_time']:.3f}s"

    return response


def _get_home_context() -> Dict:
    """
    Get context for home page with cached game cards and extended SEO data.
    ДЕБАГ ВЕРСИЯ: замер времени на каждом этапе.
    """

    import time
    from django.db import connection
    from django.db.models import Count
    from django.utils import timezone
    from datetime import timedelta
    from ..models_parts.game_card import GameCardCache
    from ..utils.game_card_utils import GameCardCreator

    timings = {}
    total_start = time.time()

    print("\n" + "=" * 60)
    print("ДЕБАГ _get_home_context() - ЗАМЕР ВРЕМЕНИ")
    print("=" * 60)

    try:
        two_years_ago = timezone.now() - timedelta(days=730)

        start = time.time()
        popular_ids = list(Game.objects.filter(
            rating_count__gt=10,
            rating__gte=3.0
        ).order_by('-rating_count', '-rating')[:20].values_list('id', flat=True))
        timings['1_popular_ids'] = time.time() - start
        print(f"1. Получение ID популярных игр: {timings['1_popular_ids'] * 1000:.2f} мс, найдено: {len(popular_ids)}")

        start = time.time()
        recent_ids = list(Game.objects.filter(
            first_release_date__gte=two_years_ago,
            first_release_date__lte=timezone.now()
        ).order_by('-first_release_date')[:20].values_list('id', flat=True))
        timings['2_recent_ids'] = time.time() - start
        print(f"2. Получение ID новых релизов: {timings['2_recent_ids'] * 1000:.2f} мс, найдено: {len(recent_ids)}")

        start = time.time()
        added_ids = list(Game.objects.order_by('-date_added')[:20].values_list('id', flat=True))
        timings['3_added_ids'] = time.time() - start
        print(f"3. Получение ID недавно добавленных: {timings['3_added_ids'] * 1000:.2f} мс, найдено: {len(added_ids)}")

        start = time.time()
        all_ids = list(set(popular_ids + recent_ids + added_ids))
        timings['4_merge_ids'] = time.time() - start
        print(f"4. Объединение ID: {timings['4_merge_ids'] * 1000:.2f} мс, всего уникальных: {len(all_ids)}")

        start = time.time()
        games_with_prefetch = list(Game.objects.filter(
            id__in=all_ids
        ).prefetch_related(
            'genres', 'platforms', 'player_perspectives'
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        ))
        timings['5_games_prefetch'] = time.time() - start
        print(
            f"5. Получение игр с prefetch: {timings['5_games_prefetch'] * 1000:.2f} мс, получено: {len(games_with_prefetch)}")

        games_dict = {game.id: game for game in games_with_prefetch}

        start = time.time()
        cards = GameCardCache.objects.filter(
            game_id__in=all_ids,
            is_active=True
        )
        cards_dict = {card.game_id: card for card in cards}
        timings['6_card_cache'] = time.time() - start
        print(f"6. Получение карточек из кэша: {timings['6_card_cache'] * 1000:.2f} мс, в кэше: {len(cards_dict)}")

        missing_ids = [gid for gid in all_ids if gid not in cards_dict]
        timings['7_missing_count'] = len(missing_ids)
        print(f"7. Отсутствующих карточек: {len(missing_ids)}")

        if missing_ids:
            start = time.time()
            GameCardCreator.create_cards_for_games(
                game_ids=missing_ids,
                show_similarity=False,
                batch_size=100,
                force=False
            )
            timings['7_create_cards'] = time.time() - start
            print(f"   Время создания карточек: {timings['7_create_cards'] * 1000:.2f} мс")

            start = time.time()
            new_cards = GameCardCache.objects.filter(
                game_id__in=missing_ids,
                is_active=True
            )
            for card in new_cards:
                cards_dict[card.game_id] = card
            timings['7_reload_cards'] = time.time() - start
            print(f"   Перезагрузка карточек: {timings['7_reload_cards'] * 1000:.2f} мс")
        else:
            timings['7_create_cards'] = 0
            timings['7_reload_cards'] = 0

        start = time.time()
        popular_cards = []
        for gid in popular_ids:
            if gid in cards_dict:
                popular_cards.append(cards_dict[gid])
            elif gid in games_dict:
                class SimpleCard:
                    def __init__(self, game):
                        self.rendered_card = f'<div>Game ID: {game.id}</div>'

                popular_cards.append(SimpleCard(games_dict[gid]))

        recent_release_cards = []
        for gid in recent_ids:
            if gid in cards_dict:
                recent_release_cards.append(cards_dict[gid])
            elif gid in games_dict:
                class SimpleCard:
                    def __init__(self, game):
                        self.rendered_card = f'<div>Game ID: {game.id}</div>'

                recent_release_cards.append(SimpleCard(games_dict[gid]))

        recently_added_cards = []
        for gid in added_ids:
            if gid in cards_dict:
                recently_added_cards.append(cards_dict[gid])
            elif gid in games_dict:
                class SimpleCard:
                    def __init__(self, game):
                        self.rendered_card = f'<div>Game ID: {game.id}</div>'

                recently_added_cards.append(SimpleCard(games_dict[gid]))

        timings['8_format_cards'] = time.time() - start
        print(f"8. Формирование списков карточек: {timings['8_format_cards'] * 1000:.2f} мс")

        start = time.time()
        popular_keywords = list(Keyword.objects.filter(
            cached_usage_count__gt=0
        ).only('id', 'name').order_by('-cached_usage_count')[:20])
        timings['9_keywords'] = time.time() - start
        print(f"9. Получение ключевых слов: {timings['9_keywords'] * 1000:.2f} мс, найдено: {len(popular_keywords)}")

        start = time.time()
        popular_genres = list(Genre.objects.annotate(
            total_games=Count('game')
        ).filter(
            total_games__gt=0
        ).order_by('-total_games', 'name').only('id', 'name')[:20])
        timings['10_genres'] = time.time() - start
        print(f"10. Получение жанров: {timings['10_genres'] * 1000:.2f} мс, найдено: {len(popular_genres)}")

        start = time.time()
        popular_platforms = list(Platform.objects.annotate(
            game_count=Count('game', distinct=True)
        ).filter(
            game_count__gt=0
        ).order_by('-game_count', 'name').only('id', 'name', 'slug')[:12])
        timings['11_platforms'] = time.time() - start
        print(f"11. Получение платформ: {timings['11_platforms'] * 1000:.2f} мс, найдено: {len(popular_platforms)}")

        total_time = time.time() - total_start
        timings['total'] = total_time

        print("\n" + "-" * 40)
        print("СВОДКА ПО ВРЕМЕНИ:")
        print("-" * 40)
        for key, value in timings.items():
            if 'time' in key or key == 'total' or key == '7_create_cards' or key == '7_reload_cards':
                if isinstance(value, float):
                    print(f"   {key}: {value * 1000:.2f} мс")
            elif key == '7_missing_count':
                print(f"   {key}: {value}")

        print(f"\n   ОБЩЕЕ ВРЕМЯ: {total_time * 1000:.2f} мс")

        if timings.get('7_create_cards', 0) > 1.0:
            print("\n⚠️  ПРОБЛЕМА В create_cards_for_games() - это самый медленный этап!")

        print("=" * 60)

        context = {
            'popular_cards': popular_cards,
            'recent_release_cards': recent_release_cards,
            'recently_added_cards': recently_added_cards,
            'popular_keywords': popular_keywords,
            'popular_genres': popular_genres,
            'popular_platforms': popular_platforms,
            'execution_time': round(total_time, 3),
            'query_count': len(connection.queries),
            'cached': len(missing_ids) == 0,
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
        import traceback
        logger = logging.getLogger(__name__)
        logger.error(f"Home page error: {str(e)}", exc_info=True)
        traceback.print_exc()

        return {
            'popular_cards': [],
            'recent_release_cards': [],
            'recently_added_cards': [],
            'popular_keywords': [],
            'popular_genres': [],
            'popular_platforms': [],
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
    """
    Search games by name - redirects to game_list with search parameter.

    This integrates text search with the existing AJAX pagination and filtering system.
    The search query is passed as 'search_k' parameter which is already supported
    by the filtering system.

    Args:
        request: HTTP request object with 'q' parameter for search query

    Returns:
        Redirect to game_list with search parameter
    """
    import logging
    import time
    from django.urls import reverse
    from urllib.parse import urlencode

    logger = logging.getLogger(__name__)
    search_query = request.GET.get('q', '').strip()
    start_time = time.time()

    # Empty query - just go to game list
    if not search_query:
        execution_time = (time.time() - start_time) * 1000
        logger.info(f"SEARCH: empty query - redirect to game_list - {execution_time:.2f}ms")
        return redirect('game_list')

    # Build URL for game_list with search parameter
    base_url = reverse('game_list')

    # Create parameters for redirect
    params = {
        'search_k': search_query,  # Pass search query as keyword filter
        'find_similar': '0',  # Regular mode, not similarity search
        'page': '1',  # Reset to first page
    }

    redirect_url = f"{base_url}?{urlencode(params)}"

    execution_time = (time.time() - start_time) * 1000
    logger.info(f"SEARCH: '{search_query}' - redirect to {redirect_url} - {execution_time:.2f}ms")

    return redirect(redirect_url)


@csrf_exempt
@require_http_methods(["POST"])
def send_feedback(request: HttpRequest) -> JsonResponse:
    """
    Send feedback email to gamespeek@mail.ru with optional user email for reply.
    """
    logger = logging.getLogger(__name__)

    try:
        data = json.loads(request.body)
        message_text = data.get('message', '').strip()
        user_email = data.get('email', '').strip()

        if not message_text:
            return JsonResponse({'status': 'error', 'message': 'Please enter your feedback'}, status=400)

        if len(message_text) < 5:
            return JsonResponse({'status': 'error', 'message': 'Message too short (min 5 chars)'}, status=400)

        # Validate email format if provided
        if user_email:
            from django.core.validators import validate_email
            from django.core.exceptions import ValidationError
            try:
                validate_email(user_email)
            except ValidationError:
                return JsonResponse({'status': 'error', 'message': 'Please enter a valid email address'}, status=400)

        subject = f"GamesPeek Feedback"

        # Build email body with user email if provided
        body = f"Message: {message_text}\n\n"
        if user_email:
            body += f"From Email: {user_email}\n"
        else:
            body += f"From Email: Not provided (anonymous feedback)\n"
        body += f"From IP: {request.META.get('REMOTE_ADDR', 'Unknown')}\n"
        body += f"User Agent: {request.META.get('HTTP_USER_AGENT', 'Unknown')}"

        logger.info(f"Attempting to send feedback email to gamespeek@mail.ru")
        logger.info(f"EMAIL_HOST_USER: {settings.EMAIL_HOST_USER}")
        logger.info(f"EMAIL_HOST_PASSWORD set: {bool(settings.EMAIL_HOST_PASSWORD)}")

        send_mail(
            subject=subject,
            message=body,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=['gamespeek@mail.ru'],
            fail_silently=False,
        )

        logger.info(f"Feedback email sent successfully")
        return JsonResponse({'status': 'success', 'message': 'Feedback sent successfully!'})

    except Exception as e:
        logger.error(f"Feedback error: {str(e)}", exc_info=True)
        return JsonResponse({'status': 'error', 'message': f'Failed to send feedback: {str(e)}'}, status=500)

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