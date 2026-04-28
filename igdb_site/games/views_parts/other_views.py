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
    """Get context for home page with cached game cards and extended SEO data."""
    start_time = time.time()

    try:
        from django.db import connection

        popular_games_ids = list(Game.objects.filter(
            rating_count__gt=10,
            rating__gte=3.0
        ).only('id').order_by('-rating_count', '-rating')[:20].values_list('id', flat=True))

        two_years_ago = timezone.now() - timedelta(days=730)
        recent_release_ids = list(Game.objects.filter(
            first_release_date__gte=two_years_ago,
            first_release_date__lte=timezone.now()
        ).only('id').order_by('-first_release_date')[:20].values_list('id', flat=True))

        recently_added_ids = list(Game.objects.filter(
            rating_count__gt=0
        ).only('id').order_by('-id')[:20].values_list('id', flat=True))

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

            game_dict = {game.id: game for game in popular_games}
            popular_games = [game_dict[game_id] for game_id in popular_games_ids if game_id in game_dict]

        recent_release_games = []
        if recent_release_ids:
            recent_release_games = list(Game.objects.filter(
                id__in=recent_release_ids
            ).prefetch_related(
                'genres', 'platforms', 'player_perspectives'
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            ))

            game_dict = {game.id: game for game in recent_release_games}
            recent_release_games = [game_dict[game_id] for game_id in recent_release_ids if game_id in game_dict]

        recently_added_games = []
        if recently_added_ids:
            recently_added_games = list(Game.objects.filter(
                id__in=recently_added_ids
            ).prefetch_related(
                'genres', 'platforms', 'player_perspectives'
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            ))

            game_dict = {game.id: game for game in recently_added_games}
            recently_added_games = [game_dict[game_id] for game_id in recently_added_ids if game_id in game_dict]

        all_game_ids = list(set(popular_games_ids + recent_release_ids + recently_added_ids))

        if all_game_ids:
            from ..utils.game_card_utils import GameCardCreator

            GameCardCreator.create_cards_for_games(
                game_ids=all_game_ids,
                show_similarity=False,
                batch_size=100,
                force=False
            )

        popular_cards = []
        if popular_games:
            from ..models_parts.game_card import GameCardCache

            cards = {}
            try:
                card_objects = GameCardCache.objects.filter(
                    game_id__in=popular_games_ids,
                    is_active=True
                )

                for card in card_objects:
                    expected_key = GameCardCache.generate_cache_key_for_game(card.game_id)
                    if card.cache_key == expected_key:
                        cards[card.game_id] = card
                    else:
                        card.is_active = False
                        card.save(update_fields=['is_active'])

            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error batch loading card caches: {str(e)}")

            for idx, game in enumerate(popular_games):
                if game.id in cards:
                    popular_cards.append(cards[game.id])
                else:
                    from django.template.loader import render_to_string
                    card_context = {
                        'game': game,
                        'show_similarity': False,
                        'source_game': None,
                        'current_page': 1,
                        'game_index': idx,
                        'game_index_offset': idx,
                        'forloop': {'counter0': idx, 'counter': idx + 1},
                    }
                    rendered_card = render_to_string('games/partials/_game_card.html', card_context)

                    related_data = GameCardCreator._extract_related_data(game)

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
                        class SimpleCard:
                            def __init__(self, rendered):
                                self.rendered_card = rendered

                        popular_cards.append(SimpleCard(rendered_card))

        recent_release_cards = []
        if recent_release_games:
            from ..models_parts.game_card import GameCardCache

            cards = {}
            try:
                card_objects = GameCardCache.objects.filter(
                    game_id__in=recent_release_ids,
                    is_active=True
                )

                for card in card_objects:
                    expected_key = GameCardCache.generate_cache_key_for_game(card.game_id)
                    if card.cache_key == expected_key:
                        cards[card.game_id] = card
                    else:
                        card.is_active = False
                        card.save(update_fields=['is_active'])

            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error batch loading card caches: {str(e)}")

            for idx, game in enumerate(recent_release_games):
                if game.id in cards:
                    recent_release_cards.append(cards[game.id])
                else:
                    from django.template.loader import render_to_string
                    card_context = {
                        'game': game,
                        'show_similarity': False,
                        'source_game': None,
                        'current_page': 1,
                        'game_index': idx,
                        'game_index_offset': idx,
                        'forloop': {'counter0': idx, 'counter': idx + 1},
                    }
                    rendered_card = render_to_string('games/partials/_game_card.html', card_context)

                    related_data = GameCardCreator._extract_related_data(game)

                    try:
                        card_cache, created = GameCardCache.get_or_create_card(
                            game=game,
                            rendered_card=rendered_card,
                            show_similarity=False,
                            similarity_percent=None,
                            card_size='normal',
                            **related_data
                        )
                        recent_release_cards.append(card_cache)
                    except Exception as e:
                        class SimpleCard:
                            def __init__(self, rendered):
                                self.rendered_card = rendered

                        recent_release_cards.append(SimpleCard(rendered_card))

        recently_added_cards = []
        if recently_added_games:
            from ..models_parts.game_card import GameCardCache

            cards = {}
            try:
                card_objects = GameCardCache.objects.filter(
                    game_id__in=recently_added_ids,
                    is_active=True
                )

                for card in card_objects:
                    expected_key = GameCardCache.generate_cache_key_for_game(card.game_id)
                    if card.cache_key == expected_key:
                        cards[card.game_id] = card
                    else:
                        card.is_active = False
                        card.save(update_fields=['is_active'])

            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error batch loading card caches: {str(e)}")

            for idx, game in enumerate(recently_added_games):
                if game.id in cards:
                    recently_added_cards.append(cards[game.id])
                else:
                    from django.template.loader import render_to_string
                    card_context = {
                        'game': game,
                        'show_similarity': False,
                        'source_game': None,
                        'current_page': 1,
                        'game_index': idx,
                        'game_index_offset': idx,
                        'forloop': {'counter0': idx, 'counter': idx + 1},
                    }
                    rendered_card = render_to_string('games/partials/_game_card.html', card_context)

                    related_data = GameCardCreator._extract_related_data(game)

                    try:
                        card_cache, created = GameCardCache.get_or_create_card(
                            game=game,
                            rendered_card=rendered_card,
                            show_similarity=False,
                            similarity_percent=None,
                            card_size='normal',
                            **related_data
                        )
                        recently_added_cards.append(card_cache)
                    except Exception as e:
                        class SimpleCard:
                            def __init__(self, rendered):
                                self.rendered_card = rendered

                        recently_added_cards.append(SimpleCard(rendered_card))

        popular_keywords = list(Keyword.objects.filter(
            cached_usage_count__gt=0
        ).only('id', 'name').order_by('-cached_usage_count')[:20])

        popular_genres = list(Genre.objects.annotate(
            total_game_count=Count('game')
        ).filter(
            total_game_count__gt=0
        ).order_by('-total_game_count', 'name').only('id', 'name')[:20])

        popular_platforms = list(Platform.objects.annotate(
            game_count=Count('game', distinct=True)
        ).filter(
            game_count__gt=0
        ).order_by('-game_count', 'name').only('id', 'name', 'slug')[:12])

        query_count = len(connection.queries)

        context = {
            'popular_cards': popular_cards,
            'recent_release_cards': recent_release_cards,
            'recently_added_cards': recently_added_cards,
            'popular_keywords': popular_keywords,
            'popular_genres': popular_genres,
            'popular_platforms': popular_platforms,
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