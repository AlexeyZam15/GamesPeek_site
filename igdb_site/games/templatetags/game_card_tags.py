"""Template tags for game card caching."""

from django import template
from django.utils.safestring import mark_safe
from django.core.cache import cache
from games.models import GameCardCache
from games.models_parts.game import Game
from typing import Dict, Any, Optional

register = template.Library()


@register.simple_tag
def get_cached_game_card(
        game: Game,
        context: Dict[str, Any],
        show_similarity: bool = False,
        similarity_percent: float = None,
        card_size: str = 'normal'
) -> str:
    """
    Get cached game card or render new one.

    Args:
        game: Game object
        context: Template context
        show_similarity: Whether to show similarity
        similarity_percent: Similarity percentage if showing
        card_size: Card size configuration

    Returns:
        Rendered HTML card
    """
    # Try to get from cache - только game_id
    cached_card = GameCardCache.get_card_for_game(game.id)

    if cached_card:
        return mark_safe(cached_card.rendered_card)

    # If not in cache, we'll need to render it
    return ""


@register.simple_tag
def render_game_card_with_cache(
        game: Game,
        source_game: Any = None,
        selected_genres: list = None,
        selected_keywords: list = None,
        selected_themes: list = None,
        selected_perspectives: list = None,
        selected_developers: list = None,
        selected_game_modes: list = None,
        show_similarity: bool = False,
        similarity_percent: float = None
) -> str:
    """
    Render game card with caching support.
    """
    from django.template.loader import render_to_string

    # Prepare context
    context = {
        'game': game,
        'source_game': source_game,
        'selected_genres': selected_genres or [],
        'selected_keywords': selected_keywords or [],
        'selected_themes': selected_themes or [],
        'selected_perspectives': selected_perspectives or [],
        'selected_developers': selected_developers or [],
        'selected_game_modes': selected_game_modes or [],
        'show_similarity': show_similarity,
    }

    # Add similarity if provided
    if similarity_percent is not None and show_similarity:
        if hasattr(game, 'similarity'):
            game.similarity = similarity_percent
        else:
            # Create a wrapper with similarity attribute
            class GameWithSimilarity:
                def __init__(self, game_obj, similarity_val):
                    self.__dict__ = game_obj.__dict__.copy()
                    self.similarity = similarity_val

            game = GameWithSimilarity(game, similarity_percent)

    # Try to get from cache first
    cache_key = f"card_{game.id}_{show_similarity}_{similarity_percent}"
    cached_html = cache.get(cache_key)

    if cached_html:
        return mark_safe(cached_html)

    # Render fresh card
    html = render_to_string('games/partials/_game_card.html', context)

    # Store in cache for future use
    cache.set(cache_key, html, 3600)  # 1 hour cache

    return mark_safe(html)


@register.filter
def needs_card_update(game: Game, last_update_check: str = None) -> bool:
    """
    Check if game card needs update based on game changes.

    Args:
        game: Game object
        last_update_check: Last time card was checked/updated

    Returns:
        True if card needs update
    """
    from django.utils import timezone

    if not last_update_check:
        return True

    try:
        # Check if game was updated after last card update
        if game.updated_at and game.updated_at > last_update_check:
            return True

        # Check cached counts
        if game._cache_updated_at and game._cache_updated_at > last_update_check:
            return True

        return False
    except (AttributeError, TypeError):
        return True


@register.simple_tag
def get_card_cache_stats() -> Dict[str, Any]:
    """
    Get card cache statistics.

    Returns:
        Dictionary with cache statistics
    """
    stats = {
        'total_cards': GameCardCache.objects.filter(is_active=True).count(),
        'total_hits': GameCardCache.objects.filter(is_active=True).aggregate(
            total=models.Sum('hit_count')
        )['total'] or 0,
        'recently_accessed': GameCardCache.objects.filter(
            is_active=True,
            last_accessed__gte=timezone.now() - timezone.timedelta(hours=1)
        ).count(),
    }

    return stats