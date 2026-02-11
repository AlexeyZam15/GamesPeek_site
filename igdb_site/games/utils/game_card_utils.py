"""
Utility functions for game card creation and management.
"""

from django.db import transaction
from django.utils import timezone
from django.template.loader import render_to_string
from django.db.models import Prefetch
from typing import List, Dict, Any, Optional, Tuple
import logging

from games.models import Game, GameCardCache, Genre, Platform, PlayerPerspective, Keyword, Theme, GameMode

logger = logging.getLogger(__name__)


class GameCardCreator:
    """Utility class for creating and managing game cards."""

    @classmethod
    def create_card_for_game(
            cls,
            game: Game,
            show_similarity: bool = False,
            similarity_percent: float = None,
            card_size: str = 'normal',
            force: bool = False
    ) -> Tuple[Optional[GameCardCache], bool]:
        """
        Create cached card for a single game.

        Returns:
            Tuple of (card object, created_new)
        """
        # Check if card already exists
        existing_card = None
        if not force:
            existing_card = GameCardCache.get_card_for_game(
                game.id, show_similarity, similarity_percent, card_size
            )

            if existing_card:
                return existing_card, False

        # Load game with all related data
        game_with_data = cls._load_game_with_data(game.id)
        if not game_with_data:
            return None, False

        # Render card
        rendered_card = cls._render_card_html(
            game_with_data, show_similarity, similarity_percent
        )

        # Extract related data
        related_data = cls._extract_related_data(game_with_data)

        # Create card cache
        try:
            with transaction.atomic():
                card = GameCardCache.create_card(
                    game=game_with_data,
                    rendered_card=rendered_card,
                    show_similarity=show_similarity,
                    similarity_percent=similarity_percent,
                    card_size=card_size,
                    **related_data
                )

            logger.info(f"Created card cache for game {game.id} ({game.name})")
            return card, True

        except Exception as e:
            logger.error(f"Error creating card for game {game.id}: {str(e)}", exc_info=True)
            return None, False

    @classmethod
    def create_cards_for_games(
            cls,
            game_ids: List[int],
            show_similarity: bool = False,
            batch_size: int = 100,
            force: bool = False
    ) -> Dict[str, int]:
        """
        Create cards for multiple games.

        Returns:
            Dictionary with creation statistics
        """
        stats = {
            'total': len(game_ids),
            'created': 0,
            'skipped': 0,
            'errors': 0
        }

        # Process in batches
        for i in range(0, len(game_ids), batch_size):
            batch_ids = game_ids[i:i + batch_size]

            # Load all games with data
            games_dict = cls._load_games_with_data(batch_ids)

            batch_cards = []

            for game_id in batch_ids:
                game = games_dict.get(game_id)
                if not game:
                    stats['errors'] += 1
                    continue

                # Check existing
                existing_card = None
                if not force:
                    existing_card = GameCardCache.get_card_for_game(
                        game_id, show_similarity, None, 'normal'
                    )

                if existing_card and not force:
                    stats['skipped'] += 1
                    continue

                # Render card
                rendered_card = cls._render_card_html(game, show_similarity)

                # Extract related data
                related_data = cls._extract_related_data(game)

                batch_cards.append((
                    game,
                    rendered_card,
                    show_similarity,
                    None,
                    'normal',
                    related_data
                ))

            # Bulk create
            if batch_cards:
                try:
                    with transaction.atomic():
                        created_count = GameCardCache.bulk_create_cards(
                            batch_cards, batch_size=50
                        )
                        stats['created'] += created_count

                except Exception as e:
                    stats['errors'] += len(batch_cards)
                    logger.error(f"Error bulk creating cards: {str(e)}", exc_info=True)

        return stats

    @classmethod
    def update_card_for_game(
            cls,
            game: Game,
            show_similarity: bool = False,
            similarity_percent: float = None
    ) -> bool:
        """Update existing card for game."""
        try:
            # Invalidate old cards
            GameCardCache.invalidate_game_cards(game.id)

            # Create new card
            card, created = cls.create_card_for_game(
                game, show_similarity, similarity_percent, force=True
            )

            return card is not None

        except Exception as e:
            logger.error(f"Error updating card for game {game.id}: {str(e)}", exc_info=True)
            return False

    @classmethod
    def cleanup_old_cards(cls, days_old: int = 30) -> int:
        """Cleanup old inactive cards."""
        try:
            deleted_count = GameCardCache.cleanup_old_cards(days_old)

            logger.info(f"Cleaned up {deleted_count} old card caches")
            return deleted_count

        except Exception as e:
            logger.error(f"Error cleaning up old cards: {str(e)}", exc_info=True)
            return 0

    @classmethod
    def get_card_stats(cls) -> Dict[str, Any]:
        """Get card cache statistics."""
        try:
            from django.db.models import Sum, Count, Avg

            stats = GameCardCache.objects.filter(is_active=True).aggregate(
                total_cards=Count('id'),
                total_hits=Sum('hit_count'),
                avg_hits=Avg('hit_count'),
                newest=Max('created_at'),
                oldest=Min('created_at')
            )

            return {
                'total_cards': stats['total_cards'] or 0,
                'total_hits': stats['total_hits'] or 0,
                'avg_hits_per_card': round(stats['avg_hits'] or 0, 2),
                'newest_card': stats['newest'],
                'oldest_card': stats['oldest'],
            }

        except Exception as e:
            logger.error(f"Error getting card stats: {str(e)}", exc_info=True)
            return {}

    @classmethod
    def _load_game_with_data(cls, game_id: int) -> Optional[Game]:
        """Load game with all required prefetched data."""
        try:
            # Create prefetch objects
            genre_prefetch = Prefetch('genres', queryset=Genre.objects.only('id', 'name'))
            platform_prefetch = Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug'))
            perspective_prefetch = Prefetch('player_perspectives',
                                            queryset=PlayerPerspective.objects.only('id', 'name'))

            keyword_prefetch = Prefetch(
                'keywords',
                queryset=Keyword.objects.select_related('category').only(
                    'id', 'name', 'category__id', 'category__name'
                )
            )

            theme_prefetch = Prefetch('themes', queryset=Theme.objects.only('id', 'name'))
            game_mode_prefetch = Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name'))

            # Load game
            game = Game.objects.filter(id=game_id).prefetch_related(
                genre_prefetch,
                platform_prefetch,
                perspective_prefetch,
                keyword_prefetch,
                theme_prefetch,
                game_mode_prefetch
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            ).first()

            return game

        except Exception as e:
            logger.error(f"Error loading game {game_id} with data: {str(e)}", exc_info=True)
            return None

    @classmethod
    def _load_games_with_data(cls, game_ids: List[int]) -> Dict[int, Game]:
        """Load multiple games with all required data."""
        try:
            # Create prefetch objects
            genre_prefetch = Prefetch('genres', queryset=Genre.objects.only('id', 'name'))
            platform_prefetch = Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug'))
            perspective_prefetch = Prefetch('player_perspectives',
                                            queryset=PlayerPerspective.objects.only('id', 'name'))

            keyword_prefetch = Prefetch(
                'keywords',
                queryset=Keyword.objects.select_related('category').only(
                    'id', 'name', 'category__id', 'category__name'
                )
            )

            theme_prefetch = Prefetch('themes', queryset=Theme.objects.only('id', 'name'))
            game_mode_prefetch = Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name'))

            # Load games
            games = Game.objects.filter(id__in=game_ids).prefetch_related(
                genre_prefetch,
                platform_prefetch,
                perspective_prefetch,
                keyword_prefetch,
                theme_prefetch,
                game_mode_prefetch
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            )

            return {game.id: game for game in games}

        except Exception as e:
            logger.error(f"Error loading games with data: {str(e)}", exc_info=True)
            return {}

    @staticmethod
    def _render_card_html(
            game: Game,
            show_similarity: bool = False,
            similarity_percent: float = None
    ) -> str:
        """Render game card HTML."""
        context = {
            'game': game,
            'show_similarity': show_similarity,
        }

        if show_similarity and similarity_percent is not None:
            if not hasattr(game, 'similarity'):
                game.similarity = similarity_percent

        return render_to_string('games/partials/_game_card.html', context)

    @staticmethod
    def _extract_related_data(game: Game) -> Dict[str, List[Dict]]:
        """Extract related data from game."""
        return {
            'genres': [
                {'id': genre.id, 'name': genre.name}
                for genre in game.genres.all()
            ],
            'platforms': [
                {'id': platform.id, 'name': platform.name, 'slug': platform.slug or ''}
                for platform in game.platforms.all()
            ],
            'perspectives': [
                {'id': perspective.id, 'name': perspective.name}
                for perspective in game.player_perspectives.all()
            ],
            'keywords': [
                {
                    'id': keyword.id,
                    'name': keyword.name,
                    'category_id': keyword.category.id if keyword.category else None,
                    'category_name': keyword.category.name if keyword.category else None
                }
                for keyword in game.keywords.all()
            ],
            'themes': [
                {'id': theme.id, 'name': theme.name}
                for theme in game.themes.all()
            ],
            'game_modes': [
                {'id': game_mode.id, 'name': game_mode.name}
                for game_mode in game.game_modes.all()
            ],
        }