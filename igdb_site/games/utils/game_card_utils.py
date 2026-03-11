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
        Create cached card for a single game using get_or_create pattern.

        Returns:
            Tuple of (card object, created_new)
        """
        # Проверяем существующую карточку (только по game_id)
        existing_card = GameCardCache.get_card_for_game(game.id)

        # Если карточка существует, не пересоздаем (если только force=True)
        if existing_card and not force:
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

        # Create or update card cache
        try:
            with transaction.atomic():
                card, created = GameCardCache.get_or_create_card(
                    game=game_with_data,
                    rendered_card=rendered_card,
                    **related_data  # Убираем show_similarity, similarity_percent, card_size
                )

            logger.info(f"{'Created' if created else 'Updated'} card cache for game {game.id} ({game.name})")
            return card, created

        except Exception as e:
            logger.error(f"Error creating/updating card for game {game.id}: {str(e)}", exc_info=True)
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
        # Load all games with data
        games_dict = cls._load_games_with_data(game_ids)

        batch_cards = []

        for game_id in game_ids:
            game = games_dict.get(game_id)
            if not game:
                continue

            # Render card
            rendered_card = cls._render_card_html(game, show_similarity)

            # Extract related data
            related_data = cls._extract_related_data(game)

            # Убираем show_similarity, similarity_percent, card_size из данных
            batch_cards.append((
                game,
                rendered_card,
                related_data
            ))

        # Bulk create or update
        if batch_cards:
            try:
                stats = GameCardCache.bulk_create_or_update_cards(
                    batch_cards, batch_size=batch_size
                )

                logger.info(f"Bulk processed {len(batch_cards)} cards: "
                            f"created={stats['created']}, "
                            f"updated={stats['updated']}, "
                            f"skipped={stats['skipped']}, "
                            f"errors={stats['errors']}")

                return {
                    'total': len(batch_cards),
                    'created': stats['created'],
                    'updated': stats['updated'],
                    'skipped': stats['skipped'],
                    'errors': stats['errors']
                }

            except Exception as e:
                logger.error(f"Error bulk processing cards: {str(e)}", exc_info=True)
                return {
                    'total': len(batch_cards),
                    'created': 0,
                    'updated': 0,
                    'skipped': 0,
                    'errors': len(batch_cards)
                }

        return {
            'total': 0,
            'created': 0,
            'updated': 0,
            'skipped': 0,
            'errors': 0
        }

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