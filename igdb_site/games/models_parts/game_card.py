"""Optimized game card caching model."""

from django.db import models
from django.utils import timezone
from django.core.cache import cache
from typing import Dict, List, Optional, Tuple
import json
import hashlib


class GameCardCache(models.Model):
    """Model for caching pre-rendered game cards with all related data."""

    game = models.OneToOneField(
        'Game',
        on_delete=models.CASCADE,
        related_name='card_cache',
        db_index=True,
        verbose_name="Game"
    )

    # Main card data
    rendered_card = models.TextField(verbose_name="Rendered HTML card")
    compressed_card = models.BinaryField(null=True, blank=True, verbose_name="Compressed card data")

    # Cached game data for quick access
    game_name = models.CharField(max_length=255, db_index=True, verbose_name="Game name")
    game_rating = models.FloatField(null=True, blank=True, db_index=True, verbose_name="Game rating")
    game_cover_url = models.URLField(max_length=500, blank=True, null=True, verbose_name="Cover URL")
    game_type = models.IntegerField(null=True, blank=True, db_index=True, verbose_name="Game type")

    # Cached related data (as JSON)
    genres_json = models.JSONField(default=list, verbose_name="Genres JSON")
    platforms_json = models.JSONField(default=list, verbose_name="Platforms JSON")
    perspectives_json = models.JSONField(default=list, verbose_name="Perspectives JSON")
    keywords_json = models.JSONField(default=list, verbose_name="Keywords JSON")
    themes_json = models.JSONField(default=list, verbose_name="Themes JSON")
    game_modes_json = models.JSONField(default=list, verbose_name="Game modes JSON")

    # Card configuration
    show_similarity = models.BooleanField(default=False, verbose_name="Show similarity")
    similarity_percent = models.FloatField(null=True, blank=True, verbose_name="Similarity percent")
    card_size = models.CharField(max_length=20, default='normal', verbose_name="Card size")

    # Cache metadata
    is_active = models.BooleanField(default=True, db_index=True, verbose_name="Active")
    cache_key = models.CharField(max_length=64, unique=True, db_index=True, verbose_name="Cache key")
    card_hash = models.CharField(max_length=64, db_index=True, verbose_name="Card hash")

    # Statistics
    hit_count = models.IntegerField(default=0, verbose_name="Hit count")
    last_accessed = models.DateTimeField(auto_now=True, verbose_name="Last accessed")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Created at")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="Updated at")

    class Meta:
        verbose_name = "Game card cache"
        verbose_name_plural = "Game card caches"
        ordering = ['-last_accessed']
        indexes = [
            models.Index(fields=['game', 'is_active']),
            models.Index(fields=['cache_key', 'is_active']),
            models.Index(fields=['game_rating', 'is_active']),
            models.Index(fields=['created_at', 'is_active']),
            models.Index(fields=['hit_count', 'is_active']),
            models.Index(fields=['card_size', 'is_active']),
            models.Index(fields=['show_similarity', 'is_active']),
        ]

    def __str__(self) -> str:
        return f"Card cache for {self.game_name}"

    def save(self, *args, **kwargs) -> None:
        """Override save to generate cache key and hash."""
        if not self.cache_key:
            self.cache_key = self.generate_cache_key()
        if not self.card_hash:
            self.card_hash = self.generate_card_hash()
        super().save(*args, **kwargs)

    @classmethod
    def bulk_create_cards(
            cls,
            cards_data: List[Tuple],
            batch_size: int = 100
    ) -> int:
        """Bulk create card caches and return number of successfully saved cards."""
        import logging
        logger = logging.getLogger(__name__)

        cards_to_create = []
        saved_count = 0

        for data in cards_data:
            game, rendered_card, show_similarity, similarity_percent, card_size, related_data = data

            cache_key = cls._generate_key(
                game.id, show_similarity, similarity_percent, card_size
            )

            card = cls(
                game=game,
                rendered_card=rendered_card,
                game_name=game.name,
                game_rating=game.rating,
                game_cover_url=game.cover_url,
                game_type=game.game_type,
                genres_json=related_data.get('genres', []),
                platforms_json=related_data.get('platforms', []),
                perspectives_json=related_data.get('perspectives', []),
                keywords_json=related_data.get('keywords', []),
                themes_json=related_data.get('themes', []),
                game_modes_json=related_data.get('game_modes', []),
                show_similarity=show_similarity,
                similarity_percent=similarity_percent,
                card_size=card_size,
                cache_key=cache_key
            )

            # Generate card hash
            content = rendered_card or ""
            card.card_hash = hashlib.md5(content.encode() if isinstance(content, str) else content).hexdigest()

            cards_to_create.append(card)

        try:
            # Пытаемся сохранить все карточки одним запросом
            cls.objects.bulk_create(cards_to_create, batch_size=batch_size)
            saved_count = len(cards_to_create)

            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Successfully bulk created {saved_count} cards in one batch")

        except Exception as e:
            # Если ошибка массового создания, пытаемся сохранить по одной
            logger.error(f"Batch creation failed, trying one by one: {str(e)}")

            for card in cards_to_create:
                try:
                    card.save()
                    saved_count += 1

                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Successfully saved card for game {card.game_id}")

                except Exception as e2:
                    logger.error(f"Failed to save card for game {card.game_id}: {str(e2)}")
                    continue

        return saved_count

    def generate_cache_key(self) -> str:
        """Generate unique cache key for this card configuration."""
        key_data = {
            'game_id': self.game_id,
            'show_similarity': self.show_similarity,
            'similarity_percent': self.similarity_percent,
            'card_size': self.card_size,
            'version': 'v2'
        }

        key_str = json.dumps(key_data, sort_keys=True)
        return f"game_card_{hashlib.md5(key_str.encode()).hexdigest()}"

    def generate_card_hash(self) -> str:
        """Generate hash of card content for change detection."""
        content = self.rendered_card or ""
        if self.compressed_card:
            content += str(self.compressed_card)

        return hashlib.md5(content.encode() if isinstance(content, str) else content).hexdigest()

    def increment_hit(self) -> None:
        """Increment hit count and update last accessed time."""
        self.hit_count += 1
        self.last_accessed = timezone.now()
        # Use update to avoid full save
        GameCardCache.objects.filter(id=self.id).update(
            hit_count=self.hit_count,
            last_accessed=self.last_accessed
        )

    def get_genres(self) -> List[Dict]:
        """Get genres as list of dictionaries."""
        return self.genres_json or []

    def get_platforms(self) -> List[Dict]:
        """Get platforms as list of dictionaries."""
        return self.platforms_json or []

    def get_keywords(self) -> List[Dict]:
        """Get keywords as list of dictionaries."""
        return self.keywords_json or []

    def get_themes(self) -> List[Dict]:
        """Get themes as list of dictionaries."""
        return self.themes_json or []

    def get_perspectives(self) -> List[Dict]:
        """Get perspectives as list of dictionaries."""
        return self.perspectives_json or []

    def get_game_modes(self) -> List[Dict]:
        """Get game modes as list of dictionaries."""
        return self.game_modes_json or []

    @classmethod
    def get_card_for_game(
            cls,
            game_id: int,
            show_similarity: bool = False,
            similarity_percent: float = None,
            card_size: str = 'normal'
    ) -> Optional['GameCardCache']:
        """Get cached card for game with specific configuration."""
        cache_key = cls._generate_key(game_id, show_similarity, similarity_percent, card_size)

        try:
            card = cls.objects.select_related('game').get(
                cache_key=cache_key,
                is_active=True
            )
            card.increment_hit()
            return card
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_card(
            cls,
            game,
            rendered_card: str,
            show_similarity: bool = False,
            similarity_percent: float = None,
            card_size: str = 'normal',
            **related_data
    ) -> 'GameCardCache':
        """Create new card cache entry."""

        # Prepare related data JSON
        genres_json = related_data.get('genres', [])
        platforms_json = related_data.get('platforms', [])
        perspectives_json = related_data.get('perspectives', [])
        keywords_json = related_data.get('keywords', [])
        themes_json = related_data.get('themes', [])
        game_modes_json = related_data.get('game_modes', [])

        # Generate cache key
        cache_key = cls._generate_key(
            game.id, show_similarity, similarity_percent, card_size
        )

        # Create card cache
        card = cls(
            game=game,
            rendered_card=rendered_card,
            game_name=game.name,
            game_rating=game.rating,
            game_cover_url=game.cover_url,
            game_type=game.game_type,
            genres_json=genres_json,
            platforms_json=platforms_json,
            perspectives_json=perspectives_json,
            keywords_json=keywords_json,
            themes_json=themes_json,
            game_modes_json=game_modes_json,
            show_similarity=show_similarity,
            similarity_percent=similarity_percent,
            card_size=card_size,
            cache_key=cache_key
        )

        card.save()
        return card

    @classmethod
    def bulk_create_cards(
            cls,
            cards_data: List[Tuple],
            batch_size: int = 100
    ) -> int:
        """Bulk create card caches."""
        cards_to_create = []

        for data in cards_data:
            game, rendered_card, show_similarity, similarity_percent, card_size, related_data = data

            cache_key = cls._generate_key(
                game.id, show_similarity, similarity_percent, card_size
            )

            card = cls(
                game=game,
                rendered_card=rendered_card,
                game_name=game.name,
                game_rating=game.rating,
                game_cover_url=game.cover_url,
                game_type=game.game_type,
                genres_json=related_data.get('genres', []),
                platforms_json=related_data.get('platforms', []),
                perspectives_json=related_data.get('perspectives', []),
                keywords_json=related_data.get('keywords', []),
                themes_json=related_data.get('themes', []),
                game_modes_json=related_data.get('game_modes', []),
                show_similarity=show_similarity,
                similarity_percent=similarity_percent,
                card_size=card_size,
                cache_key=cache_key
            )
            card.card_hash = card.generate_card_hash()
            cards_to_create.append(card)

        cls.objects.bulk_create(cards_to_create, batch_size=batch_size)
        return len(cards_to_create)

    @classmethod
    def invalidate_game_cards(cls, game_id: int) -> int:
        """Invalidate all card caches for a game."""
        updated = cls.objects.filter(game_id=game_id, is_active=True).update(
            is_active=False,
            updated_at=timezone.now()
        )

        # Clear related cache keys
        cache_keys = cls.objects.filter(game_id=game_id).values_list('cache_key', flat=True)
        for cache_key in cache_keys:
            cache.delete(cache_key)

        return updated

    @classmethod
    def cleanup_old_cards(cls, days_old: int = 30) -> int:
        """Cleanup old inactive card caches."""
        cutoff_date = timezone.now() - timezone.timedelta(days=days_old)

        old_cards = cls.objects.filter(
            is_active=False,
            updated_at__lt=cutoff_date
        )

        count = old_cards.count()
        old_cards.delete()

        return count

    @staticmethod
    def _generate_key(
            game_id: int,
            show_similarity: bool,
            similarity_percent: float,
            card_size: str
    ) -> str:
        """Generate cache key for parameters."""
        key_data = {
            'game_id': game_id,
            'show_similarity': show_similarity,
            'similarity_percent': similarity_percent,
            'card_size': card_size,
            'version': 'v2'
        }

        key_str = json.dumps(key_data, sort_keys=True)
        return f"game_card_{hashlib.md5(key_str.encode()).hexdigest()}"

