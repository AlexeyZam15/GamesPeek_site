"""Optimized game card caching model."""

from django.db import models
from django.utils import timezone
from django.core.cache import cache
from typing import Dict, List, Optional, Tuple
import json
import hashlib


class GameCardCache(models.Model):
    """Model for caching pre-rendered game cards with all related data."""

    CARD_CACHE_VERSION = 'v2'

    game = models.OneToOneField(
        'Game',
        on_delete=models.CASCADE,
        related_name='card_cache',
        db_index=True,
        verbose_name="Game"
    )

    rendered_card = models.TextField(verbose_name="Rendered HTML card")
    compressed_card = models.BinaryField(null=True, blank=True, verbose_name="Compressed card data")

    game_name = models.CharField(max_length=255, db_index=True, verbose_name="Game name")
    game_rating = models.FloatField(null=True, blank=True, db_index=True, verbose_name="Game rating")
    game_cover_url = models.URLField(max_length=500, blank=True, null=True, verbose_name="Cover URL")
    game_type = models.IntegerField(null=True, blank=True, db_index=True, verbose_name="Game type")

    genres_json = models.JSONField(default=list, verbose_name="Genres JSON")
    platforms_json = models.JSONField(default=list, verbose_name="Platforms JSON")
    perspectives_json = models.JSONField(default=list, verbose_name="Perspectives JSON")
    keywords_json = models.JSONField(default=list, verbose_name="Keywords JSON")
    themes_json = models.JSONField(default=list, verbose_name="Themes JSON")
    game_modes_json = models.JSONField(default=list, verbose_name="Game modes JSON")

    screenshots_json = models.JSONField(default=list, verbose_name="Screenshots JSON")

    is_active = models.BooleanField(default=True, db_index=True, verbose_name="Active")
    cache_key = models.CharField(max_length=64, unique=True, db_index=True, verbose_name="Cache key")
    card_hash = models.CharField(max_length=64, db_index=True, verbose_name="Card hash")
    template_version = models.CharField(max_length=10, default=CARD_CACHE_VERSION, verbose_name="Template version")

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
            models.Index(fields=['template_version', 'is_active']),
        ]

    def __str__(self) -> str:
        return f"Card cache for {self.game_name}"

    def save(self, *args, **kwargs) -> None:
        if not self.cache_key:
            self.cache_key = self.generate_cache_key()
        if not self.card_hash:
            self.card_hash = self.generate_card_hash()
        if not self.template_version:
            self.template_version = self.CARD_CACHE_VERSION
        self.updated_at = timezone.now()
        super().save(*args, **kwargs)

    @classmethod
    def get_card_for_game(cls, game_id: int, game=None) -> Optional['GameCardCache']:
        try:
            card = cls.objects.select_related('game').get(
                game_id=game_id,
                is_active=True
            )

            if card.template_version != cls.CARD_CACHE_VERSION:
                card.is_active = False
                card.save(update_fields=['is_active'])
                return None

            if game:
                if (card.game_name != game.name or
                        card.game_rating != getattr(game, 'rating', None) or
                        card.game_cover_url != getattr(game, 'cover_url', None) or
                        card.game_type != getattr(game, 'game_type', None)):
                    card.is_active = False
                    card.save(update_fields=['is_active'])
                    return None

                from games.utils.game_card_utils import GameCardCreator
                current_related_data = GameCardCreator._extract_related_data(game)

                if (card.genres_json != current_related_data.get('genres', []) or
                        card.platforms_json != current_related_data.get('platforms', []) or
                        card.perspectives_json != current_related_data.get('perspectives', []) or
                        card.keywords_json != current_related_data.get('keywords', []) or
                        card.themes_json != current_related_data.get('themes', []) or
                        card.game_modes_json != current_related_data.get('game_modes', []) or
                        card.screenshots_json != current_related_data.get('screenshots', [])):
                    card.is_active = False
                    card.save(update_fields=['is_active'])
                    return None

            return card
        except cls.DoesNotExist:
            return None

    @classmethod
    def bulk_create_or_update_cards(cls, cards_data: List[Tuple], batch_size: int = 100) -> Dict[str, int]:
        import logging
        logger = logging.getLogger(__name__)

        stats = {'created': 0, 'updated': 0, 'errors': 0, 'skipped': 0}

        print(f"\n=== BULK CREATE/UPDATE CARDS START ===")
        print(f"Received {len(cards_data)} cards to process")
        print(f"Current template version: {cls.CARD_CACHE_VERSION}")

        if not cards_data:
            print("No cards data to process")
            return stats

        current_template_version = cls.CARD_CACHE_VERSION

        for data in cards_data:
            try:
                if len(data) == 3:
                    game, rendered_card, related_data = data
                elif len(data) == 6:
                    game, rendered_card, _, _, _, related_data = data
                else:
                    logger.error(f"Invalid card data format: {len(data)} elements")
                    stats['errors'] += 1
                    continue

                expected_key = cls.generate_cache_key_for_game(game.id)

                try:
                    card = cls.objects.get(game=game)

                    new_card_hash = cls._calculate_card_hash(rendered_card)

                    if (card.card_hash != new_card_hash or
                            card.template_version != current_template_version or
                            card.screenshots_json != related_data.get('screenshots', [])):

                        card.rendered_card = rendered_card
                        card.card_hash = new_card_hash
                        card.template_version = current_template_version
                        card.game_name = game.name
                        card.game_rating = getattr(game, 'rating', None)
                        card.game_cover_url = getattr(game, 'cover_url', None)
                        card.game_type = getattr(game, 'game_type', None)
                        card.genres_json = related_data.get('genres', [])
                        card.platforms_json = related_data.get('platforms', [])
                        card.perspectives_json = related_data.get('perspectives', [])
                        card.keywords_json = related_data.get('keywords', [])
                        card.themes_json = related_data.get('themes', [])
                        card.game_modes_json = related_data.get('game_modes', [])
                        card.screenshots_json = related_data.get('screenshots', [])
                        card.cache_key = expected_key
                        card.updated_at = timezone.now()
                        card.is_active = True

                        card.save()
                        stats['updated'] += 1
                    else:
                        if not card.is_active:
                            card.is_active = True
                            card.save(update_fields=['is_active'])
                        stats['skipped'] += 1

                except cls.DoesNotExist:
                    card = cls(
                        game=game,
                        rendered_card=rendered_card,
                        game_name=game.name,
                        game_rating=getattr(game, 'rating', None),
                        game_cover_url=getattr(game, 'cover_url', None),
                        game_type=getattr(game, 'game_type', None),
                        genres_json=related_data.get('genres', []),
                        platforms_json=related_data.get('platforms', []),
                        perspectives_json=related_data.get('perspectives', []),
                        keywords_json=related_data.get('keywords', []),
                        themes_json=related_data.get('themes', []),
                        game_modes_json=related_data.get('game_modes', []),
                        screenshots_json=related_data.get('screenshots', []),
                        cache_key=expected_key,
                        template_version=current_template_version,
                        is_active=True
                    )
                    card.card_hash = cls._calculate_card_hash(rendered_card)
                    card.save()
                    stats['created'] += 1

            except Exception as e:
                game_id = 'unknown'
                if 'game' in locals() and game is not None:
                    try:
                        game_id = game.id
                    except AttributeError:
                        game_id = str(game)

                logger.error(f"Failed to save card for game {game_id}: {str(e)}")
                stats['errors'] += 1

        print(f"=== BULK CREATE/UPDATE CARDS COMPLETE: {stats} ===\n")
        return stats

    @classmethod
    def get_or_create_card(cls, game, rendered_card: str, **related_data) -> Tuple['GameCardCache', bool]:
        from django.db import transaction

        cache_key = cls.generate_cache_key_for_game(game.id)
        current_template_version = cls.CARD_CACHE_VERSION

        with transaction.atomic():
            try:
                card = cls.objects.select_for_update().get(game=game)
                created = False

                new_card_hash = cls._calculate_card_hash(rendered_card)

                needs_update = False
                update_fields = []

                if card.card_hash != new_card_hash:
                    needs_update = True
                    card.rendered_card = rendered_card
                    card.card_hash = new_card_hash
                    update_fields.extend(['rendered_card', 'card_hash'])

                if card.template_version != current_template_version:
                    needs_update = True
                    card.template_version = current_template_version
                    update_fields.append('template_version')

                if card.game_name != game.name:
                    card.game_name = game.name
                    update_fields.append('game_name')
                if card.game_rating != getattr(game, 'rating', None):
                    card.game_rating = getattr(game, 'rating', None)
                    update_fields.append('game_rating')
                if card.game_cover_url != getattr(game, 'cover_url', None):
                    card.game_cover_url = getattr(game, 'cover_url', None)
                    update_fields.append('game_cover_url')
                if card.game_type != getattr(game, 'game_type', None):
                    card.game_type = getattr(game, 'game_type', None)
                    update_fields.append('game_type')

                if card.genres_json != related_data.get('genres', []):
                    card.genres_json = related_data.get('genres', [])
                    update_fields.append('genres_json')
                if card.platforms_json != related_data.get('platforms', []):
                    card.platforms_json = related_data.get('platforms', [])
                    update_fields.append('platforms_json')
                if card.perspectives_json != related_data.get('perspectives', []):
                    card.perspectives_json = related_data.get('perspectives', [])
                    update_fields.append('perspectives_json')
                if card.keywords_json != related_data.get('keywords', []):
                    card.keywords_json = related_data.get('keywords', [])
                    update_fields.append('keywords_json')
                if card.themes_json != related_data.get('themes', []):
                    card.themes_json = related_data.get('themes', [])
                    update_fields.append('themes_json')
                if card.game_modes_json != related_data.get('game_modes', []):
                    card.game_modes_json = related_data.get('game_modes', [])
                    update_fields.append('game_modes_json')
                if card.screenshots_json != related_data.get('screenshots', []):
                    card.screenshots_json = related_data.get('screenshots', [])
                    update_fields.append('screenshots_json')

                if card.cache_key != cache_key:
                    card.cache_key = cache_key
                    update_fields.append('cache_key')

                if not card.is_active:
                    card.is_active = True
                    update_fields.append('is_active')

                if needs_update or update_fields:
                    card.updated_at = timezone.now()
                    update_fields.append('updated_at')
                    card.save(update_fields=update_fields)

            except cls.DoesNotExist:
                card = cls(
                    game=game,
                    rendered_card=rendered_card,
                    game_name=game.name,
                    game_rating=getattr(game, 'rating', None),
                    game_cover_url=getattr(game, 'cover_url', None),
                    game_type=getattr(game, 'game_type', None),
                    genres_json=related_data.get('genres', []),
                    platforms_json=related_data.get('platforms', []),
                    perspectives_json=related_data.get('perspectives', []),
                    keywords_json=related_data.get('keywords', []),
                    themes_json=related_data.get('themes', []),
                    game_modes_json=related_data.get('game_modes', []),
                    screenshots_json=related_data.get('screenshots', []),
                    cache_key=cache_key,
                    template_version=current_template_version,
                    is_active=True
                )
                card.card_hash = cls._calculate_card_hash(rendered_card)
                card.save()
                created = True

        return card, created

    @classmethod
    def generate_cache_key_for_game(cls, game_id: int) -> str:
        return f"game_card_{game_id}"

    def generate_cache_key(self) -> str:
        return self.generate_cache_key_for_game(self.game_id)

    def generate_card_hash(self) -> str:
        content = (self.rendered_card or "")
        if self.compressed_card:
            content += str(self.compressed_card)
        return hashlib.md5(content.encode() if isinstance(content, str) else content).hexdigest()

    @staticmethod
    def _calculate_card_hash(rendered_card: str) -> str:
        content = rendered_card or ""
        return hashlib.md5(content.encode()).hexdigest()

    def increment_hit(self) -> None:
        self.hit_count += 1
        self.last_accessed = timezone.now()
        GameCardCache.objects.filter(id=self.id).update(
            hit_count=self.hit_count,
            last_accessed=self.last_accessed
        )

    def get_genres(self) -> List[Dict]:
        return self.genres_json or []

    def get_platforms(self) -> List[Dict]:
        return self.platforms_json or []

    def get_keywords(self) -> List[Dict]:
        return self.keywords_json or []

    def get_themes(self) -> List[Dict]:
        return self.themes_json or []

    def get_perspectives(self) -> List[Dict]:
        return self.perspectives_json or []

    def get_game_modes(self) -> List[Dict]:
        return self.game_modes_json or []

    def get_screenshots(self) -> List[Dict]:
        return self.screenshots_json or []

    @classmethod
    def create_card(cls, game, rendered_card: str, **related_data) -> 'GameCardCache':
        cache_key = cls.generate_cache_key_for_game(game.id)

        card, _ = cls.get_or_create_card(
            game=game,
            rendered_card=rendered_card,
            **related_data
        )
        return card

    @classmethod
    def invalidate_game_cards(cls, game_id: int) -> int:
        updated = cls.objects.filter(game_id=game_id, is_active=True).update(
            is_active=False,
            updated_at=timezone.now()
        )

        cache_keys = cls.objects.filter(game_id=game_id).values_list('cache_key', flat=True)
        for cache_key in cache_keys:
            cache.delete(cache_key)

        return updated

    @classmethod
    def cleanup_old_cards(cls, days_old: int = 30) -> int:
        cutoff_date = timezone.now() - timezone.timedelta(days=days_old)
        old_cards = cls.objects.filter(
            is_active=False,
            updated_at__lt=cutoff_date
        )
        count = old_cards.count()
        old_cards.delete()
        return count

    @classmethod
    def bump_cache_version(cls, new_version: str = None) -> str:
        if new_version:
            cls.CARD_CACHE_VERSION = new_version
        else:
            current = cls.CARD_CACHE_VERSION
            if current.startswith('v') and current[1:].isdigit():
                num = int(current[1:]) + 1
                cls.CARD_CACHE_VERSION = f'v{num}'
            else:
                cls.CARD_CACHE_VERSION = 'v2'
        return cls.CARD_CACHE_VERSION