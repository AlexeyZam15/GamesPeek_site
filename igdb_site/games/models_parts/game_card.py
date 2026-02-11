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
        """Override save to handle unique constraint on cache_key."""
        # Генерируем ключ кэша если его нет
        if not self.cache_key:
            self.cache_key = self.generate_cache_key()

        # Генерируем хэш если его нет
        if not self.card_hash:
            self.card_hash = self.generate_card_hash()

        try:
            # Пытаемся сохранить с проверкой уникальности
            super().save(*args, **kwargs)
        except Exception as e:
            # Если ошибка уникальности, пытаемся обновить существующую запись
            if 'cache_key_key' in str(e) or 'unique' in str(e).lower():
                self.update_existing_record()
            else:
                raise e

    def update_existing_record(self) -> None:
        """Update existing record with same cache_key."""
        try:
            existing = GameCardCache.objects.get(cache_key=self.cache_key)

            # Обновляем все поля
            existing.rendered_card = self.rendered_card
            existing.compressed_card = self.compressed_card
            existing.game_name = self.game_name
            existing.game_rating = self.game_rating
            existing.game_cover_url = self.game_cover_url
            existing.game_type = self.game_type
            existing.genres_json = self.genres_json
            existing.platforms_json = self.platforms_json
            existing.perspectives_json = self.perspectives_json
            existing.keywords_json = self.keywords_json
            existing.themes_json = self.themes_json
            existing.game_modes_json = self.game_modes_json
            existing.show_similarity = self.show_similarity
            existing.similarity_percent = self.similarity_percent
            existing.card_size = self.card_size
            existing.is_active = self.is_active
            existing.card_hash = self.card_hash
            existing.hit_count = self.hit_count
            existing.updated_at = timezone.now()

            # Сохраняем обновленную запись
            existing.save()

            # Обновляем ID текущего объекта чтобы ссылка была корректной
            self.id = existing.id
            self.created_at = existing.created_at

        except GameCardCache.DoesNotExist:
            # Это не должно происходить, но на всякий случай
            raise Exception(f"Cannot update non-existing record with cache_key: {self.cache_key}")

    @classmethod
    def get_or_create_card(
            cls,
            game,
            rendered_card: str,
            show_similarity: bool = False,
            similarity_percent: float = None,
            card_size: str = 'normal',
            **related_data
    ) -> Tuple['GameCardCache', bool]:
        """
        Get existing card or create new one.

        Returns:
            Tuple of (card object, created_new)
        """
        cache_key = cls._generate_key(
            game.id, show_similarity, similarity_percent, card_size
        )

        try:
            # Пытаемся найти существующую запись
            card = cls.objects.get(cache_key=cache_key)
            created = False

            # Проверяем, не изменились ли данные
            new_card_hash = cls._calculate_card_hash(rendered_card)
            if card.card_hash != new_card_hash:
                # Данные изменились - обновляем
                card.rendered_card = rendered_card
                card.game_name = game.name
                card.game_rating = game.rating
                card.game_cover_url = game.cover_url
                card.game_type = game.game_type
                card.genres_json = related_data.get('genres', [])
                card.platforms_json = related_data.get('platforms', [])
                card.perspectives_json = related_data.get('perspectives', [])
                card.keywords_json = related_data.get('keywords', [])
                card.themes_json = related_data.get('themes', [])
                card.game_modes_json = related_data.get('game_modes', [])
                card.card_hash = new_card_hash
                card.is_active = True
                card.save()

        except cls.DoesNotExist:
            # Создаем новую запись
            card = cls.create_card(
                game=game,
                rendered_card=rendered_card,
                show_similarity=show_similarity,
                similarity_percent=similarity_percent,
                card_size=card_size,
                **related_data
            )
            created = True

        return card, created

    @classmethod
    def bulk_create_or_update_cards(
            cls,
            cards_data: List[Tuple],
            batch_size: int = 100
    ) -> Dict[str, int]:
        """
        Bulk create or update card caches.

        Returns:
            Dictionary with statistics
        """
        import logging
        logger = logging.getLogger(__name__)

        stats = {
            'created': 0,
            'updated': 0,
            'errors': 0,
            'skipped': 0
        }

        # Сначала получаем все существующие ключи
        cache_keys_to_check = []
        card_objects = {}

        for data in cards_data:
            game, rendered_card, show_similarity, similarity_percent, card_size, related_data = data
            cache_key = cls._generate_key(
                game.id, show_similarity, similarity_percent, card_size
            )
            cache_keys_to_check.append(cache_key)

            card_objects[cache_key] = {
                'game': game,
                'rendered_card': rendered_card,
                'show_similarity': show_similarity,
                'similarity_percent': similarity_percent,
                'card_size': card_size,
                'related_data': related_data
            }

        # Получаем существующие записи
        existing_cards = cls.objects.filter(cache_key__in=cache_keys_to_check)
        existing_keys = {card.cache_key: card for card in existing_cards}

        cards_to_create = []
        cards_to_update = []

        for cache_key, data in card_objects.items():
            if cache_key in existing_keys:
                # Существует - проверяем нужно ли обновить
                existing_card = existing_keys[cache_key]
                new_card_hash = cls._calculate_card_hash(data['rendered_card'])

                if existing_card.card_hash != new_card_hash:
                    # Данные изменились - обновляем
                    existing_card.rendered_card = data['rendered_card']
                    existing_card.game_name = data['game'].name
                    existing_card.game_rating = data['game'].rating
                    existing_card.game_cover_url = data['game'].cover_url
                    existing_card.game_type = data['game'].game_type
                    existing_card.genres_json = data['related_data'].get('genres', [])
                    existing_card.platforms_json = data['related_data'].get('platforms', [])
                    existing_card.perspectives_json = data['related_data'].get('perspectives', [])
                    existing_card.keywords_json = data['related_data'].get('keywords', [])
                    existing_card.themes_json = data['related_data'].get('themes', [])
                    existing_card.game_modes_json = data['related_data'].get('game_modes', [])
                    existing_card.card_hash = new_card_hash
                    existing_card.is_active = True
                    cards_to_update.append(existing_card)
                    stats['updated'] += 1
                else:
                    stats['skipped'] += 1
            else:
                # Не существует - создаем новую
                card = cls(
                    game=data['game'],
                    rendered_card=data['rendered_card'],
                    game_name=data['game'].name,
                    game_rating=data['game'].rating,
                    game_cover_url=data['game'].cover_url,
                    game_type=data['game'].game_type,
                    genres_json=data['related_data'].get('genres', []),
                    platforms_json=data['related_data'].get('platforms', []),
                    perspectives_json=data['related_data'].get('perspectives', []),
                    keywords_json=data['related_data'].get('keywords', []),
                    themes_json=data['related_data'].get('themes', []),
                    game_modes_json=data['related_data'].get('game_modes', []),
                    show_similarity=data['show_similarity'],
                    similarity_percent=data['similarity_percent'],
                    card_size=data['card_size'],
                    cache_key=cache_key
                )
                card.card_hash = cls._calculate_card_hash(data['rendered_card'])
                cards_to_create.append(card)
                stats['created'] += 1

        # Массовое создание
        if cards_to_create:
            try:
                cls.objects.bulk_create(cards_to_create, batch_size=batch_size)
            except Exception as e:
                logger.error(f"Batch creation failed: {str(e)}")
                stats['errors'] += len(cards_to_create)
                # Пробуем создать по одной
                for card in cards_to_create:
                    try:
                        card.save()
                    except Exception as e2:
                        logger.error(f"Failed to save card: {str(e2)}")
                        stats['errors'] += 1
                        stats['created'] -= 1

        # Массовое обновление
        if cards_to_update:
            try:
                cls.objects.bulk_update(
                    cards_to_update,
                    fields=[
                        'rendered_card', 'game_name', 'game_rating', 'game_cover_url',
                        'game_type', 'genres_json', 'platforms_json', 'perspectives_json',
                        'keywords_json', 'themes_json', 'game_modes_json', 'card_hash',
                        'is_active', 'updated_at'
                    ],
                    batch_size=batch_size
                )
            except Exception as e:
                logger.error(f"Batch update failed: {str(e)}")
                # Пробуем обновить по одной
                for card in cards_to_update:
                    try:
                        card.save()
                    except Exception as e2:
                        logger.error(f"Failed to update card: {str(e2)}")
                        stats['errors'] += 1
                        stats['updated'] -= 1

        return stats

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

    @staticmethod
    def _calculate_card_hash(rendered_card: str) -> str:
        """Calculate hash for rendered card content."""
        content = rendered_card or ""
        return hashlib.md5(content.encode()).hexdigest()

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
        card.card_hash = cls._calculate_card_hash(rendered_card)
        card.save()
        return card

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