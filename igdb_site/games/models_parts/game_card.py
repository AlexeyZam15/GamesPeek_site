"""Optimized game card caching model."""

from django.db import models
from django.utils import timezone
from django.core.cache import cache
from typing import Dict, List, Optional, Tuple
import json
import hashlib


class GameCardCache(models.Model):
    """Model for caching pre-rendered game cards with all related data."""

    # Константа версии кэша карточек - увеличивать при изменении структуры HTML
    CARD_CACHE_VERSION = 'v1'  # Было без версии, теперь v1

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
        ]

    def __str__(self) -> str:
        return f"Card cache for {self.game_name}"

    def save(self, *args, **kwargs) -> None:
        """Сохраняет карточку игры - всегда только одна запись на игру."""
        # Генерируем ключ кэша если его нет
        if not self.cache_key:
            self.cache_key = self.generate_cache_key()

        # Генерируем хэш если его нет
        if not self.card_hash:
            self.card_hash = self.generate_card_hash()

        self.updated_at = timezone.now()

        # Всегда только одна запись на игру
        super().save(*args, **kwargs)

    @classmethod
    def get_card_for_game(cls, game_id: int) -> Optional['GameCardCache']:
        """
        Get cached card for game.

        Args:
            game_id: ID of the game

        Returns:
            GameCardCache object or None if not found
        """
        try:
            card = cls.objects.select_related('game').get(
                game_id=game_id,
                is_active=True
            )
            return card
        except cls.DoesNotExist:
            return None

    @classmethod
    def bulk_create_or_update_cards(
            cls,
            cards_data: List[Tuple],
            batch_size: int = 100
    ) -> Dict[str, int]:
        """
        Массовое обновление карточек игр.

        Args:
            cards_data: List of tuples (game, rendered_card, related_data)
            batch_size: Batch size for bulk operations (unused in this version)

        Returns:
            Dictionary with statistics
        """
        import logging
        logger = logging.getLogger(__name__)

        stats = {'created': 0, 'updated': 0, 'errors': 0, 'skipped': 0}

        if not cards_data:
            return stats

        from django.db import transaction

        for data in cards_data:
            try:
                # Поддержка разных форматов для обратной совместимости
                if len(data) == 3:
                    game, rendered_card, related_data = data
                elif len(data) == 6:
                    # Старый формат: (game, rendered_card, show_similarity, similarity_percent, card_size, related_data)
                    game, rendered_card, _, _, _, related_data = data
                else:
                    logger.error(f"Invalid card data format: {len(data)} elements")
                    stats['errors'] += 1
                    continue

                with transaction.atomic():
                    _, created = cls.get_or_create_card(
                        game=game,
                        rendered_card=rendered_card,
                        **related_data
                    )
                    if created:
                        stats['created'] += 1
                    else:
                        stats['updated'] += 1

            except Exception as e:
                # Безопасно получаем game_id
                game_id = 'unknown'
                if 'game' in locals() and game is not None:
                    try:
                        game_id = game.id
                    except AttributeError:
                        game_id = str(game)

                logger.error(f"Failed to save card for game {game_id}: {str(e)}")
                stats['errors'] += 1

        return stats

    @classmethod
    def get_or_create_card(
            cls,
            game,
            rendered_card: str,
            **related_data
    ) -> Tuple['GameCardCache', bool]:
        """
        Получает существующую карточку или создает новую.
        Процент схожести НЕ кэшируется в карточке!
        """
        from django.db import transaction

        cache_key = cls.generate_cache_key_for_game(game.id)

        with transaction.atomic():
            try:
                card = cls.objects.select_for_update().get(game=game)
                created = False

                # Проверяем, не изменились ли данные игры или версия кэша
                new_card_hash = cls._calculate_card_hash(rendered_card)

                # Также проверяем, что ключ кэша соответствует текущей версии
                expected_cache_key = cls.generate_cache_key_for_game(game.id)

                if card.card_hash != new_card_hash or card.cache_key != expected_cache_key:
                    card.rendered_card = rendered_card
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
                    card.card_hash = new_card_hash
                    card.cache_key = expected_cache_key  # Обновляем ключ с новой версией
                    card.save(update_fields=[
                        'rendered_card', 'game_name', 'game_rating', 'game_cover_url',
                        'game_type', 'genres_json', 'platforms_json', 'perspectives_json',
                        'keywords_json', 'themes_json', 'game_modes_json',
                        'card_hash', 'cache_key', 'updated_at'
                    ])

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
                    cache_key=cache_key,
                    is_active=True
                )
                card.card_hash = cls._calculate_card_hash(rendered_card)
                card.save()
                created = True

        return card, created

    @classmethod
    def generate_cache_key_for_game(cls, game_id: int) -> str:
        """Генерирует ключ кэша для игры с учетом версии."""
        return f"game_card_{cls.CARD_CACHE_VERSION}_{game_id}"

    def generate_cache_key(self) -> str:
        """Генерирует ключ кэша с учетом версии."""
        return self.generate_cache_key_for_game(self.game_id)

    def generate_card_hash(self) -> str:
        """Generate hash of card content for change detection."""
        # Добавляем версию кэша в хэш, чтобы изменения версии тоже инвалидировали карточки
        content = (self.rendered_card or "") + self.CARD_CACHE_VERSION
        if self.compressed_card:
            content += str(self.compressed_card)
        return hashlib.md5(content.encode() if isinstance(content, str) else content).hexdigest()

    @staticmethod
    def _calculate_card_hash(rendered_card: str) -> str:
        """Calculate hash for rendered card content."""
        # Статический метод не имеет доступа к CARD_CACHE_VERSION,
        # поэтому версия добавляется в get_or_create_card при сравнении
        content = rendered_card or ""
        return hashlib.md5(content.encode()).hexdigest()

    def increment_hit(self) -> None:
        """Increment hit count and update last accessed time."""
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

    @classmethod
    def create_card(cls, game, rendered_card: str, **related_data) -> 'GameCardCache':
        """Create new card cache entry."""
        cache_key = cls.generate_cache_key_for_game(game.id)

        card, _ = cls.get_or_create_card(
            game=game,
            rendered_card=rendered_card,
            **related_data
        )
        return card

    @classmethod
    def invalidate_game_cards(cls, game_id: int) -> int:
        """Invalidate card cache for a game."""
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

    @classmethod
    def bump_cache_version(cls, new_version: str = None) -> str:
        """
        Увеличивает версию кэша карточек.
        Использовать после изменений в структуре HTML карточек.

        Args:
            new_version: Новая версия (если не указана, увеличивает текущую)

        Returns:
            Новая версия кэша
        """
        if new_version:
            cls.CARD_CACHE_VERSION = new_version
        else:
            # Парсим текущую версию и увеличиваем номер
            current = cls.CARD_CACHE_VERSION
            if current.startswith('v') and current[1:].isdigit():
                num = int(current[1:]) + 1
                cls.CARD_CACHE_VERSION = f'v{num}'
            else:
                cls.CARD_CACHE_VERSION = f'v2'  # Если не в формате v1, ставим v2

        return cls.CARD_CACHE_VERSION