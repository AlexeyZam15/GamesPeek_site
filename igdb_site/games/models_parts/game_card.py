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
        """
        Сохраняет карточку игры с гарантией уникальности cache_key.
        Всегда удаляет существующие карточки с таким же ключом перед сохранением.
        """
        # Генерируем ключ кэша если его нет
        if not self.cache_key:
            self.cache_key = self.generate_cache_key()

        # Генерируем хэш если его нет
        if not self.card_hash:
            self.card_hash = self.generate_card_hash()

        # Устанавливаем updated_at
        self.updated_at = timezone.now()

        # Удаляем все существующие карточки с таким же cache_key
        # Это гарантирует уникальность без ошибок constraint violation
        if self.pk is None:
            GameCardCache.objects.filter(cache_key=self.cache_key).delete()
        else:
            GameCardCache.objects.filter(cache_key=self.cache_key).exclude(pk=self.pk).delete()

        # Сохраняем новую запись
        try:
            super().save(*args, **kwargs)
        except Exception as e:
            # На случай race condition - пробуем еще раз с удалением
            if 'unique' in str(e).lower() or 'duplicate' in str(e).lower():
                GameCardCache.objects.filter(cache_key=self.cache_key).delete()
                super().save(*args, **kwargs)
            else:
                raise e

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
        Получает существующую карточку или создает новую.
        Гарантирует уникальность и атомарность операции.

        Returns:
            Tuple of (card object, created_new)
        """
        cache_key = cls._generate_key(
            game.id, show_similarity, similarity_percent, card_size
        )

        from django.db import transaction

        with transaction.atomic():
            try:
                # Пытаемся найти существующую запись с блокировкой
                card = cls.objects.select_for_update().get(cache_key=cache_key)
                created = False

                # Проверяем, не изменились ли данные
                new_card_hash = cls._calculate_card_hash(rendered_card)
                if card.card_hash != new_card_hash:
                    # Данные изменились - обновляем
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
                    card.show_similarity = show_similarity
                    card.similarity_percent = similarity_percent
                    card.card_hash = new_card_hash
                    card.is_active = True
                    card.updated_at = timezone.now()
                    card.save(update_fields=[
                        'rendered_card', 'game_name', 'game_rating', 'game_cover_url',
                        'game_type', 'genres_json', 'platforms_json', 'perspectives_json',
                        'keywords_json', 'themes_json', 'game_modes_json',
                        'show_similarity', 'similarity_percent', 'card_hash',
                        'is_active', 'updated_at'
                    ])

            except cls.DoesNotExist:
                # Создаем новую запись
                # Предварительно удаляем любые дубликаты для этого ключа
                cls.objects.filter(cache_key=cache_key).delete()

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
                    show_similarity=show_similarity,
                    similarity_percent=similarity_percent,
                    card_size=card_size,
                    cache_key=cache_key,
                    is_active=True
                )
                card.card_hash = cls._calculate_card_hash(rendered_card)
                card.save()
                created = True

        return card, created

    @classmethod
    def bulk_create_or_update_cards(
            cls,
            cards_data: List[Tuple],
            batch_size: int = 100
    ) -> Dict[str, int]:
        """
        Массовое создание или обновление карточек игр.
        Использует стратегию: удалить все существующие + создать новые.
        Гарантирует отсутствие дубликатов.

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

        if not cards_data:
            return stats

        # Генерируем все cache_key для пакета
        cards_to_create = []
        cache_keys_to_process = []

        for data in cards_data:
            try:
                game, rendered_card, show_similarity, similarity_percent, card_size, related_data = data

                cache_key = cls._generate_key(
                    game.id, show_similarity, similarity_percent, card_size
                )

                card_hash = cls._calculate_card_hash(rendered_card)

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
                    show_similarity=show_similarity,
                    similarity_percent=similarity_percent,
                    card_size=card_size,
                    cache_key=cache_key,
                    card_hash=card_hash,
                    is_active=True
                )

                cards_to_create.append(card)
                cache_keys_to_process.append(cache_key)

            except Exception as e:
                logger.error(f"Error preparing card: {str(e)}")
                stats['errors'] += 1

        if not cards_to_create:
            return stats

        from django.db import transaction

        try:
            with transaction.atomic():
                # Удаляем ВСЕ существующие карточки с этими ключами
                deleted_count = cls.objects.filter(
                    cache_key__in=cache_keys_to_process
                ).delete()[0]

                stats['updated'] = deleted_count

                # Создаем новые карточки
                created_objects = cls.objects.bulk_create(
                    cards_to_create,
                    batch_size=batch_size
                )
                stats['created'] = len(created_objects)

        except Exception as e:
            logger.error(f"Bulk operation failed: {str(e)}", exc_info=True)

            # Fallback: создаем по одной
            stats['created'] = 0
            stats['updated'] = 0
            stats['errors'] = 0

            for card in cards_to_create:
                try:
                    _, created = cls.get_or_create_card(
                        game=card.game,
                        rendered_card=card.rendered_card,
                        show_similarity=card.show_similarity,
                        similarity_percent=card.similarity_percent,
                        card_size=card.card_size,
                        genres=card.genres_json,
                        platforms=card.platforms_json,
                        perspectives=card.perspectives_json,
                        keywords=card.keywords_json,
                        themes=card.themes_json,
                        game_modes=card.game_modes_json
                    )

                    if created:
                        stats['created'] += 1
                    else:
                        stats['updated'] += 1

                except Exception as e2:
                    logger.error(f"Failed to save card: {str(e2)}")
                    stats['errors'] += 1

        return stats

    def generate_cache_key(self) -> str:
        """
        Генерирует уникальный ключ кэша для конфигурации карточки.

        Ключ зависит от:
        - ID игры (game_id)
        - Размера карточки (card_size)
        - Наличия блока схожести (show_similarity)

        Процент схожести НЕ влияет на ключ, так как не меняет структуру HTML.
        """
        key_data = {
            'game_id': self.game_id,
            'card_size': self.card_size,
            'has_similarity': self.show_similarity,  # Только булево значение!
            'version': 'v4'  # Увеличена версия для инвалидации старых ключей
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
        genres_json = related_data.get('genres', [])
        platforms_json = related_data.get('platforms', [])
        perspectives_json = related_data.get('perspectives', [])
        keywords_json = related_data.get('keywords', [])
        themes_json = related_data.get('themes', [])
        game_modes_json = related_data.get('game_modes', [])

        cache_key = cls._generate_key(
            game.id, show_similarity, similarity_percent, card_size
        )

        # Удаляем любые существующие карточки с таким ключом
        cls.objects.filter(cache_key=cache_key).delete()

        card = cls(
            game=game,
            rendered_card=rendered_card,
            game_name=game.name,
            game_rating=getattr(game, 'rating', None),
            game_cover_url=getattr(game, 'cover_url', None),
            game_type=getattr(game, 'game_type', None),
            genres_json=genres_json,
            platforms_json=platforms_json,
            perspectives_json=perspectives_json,
            keywords_json=keywords_json,
            themes_json=themes_json,
            game_modes_json=game_modes_json,
            show_similarity=show_similarity,
            similarity_percent=similarity_percent,
            card_size=card_size,
            cache_key=cache_key,
            is_active=True
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
        """
        Генерирует ключ кэша для карточки игры.

        Args:
            game_id: ID игры
            show_similarity: ВЛИЯЕТ НА КЛЮЧ - определяет наличие блока схожести в HTML
            similarity_percent: НЕ ВЛИЯЕТ НА КЛЮЧ - только значение для подстановки
            card_size: ВЛИЯЕТ НА КЛЮЧ - размер карточки меняет верстку

        Returns:
            Уникальный ключ кэша
        """
        key_data = {
            'game_id': game_id,
            'card_size': card_size,
            'has_similarity': show_similarity,  # Только булево значение!
            'version': 'v4'
        }

        key_str = json.dumps(key_data, sort_keys=True)
        return f"game_card_{hashlib.md5(key_str.encode()).hexdigest()}"