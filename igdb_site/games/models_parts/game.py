"""Main Game model with all related functionality."""

from django.db import models
from django.utils import timezone
from functools import lru_cache
from typing import List, Dict, Optional, Tuple

from .enums import GameTypeEnum
from .managers import GameManager


class Game(models.Model):
    """Optimized Game model with cached properties."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=255, db_index=True)
    summary = models.TextField(blank=True, null=True)

    date_added = models.DateTimeField(
        auto_now_add=True,
        verbose_name="Дата добавления",
        db_index=True
    )

    last_analyzed_date = models.DateTimeField(
        null=True,
        blank=True,
        verbose_name="Дата последнего анализа"
    )

    game_type = models.IntegerField(
        choices=GameTypeEnum.CHOICES,
        null=True,
        blank=True,
        help_text="Game type from IGDB",
        db_index=True
    )

    parent_game = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='child_games',
        help_text="Parent game (for DLC, expansions, etc.)",
        db_index=True
    )

    version_parent = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='version_children',
        help_text="Version parent (base version of the game)",
        db_index=True
    )

    version_title = models.CharField(
        max_length=255,
        blank=True,
        null=True,
        help_text="Version title"
    )

    rawg_description = models.TextField(
        blank=True,
        null=True,
        help_text="Game description imported from RAWG.io",
        verbose_name="Description (RAWG)"
    )

    storyline = models.TextField(blank=True, null=True)
    rating = models.FloatField(blank=True, null=True, db_index=True)
    rating_count = models.IntegerField(default=0, db_index=True)
    first_release_date = models.DateTimeField(null=True, blank=True)

    # Many-to-many relationships - используем строковые ссылки для избежания циклических импортов
    genres = models.ManyToManyField('Genre', blank=True)
    platforms = models.ManyToManyField('Platform', blank=True)
    keywords = models.ManyToManyField('Keyword', blank=True)

    series = models.ManyToManyField(
        'Series',
        blank=True,
        related_name='games'
    )

    series_order = models.PositiveIntegerField(
        null=True,
        blank=True,
        help_text="Order in series (1, 2, 3...)"
    )

    developers = models.ManyToManyField(
        'Company',
        related_name='developed_games',
        blank=True
    )

    publishers = models.ManyToManyField(
        'Company',
        related_name='published_games',
        blank=True
    )

    themes = models.ManyToManyField('Theme', blank=True)
    player_perspectives = models.ManyToManyField('PlayerPerspective', blank=True)
    game_modes = models.ManyToManyField('GameMode', blank=True)

    cover_url = models.URLField(blank=True, null=True, max_length=500)

    wiki_description = models.TextField(
        blank=True,
        null=True,
        help_text="Game description from Wikipedia (Gameplay section)",
        verbose_name="Description (Wikipedia)"
    )

    # Cached counts for performance
    _cached_genre_count = models.IntegerField(null=True, blank=True, editable=False)
    _cached_keyword_count = models.IntegerField(null=True, blank=True, editable=False)
    _cached_platform_count = models.IntegerField(null=True, blank=True, editable=False)
    _cached_developer_count = models.IntegerField(null=True, blank=True, editable=False)
    _cache_updated_at = models.DateTimeField(null=True, blank=True, editable=False)

    # Optimized manager
    objects = GameManager()

    class Meta:
        ordering = ['-rating_count']
        indexes = [
            models.Index(fields=['-rating_count']),
            models.Index(fields=['-rating']),
            models.Index(fields=['name']),
            models.Index(fields=['-first_release_date']),
            models.Index(fields=['igdb_id']),
            models.Index(fields=['game_type']),
            models.Index(fields=['id', 'name']),
            models.Index(fields=['summary', 'storyline', 'rawg_description', 'wiki_description']),
            models.Index(fields=['_cached_genre_count']),
            models.Index(fields=['_cached_keyword_count']),
            models.Index(fields=['_cached_platform_count']),
            models.Index(fields=['_cached_developer_count']),
            models.Index(fields=['rating_count', 'rating']),
            models.Index(fields=['rating_count', '-rating']),
            models.Index(fields=['rating', '-rating_count']),
            models.Index(fields=['-first_release_date', 'game_type']),
            models.Index(fields=['game_type', '-first_release_date']),
            models.Index(fields=['rating_count', 'game_type', '-rating']),
            models.Index(fields=['first_release_date', 'game_type']),
            models.Index(fields=['game_type', 'rating_count']),
            models.Index(fields=['-date_added']),
        ]

    def save(self, *args, **kwargs):
        """Override save to fix naive dates and update cached counts."""
        # Исправляем наивные даты при сохранении
        if self.first_release_date and timezone.is_naive(self.first_release_date):
            # Предполагаем UTC (или ваш локальный часовой пояс)
            self.first_release_date = timezone.make_aware(
                self.first_release_date,
                timezone.get_current_timezone()  # или timezone.utc для UTC
            )

        is_new = self.pk is None
        super().save(*args, **kwargs)

        # Update cached counts for new games or if cache is stale
        if is_new or (self._cache_updated_at and
                      (timezone.now() - self._cache_updated_at).days > 1):
            self.update_cached_counts()

    def update_cached_counts(self, force: bool = False, async_update: bool = False) -> None:
        """
        Update all cached counts at once.

        Args:
            force: Принудительное обновление
            async_update: Обновление в фоне
        """
        from django.conf import settings

        # Отключаем автоматическое обновление в DEBUG режиме
        if getattr(settings, 'DISABLE_AUTO_CACHE_UPDATES', False) and not force:
            return

        # Если асинхронное обновление
        if async_update and hasattr(settings, 'CELERY_BROKER_URL'):
            from .tasks import update_game_cache_async
            update_game_cache_async.delay(self.id)
            return

        try:
            # Используем select_related/prefetch для быстрого подсчета
            game = Game.objects.filter(id=self.id).prefetch_related(
                'genres', 'keywords', 'platforms', 'developers'
            ).first()

            if not game:
                return

            # Подсчитываем через базу для скорости
            counts = {
                'genres': game.genres.count(),
                'keywords': game.keywords.count(),
                'platforms': game.platforms.count(),
                'developers': game.developers.count(),
            }

            # Проверяем, нужно ли обновлять
            needs_update = (
                    self._cached_genre_count != counts['genres'] or
                    self._cached_keyword_count != counts['keywords'] or
                    self._cached_platform_count != counts['platforms'] or
                    self._cached_developer_count != counts['developers']
            )

            if needs_update or force:
                self._cached_genre_count = counts['genres']
                self._cached_keyword_count = counts['keywords']
                self._cached_platform_count = counts['platforms']
                self._cached_developer_count = counts['developers']
                self._cache_updated_at = timezone.now()

                # Быстрое обновление через update()
                Game.objects.filter(id=self.id).update(
                    _cached_genre_count=counts['genres'],
                    _cached_keyword_count=counts['keywords'],
                    _cached_platform_count=counts['platforms'],
                    _cached_developer_count=counts['developers'],
                    _cache_updated_at=self._cache_updated_at
                )

                # Обновляем локальный объект
                self.refresh_from_db(fields=[
                    '_cached_genre_count', '_cached_keyword_count',
                    '_cached_platform_count', '_cached_developer_count',
                    '_cache_updated_at'
                ])
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error updating game cache for {self.id}: {str(e)}")

    @classmethod
    def bulk_update_cache_counts(cls, game_ids: List[int] = None, batch_size: int = 50) -> int:
        """
        Массовое обновление счетчиков для игр.

        Returns:
            Количество обновленных игр
        """
        from django.db.models import Count

        queryset = cls.objects.all()
        if game_ids:
            queryset = queryset.filter(id__in=game_ids)

        updated_count = 0

        for i in range(0, queryset.count(), batch_size):
            batch = queryset[i:i + batch_size]

            # Получаем все связанные счетчики одним запросом
            game_counts = {}

            # Считаем через агрегацию для скорости
            for game in batch:
                counts = {
                    'genres': game.genres.count(),
                    'keywords': game.keywords.count(),
                    'platforms': game.platforms.count(),
                    'developers': game.developers.count(),
                }
                game_counts[game.id] = counts

            # Определяем какие нужно обновить
            to_update = []
            for game in batch:
                counts = game_counts.get(game.id, {})

                if (game._cached_genre_count != counts.get('genres') or
                        game._cached_keyword_count != counts.get('keywords') or
                        game._cached_platform_count != counts.get('platforms') or
                        game._cached_developer_count != counts.get('developers')):
                    game._cached_genre_count = counts.get('genres', 0)
                    game._cached_keyword_count = counts.get('keywords', 0)
                    game._cached_platform_count = counts.get('platforms', 0)
                    game._cached_developer_count = counts.get('developers', 0)
                    game._cache_updated_at = timezone.now()
                    to_update.append(game)

            # Массовое обновление
            if to_update:
                cls.objects.bulk_update(
                    to_update,
                    [
                        '_cached_genre_count', '_cached_keyword_count',
                        '_cached_platform_count', '_cached_developer_count',
                        '_cache_updated_at'
                    ],
                    batch_size=batch_size
                )
                updated_count += len(to_update)

        return updated_count

    # Свойства для быстрого доступа с ленивым обновлением
    @property
    def cached_genre_count(self) -> int:
        """Get cached genre count with lazy update."""
        if self._cached_genre_count is None or (
                self._cache_updated_at and
                (timezone.now() - self._cache_updated_at).days > 7
        ):
            self.update_cached_counts()
        return self._cached_genre_count or 0

    @property
    def cached_keyword_count(self) -> int:
        """Get cached keyword count with lazy update."""
        if self._cached_keyword_count is None or (
                self._cache_updated_at and
                (timezone.now() - self._cache_updated_at).days > 7
        ):
            self.update_cached_counts()
        return self._cached_keyword_count or 0

    @property
    def cached_platform_count(self) -> int:
        """Get cached platform count with lazy update."""
        if self._cached_platform_count is None or (
                self._cache_updated_at and
                (timezone.now() - self._cache_updated_at).days > 7
        ):
            self.update_cached_counts()
        return self._cached_platform_count or 0

    @property
    def cached_developer_count(self) -> int:
        """Get cached developer count with lazy update."""
        if self._cached_developer_count is None or (
                self._cache_updated_at and
                (timezone.now() - self._cache_updated_at).days > 7
        ):
            self.update_cached_counts()
        return self._cached_developer_count or 0

    # ===== CACHED GAME TYPE PROPERTIES =====
    @property
    @lru_cache(maxsize=1)
    def game_type_info(self) -> Dict:
        """Get complete game type information with caching."""
        return GameTypeEnum.get_type_info(self.game_type) if self.game_type is not None else {}

    @property
    @lru_cache(maxsize=1)
    def game_type_name(self) -> str:
        """Get game type name with caching."""
        return GameTypeEnum.get_name(self.game_type) if self.game_type is not None else "No game type"

    @property
    @lru_cache(maxsize=1)
    def is_primary_game(self) -> bool:
        """Check if this is a primary game with caching."""
        return GameTypeEnum.is_primary(self.game_type) if self.game_type is not None else False

    @property
    def game_type_flag(self) -> Optional[str]:
        """Get game type flag name."""
        info = self.game_type_info
        return info.get('flag')

    # Individual type check properties with caching
    @property
    @lru_cache(maxsize=1)
    def is_main_game(self) -> bool:
        return self.game_type == GameTypeEnum.MAIN_GAME

    @property
    @lru_cache(maxsize=1)
    def is_standalone_expansion(self) -> bool:
        return self.game_type == GameTypeEnum.STANDALONE_EXPANSION

    @property
    @lru_cache(maxsize=1)
    def is_season(self) -> bool:
        return self.game_type == GameTypeEnum.SEASON

    @property
    @lru_cache(maxsize=1)
    def is_remake(self) -> bool:
        return self.game_type == GameTypeEnum.REMAKE

    @property
    @lru_cache(maxsize=1)
    def is_remaster(self) -> bool:
        return self.game_type == GameTypeEnum.REMASTER

    @property
    @lru_cache(maxsize=1)
    def is_expanded_game(self) -> bool:
        return self.game_type == GameTypeEnum.EXPANDED_GAME

    @property
    @lru_cache(maxsize=1)
    def is_dlc(self) -> bool:
        return self.game_type == GameTypeEnum.DLC_ADDON

    @property
    @lru_cache(maxsize=1)
    def is_expansion(self) -> bool:
        return self.game_type == GameTypeEnum.EXPANSION

    @property
    @lru_cache(maxsize=1)
    def is_bundle_component(self) -> bool:
        return self.game_type == GameTypeEnum.BUNDLE

    @property
    @lru_cache(maxsize=1)
    def is_mod(self) -> bool:
        return self.game_type == GameTypeEnum.MOD

    @property
    @lru_cache(maxsize=1)
    def is_episode(self) -> bool:
        return self.game_type == GameTypeEnum.EPISODE

    @property
    @lru_cache(maxsize=1)
    def is_port(self) -> bool:
        return self.game_type == GameTypeEnum.PORT

    @property
    @lru_cache(maxsize=1)
    def is_fork(self) -> bool:
        return self.game_type == GameTypeEnum.FORK

    @property
    @lru_cache(maxsize=1)
    def is_pack_addon(self) -> bool:
        return self.game_type == GameTypeEnum.PACK_ADDON

    @property
    @lru_cache(maxsize=1)
    def is_update(self) -> bool:
        return self.game_type == GameTypeEnum.UPDATE

    # ===== OPTIMIZED SERIES PROPERTIES =====
    @property
    def is_part_of_series(self) -> bool:
        """Check if game belongs to any series."""
        return self.series.exists()

    @property
    @lru_cache(maxsize=1)
    def main_series(self) -> Optional['Series']:
        """Get first (main) series of the game with caching."""
        return self.series.first()

    @property
    def display_series_info(self) -> str:
        """Display information about series."""
        if not self.series.exists():
            return ""

        series_names = list(self.series.values_list('name', flat=True))
        if len(series_names) == 1:
            info = series_names[0]
            if self.series_order:
                info += f" (Part {self.series_order})"
            return info
        else:
            info = ", ".join(series_names)
            if self.series_order:
                info += f" (Main series: Part {self.series_order})"
            return f"Series: {info}"

    def get_series_games(self, series: Optional['Series'] = None) -> models.QuerySet:
        """Get all games from the same series/series."""
        if series:
            return series.games.exclude(id=self.id).order_by('first_release_date')
        elif self.series.exists():
            main_series = self.series.first()
            return main_series.games.exclude(id=self.id).order_by('first_release_date')
        return Game.objects.none()

    def get_all_series_games(self) -> models.QuerySet:
        """Get all games from all series of this game."""
        from django.db.models import Q
        series_ids = list(self.series.values_list('id', flat=True))
        if series_ids:
            return Game.objects.filter(series__id__in=series_ids).exclude(id=self.id).distinct()
        return Game.objects.none()

    @property
    def series_count(self) -> int:
        """Number of series the game belongs to."""
        return self.series.count()

    # ===== OPTIMIZED COMPANY PROPERTIES =====
    @property
    @lru_cache(maxsize=1)
    def main_developer(self) -> Optional['Company']:
        """Main developer (first in list) with caching."""
        return self.developers.first()

    @property
    @lru_cache(maxsize=1)
    def main_publisher(self) -> Optional['Company']:
        """Main publisher (first in list) with caching."""
        return self.publishers.first()

    @property
    @lru_cache(maxsize=1)
    def developer_names(self) -> List[str]:
        """List of developer names with caching."""
        return list(self.developers.values_list('name', flat=True))

    @property
    @lru_cache(maxsize=1)
    def publisher_names(self) -> List[str]:
        """List of publisher names with caching."""
        return list(self.publishers.values_list('name', flat=True))

    # ===== OPTIMIZED THEME & PERSPECTIVE PROPERTIES =====
    @property
    @lru_cache(maxsize=1)
    def theme_names(self) -> List[str]:
        """List of themes with caching."""
        return list(self.themes.values_list('name', flat=True))

    @property
    @lru_cache(maxsize=1)
    def perspective_names(self) -> List[str]:
        """List of perspectives with caching."""
        return list(self.player_perspectives.values_list('name', flat=True))

    @property
    @lru_cache(maxsize=1)
    def game_mode_names(self) -> List[str]:
        """List of game modes with caching."""
        return list(self.game_modes.values_list('name', flat=True))

    # ===== OPTIMIZED KEYWORD CATEGORY PROPERTIES =====
    @property
    @lru_cache(maxsize=1)
    def gameplay_keywords(self) -> models.QuerySet:
        """Get gameplay keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Gameplay')

    @property
    @lru_cache(maxsize=1)
    def setting_keywords(self) -> models.QuerySet:
        """Get setting keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Setting')

    @property
    @lru_cache(maxsize=1)
    def genre_keywords(self) -> models.QuerySet:
        """Get genre keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Genre')

    @property
    @lru_cache(maxsize=1)
    def narrative_keywords(self) -> models.QuerySet:
        """Get narrative keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Narrative')

    @property
    @lru_cache(maxsize=1)
    def character_keywords(self) -> models.QuerySet:
        """Get character keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Characters')

    @property
    @lru_cache(maxsize=1)
    def technical_keywords(self) -> models.QuerySet:
        """Get technical keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Technical')

    @property
    @lru_cache(maxsize=1)
    def graphics_keywords(self) -> models.QuerySet:
        """Get graphics keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Graphics')

    @property
    @lru_cache(maxsize=1)
    def platform_keywords(self) -> models.QuerySet:
        """Get platform keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Platform')

    @property
    @lru_cache(maxsize=1)
    def multiplayer_keywords(self) -> models.QuerySet:
        """Get multiplayer keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Multiplayer')

    @property
    @lru_cache(maxsize=1)
    def achievement_keywords(self) -> models.QuerySet:
        """Get achievement keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Achievements')

    @property
    @lru_cache(maxsize=1)
    def audio_keywords(self) -> models.QuerySet:
        """Get audio keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Audio')

    @property
    @lru_cache(maxsize=1)
    def context_keywords(self) -> models.QuerySet:
        """Get context keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Context')

    @property
    @lru_cache(maxsize=1)
    def development_keywords(self) -> models.QuerySet:
        """Get development keywords with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name='Development')

    @lru_cache(maxsize=32)
    def get_keywords_by_category(self, category_name: str) -> models.QuerySet:
        """Universal method to get keywords by category with caching."""
        from .keywords import KeywordCategory
        return self.keywords.filter(category__name=category_name)

    # ===== OPTIMIZED DISPLAY METHODS =====
    @property
    def get_game_type_display(self) -> str:
        """Get display name for game type."""
        if self.game_type is None:
            return "No game type"
        return dict(GameTypeEnum.CHOICES).get(self.game_type, f"Unknown ({self.game_type})")

    @property
    def get_full_title(self) -> str:
        """Get full title with version info."""
        if self.version_title:
            return f"{self.name} - {self.version_title}"
        return self.name

    def __str__(self) -> str:
        type_suffix = f" [{self.get_game_type_display}]" if self.game_type is not None else ""
        return f"{self.name}{type_suffix}"