"""Optimized models for game database."""

from django.db import models
from django.utils.translation import gettext_lazy as _
from django.utils import timezone
from functools import lru_cache
from typing import Set, List, Dict, Optional, Tuple


# ===== GAME TYPE ENUMS =====
class GameTypeEnum:
    """Enum for predefined game types from IGDB with optimized lookup."""

    # Main games
    MAIN_GAME = 0
    STANDALONE_EXPANSION = 4
    SEASON = 7
    REMAKE = 8
    REMASTER = 9
    EXPANDED_GAME = 10

    # Non-main games
    DLC_ADDON = 1
    EXPANSION = 2
    BUNDLE = 3
    MOD = 5
    EPISODE = 6
    PORT = 11
    FORK = 12
    PACK_ADDON = 13
    UPDATE = 14

    # Django choices
    CHOICES = [
        (MAIN_GAME, _('Main game')),
        (STANDALONE_EXPANSION, _('Standalone expansion')),
        (SEASON, _('Season')),
        (REMAKE, _('Remake')),
        (REMASTER, _('Remaster')),
        (EXPANDED_GAME, _('Expanded game')),
        (DLC_ADDON, _('DLC/Addon')),
        (EXPANSION, _('Expansion')),
        (BUNDLE, _('Bundle')),
        (MOD, _('Mod')),
        (EPISODE, _('Episode')),
        (PORT, _('Port')),
        (FORK, _('Fork')),
        (PACK_ADDON, _('Pack / Addon')),
        (UPDATE, _('Update')),
    ]

    # Precomputed lookup tables
    _TYPE_INFO = {
        # Main games
        MAIN_GAME: {'name': 'main_game', 'is_primary': True, 'flag': None},
        STANDALONE_EXPANSION: {'name': 'standalone_expansion', 'is_primary': True, 'flag': 'is_standalone_expansion'},
        SEASON: {'name': 'season', 'is_primary': True, 'flag': 'is_season'},
        REMAKE: {'name': 'remake', 'is_primary': True, 'flag': 'is_remake'},
        REMASTER: {'name': 'remaster', 'is_primary': True, 'flag': 'is_remaster'},
        EXPANDED_GAME: {'name': 'expanded_game', 'is_primary': True, 'flag': 'is_expanded_game'},

        # Non-main games
        DLC_ADDON: {'name': 'dlc_addon', 'is_primary': False, 'flag': 'is_dlc'},
        EXPANSION: {'name': 'expansion', 'is_primary': False, 'flag': 'is_expansion'},
        BUNDLE: {'name': 'bundle', 'is_primary': False, 'flag': 'is_bundle_component'},
        MOD: {'name': 'mod', 'is_primary': False, 'flag': 'is_mod'},
        EPISODE: {'name': 'episode', 'is_primary': False, 'flag': 'is_episode'},
        PORT: {'name': 'port', 'is_primary': False, 'flag': 'is_port'},
        FORK: {'name': 'fork', 'is_primary': False, 'flag': 'is_fork'},
        PACK_ADDON: {'name': 'pack_addon', 'is_primary': False, 'flag': 'is_pack_addon'},
        UPDATE: {'name': 'update', 'is_primary': False, 'flag': 'is_update'},
    }

    _PRIMARY_GAME_TYPES = {MAIN_GAME, STANDALONE_EXPANSION, SEASON, REMAKE, REMASTER, EXPANDED_GAME}
    _NON_PRIMARY_GAME_TYPES = {DLC_ADDON, EXPANSION, BUNDLE, MOD, EPISODE, PORT, FORK, PACK_ADDON, UPDATE}

    _NAME_TO_ID = {
        'main_game': MAIN_GAME,
        'standalone_expansion': STANDALONE_EXPANSION,
        'season': SEASON,
        'remake': REMAKE,
        'remaster': REMASTER,
        'expanded_game': EXPANDED_GAME,
        'dlc_addon': DLC_ADDON,
        'expansion': EXPANSION,
        'bundle': BUNDLE,
        'mod': MOD,
        'episode': EPISODE,
        'port': PORT,
        'fork': FORK,
        'pack_addon': PACK_ADDON,
        'update': UPDATE,
    }

    @classmethod
    @lru_cache(maxsize=32)
    def get_type_info(cls, game_type_id: int) -> Dict:
        """Get type information by ID with caching."""
        return cls._TYPE_INFO.get(game_type_id, {
            'name': f'unknown_{game_type_id}',
            'is_primary': False,
            'flag': None,
        })

    @classmethod
    @lru_cache(maxsize=32)
    def get_name(cls, game_type_id: int) -> str:
        """Get type name by ID with caching."""
        info = cls.get_type_info(game_type_id)
        return info['name']

    @classmethod
    @lru_cache(maxsize=32)
    def is_primary(cls, game_type_id: int) -> bool:
        """Check if game type is primary with caching."""
        return game_type_id in cls._PRIMARY_GAME_TYPES

    @classmethod
    @lru_cache(maxsize=32)
    def get_id_by_name(cls, type_name: str) -> Optional[int]:
        """Get ID by type name with caching."""
        return cls._NAME_TO_ID.get(type_name)

    @classmethod
    def get_all_flags(cls) -> List[str]:
        """Get all flag names."""
        return [info['flag'] for info in cls._TYPE_INFO.values() if info['flag']]


# ===== OPTIMIZED GAME MANAGER =====
class GameManager(models.Manager):
    """Optimized custom manager for Game model."""

    def get_home_page_games_optimized(self, limit=12):
        """Оптимизированный метод для получения игр главной страницы."""
        from django.db.models import Prefetch

        # Создаем prefetch объекты
        genre_prefetch = Prefetch('genres', queryset=Genre.objects.only('id', 'name'))
        platform_prefetch = Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug'))
        perspective_prefetch = Prefetch('player_perspectives',
                                        queryset=PlayerPerspective.objects.only('id', 'name'))

        return self.prefetch_related(
            genre_prefetch,
            platform_prefetch,
            perspective_prefetch
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        )

    def get_home_page_data(self, limit: int = 12):
        """Оптимизированный запрос для данных главной страницы."""
        # Создаем базовый prefetch для игр
        game_prefetch = Prefetch('genres', queryset=Genre.objects.only('id', 'name'))
        platform_prefetch = Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug'))
        perspective_prefetch = Prefetch('player_perspectives',
                                        queryset=PlayerPerspective.objects.only('id', 'name'))

        # Популярные игры - кэшируем ID
        popular_game_ids = cache.get('home_popular_game_ids')
        if not popular_game_ids:
            popular_game_ids = list(self.filter(
                rating_count__gt=20,
                rating__gte=3.0,
                game_type__in=GameTypeEnum._PRIMARY_GAME_TYPES
            ).order_by('-rating_count', '-rating').values_list('id', flat=True)[:limit])
            cache.set('home_popular_game_ids', popular_game_ids, 3600)  # 1 час

        # Недавние игры - кэшируем ID
        recent_game_ids = cache.get('home_recent_game_ids')
        if not recent_game_ids:
            two_years_ago = timezone.now() - timezone.timedelta(days=730)
            recent_game_ids = list(self.filter(
                first_release_date__gte=two_years_ago,
                game_type__in=GameTypeEnum._PRIMARY_GAME_TYPES
            ).order_by('-first_release_date').values_list('id', flat=True)[:limit])
            cache.set('home_recent_game_ids', recent_game_ids, 1800)  # 30 минут

        # Загружаем игры с prefetch
        all_game_ids = set(popular_game_ids + recent_game_ids)
        games_dict = {}

        if all_game_ids:
            games_with_prefetch = self.filter(id__in=all_game_ids).prefetch_related(
                game_prefetch,
                platform_prefetch,
                perspective_prefetch
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            )

            for game in games_with_prefetch:
                games_dict[game.id] = game

        # Собираем результаты в правильном порядке
        popular_games = [games_dict.get(game_id) for game_id in popular_game_ids
                         if games_dict.get(game_id)]
        recent_games = [games_dict.get(game_id) for game_id in recent_game_ids
                        if games_dict.get(game_id)]

        return {
            'popular_games': popular_games,
            'recent_games': recent_games,
        }

    def bulk_prefetch_for_list(self, game_ids: List[int]) -> models.QuerySet:
        """Батчинговый prefetch для списка игр."""
        return self.filter(id__in=game_ids).prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('keywords', queryset=Keyword.objects.select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            )),
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type',
            '_cached_genre_count', '_cached_keyword_count',
            '_cached_platform_count', '_cached_developer_count'
        )

    def get_platform_ids_for_games(self, game_ids: List[int]) -> Dict[int, List[int]]:
        """Получает ID платформ для списка игр одним запросом."""
        from django.db.models import F

        # Один запрос для всех связей игр с платформами
        mappings = self.filter(id__in=game_ids).values_list(
            'id', 'platforms__id'
        ).distinct()

        # Группируем по игре
        result = {}
        for game_id, platform_id in mappings:
            if game_id not in result:
                result[game_id] = []
            if platform_id:
                result[game_id].append(platform_id)

        return result

    def bulk_update_wiki_descriptions(self, descriptions_dict: Dict[int, str], batch_size: int = 1000) -> int:
        """Mass update wiki descriptions with optimized batch processing."""
        if not descriptions_dict:
            return 0

        from django.db import transaction
        from django.db.models import Case, When, Value

        game_ids = list(descriptions_dict.keys())
        updated_count = 0
        total_batches = (len(game_ids) + batch_size - 1) // batch_size

        for i in range(0, len(game_ids), batch_size):
            batch_ids = game_ids[i:i + batch_size]

            # Build CASE expressions
            whens = [
                When(id=game_id, then=Value(description))
                for game_id in batch_ids
                if (description := descriptions_dict.get(game_id)) is not None
            ]

            if not whens:
                continue

            with transaction.atomic():
                result = self.filter(id__in=batch_ids).update(
                    wiki_description=Case(*whens, default='wiki_description')
                )
                updated_count += result

            # Progress indicator for large batches
            if total_batches > 1 and (i // batch_size) % 10 == 0:
                print(f"Processed {min(i + batch_size, len(game_ids))}/{len(game_ids)} games...")

        return updated_count

    @lru_cache(maxsize=128)
    def get_games_for_wiki_import(self, empty_wiki: bool = False,
                                  empty_all: bool = False,
                                  has_summary: bool = False,
                                  limit: Optional[int] = None,
                                  offset: int = 0) -> models.QuerySet:
        """Get games for wiki import with caching."""
        queryset = self.all()

        # Apply filters
        if empty_wiki:
            queryset = queryset.filter(wiki_description__isnull=True)
        if empty_all:
            queryset = queryset.filter(
                wiki_description__isnull=True,
                rawg_description__isnull=True,
                summary__isnull=True
            )
        if has_summary:
            queryset = queryset.filter(summary__isnull=False)

        # Order for consistent pagination
        queryset = queryset.order_by('id')

        if limit:
            return queryset[offset:offset + limit]

        return queryset.values('id', 'name')

    @lru_cache(maxsize=1)
    def count_games_without_wiki(self) -> int:
        """Count games without wiki description with caching."""
        return self.filter(wiki_description__isnull=True).count()

    def get_chunked_games(self, chunk_size: int = 100, **filters) -> models.QuerySet:
        """Generator for chunked game retrieval."""
        offset = 0

        while True:
            games_chunk = list(self.get_games_for_wiki_import(
                limit=chunk_size,
                offset=offset,
                **filters
            ))

            if not games_chunk:
                break

            yield games_chunk
            offset += chunk_size

    def get_games_without_wiki(self, limit: Optional[int] = None) -> models.QuerySet:
        """Get games without wiki description."""
        queryset = self.filter(wiki_description__isnull=True)
        if limit:
            queryset = queryset[:limit]
        return queryset

    def get_all_for_wiki_import(self, chunk_size: int = 100) -> models.QuerySet:
        """Generator for chunked game retrieval."""
        games = self.all().order_by('id').values('id', 'name')
        total = games.count()

        for i in range(0, total, chunk_size):
            yield games[i:i + chunk_size]

    # Type-specific filters with caching
    @lru_cache(maxsize=1)
    def primary(self) -> models.QuerySet:
        """Get primary games."""
        return self.filter(game_type__in=GameTypeEnum._PRIMARY_GAME_TYPES)

    @lru_cache(maxsize=1)
    def non_primary(self) -> models.QuerySet:
        """Get non-primary games."""
        return self.filter(game_type__in=GameTypeEnum._NON_PRIMARY_GAME_TYPES)

    @lru_cache(maxsize=32)
    def by_type(self, type_name: str) -> models.QuerySet:
        """Get games by type name."""
        type_id = GameTypeEnum.get_id_by_name(type_name)
        if type_id is not None:
            return self.filter(game_type=type_id)
        return self.none()

    # Precomputed type-specific queries
    @lru_cache(maxsize=1)
    def main_games(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.MAIN_GAME)

    @lru_cache(maxsize=1)
    def dlc_games(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.DLC_ADDON)

    @lru_cache(maxsize=1)
    def expansions(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.EXPANSION)

    @lru_cache(maxsize=1)
    def standalone_expansions(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.STANDALONE_EXPANSION)

    @lru_cache(maxsize=1)
    def remakes(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.REMAKE)

    @lru_cache(maxsize=1)
    def remasters(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.REMASTER)

    @lru_cache(maxsize=1)
    def bundles(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.BUNDLE)

    @lru_cache(maxsize=1)
    def mods(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.MOD)

    @lru_cache(maxsize=1)
    def episodes(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.EPISODE)

    @lru_cache(maxsize=1)
    def seasons(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.SEASON)

    @lru_cache(maxsize=1)
    def ports(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.PORT)

    @lru_cache(maxsize=1)
    def forks(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.FORK)

    @lru_cache(maxsize=1)
    def pack_addons(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.PACK_ADDON)

    @lru_cache(maxsize=1)
    def updates(self) -> models.QuerySet:
        return self.filter(game_type=GameTypeEnum.UPDATE)


# ===== OPTIMIZED GAME MODEL =====
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
    first_release_date = models.DateTimeField(blank=True, null=True, db_index=True)

    # Many-to-many relationships
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
            # Существующие индексы...
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

            # НОВЫЕ ИНДЕКСЫ ДЛЯ ГЛАВНОЙ СТРАНИЦЫ:

            # Для популярных игр (фильтрация и сортировка)
            models.Index(fields=['rating_count', 'rating']),
            models.Index(fields=['rating_count', '-rating']),
            models.Index(fields=['rating', '-rating_count']),

            # Для недавних игр (фильтрация по дате и типу)
            models.Index(fields=['-first_release_date', 'game_type']),
            models.Index(fields=['game_type', '-first_release_date']),

            # Составные индексы для оптимизации фильтров главной страницы
            models.Index(fields=['rating_count', 'game_type', '-rating']),
            models.Index(fields=['first_release_date', 'game_type']),

            # Индекс для быстрого подсчета
            models.Index(fields=['game_type', 'rating_count']),

            models.Index(fields=['-date_added']),  # Для сортировки новых игр
        ]

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
        return self.keywords.filter(category__name='Gameplay')

    @property
    @lru_cache(maxsize=1)
    def setting_keywords(self) -> models.QuerySet:
        """Get setting keywords with caching."""
        return self.keywords.filter(category__name='Setting')

    @property
    @lru_cache(maxsize=1)
    def genre_keywords(self) -> models.QuerySet:
        """Get genre keywords with caching."""
        return self.keywords.filter(category__name='Genre')

    @property
    @lru_cache(maxsize=1)
    def narrative_keywords(self) -> models.QuerySet:
        """Get narrative keywords with caching."""
        return self.keywords.filter(category__name='Narrative')

    @property
    @lru_cache(maxsize=1)
    def character_keywords(self) -> models.QuerySet:
        """Get character keywords with caching."""
        return self.keywords.filter(category__name='Characters')

    @property
    @lru_cache(maxsize=1)
    def technical_keywords(self) -> models.QuerySet:
        """Get technical keywords with caching."""
        return self.keywords.filter(category__name='Technical')

    @property
    @lru_cache(maxsize=1)
    def graphics_keywords(self) -> models.QuerySet:
        """Get graphics keywords with caching."""
        return self.keywords.filter(category__name='Graphics')

    @property
    @lru_cache(maxsize=1)
    def platform_keywords(self) -> models.QuerySet:
        """Get platform keywords with caching."""
        return self.keywords.filter(category__name='Platform')

    @property
    @lru_cache(maxsize=1)
    def multiplayer_keywords(self) -> models.QuerySet:
        """Get multiplayer keywords with caching."""
        return self.keywords.filter(category__name='Multiplayer')

    @property
    @lru_cache(maxsize=1)
    def achievement_keywords(self) -> models.QuerySet:
        """Get achievement keywords with caching."""
        return self.keywords.filter(category__name='Achievements')

    @property
    @lru_cache(maxsize=1)
    def audio_keywords(self) -> models.QuerySet:
        """Get audio keywords with caching."""
        return self.keywords.filter(category__name='Audio')

    @property
    @lru_cache(maxsize=1)
    def context_keywords(self) -> models.QuerySet:
        """Get context keywords with caching."""
        return self.keywords.filter(category__name='Context')

    @property
    @lru_cache(maxsize=1)
    def development_keywords(self) -> models.QuerySet:
        """Get development keywords with caching."""
        return self.keywords.filter(category__name='Development')

    @lru_cache(maxsize=32)
    def get_keywords_by_category(self, category_name: str) -> models.QuerySet:
        """Universal method to get keywords by category with caching."""
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

    def save(self, *args, **kwargs) -> None:
        """Override save to update cached counts on save."""
        is_new = self.pk is None
        super().save(*args, **kwargs)

        # Update cached counts for new games or if cache is stale
        if is_new or (self._cache_updated_at and
                      (timezone.now() - self._cache_updated_at).days > 1):
            self.update_cached_counts()


# ===== OPTIMIZED COMPANY MODEL =====
# В models.py - упростить модель Company (опционально):

class Company(models.Model):
    """Optimized Company model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=255, db_index=True)
    description = models.TextField(blank=True)
    website = models.URLField(blank=True, max_length=500)

    # Поля логотипов УДАЛЕНЫ

    start_date = models.DateTimeField(null=True, blank=True)
    changed_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Company'
        verbose_name_plural = 'Companies'
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['igdb_id']),
        ]

    def __str__(self) -> str:
        return self.name

    @property
    @lru_cache(maxsize=1)
    def logo_image_url(self) -> str:
        """Логотипы временно недоступны"""
        return ""

    @property
    @lru_cache(maxsize=1)
    def developed_games_count(self) -> int:
        """Count of developed games with caching."""
        return self.developed_games.count()

    @property
    @lru_cache(maxsize=1)
    def published_games_count(self) -> int:
        """Count of published games with caching."""
        return self.published_games.count()


# ===== OPTIMIZED SERIES MODEL =====
class Series(models.Model):
    """Optimized Series model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=255, db_index=True)
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)

    slug = models.SlugField(max_length=255, unique=True, blank=True, null=True, db_index=True)
    is_main_series = models.BooleanField(default=True, help_text="Основная серия (не спин-офф)")
    parent_series = models.ForeignKey(
        'self',
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='subseries',
        help_text="Родительская серия, если это подсерия",
        db_index=True
    )

    class Meta:
        verbose_name_plural = "Series"
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['slug']),
            models.Index(fields=['is_main_series']),
        ]

    def __str__(self) -> str:
        return self.name

    @property
    @lru_cache(maxsize=1)
    def game_count(self) -> int:
        """Количество игр в серии с кэшированием."""
        return self.games.count()

    @property
    @lru_cache(maxsize=1)
    def first_game(self) -> Optional['Game']:
        """Первая игра в серии с кэшированием."""
        return self.games.order_by('first_release_date', 'series_order').first()

    @property
    @lru_cache(maxsize=1)
    def latest_game(self) -> Optional['Game']:
        """Последняя игра в серии с кэшированием."""
        return self.games.order_by('-first_release_date').first()

    @property
    def years_active(self) -> str:
        """Годы активности серии."""
        years = self.games.exclude(first_release_date__isnull=True) \
            .values_list('first_release_date__year', flat=True) \
            .distinct()
        years_list = [y for y in years if y]
        if years_list:
            return f"{min(years_list)} - {max(years_list)}"
        return "N/A"

    def get_games_in_order(self) -> models.QuerySet:
        """Возвращает игры в правильном порядке."""
        return self.games.order_by('series_order', 'first_release_date')


# ===== OPTIMIZED SIMPLE MODELS =====
class Theme(models.Model):
    """Optimized Theme model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=100, db_index=True)

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
        ]

    def __str__(self) -> str:
        return self.name


class PlayerPerspective(models.Model):
    """Optimized PlayerPerspective model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=100, db_index=True)

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
        ]

    def __str__(self) -> str:
        return self.name


class GameMode(models.Model):
    """Optimized GameMode model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=100, db_index=True)

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
        ]

    def __str__(self) -> str:
        return self.name


# ===== OPTIMIZED KEYWORD CATEGORY =====
class KeywordCategory(models.Model):
    """Optimized KeywordCategory model."""

    name = models.CharField(max_length=100, unique=True, db_index=True)
    description = models.TextField(blank=True)

    class Meta:
        verbose_name_plural = "Keyword Categories"
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
        ]

    def __str__(self) -> str:
        return self.name

    @property
    @lru_cache(maxsize=1)
    def keyword_count(self) -> int:
        """Count of keywords in category with caching."""
        return self.keywords.count()


# ===== OPTIMIZED KEYWORD MODEL =====
class Keyword(models.Model):
    """Optimized Keyword model with efficient count caching."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=100, db_index=True)
    category = models.ForeignKey(
        KeywordCategory,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='keywords',
        db_index=True
    )

    cached_usage_count = models.IntegerField(default=0, db_index=True)
    last_count_update = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['cached_usage_count']),
            models.Index(fields=['name']),
            models.Index(fields=['category', 'cached_usage_count']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self) -> str:
        category_name = self.category.name if self.category else "No Category"
        return f"{self.name} ({category_name})"

    def update_cached_count(self, force: bool = False, async_update: bool = False) -> None:
        """
        Обновляет кэшированное значение с оптимизацией.
        """
        from django.conf import settings

        # ⬇⬇⬇ ДОБАВЛЯЕМ ЭТОТ КОД В САМОЕ НАЧАЛО МЕТОДА ⬇⬇⬇
        # Отключаем автоматическое обновление в DEBUG режиме
        if settings.DEBUG and not force:
            return
        # ⬆⬆⬆ ДОБАВЛЯЕМ ЭТОТ КОД В САМОЕ НАЧАЛО МЕТОДА ⬆⬆⬆

        # Оригинальный код продолжается...
        # Проверяем, нужно ли обновлять
        if not force and self.last_count_update:
            age_hours = (timezone.now() - self.last_count_update).total_seconds() / 3600
            if age_hours < 24:  # Обновляем раз в сутки
                return

        try:
            # Используем быстрый подсчет через базу данных
            actual_count = self.game_set.count()

            # Обновляем только если значение изменилось
            if self.cached_usage_count != actual_count:
                self.cached_usage_count = actual_count
                self.last_count_update = timezone.now()

                # Используем update() вместо save() для скорости
                Keyword.objects.filter(id=self.id).update(
                    cached_usage_count=actual_count,
                    last_count_update=self.last_count_update
                )

                # Обновляем локальный объект
                self.refresh_from_db(fields=['cached_usage_count', 'last_count_update'])
        except Exception as e:
            # Логируем ошибку, но не падаем
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error updating keyword cache for {self.id}: {str(e)}")

    @classmethod
    def bulk_update_cache_counts(cls, keyword_ids: List[int] = None, batch_size: int = 100) -> int:
        """
        Массовое обновление счетчиков для списка ключевых слов.

        Returns:
            Количество обновленных записей
        """
        from django.db.models import Count

        queryset = cls.objects.all()
        if keyword_ids:
            queryset = queryset.filter(id__in=keyword_ids)

        updated_count = 0

        # Разбиваем на батчи
        for i in range(0, queryset.count(), batch_size):
            batch = queryset[i:i + batch_size]

            # Аннотируем актуальные счетчики
            annotated = batch.annotate(
                actual_count=Count('game')
            )

            # Фильтруем те, что нужно обновить
            to_update = []
            for keyword in annotated:
                if keyword.cached_usage_count != keyword.actual_count:
                    keyword.cached_usage_count = keyword.actual_count
                    keyword.last_count_update = timezone.now()
                    to_update.append(keyword)

            # Массовое обновление
            if to_update:
                cls.objects.bulk_update(
                    to_update,
                    ['cached_usage_count', 'last_count_update'],
                    batch_size=batch_size
                )
                updated_count += len(to_update)

        return updated_count

    @property
    def usage_count(self) -> int:
        """Возвращает кэшированное или вычисленное значение."""
        # Если кэш пустой или устаревший, обновляем
        if self.cached_usage_count is None or (
                self.last_count_update and
                (timezone.now() - self.last_count_update).days > 7  # Раз в неделю проверяем
        ):
            self.update_cached_count()

        return self.cached_usage_count or 0

    def get_fresh_usage_count(self) -> int:
        """Всегда получает свежее значение (дорогая операция)."""
        return self.game_set.count()

    @property
    @lru_cache(maxsize=1)
    def popularity_score(self) -> float:
        """Популярность равна количеству использований с кэшированием."""
        return float(self.usage_count)

    def save(self, *args, **kwargs) -> None:
        """При сохранении обновляем кэшированный счетчик."""
        is_new = self.pk is None
        super().save(*args, **kwargs)

        if is_new:
            self.update_cached_count(force=True)


# ===== OPTIMIZED GENRE MODEL =====
class Genre(models.Model):
    """Optimized Genre model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=100, db_index=True)

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
        ]

    def __str__(self) -> str:
        return self.name

    @property
    @lru_cache(maxsize=1)
    def game_count(self) -> int:
        """Count of games in this genre with caching."""
        return self.game_set.count()


# ===== OPTIMIZED PLATFORM MODEL =====
class Platform(models.Model):
    """Optimized Platform model with slug precomputation."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=100, db_index=True)
    slug = models.SlugField(max_length=50, blank=True, null=True, db_index=True)

    # Precomputed platform slug mapping
    _PLATFORM_SLUGS = {
        # PlayStation
        'playstation 5': 'playstation5',
        'ps5': 'playstation5',
        'playstation 4': 'playstation4',
        'ps4': 'playstation4',
        'playstation 3': 'playstation3',
        'ps3': 'playstation3',
        'playstation 2': 'playstation2',
        'ps2': 'playstation2',
        'playstation 1': 'playstation',
        'ps1': 'playstation',
        'psp': 'psp',
        'vita': 'vita',

        # Xbox
        'xbox series x': 'xbox-series-x',
        'xbox series s': 'xbox-series-x',
        'xbox one': 'xbox-one',
        'xbox 360': 'xbox-360',
        'xbox': 'xbox',

        # Nintendo
        'nintendo switch': 'nintendo-switch',
        'wii u': 'wii-u',
        'wii': 'wii',
        'nintendo 3ds': 'nintendo-3ds',
        'nintendo ds': 'nintendo-ds',
        'game boy': 'game-boy',

        # PC
        'windows': 'windows',
        'linux': 'linux',
        'mac': 'macos',
        'macos': 'macos',

        # Mobile
        'android': 'android',
        'ios': 'ios',
    }

    _PLATFORM_DISPLAY_NAMES = {
        'pc (microsoft windows)': 'PC',
        'xbox series x|s': 'Xbox Series X/S',
        'super nintendo entertainment system': 'Super Nintendo',
        'commodore c64/128/max': 'Commodore 64',
        'sega mega drive/genesis': 'Sega Genesis',
        '3do interactive multiplayer': '3DO',
        'legacy mobile device': 'Mobile',
        'turbografx-16/pc engine cd': 'TurboGrafx-CD',
        'intellivision amico': 'Intellivision',
        'neo geo pocket color': 'Neo Geo Pocket',
        'wonderswan color': 'WonderSwan',
        'atari 8-bit': 'Atari 8-bit',
        'bbc microcomputer system': 'BBC Micro',
        'pc-9800 series': 'PC-9800',
        'pc-8800 series': 'PC-8800',
        'sharp mz-2200': 'Sharp MZ',
        'turbografx-16/pc engine': 'TurboGrafx-16',
        'nintendo entertainment system': 'NES',
        'meta quest': 'Meta Quest',
        'playstation vr2': 'PS VR2',
        'oculus quest': 'Oculus Quest',
        'blackberry os': 'BlackBerry',
        'sega cd': 'Sega CD',
        'oculus vr': 'Oculus',
        'windows': 'PC',
    }

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['slug']),
        ]

    def __str__(self) -> str:
        return self.name

    def save(self, *args, **kwargs) -> None:
        """Auto-generate slug on save if not provided."""
        if not self.slug:
            self.slug = self.get_platform_slug()
        super().save(*args, **kwargs)

    @lru_cache(maxsize=1)
    def get_platform_slug(self) -> str:
        """Возвращает slug для платформы с кэшированием."""
        name_lower = self.name.lower()

        for key, slug in self._PLATFORM_SLUGS.items():
            if key in name_lower:
                return slug

        return 'default'

    def game_count(self) -> int:
        """Количество игр на этой платформе."""
        return self.game_set.count()

    @property
    @lru_cache(maxsize=1)
    def icon_class(self) -> str:
        """Возвращает CSS класс для иконки платформы с кэшированием."""
        slug = self.get_platform_slug()
        return f"platform-icon platform-{slug}"

    @property
    @lru_cache(maxsize=1)
    def icon_url(self) -> str:
        """Возвращает URL иконки платформы с кэшированием."""
        slug = self.get_platform_slug()
        return f"/static/platforms/{slug}.png"

    @property
    @lru_cache(maxsize=1)
    def display_name(self) -> str:
        """Возвращает отображаемое имя платформы с кэшированием."""
        name_lower = self.name.lower()
        return self._PLATFORM_DISPLAY_NAMES.get(name_lower, self.name)


# ===== OPTIMIZED SIMILARITY MODELS =====
class GameSimilarityDetail(models.Model):
    """Optimized detailed cache for pre-calculated game similarity."""

    source_game = models.ForeignKey(
        'Game',
        on_delete=models.CASCADE,
        related_name='similarity_details_as_source',
        db_index=True
    )

    target_game = models.ForeignKey(
        'Game',
        on_delete=models.CASCADE,
        related_name='similarity_details_as_target',
        db_index=True
    )

    # Common elements
    common_genres = models.IntegerField(default=0)
    common_keywords = models.IntegerField(default=0)
    common_themes = models.IntegerField(default=0)
    common_developers = models.IntegerField(default=0)
    common_perspectives = models.IntegerField(default=0)
    common_game_modes = models.IntegerField(default=0)

    # Calculated similarity
    calculated_similarity = models.FloatField(default=0.0, db_index=True)

    # Metadata
    updated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        unique_together = ['source_game', 'target_game']
        indexes = [
            models.Index(fields=['source_game', 'calculated_similarity']),
            models.Index(fields=['calculated_similarity']),
            models.Index(fields=['updated_at']),
        ]
        verbose_name = "Game similarity detail"
        verbose_name_plural = "Game similarity details"

    def __str__(self) -> str:
        return f"{self.source_game.name} -> {self.target_game.name}: {self.calculated_similarity:.1f}%"

    @classmethod
    def get_similarity(cls, source_game_id: int, target_game_id: int) -> Optional[float]:
        """Get cached similarity score."""
        try:
            detail = cls.objects.get(source_game_id=source_game_id, target_game_id=target_game_id)
            return detail.calculated_similarity
        except cls.DoesNotExist:
            return None


class GameCountsCache(models.Model):
    """Optimized model for caching game element counts."""

    game = models.OneToOneField(
        'Game',
        on_delete=models.CASCADE,
        related_name='counts_cache',
        db_index=True
    )

    # Element counts
    genres_count = models.IntegerField(default=0)
    keywords_count = models.IntegerField(default=0)
    themes_count = models.IntegerField(default=0)
    developers_count = models.IntegerField(default=0)
    perspectives_count = models.IntegerField(default=0)
    game_modes_count = models.IntegerField(default=0)

    # Metadata
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    updated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        indexes = [
            models.Index(fields=['game']),
            models.Index(fields=['updated_at']),
        ]

    def __str__(self) -> str:
        return f"Counts for {self.game.name}"

    def update_counts(self) -> None:
        """Update all counts from the game."""
        self.genres_count = self.game.genres.count()
        self.keywords_count = self.game.keywords.count()
        self.themes_count = self.game.themes.count()
        self.developers_count = self.game.developers.count()
        self.perspectives_count = self.game.player_perspectives.count()
        self.game_modes_count = self.game.game_modes.count()
        self.save()


class GameSimilarityCache(models.Model):
    """Optimized cache for pre-calculated game similarity."""

    game1 = models.ForeignKey(
        Game,
        on_delete=models.CASCADE,
        related_name='similarity_as_source',
        db_index=True
    )

    game2 = models.ForeignKey(
        Game,
        on_delete=models.CASCADE,
        related_name='similarity_as_target',
        db_index=True
    )

    similarity_score = models.FloatField(db_index=True)
    calculated_at = models.DateTimeField(auto_now=True, db_index=True)

    class Meta:
        unique_together = ['game1', 'game2']
        indexes = [
            models.Index(fields=['game1', '-similarity_score']),
            models.Index(fields=['game2', '-similarity_score']),
            models.Index(fields=['calculated_at']),
        ]
        verbose_name_plural = "Game similarity cache"

    def __str__(self) -> str:
        return f"{self.game1} -> {self.game2}: {self.similarity_score}%"

    @classmethod
    def get_top_similar(cls, game_id: int, limit: int = 10) -> List[Tuple['Game', float]]:
        """Get top similar games from cache."""
        return list(cls.objects.filter(game1_id=game_id)
                    .select_related('game2')
                    .order_by('-similarity_score')[:limit]
                    .values_list('game2', 'similarity_score'))


class Screenshot(models.Model):
    """Optimized Screenshot model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    game = models.ForeignKey(
        Game,
        on_delete=models.CASCADE,
        related_name='screenshots',
        db_index=True
    )

    image_url = models.URLField(max_length=500)
    width = models.IntegerField(default=1920)
    height = models.IntegerField(default=1080)

    is_primary = models.BooleanField(default=False, db_index=True)
    caption = models.CharField(max_length=255, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ['-is_primary', 'id']
        indexes = [
            models.Index(fields=['game', 'is_primary']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self) -> str:
        return f"Screenshot {self.igdb_id} for {self.game.name}"
