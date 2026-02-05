"""Optimized managers for game models."""

from django.db import models
from django.utils import timezone
from django.core.cache import cache
from django.db.models import Prefetch
from functools import lru_cache
from typing import List, Dict, Optional, Tuple

from .enums import GameTypeEnum
from .simple_models import Genre, Platform, PlayerPerspective
from .keywords import Keyword


class GameManager(models.Manager):
    """Optimized custom manager for Game model."""

    def get_home_page_games_optimized(self, limit=12):
        """Оптимизированный метод для получения игр главной страницы."""
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