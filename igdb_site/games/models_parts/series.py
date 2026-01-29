"""Series model."""

from django.db import models
from functools import lru_cache
from typing import Optional


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