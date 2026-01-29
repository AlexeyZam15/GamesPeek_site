"""Company model."""

from django.db import models
from functools import lru_cache


class Company(models.Model):
    """Optimized Company model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=255, db_index=True)
    description = models.TextField(blank=True)
    website = models.URLField(blank=True, max_length=500)

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