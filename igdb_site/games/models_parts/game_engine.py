"""Game Engine model."""

from django.db import models
from functools import lru_cache


class GameEngine(models.Model):
    """Game engine model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=255, db_index=True)
    description = models.TextField(blank=True, null=True)
    logo_url = models.URLField(blank=True, null=True, max_length=500)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = 'Game engine'
        verbose_name_plural = 'Game engines'
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
            models.Index(fields=['igdb_id']),
        ]

    def __str__(self) -> str:
        return self.name

    @property
    @lru_cache(maxsize=1)
    def games_count(self) -> int:
        """Count of games using this engine with caching."""
        return self.game_set.count()