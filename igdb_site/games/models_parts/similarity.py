"""Similarity models for game recommendations."""

from django.db import models
from typing import Optional, List, Tuple


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
            models.Index(fields=['source_game', 'common_genres', 'calculated_similarity']),
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
        'Game',
        on_delete=models.CASCADE,
        related_name='similarity_as_source',
        db_index=True
    )

    game2 = models.ForeignKey(
        'Game',
        on_delete=models.CASCADE,
        related_name='similarity_as_target',
        db_index=True
    )

    similarity_score = models.FloatField(db_index=True)
    calculated_at = models.DateTimeField(auto_now=True, db_index=True)

    # Добавляем поля для версии алгоритма и фильтров
    algorithm_version = models.IntegerField(default=7, db_index=True)
    cache_key = models.CharField(max_length=64, blank=True, null=True, db_index=True)

    class Meta:
        unique_together = ['game1', 'game2']
        indexes = [
            models.Index(fields=['game1', '-similarity_score']),
            models.Index(fields=['game2', '-similarity_score']),
            models.Index(fields=['calculated_at']),
            models.Index(fields=['algorithm_version']),
            models.Index(fields=['game1', 'algorithm_version']),
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
