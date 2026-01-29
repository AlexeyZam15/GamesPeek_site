"""Media models like screenshots."""

from django.db import models


class Screenshot(models.Model):
    """Optimized Screenshot model."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    game = models.ForeignKey(
        'Game',
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