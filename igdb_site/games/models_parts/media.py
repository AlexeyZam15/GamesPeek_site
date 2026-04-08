"""Media models like screenshots."""

from django.db import models


class Screenshot(models.Model):
    game = models.ForeignKey('Game', on_delete=models.CASCADE, related_name='screenshots', db_index=True)
    url = models.URLField(max_length=500)
    w = models.SmallIntegerField(default=1920)
    h = models.SmallIntegerField(default=1080)
    primary = models.BooleanField(default=False, db_index=True)

    class Meta:
        ordering = ['-primary', 'id']
        indexes = [
            models.Index(fields=['game', 'primary']),
        ]


    def __str__(self) -> str:
        return f"Screenshot {self.id} for {self.game.name}"