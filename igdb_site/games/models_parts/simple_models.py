"""Simple models with minimal functionality."""

from django.db import models
from functools import lru_cache


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