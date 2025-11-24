from django.db import models


class KeywordCategory(models.Model):
    """Категория ключевых слов (геймплей, сеттинг, etc.)"""
    name = models.CharField(max_length=100)
    description = models.TextField(blank=True)

    def __str__(self):
        return self.name

    class Meta:
        verbose_name_plural = "Keyword Categories"


class Keyword(models.Model):
    """Базовая модель ключевых слов из IGDB"""
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)
    category = models.ForeignKey(
        KeywordCategory,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='keywords'
    )
    usage_count = models.IntegerField(default=0)  # ← ДОБАВИТЬ ЭТО ПОЛЕ
    popularity_score = models.FloatField(default=0.0)  # ← И ЭТО

    def __str__(self):
        category_name = self.category.name if self.category else "No Category"
        return f"{self.name} ({category_name})"

    def update_popularity(self):
        """Обновляет счетчик использования и популярность"""
        self.usage_count = self.game_set.count()
        # Можно добавить более сложную формулу для популярности
        self.popularity_score = self.usage_count
        self.save()

    @property
    def popularity_level(self):
        """Возвращает уровень популярности (Low, Medium, High, Very High)"""
        if self.usage_count == 0:
            return "Unused"
        elif self.usage_count <= 5:
            return "Low"
        elif self.usage_count <= 20:
            return "Medium"
        elif self.usage_count <= 100:
            return "High"
        else:
            return "Very High"


class Genre(models.Model):
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)

    def __str__(self):
        return self.name


class Platform(models.Model):
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=100)
    slug = models.SlugField(max_length=50, blank=True, null=True)

    def __str__(self):
        return self.name

    def get_platform_slug(self):
        """Возвращает slug для платформы для использования в URL иконок"""
        name_lower = self.name.lower()

        # PlayStation
        if 'playstation 5' in name_lower or 'ps5' in name_lower:
            return 'playstation5'
        elif 'playstation 4' in name_lower or 'ps4' in name_lower:
            return 'playstation4'
        elif 'playstation 3' in name_lower or 'ps3' in name_lower:
            return 'playstation3'
        elif 'playstation 2' in name_lower or 'ps2' in name_lower:
            return 'playstation2'
        elif 'playstation 1' in name_lower or 'ps1' in name_lower:
            return 'playstation'
        elif 'psp' in name_lower:
            return 'psp'
        elif 'vita' in name_lower:
            return 'vita'

        # Xbox
        elif 'xbox series x' in name_lower or 'xbox series s' in name_lower:
            return 'xbox-series-x'
        elif 'xbox one' in name_lower:
            return 'xbox-one'
        elif 'xbox 360' in name_lower:
            return 'xbox-360'
        elif 'xbox' in name_lower:
            return 'xbox'

        # Nintendo
        elif 'nintendo switch' in name_lower:
            return 'nintendo-switch'
        elif 'wii u' in name_lower:
            return 'wii-u'
        elif 'wii' in name_lower:
            return 'wii'
        elif 'nintendo 3ds' in name_lower:
            return 'nintendo-3ds'
        elif 'nintendo ds' in name_lower:
            return 'nintendo-ds'
        elif 'game boy' in name_lower:
            return 'game-boy'

        # PC
        elif 'windows' in name_lower:
            return 'windows'
        elif 'linux' in name_lower:
            return 'linux'
        elif 'mac' in name_lower or 'macos' in name_lower:
            return 'macos'

        # Mobile
        elif 'android' in name_lower:
            return 'android'
        elif 'ios' in name_lower:
            return 'ios'

        else:
            return 'default'


class Game(models.Model):
    igdb_id = models.IntegerField(unique=True)
    name = models.CharField(max_length=255)
    summary = models.TextField(blank=True, null=True)
    storyline = models.TextField(blank=True, null=True)
    rating = models.FloatField(blank=True, null=True)
    rating_count = models.IntegerField(default=0)
    first_release_date = models.DateTimeField(blank=True, null=True)

    # Связи
    genres = models.ManyToManyField(Genre, blank=True)
    platforms = models.ManyToManyField(Platform, blank=True)
    keywords = models.ManyToManyField(Keyword, blank=True)  # Все ключевые слова

    cover_url = models.URLField(blank=True, null=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ['-rating_count']

    # Property для удобного доступа к ключевым словам по категориям
    @property
    def gameplay_keywords(self):
        """Возвращает только геймплейные ключевые слова"""
        return self.keywords.filter(category__name='Gameplay')

    @property
    def setting_keywords(self):
        """Возвращает только ключевые слова сеттинга"""
        return self.keywords.filter(category__name='Setting')

    @property
    def genre_keywords(self):
        """Возвращает только ключевые слова жанров"""
        return self.keywords.filter(category__name='Genre')

    @property
    def narrative_keywords(self):
        """Возвращает только нарративные ключевые слова"""
        return self.keywords.filter(category__name='Narrative')

    @property
    def character_keywords(self):
        """Возвращает только ключевые слова персонажей"""
        return self.keywords.filter(category__name='Characters')

    @property
    def technical_keywords(self):
        """Возвращает только технические ключевые слова"""
        return self.keywords.filter(category__name='Technical')

    @property
    def graphics_keywords(self):
        """Возвращает только ключевые слова графики"""
        return self.keywords.filter(category__name='Graphics')

    @property
    def platform_keywords(self):
        """Возвращает только ключевые слова платформ"""
        return self.keywords.filter(category__name='Platform')

    @property
    def multiplayer_keywords(self):
        """Возвращает только мультиплеерные ключевые слова"""
        return self.keywords.filter(category__name='Multiplayer')

    @property
    def achievement_keywords(self):
        """Возвращает только ключевые слова достижений"""
        return self.keywords.filter(category__name='Achievements')

    @property
    def audio_keywords(self):
        """Возвращает только аудио ключевые слова"""
        return self.keywords.filter(category__name='Audio')

    @property
    def context_keywords(self):
        """Возвращает только контекстные ключевые слова"""
        return self.keywords.filter(category__name='Context')

    @property
    def development_keywords(self):
        """Возвращает только ключевые слова разработки"""
        return self.keywords.filter(category__name='Development')

    def get_keywords_by_category(self, category_name):
        """Универсальный метод для получения ключевых слов по категории"""
        return self.keywords.filter(category__name=category_name)


class GameSimilarityCache(models.Model):
    """Кэш для предварительно рассчитанной схожести игр"""
    game1 = models.ForeignKey(Game, on_delete=models.CASCADE, related_name='similarity_as_source')
    game2 = models.ForeignKey(Game, on_delete=models.CASCADE, related_name='similarity_as_target')
    similarity_score = models.FloatField()
    calculated_at = models.DateTimeField(auto_now=True)

    class Meta:
        unique_together = ['game1', 'game2']
        indexes = [
            models.Index(fields=['game1', '-similarity_score']),
            models.Index(fields=['game2', '-similarity_score']),
        ]
        verbose_name_plural = "Game similarity cache"

    def __str__(self):
        return f"{self.game1} -> {self.game2}: {self.similarity_score}%"


class Screenshot(models.Model):
    """Скриншоты игр из IGDB"""
    igdb_id = models.IntegerField(unique=True)
    game = models.ForeignKey(Game, on_delete=models.CASCADE, related_name='screenshots')
    image_url = models.URLField()
    width = models.IntegerField(default=1920)
    height = models.IntegerField(default=1080)

    # Можно добавить дополнительные поля
    is_primary = models.BooleanField(default=False)
    caption = models.CharField(max_length=255, blank=True, null=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"Screenshot {self.igdb_id} for {self.game.name}"

    class Meta:
        ordering = ['-is_primary', 'id']
