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

    def __str__(self):
        return self.name


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