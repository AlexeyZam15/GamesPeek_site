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

    def __str__(self):
        category_name = self.category.name if self.category else "No Category"
        return f"{self.name} ({category_name})"


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
        return self.keywords.filter(category__name__icontains='gameplay')

    @property
    def setting_keywords(self):
        """Возвращает только ключевые слова сеттинга"""
        return self.keywords.filter(category__name__icontains='setting')