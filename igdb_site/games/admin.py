from django.contrib import admin
from .models import Game, Genre, Platform, Keyword, KeywordCategory


@admin.register(KeywordCategory)
class KeywordCategoryAdmin(admin.ModelAdmin):
    list_display = ['name', 'keywords_count', 'description']
    search_fields = ['name']

    def keywords_count(self, obj):
        return obj.keywords.count()

    keywords_count.short_description = 'Кол-во ключевых слов'


@admin.register(Keyword)
class KeywordAdmin(admin.ModelAdmin):
    list_display = ['name', 'category', 'usage_count', 'popularity_level', 'igdb_id']
    list_filter = ['category', 'usage_count']
    search_fields = ['name']
    list_editable = ['category']
    list_per_page = 50
    ordering = ['-usage_count']  # Сортировка по популярности

    def popularity_level(self, obj):
        return obj.popularity_level

    popularity_level.short_description = 'Popularity'


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id', 'games_count']
    search_fields = ['name']

    def games_count(self, obj):
        return obj.game_set.count()

    games_count.short_description = 'Кол-во игр'


@admin.register(Platform)
class PlatformAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id', 'games_count']
    search_fields = ['name']

    def games_count(self, obj):
        return obj.game_set.count()

    games_count.short_description = 'Кол-во игр'


@admin.register(Game)
class GameAdmin(admin.ModelAdmin):
    list_display = ['name', 'rating', 'rating_count', 'first_release_date', 'get_genres', 'get_platforms',
                    'get_keywords_count']
    list_filter = ['genres', 'platforms', 'first_release_date',
                   'keywords__category']  # Добавил фильтр по категориям ключевых слов
    search_fields = ['name']
    readonly_fields = ['igdb_id', 'display_genres', 'display_platforms', 'display_keywords_by_category']

    fieldsets = (
        ('Основная информация', {
            'fields': (
            'name', 'igdb_id', 'summary', 'storyline', 'rating', 'rating_count', 'first_release_date', 'cover_url')
        }),
        ('Связи (только просмотр)', {
            'fields': ('display_genres', 'display_platforms', 'display_keywords_by_category'),
            'classes': ('collapse',)
        }),
    )

    def display_genres(self, obj):
        return ", ".join([genre.name for genre in obj.genres.all()])

    display_genres.short_description = 'Жанры'

    def display_platforms(self, obj):
        return ", ".join([platform.name for platform in obj.platforms.all()])

    display_platforms.short_description = 'Платформы'

    def display_keywords_by_category(self, obj):
        """Показывает ключевые слова сгруппированные по категориям"""
        categories = {}
        for keyword in obj.keywords.select_related('category').all():
            category_name = keyword.category.name if keyword.category else "Без категории"
            if category_name not in categories:
                categories[category_name] = []
            categories[category_name].append(keyword.name)

        result = []
        for category_name, keywords in sorted(categories.items()):
            result.append(f"<strong>{category_name}:</strong> {', '.join(keywords)}")

        return "<br>".join(result)

    display_keywords_by_category.short_description = 'Ключевые слова по категориям'
    display_keywords_by_category.allow_tags = True

    def get_genres(self, obj):
        return ", ".join([genre.name for genre in obj.genres.all()[:3]])

    get_genres.short_description = 'Жанры'

    def get_platforms(self, obj):
        return ", ".join([platform.name for platform in obj.platforms.all()[:3]])

    get_platforms.short_description = 'Платформы'

    def get_keywords_count(self, obj):
        return obj.keywords.count()

    get_keywords_count.short_description = 'Ключевых слов'

    # Оптимизируем запросы
    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related(
            'genres',
            'platforms',
            'keywords',
            'keywords__category'  # Добавил оптимизацию для категорий
        )