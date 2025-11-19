from django.contrib import admin
from .models import Game, Genre, Platform, Keyword, KeywordCategory


@admin.register(KeywordCategory)
class KeywordCategoryAdmin(admin.ModelAdmin):
    list_display = ['name', 'description']
    search_fields = ['name']


@admin.register(Keyword)
class KeywordAdmin(admin.ModelAdmin):
    list_display = ['name', 'category', 'igdb_id']
    list_filter = ['category']
    search_fields = ['name']
    list_editable = ['category']


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id']
    search_fields = ['name']


@admin.register(Platform)
class PlatformAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id']
    search_fields = ['name']


@admin.register(Game)
class GameAdmin(admin.ModelAdmin):
    list_display = ['name', 'rating', 'rating_count', 'first_release_date', 'get_genres', 'get_platforms',
                    'get_keywords_count']
    list_filter = ['genres', 'platforms', 'first_release_date']
    search_fields = ['name']
    readonly_fields = ['igdb_id', 'display_genres', 'display_platforms', 'display_keywords']

    # УБИРАЕМ INLINE - они тормозят!
    # exclude = ['genres', 'platforms', 'keywords']

    fieldsets = (
        ('Основная информация', {
            'fields': (
            'name', 'igdb_id', 'summary', 'storyline', 'rating', 'rating_count', 'first_release_date', 'cover_url')
        }),
        ('Связи (только просмотр)', {
            'fields': ('display_genres', 'display_platforms', 'display_keywords'),
            'classes': ('collapse',)
        }),
    )

    def display_genres(self, obj):
        return ", ".join([genre.name for genre in obj.genres.all()])

    display_genres.short_description = 'Жанры'

    def display_platforms(self, obj):
        return ", ".join([platform.name for platform in obj.platforms.all()])

    display_platforms.short_description = 'Платформы'

    def display_keywords(self, obj):
        return ", ".join([keyword.name for keyword in obj.keywords.all()])

    display_keywords.short_description = 'Ключевые слова'

    def get_genres(self, obj):
        return ", ".join([genre.name for genre in obj.genres.all()[:3]])  # Только первые 3

    get_genres.short_description = 'Жанры'

    def get_platforms(self, obj):
        return ", ".join([platform.name for platform in obj.platforms.all()[:3]])  # Только первые 3

    get_platforms.short_description = 'Платформы'

    def get_keywords_count(self, obj):
        return obj.keywords.count()

    get_keywords_count.short_description = 'Ключевых слов'

    # Оптимизируем запросы
    def get_queryset(self, request):
        return super().get_queryset(request).prefetch_related('genres', 'platforms', 'keywords')