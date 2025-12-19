# FILE: admin.py
# PATH: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\admin.py

from django.contrib import admin
from .models import (
    Game, GameSimilarityDetail, GameCountsCache, Company, Series,
    Theme, PlayerPerspective, GameMode, KeywordCategory, Keyword,
    Genre, Platform, GameSimilarityCache, Screenshot
)


# ===== BASIC ADMIN CONFIGURATIONS =====

@admin.register(Game)
class GameAdmin(admin.ModelAdmin):
    list_display = ['id', 'name', 'game_type', 'rating', 'first_release_date']
    list_filter = ['game_type', 'genres', 'platforms']
    search_fields = ['name', 'summary']
    filter_horizontal = ['genres', 'platforms', 'keywords', 'developers', 'publishers']
    raw_id_fields = ['parent_game', 'version_parent']

    fieldsets = (
        ('Основная информация', {
            'fields': ('name', 'igdb_id', 'summary', 'game_type', 'rating', 'rating_count', 'first_release_date')
        }),
        ('Связи', {
            'fields': ('parent_game', 'version_parent', 'version_title')
        }),
        ('Многие ко многим', {
            'fields': ('genres', 'platforms', 'keywords', 'developers', 'publishers', 'themes')
        }),
        ('Дополнительно', {
            'fields': ('storyline', 'rawg_description', 'cover_url')
        }),
    )


@admin.register(Company)
class CompanyAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id', 'website']
    search_fields = ['name', 'description']
    list_filter = []


@admin.register(Series)
class SeriesAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id', 'game_count', 'is_main_series']
    search_fields = ['name', 'description']
    filter_horizontal = []
    raw_id_fields = ['parent_series']


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id']
    search_fields = ['name']


@admin.register(Platform)
class PlatformAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id', 'game_count']
    search_fields = ['name']


@admin.register(Keyword)
class KeywordAdmin(admin.ModelAdmin):
    list_display = ['name', 'category', 'igdb_id', 'cached_usage_count']
    list_filter = ['category']
    search_fields = ['name']
    list_editable = ['category']


@admin.register(KeywordCategory)
class KeywordCategoryAdmin(admin.ModelAdmin):
    list_display = ['name', 'description']
    search_fields = ['name']


@admin.register(Theme)
class ThemeAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id']
    search_fields = ['name']


@admin.register(PlayerPerspective)
class PlayerPerspectiveAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id']
    search_fields = ['name']


@admin.register(GameMode)
class GameModeAdmin(admin.ModelAdmin):
    list_display = ['name', 'igdb_id']
    search_fields = ['name']


@admin.register(Screenshot)
class ScreenshotAdmin(admin.ModelAdmin):
    list_display = ['igdb_id', 'game', 'is_primary', 'width', 'height']
    list_filter = ['is_primary']
    search_fields = ['game__name', 'caption']
    raw_id_fields = ['game']


# ===== SIMILARITY/CACHE MODELS =====
# Эти модели редко редактируются вручную

@admin.register(GameSimilarityDetail)
class GameSimilarityDetailAdmin(admin.ModelAdmin):
    list_display = ['source_game', 'target_game', 'calculated_similarity', 'updated_at']
    search_fields = ['source_game__name', 'target_game__name']
    raw_id_fields = ['source_game', 'target_game']
    readonly_fields = ['updated_at']


@admin.register(GameSimilarityCache)
class GameSimilarityCacheAdmin(admin.ModelAdmin):
    list_display = ['game1', 'game2', 'similarity_score', 'calculated_at']
    search_fields = ['game1__name', 'game2__name']
    raw_id_fields = ['game1', 'game2']
    readonly_fields = ['calculated_at']


@admin.register(GameCountsCache)
class GameCountsCacheAdmin(admin.ModelAdmin):
    list_display = ['game', 'genres_count', 'keywords_count', 'updated_at']
    search_fields = ['game__name']
    raw_id_fields = ['game']
    readonly_fields = ['created_at', 'updated_at']


# ===== ADMIN SITE CONFIGURATION =====
admin.site.site_header = "GamesPeek Site Administration"
admin.site.site_title = "GamesPeek Admin"
admin.site.index_title = "Welcome to GamesPeek Admin"