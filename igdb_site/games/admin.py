# FILE: admin.py
# PATH: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\admin.py

from django.contrib import admin
from django.db.models import Count
from django.utils import timezone
from django.contrib import messages
from django.utils.translation import gettext_lazy as _
from django.utils.html import format_html
from django.urls import reverse
from .models import (
    Game, GameSimilarityDetail, GameCountsCache, Company, Series,
    Theme, PlayerPerspective, GameMode, KeywordCategory, Keyword,
    Genre, Platform, GameSimilarityCache, Screenshot
)


# ===== CUSTOM FILTERS =====

class UsageCountFilter(admin.SimpleListFilter):
    """Фильтр по количеству использований ключевых слов"""
    title = _('Использований')
    parameter_name = 'usage_count'

    def lookups(self, request, model_admin):
        return (
            ('0', _('Не используется (0)')),
            ('1-9', _('Мало (1-9)')),
            ('10-99', _('Средне (10-99)')),
            ('100-999', _('Много (100-999)')),
            ('1000+', _('Очень много (1000+)')),
        )

    def queryset(self, request, queryset):
        if self.value() == '0':
            return queryset.filter(cached_usage_count=0)
        elif self.value() == '1-9':
            return queryset.filter(cached_usage_count__gte=1, cached_usage_count__lte=9)
        elif self.value() == '10-99':
            return queryset.filter(cached_usage_count__gte=10, cached_usage_count__lte=99)
        elif self.value() == '100-999':
            return queryset.filter(cached_usage_count__gte=100, cached_usage_count__lte=999)
        elif self.value() == '1000+':
            return queryset.filter(cached_usage_count__gte=1000)
        return queryset


class PopularityFilter(admin.SimpleListFilter):
    """Фильтр по популярности (визуальные категории)"""
    title = _('Популярность')
    parameter_name = 'popularity'

    def lookups(self, request, model_admin):
        return (
            ('unused', _('🟡 Не используется')),
            ('low', _('🟢 Мало (1-9)')),
            ('medium', _('🟡 Средне (10-99)')),
            ('high', _('🔴 Популярно (100+)')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'unused':
            return queryset.filter(cached_usage_count=0)
        elif self.value() == 'low':
            return queryset.filter(cached_usage_count__gte=1, cached_usage_count__lte=9)
        elif self.value() == 'medium':
            return queryset.filter(cached_usage_count__gte=10, cached_usage_count__lte=99)
        elif self.value() == 'high':
            return queryset.filter(cached_usage_count__gte=100)
        return queryset


# ===== CUSTOM KEYWORD ADMIN =====

@admin.register(Keyword)
class KeywordAdmin(admin.ModelAdmin):
    list_display = [
        'name',
        'category',
        'delete_link',  # Отдельный столбец для крестика
        'igdb_id',
        'cached_usage_count',
        'popularity_indicator',
    ]

    list_filter = [
        'category',
        UsageCountFilter,
        PopularityFilter,
    ]

    search_fields = ['name', 'category__name']

    list_editable = ['category']

    # Сортировка по популярности (по умолчанию)
    ordering = ['-cached_usage_count', 'name']

    actions = [
        'delete_selected',
        'update_counts_action',
        'bulk_delete_unused',
        'recalculate_counts_action',
    ]

    fieldsets = (
        ('Основная информация', {
            'fields': ('name', 'igdb_id', 'category')
        }),
        ('Статистика использования', {
            'fields': ('cached_usage_count', 'last_count_update'),
            'classes': ('collapse',)
        }),
    )

    readonly_fields = ['last_count_update']

    # Кастомные колонки
    def popularity_indicator(self, obj):
        """Визуальный индикатор популярности"""
        if obj.cached_usage_count == 0:
            return "🟡 Не используется"
        elif obj.cached_usage_count < 10:
            return f"🟢 Мало ({obj.cached_usage_count})"
        elif obj.cached_usage_count < 100:
            return f"🟡 Средне ({obj.cached_usage_count})"
        else:
            return f"🔴 Популярно ({obj.cached_usage_count})"

    popularity_indicator.short_description = "Популярность"
    popularity_indicator.admin_order_field = 'cached_usage_count'

    def delete_link(self, obj):
        """Ссылка удаления в отдельном столбце"""
        url = reverse('admin:games_keyword_delete', args=[obj.id])
        return format_html(
            '<a href="{}">'
            '<img src="/static/admin/img/icon-deletelink.svg" alt="Delete" title="Удалить" width="24" height="24">'
            '</a>',
            url
        )

    delete_link.short_description = "🗑️"

    # Кастомные действия
    def update_counts_action(self, request, queryset):
        """Обновить счетчики использования для выбранных ключевых слов"""
        count = 0
        for keyword in queryset:
            keyword.update_cached_count(force=True)
            count += 1

        self.message_user(
            request,
            f"Счетчики обновлены для {count} ключевых слов.",
            messages.SUCCESS
        )

    update_counts_action.short_description = "📊 Обновить счетчики"

    def recalculate_counts_action(self, request, queryset):
        """Пересчитать счетчики с нуля для выбранных ключевых слов"""
        count = 0
        for keyword in queryset:
            # Получаем актуальное количество использований
            actual_count = keyword.game_set.count()
            keyword.cached_usage_count = actual_count
            keyword.last_count_update = timezone.now()
            keyword.save(update_fields=['cached_usage_count', 'last_count_update'])
            count += 1

        self.message_user(
            request,
            f"Счетчики полностью пересчитаны для {count} ключевых слов.",
            messages.SUCCESS
        )

    recalculate_counts_action.short_description = "🔄 Пересчитать счетчики"

    def bulk_delete_unused(self, request, queryset):
        """Массовое удаление неиспользуемых ключевых слов (0 использований)"""
        # Фильтруем только неиспользуемые
        unused_keywords = queryset.filter(cached_usage_count=0)
        count = unused_keywords.count()

        if count == 0:
            self.message_user(
                request,
                "Среди выбранных нет неиспользуемых ключевых слов (0 использований).",
                messages.WARNING
            )
            return

        # Используем стандартное действие удаления
        return self.delete_selected(request, unused_keywords)

    bulk_delete_unused.short_description = "🗑️ Удалить неиспользуемые (0)"


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