# Файл: admin.py
# Путь: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\admin.py

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

class WikiDescriptionFilter(admin.SimpleListFilter):
    """Фильтр по наличию вики описания"""
    title = _('Wiki описание')
    parameter_name = 'wiki_description'

    def lookups(self, request, model_admin):
        return (
            ('has_wiki', _('✅ Есть вики описание')),
            ('no_wiki', _('❌ Нет вики описания')),
        )

    def queryset(self, request, queryset):
        if self.value() == 'has_wiki':
            return queryset.exclude(wiki_description__isnull=True).exclude(wiki_description='')
        elif self.value() == 'no_wiki':
            return queryset.filter(wiki_description__isnull=True) | queryset.filter(wiki_description='')
        return queryset


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


# ===== CUSTOM GAME ADMIN =====

@admin.register(Game)
class GameAdmin(admin.ModelAdmin):
    # Основные настройки отображения
    list_display = [
        'name_link',  # Ссылка с имени вместо ID
        'game_type',
        'rating',
        'first_release_date',
    ]

    # Фильтры в правой панели
    list_filter = [
        'game_type',
        'genres',
        'platforms',
    ]

    # ТОЧНЫЙ ПОИСК ПО НАЗВАНИЮ (используем __iexact для точного совпадения)
    search_fields = [
        'name',  # Только по названию игры
    ]

    # Переопределяем поиск для точного совпадения
    def get_search_results(self, request, queryset, search_term):
        """
        Кастомный поиск: сначала точное совпадение, затем частичное
        """
        if not search_term:
            return queryset, False

        # Ищем точное совпадение (без учета регистра)
        exact_matches = queryset.filter(name__iexact=search_term)

        # Если есть точные совпадения - показываем только их
        if exact_matches.exists():
            return exact_matches, False

        # Ищем частичные совпадения (содержит слово)
        contains_matches = queryset.filter(name__icontains=search_term)

        # Если есть частичные совпадения - показываем их
        if contains_matches.exists():
            return contains_matches, False

        # Если ничего не найдено
        return queryset.none(), False

    # Опции редактирования связей
    filter_horizontal = ['genres', 'platforms', 'keywords', 'developers', 'publishers']

    # Оптимизация для полей с большим количеством записей
    raw_id_fields = ['parent_game', 'version_parent']

    # Группировка полей в форме редактирования
    fieldsets = (
        ('Основная информация', {
            'fields': (
                'name',
                'igdb_id',
                'summary',
                'game_type',
                'rating',
                'rating_count',
                'first_release_date',
                'last_analyzed_date',
            )
        }),
        ('Связи с играми', {
            'fields': (
                'parent_game',
                'version_parent',
                'version_title',
            ),
            'classes': ('collapse',),
        }),
        ('Серии', {
            'fields': (
                'series',
                'series_order',
            ),
            'classes': ('collapse',),
        }),
        ('Многие ко многим - Основные', {
            'fields': (
                'genres',
                'platforms',
                'keywords',
            )
        }),
        ('Многие ко многим - Компании', {
            'fields': (
                'developers',
                'publishers',
            ),
            'classes': ('collapse',),
        }),
        ('Многие ко многим - Дополнительные', {
            'fields': (
                'themes',
                'player_perspectives',
                'game_modes',
            ),
            'classes': ('collapse',),
        }),
        ('Описания', {
            'fields': (
                'storyline',
                'rawg_description',
                'wiki_description',  # Вики описание на странице редактирования
                'cover_url'
            ),
            'classes': ('wide',),
        }),
    )

    # Сортировка по умолчанию - сначала точные совпадения, затем по имени
    ordering = ['name']

    # Количество элементов на странице
    list_per_page = 50

    # Действия массового редактирования
    actions = [
        'mark_as_analyzed',
        'clear_wiki_descriptions',
        'copy_summary_to_wiki',
        'update_cached_counts_action',
    ]

    # Поля доступные для быстрого редактирования в списке
    list_editable = ['game_type']

    # ========== КАСТОМНЫЕ МЕТОДЫ ДЛЯ ОТОБРАЖЕНИЯ ==========

    def name_link(self, obj):
        """Ссылка на редактирование с названия игры"""
        url = reverse('admin:games_game_change', args=[obj.id])
        return format_html('<a href="{}"><strong>{}</strong></a>', url, obj.name)

    name_link.short_description = "Название игры"
    name_link.admin_order_field = 'name'

    # ========== КАСТОМНЫЕ АДМИН-ДЕЙСТВИЯ ==========

    def mark_as_analyzed(self, request, queryset):
        """Пометить игры как проанализированные"""
        updated = queryset.update(last_analyzed_date=timezone.now())
        self.message_user(
            request,
            f"{updated} игр помечены как проанализированные",
            messages.SUCCESS
        )

    mark_as_analyzed.short_description = "📝 Пометить как проанализированные"

    def clear_wiki_descriptions(self, request, queryset):
        """Очистить вики описания у выбранных игр"""
        updated = queryset.update(wiki_description='')
        self.message_user(
            request,
            f"Вики описания очищены у {updated} игр",
            messages.SUCCESS
        )

    clear_wiki_descriptions.short_description = "🗑️ Очистить вики описания"

    def copy_summary_to_wiki(self, request, queryset):
        """Копировать summary в wiki_description"""
        updated = 0
        for game in queryset:
            if game.summary and not game.wiki_description:
                game.wiki_description = game.summary
                game.save()
                updated += 1

        self.message_user(
            request,
            f"Описания скопированы у {updated} игр",
            messages.SUCCESS if updated > 0 else messages.WARNING
        )

    copy_summary_to_wiki.short_description = "📋 Копировать summary в wiki"

    def update_cached_counts_action(self, request, queryset):
        """Обновить кэшированные счетчики"""
        count = 0
        for game in queryset:
            game.update_cached_counts(force=True)
            count += 1

        self.message_user(
            request,
            f"Кэш обновлен у {count} игр",
            messages.SUCCESS
        )

    update_cached_counts_action.short_description = "🔄 Обновить кэш счетчиков"

    # ========== ПРИОРИТЕТНЫЙ ПОИСК ==========

    def changelist_view(self, request, extra_context=None):
        """
        Добавляем информацию о поиске в контекст
        """
        extra_context = extra_context or {}

        # Если есть поисковый запрос
        search_term = request.GET.get('q', '')
        if search_term:
            # Получаем queryset
            queryset = self.get_queryset(request)

            # Считаем результаты поиска
            search_results = self.get_search_results(request, queryset, search_term)[0]

            # Добавляем информацию в контекст
            extra_context['search_results_count'] = search_results.count()
            extra_context['search_term'] = search_term

            # Если найдены точные совпадения, показываем сообщение
            exact_count = queryset.filter(name__iexact=search_term).count()
            if exact_count > 0:
                messages.info(
                    request,
                    f"Найдено {exact_count} точных совпадений по запросу '{search_term}'"
                )

        return super().changelist_view(request, extra_context)

    # ========== ОПТИМИЗАЦИЯ ==========

    def get_queryset(self, request):
        """Оптимизация запросов"""
        queryset = super().get_queryset(request)
        # Минимальная оптимизация для производительности
        return queryset

    def formfield_for_manytomany(self, db_field, request, **kwargs):
        """Оптимизация для полей ManyToMany"""
        if db_field.name in ['genres', 'platforms', 'keywords']:
            # Сортировка по имени для удобства
            kwargs['queryset'] = db_field.remote_field.model.objects.all().order_by('name')
        return super().formfield_for_manytomany(db_field, request, **kwargs)


# ===== CUSTOM KEYWORD ADMIN =====

@admin.register(Keyword)
class KeywordAdmin(admin.ModelAdmin):
    list_display = [
        'name',
        'category',
        'delete_link',
        'igdb_id',
        'cached_usage_count',
        'popularity_indicator',
        'created_at',
    ]

    list_filter = [
        'category',
        UsageCountFilter,
        PopularityFilter,
    ]

    search_fields = ['name', 'category__name']
    list_editable = ['category']
    ordering = ['-created_at']

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
        ('Даты', {
            'fields': ('created_at',),
            'classes': ('collapse',)
        }),
    )

    readonly_fields = ['last_count_update', 'created_at']

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

    def created_at_formatted(self, obj):
        """Форматированное отображение даты создания"""
        if obj.created_at:
            return obj.created_at.strftime('%Y-%m-%d %H:%M')
        return "-"

    created_at_formatted.short_description = "Дата добавления"
    created_at_formatted.admin_order_field = 'created_at'

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
        unused_keywords = queryset.filter(cached_usage_count=0)
        count = unused_keywords.count()

        if count == 0:
            self.message_user(
                request,
                "Среди выбранных нет неиспользуемых ключевых слов (0 использований).",
                messages.WARNING
            )
            return

        return self.delete_selected(request, unused_keywords)

    bulk_delete_unused.short_description = "🗑️ Удалить неиспользуемые (0)"


# ===== BASIC ADMIN CONFIGURATIONS =====

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