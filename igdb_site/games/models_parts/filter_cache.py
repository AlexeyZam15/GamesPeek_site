"""Filter section caching model for game list filters."""

from django.db import models
from django.utils import timezone
from django.core.cache import cache
from typing import Dict, List, Optional, Tuple
import hashlib
import json


class FilterSectionCache(models.Model):
    """Model for caching pre-rendered filter sections HTML."""

    # Константа версии кэша фильтров - увеличивать при изменении структуры HTML фильтров
    FILTER_CACHE_VERSION = 'v1'

    # Типы секций фильтров
    SECTION_TYPES = [
        ('search_platforms', 'Search Platforms'),
        ('search_genres', 'Search Genres'),
        ('search_keywords', 'Search Keywords'),
        ('search_themes', 'Search Themes'),
        ('search_perspectives', 'Search Perspectives'),
        ('search_game_modes', 'Search Game Modes'),
        ('search_engines', 'Search Engines'),
        ('search_game_types', 'Search Game Types'),
        ('search_date', 'Search Date Filter'),

        ('similarity_platforms', 'Similarity Platforms'),
        ('similarity_genres', 'Similarity Genres'),
        ('similarity_keywords', 'Similarity Keywords'),
        ('similarity_themes', 'Similarity Themes'),
        ('similarity_perspectives', 'Similarity Perspectives'),
        ('similarity_game_modes', 'Similarity Game Modes'),
        ('similarity_engines', 'Similarity Engines'),
        ('similarity_date', 'Similarity Date Filter'),
    ]

    # Уникальный ключ секции
    section_key = models.CharField(
        max_length=100,
        unique=True,
        db_index=True,
        verbose_name="Section key"
    )

    # Тип секции
    section_type = models.CharField(
        max_length=50,
        choices=SECTION_TYPES,
        db_index=True,
        verbose_name="Section type"
    )

    # Рендеренный HTML
    rendered_html = models.TextField(verbose_name="Rendered HTML")

    # Хэш данных для проверки актуальности
    data_hash = models.CharField(max_length=64, db_index=True, verbose_name="Data hash")

    # Версия шаблона
    template_version = models.CharField(
        max_length=10,
        default=FILTER_CACHE_VERSION,
        verbose_name="Template version"
    )

    # Метаданные
    is_active = models.BooleanField(default=True, db_index=True, verbose_name="Active")
    hit_count = models.IntegerField(default=0, verbose_name="Hit count")
    last_accessed = models.DateTimeField(auto_now=True, verbose_name="Last accessed")
    created_at = models.DateTimeField(auto_now_add=True, verbose_name="Created at")
    updated_at = models.DateTimeField(auto_now=True, verbose_name="Updated at")

    class Meta:
        verbose_name = "Filter section cache"
        verbose_name_plural = "Filter section caches"
        ordering = ['-last_accessed']
        indexes = [
            models.Index(fields=['section_key', 'is_active']),
            models.Index(fields=['section_type', 'is_active']),
            models.Index(fields=['template_version', 'is_active']),
            models.Index(fields=['hit_count', 'is_active']),
        ]

    def __str__(self) -> str:
        return f"Filter cache: {self.section_key}"

    @classmethod
    def get_or_render(
            cls,
            section_type: str,
            params_hash: str,
            render_func: callable,
            selected_ids: List[int] = None,
            context_data: Dict = None
    ) -> str:
        """
        Получает закэшированную секцию из memory cache или создает новую.
        При рестарте сервера кэш полностью сбрасывается через filter_cache_version.
        """
        from django.core.cache import cache
        import hashlib

        # Получаем текущую версию кэша (устанавливается в apps.py при рестарте)
        cache_version = cache.get('filter_cache_version', 'v1')

        # Генерируем ключ с учётом версии
        key_parts = [section_type, params_hash, cache_version]

        if selected_ids:
            sorted_ids = sorted(selected_ids) if selected_ids else []
            key_parts.append(','.join(str(i) for i in sorted_ids))

        if context_data:
            year_start = context_data.get('release_year_start')
            year_end = context_data.get('release_year_end')
            if year_start or year_end:
                key_parts.append(f"years_{year_start}_{year_end}")

        key_str = '_'.join(str(p) for p in key_parts)
        section_key = hashlib.md5(key_str.encode()).hexdigest()
        memory_cache_key = f"filter_section_{section_key}"

        # Пытаемся получить из memory cache
        try:
            cached_html = cache.get(memory_cache_key)
            if cached_html is not None:
                return cached_html
        except Exception as e:
            print(f"Cache get error: {e}")

        # Рендерим новую секцию
        html = render_func()

        # Сохраняем в memory cache
        try:
            cache.set(memory_cache_key, html, 86400)
        except Exception as e:
            print(f"Cache set error: {e}")

        return html

    @classmethod
    def get_or_create_section(
            cls,
            section_type: str,
            render_func: callable,
            selected_ids: List[int] = None,
            context_data: Dict = None,
            force_refresh: bool = False
    ) -> Tuple[str, bool]:
        """
        Получает закэшированную секцию из memory cache или создает новую.
        При рестарте сервера кэш полностью сбрасывается.
        """
        from django.core.cache import cache

        section_key = cls.get_section_key(section_type, selected_ids, context_data)
        memory_cache_key = f"filter_section_{section_key}"

        if not force_refresh:
            try:
                cached_html = cache.get(memory_cache_key)
                if cached_html is not None:
                    return cached_html, True
            except Exception as e:
                print(f"Cache get error: {e}")

        # Рендерим новую секцию
        html = render_func()

        # Сохраняем в memory cache
        try:
            cache.set(memory_cache_key, html, 86400)
        except Exception as e:
            print(f"Cache set error: {e}")

        return html, False

    @classmethod
    def get_section_key(cls, section_type: str, selected_ids: List[int] = None,
                        context_data: Dict = None) -> str:
        """
        Генерирует уникальный ключ для секции фильтра с учетом версии кэша.

        Args:
            section_type: Тип секции
            selected_ids: Список выбранных ID
            context_data: Дополнительные данные контекста (годы, и т.д.)

        Returns:
            Уникальный ключ для кэша
        """
        from django.core.cache import cache

        key_parts = [section_type]

        if selected_ids:
            sorted_ids = sorted(selected_ids) if selected_ids else []
            key_parts.append(','.join(str(i) for i in sorted_ids))

        if context_data:
            year_start = context_data.get('release_year_start')
            year_end = context_data.get('release_year_end')
            if year_start or year_end:
                key_parts.append(f"years_{year_start}_{year_end}")

        # Добавляем версию кэша из memory cache
        version_key = 'filter_cache_version'
        cache_version = cache.get(version_key, 1)
        key_parts.append(f"v{cache_version}")

        key_str = '_'.join(str(p) for p in key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()


    @classmethod
    def invalidate_all_filters(cls) -> int:
        """
        Инвалидирует все кэши фильтров в memory cache.
        При рестарте сервера этот метод можно не вызывать - кэш сбросится сам.

        Returns:
            Количество инвалидированных записей (всегда возвращает 0 для memory cache)
        """
        from django.core.cache import cache

        # Увеличиваем версию кэша фильтров
        version_key = 'filter_cache_version'
        current_version = cache.get(version_key, 0)
        cache.set(version_key, current_version + 1, 86400)

        logger.info(f"Filter cache version incremented to {current_version + 1}")
        return 0

    @classmethod
    def invalidate_section_type(cls, section_type: str) -> int:
        """
        Инвалидирует все секции определенного типа.

        Args:
            section_type: Тип секции для инвалидации

        Returns:
            Количество инвалидированных секций
        """
        updated = cls.objects.filter(
            section_type=section_type,
            is_active=True
        ).update(is_active=False, updated_at=timezone.now())
        return updated

    @classmethod
    def bump_cache_version(cls, new_version: str = None) -> str:
        """
        Увеличивает версию кэша фильтров.
        Использовать после изменений в структуре HTML фильтров.

        Args:
            new_version: Новая версия (если не указана, увеличивает текущую)

        Returns:
            Новая версия кэша
        """
        if new_version:
            cls.FILTER_CACHE_VERSION = new_version
        else:
            current = cls.FILTER_CACHE_VERSION
            if current.startswith('v') and current[1:].isdigit():
                num = int(current[1:]) + 1
                cls.FILTER_CACHE_VERSION = f'v{num}'
            else:
                cls.FILTER_CACHE_VERSION = 'v2'

        return cls.FILTER_CACHE_VERSION

    @classmethod
    def cleanup_old_caches(cls, days_old: int = 7) -> int:
        """
        Очищает старые неактивные кэши.

        Args:
            days_old: Возраст в днях для удаления

        Returns:
            Количество удаленных записей
        """
        cutoff_date = timezone.now() - timezone.timedelta(days=days_old)
        old_caches = cls.objects.filter(
            is_active=False,
            updated_at__lt=cutoff_date
        )
        count = old_caches.count()
        old_caches.delete()
        return count