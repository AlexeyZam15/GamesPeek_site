"""Filter section caching model for game list filters."""

from django.db import models
from django.utils import timezone
from django.core.cache import cache
from typing import Dict, List, Optional, Tuple
import hashlib
import json


class CacheVersion(models.Model):
    """Хранит версии различных кэшей в базе данных для сохранения между перезапусками"""

    key = models.CharField(max_length=100, unique=True, db_index=True)
    value = models.CharField(max_length=50)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Cache version"
        verbose_name_plural = "Cache versions"

    def __str__(self) -> str:
        return f"{self.key}: {self.value}"


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
    def _get_persistent_cache_version(cls) -> str:
        """Получает версию кэша из базы данных (переживает перезапуск)"""
        from django.core.cache import cache

        # Сначала пробуем получить из memory cache (быстро)
        version = cache.get('filter_cache_version')
        if version:
            return version

        # Если нет в memory cache, получаем из базы данных
        try:
            cache_version, created = CacheVersion.objects.get_or_create(
                key='filter_cache_version',
                defaults={'value': cls.FILTER_CACHE_VERSION}
            )
            version = cache_version.value
            # Сохраняем в memory cache на 1 час для быстрого доступа
            cache.set('filter_cache_version', version, 3600)
            return version
        except Exception as e:
            print(f"Error getting cache version: {e}")
            return cls.FILTER_CACHE_VERSION

    @classmethod
    def invalidate_all_filters(cls) -> int:
        """
        Инвалидирует все кэши фильтров.
        Увеличивает версию кэша в базе данных.
        """
        from django.core.cache import cache

        try:
            # Получаем текущую версию
            cache_version, created = CacheVersion.objects.get_or_create(
                key='filter_cache_version',
                defaults={'value': cls.FILTER_CACHE_VERSION}
            )

            # Увеличиваем версию
            current = cache_version.value
            if current.startswith('v') and current[1:].isdigit():
                num = int(current[1:]) + 1
                new_version = f'v{num}'
            else:
                new_version = 'v2'

            cache_version.value = new_version
            cache_version.save()

            # Очищаем memory cache
            cache.delete('filter_cache_version')
            cache.set('filter_cache_version', new_version, 3600)

            print(f"Filter cache version incremented from {current} to {new_version}")
            return 1
        except Exception as e:
            print(f"Error invalidating filters: {e}")
            return 0

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
        Получает закэшированную секцию из базы данных или создает новую.
        Версия кэша сохраняется в БД и переживает перезапуск сервера.
        """
        from django.core.cache import cache

        # Получаем текущую версию кэша из БД
        cache_version = cls._get_persistent_cache_version()

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

        # Пытаемся получить из memory cache (быстро)
        try:
            cached_html = cache.get(memory_cache_key)
            if cached_html is not None:
                return cached_html
        except Exception as e:
            print(f"Memory cache get error: {e}")

        # Пытаемся получить из базы данных
        try:
            db_record = cls.objects.filter(
                section_key=section_key,
                is_active=True,
                template_version=cache_version
            ).first()

            if db_record:
                # Обновляем счётчик и время доступа
                db_record.hit_count += 1
                db_record.last_accessed = timezone.now()
                db_record.save(update_fields=['hit_count', 'last_accessed'])

                # Сохраняем в memory cache
                try:
                    cache.set(memory_cache_key, db_record.rendered_html, 86400)
                except Exception as e:
                    print(f"Memory cache set error: {e}")

                return db_record.rendered_html
        except Exception as e:
            print(f"Database cache get error: {e}")

        # Рендерим новую секцию
        print(f"🔄 CACHE MISS: {section_type} - rendering new HTML")
        html = render_func()

        # Сохраняем в базу данных
        try:
            cls.objects.update_or_create(
                section_key=section_key,
                defaults={
                    'section_type': section_type,
                    'rendered_html': html,
                    'data_hash': section_key,
                    'template_version': cache_version,
                    'is_active': True,
                }
            )
        except Exception as e:
            print(f"Database cache save error: {e}")

        # Сохраняем в memory cache
        try:
            cache.set(memory_cache_key, html, 86400)
        except Exception as e:
            print(f"Memory cache set error: {e}")

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
        Версия кэша сохраняется в БД и переживает перезапуск сервера.
        """
        from django.core.cache import cache

        cache_version = cls._get_persistent_cache_version()

        key_parts = [section_type, cache_version]

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

        # Сохраняем в базу данных
        try:
            cls.objects.update_or_create(
                section_key=section_key,
                defaults={
                    'section_type': section_type,
                    'rendered_html': html,
                    'data_hash': section_key,
                    'template_version': cache_version,
                    'is_active': True,
                }
            )
        except Exception as e:
            print(f"Database cache save error: {e}")

        return html, False

    @classmethod
    def get_section_key(cls, section_type: str, selected_ids: List[int] = None,
                        context_data: Dict = None) -> str:
        """
        Генерирует уникальный ключ для секции фильтра с учетом версии кэша.
        """
        cache_version = cls._get_persistent_cache_version()

        key_parts = [section_type, cache_version]

        if selected_ids:
            sorted_ids = sorted(selected_ids) if selected_ids else []
            key_parts.append(','.join(str(i) for i in sorted_ids))

        if context_data:
            year_start = context_data.get('release_year_start')
            year_end = context_data.get('release_year_end')
            if year_start or year_end:
                key_parts.append(f"years_{year_start}_{year_end}")

        key_str = '_'.join(str(p) for p in key_parts)
        return hashlib.md5(key_str.encode()).hexdigest()

    @classmethod
    def invalidate_section_type(cls, section_type: str) -> int:
        """
        Инвалидирует все секции определенного типа.
        """
        updated = cls.objects.filter(
            section_type=section_type,
            is_active=True
        ).update(is_active=False, updated_at=timezone.now())
        return updated

    @classmethod
    def bump_cache_version(cls, new_version: str = None) -> str:
        """
        Увеличивает версию кэша фильтров в базе данных.
        Использовать после изменений в структуре HTML фильтров.
        """
        from django.core.cache import cache

        try:
            cache_version, created = CacheVersion.objects.get_or_create(
                key='filter_cache_version',
                defaults={'value': cls.FILTER_CACHE_VERSION}
            )

            if new_version:
                cache_version.value = new_version
            else:
                current = cache_version.value
                if current.startswith('v') and current[1:].isdigit():
                    num = int(current[1:]) + 1
                    cache_version.value = f'v{num}'
                else:
                    cache_version.value = 'v2'

            cache_version.save()

            # Очищаем memory cache
            cache.delete('filter_cache_version')
            cache.set('filter_cache_version', cache_version.value, 3600)

            return cache_version.value
        except Exception as e:
            print(f"Error bumping cache version: {e}")
            return cls.FILTER_CACHE_VERSION

    @classmethod
    def cleanup_old_caches(cls, days_old: int = 7) -> int:
        """
        Очищает старые неактивные кэши.
        """
        cutoff_date = timezone.now() - timezone.timedelta(days=days_old)
        old_caches = cls.objects.filter(
            is_active=False,
            updated_at__lt=cutoff_date
        )
        count = old_caches.count()
        old_caches.delete()
        return count