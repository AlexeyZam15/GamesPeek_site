"""Keyword models with category support."""

from django.db import models
from django.utils import timezone
from functools import lru_cache
from typing import List


class KeywordCategory(models.Model):
    """Optimized KeywordCategory model."""

    name = models.CharField(max_length=100, unique=True, db_index=True)
    description = models.TextField(blank=True)

    class Meta:
        verbose_name_plural = "Keyword Categories"
        ordering = ['name']
        indexes = [
            models.Index(fields=['name']),
        ]

    def __str__(self) -> str:
        return self.name

    @property
    @lru_cache(maxsize=1)
    def keyword_count(self) -> int:
        """Count of keywords in category with caching."""
        return self.keywords.count()


class Keyword(models.Model):
    """Optimized Keyword model with efficient count caching."""

    igdb_id = models.IntegerField(unique=True, db_index=True)
    name = models.CharField(max_length=100, db_index=True)
    category = models.ForeignKey(
        KeywordCategory,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name='keywords',
        db_index=True
    )

    cached_usage_count = models.IntegerField(default=0, db_index=True)
    last_count_update = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        ordering = ['name']
        indexes = [
            models.Index(fields=['cached_usage_count']),
            models.Index(fields=['name']),
            models.Index(fields=['category', 'cached_usage_count']),
            models.Index(fields=['created_at']),
        ]

    def __str__(self) -> str:
        category_name = self.category.name if self.category else "No Category"
        return f"{self.name} ({category_name})"

    def update_cached_count(self, force: bool = False, async_update: bool = False) -> None:
        """
        Обновляет кэшированное значение с оптимизацией.

        Args:
            force: Принудительное обновление
            async_update: Асинхронное обновление (не используется в текущей реализации)
        """
        from django.conf import settings

        # Отключаем автоматическое обновление в DEBUG режиме
        if settings.DEBUG and not force:
            return

        # Проверяем, нужно ли обновлять
        if not force and self.last_count_update:
            age_hours = (timezone.now() - self.last_count_update).total_seconds() / 3600
            if age_hours < 24:  # Обновляем раз в сутки
                return

        try:
            # Используем быстрый подсчет через базу данных
            actual_count = self.game_set.count()

            # Обновляем только если значение изменилось
            if self.cached_usage_count != actual_count:
                self.cached_usage_count = actual_count
                self.last_count_update = timezone.now()

                # Используем update() вместо save() для скорости
                Keyword.objects.filter(id=self.id).update(
                    cached_usage_count=actual_count,
                    last_count_update=self.last_count_update
                )

                # Обновляем локальный объект
                self.refresh_from_db(fields=['cached_usage_count', 'last_count_update'])

                # УДАЛЕНО: автоматическая очистка кэша Trie при каждом обновлении

        except Exception as e:
            # Логируем ошибку, но не падаем
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error updating keyword cache for {self.id}: {str(e)}")

    @classmethod
    def clear_trie_cache(cls):
        """
        Очищает кэш Trie вручную после массовых операций.
        Вызывать только один раз в конце обработки.
        """
        from games.analyze.keyword_trie import KeywordTrieManager
        KeywordTrieManager().clear_cache()
        print("✅ Кэш Trie очищен после массовой операции")

    @classmethod
    def bulk_update_cache_counts(cls, keyword_ids: List[int] = None, batch_size: int = 100) -> int:
        """
        Массовое обновление счетчиков для списка ключевых слов.

        Returns:
            Количество обновленных записей
        """
        from django.db.models import Count

        queryset = cls.objects.all()
        if keyword_ids:
            queryset = queryset.filter(id__in=keyword_ids)

        updated_count = 0

        # Разбиваем на батчи
        for i in range(0, queryset.count(), batch_size):
            batch = queryset[i:i + batch_size]

            # Аннотируем актуальные счетчики
            annotated = batch.annotate(
                actual_count=Count('game')
            )

            # Фильтруем те, что нужно обновить
            to_update = []
            for keyword in annotated:
                if keyword.cached_usage_count != keyword.actual_count:
                    keyword.cached_usage_count = keyword.actual_count
                    keyword.last_count_update = timezone.now()
                    to_update.append(keyword)

            # Массовое обновление
            if to_update:
                cls.objects.bulk_update(
                    to_update,
                    ['cached_usage_count', 'last_count_update'],
                    batch_size=batch_size
                )
                updated_count += len(to_update)

        return updated_count

    @property
    def usage_count(self) -> int:
        """Возвращает кэшированное или вычисленное значение."""
        # Если кэш пустой или устаревший, обновляем
        if self.cached_usage_count is None or (
                self.last_count_update and
                (timezone.now() - self.last_count_update).days > 7  # Раз в неделю проверяем
        ):
            self.update_cached_count()

        return self.cached_usage_count or 0

    def get_fresh_usage_count(self) -> int:
        """Всегда получает свежее значение (дорогая операция)."""
        return self.game_set.count()

    @property
    @lru_cache(maxsize=1)
    def popularity_score(self) -> float:
        """Популярность равна количеству использований с кэшированием."""
        return float(self.usage_count)

    def save(self, *args, **kwargs) -> None:
        """При сохранении обновляем кэшированный счетчик."""
        is_new = self.pk is None
        super().save(*args, **kwargs)

        if is_new:
            self.update_cached_count(force=True)