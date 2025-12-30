# Создайте файл: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\management\commands\update_keyword_counts.py

import time
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count
from django.utils import timezone
from games.models import Keyword
from typing import List, Tuple


class Command(BaseCommand):
    help = 'Обновляет cached_usage_count для всех ключевых слов'

    def add_arguments(self, parser):
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Размер батча для обработки (по умолчанию: 1000)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительное обновление, даже если данные недавно обновлялись'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод прогресса'
        )
        parser.add_argument(
            '--keyword-ids',
            type=str,
            help='Обновить только указанные ключевые слова (через запятую)'
        )
        parser.add_argument(
            '--min-age-hours',
            type=int,
            default=24,
            help='Минимальный возраст кэша для обновления (часов, по умолчанию: 24)'
        )

    def handle(self, *args, **options):
        batch_size = options['batch_size']
        force = options['force']
        verbose = options['verbose']
        min_age_hours = options['min_age_hours']

        # Определяем ключевые слова для обновления
        if options['keyword_ids']:
            keyword_ids = [int(id.strip()) for id in options['keyword_ids'].split(',')]
            queryset = Keyword.objects.filter(id__in=keyword_ids)
            self.stdout.write(f"Обновление {len(keyword_ids)} указанных ключевых слов")
        else:
            queryset = Keyword.objects.all()
            self.stdout.write(f"Обновление всех ключевых слов (всего: {queryset.count()})")

        if not force:
            # Фильтруем только те, которые не обновлялись более min_age_hours часов
            cutoff_time = timezone.now() - timezone.timedelta(hours=min_age_hours)
            old_queryset = queryset.filter(
                last_count_update__isnull=True
            ) | queryset.filter(
                last_count_update__lt=cutoff_time
            )

            if verbose:
                self.stdout.write(
                    f"Найдено {old_queryset.count()} ключевых слов для обновления "
                    f"(не обновлялись более {min_age_hours} часов)"
                )
            queryset = old_queryset

        total_keywords = queryset.count()

        if total_keywords == 0:
            self.stdout.write(self.style.SUCCESS("Нет ключевых слов для обновления"))
            return

        # Подсчитываем актуальные значения
        if verbose:
            self.stdout.write("Подсчет актуальных значений использования...")

        # Получаем актуальные счетчики
        start_time = time.time()
        updated_count = 0
        skipped_count = 0
        error_count = 0

        # Обрабатываем батчами
        for i in range(0, total_keywords, batch_size):
            batch_start = i
            batch_end = min(i + batch_size, total_keywords)
            batch = queryset[batch_start:batch_end]

            try:
                # Аннотируем актуальные счетчики
                annotated_batch = Keyword.objects.filter(
                    id__in=[kw.id for kw in batch]
                ).annotate(
                    actual_count=Count('game')
                )

                # Определяем, какие нужно обновить
                keywords_to_update = []
                for keyword in annotated_batch:
                    if force or keyword.cached_usage_count != keyword.actual_count:
                        keyword.cached_usage_count = keyword.actual_count
                        keyword.last_count_update = timezone.now()
                        keywords_to_update.append(keyword)
                        updated_count += 1
                    else:
                        skipped_count += 1

                # Массовое обновление
                if keywords_to_update:
                    Keyword.objects.bulk_update(
                        keywords_to_update,
                        ['cached_usage_count', 'last_count_update'],
                        batch_size=batch_size
                    )

                # Вывод прогресса
                if verbose or (i // batch_size) % 10 == 0:
                    elapsed = time.time() - start_time
                    processed = batch_end
                    percent = (processed / total_keywords) * 100
                    speed = processed / elapsed if elapsed > 0 else 0

                    self.stdout.write(
                        f"Обработано: {processed}/{total_keywords} ({percent:.1f}%) | "
                        f"Скорость: {speed:.1f} ключ/сек | "
                        f"Обновлено: {updated_count} | Пропущено: {skipped_count}"
                    )

            except Exception as e:
                error_count += 1
                self.stdout.write(self.style.ERROR(f"Ошибка в батче {i}-{batch_end}: {str(e)}"))
                continue

        # Финальная статистика
        total_time = time.time() - start_time

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.SUCCESS("ОБНОВЛЕНИЕ ЗАВЕРШЕНО"))
        self.stdout.write("=" * 60)
        self.stdout.write(f"Всего ключевых слов: {total_keywords}")
        self.stdout.write(f"Обновлено: {updated_count}")
        self.stdout.write(f"Пропущено (не изменились): {skipped_count}")
        self.stdout.write(f"Ошибок: {error_count}")
        self.stdout.write(f"Общее время: {total_time:.2f} секунд")

        if total_time > 0:
            self.stdout.write(f"Скорость: {total_keywords / total_time:.1f} ключ/сек")

        # Статистика по значениям
        if verbose:
            self.stdout.write("\nСтатистика использования ключевых слов:")
            stats = Keyword.objects.aggregate(
                max_usage=models.Max('cached_usage_count'),
                avg_usage=models.Avg('cached_usage_count'),
                total_usage=models.Sum('cached_usage_count')
            )
            self.stdout.write(f"Максимальное использование: {stats['max_usage']}")
            self.stdout.write(f"Среднее использование: {stats['avg_usage']:.1f}")
            self.stdout.write(f"Всего использований: {stats['total_usage']}")

            # Топ-10 самых популярных ключевых слов
            top_keywords = Keyword.objects.order_by('-cached_usage_count')[:10]
            self.stdout.write("\nТоп-10 самых популярных ключевых слов:")
            for i, kw in enumerate(top_keywords, 1):
                self.stdout.write(f"{i}. {kw.name}: {kw.cached_usage_count} использований")