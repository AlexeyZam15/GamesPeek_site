"""
Django command to delete keywords with low usage count.
Exports keywords to file first, then asks for confirmation before deletion.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Q
from django.db import transaction, connection
from django.conf import settings
from django.utils import timezone
import logging
import os
import time
from typing import List, Dict, Any

from games.models_parts.keywords import Keyword
from games.analyze.keyword_trie import KeywordTrieManager

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Команда для удаления ключевых слов с низкой частотой использования.

    Сначала обновляет кэшированные счетчики для всех ключевых слов,
    затем находит ключевые слова где cached_usage_count <= threshold,
    экспортирует их в файл и запрашивает подтверждение на удаление.

    Опции:
        --outpath: Путь к выходному файлу (по умолчанию: keywords_less_[usage]_for_delete.txt)
        --usage: Пороговое значение использования (по умолчанию 1)
        --dry-run: Только показать результат и создать файл, без удаления
    """

    help = 'Удаление ключевых слов с низкой частотой использования'

    def add_arguments(self, parser):
        """Добавляем аргументы командной строки."""
        parser.add_argument(
            '--outpath',
            type=str,
            required=False,
            help='Путь к выходному файлу (по умолчанию: keywords_less_[usage]_for_delete.txt)'
        )

        parser.add_argument(
            '--usage',
            type=int,
            default=1,
            help='Максимальное количество использований (по умолчанию: 1)'
        )

        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Только показать результат и создать файл со списком, без удаления'
        )

    def handle(self, *args, **options):
        """Основной обработчик команды."""
        threshold = options['usage']
        dry_run = options['dry_run']

        if options['outpath']:
            outpath = options['outpath']
        else:
            outpath = f"keywords_less_{threshold}_for_delete.txt"

        start_time = timezone.now()

        if dry_run:
            self.stdout.write(self.style.WARNING("🔍 РЕЖИМ ПРОСМОТРА (DRY RUN) - удаление выполняться НЕ БУДЕТ"))
        else:
            self.stdout.write(f"Начинаю удаление ключевых слов с использованием <= {threshold}")

        self.stdout.write(f"Выходной файл: {outpath}")

        try:
            # Шаг 1: Обновляем кэшированные счетчики для всех ключевых слов
            self.stdout.write("Обновляю кэшированные счетчики для всех ключевых слов...")
            update_start = timezone.now()

            updated_count = Keyword.bulk_update_cache_counts()

            update_time = (timezone.now() - update_start).total_seconds()
            self.stdout.write(
                self.style.SUCCESS(
                    f"✅ Обновлено {updated_count} ключевых слов за {update_time:.2f} сек"
                )
            )

            # Шаг 2: Получаем ключевые слова с низким usage_count
            self.stdout.write(f"Ищу ключевые слова с использованием <= {threshold}...")
            fetch_start = timezone.now()

            low_usage_keywords = Keyword.objects.filter(
                cached_usage_count__lte=threshold
            ).select_related('category').only(
                'id', 'igdb_id', 'name', 'category__name', 'cached_usage_count'
            ).order_by('name')

            total_count = low_usage_keywords.count()

            fetch_time = (timezone.now() - fetch_start).total_seconds()
            self.stdout.write(
                self.style.SUCCESS(
                    f"✅ Найдено {total_count} ключевых слов с использованием <= {threshold} за {fetch_time:.2f} сек"
                )
            )

            if total_count == 0:
                self.stdout.write(self.style.WARNING("⚠️ Ключевые слова по заданному условию не найдены"))
                self._create_empty_output_file(outpath, threshold)
                return

            # Шаг 3: Показываем статистику
            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(f"🔍 КЛЮЧЕВЫЕ СЛОВА ДЛЯ УДАЛЕНИЯ (использований <= {threshold}):")
            self.stdout.write("=" * 80)

            category_stats = {}
            for keyword in low_usage_keywords:
                category_name = keyword.category.name if keyword.category else "Без категории"
                category_stats[category_name] = category_stats.get(category_name, 0) + 1

            self.stdout.write("\n📊 Статистика по категориям:")
            for category_name, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
                self.stdout.write(f"  • {category_name}: {count} ключевых слов")

            self.stdout.write(f"\n📌 Всего ключевых слов для удаления: {total_count}")

            # Шаг 4: Создаём файл со списком ключевых слов для удаления
            self.stdout.write(f"\n💾 Создаю файл со списком ключевых слов для удаления: {outpath}...")
            self._create_delete_list_file(outpath, low_usage_keywords, total_count, threshold, dry_run)

            if dry_run:
                self.stdout.write(
                    self.style.SUCCESS(
                        f"\n✅ Режим просмотра завершён. Создан файл {outpath} с {total_count} ключевыми словами."
                    )
                )
                self.stdout.write("ℹ️ Удаление не выполнялось (режим --dry-run)")
                return

            # Шаг 5: Запрашиваем подтверждение на удаление
            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.WARNING("⚠️  ВНИМАНИЕ! Будет выполнено УДАЛЕНИЕ ключевых слов!"))
            self.stdout.write("=" * 80)
            self.stdout.write(f"📊 Найдено {total_count} ключевых слов с использованием <= {threshold}")
            self.stdout.write(f"📄 Список сохранён в: {outpath}")

            confirm = input(
                f"\n❓ Вы действительно хотите УДАЛИТЬ эти {total_count} ключевых слов? [y/N]: ").strip().lower()

            if confirm not in ['y', 'yes', 'да']:
                self.stdout.write(self.style.WARNING("❌ Удаление отменено."))
                self._mark_file_as_cancelled(outpath)
                return

            # Шаг 6: Удаляем ключевые слова (без games_game_keywords)
            self.stdout.write(f"\n🗑️ Удаляю {total_count} ключевых слов...")
            delete_start = timezone.now()

            keyword_ids = list(low_usage_keywords.values_list('id', flat=True))

            with transaction.atomic():
                with connection.cursor() as cursor:
                    # Очищаем keyword_ids у игр, убирая удаляемые ключевые слова
                    for kw_id in keyword_ids:
                        cursor.execute("""
                                       UPDATE games_game
                                       SET keyword_ids = array_remove(keyword_ids, %s)
                                       WHERE %s = ANY (keyword_ids)
                                       """, [kw_id, kw_id])

                    # Удаляем ключевые слова
                    cursor.execute("DELETE FROM games_keyword WHERE id = ANY(%s)", [keyword_ids])
                    deleted_count = cursor.rowcount

            delete_time = (timezone.now() - delete_start).total_seconds()

            # Шаг 7: Очищаем кэш Trie после удаления
            KeywordTrieManager().clear_cache()

            # Обновляем файл
            self._mark_file_as_deleted(outpath, deleted_count)

            total_time = (timezone.now() - start_time).total_seconds()

            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(
                self.style.SUCCESS(
                    f"✅ Успешно удалено {deleted_count} ключевых слов за {delete_time:.2f} сек"
                )
            )
            self.stdout.write(f"📄 Файл {outpath} обновлён (отметка об удалении)")
            self.stdout.write(self.style.SUCCESS(f"\n⏱️ Общее время выполнения: {total_time:.2f} сек"))

            self._check_remaining_locks()

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\n❌ Операция отменена пользователем."))
            return
        except Exception as e:
            logger.error(f"Ошибка в delete_low_usage_keywords: {str(e)}", exc_info=True)
            raise CommandError(f"Не удалось обработать ключевые слова: {str(e)}")

    def _check_remaining_locks(self):
        """Проверяет, не осталось ли блокировок после удаления"""
        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM pg_locks l
                                    JOIN pg_stat_activity a ON l.pid = a.pid
                           WHERE NOT granted
                             AND a.state = 'active'
                           """)
            remaining = cursor.fetchone()[0]

            if remaining > 0:
                self.stdout.write(self.style.WARNING(f"⚠️ Обнаружено {remaining} остаточных блокировок, очищаю..."))

                cursor.execute("""
                               SELECT pg_terminate_backend(pid)
                               FROM pg_stat_activity
                               WHERE state = 'active'
                                 AND pid != pg_backend_pid()
                      AND pid IN (SELECT pid FROM pg_locks WHERE NOT granted)
                               """)

                self.stdout.write(self.style.SUCCESS("✅ Остаточные блокировки очищены"))
            else:
                self.stdout.write(self.style.SUCCESS("✅ Остаточных блокировок не обнаружено"))