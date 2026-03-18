"""
Django command to delete keywords with low usage count.
Exports keywords to file first, then asks for confirmation before deletion.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Q
from django.db import transaction
from django.conf import settings
from django.utils import timezone
import logging
import os
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

        # Генерируем имя файла по умолчанию если не указан outpath
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

            # Используем только cached_usage_count для максимальной скорости
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

                # Даже если нет ключевых слов, создаём файл с информацией
                self.stdout.write(f"\n💾 Создаю пустой файл {outpath}...")

                # Создаем директорию если нужно
                if os.path.dirname(outpath):
                    os.makedirs(os.path.dirname(os.path.abspath(outpath)), exist_ok=True)

                with open(outpath, 'w', encoding='utf-8') as f:
                    f.write(f"# Ключевые слова с использованием <= {threshold}\n")
                    f.write(f"# Сгенерировано: {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"# Всего ключевых слов: 0\n")
                    f.write("# Ключевые слова по заданному условию не найдены\n")

                self.stdout.write(self.style.SUCCESS(f"✅ Создан пустой файл {outpath}"))
                return

            # Шаг 3: Показываем статистику
            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(f"🔍 КЛЮЧЕВЫЕ СЛОВА ДЛЯ УДАЛЕНИЯ (использований <= {threshold}):")
            self.stdout.write("=" * 80)

            # Группируем по категориям для статистики
            category_stats = {}
            for keyword in low_usage_keywords:
                category_name = keyword.category.name if keyword.category else "Без категории"
                if category_name not in category_stats:
                    category_stats[category_name] = 0
                category_stats[category_name] += 1

            # Показываем статистику по категориям
            self.stdout.write("\n📊 Статистика по категориям:")
            for category_name, count in sorted(category_stats.items(), key=lambda x: x[1], reverse=True):
                self.stdout.write(f"  • {category_name}: {count} ключевых слов")

            self.stdout.write(f"\n📌 Всего ключевых слов для удаления: {total_count}")

            # Шаг 4: Создаём файл со списком ключевых слов для удаления
            self.stdout.write(f"\n💾 Создаю файл со списком ключевых слов для удаления: {outpath}...")
            write_start = timezone.now()

            # Создаем директорию если нужно
            if os.path.dirname(outpath):
                os.makedirs(os.path.dirname(os.path.abspath(outpath)), exist_ok=True)

            with open(outpath, 'w', encoding='utf-8') as f:
                # Заголовок с информацией
                f.write(f"# Ключевые слова для УДАЛЕНИЯ (использований <= {threshold})\n")
                f.write(f"# Сгенерировано: {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"# Всего ключевых слов для удаления: {total_count}\n")

                if dry_run:
                    f.write(f"# РЕЖИМ ПРОСМОТРА (DRY RUN) - удаление НЕ выполнялось\n")
                else:
                    f.write(f"# Статус: ожидают подтверждения удаления\n")

                f.write("# Формат: ID | IGDB ID | Название | Категория | Количество использований\n")
                f.write("#" + "=" * 80 + "\n")

                for keyword in low_usage_keywords.iterator(chunk_size=1000):
                    category_name = keyword.category.name if keyword.category else "Без категории"
                    f.write(
                        f"{keyword.id} | {keyword.igdb_id} | {keyword.name} | "
                        f"{category_name} | {keyword.cached_usage_count}\n"
                    )

            write_time = (timezone.now() - write_start).total_seconds()
            self.stdout.write(
                self.style.SUCCESS(
                    f"✅ Успешно создан файл {outpath} с {total_count} ключевыми словами за {write_time:.2f} сек"
                )
            )

            # Шаг 5: Если dry-run, показываем примеры и завершаем
            if dry_run:
                self.stdout.write("\n" + "=" * 80)
                self.stdout.write("🔍 РЕЖИМ ПРОСМОТРА (DRY RUN) - удаление НЕ ВЫПОЛНЯЛОСЬ")
                self.stdout.write("=" * 80)

                # Показываем первые 20 ключевых слов как пример
                self.stdout.write("\n📝 Примеры ключевых слов из файла (первые 20):")
                for i, keyword in enumerate(low_usage_keywords[:20]):
                    category = keyword.category.name if keyword.category else "Без категории"
                    self.stdout.write(
                        f"  {i + 1}. {keyword.name} (ID: {keyword.igdb_id}, "
                        f"Категория: {category}, Использований: {keyword.cached_usage_count})"
                    )

                if total_count > 20:
                    self.stdout.write(f"  ... и ещё {total_count - 20} ключевых слов (полный список в файле)")

                self.stdout.write(
                    self.style.SUCCESS(
                        f"\n✅ Режим просмотра завершён. Создан файл {outpath} с {total_count} ключевыми словами."
                    )
                )
                self.stdout.write("ℹ️ Удаление не выполнялось (режим --dry-run)")
                return

            # Шаг 6: Запрашиваем подтверждение на удаление
            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(self.style.WARNING("⚠️  ВНИМАНИЕ! Будет выполнено УДАЛЕНИЕ ключевых слов!"))
            self.stdout.write("=" * 80)
            self.stdout.write(f"📊 Найдено {total_count} ключевых слов с использованием <= {threshold}")
            self.stdout.write(f"📄 Список сохранён в: {outpath}")
            self.stdout.write("\n⚠️ Пожалуйста, проверьте файл перед продолжением.")

            # Запрашиваем подтверждение
            confirm = input(
                f"\n❓ Вы действительно хотите УДАЛИТЬ эти {total_count} ключевых слов? [y/N]: "
            ).strip().lower()

            if confirm != 'y' and confirm != 'yes' and confirm != 'да':
                self.stdout.write(self.style.WARNING("❌ Удаление отменено."))

                # Обновляем файл, отмечая что удаление было отменено
                with open(outpath, 'r+', encoding='utf-8') as f:
                    content = f.read()
                    f.seek(0)
                    content = content.replace(
                        "# Статус: ожидают подтверждения удаления",
                        "# Статус: УДАЛЕНИЕ ОТМЕНЕНО пользователем"
                    )
                    f.write(content)
                    f.truncate()

                self.stdout.write(self.style.WARNING(f"Файл {outpath} обновлён (отметка об отмене)"))
                return

            # Шаг 7: Удаляем ключевые слова
            self.stdout.write(f"\n🗑️ Удаляю {total_count} ключевых слов...")
            delete_start = timezone.now()

            # Получаем ID для удаления
            keyword_ids = list(low_usage_keywords.values_list('id', flat=True))

            # Удаляем батчами для минимизации блокировок
            batch_size = 500
            deleted_count = 0

            with transaction.atomic():
                for i in range(0, len(keyword_ids), batch_size):
                    batch_ids = keyword_ids[i:i + batch_size]

                    # Удаляем ключевые слова (связи ManyToMany удалятся автоматически)
                    deleted_batch, _ = Keyword.objects.filter(id__in=batch_ids).delete()
                    deleted_count += len(batch_ids)

                    self.stdout.write(f"  Удалено {min(i + batch_size, len(keyword_ids))}/{len(keyword_ids)}",
                                      ending='\r')

            delete_time = (timezone.now() - delete_start).total_seconds()

            # Шаг 8: Очищаем кэш Trie после удаления
            KeywordTrieManager().clear_cache()

            # Обновляем файл, отмечая что удаление выполнено
            with open(outpath, 'r+', encoding='utf-8') as f:
                content = f.read()
                f.seek(0)
                content = content.replace(
                    "# Статус: ожидают подтверждения удаления",
                    f"# Статус: УДАЛЕНЫ {deleted_count} ключевых слов {timezone.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )
                f.write(content)
                f.truncate()

            self.stdout.write("\n" + "=" * 80)
            self.stdout.write(
                self.style.SUCCESS(
                    f"✅ Успешно удалено {deleted_count} ключевых слов за {delete_time:.2f} сек"
                )
            )
            self.stdout.write(self.style.SUCCESS(f"📄 Файл {outpath} обновлён (отметка об удалении)"))

            # Показываем общее время выполнения
            total_time = (timezone.now() - start_time).total_seconds()
            self.stdout.write(
                self.style.SUCCESS(
                    f"\n⏱️ Общее время выполнения: {total_time:.2f} сек"
                )
            )

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\n❌ Операция отменена пользователем."))
            return

        except Exception as e:
            logger.error(f"Ошибка в delete_low_usage_keywords: {str(e)}", exc_info=True)
            raise CommandError(f"Не удалось обработать ключевые слова: {str(e)}")