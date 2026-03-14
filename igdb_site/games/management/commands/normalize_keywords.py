# games/management/commands/normalize_keywords.py
"""
Команда для нормализации ключевых слов с использованием WordNetAPI.
Теперь использует единый метод WordNetAPI.get_best_base_form.
"""

from django.core.management.base import BaseCommand
from games.models import Keyword
from games.analyze.wordnet_api import get_wordnet_api
from tqdm import tqdm


class Command(BaseCommand):
    help = 'Нормализует ключевые слова с использованием WordNetAPI'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать что будет изменено без реальных изменений',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод',
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Ограничить количество обрабатываемых ключевых слов',
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wordnet_api = None
        # Игровые термины, которые не нужно нормализовать
        self.gaming_terms = {
            'wanted', 'stamina', 'leveling', 'hitpoint', 'manapoint',
            'healthpoint', 'skillpoint', 'spellpoint', 'stat',
        }

    def _is_gaming_term(self, word: str) -> bool:
        """Проверяет, является ли слово игровым термином"""
        return word.lower() in self.gaming_terms

    def _is_short_word(self, word: str) -> bool:
        """Проверяет, является ли слово слишком коротким для нормализации"""
        return len(word) <= 3

    def _get_base_form(self, word: str) -> str:
        """
        Получает базовую форму слова через WordNetAPI.
        УПРОЩЕНО: Использует единый метод API.
        """
        try:
            word_lower = word.lower()

            if self.verbose:
                self.stdout.write(f"\n        🔍 Анализ слова/фразы '{word_lower}':")

            # 1. Проверка на игровой термин (делаем здесь для оптимизации)
            if self._is_gaming_term(word_lower):
                if self.verbose:
                    self.stdout.write(f"        ⏺️ Игровой термин, пропускаем нормализацию.")
                return word_lower

            # 2. Используем WordNetAPI для получения лучшей базовой формы
            if not self.wordnet_api:
                self.wordnet_api = get_wordnet_api(verbose=self.verbose)

            base_form = self.wordnet_api.get_best_base_form(word_lower)

            if self.verbose:
                if base_form != word_lower:
                    self.stdout.write(f"        ✅ Базовая форма: '{base_form}'")
                else:
                    self.stdout.write(f"        ⏺️ Слово уже в базовой форме: '{base_form}'")

            return base_form

        except Exception as e:
            if self.verbose:
                self.stdout.write(f"        Ошибка: {e}")
            return word.lower()

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        self.verbose = options['verbose']
        limit = options['limit']

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("НОРМАЛИЗАЦИЯ КЛЮЧЕВЫХ СЛОВ (Упрощенная)"))
        self.stdout.write("=" * 70)

        if dry_run:
            self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА (без изменений)"))
        self.stdout.write("")

        # Получаем WordNetAPI
        self.stdout.write("🔧 Инициализация WordNetAPI...")
        self.wordnet_api = get_wordnet_api(verbose=self.verbose)

        if not self.wordnet_api.is_available():
            self.stdout.write(self.style.ERROR("❌ WordNetAPI недоступен"))
            return

        self.stdout.write("✅ WordNetAPI готов")
        self.stdout.write("")

        # Получаем ключевые слова
        keywords = Keyword.objects.all().order_by('name')
        if limit:
            keywords = keywords[:limit]

        total = keywords.count()
        self.stdout.write(f"📊 Найдено ключевых слов: {total}")
        self.stdout.write("")

        stats = {
            'processed': 0,
            'normalized': 0,
            'skipped_gaming': 0,
            'skipped_short': 0,
            'errors': 0
        }

        changes = []

        # Обрабатываем ключевые слова
        with tqdm(total=total, desc="Обработка", disable=not self.verbose) as pbar:
            for keyword in keywords:
                try:
                    original_name = keyword.name
                    original_lower = original_name.lower()

                    # Пропускаем короткие слова
                    if self._is_short_word(original_lower):
                        stats['skipped_short'] += 1
                        stats['processed'] += 1
                        pbar.update(1)
                        continue

                    # Получаем базовую форму
                    base_form = self._get_base_form(original_name)

                    # Проверяем, нужно ли обновить
                    if base_form != original_lower and not self._is_gaming_term(original_lower):
                        stats['normalized'] += 1
                        changes.append({
                            'id': keyword.id,
                            'old': original_name,
                            'new': base_form
                        })

                        if not dry_run:
                            keyword.name = base_form
                            keyword.save()

                        if self.verbose:
                            self.stdout.write(f"   ✅ {original_name} -> {base_form}")
                    else:
                        if self._is_gaming_term(original_lower):
                            stats['skipped_gaming'] += 1

                    stats['processed'] += 1
                    pbar.update(1)

                except Exception as e:
                    stats['errors'] += 1
                    if self.verbose:
                        self.stdout.write(self.style.ERROR(f"❌ Ошибка: {keyword.name} - {e}"))

        # Выводим статистику
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("СТАТИСТИКА"))
        self.stdout.write("=" * 70)
        self.stdout.write(f"📊 Всего обработано: {stats['processed']}")
        self.stdout.write(f"✅ Нормализовано: {stats['normalized']}")
        self.stdout.write(f"⏺️ Пропущено (игровые термины): {stats['skipped_gaming']}")
        self.stdout.write(f"⏺️ Пропущено (короткие слова): {stats['skipped_short']}")
        self.stdout.write(f"❌ Ошибок: {stats['errors']}")

        if changes and self.verbose:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ИЗМЕНЕНИЯ"))
            self.stdout.write("=" * 70)
            for change in changes[:20]:
                self.stdout.write(f"  {change['old']} -> {change['new']}")
            if len(changes) > 20:
                self.stdout.write(f"  ... и еще {len(changes) - 20} изменений")

        if dry_run:
            self.stdout.write("\n" + self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА - изменения не сохранены"))