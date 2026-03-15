# games/management/commands/normalize_keywords.py
"""
Команда для нормализации ключевых слов с использованием WordNetAPI.
Использует только методы из wordnet_api.py, не добавляя своей логики.
"""

from django.core.management.base import BaseCommand
from games.models import Keyword
from games.analyze.wordnet_api import get_wordnet_api
from tqdm import tqdm


class Command(BaseCommand):
    help = 'Нормализует ключевые слова или показывает формы через WordNetAPI'

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
        parser.add_argument(
            '--words',
            type=str,
            nargs='+',
            help='Список слов для показа форм (режим тестирования)',
        )
        parser.add_argument(
            '--check-db',
            action='store_true',
            help='Проверить наличие слов в базе данных (только с --words)',
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wordnet_api = None

    def _init_wordnet(self):
        """Инициализирует WordNetAPI"""
        if not self.wordnet_api:
            self.wordnet_api = get_wordnet_api(verbose=self.verbose)
        return self.wordnet_api.is_available()

    def _get_all_forms_from_wordnet(self, word: str) -> dict:
        """
        Получает все формы слова через WordNetAPI
        Просто показывает то, что возвращает wordnet_api
        """
        results = {
            'word': word,
            'best_base': None,
            'synsets_count': 0
        }

        word_lower = word.lower()

        if not self.wordnet_api or not self.wordnet_api.is_available():
            return results

        try:
            # Получаем все synsets для слова
            synsets = self.wordnet_api.wordnet.synsets(word_lower)
            results['synsets_count'] = len(synsets)

            # Получаем лучшую базовую форму
            results['best_base'] = self.wordnet_api.get_best_base_form(word_lower)

        except Exception as e:
            if self.verbose:
                self.stdout.write(f"   ❌ Ошибка при получении форм: {e}")

        return results

    def _show_word_forms(self, words, check_db=False):
        """Показывает результаты нормализации для указанных слов"""
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("ПОКАЗ РЕЗУЛЬТАТОВ НОРМАЛИЗАЦИИ"))
        self.stdout.write("=" * 70)

        # Инициализируем WordNetAPI
        self.stdout.write("\n🔧 Инициализация WordNetAPI...")
        if not self._init_wordnet():
            self.stdout.write(self.style.ERROR("❌ WordNetAPI недоступен"))
            return
        self.stdout.write("✅ WordNetAPI готов")

        # Проверяем наличие в базе если нужно
        if check_db:
            self.stdout.write("\n🔍 ПРОВЕРКА НАЛИЧИЯ В БАЗЕ:")
            self.stdout.write("-" * 50)

            for word in words:
                exists = Keyword.objects.filter(name__iexact=word).exists()
                if exists:
                    kw = Keyword.objects.get(name__iexact=word)
                    games_count = kw.game_set.count()
                    self.stdout.write(f"📌 '{word}': ✅ ЕСТЬ в базе (ID: {kw.id}, игр: {games_count})")
                else:
                    self.stdout.write(f"📌 '{word}': ❌ НЕТ в базе")

        for word in words:
            self.stdout.write(f"\n📌 СЛОВО: '{word}'")
            self.stdout.write("=" * 50)

            # Получаем базовую форму через WordNetAPI (весь процесс фильтрации выводится внутри)
            best_base = self.wordnet_api.get_best_base_form(word)

            # Выводим только результат
            self.stdout.write(f"\n📊 РЕЗУЛЬТАТ НОРМАЛИЗАЦИИ:")
            self.stdout.write("-" * 40)

            if best_base:
                self.stdout.write(f"\n   🎯 БАЗОВАЯ ФОРМА: '{best_base}'")

                # Сравниваем с оригиналом
                if best_base != word.lower():
                    self.stdout.write(f"\n   🔄 Изменение: {word.lower()} → {best_base}")
                else:
                    self.stdout.write(f"\n   ⏺️ Без изменений")
            else:
                self.stdout.write(f"\n   ❌ Не удалось получить базовую форму")

        self.stdout.write("\n" + "=" * 70)

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        self.verbose = options['verbose']
        limit = options['limit']
        words = options.get('words')
        check_db = options.get('check_db', False)

        # Если указаны слова - показываем формы
        if words:
            self._show_word_forms(words, check_db)
            return

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("НОРМАЛИЗАЦИЯ КЛЮЧЕВЫХ СЛОВ"))
        self.stdout.write("=" * 70)

        if dry_run:
            self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА (без изменений)"))
        self.stdout.write("")

        # Получаем WordNetAPI
        self.stdout.write("🔧 Инициализация WordNetAPI...")
        if not self._init_wordnet():
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
            'errors': 0
        }

        changes = []

        # Обрабатываем ключевые слова
        with tqdm(total=total, desc="Обработка", disable=not self.verbose) as pbar:
            for keyword in keywords:
                try:
                    original_name = keyword.name
                    original_lower = original_name.lower()

                    # Пропускаем короткие слова (меньше 3 символов)
                    if len(original_lower) <= 3:
                        stats['processed'] += 1
                        pbar.update(1)
                        continue

                    # Получаем базовую форму через WordNetAPI
                    base_form = self.wordnet_api.get_best_base_form(original_name)

                    # Проверяем, нужно ли обновить
                    if base_form and base_form != original_lower:
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