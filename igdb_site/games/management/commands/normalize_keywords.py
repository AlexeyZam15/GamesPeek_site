# games/management/commands/normalize_keywords.py
"""
Команда для нормализации ключевых слов с использованием WordNet
"""

from django.core.management.base import BaseCommand
from games.models import Keyword
from games.analyze.wordnet_api import get_wordnet_api
from django.db import transaction
from tqdm import tqdm
import time


class Command(BaseCommand):
    help = 'Нормализует ключевые слова с использованием WordNet'

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
            '--batch-size',
            type=int,
            default=100,
            help='Размер пакета для обработки',
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.wordnet_api = None

        # Игровые термины, которые не нужно нормализовать
        self.gaming_terms = {
            'wanted', 'stamina', 'leveling', 'hitpoint', 'manapoint',
            'healthpoint', 'skillpoint', 'spellpoint', 'stat',
        }

        # Пороги семантической близости
        self.PATH_SIMILARITY_THRESHOLD = 0.2
        self.WUP_SIMILARITY_THRESHOLD = 0.3

    def _is_gaming_term(self, word: str) -> bool:
        """Проверяет, является ли слово игровым термином"""
        return word.lower() in self.gaming_terms

    def _is_short_word(self, word: str) -> bool:
        """Проверяет, является ли слово слишком коротким для нормализации"""
        return len(word) <= 3

    def _get_base_form(self, word: str) -> str:
        """Получает базовую форму слова через WordNet"""
        try:
            import nltk
            from nltk.corpus import wordnet as wn
            from nltk.stem import WordNetLemmatizer

            word_lower = word.lower()

            if self.verbose:
                self.stdout.write(f"\n        🔍 Анализ слова/фразы '{word_lower}':")

            # Специальная обработка для фраз с пробелами
            if ' ' in word_lower:
                parts = word_lower.split()
                if self.verbose:
                    self.stdout.write(f"        Обнаружена фраза из {len(parts)} слов: {parts}")

                # Приводим каждое слово к базовой форме
                normalized_parts = []
                for i, part in enumerate(parts):
                    if len(part) >= 3:
                        part_base = self._get_base_form_single(part)
                        if self.verbose:
                            self.stdout.write(f"          Слово {i + 1} '{part}' → '{part_base}'")
                        normalized_parts.append(part_base)
                    else:
                        normalized_parts.append(part)

                # Собираем обратно через пробел
                result = ' '.join(normalized_parts)

                if self.verbose:
                    self.stdout.write(f"        Результат: '{result}'")

                return result

            # Специальная обработка для слов с дефисом
            if '-' in word_lower:
                parts = word_lower.split('-')
                if self.verbose:
                    self.stdout.write(f"        Обнаружен дефис, разбиваем на части: {parts}")

                # Приводим каждую часть к базовой форме
                normalized_parts = []
                for i, part in enumerate(parts):
                    if len(part) >= 3:
                        part_base = self._get_base_form_single(part)
                        if self.verbose:
                            self.stdout.write(f"          Часть {i + 1} '{part}' → '{part_base}'")
                        normalized_parts.append(part_base)
                    else:
                        normalized_parts.append(part)

                # Собираем обратно с дефисом
                result = '-'.join(normalized_parts)

                if self.verbose:
                    self.stdout.write(f"        Результат: '{result}'")

                return result

            # Для одиночных слов - обычная нормализация
            result = self._get_base_form_single(word_lower)

            if self.verbose:
                self.stdout.write(f"        Результат: '{result}'")

            return result

        except Exception as e:
            if self.verbose:
                self.stdout.write(f"        Ошибка: {e}")
            return word.lower()

    def _get_base_form_single(self, word: str) -> str:
        """Получает базовую форму для одного слова через WordNetAPI"""
        try:
            if not self.wordnet_api:
                from games.analyze.wordnet_api import get_wordnet_api
                self.wordnet_api = get_wordnet_api(verbose=self.verbose)

            word_lower = word.lower()

            if self.verbose:
                self.stdout.write(f"\n        🔍 Анализ отдельного слова '{word_lower}':")

            # Используем тот же метод, что и при анализе текста
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

    def _are_semantically_related(self, word1: str, word2: str) -> bool:
        """
        Проверяет семантическую связанность двух слов через WordNet
        """
        try:
            from nltk.corpus import wordnet as wn
            from nltk.stem import PorterStemmer

            word1_lower = word1.lower()
            word2_lower = word2.lower()

            # Если слова совпадают - они связаны
            if word1_lower == word2_lower:
                return True

            # Проверяем через стемминг (safe vs safety)
            stemmer = PorterStemmer()
            if stemmer.stem(word1_lower) == stemmer.stem(word2_lower):
                return True

            # Проверяем через WordNet синонимы и гипонимы
            synsets1 = wn.synsets(word1_lower)
            synsets2 = wn.synsets(word2_lower)

            if not synsets1 or not synsets2:
                return False

            # Проверяем path similarity
            for s1 in synsets1:
                for s2 in synsets2:
                    try:
                        path_sim = s1.path_similarity(s2)
                        if path_sim and path_sim >= self.PATH_SIMILARITY_THRESHOLD:
                            return True

                        wup_sim = s1.wup_similarity(s2)
                        if wup_sim and wup_sim >= self.WUP_SIMILARITY_THRESHOLD:
                            return True
                    except:
                        continue

            return False

        except Exception as e:
            if self.verbose:
                self.stdout.write(f"Ошибка в _are_semantically_related: {e}")
            return False

    def _should_normalize(self, word: str, base_form: str) -> bool:
        """
        Определяет, нужно ли нормализовать слово
        """
        if word == base_form:
            return False

        # Не нормализуем игровые термины
        if self._is_gaming_term(word):
            if self.verbose:
                self.stdout.write(f"   ⏺️ Игровой термин: '{word}'")
            return False

        # Не нормализуем короткие слова
        if self._is_short_word(word):
            if self.verbose:
                self.stdout.write(f"   ⏺️ Короткое слово: '{word}'")
            return False

        # Проверяем семантическую связанность
        if not self._are_semantically_related(word, base_form):
            if self.verbose:
                self.stdout.write(f"   ⏺️ Нет семантической связи: '{word}' -> '{base_form}'")
            return False

        return True

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        self.verbose = options['verbose']
        limit = options['limit']
        batch_size = options['batch_size']

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("НОРМАЛИЗАЦИЯ КЛЮЧЕВЫХ СЛОВ"))
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
            'skipped_no_semantic': 0,
            'errors': 0
        }

        changes = []

        # Обрабатываем ключевые слова
        with tqdm(total=total, desc="Обработка", disable=not self.verbose) as pbar:
            for keyword in keywords:
                try:
                    original_name = keyword.name
                    base_form = self._get_base_form(original_name)

                    if self._should_normalize(original_name, base_form):
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
                        if original_name.lower() != base_form:
                            stats['skipped_no_semantic'] += 1
                        elif self._is_gaming_term(original_name):
                            stats['skipped_gaming'] += 1
                        elif self._is_short_word(original_name):
                            stats['skipped_short'] += 1

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
        self.stdout.write(f"⏺️ Пропущено (нет семантики): {stats['skipped_no_semantic']}")
        self.stdout.write(f"❌ Ошибок: {stats['errors']}")

        if changes and self.verbose:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ИЗМЕНЕНИЯ"))
            self.stdout.write("=" * 70)
            for change in changes[:20]:  # Показываем первые 20
                self.stdout.write(f"  {change['old']} -> {change['new']}")
            if len(changes) > 20:
                self.stdout.write(f"  ... и еще {len(changes) - 20} изменений")

        if dry_run:
            self.stdout.write("\n" + self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА - изменения не сохранены"))