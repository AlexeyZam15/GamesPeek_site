# games/management/commands/normalize_keywords.py
"""
Команда для нормализации ключевых слов с использованием WordNetAPI и объединения дубликатов.
Использует только методы из wordnet_api.py, не добавляя своей логики.
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Keyword
from games.analyze.wordnet_api import get_wordnet_api
from tqdm import tqdm
from collections import defaultdict


class Command(BaseCommand):
    help = 'Нормализует ключевые слова, объединяет дубликаты и показывает формы через WordNetAPI'

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
        parser.add_argument(
            '--skip-merge',
            action='store_true',
            help='Пропустить объединение дубликатов (по умолчанию дубликаты объединяются)',
        )
        parser.add_argument(
            '--merge-only',
            action='store_true',
            help='Только объединить дубликаты без нормализации',
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
                    duplicates = Keyword.objects.filter(name__iexact=word)
                    count = duplicates.count()
                    if count > 1:
                        kw = duplicates.first()
                        games_count = kw.game_set.count()
                        self.stdout.write(
                            f"📌 '{word}': ✅ ЕСТЬ в базе (ID: {kw.id}, дубликатов: {count}, игр: {games_count})")
                    else:
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

    def _merge_duplicate_keywords(self, dry_run=False):
        """
        Объединяет дубликаты ключевых слов с одинаковым именем (регистронезависимо)
        """
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("ОБЪЕДИНЕНИЕ ДУБЛИКАТОВ КЛЮЧЕВЫХ СЛОВ"))
        self.stdout.write("=" * 70)

        if dry_run:
            self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА (без изменений)"))
        self.stdout.write("")

        # Находим все дубликаты по имени (регистронезависимо)
        all_keywords = Keyword.objects.all()

        # Группируем по нижнему регистру
        groups = defaultdict(list)
        for kw in all_keywords:
            groups[kw.name.lower()].append(kw)

        # Фильтруем только группы с дубликатами
        duplicate_groups = {name: kws for name, kws in groups.items() if len(kws) > 1}

        total_duplicate_groups = len(duplicate_groups)
        total_duplicate_keywords = sum(len(kws) for kws in duplicate_groups.values())

        self.stdout.write(f"📊 Найдено групп с дубликатами: {total_duplicate_groups}")
        self.stdout.write(f"📊 Всего ключевых слов-дубликатов: {total_duplicate_keywords}")

        if total_duplicate_groups == 0:
            self.stdout.write(self.style.SUCCESS("✅ Дубликаты не найдены"))
            return {'merged_groups': 0, 'merged_keywords': 0, 'deleted_keywords': 0}

        stats = {
            'merged_groups': 0,
            'merged_keywords': 0,
            'deleted_keywords': 0
        }

        # Сначала показываем список групп если нужно
        if self.verbose and not dry_run:
            self.stdout.write("\n📌 НАЙДЕННЫЕ ГРУППЫ ДУБЛИКАТОВ:")
            self.stdout.write("-" * 50)
            for name_lower, keywords in list(duplicate_groups.items())[:10]:
                names = [f"'{kw.name}' (ID:{kw.id})" for kw in keywords]
                self.stdout.write(f"  {name_lower}: {', '.join(names)}")
            if len(duplicate_groups) > 10:
                self.stdout.write(f"  ... и еще {len(duplicate_groups) - 10} групп")
            self.stdout.write("")

        # Обрабатываем каждую группу дубликатов
        with tqdm(total=len(duplicate_groups), desc="Объединение дубликатов", disable=not self.verbose, position=0,
                  leave=True) as pbar:
            for name_lower, keywords in duplicate_groups.items():
                # Сортируем по ID (оставляем наименьший ID как основной)
                keywords.sort(key=lambda x: x.id)
                main_keyword = keywords[0]
                duplicates_to_merge = keywords[1:]

                # В verbose режиме показываем детали, но без нарушения прогресс-бара
                if self.verbose and pbar.n < 3:  # Показываем только первые 3 группы для примера
                    pbar.write(
                        f"  📌 Группа '{name_lower}': основной {main_keyword.name} (ID:{main_keyword.id}), дубликатов: {len(duplicates_to_merge)}")

                if not dry_run:
                    with transaction.atomic():
                        # Переносим все связи с дубликатов на основной
                        for dup in duplicates_to_merge:
                            # Получаем все игры, связанные с дубликатом
                            games_with_dup = list(dup.game_set.all())

                            for game in games_with_dup:
                                # Добавляем основной ключ, если его ещё нет
                                if main_keyword not in game.keywords.all():
                                    game.keywords.add(main_keyword)

                            # Обновляем счётчик использования основного ключа
                            main_keyword.update_cached_count(force=True)

                            # Удаляем дубликат
                            dup.delete()
                            stats['deleted_keywords'] += 1

                stats['merged_groups'] += 1
                stats['merged_keywords'] += len(duplicates_to_merge)
                pbar.update(1)

        # Финальная статистика
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("СТАТИСТИКА ОБЪЕДИНЕНИЯ"))
        self.stdout.write("=" * 70)
        self.stdout.write(f"📊 Обработано групп: {stats['merged_groups']}")
        self.stdout.write(f"✅ Объединено ключевых слов: {stats['merged_keywords']}")
        self.stdout.write(f"🗑️ Удалено дубликатов: {stats['deleted_keywords']}")

        if dry_run:
            self.stdout.write(self.style.WARNING("\n🔧 РЕЖИМ ПРОСМОТРА - изменения не сохранены"))
        else:
            self.stdout.write(self.style.SUCCESS(f"\n✅ Объединение завершено"))

        return stats

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        self.verbose = options['verbose']
        limit = options['limit']
        words = options.get('words')
        check_db = options.get('check_db', False)
        skip_merge = options.get('skip_merge', False)
        merge_only = options.get('merge_only', False)

        # Если указаны слова - показываем формы
        if words:
            self._show_word_forms(words, check_db)
            return

        # Если merge_only - только объединяем дубликаты без нормализации
        if merge_only:
            self._merge_duplicate_keywords(dry_run=dry_run)
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

        # Выводим статистику нормализации
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("СТАТИСТИКА НОРМАЛИЗАЦИИ"))
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

        # После нормализации показываем информацию о дубликатах
        if not skip_merge:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ПРОВЕРКА ДУБЛИКАТОВ"))
            self.stdout.write("=" * 70)

            # Находим все дубликаты по имени (регистронезависимо)
            all_keywords = Keyword.objects.all()

            # Группируем по нижнему регистру
            groups = defaultdict(list)
            for kw in all_keywords:
                groups[kw.name.lower()].append(kw)

            # Фильтруем только группы с дубликатами
            duplicate_groups = {name: kws for name, kws in groups.items() if len(kws) > 1}

            total_duplicate_groups = len(duplicate_groups)
            total_duplicate_keywords = sum(len(kws) for kws in duplicate_groups.values())

            if total_duplicate_groups > 0:
                self.stdout.write(f"\n📊 Найдено групп с дубликатами: {total_duplicate_groups}")
                self.stdout.write(f"📊 Всего ключевых слов-дубликатов: {total_duplicate_keywords}")

                if self.verbose:
                    self.stdout.write("\n📌 ГРУППЫ ДУБЛИКАТОВ:")
                    self.stdout.write("-" * 50)
                    for name_lower, kws in list(duplicate_groups.items())[:10]:
                        names = [f"'{kw.name}' (ID:{kw.id})" for kw in kws]
                        self.stdout.write(f"  {name_lower}: {', '.join(names)}")
                        # Показываем количество игр для каждого
                        for kw in kws:
                            games_count = kw.game_set.count()
                            self.stdout.write(f"    ↳ игр: {games_count}")
                    if len(duplicate_groups) > 10:
                        self.stdout.write(f"  ... и еще {len(duplicate_groups) - 10} групп")

                if dry_run:
                    self.stdout.write(self.style.WARNING("\n🔧 РЕЖИМ ПРОСМОТРА - дубликаты НЕ будут объединены"))
                    self.stdout.write("   Чтобы объединить дубликаты, запустите без --dry-run")
                else:
                    self.stdout.write("\n🔄 Автоматическое объединение дубликатов...")
                    self._merge_duplicate_keywords(dry_run=False)
            else:
                self.stdout.write(self.style.SUCCESS("✅ Дубликаты не найдены"))

        if dry_run:
            self.stdout.write("\n" + self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА - изменения не сохранены"))
