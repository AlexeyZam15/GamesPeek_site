# games/management/commands/normalize_keywords.py
"""
Команда для нормализации ключевых слов с использованием WordNetAPI и объединения дубликатов.
Использует только методы из wordnet_api.py, не добавляя своей логики.
"""

from django.core.management.base import BaseCommand
from django.db import transaction, connection
from games.models import Keyword
from games.analyze.wordnet_api import get_wordnet_api
from tqdm import tqdm
from collections import defaultdict
import time
import sys


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
            '--debug',
            action='store_true',
            help='Режим отладки: показывает детальную информацию о каждой операции',
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
        parser.add_argument(
            '--delete-duplicates',
            action='store_true',
            help='УДАЛИТЬ дубликаты без переноса связей (ВНИМАНИЕ: игры потеряют связи!)',
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

            # Получаем базовую форму через WordNetAPI
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

    def _find_duplicate_groups(self):
        """
        Находит все группы дубликатов через SQL.
        Returns: (duplicate_rows, total_groups, total_keywords) или (None, 0, 0) при ошибке
        """
        self.stdout.write("🔍 Поиск дубликатов в базе данных...")
        start_time = time.time()

        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                               SELECT LOWER(name)               as name_lower,
                                      array_agg(id ORDER BY id) as ids,
                                      COUNT(*) as count
                               FROM games_keyword
                               GROUP BY LOWER (name)
                               HAVING COUNT (*) > 1
                               ORDER BY COUNT (*) DESC
                               """)
                duplicate_rows = cursor.fetchall()
        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\n\n⚠️ Поиск прерван пользователем"))
            return None, 0, 0
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Ошибка при поиске дубликатов: {e}"))
            return None, 0, 0

        total_groups = len(duplicate_rows)
        total_keywords = sum(row[2] for row in duplicate_rows)

        search_time = time.time() - start_time
        self.stdout.write(f"   ✅ Поиск завершен за {search_time:.2f} сек")
        self.stdout.write(f"📊 Найдено групп с дубликатами: {total_groups}")
        self.stdout.write(f"📊 Всего ключевых слов-дубликатов: {total_keywords}")

        return duplicate_rows, total_groups, total_keywords

    def _merge_duplicate_keywords(self, dry_run=False):
        """
        Объединяет дубликаты ключевых слов с одинаковым именем (регистронезависимо).
        Использует материализованный вектор keyword_ids вместо games_game_keywords.
        """

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("ОБЪЕДИНЕНИЕ ДУБЛИКАТОВ КЛЮЧЕВЫХ СЛОВ"))
        self.stdout.write("=" * 70)

        if dry_run:
            self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА (без изменений)"))
        self.stdout.write("")

        # ПОИСК ДУБЛИКАТОВ
        duplicate_rows, total_groups, total_keywords = self._find_duplicate_groups()

        if duplicate_rows is None:
            return {'merged_groups': 0, 'merged_keywords': 0, 'deleted_keywords': 0}

        if total_groups == 0:
            self.stdout.write(self.style.SUCCESS("✅ Дубликаты не найдены"))
            return {'merged_groups': 0, 'merged_keywords': 0, 'deleted_keywords': 0}

        # ОБЪЕДИНЕНИЕ ДУБЛИКАТОВ
        stats = self._process_merge_groups(duplicate_rows, total_groups, dry_run)

        # ПРОВЕРКА РЕЗУЛЬТАТА
        self._check_remaining_duplicates(stats, "объединения")

        return stats

    def _process_merge_groups(self, duplicate_rows, total_groups, dry_run):
        """
        МАКСИМАЛЬНО БЫСТРОЕ объединение групп дубликатов.
        Обновляет keyword_ids у игр, заменяя ID дубликатов на ID основного ключевого слова.
        """

        from django.db import connection, transaction

        stats = {
            'merged_groups': 0,
            'merged_keywords': 0,
            'deleted_keywords': 0,
            'start_time': time.time()
        }

        BATCH_SIZE = 500
        current_batch = []

        next_progress = 1000

        try:
            for i, (name_lower, ids, count) in enumerate(duplicate_rows):
                main_id = ids[0]
                duplicate_ids = ids[1:]

                if not dry_run and duplicate_ids:
                    current_batch.append((main_id, duplicate_ids))
                    stats['deleted_keywords'] += len(duplicate_ids)

                stats['merged_groups'] += 1
                stats['merged_keywords'] += len(duplicate_ids)

                if len(current_batch) >= BATCH_SIZE:
                    self._execute_batch_merge(current_batch)
                    current_batch = []

                if stats['merged_groups'] >= next_progress:
                    elapsed = time.time() - stats['start_time']
                    rate = stats['merged_groups'] / elapsed
                    self.stdout.write(f"   ✅ {stats['merged_groups']}/{total_groups} ({rate:.0f}/сек)")
                    next_progress += 1000

            if current_batch and not dry_run:
                self._execute_batch_merge(current_batch)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING(f"\n⚠️ Прервано на группе {stats['merged_groups']}/{total_groups}"))
            return stats

        return stats

    def _execute_batch_merge(self, batch):
        """
        Выполняет массовое объединение для батча групп.
        Обновляет keyword_ids у игр, заменяя duplicate_ids на main_id.
        batch: список кортежей (main_id, [duplicate_ids])
        """

        from django.db import connection, transaction

        with transaction.atomic():
            with connection.cursor() as cursor:
                for main_id, duplicate_ids in batch:
                    if not duplicate_ids:
                        continue

                    # Обновляем keyword_ids у всех игр, заменяя duplicate_ids на main_id
                    for dup_id in duplicate_ids:
                        cursor.execute("""
                                       UPDATE games_game
                                       SET keyword_ids = array_replace(keyword_ids, %s, %s)
                                       WHERE %s = ANY (keyword_ids)
                                       """, [dup_id, main_id, dup_id])

                # Удаляем все дубликаты после обновления
                all_duplicate_ids = []
                for _, duplicate_ids in batch:
                    all_duplicate_ids.extend(duplicate_ids)

                if all_duplicate_ids:
                    cursor.execute("DELETE FROM games_keyword WHERE id = ANY(%s)", [all_duplicate_ids])

    def _delete_duplicate_keywords(self, dry_run=False):
        """
        УДАЛЯЕТ дубликаты ключевых слов, оставляя по одному из каждой группы.
        ВНИМАНИЕ: игры потеряют связи с удаленными ключевыми словами!
        """
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.WARNING("УДАЛЕНИЕ ДУБЛИКАТОВ КЛЮЧЕВЫХ СЛОВ (БЕЗ ПЕРЕНОСА)"))
        self.stdout.write("=" * 70)

        if dry_run:
            self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА (без изменений)"))
        self.stdout.write("")

        # ПОИСК ДУБЛИКАТОВ
        duplicate_rows, total_groups, total_keywords = self._find_duplicate_groups()

        if duplicate_rows is None:
            return {'merged_groups': 0, 'merged_keywords': 0, 'deleted_keywords': 0}

        if total_groups == 0:
            self.stdout.write(self.style.SUCCESS("✅ Дубликаты не найдены"))
            return {'merged_groups': 0, 'merged_keywords': 0, 'deleted_keywords': 0}

        self.stdout.write("")

        # Запрашиваем подтверждение
        if not dry_run:
            self.stdout.write(self.style.WARNING(
                "⚠️  ВНИМАНИЕ: Игры ПОТЕРЯЮТ связи с удаленными ключевыми словами!"
            ))
            response = input("   Продолжить? (yes/no): ")
            if response.lower() != 'yes':
                self.stdout.write(self.style.WARNING("   Операция отменена"))
                return {'merged_groups': 0, 'merged_keywords': 0, 'deleted_keywords': 0}

        # УДАЛЕНИЕ ДУБЛИКАТОВ
        stats = self._process_delete_groups(duplicate_rows, total_groups, dry_run)

        # ПРОВЕРКА РЕЗУЛЬТАТА
        self._check_remaining_duplicates(stats, "удаления")

        return stats

    def _process_delete_groups(self, duplicate_rows, total_groups, dry_run):
        """
        МАКСИМАЛЬНО БЫСТРОЕ удаление групп дубликатов.
        Оставляет по одному ключевому слову из каждой группы.
        """
        stats = {
            'merged_groups': 0,
            'merged_keywords': 0,
            'deleted_keywords': 0,
            'start_time': time.time()
        }

        # БАТЧИ ПО 1000 ГРУПП (максимальная скорость)
        BATCH_SIZE = 1000
        current_batch = []

        # Прогресс каждые 5000 групп
        next_progress = 5000

        try:
            for i, (name_lower, ids, count) in enumerate(duplicate_rows):
                # Оставляем первый (наименьший ID), удаляем остальные
                delete_ids = ids[1:]

                if not dry_run and delete_ids:
                    current_batch.extend(delete_ids)
                    stats['deleted_keywords'] += len(delete_ids)

                stats['merged_groups'] += 1
                stats['merged_keywords'] += len(delete_ids)

                # Обрабатываем батч
                if len(current_batch) >= BATCH_SIZE:
                    self._execute_batch_delete(current_batch)
                    current_batch = []

                # Прогресс
                if stats['merged_groups'] >= next_progress:
                    elapsed = time.time() - stats['start_time']
                    rate = stats['merged_groups'] / elapsed
                    self.stdout.write(
                        f"   ✅ {stats['merged_groups']}/{total_groups} "
                        f"({rate:.0f}/сек)"
                    )
                    next_progress += 5000

            # Финальный батч
            if current_batch and not dry_run:
                self._execute_batch_delete(current_batch)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING(
                f"\n⚠️ Прервано на группе {stats['merged_groups']}/{total_groups}"
            ))
            return stats

        return stats

    def _execute_batch_delete(self, keyword_ids):
        """
        Выполняет массовое удаление ключевых слов одним запросом.
        """
        from django.db import connection

        with connection.cursor() as cursor:
            cursor.execute("""
                           DELETE
                           FROM games_keyword
                           WHERE id = ANY (%s)
                           """, [keyword_ids])

    def _check_remaining_duplicates(self, stats, operation_name):
        """
        Проверяет, остались ли дубликаты после операции.
        """
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS(f"СТАТИСТИКА {operation_name.upper()}"))
        self.stdout.write("=" * 70)
        self.stdout.write(f"📊 Обработано групп: {stats['merged_groups']}")
        self.stdout.write(f"🗑️ Удалено ключевых слов: {stats['deleted_keywords']}")

        # ПРОВЕРКА РЕЗУЛЬТАТА
        self.stdout.write("\n🔍 Проверка результатов...")

        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM (SELECT LOWER(name)
                                 FROM games_keyword
                                 GROUP BY LOWER(name)
                                 HAVING COUNT(*) > 1) t
                           """)
            remaining = cursor.fetchone()[0]

        if remaining == 0:
            self.stdout.write(self.style.SUCCESS("✅ Все дубликаты успешно обработаны"))
        else:
            self.stdout.write(self.style.WARNING(f"⚠️ Осталось групп с дубликатами: {remaining}"))

        elapsed = time.time() - stats['start_time']
        self.stdout.write(f"\n⏱️ Общее время: {elapsed / 60:.1f} минут")

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        self.verbose = options['verbose']
        self.debug = options.get('debug', False)
        limit = options['limit']
        words = options.get('words')
        check_db = options.get('check_db', False)
        skip_merge = options.get('skip_merge', False)
        merge_only = options.get('merge_only', False)
        delete_duplicates = options.get('delete_duplicates', False)

        # Если указан debug, автоматически включаем verbose
        if self.debug:
            self.verbose = True
            self.stdout.write(self.style.SUCCESS("🔍 РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН"))

        # Если указаны слова - показываем формы
        if words:
            self._show_word_forms(words, check_db)
            return

        # Если delete_duplicates - удаляем дубликаты без переноса
        if delete_duplicates:
            merge_stats = self._delete_duplicate_keywords(dry_run=dry_run)

            # Очищаем кэш Trie после удаления дубликатов
            if not dry_run and merge_stats['merged_groups'] > 0:
                try:
                    from games.analyze.keyword_trie import KeywordTrieManager
                    KeywordTrieManager().clear_cache()
                    self.stdout.write(self.style.SUCCESS("\n✅ Кэш Trie очищен после удаления дубликатов"))
                except ImportError:
                    self.stdout.write(self.style.WARNING("\n⚠️ Не удалось очистить кэш Trie: модуль не найден"))
            return

        # Если merge_only - только объединяем дубликаты без нормализации
        if merge_only:
            merge_stats = self._merge_duplicate_keywords(dry_run=dry_run)

            # Очищаем кэш Trie после объединения дубликатов
            if not dry_run and merge_stats['merged_groups'] > 0:
                try:
                    from games.analyze.keyword_trie import KeywordTrieManager
                    KeywordTrieManager().clear_cache()
                    self.stdout.write(self.style.SUCCESS("\n✅ Кэш Trie очищен после объединения дубликатов"))
                except ImportError:
                    self.stdout.write(self.style.WARNING("\n⚠️ Не удалось очистить кэш Trie: модуль не найден"))
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
                        if self.debug:
                            self.stdout.write(f"   ⏭️ Пропуск короткого слова: '{original_name}'")
                        stats['processed'] += 1
                        pbar.update(1)
                        continue

                    # Получаем базовую форму через WordNetAPI
                    if self.debug:
                        self.stdout.write(f"\n   🔍 Анализ слова: '{original_name}'")

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
                            if self.debug:
                                self.stdout.write(f"   ✅ ИЗМЕНЕНО: {original_name} -> {base_form}")
                        elif self.debug:
                            self.stdout.write(f"   🔄 БУДЕТ ИЗМЕНЕНО: {original_name} -> {base_form}")
                    else:
                        if self.debug:
                            self.stdout.write(f"   ⏺️ Без изменений: '{original_name}'")

                    stats['processed'] += 1
                    pbar.update(1)

                except Exception as e:
                    stats['errors'] += 1
                    if self.debug:
                        self.stdout.write(self.style.ERROR(f"   ❌ Ошибка: {keyword.name} - {e}"))
                    elif self.verbose:
                        self.stdout.write(self.style.ERROR(f"❌ Ошибка: {keyword.name} - {e}"))

        # Выводим статистику нормализации
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("СТАТИСТИКА НОРМАЛИЗАЦИИ"))
        self.stdout.write("=" * 70)
        self.stdout.write(f"📊 Всего обработано: {stats['processed']}")
        self.stdout.write(f"✅ Нормализовано: {stats['normalized']}")
        self.stdout.write(f"❌ Ошибок: {stats['errors']}")

        if changes and (self.verbose or self.debug):
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ИЗМЕНЕНИЯ"))
            self.stdout.write("=" * 70)
            for change in changes[:20]:
                self.stdout.write(f"  {change['old']} -> {change['new']}")
            if len(changes) > 20:
                self.stdout.write(f"  ... и еще {len(changes) - 20} изменений")
            if self.debug and changes:
                self.stdout.write(f"\n📝 Всего изменений: {len(changes)}")

        # После нормализации показываем информацию о дубликатах
        merge_stats = None
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

                if self.verbose or self.debug:
                    self.stdout.write("\n📌 ГРУППЫ ДУБЛИКАТОВ:")
                    self.stdout.write("-" * 50)
                    for name_lower, kws in list(duplicate_groups.items())[:10 if not self.debug else None]:
                        names = [f"'{kw.name}' (ID:{kw.id})" for kw in kws]
                        self.stdout.write(f"  {name_lower}: {', '.join(names)}")
                        # Показываем количество игр для каждого
                        for kw in kws:
                            games_count = kw.game_set.count()
                            self.stdout.write(f"    ↳ игр: {games_count}")
                    if len(duplicate_groups) > 10 and not self.debug:
                        self.stdout.write(f"  ... и еще {len(duplicate_groups) - 10} групп")

                if dry_run:
                    self.stdout.write(self.style.WARNING("\n🔧 РЕЖИМ ПРОСМОТРА - дубликаты НЕ будут объединены"))
                    self.stdout.write("   Чтобы объединить дубликаты, запустите без --dry-run")
                else:
                    self.stdout.write("\n🔄 Автоматическое объединение дубликатов...")
                    merge_stats = self._merge_duplicate_keywords(dry_run=False)
            else:
                self.stdout.write(self.style.SUCCESS("✅ Дубликаты не найдены"))

        if dry_run:
            self.stdout.write("\n" + self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА - изменения не сохранены"))

        # Очищаем кэш Trie в конце, если были изменения
        if not dry_run and (stats['normalized'] > 0 or (merge_stats and merge_stats['merged_groups'] > 0)):
            try:
                from games.analyze.keyword_trie import KeywordTrieManager
                KeywordTrieManager().clear_cache()
                self.stdout.write(self.style.SUCCESS("\n✅ Кэш Trie очищен после всех операций"))
            except ImportError:
                self.stdout.write(self.style.WARNING("\n⚠️ Не удалось очистить кэш Trie: модуль не найден"))