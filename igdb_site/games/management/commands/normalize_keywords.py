# games/management/commands/normalize_keywords.py
"""
Django команда для нормализации ключевых слов:
- Использует WordNetAPI для определения исходных форм слов
- Объединяет связанные слова (trader → trade, player → play)
- Игнорирует специальные игровые термины и аббревиатуры
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Keyword
from games.analyze.wordnet_api import get_wordnet_api
import time
from collections import defaultdict


class Command(BaseCommand):
    help = 'Нормализует ключевые слова используя WordNetAPI'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.verbose = False
        self.wordnet_api = None

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать что будет сделано без фактических изменений',
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод',
        )
        parser.add_argument(
            '--fix-specific',
            type=str,
            help='Исправить конкретное слово (например, "trader")',
        )

    def _is_gaming_term(self, word: str) -> bool:
        """
        Проверяет, является ли слово специальным игровым термином,
        который не нужно нормализовать
        """
        word_lower = word.lower()

        # Игровые аббревиатуры и сокращения
        gaming_abbr = {
            'cod', 'fps', 'rpg', 'mmo', 'rts', 'moba', 'pvp', 'pve',
            'hp', 'mp', 'xp', 'ap', 'dp', 'dps', 'hps', 'gcd', 'cd',
            'boss', 'mob', 'npc', 'pc', 'ai', 'ui', 'gui', 'hud',
            'diy', 'dlc', 'gacha', 'rogue', 'roguelike', 'roguelite'
        }

        if word_lower in gaming_abbr:
            return True

        # Игровые термины
        gaming_terms = {
            'wanted', 'stamina', 'leveling', 'hitpoint', 'manapoint',
            'healthpoint', 'skillpoint', 'spellpoint', 'stat',
        }

        if word_lower in gaming_terms:
            return True

        return False

    def _is_short_word(self, word: str) -> bool:
        """
        Проверяет, является ли слово коротким (3 буквы или меньше)
        """
        word_lower = word.lower()

        if len(word_lower) <= 3:
            return True

        short_forms = {'run', 'ran', 'set', 'sit', 'sat', 'eat', 'ate', 'fly', 'flew'}
        if word_lower in short_forms:
            return False

        return False

    def _get_base_form(self, word: str) -> str:
        """
        Определяет исходную форму слова используя WordNetAPI

        Args:
            word: Слово для нормализации (например: "trader", "player", "drawing")

        Returns:
            Базовая форма слова (например: "trade", "play", "draw")
        """
        word_lower = word.lower()

        # ========== ПРОВЕРЯЕМ ИГРОВЫЕ ТЕРМИНЫ ==========
        if self._is_gaming_term(word_lower):
            return word_lower

        # ========== НЕ ОБРАБАТЫВАЕМ КОРОТКИЕ СЛОВА ==========
        if self._is_short_word(word_lower):
            return word_lower

        # ========== ОБРАБОТКА ФРАЗ С ПРОБЕЛАМИ ==========
        if ' ' in word_lower:
            parts = word_lower.split()
            if len(parts) == 2:
                first, second = parts
                normalized_second = self._get_base_form(second)
                return f"{first} {normalized_second}"
            return word_lower

        # ========== ОБРАБОТКА СОСТАВНЫХ СЛОВ С ДЕФИСАМИ ==========
        if '-' in word_lower:
            parts = word_lower.split('-')
            if len(parts) == 2:
                first, second = parts
                normalized_second = self._get_base_form(second)
                return f"{first}-{normalized_second}"
            return word_lower

        # ========== ДЛЯ ОБЫЧНЫХ СЛОВ ИСПОЛЬЗУЕМ WORDNETAPI ==========
        return self.wordnet_api.get_best_base_form(word_lower)

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        verbose = options['verbose']
        fix_specific = options['fix_specific']

        # Сохраняем verbose как атрибут класса
        self.verbose = verbose

        # Инициализируем WordNetAPI
        self.wordnet_api = get_wordnet_api(verbose=verbose)

        start_time = time.time()

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("НОРМАЛИЗАЦИЯ КЛЮЧЕВЫХ СЛОВ (WordNetAPI)"))
        self.stdout.write("=" * 70)

        if not self.wordnet_api.is_available():
            self.stdout.write(self.style.ERROR("❌ WordNetAPI недоступен. Невозможно выполнить нормализацию."))
            return

        if dry_run:
            self.stdout.write(self.style.WARNING("🏃 РЕЖИМ DRY RUN - изменения не будут сохранены"))

        # Получаем все ключевые слова
        if fix_specific:
            keywords = list(Keyword.objects.filter(name__iexact=fix_specific))
            if not keywords:
                keywords = list(Keyword.objects.filter(name__icontains=fix_specific))
            self.stdout.write(f"🔍 Ищем слова, содержащие '{fix_specific}': найдено {len(keywords)}")
        else:
            keywords = list(Keyword.objects.all().order_by('name'))
            self.stdout.write(f"📊 Всего ключевых слов в базе: {len(keywords)}")

        # Создаем словарь для быстрого поиска
        keyword_by_name = {kw.name.lower(): kw for kw in keywords}

        # Группируем слова по их исходной форме
        base_groups = defaultdict(list)
        gaming_terms_found = []
        short_words_found = []

        # Показываем пример для fix_specific
        if fix_specific and verbose:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS(f"АНАЛИЗ СЛОВА: {fix_specific}"))
            self.stdout.write("=" * 70)

            # Показываем прямые деривации
            derivations = self.wordnet_api.get_direct_derivations(fix_specific)
            if derivations:
                self.stdout.write(f"\n📌 Прямые деривации для '{fix_specific}':")
                for deriv in sorted(derivations)[:10]:
                    self.stdout.write(f"   • {deriv}")

            # Показываем базовую форму
            base_form = self._get_base_form(fix_specific)
            self.stdout.write(f"\n✅ Базовая форма: '{base_form}'")

        # Анализируем все ключевые слова
        for kw in keywords:
            word_lower = kw.name.lower()

            # Проверяем короткие слова
            if self._is_short_word(word_lower):
                short_words_found.append(kw.name)
                continue

            # Проверяем, не игровой ли это термин
            if self._is_gaming_term(word_lower):
                gaming_terms_found.append(kw.name)
                continue

            base_form = self._get_base_form(word_lower)

            if base_form != word_lower:
                base_groups[base_form].append(kw)

        # Показываем найденные группы
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write(self.style.SUCCESS("НАЙДЕННЫЕ ГРУППЫ СЛОВ"))
        self.stdout.write("=" * 70)

        # Сортируем группы по размеру
        sorted_groups = sorted(base_groups.items(), key=lambda x: len(x[1]), reverse=True)

        for base_form, words in sorted_groups:
            self.stdout.write(f"\n📌 {base_form.upper()} (группа из {len(words)} слов):")

            # Проверяем, есть ли исходная форма в базе
            base_exists = base_form in keyword_by_name

            if base_exists:
                base_word = keyword_by_name[base_form]
                self.stdout.write(
                    f"   ✅ Исходная форма есть: {base_word.name} (ID: {base_word.id}) - игр: {base_word.game_set.count()}")
            else:
                self.stdout.write(f"   ⚠️ Исходной формы '{base_form}' НЕТ в базе")

            # Показываем все слова в группе
            for w in sorted(words, key=lambda x: x.game_set.count(), reverse=True):
                if base_exists and w.name.lower() == base_form:
                    continue
                self.stdout.write(f"   • {w.name} (ID: {w.id}) - игр: {w.game_set.count()}")

        # Показываем статистику по игровым терминам и коротким словам
        if gaming_terms_found:
            self.stdout.write(f"\n🎮 Игровые термины (не нормализуются): {len(gaming_terms_found)}")
            if verbose:
                self.stdout.write(f"   {', '.join(gaming_terms_found[:20])}")

        if short_words_found:
            self.stdout.write(f"\n📏 Короткие слова (не нормализуются): {len(short_words_found)}")

        # Если dry-run, показываем только статистику
        if dry_run:
            elapsed_time = time.time() - start_time
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("СТАТИСТИКА"))
            self.stdout.write("=" * 70)
            self.stdout.write(f"⏱️  Время выполнения: {elapsed_time:.2f} сек")
            self.stdout.write(f"📊 Найдено групп: {len(base_groups)}")

            total_forms = sum(len(words) for words in base_groups.values())
            self.stdout.write(f"📊 Всего слов-форм: {total_forms}")

            self.stdout.write("\n" + self.style.WARNING("🏃 DRY RUN - запустите без --dry-run для применения"))
            return

        # Если fix_specific и не dry_run, показываем только группы и завершаем
        if fix_specific and not dry_run:
            self.stdout.write(
                "\n" + self.style.WARNING(f"🔍 Режим --fix-specific: группы для '{fix_specific}' показаны выше"))
            self.stdout.write(self.style.WARNING("Для применения изменений запустите без --fix-specific"))
            return

        # Применяем изменения (только если не dry_run и не fix_specific)
        if not dry_run and not fix_specific:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ПРИМЕНЕНИЕ ИЗМЕНЕНИЙ"))
            self.stdout.write("=" * 70)

            stats = {
                'renamed': 0,
                'merged': 0,
                'moved_relations': 0
            }

            with transaction.atomic():
                for base_form, words in base_groups.items():
                    base_exists = base_form in keyword_by_name

                    if base_exists:
                        # Исходная форма есть - переносим все связи
                        base_word = keyword_by_name[base_form]
                        forms = [w for w in words if w.name.lower() != base_form]

                        if not forms:
                            continue

                        self.stdout.write(f"\n📌 Группа: {base_form}")
                        self.stdout.write(f"   ✅ Исходная форма: {base_word.name} (ID: {base_word.id})")

                        for form in forms:
                            games = list(form.game_set.all())
                            if games:
                                self.stdout.write(f"   🔄 Переносим {len(games)} игр с '{form.name}'")
                                for game in games:
                                    if not game.keywords.filter(id=base_word.id).exists():
                                        game.keywords.add(base_word)
                                        stats['moved_relations'] += 1

                            form.delete()
                            stats['merged'] += 1
                            self.stdout.write(f"   ✅ Удалена форма '{form.name}'")

                    else:
                        # Исходной формы нет - выбираем самое популярное слово
                        words.sort(key=lambda w: w.game_set.count(), reverse=True)
                        base_word = words[0]
                        other_forms = words[1:]

                        self.stdout.write(f"\n📌 Группа: {base_form}")
                        self.stdout.write(f"   🔄 Выбираем базовым: {base_word.name} (ID: {base_word.id})")

                        # Переименовываем
                        if base_word.name.lower() != base_form:
                            old_name = base_word.name
                            base_word.name = base_form
                            base_word.save()
                            stats['renamed'] += 1
                            self.stdout.write(f"   🔄 Переименовано: '{old_name}' -> '{base_form}'")

                        # Переносим связи с других форм
                        for form in other_forms:
                            games = list(form.game_set.all())
                            if games:
                                self.stdout.write(f"   🔄 Переносим {len(games)} игр с '{form.name}'")
                                for game in games:
                                    if not game.keywords.filter(id=base_word.id).exists():
                                        game.keywords.add(base_word)
                                        stats['moved_relations'] += 1

                            form.delete()
                            stats['merged'] += 1
                            self.stdout.write(f"   ✅ Удалена форма '{form.name}'")

            # Итоговая статистика
            elapsed_time = time.time() - start_time

            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ИТОГОВАЯ СТАТИСТИКА"))
            self.stdout.write("=" * 70)
            self.stdout.write(f"⏱️  Время выполнения: {elapsed_time:.2f} сек")
            self.stdout.write(f"📊 Найдено групп: {len(base_groups)}")
            self.stdout.write(f"📊 Переименовано слов: {stats['renamed']}")
            self.stdout.write(f"📊 Объединено слов: {stats['merged']}")
            self.stdout.write(f"📊 Перенесено связей: {stats['moved_relations']}")

            # Сбрасываем кэш Trie
            self.stdout.write("\n" + self.style.SUCCESS("🔄 Сбрасываем кэш Trie..."))
            try:
                from games.analyze.keyword_trie import KeywordTrieManager
                KeywordTrieManager().clear_cache()
                self.stdout.write(self.style.SUCCESS("✅ Кэш Trie очищен"))
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"⚠️ Не удалось очистить кэш: {e}"))

            # Сбрасываем кэш WordNetAPI
            self.stdout.write(self.style.SUCCESS("🔄 Сбрасываем кэш WordNetAPI..."))
            try:
                self.wordnet_api.clear_cache()
                self.stdout.write(self.style.SUCCESS("✅ Кэш WordNetAPI очищен"))
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"⚠️ Не удалось очистить кэш WordNetAPI: {e}"))

            self.stdout.write("=" * 70)