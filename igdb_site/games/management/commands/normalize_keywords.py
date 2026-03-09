# games/management/commands/normalize_keywords.py
"""
Django команда для нормализации ключевых слов:
- Использует NLTK для определения исходных форм слов
- Объединяет формы слов (drawing → draw, cooking → cook и т.д.)
- Игнорирует специальные игровые термины и аббревиатуры
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Count
from games.models import Keyword
import time
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from collections import defaultdict


class Command(BaseCommand):
    help = 'Нормализует ключевые слова используя NLTK лемматизатор'

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
            help='Исправить конкретное слово (например, "drawing")',
        )

    def _init_nltk(self):
        """Инициализирует NLTK и скачивает необходимые данные"""
        try:
            # Проверяем доступность wordnet
            wordnet.synsets('test')
        except LookupError:
            self.stdout.write("📥 Загружаем WordNet...")
            nltk.download('wordnet', quiet=False)
            nltk.download('omw-1.4', quiet=False)

        self.lemmatizer = WordNetLemmatizer()

    def _is_gaming_term(self, word: str) -> bool:
        """
        Проверяет, является ли слово специальным игровым термином,
        который не нужно нормализовать
        """
        word_lower = word.lower()

        # Игровые аббревиатуры и сокращения (только точные совпадения)
        gaming_abbr = {
            'cod', 'fps', 'rpg', 'mmo', 'rts', 'moba', 'pvp', 'pve',
            'hp', 'mp', 'xp', 'ap', 'dp', 'dps', 'hps', 'gcd', 'cd',
            'boss', 'mob', 'npc', 'pc', 'ai', 'ui', 'gui', 'hud',
            'diy', 'dlc', 'gacha', 'rogue', 'roguelike', 'roguelite'
        }

        # Точное совпадение с аббревиатурами
        if word_lower in gaming_abbr:
            return True

        # Игровые атрибуты и характеристики (только точные совпадения)
        gaming_terms = {
            'stamina', 'mana', 'health', 'armor', 'damage', 'defense', 'attack',
            'strength', 'agility', 'intelligence', 'wisdom', 'charisma', 'luck',
            'skill', 'level', 'exp', 'experience', 'gold', 'coin', 'currency',
            'inventory', 'quest', 'mission', 'achievement', 'trophy',

            # Ресурсы и материалы
            'wood', 'stone', 'iron', 'steel', 'gold', 'silver', 'copper',
            'leather', 'cloth', 'silk', 'wool', 'cotton', 'herb', 'potion',

            # Оружие и экипировка
            'sword', 'axe', 'bow', 'staff', 'wand', 'shield', 'helmet',
            'armor', 'boots', 'gloves', 'ring', 'amulet', 'necklace',

            # ИГРОВЫЕ ТЕРМИНЫ В ФОРМЕ ПРОШЕДШЕГО ВРЕМЕНИ
            'wanted',  # "Most Wanted" - название игры, термин
        }

        # Точное совпадение с игровыми терминами
        if word_lower in gaming_terms:
            return True

        return False

    def _is_short_word(self, word: str) -> bool:
        """
        Проверяет, является ли слово коротким (3 буквы или меньше)
        Такие слова обычно не нормализуем
        """
        word_lower = word.lower()

        # Слова из 3 букв и меньше оставляем как есть
        if len(word_lower) <= 3:
            return True

        # Но есть исключения - короткие слова, которые могут быть формами
        short_forms = {'run', 'ran', 'set', 'sit', 'sat', 'eat', 'ate', 'fly', 'flew'}
        if word_lower in short_forms:
            return False

        return False

    def _get_base_form(self, word: str) -> str:
        """
        Определяет исходную форму слова используя NLTK
        """
        word_lower = word.lower()

        # Пропускаем слова с дефисами и пробелами
        if '-' in word_lower or ' ' in word_lower:
            return word_lower

        # Короткие слова обычно не нормализуем
        if self._is_short_word(word_lower):
            return word_lower

        # Проверяем, не является ли слово специальным игровым термином
        if self._is_gaming_term(word_lower):
            return word_lower

        # ========== СПЕЦИАЛЬНАЯ ОБРАБОТКА ДЛЯ НАРЕЧИЙ НА -ly ==========
        if word_lower.endswith('ly') and len(word_lower) > 3:
            try:
                from nltk.corpus import wordnet as wn

                # Вариант 1: deadly → dead (убираем ly)
                base_candidate1 = word_lower[:-2]
                synsets1 = wn.synsets(base_candidate1)
                if synsets1:
                    # Проверяем, что это прилагательное
                    is_adj = any(s.pos() == 'a' or s.pos() == 's' for s in synsets1)
                    if is_adj:
                        return base_candidate1

                # Вариант 2: happily → happy (ily → y)
                if word_lower.endswith('ily'):
                    base_candidate2 = word_lower[:-3] + 'y'
                    if wn.synsets(base_candidate2):
                        return base_candidate2

                # Вариант 3: basically → basic (убираем ally)
                if word_lower.endswith('ically'):
                    base_candidate3 = word_lower[:-5]
                    if wn.synsets(base_candidate3):
                        return base_candidate3

                # Вариант 4: gently → gentle (tly → tle)
                if word_lower.endswith('tly'):
                    base_candidate4 = word_lower[:-2] + 'le'
                    if wn.synsets(base_candidate4):
                        return base_candidate4

            except:
                # Если WordNet недоступен, просто продолжаем
                pass

        # Пробуем разные части речи через лемматизатор
        verb_form = self.lemmatizer.lemmatize(word_lower, 'v')
        if verb_form != word_lower:
            if not self._is_gaming_term(verb_form) and not self._is_short_word(verb_form):
                return verb_form

        noun_form = self.lemmatizer.lemmatize(word_lower, 'n')
        if noun_form != word_lower:
            if not self._is_gaming_term(noun_form) and not self._is_short_word(noun_form):
                return noun_form

        adj_form = self.lemmatizer.lemmatize(word_lower, 'a')
        if adj_form != word_lower:
            if not self._is_gaming_term(adj_form) and not self._is_short_word(adj_form):
                return adj_form

        adv_form = self.lemmatizer.lemmatize(word_lower, 'r')
        if adv_form != word_lower:
            if not self._is_gaming_term(adv_form) and not self._is_short_word(adv_form):
                return adv_form

        return word_lower

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        verbose = options['verbose']
        fix_specific = options['fix_specific']

        start_time = time.time()

        self.stdout.write("=" * 70)
        self.stdout.write(self.style.SUCCESS("НОРМАЛИЗАЦИЯ КЛЮЧЕВЫХ СЛОВ (NLTK)"))
        self.stdout.write("=" * 70)

        # Инициализируем NLTK
        self._init_nltk()

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

        # Показываем найденные игровые термины
        if gaming_terms_found and verbose:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("ИГРОВЫЕ ТЕРМИНЫ (НЕ НОРМАЛИЗУЮТСЯ)"))
            self.stdout.write("=" * 70)
            for term in sorted(gaming_terms_found):
                self.stdout.write(f"   • {term}")

        # Показываем короткие слова
        if short_words_found and verbose:
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("КОРОТКИЕ СЛОВА (<=3 БУКВ, НЕ НОРМАЛИЗУЮТСЯ)"))
            self.stdout.write("=" * 70)
            for word in sorted(short_words_found):
                self.stdout.write(f"   • {word}")

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

        # Если dry-run, показываем только статистику
        if dry_run or fix_specific:
            elapsed_time = time.time() - start_time
            self.stdout.write("\n" + "=" * 70)
            self.stdout.write(self.style.SUCCESS("СТАТИСТИКА"))
            self.stdout.write("=" * 70)
            self.stdout.write(f"⏱️  Время выполнения: {elapsed_time:.2f} сек")
            self.stdout.write(f"📊 Найдено групп: {len(base_groups)}")

            total_forms = sum(len(words) for words in base_groups.values())
            self.stdout.write(f"📊 Всего слов-форм: {total_forms}")
            self.stdout.write(f"📊 Игровых терминов (пропущено): {len(gaming_terms_found)}")
            self.stdout.write(f"📊 Коротких слов (пропущено): {len(short_words_found)}")

            if dry_run:
                self.stdout.write("\n" + self.style.WARNING("🏃 DRY RUN - запустите без --dry-run для применения"))
            return

        # Применяем изменения
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
        self.stdout.write(f"📊 Игровых терминов (пропущено): {len(gaming_terms_found)}")
        self.stdout.write(f"📊 Коротких слов (пропущено): {len(short_words_found)}")

        # Сбрасываем кэш Trie
        self.stdout.write("\n" + self.style.SUCCESS("🔄 Сбрасываем кэш Trie..."))
        try:
            from games.analyze.keyword_trie import KeywordTrieManager
            KeywordTrieManager().clear_cache()
            self.stdout.write(self.style.SUCCESS("✅ Кэш Trie очищен"))
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"⚠️ Не удалось очистить кэш: {e}"))

        self.stdout.write("=" * 70)