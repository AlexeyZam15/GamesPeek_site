# games/management/commands/normalize_keywords.py
"""
Django команда для нормализации ключевых слов:
- Использует NLTK для определения исходных форм слов
- Объединяет формы слов (drawing → draw, cooking → cook и т.д.)
- Игнорирует специальные игровые термины и аббревиатуры
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Keyword
import time
import nltk
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from collections import defaultdict


class Command(BaseCommand):
    help = 'Нормализует ключевые слова используя NLTK лемматизатор'

    # Константы для семантической проверки
    PATH_SIMILARITY_THRESHOLD = 0.3  # Порог для path similarity (0.167 слишком низкое)
    WUP_SIMILARITY_THRESHOLD = 0.7  # Порог для Wu & Palmer similarity

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

    def _check_semantic_relation(self, word1: str, word2: str) -> bool:
        """
        Проверяет семантическую связь между двумя словами через WordNet
        Возвращает True, если слова связаны (однокоренные или грамматические формы)

        ПОРОГОВЫЕ ЗНАЧЕНИЯ:
        - path_similarity > PATH_SIMILARITY_THRESHOLD (0.3)
        - wup_similarity > WUP_SIMILARITY_THRESHOLD (0.7)
        - общие леммы
        """
        if word1 == word2:
            return True

        try:
            from nltk.corpus import wordnet as wn

            word1_lower = word1.lower()
            word2_lower = word2.lower()

            # Получаем все synsets для обоих слов
            synsets1 = wn.synsets(word1_lower)
            synsets2 = wn.synsets(word2_lower)

            if not synsets1 or not synsets2:
                return False

            # Собираем все леммы для первого слова
            lemmas1 = set()
            for syn in synsets1:
                for lemma in syn.lemmas():
                    lemma_name = lemma.name().lower().replace('_', ' ')
                    lemmas1.add(lemma_name)

            # Собираем все леммы для второго слова
            lemmas2 = set()
            for syn in synsets2:
                for lemma in syn.lemmas():
                    lemma_name = lemma.name().lower().replace('_', ' ')
                    lemmas2.add(lemma_name)

            # Если есть общие леммы, слова связаны
            common_lemmas = lemmas1.intersection(lemmas2)
            if common_lemmas:
                return True

            # Проверяем path similarity для всех пар synsets
            max_path_sim = 0.0
            max_wup_sim = 0.0

            for s1 in synsets1:
                for s2 in synsets2:
                    try:
                        # path similarity от 0 до 1, чем ближе к 1, тем ближе слова
                        path_sim = s1.path_similarity(s2)
                        if path_sim and path_sim > max_path_sim:
                            max_path_sim = path_sim

                        # wup similarity (Wu & Palmer)
                        wup_sim = s1.wup_similarity(s2)
                        if wup_sim and wup_sim > max_wup_sim:
                            max_wup_sim = wup_sim
                    except:
                        continue

            # ПРОВЕРКА ПО ПОРОГОВЫМ ЗНАЧЕНИЯМ
            if max_path_sim > self.PATH_SIMILARITY_THRESHOLD:
                return True

            if max_wup_sim > self.WUP_SIMILARITY_THRESHOLD:
                return True

            return False

        except Exception as e:
            # В случае ошибки считаем, что слова не связаны
            return False

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

        # Игровые термины
        gaming_terms = {
            'wanted',
            'stamina',
            'leveling',
            'hitpoint',
            'manapoint',
            'healthpoint',
            'skillpoint',
            'spellpoint',
            'stat',
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
        ИСПРАВЛЕНО: СНАЧАЛА ПРОВЕРЯЕМ ИСКЛЮЧЕНИЯ
        """
        word_lower = word.lower()

        # ========== СНАЧАЛА ПРОВЕРЯЕМ ИСКЛЮЧЕНИЯ ==========
        exception_result = self._apply_exceptions(word)
        if exception_result is not None:
            return exception_result

        # ========== ПРОВЕРКА ИГРОВЫХ ТЕРМИНОВ ==========
        if self._is_gaming_term(word_lower):
            return word_lower

        # ========== НЕ ОБРАБАТЫВАЕМ КОРОТКИЕ СЛОВА ==========
        if len(word_lower) <= 4:
            return word_lower

        # ========== ОБРАБОТКА ФРАЗ С ПРОБЕЛАМИ ==========
        if ' ' in word_lower:
            parts = word_lower.split()

            if len(parts) == 2:
                first, second = parts
                normalized_second = self._normalize_single_word(second)
                return f"{first} {normalized_second}"

            return word_lower

        # ========== ОБРАБОТКА СОСТАВНЫХ СЛОВ С ДЕФИСАМИ ==========
        if '-' in word_lower:
            parts = word_lower.split('-')

            if len(parts) != 2:
                return word_lower

            first, second = parts
            normalized_second = self._normalize_single_word(second)
            return f"{first}-{normalized_second}"

        # ========== ДЛЯ ОБЫЧНЫХ СЛОВ ==========
        return self._normalize_single_word(word_lower)

    def _is_grammatical_form(self, word: str, base: str) -> bool:
        """
        Проверяет, является ли слово грамматической формой базового слова
        """
        # Множественное число
        if word == base + 's' or word == base + 'es':
            return True
        if base.endswith('y') and word == base[:-1] + 'ies':
            return True

        # -ing формы
        if word == base + 'ing':
            return True
        if base.endswith('e') and word == base[:-1] + 'ing':
            return True

        # -ed формы
        if word == base + 'ed' or word == base + 'd':
            return True
        if base.endswith('y') and word == base[:-1] + 'ied':
            return True

        # -er/-est формы
        if word == base + 'er' or word == base + 'est':
            return True
        if base.endswith('y') and (word == base[:-1] + 'ier' or word == base[:-1] + 'iest'):
            return True

        # -ly формы
        if word == base + 'ly':
            return True
        if base.endswith('y') and word == base[:-1] + 'ily':
            return True

        return False

    def _normalize_by_rules(self, word: str) -> str:
        """
        Нормализует слово по лингвистическим правилам (без WordNet)
        ИСПРАВЛЕНО: ПРАВИЛЬНАЯ ОБРАБОТКА -ness СУФФИКСА
        """
        word_lower = word.lower()

        # ========== СУФФИКС -ness (weightlessness → weightless) ==========
        # Существительные на -ness образуются от прилагательных
        if word_lower.endswith('ness') and len(word_lower) > 5:
            # Убираем "ness"
            base = word_lower[:-4]

            # Проверяем, что основа существует (должна быть длиной не менее 3)
            if len(base) >= 3:
                # Для слов типа weightless → weightless (уже прилагательное)
                # Не нужно дальше нормализовать
                return base

            return word_lower

        # ========== ОШИБОЧНЫЕ ФОРМЫ ==========
        # Если слово заканчивается на "nes" (возможно опечатка от "ness")
        if word_lower.endswith('nes') and len(word_lower) > 4:
            # Может быть опечатка: weightlessnes → weightlessness
            # Но лучше проверить через WordNet
            pass

        # ========== СУФФИКС -ty (safety → safe) ==========
        if word_lower.endswith('ty') and len(word_lower) > 4:
            base = word_lower[:-2]
            if len(base) >= 3:
                # Проверяем, не заканчивается ли основа на 'e'
                if base.endswith('t') and len(base) > 3:
                    # safety → safe (t → te)
                    return base + 'e'
                return base

        # ========== СУФФИКС -ings (buildings → build) ==========
        if word_lower.endswith('ings') and len(word_lower) > 5:
            base = word_lower[:-4]
            if len(base) >= 3:
                return base

        # ========== СУФФИКС -ing ==========
        if word_lower.endswith('ing') and len(word_lower) > 4:
            base = word_lower[:-3]
            # Удвоение согласной (running → run)
            if len(base) >= 2 and base[-1] == base[-2]:
                base = base[:-1]
            if len(base) >= 3:
                return base

        # ========== СУФФИКС -ed ==========
        if word_lower.endswith('ed') and len(word_lower) > 4:
            base = word_lower[:-2]
            # Удвоение согласной (planned → plan)
            if len(base) >= 2 and base[-1] == base[-2]:
                base = base[:-1]
            if len(base) >= 3:
                return base

        # ========== СУФФИКС -er ==========
        if word_lower.endswith('er') and len(word_lower) > 4:
            base = word_lower[:-2]
            if len(base) >= 3:
                return base

        # ========== МНОЖЕСТВЕННОЕ ЧИСЛО ==========
        # -ies (cities → city)
        if word_lower.endswith('ies') and len(word_lower) > 4:
            return word_lower[:-3] + 'y'

        # -es (boxes → box)
        if word_lower.endswith('es') and len(word_lower) > 4:
            base = word_lower[:-2]
            if len(base) >= 3:
                return base

        # -s (cats → cat)
        if word_lower.endswith('s') and len(word_lower) > 3:
            base = word_lower[:-1]
            if len(base) >= 3:
                return base

        return word_lower

    def _apply_exceptions(self, word: str) -> str:
        """
        Применяет специальные исключения для нормализации слов
        Возвращает нормализованное слово или None, если исключение не применимо
        """
        word_lower = word.lower()

        # Словарь исключений: неправильная форма -> правильная форма
        exceptions = {
            # Только два исключения
            'riding': 'ride',
            'coding': 'code',
        }

        # Проверяем точное совпадение
        if word_lower in exceptions:
            return exceptions[word_lower]

        # Для составных слов и фраз
        if ' ' in word_lower or '-' in word_lower:
            # Проверяем, заканчивается ли слово на 'riding' или 'coding'
            if word_lower.endswith('riding'):
                # dragon riding → dragon ride
                base = word_lower[:-6]  # убираем 'riding'
                if base.endswith(' ') or base.endswith('-'):
                    return f"{base}ride"
                return f"{base} ride"

            if word_lower.endswith('coding'):
                # game coding → game code
                base = word_lower[:-6]  # убираем 'coding'
                if base.endswith(' ') or base.endswith('-'):
                    return f"{base}code"
                return f"{base} code"

        return None

    def _normalize_single_word(self, word: str) -> str:
        """
        Нормализует одно слово (без дефисов) используя существующие правила
        ИСПРАВЛЕНО: ИСПОЛЬЗОВАНИЕ PATH SIMILARITY ПОРОГА
        """
        original = word

        # Слова короче 4 символов не нормализуем
        if len(word) <= 4:
            return original

        # Список всех возможных кандидатов
        all_candidates = []

        # ========== ПРОВЕРКА -ing ==========
        if word.endswith('ing') and len(word) > 4:
            base = word[:-3]

            # Просто убираем ing (splattering → splatter)
            all_candidates.append(base)

            # Убираем удвоенную согласную (running → run)
            if len(base) >= 2 and base[-1] == base[-2]:
                all_candidates.append(base[:-1])

            # Добавляем 'e' (changing → change)
            all_candidates.append(base + 'e')

            # Для splatter → splat (убираем ter)
            if base.endswith('ter') and len(base) > 5:
                all_candidates.append(base[:-3])

            # Для слова с удвоением перед ing (splitting → split)
            if len(word) > 6 and word[-4] == word[-5]:
                all_candidates.append(word[:-4])

        # ========== ПРОВЕРКА -ed ==========
        if word.endswith('ed') and len(word) > 4:
            base = word[:-2]

            # Просто убираем ed (played → play)
            all_candidates.append(base)

            # Убираем удвоенную согласную (planned → plan)
            if len(base) >= 2 and base[-1] == base[-2]:
                all_candidates.append(base[:-1])

            # Добавляем 'e' (based → base)
            all_candidates.append(base + 'e')

        # ========== ПРОВЕРКА -er ==========
        if word.endswith('er') and len(word) > 4:
            base = word[:-2]

            # Просто убираем er (parser → pars)
            all_candidates.append(base)

            # Добавляем 'e' (player → play)
            all_candidates.append(base + 'e')

            # Для слов типа runner → run
            if len(base) >= 2 and base[-1] == base[-2]:
                all_candidates.append(base[:-1])

        # ========== ПРОВЕРКА -ly ==========
        if word.endswith('ly') and len(word) > 5:
            base = word[:-2]
            all_candidates.append(base)

            # happily → happy
            if base.endswith('i') and len(base) > 3:
                all_candidates.append(base[:-1] + 'y')

        # ========== ПРОВЕРКА МНОЖЕСТВЕННОГО ЧИСЛА ==========
        if word.endswith('ies') and len(word) > 5:
            # cities → city
            all_candidates.append(word[:-3] + 'y')

        if word.endswith('es') and len(word) > 5:
            # boxes → box
            all_candidates.append(word[:-2])

        if word.endswith('s') and len(word) > 4 and not word.endswith('ss'):
            # cats → cat
            all_candidates.append(word[:-1])

        # ========== ПРОВЕРКА -ness ==========
        if word.endswith('ness') and len(word) > 6:
            # happiness → happy
            base = word[:-4]
            all_candidates.append(base)
            if base.endswith('i'):
                all_candidates.append(base[:-1] + 'y')

        # Убираем дубликаты и пустые строки
        unique_candidates = []
        for candidate in all_candidates:
            if candidate and len(candidate) >= 3 and candidate not in unique_candidates:
                unique_candidates.append(candidate)

        # Если нет кандидатов, возвращаем оригинал
        if not unique_candidates:
            return original

        try:
            from nltk.corpus import wordnet as wn

            # Получаем synsets для исходного слова
            word_synsets = wn.synsets(word.lower())

            best_candidate = None
            best_path_sim = 0.0
            best_candidate_exists = False

            # Проверяем каждого кандидата
            for candidate in unique_candidates:
                candidate_synsets = wn.synsets(candidate.lower())

                # Проверка 1: Существует ли кандидат в WordNet?
                candidate_exists = len(candidate_synsets) > 0

                # Проверка 2: Семантическая близость
                if word_synsets and candidate_synsets:
                    max_path_sim = 0.0
                    for s1 in word_synsets:
                        for s2 in candidate_synsets:
                            try:
                                path_sim = s1.path_similarity(s2)
                                if path_sim and path_sim > max_path_sim:
                                    max_path_sim = path_sim
                            except:
                                continue

                    # Если кандидат существует в WordNet И path similarity выше порога
                    if candidate_exists and max_path_sim > self.PATH_SIMILARITY_THRESHOLD:
                        # Выбираем кандидата с наибольшей path similarity
                        if max_path_sim > best_path_sim:
                            best_path_sim = max_path_sim
                            best_candidate = candidate
                            best_candidate_exists = True

            # Если нашли подходящего кандидата, возвращаем его
            if best_candidate:
                return best_candidate

            # Если ни один кандидат не прошел проверку, возвращаем оригинал
            return original

        except Exception as e:
            # В случае ошибки возвращаем оригинал
            return original

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

            self.stdout.write("=" * 70)
