# games/analyze/keyword_trie.py
import re
from typing import Dict, List, Set, Optional
from django.core.cache import cache
from functools import lru_cache


class KeywordTrieNode:
    """Узел префиксного дерева для ключевых слов"""

    def __init__(self):
        self.children: Dict[str, 'KeywordTrieNode'] = {}
        self.is_end: bool = False
        self.keyword_id: Optional[int] = None
        self.keyword_name: Optional[str] = None
        self.length: int = 0  # Длина ключевого слова


class KeywordTrie:
    """Префиксное дерево для быстрого поиска ключевых слов с поддержкой всех форм слов"""

    def __init__(self, verbose: bool = False):
        self.root = KeywordTrieNode()
        self.keywords_cache: Dict[int, dict] = {}
        self.verbose = verbose
        self._verb_forms_cache: Dict[str, List[str]] = {}  # Кэш глагольных форм
        self._all_forms_cache: Dict[str, Set[str]] = {}  # ДОБАВЛЯЕМ кэш для всех форм

        # Инициализируем WordNet
        self.wordnet_available = self._init_wordnet()

        if verbose:
            if self.wordnet_available:
                print("✅ WordNet доступен для определения глаголов")
            else:
                print("⚠️ WordNet недоступен. Глагольные формы определяться не будут.")

    def _generate_single_word_forms(self, word: str) -> Set[str]:
        """
        Генерирует все формы для одного слова (без обработки дефисов)
        Используется для частей составных слов
        ИСПРАВЛЕНО: Добавлена генерация всех необходимых форм
        """
        forms = {word}

        if len(word) < 3:
            return forms

        # ========== МНОЖЕСТВЕННОЕ ЧИСЛО ==========
        if word.endswith('y') and len(word) > 1 and word[-2] not in 'aeiou':
            forms.add(word[:-1] + 'ies')
        forms.add(word + 's')

        if word.endswith(('s', 'x', 'z', 'ch', 'sh')):
            forms.add(word + 'es')

        # ========== -ing ФОРМЫ ==========
        if self._should_double_consonant(word):
            forms.add(word + word[-1] + 'ing')
        else:
            forms.add(word + 'ing')

        if word.endswith('e'):
            forms.add(word[:-1] + 'ing')

        # ========== -ed ФОРМЫ (ЭТО КЛЮЧЕВОЕ ДЛЯ base → based) ==========
        if word.endswith('e'):
            # Для слов на e добавляем только d
            forms.add(word + 'd')
        else:
            if self._should_double_consonant(word):
                forms.add(word + word[-1] + 'ed')
            forms.add(word + 'ed')

        # Формы на -ed для слов, заканчивающихся на -y
        if word.endswith('y') and len(word) > 3 and word[-2] not in 'aeiou':
            forms.add(word[:-1] + 'ied')

        # ========== -er ФОРМЫ ==========
        if word.endswith('e'):
            forms.add(word[:-1] + 'er')
        else:
            if self._should_double_consonant(word):
                forms.add(word + word[-1] + 'er')
            forms.add(word + 'er')

        # ========== -est ФОРМЫ ==========
        if word.endswith('e'):
            forms.add(word[:-1] + 'est')
        else:
            if self._should_double_consonant(word):
                forms.add(word + word[-1] + 'est')
            forms.add(word + 'est')

        # ========== -ly ФОРМЫ ==========
        if word.endswith('y') and word[-2] not in 'aeiou':
            forms.add(word[:-1] + 'ily')
        elif word.endswith('le'):
            forms.add(word[:-2] + 'ly')
        elif word.endswith('ic'):
            forms.add(word + 'ally')
        else:
            forms.add(word + 'ly')

        # ========== -tion/-sion ФОРМЫ ==========
        if word.endswith('e'):
            forms.add(word[:-1] + 'tion')
        else:
            forms.add(word + 'tion')

        if word.endswith('de'):
            forms.add(word[:-2] + 'sion')
        elif word.endswith('d'):
            forms.add(word + 'sion')

        # ========== -ive ФОРМЫ ==========
        if word.endswith('e'):
            forms.add(word[:-1] + 'ive')
        else:
            forms.add(word + 'ive')

        return forms

    def _generate_all_forms(self, word: str) -> Set[str]:
        """
        Генерирует ВСЕ возможные формы слова (существительные, глаголы, прилагательные, наречия)
        ИСПРАВЛЕНО: ТЕПЕРЬ ГЕНЕРИРУЕТ -ty ФОРМЫ (safe → safety) И ОБРАБАТЫВАЕТ ФРАЗЫ
        """
        word_lower = word.lower()

        # Проверяем кэш
        if word_lower in self._all_forms_cache:
            return self._all_forms_cache[word_lower]

        forms = set()

        # Базовая форма всегда добавляется
        forms.add(word_lower)

        # ========== ОБРАБОТКА ФРАЗ С ПРОБЕЛАМИ ==========
        if ' ' in word_lower:
            # Разбиваем фразу на отдельные слова
            words = word_lower.split()

            # Если фраза из 2 слов, генерируем комбинации форм
            if len(words) == 2:
                word1, word2 = words

                # Генерируем формы для каждого слова отдельно
                forms1 = self._generate_single_word_forms(word1)
                forms2 = self._generate_single_word_forms(word2)

                # Создаем все комбинации
                for f1 in forms1:
                    for f2 in forms2:
                        combined = f"{f1} {f2}"
                        if 3 <= len(combined) <= 50:
                            forms.add(combined)

            # Добавляем вариант без пробела (как одно слово)
            forms.add(word_lower.replace(' ', ''))

            # Добавляем вариант с дефисом
            forms.add(word_lower.replace(' ', '-'))

            # Фильтруем
            valid_forms = {f for f in forms if 3 <= len(f) <= 50}
            self._all_forms_cache[word_lower] = valid_forms
            return valid_forms

        # ========== ОБРАБОТКА СОСТАВНЫХ СЛОВ С ДЕФИСАМИ ==========
        if '-' in word_lower:
            parts = word_lower.split('-')
            forms.add(word_lower)
            forms.add(' '.join(parts))
            forms.add(''.join(parts))

            for i, part in enumerate(parts):
                if len(part) >= 3:
                    part_forms = self._generate_single_word_forms(part)
                    for part_form in part_forms:
                        if part_form != part:
                            new_parts = parts.copy()
                            new_parts[i] = part_form
                            new_word = '-'.join(new_parts)
                            forms.add(new_word)
                            forms.add(' '.join(new_parts))
                            forms.add(''.join(new_parts))

            valid_forms = {f for f in forms if 3 <= len(f) <= 30}
            self._all_forms_cache[word_lower] = valid_forms
            return valid_forms

        # ========== ДЛЯ ОБЫЧНЫХ СЛОВ ==========

        # Если слово короткое (3 буквы и меньше)
        if len(word_lower) <= 3:
            self._all_forms_cache[word_lower] = {word_lower}
            return {word_lower}

        # ========== ФОРМЫ С СУФФИКСОМ -ty (safe → safety) ==========
        # Прилагательные превращаются в существительные на -ty/-ity/-ety
        if len(word_lower) >= 4:
            # Правило 1: добавляем ty (cruel → cruelty)
            forms.add(word_lower + 'ty')

            # Правило 2: добавляем ity (intense → intensity)
            forms.add(word_lower + 'ity')

            # Правило 3: добавляем ety (various → variety? нет, various → variety)
            forms.add(word_lower + 'ety')

            # Правило 4: если слово заканчивается на e, убираем e и добавляем ity
            # safe → saf + ity → safety
            if word_lower.endswith('e'):
                forms.add(word_lower[:-1] + 'ity')
                forms.add(word_lower[:-1] + 'ety')

            # Правило 5: если слово заканчивается на us, меняем на ity
            # various → vari + ety? нет, various → variety
            if word_lower.endswith('ous'):
                # various → vari + ety? нет, various → variety (убираем ous, добавляем iety)
                forms.add(word_lower[:-3] + 'iety')

        # ========== МНОЖЕСТВЕННОЕ ЧИСЛО ==========
        if word_lower.endswith('y') and len(word_lower) > 1 and word_lower[-2] not in 'aeiou':
            forms.add(word_lower[:-1] + 'ies')
        forms.add(word_lower + 's')

        if word_lower.endswith(('s', 'x', 'z', 'ch', 'sh')):
            forms.add(word_lower + 'es')

        # ========== ГЛАГОЛЬНЫЕ ФОРМЫ ==========

        # Формы на -ing
        if len(word_lower) >= 4:
            if self._should_double_consonant(word_lower):
                forms.add(word_lower + word_lower[-1] + 'ing')
            else:
                forms.add(word_lower + 'ing')

            if word_lower.endswith('e') and len(word_lower) >= 4:
                forms.add(word_lower[:-1] + 'ing')

        # Формы на -ed для слов, заканчивающихся на -y
        if word_lower.endswith('y') and len(word_lower) > 3 and word_lower[-2] not in 'aeiou':
            forms.add(word_lower[:-1] + 'ied')

        # Обычные формы на -ed (ЭТО КЛЮЧЕВОЕ ДЛЯ base → based)
        if word_lower.endswith('e'):
            # Для слов на e добавляем только d
            forms.add(word_lower + 'd')
        else:
            if self._should_double_consonant(word_lower):
                forms.add(word_lower + word_lower[-1] + 'ed')
            forms.add(word_lower + 'ed')

        # ========== ФОРМЫ С СУФФИКСАМИ -er, -or ==========
        if len(word_lower) >= 4:
            forms.add(word_lower + 'er')
            if word_lower.endswith('e'):
                forms.add(word_lower[:-1] + 'er')
            forms.add(word_lower + 'or')
            if word_lower.endswith('e'):
                forms.add(word_lower[:-1] + 'or')

        # ========== ФОРМЫ МНОЖЕСТВЕННОГО ЧИСЛА ОТ -ing ФОРМ ==========
        if len(word_lower) >= 4:
            ing_form = word_lower + 'ing'
            if word_lower.endswith('e'):
                ing_form = word_lower[:-1] + 'ing'
            forms.add(ing_form + 's')

        er_form = word_lower + 'er'
        if word_lower.endswith('e'):
            er_form = word_lower[:-1] + 'er'
        forms.add(er_form + 's')

        # ========== ФОРМЫ С СУФФИКСАМИ -tion, -sion ==========
        if len(word_lower) >= 4:
            if word_lower.endswith('e'):
                forms.add(word_lower[:-1] + 'tion')
            else:
                forms.add(word_lower + 'tion')

            if word_lower.endswith('de'):
                forms.add(word_lower[:-2] + 'sion')
            elif word_lower.endswith('d'):
                forms.add(word_lower + 'sion')

        # ========== ФОРМЫ С СУФФИКСАМИ -ive, -ative ==========
        if len(word_lower) >= 4:
            if word_lower.endswith('e'):
                forms.add(word_lower[:-1] + 'ive')
            else:
                forms.add(word_lower + 'ive')
            forms.add(word_lower + 'ative')

        # ========== ПРИЛАГАТЕЛЬНЫЕ НА -y ==========
        if len(word_lower) >= 4:
            forms.add(word_lower + 'y')
            if word_lower.endswith('e'):
                forms.add(word_lower[:-1] + 'y')
            forms.add(word_lower + 'ier')
            forms.add(word_lower + 'iest')

        # ========== НАРЕЧИЯ НА -ly ==========
        if len(word_lower) >= 4:
            forms.add(word_lower + 'ly')
            if word_lower.endswith('y') and word_lower[-2] not in 'aeiou':
                forms.add(word_lower[:-1] + 'ily')
            if word_lower.endswith('le'):
                forms.add(word_lower[:-2] + 'ly')
            if word_lower.endswith('ic'):
                forms.add(word_lower + 'ally')

        # ========== ПРИЛАГАТЕЛЬНЫЕ НА -ful ==========
        if len(word_lower) >= 4:
            forms.add(word_lower + 'ful')
            if word_lower.endswith('ll'):
                forms.add(word_lower[:-1] + 'ful')
            if word_lower.endswith('y') and word_lower[-2] not in 'aeiou':
                forms.add(word_lower[:-1] + 'iful')

        # ========== ПРИЛАГАТЕЛЬНЫЕ НА -al, -ial, -ual ==========
        if len(word_lower) >= 4:
            forms.add(word_lower + 'al')
            if word_lower.endswith('tion'):
                forms.add(word_lower + 'al')
            if word_lower.endswith('ic'):
                forms.add(word_lower + 'al')
            forms.add(word_lower + 'ual')
            forms.add(word_lower + 'ial')

        # ========== ФОРМЫ С ПРЕФИКСАМИ ==========
        if len(word_lower) >= 4:
            forms.add('re' + word_lower)
            forms.add('pre' + word_lower)
            forms.add('over' + word_lower)
            forms.add('under' + word_lower)
            forms.add('sub' + word_lower)
            forms.add('super' + word_lower)
            forms.add('post' + word_lower)
            forms.add('anti' + word_lower)
            forms.add('counter' + word_lower)

        # ========== ФИЛЬТРАЦИЯ ==========
        valid_forms = set()
        for form in forms:
            if 3 <= len(form) <= 50:
                if all(c.isalpha() or c in ' -' for c in form):
                    valid_forms.add(form)

        # Сохраняем в кэш
        self._all_forms_cache[word_lower] = valid_forms

        return valid_forms

    def _generate_ed_forms(self, word: str) -> Set[str]:
        """
        Генерирует формы с суффиксом -ed
        """
        forms = set()
        word_lower = word.lower()

        if len(word_lower) < 3:
            return forms

        # Обычное добавление -ed
        if word_lower.endswith('e'):
            # Уже заканчивается на e, добавляем только d
            forms.add(word_lower + 'd')
        else:
            # Проверяем, нужно ли удвоение согласной
            if self._should_double_consonant(word_lower):
                forms.add(word_lower + word_lower[-1] + 'ed')
            forms.add(word_lower + 'ed')

        # Специальные случаи
        if word_lower.endswith('y') and word_lower[-2] not in 'aeiou':
            # y меняется на i перед ed (carry → carried)
            forms.add(word_lower[:-1] + 'ied')

        return forms

    def _init_wordnet(self) -> bool:
        """Инициализирует WordNet для определения глаголов"""
        try:
            import nltk
            from nltk.corpus import wordnet as wn

            # Проверяем, есть ли уже данные WordNet
            try:
                # Быстрая проверка доступности WordNet
                wn.synsets('test', pos='n')
                return True
            except LookupError:
                # Скачиваем только если нет
                if self.verbose:
                    print("   📥 Загружаем WordNet (только один раз)...")

                # Скачиваем минимально необходимые данные
                nltk.download('wordnet', quiet=not self.verbose)

                # Проверяем что скачалось
                wn.synsets('test', pos='n')
                return True

        except Exception as e:
            if self.verbose:
                print(f"⚠️ WordNet недоступен: {e}")
            return False

    @lru_cache(maxsize=10000)
    def _is_verb_wordnet(self, word: str) -> bool:
        """
        Использует WordNet для определения, является ли слово глаголом
        """
        if not self.wordnet_available:
            return False

        try:
            from nltk.corpus import wordnet as wn

            # Получаем все synsets для слова
            synsets = wn.synsets(word.lower())

            # Проверяем, есть ли среди synsets глаголы
            for synset in synsets:
                if synset.pos() == 'v':  # 'v' означает глагол
                    return True

            return False

        except Exception as e:
            if self.verbose:
                print(f"⚠️ Ошибка WordNet для '{word}': {e}")
            return False

    def _should_double_consonant(self, word: str) -> bool:
        """
        Проверяет, нужно ли удваивать последнюю согласную перед -ing/-ed
        """
        if len(word) < 3:
            return False

        vowels = set('aeiou')
        word_lower = word.lower()

        # Более точное правило для удвоения согласных
        last_three = word_lower[-3:]

        # Проверяем шаблон CVC (согласная-гласная-согласная)
        if (last_three[0] not in vowels and  # C - согласная
                last_three[1] in vowels and  # V - гласная
                last_three[2] not in vowels and  # C - согласная
                last_three[2] not in ('w', 'x', 'y')):  # некоторые согласные не удваиваются

            # Проверяем, односложное ли слово или ударение на последнем слоге
            # Для простоты считаем что если слово короткое (<= 4 буквы), то удваиваем
            if len(word_lower) <= 4:
                return True

            # Для более длинных слов проверяем есть ли другие гласные
            vowel_count = sum(1 for char in word_lower if char in vowels)
            if vowel_count == 1:  # Одна гласная - односложное слово
                return True

        return False

    def _is_word_boundary(self, text: str, start: int, end: int) -> bool:
        """
        Проверяет границы слова
        """
        # Проверяем начало
        if start > 0:
            prev_char = text[start - 1]
            if prev_char.isalnum() or prev_char == '_':
                return False

        # Проверяем конец
        if end < len(text):
            next_char = text[end]

            # Допускаем дефис после слова
            if next_char == '-':
                return True

            # Допускаем апостроф после слова
            if next_char == '\'':
                return True

            # Допускаем множественные формы с 's'
            if next_char == 's':
                # Проверяем, что после 's' не идет буква или это конец слова
                if end + 1 >= len(text):
                    return True
                char_after_s = text[end + 1]
                if not char_after_s.isalnum():
                    return True
                return False

            # Любой другой буквенно-цифровой символ или подчеркивание не допустим
            if next_char.isalnum() or next_char == '_':
                return False

        return True

    def find_all_in_text(self, text: str, unique_only: bool = True) -> List[dict]:
        """
        Находит ключевые слова в тексте
        ИСПРАВЛЕНО: Сначала ищет фразы (с пробелами), потом отдельные слова
        """
        import re

        text_lower = text.lower()

        # Находим все слова, исключая кавычки из токенов
        words_with_positions = []
        i = 0
        while i < len(text_lower):
            if text_lower[i] in '\'"':
                i += 1
                continue

            start = i
            while i < len(text_lower) and (text_lower[i].isalnum() or text_lower[i] == '-'):
                i += 1

            if i > start:
                words_with_positions.append({
                    'word': text_lower[start:i],
                    'start': start,
                    'end': i
                })
            else:
                i += 1

        if unique_only:
            found_keywords = set()
        else:
            found_keywords = None

        results = []
        occupied_positions = []

        # ========== СОРТИРУЕМ КЛЮЧЕВЫЕ СЛОВА ПО ДЛИНЕ (СНАЧАЛА САМЫЕ ДЛИННЫЕ) ==========
        sorted_keywords = sorted(
            self.keywords_cache.items(),
            key=lambda item: len(item[1]['name_lower']),
            reverse=True
        )

        # ========== СНАЧАЛА ИЩЕМ ФРАЗЫ (С ПРОБЕЛАМИ) ==========
        for keyword_id, keyword_data in sorted_keywords:
            keyword_lower = keyword_data['name_lower']

            # Получаем все формы ключевого слова
            all_forms = self._all_forms_cache.get(keyword_lower, {keyword_lower})

            # Для каждой формы ищем в тексте
            for form in all_forms:
                if ' ' in form:  # Ищем только формы с пробелами
                    pos = 0
                    while True:
                        pos = text_lower.find(form, pos)
                        if pos == -1:
                            break

                        start_ok = (pos == 0 or not text_lower[pos - 1].isalnum())
                        end_pos = pos + len(form)
                        end_ok = (end_pos == len(text_lower) or not text_lower[end_pos].isalnum())

                        if start_ok and end_ok:
                            is_occupied = False
                            for occ_start, occ_end in occupied_positions:
                                if not (end_pos <= occ_start or pos >= occ_end):
                                    is_occupied = True
                                    break

                            if not is_occupied:
                                result = {
                                    'id': keyword_id,
                                    'name': keyword_data['name'],
                                    'position': pos,
                                    'length': len(form),
                                    'text': text_lower[pos:end_pos],
                                    'is_phrase': True
                                }

                                if unique_only:
                                    if result['id'] not in found_keywords:
                                        found_keywords.add(result['id'])
                                        results.append(result)
                                        occupied_positions.append((pos, end_pos))
                                else:
                                    results.append(result)
                                    occupied_positions.append((pos, end_pos))

                        pos = end_pos

        # ========== ТЕПЕРЬ ИЩЕМ ОТДЕЛЬНЫЕ СЛОВА (ТОЖЕ ОТ ДЛИННЫХ К КОРОТКИМ) ==========
        # Сначала собираем все возможные совпадения для каждого слова
        word_matches = []

        for word_info in words_with_positions:
            full_word = word_info['word']
            word_start = word_info['start']
            word_end = word_info['end']

            # Проверяем, не занята ли позиция
            is_occupied = False
            for occ_start, occ_end in occupied_positions:
                if not (word_end <= occ_start or word_start >= occ_end):
                    is_occupied = True
                    break

            if is_occupied:
                continue

            # Проверяем полное слово в Trie
            node = self.root
            found = True
            for char in full_word:
                if char in node.children:
                    node = node.children[char]
                else:
                    found = False
                    break

            if found and node.is_end and node.keyword_id:
                if not (unique_only and node.keyword_id in found_keywords):
                    word_matches.append({
                        'id': node.keyword_id,
                        'name': node.keyword_name,
                        'position': word_start,
                        'length': len(full_word),
                        'text': full_word,
                        'word_length': len(full_word)
                    })
                continue

            # Проверка составных слов через дефис
            if '-' in full_word:
                parts = full_word.split('-')
                current_pos = word_start

                for part in parts:
                    if len(part) >= 2:
                        part_end = current_pos + len(part)

                        is_part_occupied = False
                        for occ_start, occ_end in occupied_positions:
                            if not (part_end <= occ_start or current_pos >= occ_end):
                                is_part_occupied = True
                                break

                        if is_part_occupied:
                            current_pos += len(part) + 1
                            continue

                        node = self.root
                        found = True
                        for char in part:
                            if char in node.children:
                                node = node.children[char]
                            else:
                                found = False
                                break

                        if found and node.is_end and node.keyword_id:
                            if not (unique_only and node.keyword_id in found_keywords):
                                word_matches.append({
                                    'id': node.keyword_id,
                                    'name': node.keyword_name,
                                    'position': current_pos,
                                    'length': len(part),
                                    'text': part,
                                    'word_length': len(part),
                                    'matched_in_hyphenated': True,
                                    'full_word': full_word
                                })

                    current_pos += len(part) + 1

        # Сортируем совпадения по длине (сначала самые длинные)
        word_matches.sort(key=lambda x: x['word_length'], reverse=True)

        # Добавляем отсортированные совпадения
        for match in word_matches:
            if unique_only and match['id'] in found_keywords:
                continue

            results.append(match)
            if unique_only:
                found_keywords.add(match['id'])
            occupied_positions.append((match['position'], match['position'] + match['length']))

        return results

    def _is_valid_suffix(self, suffix: str) -> bool:
        """
        Проверяет, является ли строка допустимым английским суффиксом
        """
        if not suffix:
            return True

        # Распространенные английские суффиксы
        common_suffixes = {
            's', 'es', 'ies', 'ed', 'ing', 'er', 'est',
            'ly', 'ness', 'ment', 'tion', 'sion', 'able',
            'ible', 'al', 'ial', 'ual', 'ive', 'ative',
            'ful', 'less', 'ous', 'ious', 'en', 'ize',
            'ise', 'ify', 'fy', 'ward', 'wise', 'fold'
        }

        if suffix in common_suffixes:
            return True

        # Проверяем по длине (разумные суффиксы не длиннее 6 символов)
        if len(suffix) > 6:
            return False

        # Проверяем, состоит ли суффикс только из букв
        if not suffix.isalpha():
            return False

        return True

    def _is_valid_prefix(self, prefix: str) -> bool:
        """
        Проверяет, является ли строка допустимым английским префиксом
        """
        if not prefix:
            return True

        # Распространенные английские префиксы
        common_prefixes = {
            're', 'pre', 'post', 'over', 'under', 'sub', 'super',
            'un', 'in', 'im', 'il', 'ir', 'dis', 'non', 'anti',
            'de', 'bi', 'tri', 'multi', 'semi', 'mini', 'micro',
            'macro', 'hyper', 'hypo', 'inter', 'intra', 'extra'
        }

        if prefix in common_prefixes:
            return True

        # Проверяем по длине
        if len(prefix) > 5:
            return False

        # Проверяем, состоит ли префикс только из букв
        if not prefix.isalpha():
            return False

        return True

    def find_all_occurrences_in_text(self, text: str, search_text: str, name: str, category: str) -> List[Dict]:
        """
        Находит все вхождения текста в строке
        """
        occurrences = []
        if not search_text or not text:
            return occurrences

        search_lower = search_text.lower()
        text_lower = text.lower()
        search_len = len(search_text)

        pos = 0
        while True:
            # Ищем вхождение
            found_pos = text_lower.find(search_lower, pos)
            if found_pos == -1:
                break

            # Проверяем границы слова с поддержкой дефисов
            is_valid = True

            # Проверяем начало
            if found_pos > 0:
                prev_char = text[found_pos - 1]
                if prev_char.isalnum() and prev_char != '-':
                    is_valid = False

            # Проверяем конец
            end_pos = found_pos + search_len
            if end_pos < len(text):
                next_char = text[end_pos]
                # Допускаем дефис, апостроф, или конец слова
                if next_char.isalnum() and next_char not in "s'-":
                    is_valid = False
                # Разрешаем 's' только если дальше не буква
                elif next_char == 's':
                    if end_pos + 1 < len(text) and text[end_pos + 1].isalnum():
                        is_valid = False

            if is_valid:
                occurrences.append({
                    'start': found_pos,
                    'end': end_pos,
                    'name': name,
                    'category': category,
                    'text': text[found_pos:end_pos]
                })

            pos = found_pos + 1

        return occurrences

    def insert(self, word: str, keyword_id: int, keyword_name: str):
        """Вставляет ключевое слово в дерево"""
        node = self.root
        word_lower = word.lower()

        for char in word_lower:
            if char not in node.children:
                node.children[char] = KeywordTrieNode()
            node = node.children[char]

        node.is_end = True
        node.keyword_id = keyword_id
        node.keyword_name = keyword_name
        node.length = len(word_lower)

    def build_from_queryset(self, keywords_queryset):
        """Строит дерево из QuerySet ключевых слов"""
        self.keywords_cache.clear()
        self._all_forms_cache.clear()
        self._verb_forms_cache.clear()

        if self.verbose:
            print(f"🔨 Строим Trie из {keywords_queryset.count()} ключевых слов...")

        count = 0
        total_forms = 0

        for keyword in keywords_queryset:
            keyword_name_lower = keyword.name.lower()

            # Всегда добавляем оригинальное ключевое слово
            self.insert(keyword_name_lower, keyword.id, keyword.name)
            total_forms += 1

            # Генерируем дополнительные формы для ВСЕХ ключевых слов (включая фразы)
            if len(keyword_name_lower) > 3:
                forms = self._generate_all_forms(keyword_name_lower)
                for form in forms:
                    if form != keyword_name_lower:
                        self.insert(form, keyword.id, keyword.name)
                        total_forms += 1

            # Сохраняем в кэш
            self.keywords_cache[keyword.id] = {
                'id': keyword.id,
                'name': keyword.name,
                'name_lower': keyword.name.lower()
            }

            count += 1
            if self.verbose and count % 1000 == 0:
                print(f"  Загружено {count} ключевых слов ({total_forms} форм)...")

        if self.verbose:
            print(f"✅ Trie построен: {count} ключевых слов, {total_forms} форм")
            print(f"   Среднее количество форм на слово: {total_forms / count:.1f}")

    def get_keyword_by_id(self, keyword_id: int) -> Optional[dict]:
        """Быстро получает ключевое слово по ID"""
        return self.keywords_cache.get(keyword_id)


class KeywordTrieManager:
    """Менеджер для работы с Trie ключевых слов"""

    _instance = None
    _trie_cache_key = 'keyword_trie_data'

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        if not hasattr(self, 'initialized'):
            self.trie = None
            self.keywords_count = 0
            self.last_update = None
            self.initialized = False

    def get_trie(self, verbose: bool = False, force_rebuild: bool = False) -> KeywordTrie:
        """Получает или создает Trie ключевых слов с кэшированием"""
        from games.models import Keyword
        from django.db.models import Max

        # Проверяем актуальность
        current_count = Keyword.objects.count()

        # Если Trie еще не создан или изменилось количество ключевых слов
        if force_rebuild or self.trie is None or self.keywords_count != current_count:
            if verbose:
                print("♻️ Обновляем Trie ключевых слов...")

            # Пробуем получить из кэша
            cached_trie = cache.get(self._trie_cache_key)
            if cached_trie and not force_rebuild:
                self.trie = cached_trie
                self.keywords_count = current_count
                if verbose:
                    print(f"✅ Trie загружен из кэша ({current_count} ключевых слов)")
            else:
                # Строим новое дерево
                self.trie = KeywordTrie(verbose=verbose)
                keywords = Keyword.objects.only('id', 'name').order_by('name')
                self.trie.build_from_queryset(keywords)
                self.keywords_count = current_count

                # Кэшируем на 1 час
                cache.set(self._trie_cache_key, self.trie, 3600)

                if verbose:
                    print(f"✅ Trie построен и закэширован ({current_count} ключевых слов)")

        return self.trie

    def clear_cache(self):
        """Очищает кэш Trie"""
        cache.delete(self._trie_cache_key)
        self.trie = None
        self.keywords_count = 0
        self.initialized = False