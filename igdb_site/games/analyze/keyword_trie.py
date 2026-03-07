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

    def _generate_all_forms(self, word: str) -> Set[str]:
        """
        Генерирует ВСЕ возможные формы слова (существительные, глаголы, прилагательные)
        ИСПРАВЛЕНО: гарантированно генерирует форму interwoven для interweave
        """
        word_lower = word.lower()

        # Проверяем кэш
        if word_lower in self._all_forms_cache:
            return self._all_forms_cache[word_lower]

        forms = set()

        # Базовая форма всегда добавляется
        forms.add(word_lower)

        # ========== МНОЖЕСТВЕННОЕ ЧИСЛО ==========
        # Правило 1: y → ies (army → armies)
        if word_lower.endswith('y') and len(word_lower) > 1 and word_lower[-2] not in 'aeiou':
            forms.add(word_lower[:-1] + 'ies')

        # Правило 2: стандартное s
        if len(word_lower) > 1:
            forms.add(word_lower + 's')

        # Правило 3: es для окончаний s, x, z, ch, sh
        if word_lower.endswith(('s', 'x', 'z', 'ch', 'sh')):
            forms.add(word_lower + 'es')

        # ========== СПЕЦИАЛЬНЫЕ СЛУЧАИ ДЛЯ НЕПРАВИЛЬНЫХ ГЛАГОЛОВ ==========

        # Специальный случай 1: слова, оканчивающиеся на 'eave' → 'oven' (weave → woven, interweave → interwoven)
        if word_lower.endswith('eave'):
            # weave → woven, interweave → interwoven
            base = word_lower[:-4]  # убираем 'eave'
            if base:  # для interweave: inter + woven
                forms.add(base + 'oven')  # interwoven
            else:  # для weave: woven
                forms.add('woven')

            # Также добавляем форму с 'ing' (weaving)
            forms.add(word_lower[:-1] + 'ing')  # weaving
            # И форму с 'ed' (weaved)
            forms.add(word_lower + 'd')  # weaved

            # Добавляем форму с 'en' (woven) - уже добавлено выше как 'woven'

        # Специальный случай 2: слова, оканчивающиеся на 'ing' (отглагольные формы)
        if word_lower.endswith('ing') and len(word_lower) > 4:
            base = word_lower[:-3]  # убираем 'ing'
            # Проверяем удвоение согласных (stopping → stop)
            if len(base) > 1 and base[-1] == base[-2]:
                forms.add(base[:-1])  # stopping → stop
            else:
                forms.add(base)  # playing → play

                # Если основа заканчивается на 'e', убираем её (making → make)
                if base.endswith('k') and word_lower.endswith('king'):
                    forms.add(base + 'e')  # making → make

        # Специальный случай 3: слова, оканчивающиеся на 'en' (причастия)
        if word_lower.endswith('en') and len(word_lower) > 3:
            base = word_lower[:-2]  # убираем 'en'
            forms.add(base)  # woven → weav?
            # Проверяем, нужно ли добавить 'e'
            if base.endswith('v'):
                forms.add(base + 'e')  # woven → weave

        # ========== ГЛАГОЛЬНЫЕ ФОРМЫ ==========
        # Проверяем через WordNet, является ли слово глаголом
        is_verb = self._is_verb_wordnet(word_lower)

        if is_verb:
            # Формы прошедшего времени и причастия
            if word_lower.endswith('e'):
                # explore → explored, exploring
                forms.add(word_lower + 'd')
                forms.add(word_lower[:-1] + 'ing')
            elif word_lower.endswith('y') and len(word_lower) > 1 and word_lower[-2] not in 'aeiou':
                # try → tried, trying
                forms.add(word_lower[:-1] + 'ied')
                forms.add(word_lower + 'ing')
            elif word_lower.endswith('ie'):
                # die → died, dying
                forms.add(word_lower + 'd')
                forms.add(word_lower[:-2] + 'ying')
            elif self._should_double_consonant(word_lower):
                # stop → stopped, stopping
                doubled = word_lower + word_lower[-1]
                forms.add(doubled + 'ed')
                forms.add(doubled + 'ing')
            else:
                # play → played, playing
                forms.add(word_lower + 'ed')
                forms.add(word_lower + 'ing')

            # Формы 3-го лица единственного числа
            if word_lower.endswith(('s', 'x', 'z', 'ch', 'sh')):
                forms.add(word_lower + 'es')
            elif word_lower.endswith('y') and len(word_lower) > 1 and word_lower[-2] not in 'aeiou':
                forms.add(word_lower[:-1] + 'ies')
            elif word_lower.endswith('o'):
                forms.add(word_lower + 'es')
            else:
                forms.add(word_lower + 's')

        # ========== СУЩЕСТВИТЕЛЬНЫЕ ОТ ГЛАГОЛОВ ==========
        # Существительные на -tion/-ation (explore → exploration)
        if word_lower.endswith('e'):
            # Убираем 'e' и добавляем 'ation'
            forms.add(word_lower[:-1] + 'ation')  # explore → exploration
            # Также добавляем 'tion' для слов типа 'act' → 'action'
            forms.add(word_lower + 'tion')
        else:
            forms.add(word_lower + 'tion')

        # Существительные на -er/-or (исполнитель действия)
        forms.add(word_lower + 'r')  # explore → explorer
        forms.add(word_lower + 'er')  # explore → explorer (альтернатива)
        if word_lower.endswith('t'):
            forms.add(word_lower + 'or')  # act → actor

        # ========== ПРИЛАГАТЕЛЬНЫЕ ==========
        # Прилагательные на -ive (explore → explorative)
        if word_lower.endswith('e'):
            forms.add(word_lower[:-1] + 'ive')
            forms.add(word_lower[:-1] + 'ative')
        else:
            forms.add(word_lower + 'ive')

        # Прилагательные на -ory (explore → exploratory)
        if word_lower.endswith('e'):
            forms.add(word_lower[:-1] + 'ory')
            forms.add(word_lower[:-1] + 'atory')
        else:
            forms.add(word_lower + 'ory')

        # Прилагательные на -able (explore → explorable)
        if word_lower.endswith('e'):
            forms.add(word_lower[:-1] + 'able')
        else:
            forms.add(word_lower + 'able')

        # ========== ФИЛЬТРАЦИЯ ==========
        # Убираем слишком короткие или длинные формы
        valid_forms = set()
        for form in forms:
            if 3 <= len(form) <= 30:
                valid_forms.add(form)

        # Сохраняем в кэш
        self._all_forms_cache[word_lower] = valid_forms

        return valid_forms

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
        Исправлено: не находит части слов (например, "bee" в "beer")
        """
        text_lower = text.lower()
        n = len(text_lower)

        if unique_only:
            found_keywords = set()  # Для уникальности
        else:
            found_keywords = None

        results = []

        for i in range(n):
            # Пропускаем, если текущая позиция - часть слова
            if i > 0 and text_lower[i - 1].isalnum():
                continue

            node = self.root
            j = i
            last_valid_match = None  # Запоминаем последнее валидное совпадение

            # Проходим по дереву пока есть совпадения
            while j < n and text_lower[j] in node.children:
                node = node.children[text_lower[j]]
                j += 1

                # Если нашли конец ключевого слова
                if node.is_end and node.keyword_id:
                    # Проверяем, что это действительно конец слова
                    if j == n or not text_lower[j].isalnum():
                        # Это конец слова - сохраняем как валидное совпадение
                        last_valid_match = {
                            'id': node.keyword_id,
                            'name': node.keyword_name,
                            'position': i,
                            'length': j - i,
                            'text': text_lower[i:j]
                        }

            # После завершения цикла, если есть валидное совпадение в конце слова
            if last_valid_match:
                if unique_only:
                    if last_valid_match['id'] not in found_keywords:
                        found_keywords.add(last_valid_match['id'])
                        results.append(last_valid_match)
                else:
                    results.append(last_valid_match)

        return results

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
        for char in word:
            if char not in node.children:
                node.children[char] = KeywordTrieNode()
            node = node.children[char]
        node.is_end = True
        node.keyword_id = keyword_id
        node.keyword_name = keyword_name
        node.length = len(word)

    def build_from_queryset(self, keywords_queryset):
        """Строит дерево из QuerySet ключевых слов"""
        self.keywords_cache.clear()
        self._all_forms_cache.clear()  # ОЧИЩАЕМ КЭШ ФОРМ
        self._verb_forms_cache.clear()

        if self.verbose:
            print(f"🔨 Строим Trie из {keywords_queryset.count()} ключевых слов...")

        count = 0
        total_forms = 0

        for keyword in keywords_queryset:
            keyword_name_lower = keyword.name.lower()

            # Генерируем ВСЕ формы слова одним методом
            all_forms = self._generate_all_forms(keyword_name_lower)

            # Вставляем все формы в дерево
            for form in all_forms:
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
