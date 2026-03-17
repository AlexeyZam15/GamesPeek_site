# games/analyze/keyword_trie.py
import re
import pickle
from typing import Dict, List, Optional
from django.core.cache import cache
from .wordnet_api import get_wordnet_api


class KeywordTrieNode:
    """Узел префиксного дерева для ключевых слов"""

    def __init__(self):
        self.children: Dict[str, 'KeywordTrieNode'] = {}
        self.is_end: bool = False
        self.keyword_id: Optional[int] = None
        self.keyword_name: Optional[str] = None
        self.length: int = 0


class KeywordTrie:
    """
    Префиксное дерево для быстрого поиска ключевых слов
    Использует WordNetAPI для лемматизации и поиска связанных форм
    """

    def __init__(self, verbose: bool = False):
        self.root = KeywordTrieNode()
        self.keywords_cache: Dict[int, dict] = {}
        self.verbose = verbose
        # НЕ сохраняем wordnet_api как атрибут - будем получать при каждом поиске
        # Это решает проблему с pickle

    def __getstate__(self):
        """Подготовка объекта для pickle - исключаем wordnet_api"""
        state = self.__dict__.copy()
        # Удаляем всё, что может содержать _thread.RLock
        # wordnet_api не сохраняем - будем создавать заново при поиске
        return state

    def __setstate__(self, state):
        """Восстановление объекта из pickle"""
        self.__dict__.update(state)

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

        if self.verbose:
            print(f"🔨 Строим Trie из {keywords_queryset.count()} ключевых слов...")

        count = 0

        for keyword in keywords_queryset:
            keyword_name_lower = keyword.name.lower()

            # Вставляем оригинальное ключевое слово
            self.insert(keyword_name_lower, keyword.id, keyword.name)

            # Сохраняем в кэш
            self.keywords_cache[keyword.id] = {
                'id': keyword.id,
                'name': keyword.name,
                'name_lower': keyword.name.lower()
            }

            count += 1
            if self.verbose and count % 1000 == 0:
                print(f"  Загружено {count} ключевых слов...")

        if self.verbose:
            print(f"✅ Trie построен: {count} ключевых слов")

    def get_keyword_by_id(self, keyword_id: int) -> Optional[dict]:
        """Быстро получает ключевое слово по ID"""
        return self.keywords_cache.get(keyword_id)

    def _ensure_wordnet(self):
        """Получает WordNetAPI (создает при первом обращении)"""
        # Не сохраняем как атрибут, чтобы избежать проблем с pickle
        return get_wordnet_api(verbose=self.verbose)

    def find_all_in_text(self, text: str, unique_only: bool = True) -> List[dict]:
        """
        Находит ключевые слова в тексте используя лемматизацию каждого слова
        и derivationally related forms из WordNetAPI
        ИСПРАВЛЕНО: всё приводится к нижнему регистру для поиска
        """
        wordnet_api = self._ensure_wordnet()

        if not wordnet_api.is_available():
            raise RuntimeError("WordNetAPI недоступен. Невозможно выполнить лемматизацию.")

        try:
            # Заменяем кавычки на пробелы перед обработкой
            import re
            text_for_search = re.sub(r'["\']', ' ', text)
            text_lower = text_for_search.lower()

            # Создаем структуры для быстрого поиска - ТОЛЬКО ОРИГИНАЛЬНЫЕ ключевые слова
            exact_phrases = {}  # точные фразы с пробелами -> id
            hyphenated_keywords = {}  # составные слова с дефисом -> id
            two_word_phrases = {}  # ДВУХСЛОВНЫЕ ФРАЗЫ
            all_keywords = {}  # все ключевые слова (оригинальные названия) -> id

            for kid, kdata in self.keywords_cache.items():
                name_lower = kdata['name_lower']
                all_keywords[name_lower] = kid

                # Сохраняем фразы с пробелами
                if ' ' in name_lower:
                    exact_phrases[name_lower] = kid
                    # Если фраза состоит из двух слов, сохраняем для специальной обработки
                    words = name_lower.split()
                    if len(words) == 2:
                        two_word_phrases[name_lower] = {
                            'id': kid,
                            'first': words[0],
                            'second': words[1],
                            'name': kdata['name']
                        }

                # Сохраняем составные слова с дефисом
                if '-' in name_lower:
                    hyphenated_keywords[name_lower] = kid

            if self.verbose:
                print(f"🔍 Загружено {len(all_keywords)} ключевых слов")
                print(f"🔍 Фраз с пробелами: {len(exact_phrases)}")
                print(f"🔍 Двухсловных фраз: {len(two_word_phrases)}")
                print(f"🔍 Составных слов с дефисом: {len(hyphenated_keywords)}")

            results = []
            occupied_positions = []  # Отслеживаем занятые позиции

            # ========== ПРИОРИТЕТ 1: Поиск точных фраз с пробелами ==========
            if self.verbose:
                print("\n🔍 ПРИОРИТЕТ 1: Поиск точных фраз с пробелами")

            sorted_phrases = sorted(exact_phrases.items(), key=lambda x: len(x[0]), reverse=True)

            for phrase, kid in sorted_phrases:
                pos = 0
                while True:
                    pos = text_lower.find(phrase, pos)
                    if pos == -1:
                        break

                    end_pos = pos + len(phrase)

                    # Проверяем границы
                    start_ok = (pos == 0 or not text_lower[pos - 1].isalnum())
                    end_ok = (end_pos == len(text_lower) or not text_lower[end_pos].isalnum())

                    if start_ok and end_ok:
                        # Проверяем, не занята ли позиция
                        is_occupied = False
                        for occ_start, occ_end in occupied_positions:
                            if not (end_pos <= occ_start or pos >= occ_end):
                                is_occupied = True
                                break

                        if not is_occupied:
                            keyword_data = self.keywords_cache[kid]
                            original_text = text[pos:end_pos]

                            results.append({
                                'id': kid,
                                'name': keyword_data['name'],
                                'position': pos,
                                'length': len(phrase),
                                'text': original_text,
                                'matched_phrase': phrase
                            })

                            occupied_positions.append((pos, end_pos))
                            if self.verbose:
                                print(f"  ✅ Найдена фраза: '{original_text}'")

                    pos = end_pos

            # ========== ПРИОРИТЕТ 2: Поиск составных слов с дефисом ==========
            if self.verbose:
                print("\n🔍 ПРИОРИТЕТ 2: Поиск составных слов с дефисом")

            sorted_hyphenated = sorted(hyphenated_keywords.items(), key=lambda x: len(x[0]), reverse=True)

            for hyphenated_word, kid in sorted_hyphenated:
                pos = 0
                while True:
                    pos = text_lower.find(hyphenated_word, pos)
                    if pos == -1:
                        break

                    end_pos = pos + len(hyphenated_word)

                    # Проверяем, не занята ли позиция
                    is_occupied = False
                    for occ_start, occ_end in occupied_positions:
                        if not (end_pos <= occ_start or pos >= occ_end):
                            is_occupied = True
                            break

                    if not is_occupied:
                        # Находим конец фактического слова в тексте
                        actual_end = pos
                        while actual_end < len(text_lower) and (
                                text_lower[actual_end].isalnum() or text_lower[actual_end] == '-'):
                            actual_end += 1

                        keyword_data = self.keywords_cache[kid]
                        original_text = text[pos:actual_end]

                        # Проверяем, что найденный текст содержит искомое слово
                        if hyphenated_word in original_text.lower():
                            results.append({
                                'id': kid,
                                'name': keyword_data['name'],
                                'position': pos,
                                'length': actual_end - pos,
                                'text': original_text,
                                'matched_hyphenated': hyphenated_word
                            })

                            occupied_positions.append((pos, actual_end))
                            if self.verbose:
                                print(f"  ✅ Найдено составное слово: '{original_text}'")

                    pos = end_pos

            # ========== ПРИОРИТЕТ 3: Поиск двухсловных фраз с лемматизацией второго слова ==========
            if self.verbose:
                print("\n🔍 ПРИОРИТЕТ 3: Поиск двухсловных фраз с лемматизацией")

            # Разбиваем текст на последовательности слов
            words = []
            i = 0
            while i < len(text_lower):
                if not text_lower[i].isalnum():
                    i += 1
                    continue

                start = i
                while i < len(text_lower) and (text_lower[i].isalnum() or text_lower[i] == "'"):
                    i += 1

                if i > start:
                    words.append({
                        'word': text_lower[start:i],
                        'start': start,
                        'end': i,
                        'original': text[start:i]
                    })

            # Ищем все последовательности из двух слов
            for i in range(len(words) - 1):
                word1 = words[i]
                word2 = words[i + 1]

                # Проверяем, что между словами только пробел
                if word1['end'] + 1 != word2['start']:
                    continue

                # Проверяем, не занята ли позиция
                is_occupied = False
                for occ_start, occ_end in occupied_positions:
                    if not (word2['end'] <= occ_start or word1['start'] >= occ_end):
                        is_occupied = True
                        break

                if is_occupied:
                    continue

                # Пробуем найти совпадение среди двухсловных фраз
                for phrase_data in two_word_phrases.values():
                    # Проверяем первое слово (точное совпадение)
                    if word1['word'] != phrase_data['first']:
                        continue

                    # Получаем базовую форму второго слова
                    second_base = wordnet_api.get_best_base_form(word2['word'])

                    # Проверяем второе слово (по базовой форме)
                    if second_base == phrase_data['second']:
                        # Нашли совпадение!
                        start_pos = word1['start']
                        end_pos = word2['end']

                        keyword_data = self.keywords_cache[phrase_data['id']]
                        original_text = text[start_pos:end_pos]

                        results.append({
                            'id': phrase_data['id'],
                            'name': phrase_data['name'],
                            'position': start_pos,
                            'length': end_pos - start_pos,
                            'text': original_text,
                            'matched_lemma': f"{word1['word']} {second_base}"
                        })

                        occupied_positions.append((start_pos, end_pos))
                        if self.verbose:
                            print(f"  ✅ Найдена двухсловная фраза: '{original_text}' → '{word1['word']} {second_base}'")

                        break

            # ========== ПРИОРИТЕТ 4: Поиск отдельных слов ==========
            if self.verbose:
                print("\n🔍 ПРИОРИТЕТ 4: Поиск отдельных слов")

            for word_info in words:
                word = word_info['word']
                start = word_info['start']
                end = word_info['end']

                # Проверяем, не занята ли позиция
                is_occupied = False
                for occ_start, occ_end in occupied_positions:
                    if not (end <= occ_start or start >= occ_end):
                        is_occupied = True
                        break

                if is_occupied:
                    continue

                # Для коротких слов (2-3 буквы) не проверяем границы
                if len(word) <= 3:
                    # Просто проверяем точное совпадение
                    if word in all_keywords:
                        kid = all_keywords[word]
                        keyword_data = self.keywords_cache[kid]
                        original_text = text[start:end]

                        results.append({
                            'id': kid,
                            'name': keyword_data['name'],
                            'position': start,
                            'length': len(word),
                            'text': original_text,
                            'matched_exact': word
                        })

                        occupied_positions.append((start, end))
                        if self.verbose:
                            print(f"  ✅ Найдено короткое слово: '{original_text}'")
                        continue

                # Для длинных слов проверяем границы
                else:
                    # Проверяем границы
                    is_valid = True

                    # Проверяем начало
                    if start > 0:
                        prev_char = text[start - 1]
                        if prev_char.isalnum() and prev_char != '-':
                            is_valid = False

                    # Проверяем конец
                    if end < len(text):
                        next_char = text[end]
                        if next_char.isalnum() and next_char not in "s'-":
                            is_valid = False

                    if not is_valid:
                        continue

                    # Проверяем точное совпадение
                    if word in all_keywords:
                        kid = all_keywords[word]
                        keyword_data = self.keywords_cache[kid]
                        original_text = text[start:end]

                        results.append({
                            'id': kid,
                            'name': keyword_data['name'],
                            'position': start,
                            'length': len(word),
                            'text': original_text,
                            'matched_exact': word
                        })

                        occupied_positions.append((start, end))
                        if self.verbose:
                            print(f"  ✅ Найдено точное слово: '{original_text}'")
                        continue

                    # Если точного совпадения нет, пробуем нормализацию
                    base_form = wordnet_api.get_best_base_form(word)

                    if base_form in all_keywords:
                        kid = all_keywords[base_form]
                        keyword_data = self.keywords_cache[kid]
                        original_text = text[start:end]

                        results.append({
                            'id': kid,
                            'name': keyword_data['name'],
                            'position': start,
                            'length': len(word),
                            'text': original_text,
                            'matched_lemma': base_form
                        })

                        occupied_positions.append((start, end))
                        if self.verbose:
                            # Здесь мы можем увидеть, что слово прошло через обработку приставок
                            if base_form != word and base_form != wordnet_api.lemmatize(word):
                                print(
                                    f"  ✅ Найдено слово через нормализацию/удаление приставки: '{original_text}' → '{base_form}'")
                            else:
                                print(f"  ✅ Найдено слово через нормализацию: '{original_text}' → '{base_form}'")

            # Удаляем дубликаты если нужно
            if unique_only:
                unique_results = []
                seen_ids = set()
                for r in results:
                    if r['id'] not in seen_ids:
                        seen_ids.add(r['id'])
                        unique_results.append(r)
                results = unique_results

            return results

        except Exception as e:
            if self.verbose:
                print(f"⚠️ Ошибка при поиске: {e}")
            raise

    def _tokenize_text(self, text: str) -> List[Dict]:
        """Разбивает текст на слова с позициями"""
        words = []
        i = 0
        while i < len(text):
            if not text[i].isalnum():
                i += 1
                continue

            start = i
            while i < len(text) and (text[i].isalnum() or text[i] == "'"):
                i += 1

            if i > start:
                words.append({
                    'word': text[start:i],
                    'start': start,
                    'end': i
                })
        return words

    def _add_result(self, results, found_keywords, keyword_id, position, text, lemma):
        """Добавляет результат в список"""
        keyword_data = self.keywords_cache[keyword_id]
        result = {
            'id': keyword_id,
            'name': keyword_data['name'],
            'position': position,
            'length': len(text),
            'text': text,
            'matched_lemma': lemma
        }
        results.append(result)
        if found_keywords is not None:
            found_keywords.add(keyword_id)


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
                try:
                    cache.set(self._trie_cache_key, self.trie, 3600)
                    if verbose:
                        print(f"✅ Trie построен и закэширован ({current_count} ключевых слов)")
                except Exception as e:
                    if verbose:
                        print(f"⚠️ Не удалось закэшировать Trie: {e}")
                    # Продолжаем работу без кэширования

        return self.trie

    def clear_cache(self):
        """Очищает кэш Trie"""
        cache.delete(self._trie_cache_key)
        self.trie = None
        self.keywords_count = 0
        self.initialized = False