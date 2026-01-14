# games/analyze/keyword_trie.py
import re
from typing import Dict, List, Set, Optional
from django.core.cache import cache


class KeywordTrieNode:
    """Узел префиксного дерева для ключевых слов"""

    def __init__(self):
        self.children: Dict[str, 'KeywordTrieNode'] = {}
        self.is_end: bool = False
        self.keyword_id: Optional[int] = None
        self.keyword_name: Optional[str] = None
        self.length: int = 0  # Длина ключевого слова


class KeywordTrie:
    """Префиксное дерево для быстрого поиска ключевых слов"""

    def __init__(self, verbose: bool = False):
        self.root = KeywordTrieNode()
        self.keywords_cache: Dict[int, dict] = {}
        self.verbose = verbose

    def build_from_queryset(self, keywords_queryset):
        """Строит дерево из QuerySet ключевых слов"""
        self.keywords_cache.clear()

        if self.verbose:
            print(f"🔨 Строим Trie из {keywords_queryset.count()} ключевых слов...")

        count = 0
        for keyword in keywords_queryset:
            # Вставляем слово в нижнем регистре
            self.insert(keyword.name.lower(), keyword.id, keyword.name)
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

    def find_all_in_text(self, text: str) -> List[dict]:
        """Находит все ключевые слова в тексте за O(n*m), где m - максимальная длина ключевого слова"""
        text_lower = text.lower()
        n = len(text_lower)
        found_keywords = set()  # Для уникальности
        results = []

        for i in range(n):
            node = self.root
            j = i

            # Проходим по дереву пока есть совпадения
            while j < n and text_lower[j] in node.children:
                node = node.children[text_lower[j]]
                j += 1

                # Если нашли конец ключевого слова
                if node.is_end and node.keyword_id:
                    # Проверяем границы слова
                    if self._is_word_boundary(text_lower, i, j):
                        if node.keyword_id not in found_keywords:
                            found_keywords.add(node.keyword_id)
                            results.append({
                                'id': node.keyword_id,
                                'name': node.keyword_name,
                                'position': i,
                                'length': node.length,
                                'text': text_lower[i:j]
                            })
                            # Не прерываем поиск - может быть более длинное ключевое слово

            # Пропускаем текущее слово если оно уже обработано
            # i будет увеличиваться в цикле for

        return results

    def _is_word_boundary(self, text: str, start: int, end: int) -> bool:
        """Проверяет границы слова (только для буквенных символов)"""
        # Проверяем начало (если есть предыдущий символ)
        if start > 0:
            prev_char = text[start - 1]
            if prev_char.isalpha() or prev_char.isdigit():
                return False

        # Проверяем конец (если есть следующий символ)
        if end < len(text):
            next_char = text[end]
            if next_char.isalpha() or next_char.isdigit():
                return False

        return True

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