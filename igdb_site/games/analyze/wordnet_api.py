# games/analyze/wordnet_api.py
"""
WordNet API для работы с лемматизацией и derivationally related forms
Используется как в анализе, так и в нормализации ключевых слов
"""

import time
from typing import Dict, List, Set, Optional, Tuple
from functools import lru_cache
from django.core.cache import cache


class WordNetAPI:
    """
    Единый API для работы с WordNet
    Предоставляет методы для лемматизации и получения связанных форм
    """

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.lemmatizer = None
        self.wordnet = None
        self._initialized = False
        self._init_nltk()

    def _init_nltk(self) -> bool:
        """Инициализирует NLTK и WordNet"""
        try:
            import nltk
            from nltk.stem import WordNetLemmatizer
            from nltk.corpus import wordnet

            # Проверяем доступность wordnet
            try:
                wordnet.synsets('test')
            except LookupError:
                if self.verbose:
                    print("📥 Загружаем WordNet...")
                nltk.download('wordnet', quiet=not self.verbose)
                nltk.download('omw-1.4', quiet=not self.verbose)

            self.lemmatizer = WordNetLemmatizer()
            self.wordnet = wordnet
            self._initialized = True

            if self.verbose:
                print("✅ WordNet API инициализирован")

            return True

        except Exception as e:
            if self.verbose:
                print(f"⚠️ Ошибка инициализации WordNet: {e}")
            return False

    def is_available(self) -> bool:
        """Проверяет доступность WordNet"""
        return self._initialized and self.lemmatizer is not None

    @lru_cache(maxsize=10000)
    def lemmatize(self, word: str, pos: str = 'v') -> str:
        """
        Лемматизирует слово с указанной частью речи

        Args:
            word: Слово для лемматизации
            pos: Часть речи ('v', 'n', 'a', 'r')

        Returns:
            Лемма слова
        """
        if not self.is_available() or not word:
            return word.lower()

        try:
            return self.lemmatizer.lemmatize(word.lower(), pos=pos)
        except:
            return word.lower()

    @lru_cache(maxsize=5000)
    def get_all_lemmas(self, word: str) -> Set[str]:
        """
        Получает все возможные леммы слова для разных частей речи

        Args:
            word: Слово для анализа

        Returns:
            Множество всех лемм
        """
        if not self.is_available() or not word:
            return {word.lower()}

        word_lower = word.lower()
        lemmas = {word_lower}

        for pos in ['v', 'n', 'a', 'r']:
            lemma = self.lemmatize(word_lower, pos=pos)
            lemmas.add(lemma)

        return lemmas

    @lru_cache(maxsize=5000)
    def get_direct_derivations(self, word: str) -> Set[str]:
        """
        Получает ТОЛЬКО прямые derivationally related forms

        Правила:
        1. Только непосредственные связи (одно слово)
        2. Слова должны содержать исходное слово или наоборот
        3. Игнорируем цепочки (trader → trade ✅, trader → deal ❌)

        Args:
            word: Слово для анализа

        Returns:
            Множество напрямую связанных слов
        """
        if not self.is_available() or not word:
            return set()

        word_lower = word.lower()
        related = set()

        try:
            # Получаем все synsets для слова
            synsets = self.wordnet.synsets(word_lower)

            for syn in synsets:
                for lemma in syn.lemmas():
                    # Проверяем derivationally related forms
                    if lemma.derivationally_related_forms():
                        for rel_lemma in lemma.derivationally_related_forms():
                            rel_name = rel_lemma.name().lower().replace('_', ' ')

                            # Проверяем прямую связь через общий корень
                            if self._is_direct_derivation(word_lower, rel_name):
                                related.add(rel_name)

                    # Также проверяем антонимы (но только прямые)
                    if lemma.antonyms():
                        for ant in lemma.antonyms():
                            ant_name = ant.name().lower().replace('_', ' ')
                            if self._is_direct_derivation(word_lower, ant_name):
                                related.add(ant_name)

            # Добавляем леммы (только если они короче исходного слова)
            for pos in ['v', 'n', 'a', 'r']:
                lemma = self.lemmatize(word_lower, pos=pos)
                if lemma != word_lower and len(lemma) < len(word_lower):
                    if lemma in word_lower:  # Лемма является частью исходного слова
                        related.add(lemma)

        except Exception as e:
            if self.verbose:
                print(f"⚠️ Ошибка получения derivations для '{word}': {e}")

        return related

    def _is_direct_derivation(self, word1: str, word2: str) -> bool:
        """
        Проверяет, являются ли слова прямыми деривациями друг друга

        Правила:
        1. Одно слово является основой другого с распространенными суффиксами
        2. Слова имеют общий корень и различаются только суффиксом

        Примеры:
        trade → trader (trade + er) ✅
        trade → trading (trade + ing) ✅
        trade → traded (trade + ed) ✅
        trader → deal (разные корни) ❌
        """
        if word1 == word2:
            return True

        # Распространенные суффиксы в английском
        suffixes = [
            'er', 'or',  # деятель (trader, actor)
            'ing',  # процесс (trading)
            'ed',  # прошедшее время (traded)
            'tion', 'sion',  # существительные (creation, mission)
            'ment',  # результат (agreement)
            'ance', 'ence',  # качество (importance, difference)
            'ness',  # состояние (happiness)
            'ity', 'ty',  # абстрактные существительные (ability, safety)
            'ive', 'ative',  # прилагательные (creative, talkative)
            'al', 'ial',  # прилагательные (cultural, commercial)
            'ous', 'ious',  # прилагательные (dangerous, curious)
            'ful', 'less',  # прилагательные (useful, useless)
            'ly',  # наречия (quickly)
            'ize', 'ise',  # глаголы (modernize, advertise)
        ]

        # Проверяем суффиксы
        for suffix in suffixes:
            # word2 образовано от word1 добавлением суффикса
            if word2.endswith(suffix) and word2[:-len(suffix)] == word1:
                return True

            # word1 образовано от word2 добавлением суффикса
            if word1.endswith(suffix) and word1[:-len(suffix)] == word2:
                return True

        # Проверяем, что одно слово является подстрокой другого
        # с минимальной разницей (не более 4 символов)
        if len(word1) <= len(word2) and word2.startswith(word1):
            remaining = word2[len(word1):]
            if len(remaining) <= 4:  # разумная длина суффикса
                return True

        if len(word2) <= len(word1) and word1.startswith(word2):
            remaining = word1[len(word2):]
            if len(remaining) <= 4:
                return True

        return False

    def _check_exceptions(self, word: str) -> tuple:
        """
        Проверяет, является ли слово исключением.

        Returns:
            tuple: (is_exception, base_form)
        """
        word_lower = word.lower()

        exceptions = {
            # Раны/травмы
            'wound': 'wound',
            'wounded': 'wound',
            'wounds': 'wound',

            # Боссы/враги
            'boss': 'boss',
            'bosses': 'boss',

            # Игровые термины
            'stats': 'stats',
            'stat': 'stats',
            'stamina': 'stamina',

            # Глаголы движения
            'riding': 'ride',  # кататься верхом / ехать

            # Существа
            'orcs': 'orc',  # орки
        }

        if word_lower in exceptions:
            return True, exceptions[word_lower]

        return False, word

    @lru_cache(maxsize=2000)
    def get_best_base_form(self, word: str) -> str:
        """
        Определяет базовую форму слова используя WordNet.
        Если после лемматизации слово осталось исходным, но оно существует в WordNet - возвращаем как есть.
        Удаление приставок применяется ТОЛЬКО для слов, которых НЕТ в WordNet И которые не изменились после лемматизации.

        Args:
            word: Слово для нормализации

        Returns:
            Базовая форма слова
        """
        if not self.is_available() or len(word) < 3:
            return word.lower()

        word_lower = word.lower()

        # Проверяем исключения
        is_exception, exception_base = self._check_exceptions(word)
        if is_exception:
            if self.verbose:
                print(f"   Исключение: {word_lower} → {exception_base}")
            return exception_base

        # ========== ШАГ 1: ПРОБУЕМ ЛЕММАТИЗАЦИЮ ==========
        if self.verbose:
            print(f"\n   🔍 Анализ слова: '{word_lower}'")

        # Переменная для хранения результата стандартной нормализации
        standard_result = None
        lemmatization_source = None

        # Проверяем глагольную форму (приоритет)
        if self.wordnet.synsets(word_lower, pos='v'):
            verb_lemma = self.lemmatize(word_lower, pos='v')
            if verb_lemma != word_lower:
                standard_result = verb_lemma
                lemmatization_source = 'verb_lemma'
                if self.verbose:
                    print(f"   → лемматизация как глагол: {verb_lemma}")

        # Если глагол не дал результата, пробуем существительное
        if standard_result is None and self.wordnet.synsets(word_lower, pos='n'):
            noun_lemma = self.lemmatize(word_lower, pos='n')
            if noun_lemma != word_lower:
                standard_result = noun_lemma
                lemmatization_source = 'noun_lemma'
                if self.verbose:
                    print(f"   → лемматизация как существительное: {noun_lemma}")

        # Если существительное не дало результата, пробуем прилагательное
        if standard_result is None and self.wordnet.synsets(word_lower, pos='a'):
            adj_lemma = self.lemmatize(word_lower, pos='a')
            if adj_lemma != word_lower:
                standard_result = adj_lemma
                lemmatization_source = 'adj_lemma'
                if self.verbose:
                    print(f"   → лемматизация как прилагательное: {adj_lemma}")

        # ========== ШАГ 2: ЕСЛИ ЛЕММАТИЗАЦИЯ ИЗМЕНИЛА СЛОВО ==========
        if standard_result is not None:
            if self.verbose:
                print(f"   ✅ Лемматизация изменила слово: {word_lower} → {standard_result}")
            return standard_result

        # ========== ШАГ 3: ЛЕММАТИЗАЦИЯ НЕ ИЗМЕНИЛА СЛОВО ==========
        # Проверяем, есть ли исходное слово в WordNet
        has_verb = len(self.wordnet.synsets(word_lower, pos='v')) > 0
        has_noun = len(self.wordnet.synsets(word_lower, pos='n')) > 0
        has_adj = len(self.wordnet.synsets(word_lower, pos='a')) > 0

        if has_verb or has_noun or has_adj:
            if self.verbose:
                print(f"   ✅ Слово '{word_lower}' существует в WordNet, оставляем как есть")
            return word_lower

        # ========== ШАГ 4: СЛОВА НЕТ В WORDNET, ПРОБУЕМ УДАЛИТЬ ПРИСТАВКУ ==========
        if self.verbose:
            print(f"   🔍 Слова '{word_lower}' нет в WordNet, пробуем удалить приставку...")

        # Список распространенных приставок, которые могут быть удалены
        prefixes_to_remove = [
            're',  # reimagine -> imagine
        ]

        # Пробуем удалить каждую приставку
        for prefix in prefixes_to_remove:
            if word_lower.startswith(prefix) and len(word_lower) > len(prefix) + 2:
                stripped_word = word_lower[len(prefix):]

                # Проверяем, существует ли слово без приставки в WordNet
                if (len(self.wordnet.synsets(stripped_word, pos='v')) > 0 or
                        len(self.wordnet.synsets(stripped_word, pos='n')) > 0):

                    # Рекурсивно нормализуем полученное слово
                    normalized_stripped = self.get_best_base_form(stripped_word)
                    if self.verbose:
                        print(f"   ✅ Удалена приставка '{prefix}': {word_lower} → {normalized_stripped}")
                    return normalized_stripped

        # ========== ШАГ 5: НИЧЕГО НЕ ПОМОГЛО ==========
        if self.verbose:
            print(f"   ⏺️ Слово '{word_lower}' оставляем без изменений")

        return word_lower


    def clear_cache(self):
        """Очищает все кэши WordNet"""
        self.get_all_lemmas.cache_clear()
        self.get_direct_derivations.cache_clear()
        self.get_best_base_form.cache_clear()
        self.lemmatize.cache_clear()
        if self.verbose:
            print("✅ Кэш WordNet очищен")


# Глобальный экземпляр для повторного использования
_wordnet_api = None


def get_wordnet_api(verbose: bool = False) -> WordNetAPI:
    """
    Возвращает глобальный экземпляр WordNetAPI
    """
    global _wordnet_api
    if _wordnet_api is None:
        _wordnet_api = WordNetAPI(verbose=verbose)
    return _wordnet_api