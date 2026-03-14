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

    @lru_cache(maxsize=2000)
    def get_best_base_form(self, word: str) -> str:
        """
        Определяет наилучшую базовую форму слова

        Стратегия:
        1. Сначала проверяем прямые деривации (trader → trade)
        2. Если нет, используем самую короткую лемму
        3. Если ничего не подходит, возвращаем исходное слово

        Args:
            word: Слово для нормализации

        Returns:
            Базовая форма слова
        """
        if not self.is_available() or len(word) < 3:
            return word.lower()

        word_lower = word.lower()

        # 1. Проверяем прямые деривации
        derivations = self.get_direct_derivations(word_lower)

        # Сортируем деривации: сначала те, которые являются подстрокой исходного слова
        good_derivations = [d for d in derivations if d in word_lower and len(d) < len(word_lower)]
        if good_derivations:
            good_derivations.sort(key=len)  # самые короткие первыми
            if self.verbose:
                print(f"   Найдена прямая деривация: {word_lower} → {good_derivations[0]}")
            return good_derivations[0]

        # 2. Пробуем лемматизацию
        candidates = []
        for pos in ['v', 'n', 'a', 'r']:
            lemma = self.lemmatize(word_lower, pos=pos)
            candidates.append(lemma)

        # Убираем дубликаты и сортируем по длине
        unique_candidates = list(set(candidates))
        unique_candidates.sort(key=len)

        # Выбираем самую короткую лемму
        if unique_candidates and unique_candidates[0] != word_lower:
            if self.verbose:
                print(f"   Лемматизация: {word_lower} → {unique_candidates[0]}")
            return unique_candidates[0]

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