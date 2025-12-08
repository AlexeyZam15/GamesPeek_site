# games/analyzer/keyword_finder.py
import re
from typing import Dict, List, Set, Optional, Tuple
from django.core.cache import cache
from games.models import Keyword


class KeywordFinder:
    """Класс для поиска ключевых слов по паттернам и точным совпадениям"""

    def __init__(self):
        self.model = Keyword
        self._cache = {}  # Кеш для объектов ключевых слов
        self._name_cache = {}  # Кеш для имен объектов
        self._all_keywords_lower = None  # Кеш всех ключевых слов в нижнем регистре
        self._all_keywords_objects = None  # Кеш всех объектов ключевых слов

    def _get_all_keywords_objects(self) -> List[Keyword]:
        """Получает все объекты ключевых слов из базы"""
        if self._all_keywords_objects is None:
            self._all_keywords_objects = list(Keyword.objects.all())
        return self._all_keywords_objects

    def _get_all_keywords_lower(self) -> Set[str]:
        """Получает все ключевые слова из базы в нижнем регистре"""
        if self._all_keywords_lower is None:
            keywords = Keyword.objects.values_list('name', flat=True)
            self._all_keywords_lower = {kw.lower() for kw in keywords}
        return self._all_keywords_lower

    def find(self, text: str, existing_objects: Set = None,
             pattern_collection_mode: bool = False,
             collected_patterns: Optional[List] = None) -> Tuple[List, List[Dict]]:
        """Находит ключевые слова в тексте, исключая уже существующие"""
        if not text:
            return [], []

        text_lower = text.lower()
        found_objects = []
        pattern_matches = []

        # Получаем все ключевые слова из базы
        all_keywords = self._get_all_keywords_objects()

        # Если есть существующие объекты, создаем набор их ID для быстрой проверки
        existing_ids = {obj.id for obj in existing_objects} if existing_objects else set()

        for keyword in all_keywords:
            keyword_lower = keyword.name.lower()

            # Проверяем, есть ли ключевое слово в тексте
            if ' ' in keyword_lower:
                # Для многословных ключевых слов
                if keyword_lower in text_lower:
                    start_pos = text_lower.find(keyword_lower)
                    end_pos = start_pos + len(keyword_lower)

                    # Проверяем, не является ли это ключевое слово уже существующим
                    if keyword.id in existing_ids:
                        if pattern_collection_mode:
                            pattern_matches.append({
                                'name': keyword.name,
                                'pattern': 'exact phrase match',
                                'matched_text': text[start_pos:end_pos],
                                'status': 'skipped',
                                'start_pos': start_pos,
                                'end_pos': end_pos
                            })
                        continue

                    found_objects.append(keyword)
                    pattern_matches.append({
                        'name': keyword.name,
                        'pattern': 'exact phrase match',
                        'matched_text': text[start_pos:end_pos],
                        'status': 'found',
                        'start_pos': start_pos,
                        'end_pos': end_pos
                    })
            else:
                # Для однословных ключевых слов
                # Используем регулярное выражение для поиска целых слов
                pattern = rf'\b{re.escape(keyword_lower)}\b'
                if re.search(pattern, text_lower):
                    # Находим все совпадения
                    for match in re.finditer(pattern, text_lower):
                        start_pos, end_pos = match.span()

                        # Проверяем, не является ли это ключевое слово уже существующим
                        if keyword.id in existing_ids:
                            if pattern_collection_mode:
                                pattern_matches.append({
                                    'name': keyword.name,
                                    'pattern': pattern,
                                    'matched_text': text[start_pos:end_pos],
                                    'status': 'skipped',
                                    'start_pos': start_pos,
                                    'end_pos': end_pos
                                })
                            break  # Пропускаем это ключевое слово

                        found_objects.append(keyword)
                        pattern_matches.append({
                            'name': keyword.name,
                            'pattern': pattern,
                            'matched_text': text[start_pos:end_pos],
                            'status': 'found',
                            'start_pos': start_pos,
                            'end_pos': end_pos
                        })
                        break  # Нашли одно совпадение, достаточно

        return found_objects, pattern_matches

    def _get_keyword_by_name(self, name: str):
        """Получает ключевое слово по имени с кешированием"""
        name_lower = name.lower()
        if name_lower not in self._cache:
            try:
                obj = Keyword.objects.get(name__iexact=name)
                self._cache[name_lower] = obj
                self._name_cache[name_lower] = obj
            except Keyword.DoesNotExist:
                self._cache[name_lower] = None
        return self._cache.get(name_lower)

    def get_actual_objects(self, names: List[str]) -> List:
        """Получает объекты ключевых слов из базы данных с кешированием"""
        if not names:
            return []

        unique_names = set(names)
        objects = []

        for name in unique_names:
            obj = self._get_keyword_by_name(name)
            if obj:
                objects.append(obj)

        return objects

    def clear_cache(self):
        """Очищает кеши для экономии памяти"""
        self._cache.clear()
        self._name_cache.clear()
        self._all_keywords_lower = None
        self._all_keywords_objects = None

    @classmethod
    def get_all_keywords(cls) -> List[str]:
        """Возвращает полный список всех ключевых слов ИЗ БАЗЫ ДАННЫХ"""
        return list(Keyword.objects.values_list('name', flat=True).order_by('name'))

    @classmethod
    def import_keywords_from_list(cls, keywords_list: List[str]) -> Tuple[int, int]:
        """Импортирует ключевые слова из списка в базу данных"""
        from django.db import transaction

        created_count = 0
        existing_count = 0

        with transaction.atomic():
            for keyword_name in keywords_list:
                keyword_name = keyword_name.strip()
                if not keyword_name:
                    continue

                # Проверяем, существует ли уже такое ключевое слово
                if not Keyword.objects.filter(name__iexact=keyword_name).exists():
                    Keyword.objects.create(name=keyword_name)
                    created_count += 1
                else:
                    existing_count += 1

        return created_count, existing_count