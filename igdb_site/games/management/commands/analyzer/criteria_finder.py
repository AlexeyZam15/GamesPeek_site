# games/analyzer/criteria_finder.py
import re
from typing import Dict, List, Set, Optional, Tuple
from django.core.cache import cache


class CriteriaFinder:
    """Универсальный класс для поиска критериев по паттернам с оптимизацией"""

    def __init__(self, model, patterns: Dict[str, List[re.Pattern]]):
        self.model = model
        self.patterns = patterns
        self._cache = {}  # Кеш для объектов модели
        self._name_cache = {}  # Кеш для имен объектов

    def find(self, text: str, existing_objects: Set = None,
             pattern_collection_mode: bool = False,
             collected_patterns: Optional[List] = None) -> Tuple[List, List[Dict]]:
        """Находит критерии в тексте, исключая уже существующие"""
        if not text:
            return [], []

        found_names = set()
        pattern_matches = []  # Детальная информация о совпадениях
        skipped_criteria = set()

        # Предварительно проверяем существующие объекты ТОЛЬКО если они переданы
        existing_names = {obj.name.lower() for obj in existing_objects} if existing_objects else set()

        for name, patterns in self.patterns.items():
            name_lower = name.lower()

            # Пропускаем критерии, которые уже есть у игры (ТОЛЬКО если переданы existing_objects)
            if existing_objects and name_lower in existing_names:
                skipped_criteria.add(name)
                if pattern_collection_mode:
                    pattern_matches.append({
                        'name': name,
                        'pattern': 'SKIPPED',
                        'matched_text': f'Пропущено (уже существует: {name})',
                        'status': 'skipped'
                    })
                continue

            # Проверяем паттерны
            for pattern in patterns:
                try:
                    match = pattern.search(text)
                    if match:
                        found_names.add(name)
                        matched_text = text[match.start():match.end()].strip()

                        match_info = {
                            'name': name,
                            'pattern': pattern.pattern,
                            'matched_text': matched_text,
                            'status': 'found',
                            'start_pos': match.start(),
                            'end_pos': match.end()
                        }

                        if pattern_collection_mode:
                            pattern_matches.append(match_info)

                        # Сохраняем информацию о совпадении даже если не в режиме сбора
                        pattern_matches.append(match_info)
                        break  # Прерываем после первого совпадения

                except Exception as e:
                    error_info = {
                        'name': name,
                        'pattern': pattern.pattern if hasattr(pattern, 'pattern') else str(pattern),
                        'matched_text': f'ERROR: {str(e)}',
                        'status': 'error'
                    }
                    if pattern_collection_mode:
                        pattern_matches.append(error_info)

        return self.get_actual_objects(list(found_names)), pattern_matches

    def get_actual_objects(self, names: List[str]) -> List:
        """Получает объекты из базы данных с кешированием"""
        if not names:
            return []

        # Фильтруем уникальные имена
        unique_names = set(names)
        objects = []
        missing_names = []

        for name in unique_names:
            if name not in self._cache:
                try:
                    # Используем точное совпадение без учета регистра
                    obj = self.model.objects.get(name__iexact=name)
                    self._cache[name] = obj
                    self._name_cache[name.lower()] = obj
                except self.model.DoesNotExist:
                    self._cache[name] = None
                    missing_names.append(name)

            if self._cache[name]:
                objects.append(self._cache[name])

        if missing_names and self.model._meta.verbose_name_plural == 'genres':
            print(f"⚠️ Следующие критерии не найдены в базе данных: {missing_names}")

        return objects

    def clear_cache(self):
        """Очищает кеш для экономии памяти"""
        self._cache.clear()
        self._name_cache.clear()