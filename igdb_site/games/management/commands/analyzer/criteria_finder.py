# games/analyzer/criteria_finder.py
import re
from typing import Dict, List, Set, Optional, Tuple
from django.core.cache import cache


class CriteriaFinder:
    """Универсальный класс для поиска критериев по паттернам"""

    def __init__(self, model, patterns: Dict[str, List[re.Pattern]]):
        self.model = model
        self.patterns = patterns  # ВСЕ паттерны сразу
        self._cache = {}
        self._name_cache = {}

    def find_all_patterns(self, text: str, existing_objects: Set = None,
                          pattern_collection_mode: bool = False) -> Tuple[List, List[Dict]]:
        """Ищет критерии используя ВСЕ паттерны сразу"""
        if not text:
            return [], []

        found_names = set()
        pattern_matches = []

        # Существующие объекты для пропуска
        existing_names = {obj.name.lower() for obj in existing_objects} if existing_objects else set()

        # Используем ВСЕ паттерны для каждого критерия
        for name, patterns_list in self.patterns.items():
            name_lower = name.lower()

            # Пропускаем если уже есть
            if existing_objects and name_lower in existing_names:
                if pattern_collection_mode:
                    pattern_matches.append({
                        'name': name,
                        'pattern': 'SKIPPED',
                        'matched_text': f'Пропущено (уже существует: {name})',
                        'status': 'skipped'
                    })
                continue

            # Проверяем ВСЕ паттерны для этого критерия
            for pattern in patterns_list:
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
                            'end_pos': match.end(),
                            'confidence': self._calculate_simple_confidence(match, text)
                        }

                        pattern_matches.append(match_info)
                        break  # Достаточно одного совпадения из всех паттернов

                except Exception:
                    continue

        return self.get_actual_objects(list(found_names)), pattern_matches

    def _calculate_simple_confidence(self, match, text: str) -> float:
        """Простая оценка уверенности"""
        # Базовое значение
        confidence = 0.5

        # 1. Длина совпадения
        match_len = match.end() - match.start()
        if match_len > 15:
            confidence += 0.2
        elif match_len > 8:
            confidence += 0.1

        # 2. Позиция в тексте (ранние упоминания важнее)
        text_len = len(text)
        if text_len > 0:
            position_ratio = match.start() / text_len
            if position_ratio < 0.3:  # В первой трети текста
                confidence += 0.1

        return min(1.0, confidence)

    def get_actual_objects(self, names: List[str]) -> List:
        """Получает объекты из базы данных"""
        if not names:
            return []

        unique_names = set(names)
        objects = []

        for name in unique_names:
            if name not in self._cache:
                try:
                    obj = self.model.objects.get(name__iexact=name)
                    self._cache[name] = obj
                    self._name_cache[name.lower()] = obj
                except self.model.DoesNotExist:
                    self._cache[name] = None
                    print(f"⚠️ Критерий '{name}' не найден в базе данных")

            if self._cache[name]:
                objects.append(self._cache[name])

        return objects

    def clear_cache(self):
        """Очищает кеш"""
        self._cache.clear()
        self._name_cache.clear()
