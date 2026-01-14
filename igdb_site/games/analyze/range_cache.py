# games/analyze/range_cache.py
"""
Кэширование проверенных диапазонов ID
"""
from django.core.cache import cache
from typing import Dict, List, Tuple, Optional
import json


class RangeCacheManager:
    """Менеджер кэширования диапазонов ID"""

    # Ключи для кэша
    CACHE_KEY_CRITERIA_RANGES = 'range_cache:checked_criteria_ranges'
    CACHE_KEY_GAME_RANGES = 'range_cache:checked_game_ranges'
    CACHE_TIMEOUT = 86400 * 30  # 30 дней

    @classmethod
    def get_checked_criteria_ranges(cls) -> Dict[str, List[Tuple[int, int]]]:
        """Получает кэшированные диапазоны проверенных критериев"""
        cached = cache.get(cls.CACHE_KEY_CRITERIA_RANGES)
        if cached:
            # Десериализуем из JSON
            result = {}
            for category, ranges in json.loads(cached).items():
                result[category] = [tuple(r) for r in ranges]
            return result
        return {
            'genres': [],
            'themes': [],
            'perspectives': [],
            'game_modes': [],
            'keywords': []
        }

    @classmethod
    def get_checked_game_ranges(cls) -> List[Tuple[int, int]]:
        """Получает кэшированные диапазоны проверенных игр"""
        cached = cache.get(cls.CACHE_KEY_GAME_RANGES)
        if cached:
            return [tuple(r) for r in json.loads(cached)]
        return []

    @classmethod
    def update_criteria_range(cls, category: str, min_id: int, max_id: int):
        """Обновляет диапазон проверенных критериев для категории"""
        ranges = cls.get_checked_criteria_ranges()

        if category not in ranges:
            ranges[category] = []

        # Добавляем новый диапазон и объединяем пересекающиеся
        new_range = (min_id, max_id)
        ranges[category] = cls._merge_ranges(ranges[category] + [new_range])

        # Сохраняем в кэш
        cache.set(
            cls.CACHE_KEY_CRITERIA_RANGES,
            json.dumps(ranges),
            cls.CACHE_TIMEOUT
        )

    @classmethod
    def update_game_range(cls, min_id: int, max_id: int):
        """Обновляет диапазон проверенных игр"""
        ranges = cls.get_checked_game_ranges()
        new_range = (min_id, max_id)
        ranges = cls._merge_ranges(ranges + [new_range])

        cache.set(
            cls.CACHE_KEY_GAME_RANGES,
            json.dumps(ranges),
            cls.CACHE_TIMEOUT
        )

    @classmethod
    def is_criteria_checked(cls, category: str, element_id: int) -> bool:
        """Проверяет, был ли критерий уже проверен"""
        ranges = cls.get_checked_criteria_ranges()
        if category not in ranges:
            return False

        for r_min, r_max in ranges[category]:
            if r_min <= element_id <= r_max:
                return True
        return False

    @classmethod
    def is_game_checked(cls, game_id: int) -> bool:
        """Проверяет, была ли игра уже проверена на все критерии"""
        ranges = cls.get_checked_game_ranges()
        for r_min, r_max in ranges:
            if r_min <= game_id <= r_max:
                return True
        return False

    @classmethod
    def mark_criteria_as_new(cls, category: str):
        """Отмечает, что появились новые критерии в категории - очищает её диапазоны"""
        ranges = cls.get_checked_criteria_ranges()
        if category in ranges:
            ranges[category] = []
            cache.set(
                cls.CACHE_KEY_CRITERIA_RANGES,
                json.dumps(ranges),
                cls.CACHE_TIMEOUT
            )

    @classmethod
    def mark_all_games_as_unchecked(cls):
        """Отмечает, что появились новые игры - очищает все диапазоны игр"""
        cache.set(cls.CACHE_KEY_GAME_RANGES, json.dumps([]), cls.CACHE_TIMEOUT)

    @staticmethod
    def _merge_ranges(ranges: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
        """Объединяет пересекающиеся диапазоны"""
        if not ranges:
            return []

        # Сортируем по началу диапазона
        ranges.sort(key=lambda x: x[0])

        merged = []
        current = ranges[0]

        for next_range in ranges[1:]:
            if current[1] >= next_range[0] - 1:  # Диапазоны пересекаются или смежные
                current = (current[0], max(current[1], next_range[1]))
            else:
                merged.append(current)
                current = next_range

        merged.append(current)
        return merged

    @classmethod
    def clear_all_cache(cls):
        """Очищает весь кэш диапазонов"""
        cache.delete(cls.CACHE_KEY_CRITERIA_RANGES)
        cache.delete(cls.CACHE_KEY_GAME_RANGES)