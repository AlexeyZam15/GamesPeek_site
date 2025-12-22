# games/management/commands/analyzer/keyword_finder.py
import re
from typing import Dict, List, Set, Optional, Tuple
from games.models import Keyword


class KeywordFinder:
    """Класс для поиска ВСЕХ ключевых слов сразу"""

    def __init__(self):
        self.model = Keyword
        self._cache = {}
        self._name_cache = {}
        self._all_keywords = None

    def find_all_keywords(self, text: str, existing_objects: Set = None,
                          pattern_collection_mode: bool = False) -> Tuple[List, List[Dict]]:
        """Ищет ВСЕ ключевые слова в тексте сразу"""
        if not text:
            return [], []

        text_lower = text.lower()
        found_objects = []
        pattern_matches = []

        all_keywords = self._get_all_keywords()
        existing_ids = {obj.id for obj in existing_objects} if existing_objects else set()

        for keyword in all_keywords:
            keyword_name = keyword.name
            keyword_lower = keyword.name.lower()

            if keyword.id in existing_ids:
                if pattern_collection_mode:
                    pattern_matches.append({
                        'name': keyword_name,
                        'pattern': 'SKIPPED',
                        'matched_text': f'Пропущено (уже существует: {keyword_name})',
                        'status': 'skipped'
                    })
                continue

            if ' ' in keyword_lower:
                if keyword_lower in text_lower:
                    found_objects.append(keyword)
                    start_pos = text_lower.find(keyword_lower)
                    pattern_matches.append({
                        'name': keyword_name,
                        'pattern': 'exact phrase',
                        'matched_text': text[start_pos:start_pos + len(keyword_lower)],
                        'status': 'found',
                        'start_pos': start_pos,
                        'end_pos': start_pos + len(keyword_lower)
                    })
            else:
                pattern = rf'\b{re.escape(keyword_lower)}\b'
                match = re.search(pattern, text_lower)
                if match:
                    found_objects.append(keyword)
                    pattern_matches.append({
                        'name': keyword_name,
                        'pattern': pattern,
                        'matched_text': text[match.start():match.end()],
                        'status': 'found',
                        'start_pos': match.start(),
                        'end_pos': match.end()
                    })

        return found_objects, pattern_matches

    def _get_all_keywords(self) -> List[Keyword]:
        """Получает ВСЕ ключевые слова из базы"""
        if self._all_keywords is None:
            self._all_keywords = list(Keyword.objects.all())
        return self._all_keywords

    def clear_cache(self):
        """Очищает кеши"""
        self._cache.clear()
        self._name_cache.clear()
        self._all_keywords = None

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

                if not Keyword.objects.filter(name__iexact=keyword_name).exists():
                    Keyword.objects.create(name=keyword_name)
                    created_count += 1
                else:
                    existing_count += 1

        return created_count, existing_count