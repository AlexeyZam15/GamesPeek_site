# games/analyze/text_analyzer.py
"""
Анализатор текста - ядро логики поиска критериев и ключевых слов
"""

import re
import time
from typing import Dict, Any, List, Optional, Set, Tuple

from games.models import Genre, Theme, PlayerPerspective, GameMode, Keyword
from .pattern_manager import PatternManager


class TextAnalyzer:
    """Анализатор текста для поиска критериев и ключевых слов"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self._patterns = None
        self._keywords_cache = None
        self._cache_stats = {'hits': 0, 'misses': 0}

    def analyze(
            self,
            text: str,
            analyze_keywords: bool = False,
            existing_game=None,
            detailed_patterns: bool = False
    ) -> Dict[str, Any]:
        """
        Анализирует текст на наличие критериев или ключевых слов

        Args:
            text: Текст для анализа
            analyze_keywords: Искать ключевые слова (False = критерии)
            existing_game: Существующая игра для проверки критериев
            detailed_patterns: Собирать детальную информацию о паттернах

        Returns:
            Результаты анализа
        """
        start_time = time.time()

        if not text:
            return {
                'results': {},
                'summary': {
                    'found_count': 0,
                    'has_results': False
                },
                'pattern_info': None,
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        if analyze_keywords:
            results, pattern_info = self._analyze_keywords(
                text=text,
                existing_game=existing_game,
                collect_patterns=detailed_patterns
            )
        else:
            results, pattern_info = self._analyze_criteria(
                text=text,
                existing_game=existing_game,
                collect_patterns=detailed_patterns
            )

        # Форматируем результаты
        formatted_results = self._format_results(results, analyze_keywords)
        summary = self._create_summary(results, analyze_keywords)

        return {
            'results': formatted_results,
            'summary': summary,
            'pattern_info': pattern_info if detailed_patterns else None,
            'processing_time': time.time() - start_time,
            'has_results': summary.get('found_count', 0) > 0  # ИСПРАВЛЕНИЕ: используем get
        }

    def _analyze_criteria(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = False
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """Анализирует критерии (жанры, темы и т.д.)"""
        if not text:
            return {}, {}

        patterns = self._get_patterns()
        text_lower = text.lower()

        results = {
            'genres': [],
            'themes': [],
            'perspectives': [],
            'game_modes': []
        }
        pattern_info = {
            'genres': [],
            'themes': [],
            'perspectives': [],
            'game_modes': []
        }

        # Существующие критерии игры
        existing_items = {}
        if existing_game:
            existing_items = {
                'genres': set(existing_game.genres.values_list('name', flat=True)),
                'themes': set(existing_game.themes.values_list('name', flat=True)),
                'perspectives': set(existing_game.player_perspectives.values_list('name', flat=True)),
                'game_modes': set(existing_game.game_modes.values_list('name', flat=True))
            }

        # Анализируем каждый тип критериев
        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
            criteria_results, criteria_patterns = self._find_criteria(
                text=text,
                text_lower=text_lower,
                patterns=patterns[criteria_type],
                model=self._get_model_for_criteria(criteria_type),
                existing_names=existing_items.get(criteria_type, set()),
                collect_patterns=collect_patterns
            )

            results[criteria_type] = criteria_results
            pattern_info[criteria_type] = criteria_patterns

        return results, pattern_info

    def _analyze_keywords(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = False
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """Анализирует ключевые слова"""
        if not text:
            return {'keywords': []}, {'keywords': []}

        # Получаем все ключевые слова
        all_keywords = self._get_all_keywords()
        text_lower = text.lower()

        # Существующие ключевые слова
        existing_keywords = set()
        if existing_game:
            existing_keywords = set(existing_game.keywords.values_list('name', flat=True))

        found_keywords = []
        pattern_info = []

        for keyword in all_keywords:
            keyword_name = keyword.name
            keyword_lower = keyword.name.lower()

            # Пропускаем если уже есть у игры
            if keyword_name in existing_keywords:
                if collect_patterns:
                    pattern_info.append({
                        'name': keyword_name,
                        'status': 'skipped',
                        'reason': 'already_exists'
                    })
                continue

            # Ищем ключевое слово в тексте
            if ' ' in keyword_lower:
                # Для фраз из нескольких слов
                if keyword_lower in text_lower:
                    found_keywords.append(keyword)
                    if collect_patterns:
                        start_pos = text_lower.find(keyword_lower)
                        pattern_info.append({
                            'name': keyword_name,
                            'status': 'found',
                            'pattern': 'exact_phrase',
                            'matched_text': text[start_pos:start_pos + len(keyword_lower)],
                            'position': start_pos
                        })
            else:
                # Для отдельных слов
                pattern = rf'\b{re.escape(keyword_lower)}\b'
                match = re.search(pattern, text_lower)
                if match:
                    found_keywords.append(keyword)
                    if collect_patterns:
                        pattern_info.append({
                            'name': keyword_name,
                            'status': 'found',
                            'pattern': pattern,
                            'matched_text': text[match.start():match.end()],
                            'position': match.start()
                        })

        return {'keywords': found_keywords}, {'keywords': pattern_info}

    def _find_criteria(
            self,
            text: str,
            text_lower: str,
            patterns: Dict,
            model,
            existing_names: Set[str],
            collect_patterns: bool
    ) -> Tuple[List, List]:
        """Ищет критерии по паттернам"""
        found_items = []
        pattern_matches = []

        for name, pattern_list in patterns.items():
            # Пропускаем если уже существует
            if name.lower() in existing_names:
                if collect_patterns:
                    pattern_matches.append({
                        'name': name,
                        'status': 'skipped',
                        'reason': 'already_exists'
                    })
                continue

            # Проверяем паттерны
            for pattern in pattern_list:
                match = pattern.search(text_lower)
                if match:
                    # Получаем объект из базы
                    try:
                        obj = model.objects.get(name__iexact=name)
                        found_items.append(obj)

                        if collect_patterns:
                            pattern_matches.append({
                                'name': name,
                                'status': 'found',
                                'pattern': pattern.pattern,
                                'matched_text': text[match.start():match.end()],
                                'position': match.start()
                            })
                        break  # Переходим к следующему критерию
                    except model.DoesNotExist:
                        if self.verbose:
                            print(f"⚠️ Критерий '{name}' не найден в базе")
                        continue

        return found_items, pattern_matches

    def _format_results(self, results: Dict, is_keywords: bool) -> Dict[str, Any]:
        """Форматирует результаты для ответа"""
        if is_keywords:
            keywords = results.get('keywords', [])
            return {
                'keywords': {
                    'count': len(keywords),
                    'items': [{'id': k.id, 'name': k.name} for k in keywords]
                }
            }
        else:
            formatted = {}
            for key in ['genres', 'themes', 'perspectives', 'game_modes']:
                items = results.get(key, [])
                if items:
                    formatted[key] = {
                        'count': len(items),
                        'items': [{'id': i.id, 'name': i.name} for i in items]
                    }
            # Если все пустые, возвращаем пустой словарь
            return formatted

    def _create_summary(self, results: Dict, is_keywords: bool) -> Dict[str, Any]:
        """Создает сводку результатов"""
        if is_keywords:
            found = len(results.get('keywords', []))
            return {
                'found_count': found,
                'has_results': found > 0,
                'mode': 'keywords'
            }
        else:
            total = 0
            details = {}
            for key in ['genres', 'themes', 'perspectives', 'game_modes']:
                count = len(results.get(key, []))
                total += count
                details[f'{key}_found'] = count

            # ВАЖНОЕ ИСПРАВЛЕНИЕ: добавляем ключ 'found_count' для критериев
            details['found_count'] = total
            details['has_results'] = total > 0
            details['mode'] = 'criteria'

            return details

    def _get_patterns(self) -> Dict:
        """Загружает паттерны"""
        if self._patterns is None:
            self._patterns = PatternManager.get_all_patterns()
        return self._patterns

    def _get_all_keywords(self) -> List[Keyword]:
        """Получает все ключевые слова"""
        if self._keywords_cache is None:
            self._keywords_cache = list(Keyword.objects.all().order_by('name'))
        return self._keywords_cache

    def _get_model_for_criteria(self, criteria_type: str):
        """Возвращает модель для типа критерия"""
        models = {
            'genres': Genre,
            'themes': Theme,
            'perspectives': PlayerPerspective,
            'game_modes': GameMode
        }
        return models.get(criteria_type)

    def get_cache_stats(self) -> Dict[str, Any]:
        """Статистика кеша"""
        return {
            'patterns_loaded': self._patterns is not None,
            'keywords_cached': self._keywords_cache is not None,
            'cache_stats': self._cache_stats.copy()
        }

    def get_pattern_stats(self) -> Dict[str, Any]:
        """Статистика паттернов"""
        if self._patterns is None:
            return {'loaded': False}

        stats = {'loaded': True, 'counts': {}}
        for key, patterns in self._patterns.items():
            stats['counts'][key] = len(patterns)
        return stats

    def clear_cache(self):
        """Очищает кеш"""
        self._patterns = None
        self._keywords_cache = None
        self._cache_stats = {'hits': 0, 'misses': 0}