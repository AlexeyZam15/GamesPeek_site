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


    def _analyze_criteria_comprehensive(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = True,  # Всегда собираем паттерны для комплексного анализа
            exclude_existing: bool = False  # По умолчанию показываем все вхождения
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """Комплексный анализ критериев (жанры, темы и т.д.) с поиском ВСЕХ вхождений"""
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

        # Существующие критерии игры (используем только если exclude_existing = True)
        existing_items = {}
        if existing_game and exclude_existing:
            existing_items = {
                'genres': set(existing_game.genres.values_list('name', flat=True)),
                'themes': set(existing_game.themes.values_list('name', flat=True)),
                'perspectives': set(existing_game.player_perspectives.values_list('name', flat=True)),
                'game_modes': set(existing_game.game_modes.values_list('name', flat=True))
            }

        # Анализируем каждый тип критериев
        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
            criteria_results, criteria_patterns = self._find_criteria_comprehensive(
                text=text,
                text_lower=text_lower,
                patterns=patterns[criteria_type],
                model=self._get_model_for_criteria(criteria_type),
                existing_names=existing_items.get(criteria_type, set()) if exclude_existing else set(),
                collect_patterns=collect_patterns
            )

            results[criteria_type] = criteria_results
            pattern_info[criteria_type] = criteria_patterns

        return results, pattern_info

    def _find_criteria_comprehensive(
            self,
            text: str,
            text_lower: str,
            patterns: Dict,
            model,
            existing_names: Set[str],
            collect_patterns: bool = True
    ) -> Tuple[List, List]:
        """Ищет критерии по паттернам, находит ВСЕ вхождения в тексте"""
        found_items = []  # Уникальные объекты
        pattern_matches = []  # Все совпадения
        already_added_ids = set()  # Для отслеживания уже добавленных объектов

        for name, pattern_list in patterns.items():
            # Пропускаем если уже существует И нужно исключать
            if existing_names and name.lower() in existing_names:
                if collect_patterns:
                    pattern_matches.append({
                        'name': name,
                        'status': 'exists',
                        'reason': 'already_exists_in_game'
                    })
                continue

            # Флаг, чтобы добавить объект только один раз
            object_added = False

            # Проверяем все паттерны для этого критерия
            for pattern in pattern_list:
                # Находим ВСЕ совпадения с этим паттерном
                for match in pattern.finditer(text_lower):
                    if not object_added:
                        # Получаем объект из базы (только один раз для этого критерия)
                        try:
                            obj = model.objects.get(name__iexact=name)
                            if obj.id not in already_added_ids:
                                found_items.append(obj)
                                already_added_ids.add(obj.id)
                                object_added = True
                        except model.DoesNotExist:
                            if self.verbose:
                                print(f"⚠️ Критерий '{name}' не найден в базе")
                            break
                        except model.MultipleObjectsReturned:
                            # Если несколько объектов с одинаковым именем (в разных регистрах)
                            obj = model.objects.filter(name__iexact=name).first()
                            if obj and obj.id not in already_added_ids:
                                found_items.append(obj)
                                already_added_ids.add(obj.id)
                                object_added = True

                    if collect_patterns:
                        # Добавляем информацию о каждом совпадении
                        pattern_matches.append({
                            'name': name,
                            'status': 'found',
                            'pattern': pattern.pattern,
                            'matched_text': text[match.start():match.end()],
                            'position': match.start(),
                            'matched_word': text_lower[match.start():match.end()],
                            'context': self._get_context(text, match.start(), match.end())
                        })

        return found_items, pattern_matches

    def _get_context(self, text: str, start: int, end: int, context_length: int = 50) -> str:
        """Получает контекст вокруг найденного совпадения"""
        # Вычисляем границы для контекста
        context_start = max(0, start - context_length)
        context_end = min(len(text), end + context_length)

        # Извлекаем контекст
        context = text[context_start:context_end]

        # Добавляем многоточия если выходим за границы текста
        if context_start > 0:
            context = '...' + context
        if context_end < len(text):
            context = context + '...'

        return context

    def _analyze_keywords_comprehensive(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = True,
            exclude_existing: bool = False
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """Комплексный анализ ключевых слов с поиском ВСЕХ вхождений"""
        if not text:
            return {'keywords': []}, {'keywords': []}

        # Получаем все ключевые слова
        all_keywords = self._get_all_keywords()
        text_lower = text.lower()

        # Существующие ключевые слова (используем только если exclude_existing = True)
        existing_keywords = set()
        if existing_game and exclude_existing:
            existing_keywords = set(existing_game.keywords.values_list('name', flat=True))

        found_keywords = []  # Уникальные объекты
        pattern_info = []  # Все совпадения
        already_added_ids = set()  # Для отслеживания уже добавленных объектов

        for keyword in all_keywords:
            keyword_name = keyword.name
            keyword_lower = keyword.name.lower()

            # Пропускаем если уже есть у игры И нужно исключать
            if exclude_existing and keyword_name in existing_keywords:
                if collect_patterns:
                    pattern_info.append({
                        'name': keyword_name,
                        'status': 'exists',
                        'reason': 'already_exists_in_game'
                    })
                continue

            # Флаг, чтобы добавить объект только один раз
            object_added = False

            # Ищем ВСЕ вхождения ключевого слова в тексте
            if ' ' in keyword_lower:
                # Для фраз из нескольких слов - находим все вхождения
                start_pos = 0
                while True:
                    pos = text_lower.find(keyword_lower, start_pos)
                    if pos == -1:
                        break

                    # Добавляем ключевое слово в список (только один раз)
                    if not object_added and keyword.id not in already_added_ids:
                        found_keywords.append(keyword)
                        already_added_ids.add(keyword.id)
                        object_added = True

                    if collect_patterns:
                        pattern_info.append({
                            'name': keyword_name,
                            'status': 'found',
                            'pattern': 'exact_phrase',
                            'matched_text': text[pos:pos + len(keyword_lower)],
                            'position': pos,
                            'matched_word': text_lower[pos:pos + len(keyword_lower)],
                            'context': self._get_context(text, pos, pos + len(keyword_lower))
                        })

                    start_pos = pos + 1
            else:
                # Для отдельных слов - находим все вхождения
                import re
                pattern = rf'\b{re.escape(keyword_lower)}\b'

                for match in re.finditer(pattern, text_lower):
                    # Добавляем ключевое слово в список (только один раз)
                    if not object_added and keyword.id not in already_added_ids:
                        found_keywords.append(keyword)
                        already_added_ids.add(keyword.id)
                        object_added = True

                    if collect_patterns:
                        pattern_info.append({
                            'name': keyword_name,
                            'status': 'found',
                            'pattern': pattern,
                            'matched_text': text[match.start():match.end()],
                            'position': match.start(),
                            'matched_word': text_lower[match.start():match.end()],
                            'context': self._get_context(text, match.start(), match.end())
                        })

        return {'keywords': found_keywords}, {'keywords': pattern_info}

    def analyze_comprehensive(
            self,
            text: str,
            existing_game=None,
            detailed_patterns: bool = True,
            exclude_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Комплексный анализ, который находит ВСЕ вхождения элементов в тексте
        """
        start_time = time.time()

        if not text:
            return {
                'success': False,
                'error': 'Empty text',
                'results': {},
                'summary': {'found_count': 0, 'has_results': False},
                'pattern_info': None,
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        print(f"=== TextAnalyzer.analyze_comprehensive: Starting comprehensive analysis")
        print(f"=== Text length: {len(text)} characters")

        # Анализируем критерии
        criteria_results, criteria_patterns = self._analyze_criteria_comprehensive(
            text=text,
            existing_game=existing_game,
            collect_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        # Анализируем ключевые слова
        keywords_results, keywords_patterns = self._analyze_keywords_comprehensive(
            text=text,
            existing_game=existing_game,
            collect_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        # Объединяем результаты
        combined_results = {}
        pattern_info = {}
        total_matches = 0

        # Добавляем критерии
        for key in ['genres', 'themes', 'perspectives', 'game_modes']:
            if criteria_results.get(key):
                combined_results[key] = {
                    'count': len(criteria_results[key]),
                    'items': [{'id': i.id, 'name': i.name} for i in criteria_results[key]]
                }
                if detailed_patterns:
                    pattern_info[key] = criteria_patterns.get(key, [])
                    total_matches += len(pattern_info[key])

        # Добавляем ключевые слова
        if keywords_results.get('keywords'):
            combined_results['keywords'] = {
                'count': len(keywords_results['keywords']),
                'items': [{'id': k.id, 'name': k.name} for k in keywords_results['keywords']]
            }
            if detailed_patterns:
                pattern_info['keywords'] = keywords_patterns.get('keywords', [])
                total_matches += len(pattern_info['keywords'])

        # Создаем сводку
        total_found = sum(
            len(criteria_results.get(key, [])) for key in ['genres', 'themes', 'perspectives', 'game_modes'])
        total_found += len(keywords_results.get('keywords', []))

        summary = {
            'found_count': total_found,
            'has_results': total_found > 0,
            'mode': 'comprehensive',
            'genres_found': len(criteria_results.get('genres', [])),
            'themes_found': len(criteria_results.get('themes', [])),
            'perspectives_found': len(criteria_results.get('perspectives', [])),
            'game_modes_found': len(criteria_results.get('game_modes', [])),
            'keywords_found': len(keywords_results.get('keywords', [])),
            'total_matches': total_matches
        }

        processing_time = time.time() - start_time
        print(f"=== Comprehensive analysis completed in {processing_time:.2f}s")
        print(f"=== Found: {total_found} elements")
        print(f"=== Total matches: {total_matches}")

        return {
            'success': True,
            'results': combined_results,
            'summary': summary,
            'pattern_info': pattern_info,
            'processing_time': processing_time,
            'has_results': total_found > 0,
            'total_matches': total_matches
        }

    def analyze_combined(
            self,
            text: str,
            existing_game=None,
            detailed_patterns: bool = False,
            exclude_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Анализирует текст на наличие ВСЕХ критериев и ключевых слов

        Args:
            text: Текст для анализа
            existing_game: Существующая игра для проверки критериев
            detailed_patterns: Собирать детальную информацию о паттернах
            exclude_existing: Исключать уже существующие элементы

        Returns:
            Результаты анализа
        """
        start_time = time.time()

        if not text:
            return {
                'success': False,
                'error': 'Empty text',
                'results': {},
                'summary': {
                    'found_count': 0,
                    'has_results': False
                },
                'pattern_info': None,
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        print(f"=== TextAnalyzer.analyze_combined: Starting combined analysis")
        print(f"=== Text length: {len(text)} characters")

        # Анализируем критерии
        criteria_results, criteria_patterns = self._analyze_criteria(
            text=text,
            existing_game=existing_game,
            collect_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        # Анализируем ключевые слова
        keywords_results, keywords_patterns = self._analyze_keywords(
            text=text,
            existing_game=existing_game,
            collect_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        # Объединяем результаты
        combined_results = {}
        pattern_info = {}

        # Добавляем критерии
        for key in ['genres', 'themes', 'perspectives', 'game_modes']:
            if criteria_results.get(key):
                combined_results[key] = {
                    'count': len(criteria_results[key]),
                    'items': [{'id': i.id, 'name': i.name} for i in criteria_results[key]]
                }
                if detailed_patterns:
                    pattern_info[key] = criteria_patterns.get(key, [])

        # Добавляем ключевые слова
        if keywords_results.get('keywords'):
            combined_results['keywords'] = {
                'count': len(keywords_results['keywords']),
                'items': [{'id': k.id, 'name': k.name} for k in keywords_results['keywords']]
            }
            if detailed_patterns:
                pattern_info['keywords'] = keywords_patterns.get('keywords', [])

        # Создаем сводку
        total_found = sum(
            len(criteria_results.get(key, [])) for key in ['genres', 'themes', 'perspectives', 'game_modes'])
        total_found += len(keywords_results.get('keywords', []))

        summary = {
            'found_count': total_found,
            'has_results': total_found > 0,
            'mode': 'combined',
            'genres_found': len(criteria_results.get('genres', [])),
            'themes_found': len(criteria_results.get('themes', [])),
            'perspectives_found': len(criteria_results.get('perspectives', [])),
            'game_modes_found': len(criteria_results.get('game_modes', [])),
            'keywords_found': len(keywords_results.get('keywords', []))
        }

        processing_time = time.time() - start_time
        print(f"=== Combined analysis completed in {processing_time:.2f}s")
        print(f"=== Found: {total_found} elements")
        print(f"=== Has results: {total_found > 0}")

        return {
            'success': True,
            'results': combined_results,
            'summary': summary,
            'pattern_info': pattern_info if detailed_patterns else None,
            'processing_time': processing_time,
            'has_results': total_found > 0
        }


    def analyze(
            self,
            text: str,
            analyze_keywords: bool = False,
            existing_game=None,
            detailed_patterns: bool = False,
            exclude_existing: bool = False  # НОВЫЙ ПАРАМЕТР
    ) -> Dict[str, Any]:
        """
        Анализирует текст на наличие критериев или ключевых слов

        Args:
            text: Текст для анализа
            analyze_keywords: Искать ключевые слова (False = критерии)
            existing_game: Существующая игра для проверки критериев
            detailed_patterns: Собирать детальную информацию о паттернах
            exclude_existing: Исключать уже существующие элементы (по умолчанию False)

        Returns:
            Результаты анализа
        """
        start_time = time.time()

        if not text:
            return {
                'success': False,
                'error': 'Empty text',
                'results': {},
                'summary': {
                    'found_count': 0,
                    'has_results': False
                },
                'pattern_info': None,
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        print(
            f"=== TextAnalyzer.analyze: Starting analysis (keywords={analyze_keywords}, exclude_existing={exclude_existing})")
        print(f"=== Text length: {len(text)} characters")

        if analyze_keywords:
            print("=== Analyzing keywords...")
            results, pattern_info = self._analyze_keywords(
                text=text,
                existing_game=existing_game,
                collect_patterns=detailed_patterns,
                exclude_existing=exclude_existing
            )
        else:
            print("=== Analyzing criteria...")
            results, pattern_info = self._analyze_criteria(
                text=text,
                existing_game=existing_game,
                collect_patterns=detailed_patterns,
                exclude_existing=exclude_existing
            )

        # Форматируем результаты
        formatted_results = self._format_results(results, analyze_keywords)
        summary = self._create_summary(results, analyze_keywords)

        processing_time = time.time() - start_time
        print(f"=== Analysis completed in {processing_time:.2f}s")
        print(f"=== Found: {summary.get('found_count', 0)} elements")
        print(f"=== Has results: {summary.get('has_results', False)}")

        return {
            'success': True,
            'results': formatted_results,
            'summary': summary,
            'pattern_info': pattern_info if detailed_patterns else None,
            'processing_time': processing_time,
            'has_results': summary.get('found_count', 0) > 0
        }

    def _analyze_criteria(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = False,
            exclude_existing: bool = True  # НОВЫЙ ПАРАМЕТР
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

        # Существующие критерии игры (используем только если exclude_existing = True)
        existing_items = {}
        if existing_game and exclude_existing:
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
                existing_names=existing_items.get(criteria_type, set()) if exclude_existing else set(),
                collect_patterns=collect_patterns,
                exclude_existing=exclude_existing
            )

            results[criteria_type] = criteria_results
            pattern_info[criteria_type] = criteria_patterns

        return results, pattern_info

    def _analyze_keywords(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = False,
            exclude_existing: bool = True
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """Анализирует ключевые слова, находит все вхождения"""
        if not text:
            return {'keywords': []}, {'keywords': []}

        # Получаем все ключевые слова
        all_keywords = self._get_all_keywords()
        text_lower = text.lower()

        # Существующие ключевые слова (используем только если exclude_existing = True)
        existing_keywords = set()
        if existing_game and exclude_existing:
            existing_keywords = set(existing_game.keywords.values_list('name', flat=True))

        found_keywords = []
        pattern_info = []

        for keyword in all_keywords:
            keyword_name = keyword.name
            keyword_lower = keyword.name.lower()

            # Пропускаем если уже есть у игры И нужно исключать
            if exclude_existing and keyword_name in existing_keywords:
                if collect_patterns:
                    pattern_info.append({
                        'name': keyword_name,
                        'status': 'skipped',
                        'reason': 'already_exists'
                    })
                continue

            # Ищем ВСЕ вхождения ключевого слова в тексте
            if ' ' in keyword_lower:
                # Для фраз из нескольких слов - находим все вхождения
                start_pos = 0
                while True:
                    pos = text_lower.find(keyword_lower, start_pos)
                    if pos == -1:
                        break

                    # Добавляем ключевое слово в список (только один раз)
                    if not any(k.id == keyword.id for k in found_keywords):
                        found_keywords.append(keyword)

                    if collect_patterns:
                        pattern_info.append({
                            'name': keyword_name,
                            'status': 'found',
                            'pattern': 'exact_phrase',
                            'matched_text': text[pos:pos + len(keyword_lower)],
                            'position': pos
                        })

                    start_pos = pos + 1
            else:
                # Для отдельных слов - находим все вхождения
                import re
                pattern = rf'\b{re.escape(keyword_lower)}\b'

                for match in re.finditer(pattern, text_lower):
                    # Добавляем ключевое слово в список (только один раз)
                    if not any(k.id == keyword.id for k in found_keywords):
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
            collect_patterns: bool,
            exclude_existing: bool
    ) -> Tuple[List, List]:
        """Ищет критерии по паттернам, находит все вхождения"""
        found_items = []
        pattern_matches = []

        for name, pattern_list in patterns.items():
            # Пропускаем если уже существует И нужно исключать
            if exclude_existing and name.lower() in existing_names:
                if collect_patterns:
                    pattern_matches.append({
                        'name': name,
                        'status': 'skipped',
                        'reason': 'already_exists'
                    })
                continue

            # Проверяем все паттерны для этого критерия
            found_in_text = False

            for pattern in pattern_list:
                # Находим ВСЕ совпадения с этим паттерном
                for match in pattern.finditer(text_lower):
                    if not found_in_text:
                        # Получаем объект из базы (только один раз)
                        try:
                            obj = model.objects.get(name__iexact=name)
                            found_items.append(obj)
                            found_in_text = True
                        except model.DoesNotExist:
                            if self.verbose:
                                print(f"⚠️ Критерий '{name}' не найден в базе")
                            break

                    if collect_patterns:
                        pattern_matches.append({
                            'name': name,
                            'status': 'found',
                            'pattern': pattern.pattern,
                            'matched_text': text[match.start():match.end()],
                            'position': match.start()
                        })

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