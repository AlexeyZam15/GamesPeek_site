# games/analyze/text_analyzer.py - ОБНОВЛЕННЫЙ КЛАСС
import re
import time
from typing import Dict, Any, List, Optional, Set, Tuple

from games.models import Genre, Theme, PlayerPerspective, GameMode, Keyword
from .pattern_manager import PatternManager
from .sync_patterns_to_db import ensure_patterns_in_db, PatternAutoSyncer
from .range_cache import RangeCacheManager
from .keyword_trie import KeywordTrieManager  # НОВЫЙ ИМПОРТ


class TextAnalyzer:
    """Анализатор текста с оптимизированным поиском ключевых слов"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self._patterns = None
        self._trie_manager = KeywordTrieManager()  # Используем менеджер Trie
        self._trie = None
        self._keywords_count = 0

    # ============== ОПТИМИЗИРОВАННЫЕ МЕТОДЫ КЛЮЧЕВЫХ СЛОВ ==============

    def _ensure_trie_loaded(self):
        """Гарантирует, что Trie ключевых слов загружен"""
        if self._trie is None:
            self._trie = self._trie_manager.get_trie(verbose=self.verbose)
            self._keywords_count = Keyword.objects.count()
        else:
            # Проверяем, не изменилось ли количество ключевых слов
            current_count = Keyword.objects.count()
            if current_count != self._keywords_count:
                print(f"⚠️ Количество ключевых слов изменилось: было {self._keywords_count}, стало {current_count}")
                print("⚠️ Перезагружаем Trie...")
                self._trie = self._trie_manager.get_trie(verbose=self.verbose, force_rebuild=True)
                self._keywords_count = current_count

    def _analyze_keywords_fast(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = True,
            exclude_existing: bool = False
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """
        ОПТИМИЗИРОВАННЫЙ: Анализ ключевых слов с использованием Trie
        """
        start_time = time.time()

        if not text:
            return {'keywords': []}, {'keywords': []}

        # Загружаем Trie если нужно
        self._ensure_trie_loaded()

        # Существующие ключевые слова игры
        existing_keyword_ids = set()
        if existing_game and exclude_existing:
            existing_keyword_ids = set(existing_game.keywords.values_list('id', flat=True))

        # БЫСТРЫЙ ПОИСК через Trie
        if self.verbose:
            print(f"🔍 Поиск ключевых слов в тексте ({len(text)} символов)...")

        trie_results = self._trie.find_all_in_text(text)

        if self.verbose:
            print(f"✅ Найдено {len(trie_results)} совпадений за {(time.time() - start_time) * 1000:.1f}ms")

        # ДОБАВЛЯЕМ ПОИСК МНОЖЕСТВЕННЫХ ФОРМ
        text_lower = text.lower()
        plural_matches = []
        hyphen_matches = []  # ИСПРАВЛЕНО: добавляем определение переменной

        # Проверяем каждое ключевое слово на множественные формы
        for kw_id, kw_data in self._trie.keywords_cache.items():
            if kw_id in existing_keyword_ids:
                continue

            keyword_name = kw_data['name_lower']

            # Ищем множественные формы: слово + 's'
            plural_form = keyword_name + 's'
            pos = 0
            while True:
                found_pos = text_lower.find(plural_form, pos)
                if found_pos == -1:
                    break

                # Проверяем границы слова
                if (found_pos == 0 or not text_lower[found_pos - 1].isalnum()) and \
                        (found_pos + len(plural_form) >= len(text_lower) or
                         not text_lower[found_pos + len(plural_form)].isalnum()):
                    plural_matches.append({
                        'id': kw_id,
                        'name': kw_data['name'],
                        'position': found_pos,
                        'length': len(plural_form),
                        'text': text_lower[found_pos:found_pos + len(plural_form)],
                        'is_plural': True
                    })

                pos = found_pos + 1

            # Ищем формы с дефисом: слово + '-'
            hyphen_form = keyword_name + '-'
            pos = 0
            while True:
                found_pos = text_lower.find(hyphen_form, pos)
                if found_pos == -1:
                    break

                hyphen_matches.append({
                    'id': kw_id,
                    'name': kw_data['name'],
                    'position': found_pos,
                    'length': len(hyphen_form),
                    'text': text_lower[found_pos:found_pos + len(hyphen_form)],
                    'is_hyphen': True
                })

                pos = found_pos + 1

        # Объединяем результаты Trie и поиска множественных форм
        all_results = trie_results.copy()
        all_results.extend(plural_matches)
        all_results.extend(hyphen_matches)

        # Фильтруем по существующим
        filtered_results = []
        for result in all_results:
            if result['id'] not in existing_keyword_ids:
                filtered_results.append(result)

        # Группируем уникальные ключевые слова
        unique_keywords = {}
        pattern_info = []

        for result in filtered_results:
            kw_id = result['id']

            if kw_id not in unique_keywords:
                # Получаем объект Keyword
                keyword_data = self._trie.keywords_cache.get(kw_id)
                if keyword_data:
                    unique_keywords[kw_id] = {
                        'id': kw_id,
                        'name': keyword_data['name'],
                        'count': 0,
                        'positions': [],
                        'texts': []
                    }

            if kw_id in unique_keywords:
                unique_keywords[kw_id]['count'] += 1
                unique_keywords[kw_id]['positions'].append(result['position'])
                unique_keywords[kw_id]['texts'].append(result['text'])

        # Собираем объекты Keyword
        found_keywords = []
        for kw_id, kw_data in unique_keywords.items():
            try:
                kw_obj = Keyword.objects.get(id=kw_id)
                found_keywords.append(kw_obj)

                if collect_patterns:
                    # Добавляем информацию о совпадениях
                    for pos, txt in zip(kw_data['positions'], kw_data['texts']):
                        pattern_info.append({
                            'name': kw_data['name'],
                            'status': 'found',
                            'pattern': 'exact_match',
                            'matched_text': txt,
                            'position': pos,
                            'matched_word': txt,
                            'context': self._get_context(text, pos, pos + len(txt)),
                            'keyword_id': kw_id,
                            'count': kw_data['count']
                        })
            except Keyword.DoesNotExist:
                continue

        # Обновляем кэш диапазонов если нужно
        if found_keywords:
            min_id = min(kw.id for kw in found_keywords)
            max_id = max(kw.id for kw in found_keywords)
            RangeCacheManager.update_criteria_range('keywords', min_id, max_id)

        return {'keywords': found_keywords}, {'keywords': pattern_info}

    def _analyze_keywords_comprehensive(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = True,
            exclude_existing: bool = False
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """
        ЗАМЕНЯЕТ СТАРЫЙ МЕТОД: Использует оптимизированный поиск
        """
        return self._analyze_keywords_fast(
            text=text,
            existing_game=existing_game,
            collect_patterns=collect_patterns,
            exclude_existing=exclude_existing
        )

    # ============== ОПТИМИЗИРОВАННЫЕ КОМПОЗИТНЫЕ МЕТОДЫ ==============

    def analyze(
            self,
            text: str,
            analyze_keywords: bool = False,
            existing_game=None,
            detailed_patterns: bool = False,
            exclude_existing: bool = False
    ) -> Dict[str, Any]:
        """
        ОПТИМИЗИРОВАННЫЙ: Основной метод анализа
        """
        start_time = time.time()

        if not text:
            return {
                'success': False,
                'error': 'Empty text',
                'results': {},
                'summary': {'found_count': 0, 'has_results': False},
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        # Ограничиваем текст для скорости (только для длинных текстов)
        if len(text) > 10000:
            text = text[:10000]

        text_lower = text.lower()

        if analyze_keywords:
            # Используем быстрый анализ ключевых слов
            keywords_results, keywords_patterns = self._analyze_keywords_fast(
                text=text,
                existing_game=existing_game,
                collect_patterns=detailed_patterns,
                exclude_existing=exclude_existing
            )

            total_found = len(keywords_results.get('keywords', []))

            return {
                'success': True,
                'results': keywords_results,
                'summary': {
                    'found_count': total_found,
                    'has_results': total_found > 0,
                    'mode': 'keywords_only'
                },
                'pattern_info': keywords_patterns if detailed_patterns else {},
                'processing_time': time.time() - start_time,
                'has_results': total_found > 0
            }
        else:
            # Анализ критериев (оставляем старый алгоритм, он уже быстрый)
            patterns = self._get_patterns()

            results = {}
            pattern_info = {}
            total_found = 0

            # Существующие критерии игры
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
                model = self._get_model_for_criteria(criteria_type)
                found_items = []
                patterns_for_type = patterns[criteria_type]

                if detailed_patterns:
                    pattern_info[criteria_type] = []

                for name, pattern_list in patterns_for_type.items():
                    # Пропускаем если уже существует у игры
                    if exclude_existing:
                        existing_names_lower = {n.lower() for n in existing_items.get(criteria_type, set())}
                        if name.lower() in existing_names_lower:
                            if detailed_patterns:
                                pattern_info[criteria_type].append({
                                    'name': name,
                                    'status': 'skipped',
                                    'reason': 'already_exists_in_game'
                                })
                            continue

                    # Проверяем паттерны
                    for pattern in pattern_list:
                        if pattern.search(text_lower):
                            # Нашли совпадение
                            try:
                                obj = model.objects.filter(name__iexact=name).first()
                                if obj and obj not in found_items:
                                    found_items.append(obj)

                                    if detailed_patterns:
                                        # Находим первое совпадение
                                        match = pattern.search(text_lower)
                                        if match:
                                            pattern_info[criteria_type].append({
                                                'name': name,
                                                'status': 'found',
                                                'pattern': pattern.pattern,
                                                'matched_text': text[match.start():match.end()],
                                                'position': match.start(),
                                                'matched_word': text_lower[match.start():match.end()],
                                                'context': self._get_context(text, match.start(), match.end())
                                            })
                                    break  # Нашли один паттерн - достаточно
                            except Exception:
                                pass

                if found_items:
                    results[criteria_type] = {
                        'count': len(found_items),
                        'items': [{'id': i.id, 'name': i.name} for i in found_items]
                    }
                    total_found += len(found_items)

            processing_time = time.time() - start_time

            return {
                'success': True,
                'results': results,
                'summary': {
                    'found_count': total_found,
                    'has_results': total_found > 0,
                    'mode': 'criteria_only'
                },
                'pattern_info': pattern_info if detailed_patterns else {},
                'processing_time': processing_time,
                'has_results': total_found > 0
            }

    def analyze_comprehensive(
            self,
            text: str,
            existing_game=None,
            detailed_patterns: bool = True,
            exclude_existing: bool = False
    ) -> Dict[str, Any]:
        """
        ОПТИМИЗИРОВАННЫЙ: Комплексный анализ с быстрыми ключевыми словами
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

        # Ограничиваем текст
        if len(text) > 10000:
            text = text[:10000]

        # БЫСТРЫЙ анализ ключевых слов через Trie
        keywords_results, keywords_patterns = self._analyze_keywords_fast(
            text=text,
            existing_game=existing_game,
            collect_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        # Анализ критериев (существующий алгоритм)
        patterns = self._get_patterns()
        text_lower = text.lower()

        results = {}
        pattern_info = {}

        # Существующие критерии игры
        existing_items = {}
        if existing_game and exclude_existing:
            existing_items = {
                'genres': set(existing_game.genres.values_list('name', flat=True)),
                'themes': set(existing_game.themes.values_list('name', flat=True)),
                'perspectives': set(existing_game.player_perspectives.values_list('name', flat=True)),
                'game_modes': set(existing_game.game_modes.values_list('name', flat=True))
            }

        # Анализируем критерии
        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
            model = self._get_model_for_criteria(criteria_type)
            found_items = []
            patterns_for_type = patterns[criteria_type]

            if detailed_patterns:
                pattern_info[criteria_type] = []

            for name, pattern_list in patterns_for_type.items():
                # Пропускаем если уже существует у игры
                if exclude_existing:
                    existing_names_lower = {n.lower() for n in existing_items.get(criteria_type, set())}
                    if name.lower() in existing_names_lower:
                        if detailed_patterns:
                            pattern_info[criteria_type].append({
                                'name': name,
                                'status': 'skipped',
                                'reason': 'already_exists_in_game'
                            })
                        continue

                for pattern in pattern_list:
                    if pattern.search(text_lower):
                        try:
                            obj = model.objects.filter(name__iexact=name).first()
                            if obj and obj not in found_items:
                                found_items.append(obj)

                                if detailed_patterns:
                                    match = pattern.search(text_lower)
                                    if match:
                                        pattern_info[criteria_type].append({
                                            'name': name,
                                            'status': 'found',
                                            'pattern': pattern.pattern,
                                            'matched_text': text[match.start():match.end()],
                                            'position': match.start(),
                                            'matched_word': text_lower[match.start():match.end()],
                                            'context': self._get_context(text, match.start(), match.end())
                                        })
                                break
                        except Exception:
                            pass

            if found_items:
                results[criteria_type] = {
                    'count': len(found_items),
                    'items': [{'id': i.id, 'name': i.name} for i in found_items]
                }

        # Добавляем ключевые слова
        if keywords_results.get('keywords'):
            results['keywords'] = {
                'count': len(keywords_results['keywords']),
                'items': [{'id': k.id, 'name': k.name} for k in keywords_results['keywords']]
            }

        if detailed_patterns and keywords_patterns.get('keywords'):
            pattern_info['keywords'] = keywords_patterns['keywords']

        # Считаем итоги
        total_found = sum(len(results.get(key, {}).get('items', []))
                          for key in ['genres', 'themes', 'perspectives', 'game_modes', 'keywords'])
        total_matches = sum(len(pattern_info.get(key, [])) for key in pattern_info)

        summary = {
            'found_count': total_found,
            'has_results': total_found > 0,
            'mode': 'comprehensive',
            'total_matches': total_matches
        }

        processing_time = time.time() - start_time

        if self.verbose:
            print(f"⚡ Комплексный анализ завершен за {processing_time:.2f}s")
            print(f"📊 Найдено элементов: {total_found}, совпадений: {total_matches}")

        return {
            'success': True,
            'results': results,
            'summary': summary,
            'pattern_info': pattern_info,
            'processing_time': processing_time,
            'has_results': total_found > 0,
            'total_matches': total_matches
        }

    # ============== ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ ==============

    def _get_patterns(self) -> Dict:
        """Загружает паттерны (с кэшированием)"""
        if self._patterns is None:
            self._patterns = PatternManager.get_all_patterns()
        return self._patterns

    def _get_model_for_criteria(self, criteria_type: str):
        """Возвращает модель для типа критерия"""
        models = {
            'genres': Genre,
            'themes': Theme,
            'perspectives': PlayerPerspective,
            'game_modes': GameMode
        }
        return models.get(criteria_type)

    def _get_context(self, text: str, start: int, end: int, context_length: int = 50) -> str:
        """Получает контекст вокруг найденного совпадения"""
        context_start = max(0, start - context_length)
        context_end = min(len(text), end + context_length)

        context = text[context_start:context_end]

        if context_start > 0:
            context = '...' + context
        if context_end < len(text):
            context = context + '...'

        return context

    def clear_cache(self):
        """Очищает кэши"""
        self._patterns = None
        self._trie_manager.clear_cache()
        self._trie = None
        self._keywords_count = 0
