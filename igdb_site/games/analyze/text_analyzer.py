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

    def _analyze_keywords_for_game(
            self,
            text: str,
            existing_game=None,
            exclude_existing: bool = False
    ) -> Dict[str, List]:
        """
        Анализ ключевых слов для добавления к игре
        Возвращает только уникальные ключевые слова
        """
        if not text:
            return {'keywords': []}

        # Загружаем Trie если нужно
        self._ensure_trie_loaded()

        # Существующие ключевые слова игры
        existing_keyword_ids = set()
        if existing_game and exclude_existing:
            existing_keyword_ids = set(existing_game.keywords.values_list('id', flat=True))

        # Поиск через Trie (только уникальные ключевые слова)
        trie_results = self._trie.find_all_in_text(text)  # По умолчанию unique_only=True

        # Фильтруем по существующим
        filtered_results = []
        for result in trie_results:
            if result['id'] not in existing_keyword_ids:
                filtered_results.append(result)

        # Получаем объекты Keyword для уникальных ID
        found_keywords = []
        seen_ids = set()

        for result in filtered_results:
            if result['id'] not in seen_ids:
                seen_ids.add(result['id'])
                try:
                    kw_obj = Keyword.objects.get(id=result['id'])
                    found_keywords.append(kw_obj)
                except Keyword.DoesNotExist:
                    continue

        return {'keywords': found_keywords}

    def _analyze_keywords_for_highlight(
            self,
            text: str,
            existing_game=None,
            exclude_existing: bool = False
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """
        Анализ ключевых слов для подсветки текста
        Возвращает все вхождения и pattern_info
        """
        if not text:
            return {'keywords': []}, {'keywords': []}

        # Загружаем Trie если нужно
        self._ensure_trie_loaded()

        # Существующие ключевые слова игры
        existing_keyword_ids = set()
        if existing_game and exclude_existing:
            existing_keyword_ids = set(existing_game.keywords.values_list('id', flat=True))

        # Поиск через Trie (ВСЕ вхождения)
        # Используем метод с параметром unique_only=False
        trie_results = self._trie.find_all_in_text(text, unique_only=False)

        # Фильтруем по существующим
        filtered_results = []
        for result in trie_results:
            if result['id'] not in existing_keyword_ids:
                filtered_results.append(result)

        # Группируем результаты по ключевым словам для pattern_info
        keyword_groups = {}
        pattern_info = []

        for result in filtered_results:
            kw_id = result['id']

            if kw_id not in keyword_groups:
                keyword_data = self._trie.keywords_cache.get(kw_id)
                if keyword_data:
                    keyword_groups[kw_id] = {
                        'id': kw_id,
                        'name': keyword_data['name'],
                        'count': 0,
                        'positions': [],
                        'texts': []
                    }

            if kw_id in keyword_groups:
                keyword_groups[kw_id]['count'] += 1
                keyword_groups[kw_id]['positions'].append(result['position'])
                keyword_groups[kw_id]['texts'].append(result['text'])

        # Собираем объекты Keyword (уникальные для добавления)
        found_keywords = []
        seen_ids = set()

        for result in filtered_results:
            if result['id'] not in seen_ids:
                seen_ids.add(result['id'])
                try:
                    kw_obj = Keyword.objects.get(id=result['id'])
                    found_keywords.append(kw_obj)
                except Keyword.DoesNotExist:
                    continue

        # Создаем pattern_info для ВСЕХ вхождений
        for kw_id, kw_data in keyword_groups.items():
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

        return {'keywords': found_keywords}, {'keywords': pattern_info}

    def _analyze_keywords_fast(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = True,
            exclude_existing: bool = False
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """
        ОПТИМИЗИРОВАННЫЙ: Анализ ключевых слов с использованием Trie
        УЛУЧШЕНО: находит ключевые слова в составных словах (через дефис)
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

        # БЫСТРЫЙ ПОИСК через Trie с улучшенной логикой
        if self.verbose:
            print(f"🔍 Поиск ключевых слов в тексте ({len(text)} символов)...")

        # Шаг 1: Поиск через Trie (обычные слова)
        trie_results = self._trie.find_all_in_text(text, unique_only=False)

        if self.verbose:
            print(f"✅ Найдено {len(trie_results)} совпадений за {(time.time() - start_time) * 1000:.1f}ms")

        # Шаг 2: Поиск в составных словах через дефис
        hyphenated_results = self._find_keywords_in_hyphenated_words(
            text,
            self._trie.keywords_cache,
            existing_keyword_ids
        )

        # Объединяем результаты
        all_results = trie_results + hyphenated_results

        # Фильтруем по существующим
        filtered_results = []
        for result in all_results:
            if result['id'] not in existing_keyword_ids:
                filtered_results.append(result)

        # Группируем уникальные ключевые слова ДЛЯ ДОБАВЛЕНИЯ
        unique_keywords = {}
        pattern_info = []

        for result in filtered_results:
            kw_id = result['id']

            # Для добавления - только уникальные
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

            # Собираем все вхождения для подсветки
            if kw_id in unique_keywords:
                unique_keywords[kw_id]['count'] += 1
                unique_keywords[kw_id]['positions'].append(result['position'])
                unique_keywords[kw_id]['texts'].append(result['text'])

        # Собираем объекты Keyword ДЛЯ ДОБАВЛЕНИЯ
        found_keywords = []
        for kw_id, kw_data in unique_keywords.items():
            try:
                kw_obj = Keyword.objects.get(id=kw_id)
                found_keywords.append(kw_obj)

                if collect_patterns:
                    # Добавляем информацию о ВСЕХ совпадениях для подсветки
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
                            'count': kw_data['count'],
                            'is_hyphenated_part': result.get('is_hyphenated_part', False)
                        })
            except Keyword.DoesNotExist:
                continue

        processing_time = time.time() - start_time

        if self.verbose:
            print(f"⚡ Быстрый анализ ключевых слов завершен за {processing_time:.3f} секунд")
            print(f"📊 Найдено уникальных ключевых слов: {len(found_keywords)}")
            print(f"📊 Найдено всего вхождений: {len(filtered_results)}")
            if hyphenated_results:
                print(f"📊 Из них в составных словах: {len(hyphenated_results)}")

        return {'keywords': found_keywords}, {'keywords': pattern_info}

    def _find_keywords_in_hyphenated_words(
            self,
            text: str,
            keywords_cache: Dict[int, Dict],
            existing_keyword_ids: Set[int]
    ) -> List[Dict]:
        """
        Находит ключевые слова внутри составных слов через дефис
        УПРОЩЕНО: рассматривает составные слова как отдельные части
        """
        import re

        # Ищем все слова с дефисами в тексте
        hyphenated_words = re.findall(r'\b[\w]+-[\w]+\b', text.lower())

        if not hyphenated_words:
            return []

        if self.verbose:
            print(f"🔍 Найдено {len(hyphenated_words)} составных слов через дефис")

        results = []

        # Для каждого составного слова через дефис
        for hyphen_word in hyphenated_words:
            # Разбиваем на части по дефису
            parts = hyphen_word.split('-')

            # Проверяем каждую часть
            for part in parts:
                if len(part) >= 3:  # Только значимые части (не менее 3 символов)
                    # Ищем часть в кэше ключевых слов
                    found_keyword = None
                    for keyword_id, keyword_data in keywords_cache.items():
                        if keyword_data['name_lower'] == part:
                            found_keyword = {
                                'id': keyword_id,
                                'name': keyword_data['name'],
                                'name_lower': part
                            }
                            break

                    if found_keyword and found_keyword['id'] not in existing_keyword_ids:
                        # Находим позицию этой части в исходном тексте
                        # Простой поиск части в тексте (в нижнем регистре)
                        part_lower = part.lower()
                        text_lower = text.lower()

                        # Ищем все вхождения этой части
                        pos = 0
                        while True:
                            found_pos = text_lower.find(part_lower, pos)
                            if found_pos == -1:
                                break

                            # Проверяем границы
                            # Допускаем дефис после слова
                            end_pos = found_pos + len(part_lower)
                            is_valid = True

                            # Проверяем начало
                            if found_pos > 0:
                                prev_char = text[found_pos - 1]
                                if prev_char.isalnum() and prev_char != '-':
                                    is_valid = False

                            # Проверяем конец
                            if end_pos < len(text):
                                next_char = text[end_pos]
                                # Допускаем дефис после слова
                                if next_char.isalnum() and next_char != '-':
                                    is_valid = False

                            if is_valid:
                                # Добавляем в результаты
                                results.append({
                                    'id': found_keyword['id'],
                                    'name': found_keyword['name'],
                                    'position': found_pos,
                                    'length': len(part_lower),
                                    'text': text[found_pos:end_pos].lower(),
                                    'is_hyphenated_part': True
                                })

                                if self.verbose:
                                    print(
                                        f"   ✅ Найдено ключевое слово '{found_keyword['name']}' в составном слове '{hyphen_word}'")

                            pos = found_pos + 1

        return results

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
        Основной метод анализа
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

        # Ограничиваем текст для скорости
        if len(text) > 10000:
            text = text[:10000]

        if analyze_keywords:
            # Выбираем нужную функцию в зависимости от контекста
            if detailed_patterns:
                # Для отображения с подсветкой - используем все вхождения
                keywords_results, keywords_patterns = self._analyze_keywords_for_highlight(
                    text=text,
                    existing_game=existing_game,
                    exclude_existing=exclude_existing
                )
            else:
                # Для простого анализа (только уникальные)
                keywords_results = self._analyze_keywords_for_game(
                    text=text,
                    existing_game=existing_game,
                    exclude_existing=exclude_existing
                )
                keywords_patterns = {'keywords': []}

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
            text_lower = text.lower()

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
                                        # Находим ВСЕ совпадения для подсветки
                                        matches = pattern.finditer(text_lower)
                                        for match in matches:
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
        """
        Получает контекст вокруг найденного совпадения
        УЛУЧШЕНО: лучше обрабатывает дефисы и составные слова
        """
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
