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
        self.debug = False  # ДОБАВЛЯЕМ АТРИБУТ debug
        self._patterns = None
        self._trie_manager = KeywordTrieManager()
        self._trie = None
        self._keywords_count = 0

    # ============== ОПТИМИЗИРОВАННЫЕ МЕТОДЫ КЛЮЧЕВЫХ СЛОВ ==============

    def _ensure_trie_loaded(self):
        """Гарантирует, что Trie ключевых слов загружен - С ИНДИКАТОРОМ ПРОГРЕССА"""
        if self._trie is None:
            if self.verbose:
                print("🔄 Загружаем Trie ключевых слов...")
            start_time = time.time()
            self._trie = self._trie_manager.get_trie(verbose=self.verbose)
            self._keywords_count = Keyword.objects.count()
            if self.verbose:
                print(f"✅ Trie загружен за {time.time() - start_time:.2f}с ({self._keywords_count} ключевых слов)")
        else:
            # Проверяем, не изменилось ли количество ключевых слов
            current_count = Keyword.objects.count()
            if current_count != self._keywords_count:
                print(f"⚠️ Количество ключевых слов изменилось: было {self._keywords_count}, стало {current_count}")
                print("⚠️ Перезагружаем Trie...")
                start_time = time.time()
                self._trie = self._trie_manager.get_trie(verbose=self.verbose, force_rebuild=True)
                self._keywords_count = current_count
                print(f"✅ Trie перезагружен за {time.time() - start_time:.2f}с")

    def _analyze_keywords_for_game(
            self,
            text: str,
            existing_game=None,
            exclude_existing: bool = False
    ) -> Dict[str, List]:
        """
        Анализ ключевых слов для добавления к игре
        Возвращает только НОВЫЕ ключевые слова, которых еще нет у игры
        ИСПРАВЛЕНО: правильно использует Trie для поиска
        """
        if not text:
            return {'keywords': []}

        # Загружаем Trie если нужно
        self._ensure_trie_loaded()

        # Существующие ключевые слова игры
        existing_keyword_ids = set()
        if existing_game and exclude_existing:
            existing_keyword_ids = set(existing_game.keywords.values_list('id', flat=True))

        # Поиск через Trie - получаем все вхождения с позициями
        trie_results = self._trie.find_all_in_text(text, unique_only=False)

        # ПРИНУДИТЕЛЬНЫЙ ВЫВОД В КОНСОЛЬ
        if self.debug:
            import sys
            sys.stderr.write(f"\n=== ОТЛАДКА _analyze_keywords_for_game ===\n")
            sys.stderr.write(f"Игра ID: {existing_game.id if existing_game else 'unknown'}\n")
            sys.stderr.write(f"Длина текста: {len(text)}\n")
            sys.stderr.write(f"Всего совпадений в тексте: {len(trie_results)}\n")
            for r in trie_results:
                sys.stderr.write(f"  - ID: {r['id']}, текст: '{r['text']}', позиция: {r['position']}\n")
            sys.stderr.write("=" * 50 + "\n")
            sys.stderr.flush()

        # Фильтруем по существующим у игры
        filtered_results = []
        for result in trie_results:
            if result['id'] not in existing_keyword_ids:
                filtered_results.append(result)

        # Проверяем, какие ключевые слова существуют в базе данных
        found_keyword_ids = {result['id'] for result in filtered_results}

        # Получаем объекты Keyword только для существующих в базе
        from games.models import Keyword
        existing_in_db = Keyword.objects.filter(id__in=found_keyword_ids).values_list('id', flat=True)
        existing_in_db_set = set(existing_in_db)

        # Фильтруем результаты - только ключевые слова, существующие в базе И не связанные с игрой
        valid_results = [r for r in filtered_results if r['id'] in existing_in_db_set]

        # Собираем объекты Keyword для уникальных ID
        found_keywords = []
        seen_ids = set()

        # Словарь для хранения найденного текста
        found_text_dict = {}

        for result in valid_results:
            if result['id'] not in seen_ids:
                seen_ids.add(result['id'])
                try:
                    kw_obj = Keyword.objects.get(id=result['id'])
                    found_keywords.append(kw_obj)

                    # Сохраняем текст, который был найден (первое вхождение)
                    if result['id'] not in found_text_dict:
                        found_text_dict[result['id']] = result['text']

                    if self.verbose:
                        print(
                            f"🔍 DEBUG: Для {kw_obj.name} найден текст '{result['text']}' на позиции {result['position']}")
                except Keyword.DoesNotExist:
                    continue

        if self.verbose:
            print(f"🔍 Найдено новых ключевых слов: {len(found_keywords)} (из {len(filtered_results)} совпадений)")
            if len(found_keywords) == 0 and len(trie_results) > 0:
                print(f"ℹ️ Все найденные ключевые слова уже есть у игры")

            # Показываем найденный текст для verbose режима
            if found_text_dict and found_keywords:
                print(f"📌 Найденный текст:")
                for kw in found_keywords:
                    if kw.id in found_text_dict:
                        found_text = found_text_dict[kw.id]
                        print(f"   • {kw.name} → найдено как \"{found_text}\"")

        # Возвращаем ключевые слова и найденный текст
        result_dict = {
            'keywords': found_keywords,
            '_found_text': found_text_dict
        }

        if self.verbose:
            print(f"🔍 DEBUG _analyze_keywords_for_game возвращает: {list(result_dict.keys())}")
            if found_text_dict:
                print(f"🔍 DEBUG _analyze_keywords_for_game _found_text: {found_text_dict}")

        return result_dict

    def _analyze_keywords_for_highlight(
            self,
            text: str,
            existing_game=None,
            exclude_existing: bool = False
    ) -> Tuple[Dict[str, List], Dict[str, List]]:
        """
        Анализ ключевых слов для подсветки текста
        ИСПРАВЛЕНО: использует тот же метод поиска, что и прямой Trie
        """
        if not text:
            return {'keywords': []}, {'keywords': []}

        # Загружаем Trie если нужно
        self._ensure_trie_loaded()

        # Существующие ключевые слова игры (только для информации)
        existing_keyword_ids = set()
        if existing_game and exclude_existing:
            existing_keyword_ids = set(existing_game.keywords.values_list('id', flat=True))

        # Прямой поиск через Trie - используем тот же метод, что и в тесте
        trie_results = self._trie.find_all_in_text(text, unique_only=False)

        # ИСПРАВЛЕНИЕ: Отладочный вывод только если включен self.debug
        if self.debug:
            print(f"\n=== DEBUG _analyze_keywords_for_highlight ===")
            print(f"Всего найдено вхождений в тексте: {len(trie_results)}")
            for r in trie_results:
                print(f"  - ID: {r['id']}, текст: '{r['text']}', позиция: {r['position']}")

        # Собираем уникальные ключевые слова для добавления
        found_keywords = []
        seen_ids = set()

        # Создаем pattern_info для ВСЕХ вхождений
        pattern_info = []

        for result in trie_results:
            kw_id = result['id']

            # Получаем данные ключевого слова из кэша
            keyword_data = self._trie.keywords_cache.get(kw_id)
            if not keyword_data:
                continue

            # Добавляем в pattern_info (для всех вхождений)
            pattern_info.append({
                'name': keyword_data['name'],
                'status': 'found',
                'pattern': 'exact_match',
                'matched_text': result['text'],
                'position': result['position'],
                'matched_word': result['text'],
                'context': self._get_context(text, result['position'], result['position'] + len(result['text'])),
                'keyword_id': kw_id,
                'already_exists': kw_id in existing_keyword_ids
            })

            # Для добавления - только уникальные и не существующие
            if kw_id not in seen_ids and kw_id not in existing_keyword_ids:
                seen_ids.add(kw_id)
                try:
                    kw_obj = Keyword.objects.get(id=kw_id)
                    found_keywords.append(kw_obj)
                except Keyword.DoesNotExist:
                    continue

        if self.debug:
            print(f"Создано pattern_info: {len(pattern_info)} записей")
            print(f"Найдено уникальных ключевых слов для добавления: {len(found_keywords)}")
            print("=" * 50)

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

        # Поиск через Trie
        trie_results = self._trie.find_all_in_text(text, unique_only=False)

        # Фильтруем по существующим
        filtered_results = []
        for result in trie_results:
            if result['id'] not in existing_keyword_ids:
                filtered_results.append(result)

        # Группируем уникальные ключевые слова
        unique_keywords = {}
        pattern_info = []

        for result in filtered_results:
            kw_id = result['id']
            # ВАЖНО: используем оригинальный текст из result
            matched_text = result['text']
            position = result['position']
            matched_lemma = result.get('matched_lemma', matched_text)

            if kw_id not in unique_keywords:
                keyword_data = self._trie.keywords_cache.get(kw_id)
                if keyword_data:
                    unique_keywords[kw_id] = {
                        'id': kw_id,
                        'name': keyword_data['name'],
                        'count': 0
                    }

            if kw_id in unique_keywords:
                unique_keywords[kw_id]['count'] += 1

                if collect_patterns:
                    context = self._get_context(text, position, position + len(matched_text))

                    pattern_info.append({
                        'name': unique_keywords[kw_id]['name'],
                        'status': 'found',
                        'matched_text': matched_text,  # Оригинальный текст!
                        'position': position,
                        'matched_lemma': matched_lemma,
                        'context': context,
                        'keyword_id': kw_id
                    })

        # Собираем объекты Keyword
        found_keywords = []
        for kw_id in unique_keywords:
            try:
                kw_obj = Keyword.objects.get(id=kw_id)
                found_keywords.append(kw_obj)
            except Keyword.DoesNotExist:
                continue

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
        ИСПРАВЛЕНО: для ключевых слов всегда используем _analyze_keywords_for_highlight
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
            # ВСЕГДА используем _analyze_keywords_for_highlight для получения pattern_info
            keywords_results, keywords_patterns = self._analyze_keywords_for_highlight(
                text=text,
                existing_game=existing_game,
                exclude_existing=exclude_existing
            )

            total_found = len(keywords_results.get('keywords', []))

            result = {
                'success': True,
                'results': keywords_results,
                'summary': {
                    'found_count': total_found,
                    'has_results': total_found > 0,
                    'mode': 'keywords_only'
                },
                'pattern_info': keywords_patterns,  # Всегда возвращаем pattern_info
                'processing_time': time.time() - start_time,
                'has_results': total_found > 0
            }

            return result
        else:
            # Анализ критериев
            patterns = self._get_patterns()
            text_lower = text.lower()

            results = {}
            pattern_info = {}  # СЛОВАРЬ для хранения информации о паттернах
            total_found = 0

            existing_items = {}
            if existing_game and exclude_existing:
                existing_items = {
                    'genres': set(existing_game.genres.values_list('name', flat=True)),
                    'themes': set(existing_game.themes.values_list('name', flat=True)),
                    'perspectives': set(existing_game.player_perspectives.values_list('name', flat=True)),
                    'game_modes': set(existing_game.game_modes.values_list('name', flat=True))
                }

            for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                model = self._get_model_for_criteria(criteria_type)
                found_items = []
                patterns_for_type = patterns[criteria_type]

                # Инициализируем список для этого типа критериев
                pattern_info[criteria_type] = []

                for name, pattern_list in patterns_for_type.items():
                    if exclude_existing:
                        existing_names_lower = {n.lower() for n in existing_items.get(criteria_type, set())}
                        if name.lower() in existing_names_lower:
                            continue

                    for pattern in pattern_list:
                        if pattern.search(text_lower):
                            try:
                                obj = model.objects.filter(name__iexact=name).first()
                                if obj and obj not in found_items:
                                    found_items.append(obj)

                                    # Всегда собираем информацию о первом совпадении
                                    match = pattern.search(text_lower)
                                    if match:
                                        start_pos = match.start()
                                        end_pos = match.end()
                                        pattern_info[criteria_type].append({
                                            'name': name,
                                            'status': 'found',
                                            'pattern': pattern.pattern,
                                            'matched_text': text[start_pos:end_pos],
                                            'position': start_pos,
                                            'matched_word': text_lower[start_pos:end_pos],
                                            'context': self._get_context(text, start_pos, end_pos)
                                        })
                                    break
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
                'pattern_info': pattern_info,  # Всегда возвращаем словарь pattern_info
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

        # Отладка только если включен debug
        if self.debug:
            import sys
            sys.stderr.write(f"\n=== ОТЛАДКА analyze_comprehensive ===\n")
            sys.stderr.write(f"Игра ID: {existing_game.id if existing_game else 'unknown'}\n")
            sys.stderr.write(f"Длина текста: {len(text)}\n")
            sys.stderr.write(f"detailed_patterns: {detailed_patterns}\n")
            sys.stderr.write(f"exclude_existing: {exclude_existing}\n")
            sys.stderr.flush()

        # БЫСТРЫЙ анализ ключевых слов через Trie с поддержкой exclude_existing
        keywords_results, keywords_patterns = self._analyze_keywords_fast(
            text=text,
            existing_game=existing_game,
            collect_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        if self.debug:
            import sys
            sys.stderr.write(f"keywords_results: {keywords_results}\n")
            sys.stderr.write(f"keywords_patterns: {len(keywords_patterns.get('keywords', []))} паттернов\n")
            sys.stderr.flush()

        # Анализ критериев
        patterns = self._get_patterns()
        text_lower = text.lower()

        results = {}
        pattern_info = {}

        existing_items = {}
        if existing_game and exclude_existing:
            existing_items = {
                'genres': set(existing_game.genres.values_list('name', flat=True)),
                'themes': set(existing_game.themes.values_list('name', flat=True)),
                'perspectives': set(existing_game.player_perspectives.values_list('name', flat=True)),
                'game_modes': set(existing_game.game_modes.values_list('name', flat=True))
            }

        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
            model = self._get_model_for_criteria(criteria_type)
            found_items = []
            patterns_for_type = patterns[criteria_type]

            if detailed_patterns:
                pattern_info[criteria_type] = []

            for name, pattern_list in patterns_for_type.items():
                # Проверяем, существует ли уже этот критерий у игры
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

                # Флаг, что этот элемент уже найден
                element_found = False

                for pattern in pattern_list:
                    if element_found:
                        break

                    if pattern.search(text_lower):
                        try:
                            obj = model.objects.filter(name__iexact=name).first()
                            if obj and obj not in found_items:
                                found_items.append(obj)
                                element_found = True  # Помечаем, что элемент найден

                                if detailed_patterns:
                                    # Находим ПЕРВОЕ совпадение для этого паттерна
                                    match = pattern.search(text_lower)
                                    if match:
                                        start_pos = match.start()
                                        end_pos = match.end()

                                        # Добавляем информацию о строке (приблизительно)
                                        line_number = text[:start_pos].count('\n') + 1

                                        # Получаем контекст с найденным словом
                                        context = self._get_context(text, start_pos, end_pos)

                                        # Получаем сниппет для отладки
                                        snippet_start = max(0, start_pos - 30)
                                        snippet_end = min(len(text), end_pos + 30)
                                        debug_snippet = text[snippet_start:snippet_end]
                                        if snippet_start > 0:
                                            debug_snippet = '...' + debug_snippet
                                        if snippet_end < len(text):
                                            debug_snippet = debug_snippet + '...'

                                        pattern_info[criteria_type].append({
                                            'name': name,
                                            'status': 'found',
                                            'pattern': pattern.pattern,
                                            'matched_text': text[start_pos:end_pos],
                                            'position': start_pos,
                                            'line': line_number,
                                            'matched_word': text_lower[start_pos:end_pos],
                                            'context': context,
                                            'debug_text_snippet': debug_snippet
                                        })
                                # Выходим из циклов, так как элемент уже найден
                                break
                        except Exception:
                            pass
                # Если элемент уже найден, переходим к следующему имени
                if element_found:
                    continue

            if found_items:
                results[criteria_type] = {
                    'count': len(found_items),
                    'items': [{'id': i.id, 'name': i.name} for i in found_items]
                }

        if keywords_results.get('keywords'):
            results['keywords'] = {
                'count': len(keywords_results['keywords']),
                'items': [{'id': k.id, 'name': k.name} for k in keywords_results['keywords']]
            }

        if detailed_patterns and keywords_patterns.get('keywords'):
            pattern_info['keywords'] = keywords_patterns['keywords']

        total_found = sum(len(results.get(key, {}).get('items', []))
                          for key in ['genres', 'themes', 'perspectives', 'game_modes', 'keywords'])
        total_matches = sum(len(pattern_info.get(key, [])) for key in pattern_info)

        summary = {
            'found_count': total_found,
            'has_results': total_found > 0,
            'mode': 'comprehensive',
            'total_matches': total_matches,
            'exclude_existing': exclude_existing
        }

        processing_time = time.time() - start_time

        if self.verbose:
            print(f"⚡ Комплексный анализ завершен за {processing_time:.2f}s")
            print(f"📊 Найдено элементов: {total_found}, совпадений: {total_matches}")
            print(f"📊 Pattern info keywords: {len(pattern_info.get('keywords', []))}")
            if exclude_existing:
                print(f"🚫 Режим: исключать существующие критерии")

        return {
            'success': True,
            'results': results,
            'summary': summary,
            'pattern_info': pattern_info,
            'processing_time': processing_time,
            'has_results': total_found > 0,
            'total_matches': total_matches,
            'exclude_existing': exclude_existing
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

    def _get_context(self, text: str, start: int, end: int, context_length: int = 30) -> str:
        """
        Получает контекст вокруг найденного совпадения
        """
        # Берем контекст вокруг найденной позиции
        context_start = max(0, start - context_length)
        context_end = min(len(text), end + context_length)

        # Получаем контекст
        context = text[context_start:context_end]

        # Добавляем многоточия если нужно
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