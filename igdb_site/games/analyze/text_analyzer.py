# games/analyze/text_analyzer.py
"""
Анализатор текста - ядро логики поиска критериев и ключевых слов
"""

import re
import time
from typing import Dict, Any, List, Optional, Set, Tuple

from games.models import Genre, Theme, PlayerPerspective, GameMode, Keyword
from .pattern_manager import PatternManager
from .sync_patterns_to_db import ensure_patterns_in_db, PatternAutoSyncer
from .range_cache import RangeCacheManager


class TextAnalyzer:
    """Анализатор текста для поиска критериев и ключевых слов"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self._patterns = None
        self._keywords_cache = None

    def _should_check_criteria(self, category: str, element_name: str, element_id: int) -> bool:
        """
        Определяет, нужно ли проверять критерий.
        Возвращает False если критерий уже был проверен в этом диапазоне.
        """
        # Всегда проверяем, если это verbose режим
        if self.verbose:
            return True

        # Проверяем, не находится ли ID в уже проверенном диапазоне
        if RangeCacheManager.is_criteria_checked(category, element_id):
            if self.verbose:
                print(f"ℹ️ Критерий {element_name} (ID: {element_id}) уже проверен, пропускаем")
            return False

        return True

    def _find_criteria_comprehensive(
            self,
            text: str,
            text_lower: str,
            patterns: Dict,
            model,
            existing_names: Set[str],
            collect_patterns: bool = True
    ) -> Tuple[List, List]:
        """Ищет критерии по паттернам с учетом кэширования диапазонов"""
        found_items = []  # Уникальные объекты
        pattern_matches = []  # Все совпадения
        already_added_ids = set()  # Для отслеживания уже добавленных объектов

        # Получаем минимальный и максимальный ID среди найденных элементов
        min_id = float('inf')
        max_id = 0

        # Кешируем все существующие элементы из базы данных
        all_existing_items = {item.name.lower(): item for item in model.objects.all()}

        for name, pattern_list in patterns.items():
            # Пропускаем если уже существует у игры И нужно исключать
            if existing_names and name.lower() in existing_names:
                if collect_patterns:
                    pattern_matches.append({
                        'name': name,
                        'status': 'exists',
                        'reason': 'already_exists_in_game'
                    })
                continue

            # Проверяем, существует ли элемент в базе данных
            if name.lower() not in all_existing_items:
                # Элемент отсутствует в базе данных - автоматически создаем его
                created_object = self._create_missing_criteria(name, model)
                if created_object:
                    all_existing_items[name.lower()] = created_object
                    # Обновляем диапазоны для нового элемента
                    RangeCacheManager.update_criteria_range(
                        self._get_category_for_model(model),
                        created_object.id,
                        created_object.id
                    )
                    if self.verbose:
                        print(f"✅ Автоматически создан отсутствующий элемент: {name} ({model.__name__})")
                else:
                    # Не удалось создать элемент, пропускаем
                    continue

            # Получаем объект из базы
            obj = all_existing_items.get(name.lower())
            if not obj:
                continue

            # Проверяем, нужно ли анализировать этот критерий
            if not self._should_check_criteria(self._get_category_for_model(model), name, obj.id):
                continue

            # Обновляем min/max ID
            min_id = min(min_id, obj.id)
            max_id = max(max_id, obj.id)

            # Флаг, чтобы добавить объект только один раз
            object_added = False

            # Проверяем все паттерны для этого критерия
            for pattern in pattern_list:
                # Находим ВСЕ совпадения с этим паттерном
                for match in pattern.finditer(text_lower):
                    if not object_added:
                        if obj.id not in already_added_ids:
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

        # Обновляем диапазон проверенных критериев
        if min_id <= max_id and found_items:
            category = self._get_category_for_model(model)
            RangeCacheManager.update_criteria_range(category, min_id, max_id)

        return found_items, pattern_matches

    def _get_category_for_model(self, model) -> str:
        """Возвращает имя категории для модели"""
        model_name = model.__name__
        if model_name == 'Genre':
            return 'genres'
        elif model_name == 'Theme':
            return 'themes'
        elif model_name == 'PlayerPerspective':
            return 'perspectives'
        elif model_name == 'GameMode':
            return 'game_modes'
        elif model_name == 'Keyword':
            return 'keywords'
        else:
            return 'unknown'

    def _analyze_criteria_comprehensive(
            self,
            text: str,
            existing_game=None,
            collect_patterns: bool = True,
            exclude_existing: bool = False
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

        # Получаем минимальный и максимальный ID среди найденных ключевых слов
        min_id = float('inf')
        max_id = 0

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

            # Проверяем, нужно ли анализировать это ключевое слово
            if not self._should_check_criteria('keywords', keyword_name, keyword.id):
                continue

            # Обновляем min/max ID
            min_id = min(min_id, keyword.id)
            max_id = max(max_id, keyword.id)

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

        # Обновляем диапазон проверенных ключевых слов
        if min_id <= max_id and found_keywords:
            RangeCacheManager.update_criteria_range('keywords', min_id, max_id)

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
        с поддержкой обнаружения множественных критериев на одних словах
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

        if self.verbose:
            print(f"=== TextAnalyzer.analyze_comprehensive: Starting comprehensive analysis")
            print(f"=== Text length: {len(text)} characters")

        # Получаем паттерны
        patterns = self._get_patterns()
        text_lower = text.lower()

        # Анализируем критерии с поддержкой множественных совпадений
        criteria_results, criteria_patterns, overlapping_info = self._analyze_criteria_with_overlap(
            text=text,
            text_lower=text_lower,
            patterns=patterns,
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

        # Создаем сводку с информацией о пересечениях
        total_found = sum(
            len(criteria_results.get(key, [])) for key in ['genres', 'themes', 'perspectives', 'game_modes'])
        total_found += len(keywords_results.get('keywords', []))

        # Считаем общее количество пересечений
        total_overlaps = 0
        if detailed_patterns:
            # Анализируем пересечения между всеми категориями
            all_patterns = []
            for category in ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']:
                if category in pattern_info:
                    all_patterns.extend(pattern_info[category])

            # Находим пересечения
            overlapping_positions = self._find_overlapping_positions(all_patterns)
            total_overlaps = len(overlapping_positions)

        summary = {
            'found_count': total_found,
            'has_results': total_found > 0,
            'mode': 'comprehensive',
            'genres_found': len(criteria_results.get('genres', [])),
            'themes_found': len(criteria_results.get('themes', [])),
            'perspectives_found': len(criteria_results.get('perspectives', [])),
            'game_modes_found': len(criteria_results.get('game_modes', [])),
            'keywords_found': len(keywords_results.get('keywords', [])),
            'total_matches': total_matches,
            'total_overlaps': total_overlaps
        }

        processing_time = time.time() - start_time
        if self.verbose:
            print(f"=== Comprehensive analysis completed in {processing_time:.2f}s")
            print(f"=== Found: {total_found} elements")
            print(f"=== Total matches: {total_matches}")
            print(f"=== Overlaps found: {total_overlaps}")

        return {
            'success': True,
            'results': combined_results,
            'summary': summary,
            'pattern_info': pattern_info,
            'processing_time': processing_time,
            'has_results': total_found > 0,
            'total_matches': total_matches,
            'total_overlaps': total_overlaps
        }

    def _analyze_criteria_with_overlap(
            self,
            text: str,
            text_lower: str,
            patterns: Dict,
            existing_game=None,
            collect_patterns: bool = True,
            exclude_existing: bool = False
    ) -> Tuple[Dict, Dict, Dict]:
        """
        Анализирует критерии с поддержкой обнаружения пересечений
        """
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

        overlapping_info = {}

        # Существующие критерии игры
        existing_items = {}
        if existing_game and exclude_existing:
            existing_items = {
                'genres': set(existing_game.genres.values_list('name', flat=True)),
                'themes': set(existing_game.themes.values_list('name', flat=True)),
                'perspectives': set(existing_game.player_perspectives.values_list('name', flat=True)),
                'game_modes': set(existing_game.game_modes.values_list('name', flat=True))
            }

        # Собираем все совпадения для последующего анализа пересечений
        all_matches_by_category = {}

        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
            found_items, pattern_matches = self._find_criteria_comprehensive(
                text=text,
                text_lower=text_lower,
                patterns=patterns[criteria_type],
                model=self._get_model_for_criteria(criteria_type),
                existing_names=existing_items.get(criteria_type, set()) if exclude_existing else set(),
                collect_patterns=collect_patterns
            )

            results[criteria_type] = found_items
            pattern_info[criteria_type] = pattern_matches
            all_matches_by_category[criteria_type] = pattern_matches

        # Анализируем пересечения между категориями
        if collect_patterns:
            overlapping_info = self._analyze_cross_category_overlaps(all_matches_by_category)

        return results, pattern_info, overlapping_info

    def _find_overlapping_positions(self, pattern_matches: List[Dict]) -> List[Dict]:
        """
        Находит пересечения позиций в списке совпадений
        """
        if not pattern_matches:
            return []

        # Фильтруем только найденные элементы
        found_matches = [m for m in pattern_matches if m.get('status') == 'found']

        if not found_matches:
            return []

        # Создаем список всех позиций
        positions = []
        for match in found_matches:
            position = match.get('position', 0)
            match_length = len(match.get('matched_text', ''))
            positions.append({
                'start': position,
                'end': position + match_length,
                'match': match
            })

        # Сортируем по начальной позиции
        positions.sort(key=lambda x: x['start'])

        # Находим пересечения
        overlaps = []
        i = 0

        while i < len(positions):
            current = positions[i]
            overlapping_group = [current['match']]
            group_start = current['start']
            group_end = current['end']

            j = i + 1
            while j < len(positions):
                next_pos = positions[j]

                # Проверяем пересечение
                if (current['start'] <= next_pos['start'] < current['end'] or
                        current['start'] <= next_pos['end'] < current['end'] or
                        next_pos['start'] <= current['start'] < next_pos['end']):

                    overlapping_group.append(next_pos['match'])
                    group_start = min(group_start, next_pos['start'])
                    group_end = max(group_end, next_pos['end'])
                    j += 1
                else:
                    break

            if len(overlapping_group) > 1:
                overlaps.append({
                    'start': group_start,
                    'end': group_end,
                    'matches': overlapping_group,
                    'count': len(overlapping_group)
                })

            i = j

        return overlaps

    def _analyze_cross_category_overlaps(self, matches_by_category: Dict) -> Dict:
        """
        Анализирует пересечения между разными категориями
        """
        overlaps = {}

        # Собираем все совпадения из всех категорий
        all_matches = []
        for category, matches in matches_by_category.items():
            for match in matches:
                if match.get('status') == 'found':
                    all_matches.append({
                        **match,
                        'category': category
                    })

        # Находим пересечения
        overlapping_positions = self._find_overlapping_positions(all_matches)

        if overlapping_positions:
            overlaps['cross_category'] = overlapping_positions

        return overlaps

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

    def clear_cache(self):
        """Очищает кеш"""
        self._patterns = None
        self._keywords_cache = None

    def _get_patterns(self) -> Dict:
        """Загружает паттерны"""
        if self._patterns is None:
            self._patterns = PatternManager.get_all_patterns()
        return self._patterns

    def _create_missing_criteria(self, name: str, model) -> Any:
        """
        Создает отсутствующий элемент критерия в базе данных
        ИСПРАВЛЕНИЕ: Учитываем все поля модели
        """
        try:
            # Проверяем, существует ли уже
            existing = model.objects.filter(name__iexact=name).first()
            if existing:
                return existing

            # ИСПРАВЛЕНИЕ: Используем словарь полей модели
            from django.db.models import Max

            # Получаем поля модели
            model_fields = [field.name for field in model._meta.fields]

            # Базовые данные
            create_data = {'name': name}

            # Добавляем igdb_id если поле существует
            if 'igdb_id' in model_fields:
                try:
                    max_igdb_id = model.objects.aggregate(Max('igdb_id'))['igdb_id__max'] or 1000000
                    create_data['igdb_id'] = max_igdb_id + 1
                except Exception as e:
                    if self.verbose:
                        print(f"⚠️ Не удалось получить max igdb_id для {model.__name__}: {e}")
                    create_data['igdb_id'] = 999999  # Значение по умолчанию

            # Добавляем cached_usage_count если поле существует
            if 'cached_usage_count' in model_fields:
                create_data['cached_usage_count'] = 0

            # ИСПРАВЛЕНИЕ: Для тем также добавим slug если поле существует
            if 'slug' in model_fields and model.__name__ == 'Theme':
                from django.utils.text import slugify
                create_data['slug'] = slugify(name)

            # Создаем объект
            obj = model.objects.create(**create_data)

            if self.verbose:
                print(f"✅ Автоматически создан {model.__name__}: '{name}' с ID {obj.id}")

            return obj

        except Exception as e:
            if self.verbose:
                print(f"❌ Ошибка при создании элемента '{name}' ({model.__name__}): {e}")
                import traceback
                traceback.print_exc()
            return None