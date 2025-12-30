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


# games/analyze/text_analyzer.py
class TextAnalyzer:
    """Анализатор текста для поиска критериев и ключевых слов"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self._patterns = None
        self._keywords_cache = None
        self._cache_stats = {'hits': 0, 'misses': 0}

        # Автоматически проверяем и синхронизируем паттерны с базой данных
        # Только если паттерны изменились
        self._ensure_all_patterns_in_db_if_needed()


    def _ensure_all_patterns_in_db_if_needed(self):
        """Гарантирует, что все элементы из паттернов есть в базе данных, только если паттерны изменились"""
        try:
            if self.verbose:
                print("=== Начало проверки изменений паттернов ===")

            # Проверяем, изменились ли паттерны
            from .pattern_manager import PatternManager
            patterns_changed = PatternManager.check_patterns_changed()

            if not patterns_changed and self.verbose:
                print("ℹ️ Паттерны не изменились, пропускаем проверку")

            # Даже если паттерны не изменились, проверяем наличие всех элементов
            # (на случай, если база данных была очищена)
            if self.verbose:
                print("=== Проверка наличия всех элементов в базе данных ===")

            from .sync_patterns_to_db import PatternAutoSyncer
            syncer = PatternAutoSyncer(verbose=self.verbose)

            # Быстрая проверка наличия элементов
            missing_count = syncer.get_missing_items_count()

            if missing_count > 0:
                if self.verbose:
                    print(f"⚠️ Обнаружено {missing_count} отсутствующих элементов в базе данных")

                # Запускаем синхронизацию
                from .sync_patterns_to_db import ensure_patterns_in_db
                results = ensure_patterns_in_db(self.verbose)

                # Перезагружаем кеш паттернов
                self._patterns = None

                total_added = sum(stats['added'] for stats in results.values())
                if total_added > 0 and self.verbose:
                    print(f"✅ Автоматически создано {total_added} элементов в базе данных")
            elif self.verbose:
                print("ℹ️ Все элементы уже есть в базе данных")

            if self.verbose:
                print("=== Проверка паттернов завершена ===")

        except Exception as e:
            if self.verbose:
                print(f"❌ Ошибка при проверке паттернов: {e}")

    def _get_patterns(self) -> Dict:
        """Загружает паттерны с предварительной проверкой изменений"""
        if self._patterns is None:
            # Проверяем, изменились ли паттерны
            from .pattern_manager import PatternManager
            patterns_changed = PatternManager.check_patterns_changed()

            if patterns_changed and self.verbose:
                print("=== Паттерны изменились, загружаем заново ===")

            # Загружаем паттерны (с кэшированием внутри PatternManager)
            self._patterns = PatternManager.get_all_patterns()

            if self.verbose:
                print(f"=== Загружено паттернов: ===")
                for category, patterns in self._patterns.items():
                    print(f"   {category}: {len(patterns)} элементов")

        return self._patterns

    def _ensure_all_patterns_in_db(self):
        """Гарантирует, что все элементы из паттернов есть в базе данных"""
        try:
            if self.verbose:
                print("=== Начало автоматической проверки паттернов ===")

            # Проверяем, есть ли вообще паттерны
            if self._patterns is None:
                self._patterns = PatternManager.get_all_patterns()

            # Быстрая проверка - подсчитываем отсутствующие элементы
            syncer = PatternAutoSyncer(verbose=self.verbose)
            missing_count = syncer.get_missing_items_count()

            if missing_count > 0:
                if self.verbose:
                    print(f"⚠️ Обнаружено {missing_count} отсутствующих элементов в базе данных")
                    print("=== Запуск автоматического создания отсутствующих элементов ===")

                # Синхронизируем все паттерны с базой данных
                results = ensure_patterns_in_db(self.verbose)

                # Перезагружаем кеш паттернов
                self._patterns = None

                total_added = sum(stats['added'] for stats in results.values())
                if total_added > 0 and self.verbose:
                    print(f"✅ Автоматически создано {total_added} элементов в базе данных")
            elif self.verbose:
                print("ℹ️ Все элементы уже есть в базе данных")

            if self.verbose:
                print("=== Проверка паттернов завершена ===")

        except Exception as e:
            if self.verbose:
                print(f"❌ Ошибка при проверке паттернов: {e}")

    def _find_criteria_comprehensive(
            self,
            text: str,
            text_lower: str,
            patterns: Dict,
            model,
            existing_names: Set[str],
            collect_patterns: bool = True
    ) -> Tuple[List, List]:
        """Ищет критерии по паттернам, находит ВСЕ вхождения в тексте, автоматически создает отсутствующие"""
        found_items = []  # Уникальные объекты
        pattern_matches = []  # Все совпадения
        already_added_ids = set()  # Для отслеживания уже добавленных объектов

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
                    if self.verbose:
                        print(f"✅ Автоматически создан отсутствующий элемент: {name} ({model.__name__})")
                else:
                    # Не удалось создать элемент, пропускаем
                    continue

            # Флаг, чтобы добавить объект только один раз
            object_added = False

            # Проверяем все паттерны для этого критерия
            for pattern in pattern_list:
                # Находим ВСЕ совпадения с этим паттерном
                for match in pattern.finditer(text_lower):
                    if not object_added:
                        # Получаем объект из базы (только один раз для этого критерия)
                        obj = all_existing_items.get(name.lower())
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

    def _create_missing_criteria(self, name: str, model) -> Any:
        """
        Создает отсутствующий элемент критерия в базе данных
        Вызывается автоматически при обнаружении отсутствующего элемента

        Args:
            name: Название элемента
            model: Модель Django (Genre, Theme, и т.д.)

        Returns:
            Созданный объект или None
        """
        try:
            # Проверяем, существует ли уже
            existing = model.objects.filter(name__iexact=name).first()
            if existing:
                return existing

            # Создаем элемент в зависимости от модели
            from django.db.models import Max

            if model.__name__ == 'Genre':
                max_igdb_id = model.objects.aggregate(Max('igdb_id'))['igdb_id__max'] or 1000000
                new_igdb_id = max_igdb_id + 1

                obj = model.objects.create(
                    name=name,
                    igdb_id=new_igdb_id,
                    cached_usage_count=0
                )

            elif model.__name__ == 'Theme':
                # Для тем только имя
                obj = model.objects.create(name=name)

            elif model.__name__ == 'PlayerPerspective':
                max_igdb_id = model.objects.aggregate(Max('igdb_id'))['igdb_id__max'] or 1000000
                new_igdb_id = max_igdb_id + 1

                obj = model.objects.create(
                    name=name,
                    igdb_id=new_igdb_id,
                    cached_usage_count=0
                )

            elif model.__name__ == 'GameMode':
                max_igdb_id = model.objects.aggregate(Max('igdb_id'))['igdb_id__max'] or 1000000
                new_igdb_id = max_igdb_id + 1

                obj = model.objects.create(
                    name=name,
                    igdb_id=new_igdb_id,
                    cached_usage_count=0
                )
            else:
                # Для других моделей просто создаем с именем
                obj = model.objects.create(name=name)

            if self.verbose:
                print(f"✅ Автоматически создан {model.__name__}: '{name}'")

            return obj

        except Exception as e:
            if self.verbose:
                print(f"❌ Ошибка при создании элемента '{name}' ({model.__name__}): {e}")
            return None

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

    # Все остальные методы остаются без изменений
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

        # Гарантируем, что все паттерны есть в базе данных перед анализом
        self._ensure_all_patterns_in_db()

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

    def get_cache_stats(self) -> Dict[str, Any]:
        """Статистика кеша"""
        return {
            'patterns_loaded': self._patterns is not None,
            'keywords_cached': self._keywords_cache is not None,
            'cache_stats': self._cache_stats.copy()
        }

    def clear_cache(self):
        """Очищает кеш"""
        self._patterns = None
        self._keywords_cache = None
        self._cache_stats = {'hits': 0, 'misses': 0}
