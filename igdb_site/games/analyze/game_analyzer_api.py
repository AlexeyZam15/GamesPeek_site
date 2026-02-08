# games/analyze/game_analyzer_api.py
"""
Главный API класс для анализа игр - УПРОЩЕННАЯ ВЕРСИЯ
Только анализ, без логики подготовки текста
"""

import time
from typing import Dict, Any, List, Optional

from games.models import Game
from .text_analyzer import TextAnalyzer
from .utils import update_game_criteria
from .sync_patterns_to_db import ensure_patterns_in_db
from .range_cache import RangeCacheManager
from django.utils import timezone


class GameAnalyzerAPI:
    """Главный API для анализа игр с оптимизированными ключевыми словами"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.text_analyzer = TextAnalyzer(verbose=verbose)
        self.force_restart = False

        if verbose:
            print("=== Инициализация GameAnalyzerAPI ===")
            print("=== Проверяем наличие паттернов в базе данных ===")

        ensure_patterns_in_db(verbose=verbose)

        # Предзагрузка Trie при инициализации
        if not verbose:
            # В фоновом режиме предзагружаем Trie
            self._preload_trie_async()

    def mark_new_keywords_added(self):
        """Вызывать при добавлении новых ключевых слов - очищает кэш Trie"""
        # Очищаем кэш Trie
        from .keyword_trie import KeywordTrieManager
        KeywordTrieManager().clear_cache()

        # Также помечаем критерии как новые
        self.mark_new_criteria_added('keywords')

        if self.verbose:
            print("⚠️ Кэш Trie ключевых слов очищен (добавлены новые ключевые слова)")

    def _preload_trie_async(self):
        """Асинхронная предзагрузка Trie для быстрого старта"""
        import threading

        def load_trie():
            try:
                self.text_analyzer._ensure_trie_loaded()
            except Exception:
                pass

        # Запускаем в фоновом потоке
        thread = threading.Thread(target=load_trie)
        thread.daemon = True
        thread.start()

    def analyze_game_text(
            self,
            text: str,
            game_id: Optional[int] = None,
            analyze_keywords: bool = False,
            existing_game=None,
            detailed_patterns: bool = False,
            exclude_existing: bool = False
    ) -> Dict[str, Any]:
        """
        ОПТИМИЗИРОВАННЫЙ: Анализирует текст игры с поддержкой кэша и быстрыми ключевыми словами
        """
        start_time = time.time()

        if not text:
            return {
                'success': False,
                'error': 'Пустой текст для анализа',
                'game_id': game_id,
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        # ПРОВЕРКА КЭША
        should_use_cache = (
                game_id and
                not self.verbose and
                not exclude_existing
        )

        if should_use_cache and RangeCacheManager.is_game_checked(game_id):
            return {
                'success': True,
                'error': None,
                'processing_time': 0.001,
                'text_length': len(text),
                'analysis_mode': 'keywords' if analyze_keywords else 'criteria',
                'results': {},
                'summary': {'found_count': 0, 'has_results': False, 'mode': 'cached'},
                'has_results': False,
                'exclude_existing': exclude_existing,
                'cached': True,
                'game_id': game_id,
            }

        # ВЫБОР МЕТОДА АНАЛИЗА
        if analyze_keywords:
            # Используем оптимизированный метод анализа ключевых слов
            # text_analyzer уже использует Trie внутри analyze() метода
            analysis_result = self.text_analyzer.analyze(
                text=text,
                analyze_keywords=True,
                existing_game=existing_game,
                detailed_patterns=detailed_patterns,
                exclude_existing=exclude_existing
            )
        else:
            # Анализ критериев
            analysis_result = self.text_analyzer.analyze(
                text=text,
                analyze_keywords=False,
                existing_game=existing_game,
                detailed_patterns=detailed_patterns,
                exclude_existing=exclude_existing
            )

        processing_time = time.time() - start_time

        # Формируем ответ
        response = {
            'success': analysis_result.get('success', True),
            'error': analysis_result.get('error'),
            'processing_time': processing_time,
            'text_length': len(text),
            'analysis_mode': 'keywords' if analyze_keywords else 'criteria',
            'results': analysis_result.get('results', {}),
            'summary': analysis_result.get('summary', {
                'found_count': 0,
                'has_results': False
            }),
            'has_results': analysis_result.get('has_results', False),
            'exclude_existing': exclude_existing,
            'cached': False
        }

        if detailed_patterns and 'pattern_info' in analysis_result:
            response['pattern_info'] = analysis_result['pattern_info']

        # ОБНОВЛЕНИЕ КЭША
        if response['success'] and game_id and not self.verbose and not exclude_existing:
            RangeCacheManager.update_game_range(game_id, game_id)
            response['cached'] = True

        if game_id:
            response['game_id'] = game_id

        return response

    def analyze_game_text_comprehensive(
            self,
            text: str,
            game_id: Optional[int] = None,
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
                'error': 'Пустой текст для анализа',
                'game_id': game_id,
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        # ПРОВЕРКА КЭША
        if game_id and not self.verbose:
            if RangeCacheManager.is_game_checked(game_id):
                return {
                    'success': True,
                    'error': None,
                    'processing_time': 0.001,
                    'text_length': len(text),
                    'analysis_mode': 'comprehensive',
                    'results': {},
                    'summary': {
                        'found_count': 0,
                        'has_results': False,
                        'mode': 'cached',
                        'total_matches': 0
                    },
                    'pattern_info': {},
                    'has_results': False,
                    'total_matches': 0,
                    'cached': True,
                    'game_id': game_id,
                    'message': f'Игра {game_id} уже проверена (используется кэш)'
                }

        # Используем оптимизированный комплексный анализ
        analysis_result = self.text_analyzer.analyze_comprehensive(
            text=text,
            existing_game=existing_game,
            detailed_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        response = {
            'success': analysis_result.get('success', False),
            'error': analysis_result.get('error'),
            'processing_time': time.time() - start_time,
            'text_length': len(text),
            'analysis_mode': 'comprehensive',
            'results': analysis_result.get('results', {}),
            'summary': analysis_result.get('summary', {}),
            'pattern_info': analysis_result.get('pattern_info', {}),
            'has_results': analysis_result.get('has_results', False),
            'total_matches': analysis_result.get('total_matches', 0),
            'cached': False
        }

        # ДОБАВЛЯЕМ КЭШИРОВАНИЕ
        if response['success'] and game_id and not self.verbose:
            RangeCacheManager.update_game_range(game_id, game_id)

        if game_id:
            response['game_id'] = game_id

        return response

    def clear_all_cache(self):
        """Очищает весь кэш анализатора и диапазонов"""
        # Очищаем кэш анализатора текста
        self.text_analyzer.clear_cache()

        # Очищаем кэш диапазонов
        RangeCacheManager.clear_all_cache()

        if self.verbose:
            print("✅ Весь кэш анализатора очищен")

    def force_analyze_game_text(
            self,
            text: str,
            game_id: Optional[int] = None,
            analyze_keywords: bool = False,
            existing_game=None,
            detailed_patterns: bool = False,
            exclude_existing: bool = True  # ← ИЗМЕНЕНИЕ: по умолчанию True
    ) -> Dict[str, Any]:
        """
        Принудительный анализ текста игры (игнорирует кэш)
        """
        start_time = time.time()

        if not text:
            return {
                'success': False,
                'error': 'Пустой текст для анализа',
                'game_id': game_id,
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        # ВРЕМЕННО отключаем verbose во время анализа
        original_verbose = self.verbose
        self.verbose = False

        try:
            if analyze_keywords:
                # Только если явно запрошен анализ ключевых слов
                analysis_result = self.text_analyzer.analyze_comprehensive(
                    text=text,
                    existing_game=existing_game,
                    detailed_patterns=detailed_patterns,
                    exclude_existing=exclude_existing
                )
                # Фильтруем только ключевые слова
                if 'keywords' in analysis_result.get('results', {}):
                    analysis_result['results'] = {'keywords': analysis_result['results']['keywords']}
            else:
                # ИСПРАВЛЕНИЕ: Используем быстрый анализ ТОЛЬКО критериев, без ключевых слов
                analysis_result = self._analyze_criteria_only(
                    text=text,
                    existing_game=existing_game,
                    detailed_patterns=detailed_patterns,
                    exclude_existing=exclude_existing  # ← Передаем exclude_existing
                )

            processing_time = time.time() - start_time

            # Формируем стандартизированный ответ
            response = {
                'success': analysis_result.get('success', True),
                'error': analysis_result.get('error'),
                'processing_time': processing_time,
                'text_length': len(text),
                'analysis_mode': 'keywords' if analyze_keywords else 'criteria',
                'results': analysis_result.get('results', {}),
                'summary': analysis_result.get('summary', {
                    'found_count': 0,
                    'has_results': False
                }),
                'has_results': analysis_result.get('has_results', False),
                'exclude_existing': exclude_existing,
                'cached': False,
                'force_analysis': True,
                'bypass_cache': True
            }

            # Добавляем информацию о паттернах если нужно
            if detailed_patterns and 'pattern_info' in analysis_result:
                response['pattern_info'] = analysis_result['pattern_info']

            # Добавляем ID игры
            if game_id:
                response['game_id'] = game_id

            return response

        finally:
            # Восстанавливаем original_verbose
            self.verbose = original_verbose

    def _analyze_criteria_only(
            self,
            text: str,
            existing_game=None,
            detailed_patterns: bool = False,
            exclude_existing: bool = True  # ← ИЗМЕНЕНИЕ: по умолчанию True
    ) -> Dict[str, Any]:
        """
        Быстрый анализ ТОЛЬКО критериев (жанры, темы, перспективы, режимы)
        БЕЗ анализа ключевых слов
        """
        start_time = time.time()

        # ИСПРАВЛЕНИЕ: Ограничиваем текст для скорости
        if len(text) > 5000:
            text = text[:5000]

        if not text:
            return {
                'success': False,
                'error': 'Empty text',
                'results': {},
                'summary': {'found_count': 0, 'has_results': False},
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        text_lower = text.lower()

        # Получаем паттерны
        from .pattern_manager import PatternManager
        patterns = PatternManager.get_all_patterns()

        # ИСПРАВЛЕНИЕ: По умолчанию исключаем существующие критерии
        # Если передана игра и exclude_existing=True, получаем существующие критерии
        existing_items = {}
        if existing_game and exclude_existing:
            existing_items = {
                'genres': set(existing_game.genres.values_list('name', flat=True)),
                'themes': set(existing_game.themes.values_list('name', flat=True)),
                'perspectives': set(existing_game.player_perspectives.values_list('name', flat=True)),
                'game_modes': set(existing_game.game_modes.values_list('name', flat=True))
            }

        # Анализируем каждый тип критериев
        results = {}
        pattern_info = {}
        total_found = 0

        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
            model = self._get_model_for_criteria(criteria_type)
            found_items = []
            patterns_for_type = patterns[criteria_type]

            for name, pattern_list in patterns_for_type.items():
                # ИСПРАВЛЕНИЕ: Пропускаем если уже существует у игры
                if exclude_existing:
                    # Проверяем в разных регистрах
                    existing_names_lower = {n.lower() for n in existing_items.get(criteria_type, set())}
                    if name.lower() in existing_names_lower:
                        continue

                # Проверяем паттерны
                for pattern in pattern_list:
                    if pattern.search(text_lower):
                        # Нашли совпадение - получаем объект
                        try:
                            obj = model.objects.filter(name__iexact=name).first()
                            if obj and obj not in found_items:
                                found_items.append(obj)
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
            'has_results': total_found > 0,
            'processing_time': processing_time
        }

    def _get_model_for_criteria(self, criteria_type: str):
        """Возвращает модель для типа критерия"""
        from games.models import Genre, Theme, PlayerPerspective, GameMode

        models = {
            'genres': Genre,
            'themes': Theme,
            'perspectives': PlayerPerspective,
            'game_modes': GameMode
        }
        return models.get(criteria_type)

    def clear_analysis_cache(self):
        """Очищает кэш анализатора"""
        # Очищаем кэш анализатора текста
        self.text_analyzer.clear_cache()

        # Очищаем кэш диапазонов
        RangeCacheManager.clear_all_cache()

        if self.verbose:
            print("✅ Весь кэш анализатора очищен")

    def analyze_game_text_combined(
            self,
            text: str,
            game_id: Optional[int] = None,
            existing_game=None,
            detailed_patterns: bool = False,
            exclude_existing: bool = False  # ДОБАВЛЯЕМ ПАРАМЕТР
    ) -> Dict[str, Any]:
        """
        Анализирует текст игры в комбинированном режиме (все критерии + ключевые слова) С ПОДДЕРЖКОЙ КЭША
        Теперь поддерживает exclude_existing параметр
        """
        start_time = time.time()

        if not text:
            return {
                'success': False,
                'error': 'Пустой текст для анализа',
                'game_id': game_id,
                'processing_time': time.time() - start_time,
                'has_results': False
            }

        # ПРОВЕРКА КЭША: если игра уже проверена, возвращаем кэшированный результат
        # НО: если exclude_existing=True, игнорируем кэш и анализируем заново
        if game_id and not self.verbose and not exclude_existing:
            if RangeCacheManager.is_game_checked(game_id):
                if self.verbose:
                    print(f"ℹ️ Игра {game_id} уже проверена, возвращаем пустой результат из кэша")

                return {
                    'success': True,
                    'error': None,
                    'processing_time': 0.001,
                    'text_length': len(text),
                    'analysis_mode': 'combined',
                    'results': {},
                    'summary': {
                        'found_count': 0,
                        'has_results': False,
                        'mode': 'cached'
                    },
                    'has_results': False,
                    'exclude_existing': exclude_existing,
                    'cached': True,
                    'game_id': game_id,
                    'message': f'Игра {game_id} уже проверена (используется кэш)'
                }

        if self.verbose:
            print(f"=== GameAnalyzerAPI.analyze_game_text_combined: Starting combined analysis")
            print(f"=== Game ID: {game_id}")
            print(f"=== Exclude existing: {exclude_existing}")
            print(f"=== Text length: {len(text)}")

        # Анализируем критерии с поддержкой exclude_existing
        criteria_result = self.text_analyzer.analyze(
            text=text,
            analyze_keywords=False,
            existing_game=existing_game,
            detailed_patterns=detailed_patterns,
            exclude_existing=exclude_existing  # Передаем параметр
        )

        # Анализируем ключевые слова с поддержкой exclude_existing
        keywords_result = self.text_analyzer.analyze(
            text=text,
            analyze_keywords=True,
            existing_game=existing_game,
            detailed_patterns=detailed_patterns,
            exclude_existing=exclude_existing  # Передаем параметр
        )

        # Объединяем результаты
        combined_results = {}
        if criteria_result.get('success'):
            criteria_data = criteria_result.get('results', {})
            combined_results.update(criteria_data)

        if keywords_result.get('success'):
            keywords_data = keywords_result.get('results', {})
            if 'keywords' in keywords_data:
                combined_results['keywords'] = keywords_data['keywords']

        # Объединяем информацию о паттернах
        combined_pattern_info = {}
        if detailed_patterns:
            if criteria_result.get('pattern_info'):
                combined_pattern_info.update(criteria_result['pattern_info'])
            if keywords_result.get('pattern_info'):
                combined_pattern_info['keywords'] = keywords_result['pattern_info'].get('keywords', [])

        # Считаем общее количество найденных элементов
        total_found = 0
        for category in ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']:
            if category in combined_results:
                total_found += combined_results[category].get('count', 0)

        response = {
            'success': criteria_result.get('success', False) and keywords_result.get('success', False),
            'error': criteria_result.get('error') or keywords_result.get('error'),
            'processing_time': time.time() - start_time,
            'text_length': len(text),
            'analysis_mode': 'combined',
            'results': combined_results,
            'summary': {
                'found_count': total_found,
                'has_results': total_found > 0,
                'mode': 'combined',
                'exclude_existing': exclude_existing
            },
            'has_results': total_found > 0,
            'exclude_existing': exclude_existing,
            'cached': False
        }

        # Добавляем информацию о паттернах если нужно
        if detailed_patterns and combined_pattern_info:
            response['pattern_info'] = combined_pattern_info

        # ДОБАВЛЯЕМ КЭШИРОВАНИЕ: обновляем кэш только если не exclude_existing
        if response['success'] and game_id and not self.verbose and not exclude_existing:
            # Обновляем кэш для этой игры
            RangeCacheManager.update_game_range(game_id, game_id)
            if self.verbose:
                print(f"✅ Кэш обновлен для игры {game_id}")

        # Добавляем ID игры если передан
        if game_id:
            response['game_id'] = game_id

        if self.verbose:
            print(f"=== Combined analysis completed. Success: {response['success']}")
            print(f"=== Has results: {response['has_results']}")
            print(f"=== Found count: {total_found}")
            print(f"=== Exclude existing: {exclude_existing}")
            print(f"=== Cached: {response.get('cached', False)}")

        return response

    def mark_new_games_added(self):
        """Вызывать при добавлении новых игр - помечает все игры как непроверенные"""
        RangeCacheManager.mark_all_games_as_unchecked()

        if self.verbose:
            print("⚠️ Все игры помечены как непроверенные (добавлены новые игры)")

    def mark_new_criteria_added(self, category: str):
        """Вызывать при добавлении новых критериев - помечает категорию как непроверенную"""
        valid_categories = ['genres', 'themes', 'perspectives', 'game_modes', 'keywords']
        if category in valid_categories:
            RangeCacheManager.mark_criteria_as_new(category)
            if self.verbose:
                print(f"⚠️ Категория {category} помечена как непроверенная (добавлены новые критерии)")
        else:
            if self.verbose:
                print(f"❌ Неизвестная категория: {category}")

    def update_game_with_combined_results(
            self,
            game_id: int,
            results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Обновляет игру с результатами комбинированного анализа
        """
        try:
            game = Game.objects.get(id=game_id)

            updated_categories = []
            total_updated = 0

            # Обновляем каждую категорию
            for category, data in results.items():
                if category == 'keywords':
                    keywords = data.get('items', [])
                    if keywords:
                        keyword_ids = [k['id'] for k in keywords]
                        from games.models import Keyword
                        keyword_objects = Keyword.objects.filter(id__in=keyword_ids)
                        game.keywords.add(*keyword_objects)
                        updated_categories.append(f"{len(keyword_objects)} keywords")
                        total_updated += len(keyword_objects)

                elif category == 'genres':
                    genres = data.get('items', [])
                    if genres:
                        genre_ids = [g['id'] for g in genres]
                        from games.models import Genre
                        genre_objects = Genre.objects.filter(id__in=genre_ids)
                        game.genres.add(*genre_objects)
                        updated_categories.append(f"{len(genre_objects)} genres")
                        total_updated += len(genre_objects)

                elif category == 'themes':
                    themes = data.get('items', [])
                    if themes:
                        theme_ids = [t['id'] for t in themes]
                        from games.models import Theme
                        theme_objects = Theme.objects.filter(id__in=theme_ids)
                        game.themes.add(*theme_objects)
                        updated_categories.append(f"{len(theme_objects)} themes")
                        total_updated += len(theme_objects)

                elif category == 'perspectives':
                    perspectives = data.get('items', [])
                    if perspectives:
                        perspective_ids = [p['id'] for p in perspectives]
                        from games.models import PlayerPerspective
                        perspective_objects = PlayerPerspective.objects.filter(id__in=perspective_ids)
                        game.player_perspectives.add(*perspective_objects)
                        updated_categories.append(f"{len(perspective_objects)} perspectives")
                        total_updated += len(perspective_objects)

                elif category == 'game_modes':
                    game_modes = data.get('items', [])
                    if game_modes:
                        mode_ids = [m['id'] for m in game_modes]
                        from games.models import GameMode
                        mode_objects = GameMode.objects.filter(id__in=mode_ids)
                        game.game_modes.add(*mode_objects)
                        updated_categories.append(f"{len(mode_objects)} game modes")
                        total_updated += len(mode_objects)

            if total_updated > 0:
                # Обновляем кэшированные счетчики
                game.update_cached_counts(force=True)

                # Обновляем дату последнего анализа
                game.last_analyzed_date = timezone.now()
                game.save()

                return {
                    'success': True,
                    'game_id': game_id,
                    'game_name': game.name,
                    'updated': True,
                    'total_updated': total_updated,
                    'updated_categories': updated_categories,
                    'message': f'Successfully added {total_updated} elements to game'
                }
            else:
                return {
                    'success': True,
                    'game_id': game_id,
                    'game_name': game.name,
                    'updated': False,
                    'message': 'No new elements to add'
                }

        except Game.DoesNotExist:
            return {
                'success': False,
                'error': f'Игра с ID {game_id} не найдена'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'game_id': game_id
            }

    def update_game_with_results(
            self,
            game_id: int,
            results: Dict[str, Any],
            is_keywords: bool
    ) -> Dict[str, Any]:
        """
        Обновляет игру с результатами анализа

        Args:
            game_id: ID игры
            results: Результаты анализа
            is_keywords: Обновлять ключевые слова или критерии

        Returns:
            Результат обновления
        """
        try:
            game = Game.objects.get(id=game_id)

            updated = update_game_criteria(
                game=game,
                results=results,
                is_keywords=is_keywords
            )

            return {
                'success': True,
                'game_id': game_id,
                'game_name': game.name,
                'updated': updated,
                'current_criteria': self._get_current_criteria(game, is_keywords)
            }

        except Game.DoesNotExist:
            return {
                'success': False,
                'error': f'Игра с ID {game_id} не найдена'
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e),
                'game_id': game_id
            }

    def analyze_batch(
            self,
            games_data: List[Dict],
            analyze_keywords: bool = False,
            detailed_patterns: bool = False
    ) -> Dict[str, Any]:
        """
        Анализирует несколько игр

        Args:
            games_data: Список словарей с данными игр {'id': int, 'text': str, 'existing_game': Game}
            analyze_keywords: Анализировать ключевые слова
            detailed_patterns: Подробная информация о паттернах

        Returns:
            Результаты анализа
        """
        start_time = time.time()
        results = []
        stats = {
            'total': len(games_data),
            'processed': 0,
            'with_text': 0,
            'with_results': 0,
            'total_found': 0,
            'errors': 0
        }

        for game_data in games_data:
            try:
                text = game_data.get('text', '')

                if not text:
                    results.append({
                        'game_id': game_data['id'],
                        'success': False,
                        'error': 'Нет текста'
                    })
                    continue

                stats['with_text'] += 1

                # Анализируем
                analysis_result = self.analyze_game_text(
                    text=text,
                    game_id=game_data['id'],
                    analyze_keywords=analyze_keywords,
                    existing_game=game_data.get('existing_game'),
                    detailed_patterns=detailed_patterns
                )

                if analysis_result['success']:
                    stats['processed'] += 1

                    if analysis_result['has_results']:
                        stats['with_results'] += 1
                        stats['total_found'] += analysis_result['summary'].get('found_count', 0)

                results.append(analysis_result)

            except Exception as e:
                stats['errors'] += 1
                results.append({
                    'game_id': game_data.get('id'),
                    'success': False,
                    'error': str(e)
                })
                continue

        return {
            'success': True,
            'processing_time': time.time() - start_time,
            'statistics': stats,
            'results': results
        }

    def _get_current_criteria(self, game: Game, is_keywords: bool) -> Dict[str, List]:
        """Возвращает текущие критерии игры"""
        if is_keywords:
            return {
                'keywords': list(game.keywords.values('id', 'name'))
            }
        else:
            return {
                'genres': list(game.genres.values('id', 'name')),
                'themes': list(game.themes.values('id', 'name')),
                'player_perspectives': list(game.player_perspectives.values('id', 'name')),
                'game_modes': list(game.game_modes.values('id', 'name'))
            }
