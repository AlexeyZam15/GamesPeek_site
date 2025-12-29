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


class GameAnalyzerAPI:
    """Главный API для анализа игр - только анализ текста"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.text_analyzer = TextAnalyzer(verbose=verbose)

    def analyze_game_text_comprehensive(
            self,
            text: str,
            game_id: Optional[int] = None,
            existing_game=None,
            detailed_patterns: bool = True,  # Всегда собираем подробную информацию
            exclude_existing: bool = False  # По умолчанию показываем все
    ) -> Dict[str, Any]:
        """
        Комплексный анализ текста с поиском ВСЕХ вхождений элементов
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

        print(f"=== GameAnalyzerAPI.analyze_game_text_comprehensive: Starting comprehensive analysis")
        print(f"=== Game ID: {game_id}")
        print(f"=== Text length: {len(text)}")

        # Используем обновленный анализатор с поддержкой всех вхождений
        analysis_result = self.text_analyzer.analyze_comprehensive(
            text=text,
            existing_game=existing_game,
            detailed_patterns=True,  # Всегда собираем подробную информацию
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
            'total_matches': analysis_result.get('total_matches', 0)
        }

        if game_id:
            response['game_id'] = game_id

        print(f"=== Comprehensive analysis completed. Success: {response['success']}")
        print(f"=== Has results: {response['has_results']}")
        print(f"=== Total matches: {response.get('total_matches', 0)}")

        return response

    def analyze_game_text_combined(
            self,
            text: str,
            game_id: Optional[int] = None,
            existing_game=None,
            detailed_patterns: bool = False,
            exclude_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Анализирует текст игры в комбинированном режиме (все критерии + ключевые слова)
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

        print(f"=== GameAnalyzerAPI.analyze_game_text_combined: Starting combined analysis")
        print(f"=== Game ID: {game_id}")
        print(f"=== Exclude existing: {exclude_existing}")
        print(f"=== Text length: {len(text)}")

        # Анализируем критерии
        criteria_result = self.text_analyzer.analyze(
            text=text,
            analyze_keywords=False,
            existing_game=existing_game,
            detailed_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        # Анализируем ключевые слова
        keywords_result = self.text_analyzer.analyze(
            text=text,
            analyze_keywords=True,
            existing_game=existing_game,
            detailed_patterns=detailed_patterns,
            exclude_existing=exclude_existing
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
                'mode': 'combined'
            },
            'has_results': total_found > 0,
            'exclude_existing': exclude_existing
        }

        # Добавляем информацию о паттернах если нужно
        if detailed_patterns and combined_pattern_info:
            response['pattern_info'] = combined_pattern_info

        # Добавляем ID игры если передан
        if game_id:
            response['game_id'] = game_id

        print(f"=== Combined analysis completed. Success: {response['success']}")
        print(f"=== Has results: {response['has_results']}")
        print(f"=== Found count: {total_found}")

        return response

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
        Анализирует текст игры

        Args:
            text: Текст для анализа (уже подготовленный командой)
            game_id: ID игры (опционально)
            analyze_keywords: Анализировать ключевые слова
            existing_game: Существующая игра для проверки критериев
            detailed_patterns: Подробная информация о паттернах
            exclude_existing: Исключать уже существующие элементы

        Returns:
            Результаты анализа
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

        print(f"=== GameAnalyzerAPI.analyze_game_text: Starting analysis")
        print(f"=== Game ID: {game_id}")
        print(f"=== Analyze keywords: {analyze_keywords}")
        print(f"=== Exclude existing: {exclude_existing}")
        print(f"=== Text length: {len(text)}")

        # Анализируем текст
        analysis_result = self.text_analyzer.analyze(
            text=text,
            analyze_keywords=analyze_keywords,
            existing_game=existing_game,
            detailed_patterns=detailed_patterns,
            exclude_existing=exclude_existing
        )

        response = {
            'success': analysis_result.get('success', False),
            'error': analysis_result.get('error'),
            'processing_time': time.time() - start_time,
            'text_length': len(text),
            'analysis_mode': 'keywords' if analyze_keywords else 'criteria',
            'results': analysis_result.get('results', {}),
            'summary': analysis_result.get('summary', {}),
            'has_results': analysis_result.get('has_results', False),
            'exclude_existing': exclude_existing
        }

        # Добавляем информацию о паттернах если нужно
        if detailed_patterns and analysis_result.get('pattern_info'):
            response['pattern_info'] = analysis_result['pattern_info']

        # Добавляем ID игры если передан
        if game_id:
            response['game_id'] = game_id

        print(f"=== Analysis completed. Success: {response['success']}")
        print(f"=== Has results: {response['has_results']}")
        print(f"=== Found count: {response['summary'].get('found_count', 0)}")

        return response

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

    def clear_analysis_cache(self):
        """Очищает кеш анализатора"""
        self.text_analyzer.clear_cache()