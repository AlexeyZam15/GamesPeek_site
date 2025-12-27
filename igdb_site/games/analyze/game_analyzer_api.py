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

    def analyze_game_text(
            self,
            text: str,
            game_id: Optional[int] = None,
            analyze_keywords: bool = False,
            existing_game=None,
            detailed_patterns: bool = False
    ) -> Dict[str, Any]:
        """
        Анализирует текст игры

        Args:
            text: Текст для анализа (уже подготовленный командой)
            game_id: ID игры (опционально)
            analyze_keywords: Анализировать ключевые слова
            existing_game: Существующая игра для проверки критериев
            detailed_patterns: Подробная информация о паттернах

        Returns:
            Результаты анализа
        """
        start_time = time.time()

        if not text:
            return {
                'success': False,
                'error': 'Пустой текст для анализа',
                'game_id': game_id,
                'processing_time': time.time() - start_time
            }

        # Анализируем текст
        analysis_result = self.text_analyzer.analyze(
            text=text,
            analyze_keywords=analyze_keywords,
            existing_game=existing_game,
            detailed_patterns=detailed_patterns
        )

        response = {
            'success': True,
            'processing_time': time.time() - start_time,
            'text_length': len(text),
            'analysis_mode': 'keywords' if analyze_keywords else 'criteria',
            'results': analysis_result['results'],
            'summary': analysis_result['summary'],
            'has_results': analysis_result['has_results']
        }

        # Добавляем информацию о паттернах если нужно
        if detailed_patterns and analysis_result.get('pattern_info'):
            response['pattern_info'] = analysis_result['pattern_info']

        # Добавляем ID игры если передан
        if game_id:
            response['game_id'] = game_id

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