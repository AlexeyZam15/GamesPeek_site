# games/analyze/batch_analyzer.py
"""
Пакетный анализатор игр
"""

import time
from typing import Dict, Any, List, Optional

from games.models import Game
from .text_analyzer import TextAnalyzer
from .utils import get_game_text, update_game_criteria


class BatchAnalyzer:
    """Пакетный анализатор для обработки множества игр"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.text_analyzer = TextAnalyzer(verbose=verbose)

    def analyze_games(
            self,
            game_ids: Optional[List[int]] = None,
            analyze_keywords: bool = False,
            update_database: bool = False,
            text_source: str = 'default',
            limit: Optional[int] = None,
            offset: int = 0
    ) -> Dict[str, Any]:
        """
        Анализирует несколько игр

        Args:
            game_ids: Список ID игр (None = все игры)
            analyze_keywords: Анализировать ключевые слова
            update_database: Обновить данные в базе
            text_source: Источник текста
            limit: Максимальное количество игр
            offset: Пропустить первые N игр

        Returns:
            Статистика и результаты
        """
        start_time = time.time()

        # Получаем игры
        if game_ids:
            games = Game.objects.filter(id__in=game_ids)
        else:
            games = Game.objects.all().order_by('id')

        total_games = games.count()

        # Применяем лимит и offset
        if offset:
            games = games[offset:]
        if limit:
            games = games[:limit]

        analyzed_games = games.count()

        # Статистика
        stats = {
            'total_games_in_database': total_games,
            'games_selected_for_analysis': analyzed_games,
            'games_with_text': 0,
            'games_with_results': 0,
            'total_criteria_found': 0,
            'games_updated': 0,
            'errors': 0,
            'detailed_results': []
        }

        # Анализируем каждую игру
        for index, game in enumerate(games, 1):
            try:
                game_result = self._analyze_single_game_in_batch(
                    game=game,
                    index=index,
                    analyze_keywords=analyze_keywords,
                    update_database=update_database,
                    text_source=text_source
                )

                # Обновляем статистику
                if game_result['has_text']:
                    stats['games_with_text'] += 1

                if game_result['has_results']:
                    stats['games_with_results'] += 1
                    stats['total_criteria_found'] += game_result['found_count']

                if game_result.get('updated', False):
                    stats['games_updated'] += 1

                stats['detailed_results'].append(game_result)

                # Логирование прогресса
                if self.verbose and index % 100 == 0:
                    print(f"Обработано {index}/{analyzed_games} игр")

            except Exception as e:
                stats['errors'] += 1
                if self.verbose:
                    print(f"Ошибка при анализе игры {game.id} ({game.name}): {e}")
                continue

        # Финальная статистика
        stats['execution_time'] = time.time() - start_time

        return {
            'success': True,
            'statistics': stats,
            'timestamp': start_time
        }

    def _analyze_single_game_in_batch(
            self,
            game: Game,
            index: int,
            analyze_keywords: bool,
            update_database: bool,
            text_source: str
    ) -> Dict[str, Any]:
        """Анализирует одну игру в рамках пакетной обработки"""
        # Получаем текст
        text = get_game_text(game, text_source)

        result = {
            'index': index,
            'game_id': game.id,
            'game_name': game.name,
            'has_text': bool(text),
            'text_length': len(text) if text else 0,
            'has_results': False,
            'found_count': 0,
            'updated': False
        }

        if not text:
            return result

        # Анализируем текст
        analysis_result = self.text_analyzer.analyze(
            text=text,
            analyze_keywords=analyze_keywords
        )

        result['has_results'] = analysis_result['has_results']
        result['found_count'] = analysis_result['summary'].get('total_found', 0) if analyze_keywords else \
        analysis_result['summary'].get('found_count', 0)

        # Обновляем базу если нужно
        if update_database and analysis_result['has_results']:
            updated = update_game_criteria(
                game=game,
                results=analysis_result['results'],
                is_keywords=analyze_keywords
            )
            result['updated'] = updated

        return result