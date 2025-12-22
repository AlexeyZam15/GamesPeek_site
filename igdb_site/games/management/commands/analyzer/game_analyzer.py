# games/management/commands/analyzer/game_analyzer.py
from typing import Dict, List, Set, Tuple
from games.models import Game, Genre, Theme, PlayerPerspective, GameMode, Keyword
from .pattern_manager import PatternManager
from .criteria_finder import CriteriaFinder
from .keyword_finder import KeywordFinder


class GameAnalyzer:
    """Класс для анализа игр с использованием ВСЕХ паттернов сразу"""

    def __init__(self, command_instance=None):
        self.command = command_instance

        all_patterns = PatternManager.get_all_patterns()

        self.finders = {
            'genres': CriteriaFinder(Genre, all_patterns['genres']),
            'themes': CriteriaFinder(Theme, all_patterns['themes']),
            'perspectives': CriteriaFinder(PlayerPerspective, all_patterns['perspectives']),
            'game_modes': CriteriaFinder(GameMode, all_patterns['game_modes']),
        }

        self.keyword_finder = KeywordFinder()

    def analyze_all_patterns(self, text: str, game: Game = None,
                             ignore_existing: bool = False,
                             collect_patterns: bool = False,
                             keywords_mode: bool = False) -> Tuple[Dict[str, List], Dict[str, List[Dict]]]:
        """Анализирует текст используя ВСЕ паттерны сразу"""
        if not text:
            return self._empty_results(), {}

        results = {}
        all_pattern_info = {}

        if keywords_mode:
            try:
                existing_keywords = set()
                if game and not ignore_existing:
                    existing_keywords = set(game.keywords.all())

                keywords_results, keywords_pattern_info = self.keyword_finder.find_all_keywords(
                    text,
                    existing_objects=existing_keywords,
                    pattern_collection_mode=collect_patterns
                )

                results['keywords'] = keywords_results
                all_pattern_info['keywords'] = keywords_pattern_info

            except Exception as e:
                if self.command:
                    self.command.stdout.write(f"⚠️ Ошибка поиска ключевых слов: {e}")
                results['keywords'] = []
                all_pattern_info['keywords'] = []
        else:
            for criteria_type, finder in self.finders.items():
                try:
                    existing_objects = set()
                    if game and not ignore_existing:
                        existing_objects = self._get_existing_objects(game, criteria_type)

                    criteria_results, pattern_info = finder.find_all_patterns(
                        text,
                        existing_objects=existing_objects,
                        pattern_collection_mode=collect_patterns
                    )

                    results[criteria_type] = criteria_results
                    all_pattern_info[criteria_type] = pattern_info

                except Exception as e:
                    if self.command:
                        self.command.stdout.write(f"⚠️ Ошибка анализа {criteria_type}: {e}")
                    results[criteria_type] = []
                    all_pattern_info[criteria_type] = []

            results['keywords'] = []

        return results, all_pattern_info

    def _get_existing_objects(self, game: Game, criteria_type: str) -> Set:
        """Получает существующие объекты"""
        mapping = {
            'genres': game.genres.all(),
            'themes': game.themes.all(),
            'perspectives': game.player_perspectives.all(),
            'game_modes': game.game_modes.all(),
            'keywords': game.keywords.all(),
        }
        return set(mapping.get(criteria_type, []))

    def _empty_results(self) -> Dict[str, List]:
        """Пустые результаты"""
        return {
            'genres': [], 'themes': [],
            'perspectives': [], 'game_modes': [],
            'keywords': []
        }

    def clear_caches(self):
        """Очищает кеши"""
        for finder in self.finders.values():
            finder.clear_cache()
        self.keyword_finder.clear_cache()