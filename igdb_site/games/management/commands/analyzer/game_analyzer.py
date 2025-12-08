# games/analyzer/game_analyzer.py
from typing import Dict, List, Set, Tuple
from django.db.models import QuerySet
from games.models import Game, Genre, Theme, PlayerPerspective, GameMode
from .pattern_manager import PatternManager
from .criteria_finder import CriteriaFinder


class GameAnalyzer:
    """Класс для анализа игр с оптимизацией производительности"""

    def __init__(self, command_instance=None):
        self.command = command_instance
        self.finders = self._create_finders()
        self.stats = {
            'total_matches': 0,
            'pattern_checks': 0,
            'cache_hits': 0
        }

    def _create_finders(self) -> Dict[str, CriteriaFinder]:
        """Создает экземпляры CriteriaFinder для всех типов критериев"""
        compiled_patterns = PatternManager.get_compiled_patterns()
        return {
            'genres': CriteriaFinder(Genre, compiled_patterns['genres']),
            'themes': CriteriaFinder(Theme, compiled_patterns['themes']),
            'perspectives': CriteriaFinder(PlayerPerspective, compiled_patterns['perspectives']),
            'game_modes': CriteriaFinder(GameMode, compiled_patterns['game_modes']),
        }

    def analyze_text(self, text: str, game: Game = None, ignore_existing: bool = False,
                     collect_patterns: bool = False) -> Tuple[Dict[str, List], Dict[str, List[Dict]]]:
        """Анализирует текст и возвращает найденные критерии и информацию о паттернах"""
        if not text:
            return self._empty_results(), {}

        try:
            results = {}
            all_pattern_info = {}

            for criteria_type, finder in self.finders.items():
                # Определяем существующие критерии для этого типа
                existing_objects = set()
                if game and not ignore_existing:
                    try:
                        existing_objects = self._get_existing_objects(game, criteria_type)
                    except Exception as e:
                        if self.command:
                            self.command.stdout.write(
                                f"   ⚠️ Ошибка получения существующих {criteria_type} для {game.name}: {e}")
                        existing_objects = set()

                # Анализируем и получаем как объекты, так и информацию о паттернах
                try:
                    results[criteria_type], pattern_info = finder.find(
                        text,
                        existing_objects=existing_objects,
                        pattern_collection_mode=collect_patterns
                    )

                    # Убираем дубликаты из pattern_info
                    unique_pattern_info = []
                    seen_patterns = set()
                    for match in pattern_info:
                        match_key = (match.get('pattern', ''), match.get('matched_text', ''), match.get('name', ''))
                        if match_key not in seen_patterns:
                            seen_patterns.add(match_key)
                            unique_pattern_info.append(match)

                    all_pattern_info[criteria_type] = unique_pattern_info

                except Exception as e:
                    if self.command:
                        self.command.stdout.write(f"   ⚠️ Ошибка анализа {criteria_type} для {game.name}: {e}")
                    results[criteria_type] = []
                    all_pattern_info[criteria_type] = []

            return results, all_pattern_info

        except Exception as e:
            game_name = game.name if game else "неизвестная игра"
            if self.command:
                self.command.stdout.write(f"❌ Критическая ошибка анализа для {game_name}: {str(e)}")
                import traceback
                self.command.stdout.write(f"🔍 Трассировка: {traceback.format_exc()}")
            return self._empty_results(), {}

    def _get_existing_objects(self, game: Game, criteria_type: str) -> Set:
        """Возвращает существующие объекты для указанного типа критерия"""
        mapping = {
            'genres': game.genres.all(),
            'themes': game.themes.all(),
            'perspectives': game.player_perspectives.all(),
            'game_modes': game.game_modes.all(),
        }
        return set(mapping.get(criteria_type, []))

    def _empty_results(self) -> Dict[str, List]:
        """Возвращает пустые результаты"""
        return {key: [] for key in self.finders.keys()}

    def clear_caches(self):
        """Очищает кеши всех finders для экономии памяти"""
        for finder in self.finders.values():
            finder.clear_cache()