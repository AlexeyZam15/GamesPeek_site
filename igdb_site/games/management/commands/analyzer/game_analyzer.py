# games/analyzer/game_analyzer.py
from typing import Dict, List, Set, Tuple
from django.db.models import QuerySet
from games.models import Game, Genre, Theme, PlayerPerspective, GameMode, Keyword
from .pattern_manager import PatternManager
from .criteria_finder import CriteriaFinder
from .keyword_finder import KeywordFinder


class GameAnalyzer:
    """Класс для анализа игр с оптимизацией производительности"""

    def __init__(self, command_instance=None):
        self.command = command_instance
        self.finders = self._create_finders()
        self.keyword_finder = KeywordFinder()
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
                     collect_patterns: bool = False, keywords_mode: bool = False) -> Tuple[
        Dict[str, List], Dict[str, List[Dict]]]:
        """Анализирует текст и возвращает найденные критерии"""
        if not text:
            return self._empty_results(), {}

        try:
            results = {}
            all_pattern_info = {}

            if keywords_mode:
                # РЕЖИМ КЛЮЧЕВЫХ СЛОВ
                try:
                    existing_keywords = set()
                    if game and not ignore_existing:
                        existing_keywords = set(game.keywords.all())

                    keywords_results, keywords_pattern_info = self.keyword_finder.find(
                        text,
                        existing_objects=existing_keywords,
                        pattern_collection_mode=collect_patterns
                    )

                    results['keywords'] = keywords_results

                    # Убираем дубликаты из pattern_info для ключевых слов
                    unique_keywords_pattern_info = []
                    seen_keywords_patterns = set()
                    for match in keywords_pattern_info:
                        match_key = (match.get('pattern', ''), match.get('matched_text', ''), match.get('name', ''))
                        if match_key not in seen_keywords_patterns:
                            seen_keywords_patterns.add(match_key)
                            unique_keywords_pattern_info.append(match)

                    all_pattern_info['keywords'] = unique_keywords_pattern_info

                except Exception as e:
                    if self.command:
                        self.command.stdout.write(
                            f"   ⚠️ Ошибка анализа ключевых слов для {game.name if game else 'текста'}: {e}")
                    results['keywords'] = []
                    all_pattern_info['keywords'] = []

                # Возвращаем ТОЛЬКО ключевые слова
                return results, all_pattern_info

            else:
                # РЕЖИМ ОБЫЧНЫХ КРИТЕРИЕВ
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

                # Не возвращаем ключевые слова в обычном режиме
                results['keywords'] = []
                all_pattern_info['keywords'] = []

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
            'keywords': game.keywords.all(),
        }
        return set(mapping.get(criteria_type, []))

    def _empty_results(self) -> Dict[str, List]:
        """Возвращает пустые результаты"""
        empty_results = {key: [] for key in self.finders.keys()}
        empty_results['keywords'] = []
        return empty_results

    def clear_caches(self):
        """Очищает кеши всех finders для экономии памяти"""
        for finder in self.finders.values():
            finder.clear_cache()
        self.keyword_finder.clear_cache()