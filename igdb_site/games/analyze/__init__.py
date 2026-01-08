# games/analyze/__init__.py
"""
Пакет для анализа игр

Основные классы:
    GameAnalyzerAPI - главный API для анализа
    TextAnalyzer - анализатор текста
    BatchAnalyzer - пакетный анализатор
    RangeCacheManager - менеджер кэширования диапазонов

Вспомогательные модули:
    utils - вспомогательные функции
"""

from .game_analyzer_api import GameAnalyzerAPI
from .text_analyzer import TextAnalyzer
from .batch_analyzer import BatchAnalyzer
from .range_cache import RangeCacheManager
from .utils import get_game_text, update_game_criteria, format_game_response, create_error_response

__all__ = [
    'GameAnalyzerAPI',
    'TextAnalyzer',
    'BatchAnalyzer',
    'RangeCacheManager',
    'get_game_text',
    'update_game_criteria',
    'format_game_response',
    'create_error_response'
]