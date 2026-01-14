# games/analyze/__init__.py - ДОБАВЛЯЕМ НОВЫЕ КЛАССЫ
from .game_analyzer_api import GameAnalyzerAPI
from .text_analyzer import TextAnalyzer
from .batch_analyzer import BatchAnalyzer
from .range_cache import RangeCacheManager
from .utils import get_game_text, update_game_criteria, format_game_response, create_error_response

# НОВЫЕ ОПТИМИЗИРОВАННЫЕ КЛАССЫ
from .keyword_trie import KeywordTrie, KeywordTrieManager

__all__ = [
    'GameAnalyzerAPI',
    'TextAnalyzer',
    'BatchAnalyzer',
    'RangeCacheManager',
    'get_game_text',
    'update_game_criteria',
    'format_game_response',
    'create_error_response',
    # Новые оптимизированные классы
    'KeywordTrie',
    'KeywordTrieManager'
]