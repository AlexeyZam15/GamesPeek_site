"""Optimized game models organized in modules."""

from .enums import GameTypeEnum
from .managers import GameManager
from .game import Game
from .company import Company
from .series import Series
from .keywords import KeywordCategory, Keyword
from .simple_models import (
    Theme,
    PlayerPerspective,
    GameMode,
    Genre,
    Platform
)
from .similarity import (
    GameSimilarityDetail,
    GameCountsCache,
    GameSimilarityCache
)
from .media import Screenshot
from .game_card import GameCardCache  # Добавляем новую модель
from .game_engine import GameEngine  # Новая модель для игровых движков

__all__ = [
    'GameTypeEnum',
    'GameManager',
    'Game',
    'Company',
    'Series',
    'KeywordCategory',
    'Keyword',
    'Theme',
    'PlayerPerspective',
    'GameMode',
    'Genre',
    'Platform',
    'GameSimilarityDetail',
    'GameCountsCache',
    'GameSimilarityCache',
    'Screenshot',
    'GameCardCache',  # Добавляем в экспорт
    'GameEngine',  # Добавляем новую модель
]