"""Optimized models for game database."""

from .models_parts import (
    GameTypeEnum,
    GameManager,
    Game,
    Company,
    Series,
    KeywordCategory,
    Keyword,
    Theme,
    PlayerPerspective,
    GameMode,
    Genre,
    Platform,
    GameSimilarityDetail,
    GameCountsCache,
    GameSimilarityCache,
    Screenshot,
    GameCardCache,  # Добавляем новую модель
    GameEngine,  # Новая модель для игровых движков
)

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