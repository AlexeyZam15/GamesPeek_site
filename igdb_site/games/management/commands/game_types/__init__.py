"""
Пакет для конфигурации типов игр IGDB.
"""

from .game_type_config import (
    GAME_TYPE_CONFIG,
    PRIMARY_GAME_TYPES,
    NON_PRIMARY_GAME_TYPES,
    GAME_TYPE_BY_NAME,
    get_game_type_info,
    get_game_type_description,
    get_all_flags,
    is_primary_game_type,
    get_type_statistics_key
)

__all__ = [
    'GAME_TYPE_CONFIG',
    'PRIMARY_GAME_TYPES',
    'NON_PRIMARY_GAME_TYPES',
    'GAME_TYPE_BY_NAME',
    'get_game_type_info',
    'get_game_type_description',
    'get_all_flags',
    'is_primary_game_type',
    'get_type_statistics_key'
]