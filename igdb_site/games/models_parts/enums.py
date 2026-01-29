"""Game type enumerations with optimized lookup."""

from functools import lru_cache
from typing import Dict, Optional, List
from django.utils.translation import gettext_lazy as _


class GameTypeEnum:
    """Enum for predefined game types from IGDB with optimized lookup."""

    # Main games
    MAIN_GAME = 0
    STANDALONE_EXPANSION = 4
    SEASON = 7
    REMAKE = 8
    REMASTER = 9
    EXPANDED_GAME = 10

    # Non-main games
    DLC_ADDON = 1
    EXPANSION = 2
    BUNDLE = 3
    MOD = 5
    EPISODE = 6
    PORT = 11
    FORK = 12
    PACK_ADDON = 13
    UPDATE = 14

    # Django choices
    CHOICES = [
        (MAIN_GAME, _('Main game')),
        (STANDALONE_EXPANSION, _('Standalone expansion')),
        (SEASON, _('Season')),
        (REMAKE, _('Remake')),
        (REMASTER, _('Remaster')),
        (EXPANDED_GAME, _('Expanded game')),
        (DLC_ADDON, _('DLC/Addon')),
        (EXPANSION, _('Expansion')),
        (BUNDLE, _('Bundle')),
        (MOD, _('Mod')),
        (EPISODE, _('Episode')),
        (PORT, _('Port')),
        (FORK, _('Fork')),
        (PACK_ADDON, _('Pack / Addon')),
        (UPDATE, _('Update')),
    ]

    # Precomputed lookup tables
    _TYPE_INFO = {
        # Main games
        MAIN_GAME: {'name': 'main_game', 'is_primary': True, 'flag': None},
        STANDALONE_EXPANSION: {'name': 'standalone_expansion', 'is_primary': True, 'flag': 'is_standalone_expansion'},
        SEASON: {'name': 'season', 'is_primary': True, 'flag': 'is_season'},
        REMAKE: {'name': 'remake', 'is_primary': True, 'flag': 'is_remake'},
        REMASTER: {'name': 'remaster', 'is_primary': True, 'flag': 'is_remaster'},
        EXPANDED_GAME: {'name': 'expanded_game', 'is_primary': True, 'flag': 'is_expanded_game'},

        # Non-main games
        DLC_ADDON: {'name': 'dlc_addon', 'is_primary': False, 'flag': 'is_dlc'},
        EXPANSION: {'name': 'expansion', 'is_primary': False, 'flag': 'is_expansion'},
        BUNDLE: {'name': 'bundle', 'is_primary': False, 'flag': 'is_bundle_component'},
        MOD: {'name': 'mod', 'is_primary': False, 'flag': 'is_mod'},
        EPISODE: {'name': 'episode', 'is_primary': False, 'flag': 'is_episode'},
        PORT: {'name': 'port', 'is_primary': False, 'flag': 'is_port'},
        FORK: {'name': 'fork', 'is_primary': False, 'flag': 'is_fork'},
        PACK_ADDON: {'name': 'pack_addon', 'is_primary': False, 'flag': 'is_pack_addon'},
        UPDATE: {'name': 'update', 'is_primary': False, 'flag': 'is_update'},
    }

    _PRIMARY_GAME_TYPES = {MAIN_GAME, STANDALONE_EXPANSION, SEASON, REMAKE, REMASTER, EXPANDED_GAME}
    _NON_PRIMARY_GAME_TYPES = {DLC_ADDON, EXPANSION, BUNDLE, MOD, EPISODE, PORT, FORK, PACK_ADDON, UPDATE}

    _NAME_TO_ID = {
        'main_game': MAIN_GAME,
        'standalone_expansion': STANDALONE_EXPANSION,
        'season': SEASON,
        'remake': REMAKE,
        'remaster': REMASTER,
        'expanded_game': EXPANDED_GAME,
        'dlc_addon': DLC_ADDON,
        'expansion': EXPANSION,
        'bundle': BUNDLE,
        'mod': MOD,
        'episode': EPISODE,
        'port': PORT,
        'fork': FORK,
        'pack_addon': PACK_ADDON,
        'update': UPDATE,
    }

    @classmethod
    @lru_cache(maxsize=32)
    def get_type_info(cls, game_type_id: int) -> Dict:
        """Get type information by ID with caching."""
        return cls._TYPE_INFO.get(game_type_id, {
            'name': f'unknown_{game_type_id}',
            'is_primary': False,
            'flag': None,
        })

    @classmethod
    @lru_cache(maxsize=32)
    def get_name(cls, game_type_id: int) -> str:
        """Get type name by ID with caching."""
        info = cls.get_type_info(game_type_id)
        return info['name']

    @classmethod
    @lru_cache(maxsize=32)
    def is_primary(cls, game_type_id: int) -> bool:
        """Check if game type is primary with caching."""
        return game_type_id in cls._PRIMARY_GAME_TYPES

    @classmethod
    @lru_cache(maxsize=32)
    def get_id_by_name(cls, type_name: str) -> Optional[int]:
        """Get ID by type name with caching."""
        return cls._NAME_TO_ID.get(type_name)

    @classmethod
    def get_all_flags(cls) -> List[str]:
        """Get all flag names."""
        return [info['flag'] for info in cls._TYPE_INFO.values() if info['flag']]