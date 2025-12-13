"""
Централизованная конфигурация типов игр IGDB (game_type).
Все обновления вносить ТОЛЬКО здесь.

Расположение: games/management/commands/game_types/game_type_config.py
"""

GAME_TYPE_CONFIG = {
    # Основные игры
    0: {'type': 'main_game', 'is_primary': True, 'description': 'Основная игра'},
    4: {'type': 'standalone_expansion', 'is_primary': True, 'description': 'Стенд-алоне расширение',
        'flag': 'is_standalone_expansion'},
    7: {'type': 'season', 'is_primary': True, 'description': 'Сезон', 'flag': 'is_season'},
    8: {'type': 'remake', 'is_primary': True, 'description': 'Ремейк', 'flag': 'is_remake'},
    9: {'type': 'remaster', 'is_primary': True, 'description': 'Ремастер', 'flag': 'is_remaster'},
    10: {'type': 'expanded_game', 'is_primary': True, 'description': 'Расширенное издание',
         'flag': 'is_expanded_game'},

    # Неосновные игры
    1: {'type': 'dlc_addon', 'is_primary': False, 'description': 'DLC/Дополнение', 'flag': 'is_dlc'},
    2: {'type': 'expansion', 'is_primary': False, 'description': 'Расширение', 'flag': 'is_expansion'},
    3: {'type': 'bundle', 'is_primary': False, 'description': 'Бандл', 'flag': 'is_bundle_component'},
    5: {'type': 'mod', 'is_primary': False, 'description': 'Мод', 'flag': 'is_mod'},
    6: {'type': 'episode', 'is_primary': False, 'description': 'Эпизод', 'flag': 'is_episode'},
    11: {'type': 'port', 'is_primary': False, 'description': 'Порт', 'flag': 'is_port'},
    12: {'type': 'fork', 'is_primary': False, 'description': 'Форк', 'flag': 'is_fork'},
    13: {'type': 'pack_addon', 'is_primary': False, 'description': 'Pack / Addon', 'flag': 'is_pack_addon'},
    14: {'type': 'update', 'is_primary': False, 'description': 'Обновление', 'flag': 'is_update'},
}

# Списки для удобства
PRIMARY_GAME_TYPES = [gt for gt, config in GAME_TYPE_CONFIG.items() if config['is_primary']]
NON_PRIMARY_GAME_TYPES = [gt for gt, config in GAME_TYPE_CONFIG.items() if not config['is_primary']]

# Словарь для быстрого поиска по имени типа
GAME_TYPE_BY_NAME = {config['type']: gt for gt, config in GAME_TYPE_CONFIG.items()}


def get_game_type_info(game_type):
    """Получает информацию о game_type из конфига"""
    if game_type in GAME_TYPE_CONFIG:
        return GAME_TYPE_CONFIG[game_type]
    else:
        return {
            'type': f'unknown_{game_type}',
            'is_primary': False,
            'description': f'Неизвестный тип ({game_type})'
        }


def get_game_type_description(game_type):
    """Получает описание типа игры по game_type"""
    if game_type is None:
        return "Game Type отсутствует"

    info = get_game_type_info(game_type)
    return info['description']


def get_all_flags():
    """Возвращает список всех возможных флагов из конфигурации"""
    flags = set()
    for config in GAME_TYPE_CONFIG.values():
        if 'flag' in config:
            flags.add(config['flag'])
    return list(flags)


def is_primary_game_type(game_type):
    """Проверяет, является ли тип игры основным"""
    info = get_game_type_info(game_type)
    return info['is_primary']


def get_type_statistics_key(game_type):
    """Возвращает ключ для статистики по типу игры"""
    if game_type is None:
        return 'game_type_none'

    if game_type == 1:
        return 'dlc_addon'
    elif game_type == 2:
        return 'expansion'
    elif game_type == 3:
        return 'bundle'
    elif game_type == 5:
        return 'mod'
    elif game_type == 6:
        return 'episode'  # Добавляем для Эпизодов
    elif game_type == 11:
        return 'port'  # Порты - неосновные
    elif game_type == 12:
        return 'fork'  # Добавляем для Форков
    elif game_type == 13:
        return 'pack_addon'
    elif game_type == 14:
        return 'update'
    else:
        info = get_game_type_info(game_type)
        return info['type']