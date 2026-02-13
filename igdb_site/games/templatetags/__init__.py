from .game_tags import *
from .color_tags import *
from .game_type_tags import *
from .platform_icons import *
from .optimize_tags import *
from .game_card_tags import *  # Добавляем новый темплейттег

# Убедимся, что все теги зарегистрированы
__all__ = [
    'game_tags',
    'color_tags',
    'game_type_tags',
    'platform_icons',
    'optimize_tags',
    'game_card_tags',
]