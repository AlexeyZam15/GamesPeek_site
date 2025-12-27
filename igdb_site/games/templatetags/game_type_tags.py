# games/templatetags/game_type_tags.py
from django import template
from django.utils.safestring import mark_safe

register = template.Library()


@register.simple_tag
def game_type_badge(game_type):
    """
    Возвращает HTML бейджа для типа игры.
    Использование: {% game_type_badge game.game_type %}
    """
    if game_type is None:
        return ''

    # Маппинг типов игр на [текст, css-класс, полное название для тултипа]
    type_map = {
        0: ('Game', 'type-main', 'Main game'),
        1: ('DLC', 'type-dlc', 'DLC/Addon'),
        2: ('DLC', 'type-dlc', 'Expansion'),
        3: ('Bundle', 'type-bundle', 'Bundle'),
        4: ('Expansion', 'type-expansion', 'Standalone expansion'),
        5: ('Mod', 'type-mod', 'Mod'),
        6: ('Episode', 'type-episode', 'Episode'),
        7: ('Season', 'type-season', 'Season'),
        8: ('Remake', 'type-remake', 'Remake'),
        9: ('Remaster', 'type-remaster', 'Remaster'),
        10: ('Expanded', 'type-expanded', 'Expanded game'),
        11: ('Port', 'type-port', 'Port'),
        12: ('Fork', 'type-fork', 'Fork'),
        13: ('Pack', 'type-pack', 'Pack / Addon'),
        14: ('Update', 'type-update', 'Update'),
    }

    if game_type in type_map:
        display_text, css_class, tooltip = type_map[game_type]
    else:
        display_text, css_class, tooltip = 'Other', 'type-other', 'Unknown type'

    html = f'''
    <div class="position-absolute bottom-0 end-0 p-1">
        <span class="game-type-badge {css_class}" 
              data-bs-toggle="tooltip" 
              data-bs-placement="top" 
              title="{tooltip}">
            {display_text}
        </span>
    </div>
    '''

    return mark_safe(html)