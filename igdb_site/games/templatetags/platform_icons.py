from django import template
from django.utils.safestring import mark_safe

register = template.Library()

@register.simple_tag
def platform_icon(platform_name):
    """Возвращает иконку платформы"""
    name_lower = platform_name.lower()

    static_url = "/static/games/platform_icons/"

    platform_map = {
        # PlayStation
        'playstation 5': 'playstation5.svg',
        'ps5': 'playstation5.svg',
        'playstation 4': 'playstation4.svg',
        'ps4': 'playstation4.svg',
        'playstation 3': 'playstation3.svg',
        'ps3': 'playstation3.svg',
        'playstation 2': 'playstation2.svg',
        'ps2': 'playstation2.svg',
        'playstation': 'playstation.svg',
        'ps1': 'playstation.svg',
        'psp': 'psp.svg',
        'playstation portable': 'psp.svg',
        'vita': 'vita.svg',
        'playstation vita': 'vita.svg',

        # Xbox
        'xbox series x': 'xbox-series-x.svg',
        'xbox series s': 'xbox-series-x.svg',
        'xbox one': 'xbox-one.svg',
        'xbox 360': 'xbox-360.svg',
        'xbox': 'xbox.svg',

        # Nintendo
        'nintendo switch': 'nintendo-switch.svg',
        'switch': 'nintendo-switch.svg',
        'wii u': 'wii-u.svg',
        'wii': 'wii.svg',
        'nintendo 3ds': 'nintendo-3ds.svg',
        '3ds': 'nintendo-3ds.svg',
        'nintendo ds': 'nintendo-ds.svg',
        'ds': 'nintendo-ds.svg',
        'game boy': 'game-boy.svg',

        # PC
        'windows': 'windows.svg',
        'pc': 'windows.svg',
        'microsoft windows': 'windows.svg',
        'linux': 'linux.svg',
        'mac': 'macos.svg',
        'macos': 'macos.svg',
        'apple': 'macos.svg',
        'macintosh': 'macos.svg',

        # Mobile
        'android': 'android.png',  # Теперь PNG
        'ios': 'ios.svg',
        'iphone': 'ios.svg',
        'ipad': 'ios.svg',
    }

    for key, icon_file in platform_map.items():
        if key in name_lower:
            return mark_safe(
                f'<img src="{static_url}{icon_file}" height="24" '
                f'alt="{platform_name}" title="{platform_name}" '
                f'style="display: inline-block; vertical-align: middle; border-radius: 4px;">'
            )

    return mark_safe(
        f'<img src="{static_url}default.svg" height="24" '
        f'alt="{platform_name}" title="{platform_name}" '
        f'style="display: inline-block; vertical-align: middle; border-radius: 4px;">'
    )