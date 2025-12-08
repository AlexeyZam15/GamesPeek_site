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
        'playstation vr': 'psvr.svg',
        'playstation vr2': 'psvr2.svg',

        # Xbox
        'xbox series x': 'xbox-series-x.svg',
        'xbox series s': 'xbox-series-x.svg',
        'xbox series x|s': 'xbox-series-x.svg',
        'xbox one': 'xbox-one.svg',
        'xbox 360': 'xbox-360.svg',
        'xbox': 'xbox.svg',

        # Nintendo
        'nintendo switch': 'nintendo-switch.svg',
        'switch': 'nintendo-switch.svg',
        'nintendo switch 2': 'nintendo-switch.svg',
        'wii u': 'wii-u.svg',
        'wii': 'wii.svg',
        'nintendo 3ds': 'nintendo-3ds.svg',
        '3ds': 'nintendo-3ds.svg',
        'new nintendo 3ds': 'nintendo-3ds.svg',
        'nintendo ds': 'nintendo-ds.svg',
        'ds': 'nintendo-ds.svg',
        'nintendo dsi': 'nintendo-ds.svg',
        'nintendo 64': 'nintendo64.svg',
        'nintendo gamecube': 'gamecube.svg',
        'gamecube': 'gamecube.svg',
        'nintendo entertainment system': 'nes.svg',
        'nes': 'nes.svg',
        'family computer': 'nes.svg',
        'super nintendo entertainment system': 'snes.svg',
        'snes': 'snes.svg',
        'super famicom': 'snes.svg',
        'game boy': 'game-boy.svg',
        'game boy color': 'gameboy-color.svg',
        'game boy advance': 'gba.svg',
        'gba': 'gba.svg',
        'satellaview': 'satellaview.svg',
        'family computer disk system': 'fds.svg',
        '64dd': '64dd.svg',

        # Sega
        'dreamcast': 'dreamcast.svg',
        'sega saturn': 'saturn.svg',
        'sega mega drive': 'megadrive.svg',
        'sega genesis': 'megadrive.svg',
        'sega cd': 'segacd.svg',
        'sega game gear': 'gamegear.svg',

        # PC
        'windows': 'windows.svg',
        'pc': 'windows.svg',
        'microsoft windows': 'windows.svg',
        'pc (microsoft windows)': 'windows.svg',
        'dos': 'dos.svg',
        'linux': 'linux.svg',
        'mac': 'macos.svg',
        'macos': 'macos.svg',
        'apple': 'macos.svg',
        'macintosh': 'macos.svg',

        # Mobile
        'android': 'android.png',
        'ios': 'ios.svg',
        'iphone': 'ios.svg',
        'ipad': 'ios.svg',
        'windows phone': 'windowsphone.svg',
        'blackberry os': 'blackberry.svg',
        'blackberry': 'blackberry.svg',
        'legacy mobile device': 'mobile.svg',

        # Retro/Classic
        'atari 2600': 'atari2600.svg',
        'atari jaguar': 'jaguar.svg',
        'atari st': 'atarist.svg',
        'atari ste': 'atarist.svg',
        'atari 8-bit': 'atari8bit.svg',
        'commodore 64': 'c64.svg',
        'commodore 128': 'c64.svg',
        'commodore max': 'c64.svg',
        'commodore c64/128/max': 'c64.svg',
        'amiga': 'amiga.svg',
        'amiga cd32': 'amiga.svg',
        'zx spectrum': 'zxspectrum.svg',
        'amstrad cpc': 'amstradcpc.svg',
        'msx': 'msx.svg',
        'msx2': 'msx.svg',
        'intellivision': 'intellivision.svg',
        'neo geo pocket color': 'ngpc.svg',
        'turbografx-16': 'pcengine.svg',
        'pc engine': 'pcengine.svg',
        'turbografx-16/pc engine': 'pcengine.svg',
        'turbografx-16/pc engine cd': 'pcenginecd.svg',

        # Arcade
        'arcade': 'arcade.svg',

        # Web/Cloud
        'web browser': 'webbrowser.svg',
        'google stadia': 'stadia.svg',
        'onlive game system': 'onlive.svg',

        # VR
        'oculus vr': 'oculus.svg',
        'oculus quest': 'oculus.svg',
        'oculus quest 2': 'oculus.svg',
        'meta quest 2': 'oculus.svg',
        'meta quest 3': 'oculus.svg',
        'oculus go': 'oculus.svg',
        'gear vr': 'gearvr.svg',
        'daydream': 'daydream.svg',
        'steamvr': 'steamvr.svg',

        # Other
        '3do interactive multiplayer': '3do.svg',
        'acorn archimedes': 'acorn.svg',
        'apple ii': 'appleii.svg',
        'apple iigs': 'appleii.svg',
        'bbc microcomputer system': 'bbc.svg',
        'commodore cdtv': 'cdtv.svg',
        'commodore pet': 'pet.svg',
        'commodore vic-20': 'vic20.svg',
        'fm towns': 'fmtowns.svg',
        'fm-7': 'fm7.svg',
        'intellivision amico': 'amico.svg',
        'n-gage': 'ngage.svg',
        'ouya': 'ouya.svg',
        'pc-8800 series': 'pc88.svg',
        'pc-9800 series': 'pc98.svg',
        'sharp mz-2200': 'sharp.svg',
        'sharp x1': 'sharp.svg',
        'sharp x68000': 'x68000.svg',
        'trs-80': 'trs80.svg',
        'wonderswan': 'wonderswan.svg',
        'wonderswan color': 'wonderswancolor.svg',
    }

    # Проверка точных совпадений
    exact_matches = {
        '3DO': '3do.svg',
        '64DD': '64dd.svg',
        'FDS': 'fds.svg',
        'PC Engine CD': 'pcenginecd.svg',
    }

    for key, icon_file in exact_matches.items():
        if key.lower() == name_lower:
            return mark_safe(
                f'<img src="{static_url}{icon_file}" height="24" '
                f'alt="{platform_name}" title="{platform_name}" '
                f'style="display: inline-block; vertical-align: middle; border-radius: 4px;">'
            )

    # Проверка частичных совпадений
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