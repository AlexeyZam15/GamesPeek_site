from django import template
from django.utils.safestring import mark_safe

register = template.Library()

# КОНСТАНТЫ РАЗМЕРОВ ПЛАТФОРМ
PLATFORM_FONT_SIZE = 10  # Размер шрифта в px
PLATFORM_PADDING_Y = 2  # Вертикальный padding в px
PLATFORM_PADDING_X = 5  # Горизонтальный padding в px
PLATFORM_MAX_WIDTH = 60  # Максимальная ширина в px
PLATFORM_BORDER_RADIUS = 3  # Скругление углов в px
PLATFORM_BORDER_WIDTH = 1  # Толщина рамки в px


@register.simple_tag
def platform_badge(platform_name):
    """Возвращает бейдж с текстом для платформы"""
    name_lower = platform_name.lower()

    # Собираем стили из констант
    padding = f"{PLATFORM_PADDING_Y}px {PLATFORM_PADDING_X}px"
    border = f"{PLATFORM_BORDER_WIDTH}px solid rgba(255, 255, 255, 0.2)"
    radius = f"{PLATFORM_BORDER_RADIUS}px"
    max_width = f"{PLATFORM_MAX_WIDTH}px"
    font_size = f"{PLATFORM_FONT_SIZE}px"

    # Маппинг только для заданных платформ
    short_names = {
        # Main consoles
        'playstation 5': 'PS5',
        'playstation 4': 'PS4',
        'playstation 3': 'PS3',
        'playstation 2': 'PS2',
        'playstation': 'PS1',
        'playstation portable': 'PSP',
        'playstation vita': 'Vita',
        'playstation vr': 'PSVR',
        'playstation vr2': 'VR2',

        'xbox series x|s': 'XBS',
        'xbox one': 'XOne',
        'xbox 360': '360',
        'xbox': 'Xbox',

        'nintendo switch 2': 'Switch2',
        'nintendo switch': 'Switch',
        'wii u': 'WiiU',
        'wii': 'Wii',
        'nintendo gamecube': 'GC',
        'nintendo 64': 'N64',
        'super nintendo entertainment system': 'SNES',
        'nintendo entertainment system': 'NES',
        'new nintendo 3ds': 'New3DS',
        'nintendo 3ds': '3DS',
        'nintendo ds': 'DS',
        'nintendo dsi': 'DSi',
        'game boy advance': 'GBA',
        'game boy color': 'GBC',
        'game boy': 'GB',

        # PC
        'pc (microsoft windows)': 'PC',
        'windows': 'Win',
        'mac': 'Mac',
        'linux': 'Linux',
        'dos': 'DOS',

        # Mobile
        'android': 'Droid',
        'ios': 'iOS',
        'windows phone': 'WinPh',
        'windows mobile': 'WinMob',
        'blackberry os': 'BB',

        # Sega
        'dreamcast': 'DC',
        'sega saturn': 'Saturn',
        'sega cd': 'SegaCD',
        'sega mega drive/genesis': 'MD',
        'sega master system/mark iii': 'SMS',
        'sega game gear': 'GG',

        # Retro/Classic
        'atari 2600': '2600',
        'atari 5200': '5200',
        'atari 8-bit': '8-bit',
        'atari jaguar': 'Jaguar',
        'atari jaguar cd': 'JagCD',
        'atari lynx': 'Lynx',
        'atari st/ste': 'ST',

        'commodore c64/128/max': 'C64',
        'commodore 16': 'C16',
        'commodore plus/4': '+4',
        'commodore vic-20': 'VIC20',
        'commodore pet': 'PET',
        'commodore cdtv': 'CDTV',

        'amiga': 'Amiga',
        'amiga cd32': 'CD32',

        'zx spectrum': 'ZX',
        'amstrad cpc': 'CPC',
        'amstrad pcw': 'PCW',

        'msx': 'MSX',
        'msx2': 'MSX2',

        'neo geo aes': 'NEO',
        'neo geo cd': 'NEOCD',
        'neo geo mvs': 'MVS',
        'neo geo pocket color': 'NGPC',

        'turbografx-16/pc engine': 'PCE',
        'turbografx-16/pc engine cd': 'PCE-CD',

        '3do interactive multiplayer': '3DO',
        'fm towns': 'FMT',
        'fm-7': 'FM7',
        'pc-8800 series': 'PC88',
        'pc-9800 series': 'PC98',
        'sharp mz-2200': 'MZ',
        'sharp x1': 'X1',
        'sharp x68000': 'X68K',

        # Other
        'web browser': 'Web',
        'google stadia': 'Stadia',
        'amazon fire tv': 'FireTV',
        'onlive game system': 'OnLive',
        'ouya': 'Ouya',
        'n-gage': 'N-Gage',
        'playdate': 'Playdate',
        'evercade': 'Evercade',
        'polymega': 'Poly',
        'arduboy': 'Ardu',
        'gamate': 'Gamate',
        'gizmondo': 'Gizmo',
        'tapwave zodiac': 'Zodiac',
        'zeebo': 'Zeebo',
        'super a\'can': 'A\'Can',
        'pdp-11': 'PDP11',
        'trs-80': 'TRS80',
        'trs-80 color computer': 'CoCo',
        'bbc microcomputer system': 'BBC',
        'dragon 32/64': 'Dragon',
        'acorn archimedes': 'Acorn',
        'acorn electron': 'Electron',
        'apple ii': 'AppleII',
        'apple iigs': 'AppleII',
        'apple pippin': 'Pippin',
        'colecovision': 'CV',
        'intellivision': 'Intelli',
        'intellivision amico': 'Amico',
        'vectrex': 'Vectrex',
        'wonderswan': 'WS',
        'wonderswan color': 'WSC',
        'pocketstation': 'Pocket',
        'pc-fx': 'PC-FX',
        'sg-1000': 'SG1000',
        'laseractive': 'Laser',
        'panasonic m2': 'M2',
        'philips cd-i': 'CD-i',
        'nec pc-6000 series': 'PC60',
        'thomson mo5': 'MO5',
        'texas instruments ti-99': 'TI99',
        'oovparts': 'OOP',
        'plato': 'PLATO',
        'palm os': 'Palm',

        # VR/Cloud
        'steamvr': 'VR',
        'oculus vr': 'VR',
        'oculus rift': 'Rift',
        'oculus quest': 'Quest',
        'oculus go': 'Go',
        'meta quest 2': 'Quest2',
        'meta quest 3': 'Quest3',
        'daydream': 'Daydream',
        'gear vr': 'GearVR',
        'windows mixed reality': 'WMR',
        'visionos': 'Vision',

        # Legacy
        'family computer': 'Famicom',
        'family computer disk system': 'FDS',
        'super famicom': 'SFC',
        'super nes cd-rom system': 'SNES-CD',
        '64dd': '64DD',
        'satellaview': 'BS-X',
        'handheld electronic lcd': 'LCD',
        'plug & play': 'PlugPlay',
        'dvd player': 'DVD',
        'legacy computer': 'LegacyPC',
        'legacy mobile device': 'LegacyMob',
    }

    # CSS классы для разных платформ
    platform_classes = {
        # PlayStation - синий
        'playstation': 'platform-ps',
        'ps': 'platform-ps',

        # Xbox - зеленый
        'xbox': 'platform-xbox',

        # Nintendo - красный
        'nintendo': 'platform-nintendo',
        'wii': 'platform-nintendo',
        'gamecube': 'platform-nintendo',
        'ds': 'platform-nintendo',
        'gb': 'platform-nintendo',

        # PC - серый/синий
        'pc': 'platform-pc',
        'windows': 'platform-pc',
        'win': 'platform-pc',
        'mac': 'platform-pc',
        'linux': 'platform-pc',
        'dos': 'platform-pc',

        # Mobile - фиолетовый
        'android': 'platform-mobile',
        'ios': 'platform-mobile',
        'phone': 'platform-mobile',
        'blackberry': 'platform-mobile',
        'palm': 'platform-mobile',

        # Sega - оранжевый
        'sega': 'platform-sega',
        'dreamcast': 'platform-sega',
        'saturn': 'platform-sega',
        'genesis': 'platform-sega',
        'mega': 'platform-sega',

        # Retro - коричневый
        'atari': 'platform-retro',
        'commodore': 'platform-retro',
        'amiga': 'platform-retro',
        'zx': 'platform-retro',
        'amstrad': 'platform-retro',
        'msx': 'platform-retro',
        'neo': 'platform-retro',
        '3do': 'platform-retro',
        'fm': 'platform-retro',
        'pc-': 'platform-retro',
        'sharp': 'platform-retro',

        # VR/Cloud - серый
        'vr': 'platform-vr',
        'quest': 'platform-vr',
        'oculus': 'platform-vr',
        'meta': 'platform-vr',
        'stadia': 'platform-cloud',
        'cloud': 'platform-cloud',
        'web': 'platform-cloud',
        'browser': 'platform-cloud',
        'onlive': 'platform-cloud',

        # Other - темно-серый
        'arcade': 'platform-arcade',
        'legacy': 'platform-legacy',
    }

    # Ищем точное совпадение
    for key, short_name in short_names.items():
        if key == name_lower:
            # Определяем CSS класс
            css_class = 'platform-other'
            for platform_key, platform_class in platform_classes.items():
                if platform_key in key or platform_key in name_lower:
                    css_class = platform_class
                    break

            html = f'''
            <span class="platform-badge {css_class}" 
                  data-bs-toggle="tooltip" 
                  data-bs-placement="top" 
                  title="{platform_name}"
                  style="font-size: {font_size}; padding: {padding}; max-width: {max_width}; border-radius: {radius}; border: {border};">
                {short_name}
            </span>
            '''
            return mark_safe(html)

    # Если не нашли точного совпадения, ищем частичное
    for key, short_name in short_names.items():
        if key in name_lower:
            css_class = 'platform-other'
            for platform_key, platform_class in platform_classes.items():
                if platform_key in key or platform_key in name_lower:
                    css_class = platform_class
                    break

            html = f'''
            <span class="platform-badge {css_class}" 
                  data-bs-toggle="tooltip" 
                  data-bs-placement="top" 
                  title="{platform_name}"
                  style="font-size: {font_size}; padding: {padding}; max-width: {max_width}; border-radius: {radius}; border: {border};">
                {short_name}
            </span>
            '''
            return mark_safe(html)

    # Для платформ не из списка - обрезаем до 6 символов
    default_name = platform_name[:6] + ('..' if len(platform_name) > 6 else '')

    html = f'''
    <span class="platform-badge platform-other" 
          data-bs-toggle="tooltip" 
          data-bs-placement="top" 
          title="{platform_name}"
          style="font-size: {font_size}; padding: {padding}; max-width: {max_width}; border-radius: {radius}; border: {border};">
        {default_name}
    </span>
    '''
    return mark_safe(html)


@register.filter
def split_platforms(platforms, per_row=8):
    """
    Разделяет платформы на ряды, но старается оставить в первом ряду больше,
    если во втором будет мало платформ.
    """
    if not platforms:
        return []

    total = len(platforms)

    # Если платформ мало - все в одном ряду
    if total <= per_row:
        return [platforms]

    # Если платформ немного больше, чем per_row - пробуем распределить
    if total <= per_row + 3:  # Если всего на 1-3 платформы больше
        # Считаем оптимальное распределение
        first_row = per_row
        second_row = total - per_row

        # Если во втором ряду меньше 4 платформ, берем часть из первого ряда
        if second_row < 4:
            # Перераспределяем, чтобы во втором ряду было хотя бы 4 платформы
            move_count = min(4 - second_row, first_row - 4)
            if move_count > 0:
                first_row -= move_count
                second_row += move_count

        return [platforms[:first_row], platforms[first_row:]]

    # Если платформ много - делим равномерно
    result = []
    for i in range(0, total, per_row):
        result.append(platforms[i:i + per_row])

    return result


@register.filter
def limit_platforms(platforms, limit=8):
    """Фильтр для ограничения количества платформ"""
    return platforms[:limit]


@register.filter
def has_more_platforms(platforms, limit=8):
    """Проверяет, есть ли еще платформы сверх лимита"""
    return len(platforms) > limit