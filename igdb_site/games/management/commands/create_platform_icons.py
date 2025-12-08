import os
import requests
from django.core.management.base import BaseCommand
from django.conf import settings


class Command(BaseCommand):
    help = 'Create custom platform icons'

    def handle(self, *args, **options):
        icons_dir = os.path.join(settings.BASE_DIR, 'static', 'games', 'platform_icons')
        os.makedirs(icons_dir, exist_ok=True)

        # Сначала скачиваем Android иконку
        self.download_android_icon(icons_dir)

        # Создаем остальные иконки
        icons = {
            # PlayStation
            'playstation.svg': self.create_platform_icon('PS1', '#003087'),
            'playstation2.svg': self.create_platform_icon('PS2', '#003087'),
            'playstation3.svg': self.create_platform_icon('PS3', '#003087'),
            'playstation4.svg': self.create_platform_icon('PS4', '#003087'),
            'playstation5.svg': self.create_platform_icon('PS5', '#003087'),
            'psp.svg': self.create_platform_icon('PSP', '#003087'),
            'vita.svg': self.create_platform_icon('VITA', '#003087'),
            'psvr.svg': self.create_platform_icon('PS VR', '#003087'),
            'psvr2.svg': self.create_platform_icon('PSVR2', '#003087'),

            # Xbox
            'xbox.svg': self.create_platform_icon('XBOX', '#107c10'),
            'xbox-360.svg': self.create_platform_icon('X360', '#107c10'),
            'xbox-one.svg': self.create_platform_icon('XONE', '#107c10'),
            'xbox-series-x.svg': self.create_platform_icon('XSX', '#107c10'),

            # Nintendo
            'nintendo-switch.svg': self.create_platform_icon('SWITCH', '#e60012'),
            'wii.svg': self.create_platform_icon('WII', '#e60012'),
            'wii-u.svg': self.create_platform_icon('WII U', '#e60012'),
            'nintendo-3ds.svg': self.create_platform_icon('3DS', '#e60012'),
            'nintendo-ds.svg': self.create_platform_icon('DS', '#e60012'),
            'game-boy.svg': self.create_platform_icon('GB', '#e60012'),
            'gameboy-color.svg': self.create_platform_icon('GBC', '#e60012'),
            'gba.svg': self.create_platform_icon('GBA', '#e60012'),
            'nintendo64.svg': self.create_platform_icon('N64', '#e60012'),
            'gamecube.svg': self.create_platform_icon('GCN', '#e60012'),
            'nes.svg': self.create_platform_icon('NES', '#e60012'),
            'snes.svg': self.create_platform_icon('SNES', '#e60012'),
            'satellaview.svg': self.create_platform_icon('BS-X', '#e60012'),
            'fds.svg': self.create_platform_icon('FDS', '#e60012'),
            '64dd.svg': self.create_platform_icon('64DD', '#e60012'),

            # Sega
            'dreamcast.svg': self.create_platform_icon('DC', '#1e6ca8'),
            'saturn.svg': self.create_platform_icon('SAT', '#1e6ca8'),
            'megadrive.svg': self.create_platform_icon('MD', '#1e6ca8'),
            'segacd.svg': self.create_platform_icon('SCD', '#1e6ca8'),
            'gamegear.svg': self.create_platform_icon('GG', '#1e6ca8'),

            # PC
            'windows.svg': self.create_platform_icon('PC', '#0078d7'),
            'dos.svg': self.create_platform_icon('DOS', '#0078d7'),
            'linux.svg': self.create_platform_icon('LINUX', '#ff6600'),
            'macos.svg': self.create_platform_icon('MAC', '#000000'),

            # Mobile
            'ios.svg': self.create_platform_icon('iOS', '#000000'),
            'windowsphone.svg': self.create_platform_icon('WP', '#0078d7'),
            'blackberry.svg': self.create_platform_icon('BB', '#000000'),
            'mobile.svg': self.create_platform_icon('MOB', '#666666'),

            # Retro/Classic
            'atari2600.svg': self.create_platform_icon('2600', '#8b4513'),
            'jaguar.svg': self.create_platform_icon('JAG', '#000000'),
            'atarist.svg': self.create_platform_icon('ST', '#8b4513'),
            'atari8bit.svg': self.create_platform_icon('A8', '#8b4513'),
            'c64.svg': self.create_platform_icon('C64', '#4169e1'),
            'amiga.svg': self.create_platform_icon('AMIGA', '#000000'),
            'zxspectrum.svg': self.create_platform_icon('ZX', '#ff0000'),
            'amstradcpc.svg': self.create_platform_icon('CPC', '#800080'),
            'msx.svg': self.create_platform_icon('MSX', '#4682b4'),
            'intellivision.svg': self.create_platform_icon('INTV', '#8b0000'),
            'ngpc.svg': self.create_platform_icon('NGPC', '#ff4500'),
            'pcengine.svg': self.create_platform_icon('PCE', '#ff69b4'),
            'pcenginecd.svg': self.create_platform_icon('PCE-CD', '#ff69b4'),

            # Arcade
            'arcade.svg': self.create_platform_icon('ARC', '#dc143c'),

            # Web/Cloud
            'webbrowser.svg': self.create_platform_icon('WEB', '#4285f4'),
            'stadia.svg': self.create_platform_icon('STADIA', '#008744'),
            'onlive.svg': self.create_platform_icon('ONLIVE', '#00a8ff'),

            # VR
            'oculus.svg': self.create_platform_icon('OCULUS', '#1c1e20'),
            'gearvr.svg': self.create_platform_icon('GEAR VR', '#34a853'),
            'daydream.svg': self.create_platform_icon('DAYDREAM', '#4285f4'),
            'steamvr.svg': self.create_platform_icon('VR', '#1b2838'),

            # Other
            '3do.svg': self.create_platform_icon('3DO', '#ff0000'),
            'acorn.svg': self.create_platform_icon('ACORN', '#800000'),
            'appleii.svg': self.create_platform_icon('APPLE II', '#66bb6a'),
            'bbc.svg': self.create_platform_icon('BBC', '#000080'),
            'cdtv.svg': self.create_platform_icon('CDTV', '#4169e1'),
            'pet.svg': self.create_platform_icon('PET', '#4169e1'),
            'vic20.svg': self.create_platform_icon('VIC-20', '#4169e1'),
            'fmtowns.svg': self.create_platform_icon('FM TOWNS', '#4682b4'),
            'fm7.svg': self.create_platform_icon('FM-7', '#4682b4'),
            'amico.svg': self.create_platform_icon('AMICO', '#ff6b6b'),
            'ngage.svg': self.create_platform_icon('N-GAGE', '#7b68ee'),
            'ouya.svg': self.create_platform_icon('OUYA', '#66bb6a'),
            'pc88.svg': self.create_platform_icon('PC-88', '#4682b4'),
            'pc98.svg': self.create_platform_icon('PC-98', '#4682b4'),
            'sharp.svg': self.create_platform_icon('SHARP', '#808080'),
            'x68000.svg': self.create_platform_icon('X68000', '#808080'),
            'trs80.svg': self.create_platform_icon('TRS-80', '#8b4513'),
            'wonderswan.svg': self.create_platform_icon('WS', '#ff69b4'),
            'wonderswancolor.svg': self.create_platform_icon('WSC', '#ff69b4'),

            # Default
            'default.svg': self.create_platform_icon('🎮', '#6c757d'),
        }

        created_count = 0
        for icon_file, svg_content in icons.items():
            try:
                file_path = os.path.join(icons_dir, icon_file)
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write(svg_content)
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f'✓ Created: {icon_file}'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'✗ Failed {icon_file}: {e}'))

        self.stdout.write(self.style.SUCCESS(f'Completed! Created: {created_count} icons + Android downloaded'))

    def create_platform_icon(self, text, color):
        """Создает SVG иконку с текстом"""
        text_length = len(text)
        width = max(24, text_length * 8)

        return f'''<svg width="{width}" height="24" viewBox="0 0 {width} 24" xmlns="http://www.w3.org/2000/svg">
<rect width="{width}" height="24" rx="4" fill="{color}"/>
<text x="{width / 2}" y="16" text-anchor="middle" fill="white" font-size="10" font-weight="bold" font-family="Arial, sans-serif">{text}</text>
</svg>'''

    def download_android_icon(self, icons_dir):
        """Скачивает оригинальную иконку Android"""
        try:
            url = 'https://img.icons8.com/external-tal-revivo-color-tal-revivo/96/external-android-a-mobile-operating-system-developed-by-google-logo-color-tal-revivo.png'
            response = requests.get(url, timeout=30)
            response.raise_for_status()

            file_path = os.path.join(icons_dir, 'android.png')
            with open(file_path, 'wb') as f:
                f.write(response.content)

            self.stdout.write(self.style.SUCCESS('✓ Downloaded Android icon'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'✗ Failed to download Android icon: {e}'))