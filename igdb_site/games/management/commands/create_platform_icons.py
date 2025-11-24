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

        # Затем создаем остальные иконки
        icons = {
            # PlayStation
            'playstation.svg': self.create_platform_icon('PS1', '#003087'),
            'playstation2.svg': self.create_platform_icon('PS2', '#003087'),
            'playstation3.svg': self.create_platform_icon('PS3', '#003087'),
            'playstation4.svg': self.create_platform_icon('PS4', '#003087'),
            'playstation5.svg': self.create_platform_icon('PS5', '#003087'),
            'psp.svg': self.create_platform_icon('PSP', '#003087'),
            'vita.svg': self.create_platform_icon('VITA', '#003087'),

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
            'game-boy.svg': self.create_platform_icon('GAME BOY', '#e60012'),

            # PC
            'windows.svg': self.create_platform_icon('PC', '#0078d7'),
            'linux.svg': self.create_platform_icon('LINUX', '#ff6600'),
            'macos.svg': self.create_platform_icon('MAC', '#000000'),

            # Mobile
            'ios.svg': self.create_platform_icon('iOS', '#000000'),

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