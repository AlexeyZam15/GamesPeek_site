# games/management/commands/load_igdb.py
from django.core.management.base import BaseCommand
import sys
import os

# Добавляем путь к текущей директории
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    # Пробуем импортировать из поддиректории load_igdb
    from .load_igdb.base_command import BaseIgdbCommand

    CommandBase = BaseIgdbCommand
    print("✅ BaseIgdbCommand импортирован успешно")
except ImportError as e:
    print(f"❌ Ошибка импорта: {e}")
    # Используем базовый класс как fallback
    CommandBase = BaseCommand
    print("⚠️  Используем BaseCommand как fallback")


class Command(CommandBase):
    """Команда для загрузки игр из IGDB"""

    help = 'Загрузка игр из IGDB с разными фильтрами'

    def handle(self, *args, **options):
        """Основной метод"""
        if CommandBase == BaseCommand:
            self.stdout.write("⚠️  Работает в fallback режиме")
            self.stdout.write("Доступные опции:")
            for key, value in options.items():
                self.stdout.write(f"  {key}: {value}")
        else:
            # Вызываем родительский метод
            super().handle(*args, **options)