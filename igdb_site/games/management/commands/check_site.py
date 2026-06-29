"""
Django management command to check site health.

Usage: python manage.py check_site
"""

from django.core.management.base import BaseCommand
from django.test.client import Client
from django.apps import apps


class Command(BaseCommand):
    """
    Simplified site health check command.
    Shows only whether pages work or not.
    """

    help = 'Проверяет работоспособность сайта'

    # ID существующей игры для проверки (Grand Theft Auto V)
    DEFAULT_GAME_ID = 46880

    def add_arguments(self, parser):
        parser.add_argument(
            '--game-id',
            type=int,
            default=self.DEFAULT_GAME_ID,
            help=f'ID игры для проверки (по умолчанию: {self.DEFAULT_GAME_ID})',
        )

    def handle(self, *args, **options):
        """
        Выполняет проверку сайта.
        """
        game_id = options.get('game_id', self.DEFAULT_GAME_ID)

        # Создаем тестового клиента
        client = Client()

        # Определяем маршруты для проверки
        routes_to_test = [
            ('/', 'Главная'),
            ('/games/', 'Список игр'),
            ('/search/', 'Поиск'),
            (f'/games/{game_id}/', 'Детали игры'),
        ]

        # Используем self.stdout.write для вывода
        self.stdout.write("\n" + "=" * 40)
        self.stdout.write("ПРОВЕРКА САЙТА")
        self.stdout.write("=" * 40)

        all_ok = True

        for path, name in routes_to_test:
            response = client.get(path)
            status = response.status_code

            if status == 200:
                self.stdout.write(f"✅ {name} - работает")
            else:
                self.stdout.write(f"❌ {name} - ОШИБКА {status}")
                all_ok = False

        self.stdout.write("=" * 40)

        if all_ok:
            self.stdout.write("✅ ВСЕ СТРАНИЦЫ РАБОТАЮТ")
        else:
            self.stdout.write("❌ ЕСТЬ ПРОБЛЕМЫ")

        self.stdout.write("=" * 40 + "\n")

        return 0 if all_ok else 1