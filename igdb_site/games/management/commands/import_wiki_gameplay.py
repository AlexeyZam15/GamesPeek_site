# games\management\commands\import_wiki_gameplay.py

"""
Главный файл команды импорта Wikipedia описаний
"""

import os
import sys
import django
from django.core.management.base import BaseCommand
from dotenv import load_dotenv

# Загружаем .env
load_dotenv()

# Добавляем путь к модулям
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Импортируем команду из папки
from import_wiki_gameplay_folder.command import Command as FolderCommand


class Command(BaseCommand):
    help = 'Асинхронный импорт описаний из Wikipedia'

    def add_arguments(self, parser):
        """Все аргументы перенесены сюда из command.py"""
        parser.add_argument(
            '--limit', type=int, default=0,
            help='Лимит на количество обрабатываемых игр (0 = без лимита)'
        )
        parser.add_argument(
            '--game-id', type=int,
            help='Обработать конкретную игру по ID'
        )
        parser.add_argument(
            '--game-name', type=str,
            help='Обработать конкретную игру по имени (частичное совпадение)'
        )
        parser.add_argument(
            '--chunk-size', type=int, default=200,
            help='Размер батча (по умолчанию: 200)'
        )
        parser.add_argument(
            '--max-concurrent', type=int, default=50,
            help='Максимальное количество одновременных соединений (по умолчанию: 50)'
        )
        parser.add_argument(
            '--max-save-concurrent', type=int, default=1,
            help='Максимальное количество одновременных сохранений (по умолчанию: 1)'
        )
        parser.add_argument(
            '--skip-existing', action='store_true', default=True,
            help='Пропускать игры с уже заполненным описанием Wikipedia (по умолчанию: True)'
        )
        parser.add_argument(
            '--only-empty', action='store_true',
            help='Обрабатывать только игры без ЛЮБОГО описания'
        )
        parser.add_argument(
            '--force-update', action='store_true',
            help='Принудительно обновить все описания (игнорирует --skip-existing)'
        )
        parser.add_argument(
            '--lang', default='en',
            help='Язык Wikipedia (по умолчанию: en)'
        )
        parser.add_argument(
            '--delay', type=float, default=0.3,
            help='Задержка между батчами в секундах (по умолчанию: 0.3)'
        )
        parser.add_argument(
            '--stats', action='store_true',
            help='Показать только статистику без выполнения импорта'
        )
        parser.add_argument(
            '--skip-errors', action='store_true', default=False,
            help='Пропускать ошибки без записи в файл'
        )
        parser.add_argument(
            '--no-progress', action='store_false', dest='progress', default=True,
            help='Отключить отображение прогресс-бара'
        )
        parser.add_argument(
            '--failed-file',
            default="failed_wiki_games.csv",
            help='Файл для записи ошибок (по умолчанию: failed_wiki_games.csv)'
        )
        parser.add_argument(
            '--no-save-failed', action='store_true', default=False,
            help='Не сохранять информацию о неудачных играх в файл'
        )
        parser.add_argument(
            '--skip-failed', action='store_true', default=False,
            help='Пропускать игры, которые уже в файле ошибок'
        )
        parser.add_argument(
            '--retry-failed', action='store_true', default=False,
            help='Обработать только игры из файла ошибок'
        )
        parser.add_argument(
            '--clear-failed', action='store_true', default=False,
            help='Очистить файл ошибок перед началом'
        )
        parser.add_argument(
            '--save-each-batch', action='store_true', default=True,
            help='Сохранять результаты после каждого батча (по умолчанию: True)'
        )
        parser.add_argument(
            '--include-not-found',
            action='store_true',
            default=False,
            help='Включить игры из файла ненайденных (по умолчанию: False)'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            default=False,
            help='Режим отладки - показывает все детальные логи'
        )
        parser.add_argument(
            '--clear-all',
            action='store_true',
            default=False,
            help='ОЧИСТИТЬ ВСЕ wiki_description в базе данных перед импортом'
        )
        parser.add_argument(
            '--wiki-user',
            type=str,
            default=os.getenv('WIKI_USER'),
            help='Имя пользователя Wikipedia для аутентификации (из .env если не указан)'
        )
        parser.add_argument(
            '--wiki-password',
            type=str,
            default=os.getenv('WIKI_PASSWORD'),
            help='Пароль Wikipedia для аутентификации (из .env если не указан)'
        )

    def handle(self, *args, **options):
        """Обработать поиск по имени и делегировать выполнение"""

        # Проверяем флаг очистки всех описаний
        if options.get('clear_all'):
            self.stdout.write(
                self.style.WARNING("\n⚠️  ВНИМАНИЕ! Вы собираетесь очистить ВСЕ Wikipedia описания в базе данных"))
            self.stdout.write(self.style.WARNING(f"   Будет очищено поле wiki_description у ВСЕХ игр"))
            self.stdout.write("")

            # Запрашиваем подтверждение
            confirm = input("   Для подтверждения введите 'YES' (полностью заглавными): ")

            if confirm != "YES":
                self.stdout.write(self.style.ERROR("\n❌ Операция отменена"))
                return

            self.stdout.write("")
            self.stdout.write("🔄 Очистка Wikipedia описаний...")

            # Настраиваем Django
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
            django.setup()

            from games.models import Game
            updated_count = Game.objects.filter(wiki_description__isnull=False).update(wiki_description=None)

            self.stdout.write(self.style.SUCCESS(f"✅ Очищено {updated_count:,} описаний"))
            self.stdout.write("")

            # Спрашиваем про файл ошибок
            clear_failed = input("   Очистить файл неудачных игр? (y/n): ").lower()
            if clear_failed == 'y':
                failed_file = options.get('failed_file', "failed_wiki_games.csv")
                if os.path.exists(failed_file):
                    os.remove(failed_file)
                    self.stdout.write(self.style.SUCCESS(f"✅ Файл {failed_file} удален"))
                else:
                    self.stdout.write(f"ℹ️  Файл {failed_file} не найден")

            self.stdout.write("")
            self.stdout.write("💡 Теперь можно запустить импорт командой:")
            self.stdout.write(f"   python manage.py import_wiki_gameplay --force-update --skip-failed")
            self.stdout.write("")
            return

        game_name = options.get('game_name')

        if game_name:
            # Настраиваем Django
            os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
            django.setup()

            from games.models import Game

            # Ищем игру по имени
            games = Game.objects.filter(name__icontains=game_name)

            if not games.exists():
                self.stdout.write(self.style.ERROR(f"❌ Игра '{game_name}' не найдена"))
                return

            if games.count() > 1:
                self.stdout.write(f"📋 Найдено несколько игр:")
                for i, game in enumerate(games[:10], 1):
                    self.stdout.write(f"   {i}. {game.name} (ID: {game.id})")

                if games.count() > 10:
                    self.stdout.write(f"   ... и ещё {games.count() - 10} игр")

                self.stdout.write(self.style.WARNING("\n⚠️  Уточните название или используйте --game-id"))
                return

            # Если нашлась одна игра
            game = games.first()
            self.stdout.write(f"🎯 Найдена игра: {game.name} (ID: {game.id})")

            # Заменяем game-name на game-id для передачи в folder_command
            options['game_id'] = game.id
            del options['game_name']

        # Делегируем выполнение команде из папки
        folder_command = FolderCommand()
        folder_command.stdout = self.stdout
        folder_command.stderr = self.stderr
        folder_command.style = self.style
        folder_command.handle(*args, **options)
