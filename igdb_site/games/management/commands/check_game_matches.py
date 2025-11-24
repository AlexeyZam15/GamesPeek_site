# management/commands/check_game_matches.py
import re
from django.core.management.base import BaseCommand
from django.db import models


class Command(BaseCommand):
    help = 'Check which UVList games exist in database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input',
            type=str,
            default='uvlist_tactical_rpg_games.txt',
            help='Input file with UVList games'
        )
        parser.add_argument(
            '--not-found-output',
            type=str,
            default='not_found_games.txt',
            help='Output file for NOT FOUND games with years'
        )

    def normalize_game_name(self, name):
        """Нормализует название игры для поиска"""
        # Удаляем год если он есть в названии
        name = re.sub(r'\s+\d{4}$', '', name)
        name = re.sub(r'[^\w\s]', ' ', name.lower())
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    def extract_game_name_and_year(self, line):
        """Извлекает название игры и год из строки"""
        line = line.strip()
        if not line:
            return None, None

        # Ищем год в конце строки (4 цифры)
        year_match = re.search(r'(\d{4})$', line)
        if year_match:
            year = year_match.group(1)
            game_name = line[:-4].strip()  # Убираем год из названия
        else:
            year = None
            game_name = line

        return game_name, year

    def find_game_in_db(self, game_name):
        """Ищет игру в базе данных"""
        from games.models import Game  # Импортируем вашу модель

        normalized_name = self.normalize_game_name(game_name)

        # Пробуем разные стратегии поиска
        search_attempts = [
            models.Q(name__iexact=game_name),
            models.Q(name__iexact=normalized_name),
            models.Q(name__icontains=game_name),
            models.Q(name__icontains=normalized_name),
            models.Q(name__icontains=game_name.replace(':', '').replace('-', ' ').strip()),
        ]

        for query in search_attempts:
            matches = Game.objects.filter(query)
            if matches.exists():
                return matches.first()

        return None

    def handle(self, *args, **options):
        input_file = options['input']
        not_found_output = options['not_found_output']

        self.stdout.write("=" * 50)
        self.stdout.write("Checking UVList games in database")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Input file: {input_file}")
        self.stdout.write(f"Not found output: {not_found_output}")

        # Читаем игры из файла
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except FileNotFoundError:
            self.stdout.write(self.style.ERROR(f"File {input_file} not found!"))
            self.stdout.write("Run: python manage.py export_uvlist_games first")
            return

        # Парсим игры из файла
        uvlist_games = []
        for line in lines:
            line = line.strip()
            if line:  # Игнорируем пустые строки
                game_name, year = self.extract_game_name_and_year(line)
                if game_name:
                    uvlist_games.append({
                        'original_line': line,
                        'name': game_name,
                        'year': year
                    })

        self.stdout.write(f"Found {len(uvlist_games)} games in {input_file}")

        # Проверяем каждую игру
        found_count = 0
        not_found_games = []  # Оригинальные строки с годами

        self.stdout.write(f"\nChecking games in database...")

        for i, game_data in enumerate(uvlist_games, 1):
            game_name = game_data['name']
            year = game_data['year']

            self.stdout.write(f"Checking {i}/{len(uvlist_games)}: {game_name}")

            match = self.find_game_in_db(game_name)
            if match:
                found_count += 1
                self.stdout.write("  ✅ FOUND")
            else:
                not_found_games.append(game_data['original_line'])
                self.stdout.write("  ❌ NOT FOUND")

        # Сохраняем список ненайденных игр (с годами)
        self.stdout.write(f"\nSaving {len(not_found_games)} NOT FOUND games to {not_found_output}...")
        with open(not_found_output, 'w', encoding='utf-8') as f:
            for game_line in not_found_games:
                f.write(f"{game_line}\n")

        # Выводим статистику
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("RESULTS")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Total UVList games: {len(uvlist_games)}")
        self.stdout.write(f"Found in database: {found_count}")
        self.stdout.write(f"Not found: {len(not_found_games)}")
        self.stdout.write(f"Match rate: {found_count / len(uvlist_games) * 100:.1f}%")

        self.stdout.write(f"\n✓ NOT FOUND games saved to: {not_found_output}")

        # Показываем примеры ненайденных игр
        if not_found_games:
            self.stdout.write(f"\n🎮 First 10 NOT FOUND games:")
            for i, game_line in enumerate(not_found_games[:10], 1):
                self.stdout.write(f"   {i}. {game_line}")
            if len(not_found_games) > 10:
                self.stdout.write(f"   ... and {len(not_found_games) - 10} more")