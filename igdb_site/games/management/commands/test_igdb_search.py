# management/commands/test_igdb_search.py

import os
import re
import asyncio
import aiohttp
from django.core.management.base import BaseCommand
from django.conf import settings
from games.igdb_api import get_igdb_access_token


class Command(BaseCommand):
    help = 'Поиск игр в IGDB через API по точному названию'

    def add_arguments(self, parser):
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Включить режим отладки'
        )
        parser.add_argument(
            '--file',
            type=str,
            default='not_found_games.txt',
            help='Файл со списком игр'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Количество игр для проверки (0 = все)'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='not_found_in_igdb.txt',
            help='Файл для игр которые не найдены в IGDB'
        )
        parser.add_argument(
            '--concurrency',
            type=int,
            default=5,
            help='Количество одновременных запросов'
        )

    def extract_game_name_and_year(self, line):
        """Извлекает название игры и год из строки"""
        line = line.strip()
        if not line:
            return None, None

        year_match = re.search(r'(\d{4})$', line)
        if year_match:
            year = year_match.group(1)
            game_name = line[:-4].strip()
        else:
            year = None
            game_name = line

        return game_name, year

    async def search_igdb_api(self, session, access_token, game_name):
        """Поиск игры через IGDB API по точному названию"""
        query = f'''
            search "{game_name}";
            fields name;
            limit 1;
        '''.strip()

        headers = {
            'Client-ID': settings.IGDB_CLIENT_ID,
            'Authorization': f'Bearer {access_token}',
        }

        url = 'https://api.igdb.com/v4/games'

        try:
            async with session.post(url, headers=headers, data=query) as response:
                if response.status == 200:
                    results = await response.json()
                    # Если есть хоть один результат - игра найдена
                    return len(results) > 0
        except Exception:
            pass

        return False

    async def mass_search_games(self, games_with_years, access_token, concurrency, debug):
        """Массовый поиск игр с параллельными запросами"""
        semaphore = asyncio.Semaphore(concurrency)

        found_games = []
        not_found_games = []

        async with aiohttp.ClientSession() as session:
            tasks = []
            for game_data in games_with_years:
                task = self.search_single_game(
                    game_data, access_token, session, semaphore, debug
                )
                tasks.append(task)

            results = await asyncio.gather(*tasks)

            for result in results:
                if result['found']:
                    found_games.append(result)
                else:
                    not_found_games.append(result['original_line'])

        return found_games, not_found_games

    async def search_single_game(self, game_data, access_token, session, semaphore, debug):
        """Поиск одной игры по точному названию"""
        async with semaphore:
            if debug:
                self.stdout.write(f"🔍 Поиск: {game_data['name']}")

            found = await self.search_igdb_api(session, access_token, game_data['name'])

            result = {
                'original_line': game_data['original_line'],
                'search_name': game_data['name'],
                'found': found
            }

            if found:
                if debug:
                    self.stdout.write(f"   ✅ Найдено по точному названию")
                else:
                    self.stdout.write(f"✅ {game_data['name']}")
            else:
                if debug:
                    self.stdout.write(f"   ❌ Не найдено по точному названию")
                else:
                    self.stdout.write(f"❌ {game_data['name']}")

            return result

    async def async_handle(self, *args, **options):
        debug = options['debug']
        file_path = options['file']
        limit = options['limit']
        output_file = options['output']
        concurrency = options['concurrency']

        self.stdout.write("🎮 ПОИСК ИГР В IGDB API ПО ТОЧНОМУ НАЗВАНИЮ")
        self.stdout.write("=" * 60)
        self.stdout.write(f"Входной файл: {file_path}")
        self.stdout.write(f"Выходной файл: {output_file}")
        self.stdout.write(f"Параллельных запросов: {concurrency}")
        self.stdout.write("=" * 60)

        # Читаем игры из файла
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.stderr.write(f'❌ Файл {file_path} не найден!')
            return

        if not lines:
            self.stderr.write('❌ Файл пуст!')
            return

        # Парсим игры
        games_to_check = []
        for line in lines:
            game_name, year = self.extract_game_name_and_year(line)
            if game_name:  # Берем все игры, даже без года
                games_to_check.append({
                    'original_line': line,
                    'name': game_name,
                    'year': year
                })

        if limit > 0:
            games_to_check = games_to_check[:limit]

        self.stdout.write(f"🔍 Проверяем {len(games_to_check)} игр по точному названию...")

        # Получаем токен для API
        access_token = get_igdb_access_token()

        # Запускаем массовый поиск
        found_games, not_found_games = await self.mass_search_games(
            games_to_check, access_token, concurrency, debug
        )

        # Сохраняем не найденные игры
        if not_found_games:
            with open(output_file, 'w', encoding='utf-8') as f:
                for game_line in not_found_games:
                    f.write(f"{game_line}\n")

        # Итоги
        self.stdout.write(f"\n" + "=" * 60)
        self.stdout.write("🏆 ИТОГИ ПОИСКА")
        self.stdout.write("=" * 60)
        self.stdout.write(f"✅ Найдено игр: {len(found_games)}")
        self.stdout.write(f"❌ Не найдено: {len(not_found_games)}")
        self.stdout.write(f"📊 Успешность: {len(found_games) / len(games_to_check) * 100:.1f}%")

        if not_found_games:
            self.stdout.write(f"\n📁 Не найденные игры сохранены в: {output_file}")
            self.stdout.write(f"🎮 Примеры не найденных игр:")
            for game_line in not_found_games[:5]:
                self.stdout.write(f"   • {game_line}")

        if found_games and debug:
            self.stdout.write(f"\n🎯 Примеры найденных игр:")
            for game in found_games[:3]:
                self.stdout.write(f"   • {game['search_name']}")

    def handle(self, *args, **options):
        """Точка входа для Django команды"""
        asyncio.run(self.async_handle(*args, **options))