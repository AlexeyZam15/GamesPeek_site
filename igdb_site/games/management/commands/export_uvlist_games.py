# management/commands/export_uvlist_games.py
import time
import re
from django.core.management.base import BaseCommand
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


class Command(BaseCommand):
    help = 'Export all UVList tactical RPG games to file'

    def add_arguments(self, parser):
        parser.add_argument(
            '--pages',
            type=int,
            default=13,
            help='Number of pages to parse (50 games per page)'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='uvlist_tactical_rpg_games.txt',
            help='Output filename'
        )

    def setup_webdriver(self):
        """Подключается к Chrome"""
        try:
            service = Service(ChromeDriverManager().install())
            chrome_options = Options()
            chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")
            driver = webdriver.Chrome(service=service, options=chrome_options)
            self.stdout.write("✓ Connected to Chrome")
            return driver
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error connecting to Chrome: {e}"))
            return None

    def parse_games_from_page(self, driver):
        """Парсит игры с текущей страницы"""
        games = []
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            if not tables:
                return games

            main_table = tables[0]
            rows = main_table.find_elements(By.TAG_NAME, "tr")

            for row in rows[1:]:  # Пропускаем заголовок
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if not cells:
                        continue

                    first_cell = cells[0]
                    game_links = first_cell.find_elements(By.TAG_NAME, "a")
                    if game_links:
                        game_name = game_links[0].text.strip()
                        if game_name and len(game_name) > 2:
                            clean_name = re.sub(r'\s+', ' ', game_name).strip()
                            if clean_name not in games:
                                games.append(clean_name)
                except:
                    continue

            return games
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error parsing page: {e}"))
            return []

    def handle(self, *args, **options):
        pages = options['pages']
        output_file = options['output']

        self.stdout.write("=" * 50)
        self.stdout.write(f"Exporting UVList Tactical RPG Games")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Pages to parse: {pages}")
        self.stdout.write(f"Output file: {output_file}")

        # Подключаемся к Chrome
        driver = self.setup_webdriver()
        if not driver:
            return

        all_games = []

        try:
            base_url = "https://www.uvlist.net/gamesearch/?fplat=-&fyearprecision=1&ftag=tacticalrpg&sort=name"

            for page in range(pages):
                listed_value = page * 50
                self.stdout.write(f"\nPage {page + 1}/{pages} (listed={listed_value})...")

                url = f"{base_url}&listed={listed_value}"
                driver.get(url)
                time.sleep(2)

                page_games = self.parse_games_from_page(driver)
                new_games = [game for game in page_games if game not in all_games]
                all_games.extend(new_games)

                self.stdout.write(f"Found: {len(page_games)} games ({len(new_games)} new)")

                # Если страница пустая - останавливаемся
                if not page_games:
                    self.stdout.write("Empty page, stopping...")
                    break

            # Сохраняем в файл
            self.stdout.write(f"\nSaving {len(all_games)} games to {output_file}...")

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"UVList Tactical RPG Games\n")
                f.write(f"Total games: {len(all_games)}\n")
                f.write(f"Pages parsed: {pages}\n")
                f.write("=" * 50 + "\n\n")

                for i, game in enumerate(all_games, 1):
                    f.write(f"{i:3d}. {game}\n")

            self.stdout.write("✓ Successfully saved!")
            self.stdout.write(f"📊 Total games: {len(all_games)}")

            # Показываем статистику
            self.stdout.write(f"\n📈 Statistics:")
            games_with_colons = len([g for g in all_games if ':' in g])
            games_with_digits = len([g for g in all_games if any(c.isdigit() for c in g)])
            long_names = len([g for g in all_games if len(g) > 30])

            self.stdout.write(f"   Games with ':': {games_with_colons}")
            self.stdout.write(f"   Games with digits: {games_with_digits}")
            self.stdout.write(f"   Long names (>30 chars): {long_names}")

            # Показываем примеры
            self.stdout.write(f"\n🎮 First 10 games:")
            for i, game in enumerate(all_games[:10], 1):
                self.stdout.write(f"   {i:2d}. {game}")

            if len(all_games) > 10:
                self.stdout.write(f"   ... and {len(all_games) - 10} more")

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {e}"))

        finally:
            self.stdout.write(f"\n✓ Done! File: {output_file}")