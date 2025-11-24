# management/commands/export_uvlist_games.py
import time
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
        parser.add_argument(
            '--delay',
            type=float,
            default=1.0,
            help='Delay between pages (seconds)'
        )

    def setup_webdriver(self):
        """Подключается к Chrome"""
        try:
            service = Service(ChromeDriverManager().install())
            chrome_options = Options()
            chrome_options.add_experimental_option("debuggerAddress", "127.0.0.1:9222")

            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(15)
            self.stdout.write("✓ Connected to Chrome")
            return driver
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error connecting to Chrome: {e}"))
            return None

    def parse_games_from_page(self, driver):
        """Парсит игры с текущей страницы"""
        games = []
        try:
            time.sleep(1)

            tables = driver.find_elements(By.TAG_NAME, "table")
            if not tables:
                return games

            main_table = tables[0]
            rows = main_table.find_elements(By.TAG_NAME, "tr")

            for row in rows[1:]:  # Пропускаем заголовок
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")

                    # Нужно минимум 3 ячейки (название, разработчик, год)
                    if len(cells) < 3:
                        continue

                    # Ячейка 0 - название игры
                    first_cell = cells[0]
                    game_links = first_cell.find_elements(By.TAG_NAME, "a")

                    if not game_links:
                        continue

                    game_name = game_links[0].text.strip()

                    if not game_name:
                        continue

                    # Ячейка 2 - ГОД (как мы увидели в отладке)
                    year_cell = cells[2]
                    year = year_cell.text.strip()

                    # Проверяем что это действительно год (4 цифры)
                    if year.isdigit() and len(year) == 4:
                        clean_name = game_name
                    else:
                        year = None

                    games.append({
                        'name': clean_name,
                        'year': year
                    })

                except Exception:
                    continue

            return games
        except Exception:
            return []

    def handle(self, *args, **options):
        pages = options['pages']
        output_file = options['output']
        delay = options['delay']

        self.stdout.write("=" * 50)
        self.stdout.write(f"Exporting UVList Tactical RPG Games")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Pages to parse: {pages}")
        self.stdout.write(f"Output file: {output_file}")

        driver = self.setup_webdriver()
        if not driver:
            return

        all_games = []
        start_time = time.time()

        try:
            base_url = "https://www.uvlist.net/gamesearch/?fplat=-&fyearprecision=1&ftag=tacticalrpg&sort=name"

            for page in range(pages):
                listed_value = page * 50
                self.stdout.write(f"📄 Page {page + 1}/{pages}...")

                url = f"{base_url}&listed={listed_value}"
                driver.get(url)
                time.sleep(delay)

                page_games = self.parse_games_from_page(driver)

                # Добавляем только новые игры
                for game in page_games:
                    if not any(g['name'] == game['name'] and g['year'] == game['year'] for g in all_games):
                        all_games.append(game)

                self.stdout.write(f"   ✅ Found {len(page_games)} games")

                if not page_games and page > 0:
                    break

            # Сохраняем в файл
            with open(output_file, 'w', encoding='utf-8') as f:
                for game in all_games:
                    if game['year']:
                        f.write(f"{game['name']} {game['year']}\n")
                    else:
                        f.write(f"{game['name']}\n")

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error: {e}"))

        finally:
            end_time = time.time()
            total_time = end_time - start_time

            self.stdout.write(f"\n✅ Successfully saved {len(all_games)} games!")
            self.stdout.write(f"⏱️  Total time: {total_time:.1f} seconds")
            self.stdout.write(f"📊 Games with year: {len([g for g in all_games if g['year']])}")
            self.stdout.write(f"💾 File: {output_file}")
