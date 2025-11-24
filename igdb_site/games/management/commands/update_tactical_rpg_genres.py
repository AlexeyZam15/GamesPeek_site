# management/commands/update_tactical_rpg_genres.py
import time
import re
from django.core.management.base import BaseCommand
from django.db import transaction, models
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


class Command(BaseCommand):
    help = 'Update tactical RPG genres from uvlist.net - FINAL VERSION'

    def add_arguments(self, parser):
        parser.add_argument(
            '--test-mode',
            action='store_true',
            help='Test mode without database operations'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Simulate without saving changes'
        )
        parser.add_argument(
            '--max-pages',
            type=int,
            default=13,
            help='Maximum number of pages to parse (50 games per page)'
        )
        parser.add_argument(
            '--max-games',
            type=int,
            default=650,
            help='Maximum number of games to process'
        )
        parser.add_argument(
            '--parse-only',
            action='store_true',
            help='Only parse games without any operations'
        )

    def setup_webdriver(self):
        """Подключается к уже запущенному Chrome"""
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

    def parse_games_from_current_page(self, driver):
        """Парсит игры с текущей страницы"""
        games = []
        try:
            tables = driver.find_elements(By.TAG_NAME, "table")
            if not tables:
                return games

            main_table = tables[0]
            rows = main_table.find_elements(By.TAG_NAME, "tr")

            for i, row in enumerate(rows[1:], 1):
                try:
                    cells = row.find_elements(By.TAG_NAME, "td")
                    if len(cells) < 1:
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

    def get_all_games_from_uvlist(self, driver, max_pages=13, max_games=650):
        """Парсит все игры с UVList с правильной пагинацией через listed"""
        all_games = []
        base_url = "https://www.uvlist.net/gamesearch/?fplat=-&fyearprecision=1&ftag=tacticalrpg&sort=name"

        self.stdout.write(f"Starting to parse up to {max_pages} pages (50 games per page)...")

        for page in range(max_pages):
            if len(all_games) >= max_games:
                self.stdout.write(f"Reached max games limit ({max_games})")
                break

            listed_value = page * 50
            self.stdout.write(f"\n--- Page {page + 1} (listed={listed_value}) ---")

            # Формируем URL для страницы
            url = f"{base_url}&listed={listed_value}"

            # Переходим на страницу
            self.stdout.write(f"Loading: {url}")
            driver.get(url)
            time.sleep(2)

            # Парсим игры
            page_games = self.parse_games_from_current_page(driver)
            new_games = [game for game in page_games if game not in all_games]
            all_games.extend(new_games)

            self.stdout.write(f"Games on page: {len(page_games)}")
            self.stdout.write(f"New games: {len(new_games)}")
            self.stdout.write(f"Total unique: {len(all_games)}")

            # Если на странице нет игр, останавливаемся
            if not page_games:
                self.stdout.write("No games found on this page, stopping pagination")
                break

            # Показываем пример новых игр
            if new_games:
                self.stdout.write("New games sample:")
                for i, game in enumerate(new_games[:3]):
                    self.stdout.write(f"  {i + 1}. {game}")
                if len(new_games) > 3:
                    self.stdout.write(f"  ... and {len(new_games) - 3} more")
            else:
                self.stdout.write("No new games (all duplicates)")

        return all_games[:max_games]

    def normalize_game_name(self, name):
        """Нормализует название игры для поиска"""
        name = re.sub(r'[^\w\s]', ' ', name.lower())
        name = re.sub(r'\s+', ' ', name).strip()
        return name

    def find_matching_games_in_db(self, game_names):
        """Находит совпадающие игры в базе данных"""
        from games.models import Game  # Замените на вашу модель

        matched_games = []

        for uv_name in game_names:
            try:
                normalized_uv = self.normalize_game_name(uv_name)

                # Поиск в несколько этапов
                search_queries = [
                    models.Q(name__iexact=uv_name),
                    models.Q(name__iexact=normalized_uv),
                    models.Q(name__icontains=uv_name),
                    models.Q(name__icontains=normalized_uv),
                    models.Q(name__icontains=uv_name.replace(':', '').strip()),
                    models.Q(name__icontains=uv_name.split(':')[0].strip()),
                ]

                for query in search_queries:
                    matches = Game.objects.filter(query).exclude(
                        id__in=[g.id for g in matched_games]
                    )
                    if matches.exists():
                        best_match = matches.first()
                        matched_games.append(best_match)
                        self.stdout.write(f"✓ Matched: '{uv_name}' -> '{best_match.name}'")
                        break

            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Error matching '{uv_name}': {e}"))

        return matched_games

    def assign_genres_to_games(self, games, dry_run=False):
        """Назначает жанры играм"""
        from games.models import Genre  # Замените на вашу модель

        # Получаем или создаем нужные жанры
        rpg_genre, _ = Genre.objects.get_or_create(
            name="Role-playing (RPG)",
            defaults={'igdb_id': 999001}
        )

        tactical_genre, _ = Genre.objects.get_or_create(
            name="Tactical",
            defaults={'igdb_id': 999002}
        )

        updated_count = 0

        for game in games:
            try:
                current_genres = set(game.genres.all())
                needs_update = False

                if rpg_genre not in current_genres:
                    game.genres.add(rpg_genre)
                    self.stdout.write(f"  + Added RPG to: {game.name}")
                    needs_update = True

                if tactical_genre not in current_genres:
                    game.genres.add(tactical_genre)
                    self.stdout.write(f"  + Added Tactical to: {game.name}")
                    needs_update = True

                if needs_update:
                    if not dry_run:
                        game.save()
                    updated_count += 1
                else:
                    self.stdout.write(f"  ✓ Already has genres: {game.name}")

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Error updating {game.name}: {e}"))

        return updated_count

    def handle(self, *args, **options):
        test_mode = options['test_mode']
        dry_run = options['dry_run']
        max_pages = options['max_pages']
        max_games = options['max_games']
        parse_only = options['parse_only']

        self.stdout.write("=" * 60)
        self.stdout.write("UVList Tactical RPG Genre Update - FINAL")
        self.stdout.write("=" * 60)

        if test_mode:
            self.stdout.write("🚀 TEST MODE - No database operations")
        if dry_run:
            self.stdout.write("🔍 DRY RUN - No changes will be saved")
        if parse_only:
            self.stdout.write("📄 PARSE ONLY - Only extract game names")

        self.stdout.write(f"📖 Pages to parse: {max_pages}")
        self.stdout.write(f"🎮 Max games: {max_games}")

        # Подключаемся к Chrome
        self.stdout.write("\nStep 1: Connecting to Chrome...")
        driver = self.setup_webdriver()
        if not driver:
            return

        try:
            # Парсим все игры
            self.stdout.write("\nStep 2: Parsing games from UVList...")
            uvlist_games = self.get_all_games_from_uvlist(driver, max_pages, max_games)

            if not uvlist_games:
                self.stdout.write(self.style.ERROR("No games found!"))
                return

            self.stdout.write(f"\n✅ Successfully parsed {len(uvlist_games)} unique games")

            if parse_only:
                # Сохраняем все игры в файл
                with open("all_tactical_rpg_games.txt", "w", encoding="utf-8") as f:
                    f.write(f"Total tactical RPG games from UVList: {len(uvlist_games)}\n")
                    f.write("=" * 50 + "\n")
                    for i, game in enumerate(uvlist_games, 1):
                        f.write(f"{i:3d}. {game}\n")

                self.stdout.write(f"📁 All games saved to: all_tactical_rpg_games.txt")
                self.stdout.write("\n🎮 First 10 games:")
                for i, game in enumerate(uvlist_games[:10]):
                    self.stdout.write(f"  {i + 1}. {game}")
                return

            if test_mode:
                self.stdout.write("\nStep 3: TEST MODE - Simulating database operations...")

                # Имитируем поиск в базе
                test_matches = []
                for game in uvlist_games[:20]:  # Только первые 20 для теста
                    test_matches.append({
                        'uv_name': game,
                        'db_name': f"MATCH: {game}",
                        'confidence': 'high' if len(game) > 10 else 'medium'
                    })

                self.stdout.write(f"📊 Would process {len(uvlist_games)} games")
                self.stdout.write(f"🎯 Would find ~{len(test_matches)} matches")

                if dry_run:
                    self.stdout.write("💾 DRY RUN: No genres would be assigned")
                else:
                    self.stdout.write("💾 WOULD UPDATE: Assigning RPG and Tactical genres")

            else:
                # РЕАЛЬНЫЕ ОПЕРАЦИИ С БАЗОЙ ДАННЫХ
                self.stdout.write("\nStep 3: Finding matches in database...")

                with transaction.atomic():
                    if dry_run:
                        # В dry-run создаем savepoint для отката
                        savepoint = transaction.savepoint()

                    # Ищем совпадения в базе
                    matched_games = self.find_matching_games_in_db(uvlist_games)

                    self.stdout.write(f"\n📊 Database matching results:")
                    self.stdout.write(f"   UVList games: {len(uvlist_games)}")
                    self.stdout.write(f"   Matched games: {len(matched_games)}")
                    self.stdout.write(f"   Match rate: {len(matched_games) / len(uvlist_games) * 100:.1f}%")

                    if matched_games:
                        self.stdout.write("\nStep 4: Assigning genres...")
                        updated_count = self.assign_genres_to_games(matched_games, dry_run)

                        self.stdout.write(f"\n✅ Genre assignment complete:")
                        self.stdout.write(f"   Games updated: {updated_count}")
                        self.stdout.write(f"   Operation: {'DRY RUN' if dry_run else 'ACTUAL UPDATE'}")

                        if dry_run:
                            # Откатываем dry-run
                            transaction.savepoint_rollback(savepoint)
                            self.stdout.write("🔄 Dry run completed - no changes saved")
                        else:
                            self.stdout.write("💾 Changes saved to database")
                    else:
                        self.stdout.write("❌ No matches found in database")

            self.stdout.write("\n" + "=" * 60)
            self.stdout.write("COMPLETE")
            self.stdout.write("=" * 60)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Error during processing: {e}"))
            import traceback
            self.stdout.write(traceback.format_exc())

        finally:
            self.stdout.write("\n✓ Chrome remains open.")