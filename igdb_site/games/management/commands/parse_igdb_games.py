# management/commands/parse_igdb_games.py

import time
import re
from django.core.management.base import BaseCommand
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service


class Command(BaseCommand):
    help = 'Быстрый поиск игр через Google: "название игры год igdb"'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input',
            type=str,
            default='not_found_in_igdb.txt',
            help='Файл с играми которые не найдены через API'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='found_on_igdb_website.txt',
            help='Файл для найденных игр'
        )
        parser.add_argument(
            '--not-found-output',
            type=str,
            default='still_not_found.txt',
            help='Файл для игр которые не найдены даже на сайте'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=2.0,
            help='Задержка между запросами (секунды)'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Ограничение количества игр для проверки (0 = все)'
        )

    def setup_webdriver(self):
        """Быстрая настройка браузера - сразу запускаем новый"""
        try:
            service = Service(ChromeDriverManager().install())
            chrome_options = Options()

            # Минимальные опции для скорости
            chrome_options.add_argument("--disable-blink-features=AutomationControlled")
            chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--window-size=1200,800")

            # Запускаем новый браузер быстро
            driver = webdriver.Chrome(service=service, options=chrome_options)
            driver.set_page_load_timeout(10)
            driver.implicitly_wait(2)

            self.stdout.write("✅ Браузер запущен")
            return driver
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Ошибка запуска браузера: {e}"))
            return None

    def check_captcha(self, driver):
        """Проверяет наличие капчи на странице"""
        captcha_indicators = [
            "captcha",
            "verify you are human",
            "robot check",
            "are you a robot",
            "recaptcha",
            "unusual traffic",
            "automated requests",
            "подтвердите что вы человек",
            "проверка на робота"
        ]

        page_source = driver.page_source.lower()
        for indicator in captcha_indicators:
            if indicator in page_source:
                return True

        # Дополнительная проверка по элементам
        try:
            captcha_elements = driver.find_elements(By.CSS_SELECTOR,
                                                    "#recaptcha, .g-recaptcha, iframe[src*='recaptcha'], form[action*='captcha']")
            if captcha_elements:
                return True
        except:
            pass

        return False

    def clean_game_name(self, name):
        """Очищает название игры от специальных символов"""
        if not name:
            return name

        # Заменяем амперсанды и другие проблемные символы
        cleaned = name.replace('&', 'and')
        return cleaned.strip()

    def extract_game_name_and_year(self, line):
        """Быстрое извлечение названия игры и года"""
        line = line.strip()
        if not line:
            return None, None

        # Простое извлечение года из конца строки
        year_match = re.search(r'(\d{4})$', line)
        if year_match:
            year = year_match.group(1)
            game_name = line[:-4].strip()
        else:
            year = None
            game_name = line

        # Очищаем название от амперсандов
        game_name = self.clean_game_name(game_name)

        return game_name, year

    def fast_google_search(self, driver, game_name, expected_year):
        """Быстрый поиск - берем первую же ссылку на IGDB"""
        try:
            # Простой поисковый запрос
            search_query = f'{game_name} {expected_year} igdb'
            search_url = f"https://www.google.com/search?q={search_query.replace(' ', '+')}"

            driver.get(search_url)
            time.sleep(0.8)

            # ПРОВЕРЯЕМ КАПЧУ
            if self.check_captcha(driver):
                self.stdout.write(f"\n🚨 ОБНАРУЖЕНА КАПЧА!")
                self.stdout.write(f"⏸️  Программа остановлена.")
                self.stdout.write(f"💡 Пройдите проверку в браузере и нажмите Enter чтобы продолжить...")
                input()
                self.stdout.write(f"▶️  Продолжаем поиск...")
                driver.get(search_url)
                time.sleep(0.8)

            # Ищем ссылки на IGDB
            igdb_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='igdb.com/games/']")

            # ПРОВЕРЯЕМ КАЖДУЮ ССЫЛКУ
            for link in igdb_links:
                href = link.get_attribute('href')
                link_text = link.text.strip()

                # Пропускаем пустые ссылки или ссылки без текста
                if not href or not link_text:
                    continue

                # БЕРЕМ ТОЛЬКО ПЕРВУЮ СТРОКУ ТЕКСТА
                first_line = link_text.split('\n')[0].strip()

                # Пропускаем если текст слишком короткий (меньше 3 символов)
                if len(first_line) < 3:
                    continue

                # Пропускаем если текст содержит только "IGDB" или подобное
                if first_line.lower() in ['igdb', 'igdb.com', 'games']:
                    continue

                # ОЧИСТКА ТЕКСТА - только от лишних частей в первой строке
                clean_text = first_line
                clean_text = re.sub(r'\s*IGDB\.com.*$', '', clean_text)
                clean_text = re.sub(r'\s*›\s*.*$', '', clean_text)
                clean_text = re.sub(r'\s*-\s*IGDB$', '', clean_text)
                clean_text = clean_text.strip()

                # Очищаем от амперсандов
                clean_text = self.clean_game_name(clean_text)

                self.stdout.write(f"   📝 Найдено: '{clean_text}'")

                return {
                    'name': clean_text,
                    'year': expected_year,
                    'url': href
                }

            # Если не нашли ни одной валидной ссылки
            self.stdout.write(f"   ❌ Не найдено валидных ссылок на IGDB")
            return None

        except Exception as e:
            self.stdout.write(f"   ⚠️  Ошибка: {e}")
            return None

    def handle(self, *args, **options):
        input_file = options['input']
        output_file = options['output']
        not_found_output = options['not_found_output']
        delay = options['delay']
        limit = options['limit']

        self.stdout.write("🚀 БЫСТРЫЙ ПОИСК ЧЕРЕЗ GOOGLE")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Входной файл: {input_file}")
        self.stdout.write(f"Задержка: {delay} сек")
        self.stdout.write("=" * 50)
        self.stdout.write("💡 При появлении капчи программа остановится")
        self.stdout.write("💡 Пройдите проверку в браузере и нажмите Enter")
        self.stdout.write("=" * 50)

        # Читаем игры из файла
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                lines = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.stderr.write(f'❌ Файл {input_file} не найден!')
            return

        if not lines:
            self.stderr.write('❌ Файл пуст!')
            return

        # Быстро парсим игры
        games_to_check = []
        for line in lines:
            game_name, year = self.extract_game_name_and_year(line)
            if game_name:
                games_to_check.append({
                    'original_line': line,
                    'name': game_name,
                    'year': year
                })

        if limit > 0:
            games_to_check = games_to_check[:limit]

        self.stdout.write(f"🔍 Проверяем {len(games_to_check)} игр...")

        # ЗАПУСКАЕМ БРАУЗЕР СРАЗУ - БЕЗ ПРОВЕРОК
        driver = self.setup_webdriver()
        if not driver:
            return

        found_games = []
        not_found_games = []
        start_time = time.time()
        processed = 0

        try:
            for i, game_data in enumerate(games_to_check, 1):
                self.stdout.write(f"{i}/{len(games_to_check)}: {game_data['name']} {game_data['year']}", ending='')

                # Быстрый поиск
                result = self.fast_google_search(driver, game_data['name'], game_data['year'])

                if result:
                    found_games.append({
                        'search': game_data['original_line'],
                        'found_name': result['name'],
                        'found_year': result['year'],
                        'url': result['url']
                    })
                    self.stdout.write(" ✅")
                    processed += 1
                else:
                    not_found_games.append(game_data['original_line'])
                    self.stdout.write(" ❌")

                # Пауза между запросами
                if i < len(games_to_check):
                    time.sleep(delay)

        except KeyboardInterrupt:
            self.stdout.write("\n⏹️  Поиск прерван пользователем")
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"Ошибка: {e}"))
        finally:
            # Сохраняем результаты в простом формате: одно название на строку
            if found_games:
                with open(output_file, 'w', encoding='utf-8') as f:
                    for game in found_games:
                        # Записываем только название игры
                        f.write(f"{game['found_name']}\n")
                self.stdout.write(f"✅ Найдено {len(found_games)} игр -> {output_file}")

            if not_found_games:
                with open(not_found_output, 'w', encoding='utf-8') as f:
                    for game_line in not_found_games:
                        f.write(f"{game_line}\n")
                self.stdout.write(f"❌ Не найдено {len(not_found_games)} игр -> {not_found_output}")

            # Быстрые итоги
            end_time = time.time()
            total_time = end_time - start_time
            games_per_minute = (processed / total_time) * 60 if total_time > 0 else 0

            self.stdout.write(f"\n" + "=" * 50)
            self.stdout.write(f"⏱️  Время: {total_time:.1f} сек")
            self.stdout.write(f"📊 Скорость: {games_per_minute:.1f} игр/мин")

            # Закрываем браузер
            try:
                driver.quit()
                self.stdout.write("✅ Браузер закрыт")
            except:
                pass
