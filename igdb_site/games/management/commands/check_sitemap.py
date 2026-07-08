"""
Команда для проверки и диагностики sitemap на локальном сервере.

Основные функции:
- Проверка главного sitemap.xml на корректность
- Проверка всех подкарт games и similar
- Валидация абсолютных URL в sitemap
- Проверка пагинации и количества записей
- Тестирование конкретных URL на доступность
"""

from django.core.management.base import BaseCommand
from django.test import Client
from django.core.paginator import Paginator
from games.sitemap import GameSitemap
from games.sitemap_similar_games import SimilarGamesSitemap
from games.models import Game
import xml.etree.ElementTree as ET
import re


class Command(BaseCommand):
    """
    Команда для проверки и диагностики sitemap на локальном сервере.

    Используется для отладки проблем с генерацией sitemap,
    проверки пагинации и валидации URL.
    """

    help = 'Проверяет sitemap и выводит диагностическую информацию'

    def add_arguments(self, parser):
        """
        Добавляет аргументы командной строки.

        Аргументы:
            --verbose: Показывает подробную информацию по каждой странице
            --url: Проверяет конкретный URL на доступность и возвращает его содержимое
        """
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Показывает подробную информацию по каждой странице'
        )
        parser.add_argument(
            '--url',
            type=str,
            help='Проверяет конкретный URL (например: /sitemap-games.xml?page=2)'
        )

    def handle(self, *args, **options):
        """
        Основной метод обработки команды.

        Выполняет последовательную проверку всех компонентов sitemap:
        1. Проверка количества игр в базе данных
        2. Проверка главного sitemap.xml
        3. Проверка подкарты games
        4. Проверка подкарты similar
        5. Проверка пагинации (если включен verbose режим)
        6. Проверка конкретного URL (если указан аргумент --url)
        """
        verbose = options.get('verbose', False)
        specific_url = options.get('url', None)

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('ПРОВЕРКА SITEMAP (ЛОКАЛЬНЫЙ СЕРВЕР)'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        # Если указан конкретный URL, проверяем только его
        if specific_url:
            self.check_specific_url(specific_url)
            return

        # 1. Проверяем количество игр
        game_count = Game.objects.count()
        self.stdout.write(f'\n📊 Всего игр в БД: {game_count}')

        if game_count == 0:
            self.stdout.write(self.style.ERROR('❌ В БАЗЕ НЕТ ИГР!'))
            return

        # 2. Проверяем пагинацию
        games_per_page = 1000
        total_pages = (game_count + games_per_page - 1) // games_per_page
        self.stdout.write(f'📄 Страниц в sitemap: {total_pages} (по {games_per_page} игр)')

        # Создаём клиент для запросов
        client = Client()

        # 3. Проверяем главный sitemap.xml
        self.check_main_sitemap(client)

        # 4. Проверяем подкарту games
        self.check_games_sitemap(client, verbose)

        # 5. Проверяем подкарту similar
        self.check_similar_sitemap(client)

        # 6. Проверяем пагинацию (если verbose)
        if verbose:
            self.check_pagination(client, total_pages)

        # 7. Итоговый вывод
        self.print_summary(client)

    def check_specific_url(self, url):
        """
        Проверяет конкретный URL и выводит его содержимое.

        Эта функция полезна для диагностики отдельных проблемных страниц.

        Аргументы:
            url: Путь к странице (например: /sitemap-games.xml?page=2)
        """
        self.stdout.write(f'\n🔍 ПРОВЕРКА URL: {url}')

        client = Client()
        response = client.get(url)

        self.stdout.write(f'   Статус: {response.status_code}')
        self.stdout.write(f'   Content-Type: {response.get("Content-Type", "не указан")}')

        robots_tag = response.get('X-Robots-Tag', 'отсутствует')
        self.stdout.write(f'   X-Robots-Tag: {robots_tag}')

        if response.status_code == 200:
            content = response.content.decode('utf-8')

            # Проверяем формат XML
            if '<sitemapindex' in content:
                self.stdout.write(self.style.SUCCESS('   ✅ Это SITEMAP INDEX'))
                self.print_sitemap_index_info(content)
            elif '<urlset' in content:
                self.stdout.write(self.style.SUCCESS('   ✅ Это URLSET'))
                self.print_urlset_info(content)
            else:
                self.stdout.write(self.style.WARNING('   ⚠️ Неизвестный формат'))

            # Показываем первые 500 символов для диагностики
            self.stdout.write('\n   СОДЕРЖИМОЕ (первые 500 символов):')
            self.stdout.write('   ' + '-' * 56)
            preview = content[:500]
            # Экранируем специальные символы для безопасного вывода
            preview = preview.replace('\n', '\n   ')
            self.stdout.write(f'   {preview}')
            if len(content) > 500:
                self.stdout.write('   ... (обрезано)')
            self.stdout.write('   ' + '-' * 56)

    def check_main_sitemap(self, client):
        """
        Проверяет главный sitemap.xml на корректность.

        Аргументы:
            client: Django test client для выполнения запросов
        """
        self.stdout.write('\n🔍 ПРОВЕРКА ГЛАВНОГО SITEMAP (sitemap.xml)')
        response = client.get('/sitemap.xml')

        self.stdout.write(f'   Статус: {response.status_code}')
        self.stdout.write(f'   Content-Type: {response.get("Content-Type", "не указан")}')

        robots_tag = response.get('X-Robots-Tag', 'отсутствует')
        self.stdout.write(f'   X-Robots-Tag: {robots_tag}')

        try:
            content = response.content.decode('utf-8')

            if '<sitemapindex' in content:
                self.stdout.write(self.style.SUCCESS('   ✅ Это SITEMAP INDEX'))
                self.print_sitemap_index_info(content)
            elif '<urlset' in content:
                self.stdout.write(self.style.WARNING('   ⚠️ Это URLSET, а не INDEX'))
                self.print_urlset_info(content)
            else:
                self.stdout.write(self.style.ERROR('   ❌ НЕИЗВЕСТНЫЙ ФОРМАТ XML'))

        except ET.ParseError as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка парсинга XML: {e}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

    def print_sitemap_index_info(self, content):
        """
        Выводит информацию о sitemap index.

        Аргументы:
            content: XML содержимое sitemap index
        """
        try:
            root = ET.fromstring(content)
            ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            sitemaps = root.findall('sitemap:sitemap', ns)
            self.stdout.write(f'   📂 Найдено подкарт: {len(sitemaps)}')

            # Проверяем, что URL абсолютные
            invalid_urls = []
            for i, sitemap in enumerate(sitemaps, 1):
                loc = sitemap.find('sitemap:loc', ns)
                if loc is not None:
                    url_text = loc.text
                    self.stdout.write(f'   {i}. {url_text}')

                    # Проверяем, что URL абсолютный (начинается с http:// или https://)
                    if not url_text.startswith(('http://', 'https://')):
                        invalid_urls.append(url_text)
                        self.stdout.write(self.style.ERROR(f'      ❌ ОТНОСИТЕЛЬНЫЙ URL!'))
                    else:
                        self.stdout.write(self.style.SUCCESS(f'      ✅ Абсолютный URL'))

            if invalid_urls:
                self.stdout.write(self.style.ERROR(f'   ⚠️ Найдено {len(invalid_urls)} относительных URL'))
                self.stdout.write('   Для исправления используйте get_full_url() в sitemap_views.py')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка при анализе: {e}'))

    def print_urlset_info(self, content):
        """
        Выводит информацию о urlset (подкарте).

        Аргументы:
            content: XML содержимое urlset
        """
        try:
            root = ET.fromstring(content)
            ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            urls = root.findall('sitemap:url', ns)
            self.stdout.write(f'   🔗 URL в sitemap: {len(urls)}')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка при анализе: {e}'))

    def check_games_sitemap(self, client, verbose):
        """
        Проверяет подкарту games.

        Аргументы:
            client: Django test client для выполнения запросов
            verbose: Флаг для отображения подробной информации
        """
        self.stdout.write('\n🔍 ПРОВЕРКА ПОДКАРТЫ GAMES')
        response = client.get('/sitemap-games.xml')

        self.stdout.write(f'   Статус: {response.status_code}')

        try:
            content = response.content.decode('utf-8')

            if '<urlset' in content:
                root = ET.fromstring(content)
                ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                urls = root.findall('sitemap:url', ns)
                self.stdout.write(self.style.SUCCESS(f'   ✅ Найдено URL: {len(urls)}'))

                # Проверяем URL на абсолютность
                invalid_urls = []
                for url_elem in urls[:3]:  # Проверяем первые 3 для быстроты
                    loc = url_elem.find('sitemap:loc', ns)
                    if loc is not None:
                        url_text = loc.text
                        if not url_text.startswith(('http://', 'https://')):
                            invalid_urls.append(url_text)

                if invalid_urls:
                    self.stdout.write(self.style.WARNING(f'   ⚠️ Найдены относительные URL в games sitemap'))
                    for url in invalid_urls[:3]:
                        self.stdout.write(f'      {url}')

                # Показываем первые 3 URL если verbose
                if verbose and len(urls) > 0:
                    self.stdout.write('   Первые 3 URL:')
                    for i, url_elem in enumerate(urls[:3], 1):
                        loc = url_elem.find('sitemap:loc', ns)
                        if loc is not None:
                            self.stdout.write(f'     {i}. {loc.text}')
            else:
                self.stdout.write(self.style.ERROR('   ❌ НЕ URLSET'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

    def check_similar_sitemap(self, client):
        """
        Проверяет подкарту similar.

        Аргументы:
            client: Django test client для выполнения запросов
        """
        self.stdout.write('\n🔍 ПРОВЕРКА ПОДКАРТЫ SIMILAR')
        response = client.get('/sitemap-similar.xml')

        self.stdout.write(f'   Статус: {response.status_code}')

        try:
            content = response.content.decode('utf-8')

            if '<urlset' in content:
                root = ET.fromstring(content)
                ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                urls = root.findall('sitemap:url', ns)
                self.stdout.write(self.style.SUCCESS(f'   ✅ Найдено URL: {len(urls)}'))
            else:
                self.stdout.write(self.style.ERROR('   ❌ НЕ URLSET'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

    def check_pagination(self, client, total_pages):
        """
        Проверяет пагинацию sitemap.

        Аргументы:
            client: Django test client для выполнения запросов
            total_pages: Общее количество страниц для проверки
        """
        self.stdout.write('\n📄 ПРОВЕРКА ПАГИНАЦИИ')
        self.stdout.write('   Проверяем страницы sitemap-games.xml...')

        pages_to_check = [1, 2]
        if total_pages > 2:
            pages_to_check.append(total_pages)

        for page in pages_to_check:
            url = f'/sitemap-games.xml?page={page}'
            response = client.get(url)

            if response.status_code == 200:
                try:
                    content = response.content.decode('utf-8')
                    if '<urlset' in content:
                        root = ET.fromstring(content)
                        ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                        urls = root.findall('sitemap:url', ns)
                        self.stdout.write(f'   Страница {page}: {len(urls)} URL')
                    else:
                        self.stdout.write(f'   Страница {page}: НЕ URLSET')
                except Exception:
                    self.stdout.write(f'   Страница {page}: ОШИБКА ПАРСИНГА')
            else:
                self.stdout.write(f'   Страница {page}: СТАТУС {response.status_code}')

        # Проверяем similar пагинацию
        self.stdout.write('   Проверяем страницы sitemap-similar.xml...')

        for page in pages_to_check[:2]:  # Проверяем первые 2 страницы для быстроты
            url = f'/sitemap-similar.xml?page={page}'
            response = client.get(url)

            if response.status_code == 200:
                try:
                    content = response.content.decode('utf-8')
                    if '<urlset' in content:
                        root = ET.fromstring(content)
                        ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                        urls = root.findall('sitemap:url', ns)
                        self.stdout.write(f'   Similar страница {page}: {len(urls)} URL')
                    else:
                        self.stdout.write(f'   Similar страница {page}: НЕ URLSET')
                except Exception:
                    self.stdout.write(f'   Similar страница {page}: ОШИБКА ПАРСИНГА')
            else:
                self.stdout.write(f'   Similar страница {page}: СТАТУС {response.status_code}')

    def print_summary(self, client):
        """
        Выводит итоговую информацию о состоянии sitemap.

        Аргументы:
            client: Django test client для выполнения запросов
        """
        self.stdout.write('\n' + '=' * 60)

        response = client.get('/sitemap.xml')
        content = response.content.decode('utf-8')

        if '<sitemapindex' in content:
            self.stdout.write(self.style.SUCCESS('✅ SITEMAP РАБОТАЕТ КОРРЕКТНО'))

            # Проверяем абсолютность URL в индексе
            try:
                root = ET.fromstring(content)
                ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                sitemaps = root.findall('sitemap:sitemap', ns)

                has_relative = False
                for sitemap in sitemaps:
                    loc = sitemap.find('sitemap:loc', ns)
                    if loc is not None and not loc.text.startswith(('http://', 'https://')):
                        has_relative = True
                        break

                if has_relative:
                    self.stdout.write(self.style.WARNING('⚠️ ЕСТЬ ОТНОСИТЕЛЬНЫЕ URL'))
                    self.stdout.write('   Исправьте sitemap_views.py: используйте get_full_url()')
                else:
                    self.stdout.write(self.style.SUCCESS('✅ ВСЕ URL АБСОЛЮТНЫЕ'))
            except Exception:
                pass
        else:
            self.stdout.write(self.style.ERROR('❌ SITEMAP НЕ РАБОТАЕТ (не индекс)'))
            self.stdout.write('   Проверьте:')
            self.stdout.write('   1. sitemap_views.py - должна вызывать index() для section=None')
            self.stdout.write('   2. urls.py - должен быть паттерн sitemap-<section>.xml')
            self.stdout.write('   3. Перезапустите сервер')

        self.stdout.write('=' * 60)