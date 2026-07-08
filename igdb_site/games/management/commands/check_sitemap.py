from django.core.management.base import BaseCommand
from django.test import Client
from django.core.paginator import Paginator
from games.sitemap import GameSitemap
from games.sitemap_similar_games import SimilarGamesSitemap
from games.models import Game
import xml.etree.ElementTree as ET


class Command(BaseCommand):
    """
    Команда для проверки и диагностики sitemap на локальном сервере.
    """
    help = 'Проверяет sitemap и выводит диагностическую информацию'

    def add_arguments(self, parser):
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Показывает подробную информацию по каждой странице',
        )

    def handle(self, *args, **options):
        verbose = options.get('verbose', False)

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('ПРОВЕРКА SITEMAP (ЛОКАЛЬНЫЙ СЕРВЕР)'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

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

        # 3. Создаём клиент для запросов
        client = Client()

        # 4. Проверяем главный sitemap.xml
        self.stdout.write('\n🔍 ПРОВЕРКА ГЛАВНОГО SITEMAP (sitemap.xml)')
        response = client.get('/sitemap.xml')

        self.stdout.write(f'   Статус: {response.status_code}')
        self.stdout.write(f'   Content-Type: {response.get("Content-Type", "не указан")}')

        # Проверяем X-Robots-Tag
        robots_tag = response.get('X-Robots-Tag', 'отсутствует')
        self.stdout.write(f'   X-Robots-Tag: {robots_tag}')

        # Парсим XML
        try:
            content = response.content.decode('utf-8')
            if '<sitemapindex' in content:
                self.stdout.write(self.style.SUCCESS('   ✅ Это SITEMAP INDEX'))

                # Считаем количество подкарт
                root = ET.fromstring(content)
                ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                sitemaps = root.findall('sitemap:sitemap', ns)
                self.stdout.write(f'   📂 Найдено подкарт: {len(sitemaps)}')

                # Показываем URL подкарт
                for i, sitemap in enumerate(sitemaps, 1):
                    loc = sitemap.find('sitemap:loc', ns)
                    if loc is not None:
                        self.stdout.write(f'   {i}. {loc.text}')
            elif '<urlset' in content:
                self.stdout.write(self.style.WARNING('   ⚠️ Это URLSET, а не INDEX'))

                # Считаем URL
                root = ET.fromstring(content)
                ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                urls = root.findall('sitemap:url', ns)
                self.stdout.write(f'   🔗 URL в sitemap: {len(urls)}')
            else:
                self.stdout.write(self.style.ERROR('   ❌ НЕИЗВЕСТНЫЙ ФОРМАТ XML'))

        except ET.ParseError as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка парсинга XML: {e}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

        # 5. Проверяем подкарту games
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

        # 6. Проверяем подкарту similar
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

        # 7. Проверяем пагинацию (если verbose)
        if verbose:
            self.stdout.write('\n📄 ПРОВЕРКА ПАГИНАЦИИ')
            self.stdout.write('   Проверяем страницы sitemap-games.xml...')

            for page in [1, 2, total_pages]:
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
                    except:
                        self.stdout.write(f'   Страница {page}: ОШИБКА')
                else:
                    self.stdout.write(f'   Страница {page}: СТАТУС {response.status_code}')

        # 8. Итоговый вывод
        self.stdout.write('\n' + '=' * 60)

        # Проверяем, является ли sitemap.xml индексом
        response = client.get('/sitemap.xml')
        if '<sitemapindex' in response.content.decode('utf-8'):
            self.stdout.write(self.style.SUCCESS('✅ SITEMAP РАБОТАЕТ КОРРЕКТНО'))
            self.stdout.write(self.style.SUCCESS(f'✅ {total_pages} страниц games + {total_pages} страниц similar'))
        else:
            self.stdout.write(self.style.ERROR('❌ SITEMAP НЕ РАБОТАЕТ (не индекс)'))
            self.stdout.write('   Проверьте:')
            self.stdout.write('   1. sitemap_views.py - должна вызывать index() для section=None')
            self.stdout.write('   2. urls.py - должен быть паттерн sitemap-<section>.xml')
            self.stdout.write('   3. Перезапустите сервер')

        self.stdout.write('=' * 60)