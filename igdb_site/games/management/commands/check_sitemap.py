"""
Команда для проверки и диагностики sitemap на локальном сервере или реальном домене.

Основные функции:
- Проверка главного sitemap.xml на корректность
- Проверка всех подкарт games и similar
- Валидация абсолютных URL в sitemap
- Проверка пагинации и количества записей
- Проверка количества URL на каждой странице (должно быть 1000)
- Тестирование конкретных URL на доступность
- Поддержка проверки на реальном домене через --domain
"""

from django.core.management.base import BaseCommand
from django.core.paginator import Paginator
from games.sitemap import GameSitemap
from games.sitemap_similar_games import SimilarGamesSitemap
from games.models import Game
import xml.etree.ElementTree as ET
import requests
import re


class Command(BaseCommand):
    """
    Команда для проверки и диагностики sitemap на локальном сервере или реальном домене.

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
            --domain: Проверяет sitemap на реальном домене (например: gamespeek.dpdns.org)
            --protocol: Протокол для реального домена (http или https, по умолчанию: https)
            --check-counts: Проверяет количество URL на каждой странице sitemap
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
        parser.add_argument(
            '--domain',
            type=str,
            help='Проверяет sitemap на реальном домене (например: gamespeek.dpdns.org)'
        )
        parser.add_argument(
            '--protocol',
            type=str,
            default='https',
            choices=['http', 'https'],
            help='Протокол для реального домена (по умолчанию: https)'
        )
        parser.add_argument(
            '--check-counts',
            action='store_true',
            help='Проверяет количество URL на каждой странице sitemap (должно быть 1000)'
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
        6. Проверка количества URL на каждой странице (если указан --check-counts)
        7. Проверка конкретного URL (если указан аргумент --url)
        """
        verbose = options.get('verbose', False)
        specific_url = options.get('url', None)
        domain = options.get('domain', None)
        protocol = options.get('protocol', 'https')
        check_counts = options.get('check_counts', False)

        self.stdout.write(self.style.SUCCESS('=' * 60))

        if domain:
            self.stdout.write(self.style.SUCCESS(f'ПРОВЕРКА SITEMAP НА РЕАЛЬНОМ ДОМЕНЕ: {protocol}://{domain}'))
        else:
            self.stdout.write(self.style.SUCCESS('ПРОВЕРКА SITEMAP (ЛОКАЛЬНЫЙ СЕРВЕР)'))

        self.stdout.write(self.style.SUCCESS('=' * 60))

        # Если указан конкретный URL, проверяем только его
        if specific_url:
            self.check_specific_url(specific_url, domain, protocol)
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

        # 3. Проверяем главный sitemap.xml
        self.check_main_sitemap(domain, protocol)

        # 4. Проверяем подкарту games
        self.check_games_sitemap(domain, protocol, verbose)

        # 5. Проверяем подкарту similar
        self.check_similar_sitemap(domain, protocol)

        # 6. Проверяем пагинацию (если verbose)
        if verbose:
            self.check_pagination(total_pages, domain, protocol)

        # 7. Проверяем количество URL на каждой странице (если указан --check-counts)
        if check_counts:
            self.check_page_counts(total_pages, domain, protocol)

        # 8. Итоговый вывод
        self.print_summary(domain, protocol)

    def get_response(self, url_path, domain=None, protocol='https'):
        """
        Выполняет GET запрос к указанному URL.

        Аргументы:
            url_path: Путь (например: /sitemap.xml) или полный URL
            domain: Домен или None для локального сервера
            protocol: Протокол (http/https)

        Возвращает:
            Response object с методами status_code, content, headers
        """
        if domain:
            full_url = f'{protocol}://{domain}{url_path}'
            return requests.get(full_url, timeout=30)
        else:
            from django.test import Client
            client = Client()
            return client.get(url_path)

    def check_specific_url(self, url_path, domain, protocol):
        """
        Проверяет конкретный URL и выводит его содержимое.

        Аргументы:
            url_path: Путь к странице (например: /sitemap-games.xml?page=2)
            domain: Домен или None для локального сервера
            protocol: Протокол (http/https)
        """
        if domain:
            full_url = f'{protocol}://{domain}{url_path}'
            self.stdout.write(f'\n🔍 ПРОВЕРКА URL: {full_url}')
            response = requests.get(full_url, timeout=30)
        else:
            full_url = url_path
            self.stdout.write(f'\n🔍 ПРОВЕРКА URL: {full_url}')
            from django.test import Client
            client = Client()
            response = client.get(url_path)

        self.stdout.write(f'   Статус: {response.status_code}')
        self.stdout.write(f'   Content-Type: {response.headers.get("Content-Type", "не указан")}')

        robots_tag = response.headers.get('X-Robots-Tag', 'отсутствует')
        self.stdout.write(f'   X-Robots-Tag: {robots_tag}')

        if response.status_code == 200:
            content = response.text

            if '<sitemapindex' in content:
                self.stdout.write(self.style.SUCCESS('   ✅ Это SITEMAP INDEX'))
                self.print_sitemap_index_info(content)
            elif '<urlset' in content:
                self.stdout.write(self.style.SUCCESS('   ✅ Это URLSET'))
                self.print_urlset_info(content)
            else:
                self.stdout.write(self.style.WARNING('   ⚠️ Неизвестный формат'))

            self.stdout.write('\n   СОДЕРЖИМОЕ (первые 500 символов):')
            self.stdout.write('   ' + '-' * 56)
            preview = content[:500]
            preview = preview.replace('\n', '\n   ')
            self.stdout.write(f'   {preview}')
            if len(content) > 500:
                self.stdout.write('   ... (обрезано)')
            self.stdout.write('   ' + '-' * 56)

    def check_main_sitemap(self, domain, protocol):
        """
        Проверяет главный sitemap.xml на корректность.

        Аргументы:
            domain: Домен или None для локального сервера
            protocol: Протокол (http/https)
        """
        url_path = '/sitemap.xml'

        if domain:
            full_url = f'{protocol}://{domain}{url_path}'
            self.stdout.write(f'\n🔍 ПРОВЕРКА ГЛАВНОГО SITEMAP: {full_url}')
            response = requests.get(full_url, timeout=30)
        else:
            self.stdout.write('\n🔍 ПРОВЕРКА ГЛАВНОГО SITEMAP (sitemap.xml)')
            from django.test import Client
            client = Client()
            response = client.get(url_path)

        self.stdout.write(f'   Статус: {response.status_code}')
        self.stdout.write(f'   Content-Type: {response.headers.get("Content-Type", "не указан")}')

        robots_tag = response.headers.get('X-Robots-Tag', 'отсутствует')
        self.stdout.write(f'   X-Robots-Tag: {robots_tag}')

        try:
            content = response.text

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

            invalid_urls = []
            for i, sitemap in enumerate(sitemaps, 1):
                loc = sitemap.find('sitemap:loc', ns)
                if loc is not None:
                    url_text = loc.text
                    if i <= 5:  # Показываем только первые 5 для краткости
                        self.stdout.write(f'   {i}. {url_text}')

                    if not url_text.startswith(('http://', 'https://')):
                        invalid_urls.append(url_text)
                        self.stdout.write(self.style.ERROR(f'      ❌ ОТНОСИТЕЛЬНЫЙ URL!'))
                    else:
                        if i <= 5:
                            self.stdout.write(self.style.SUCCESS(f'      ✅ Абсолютный URL'))

            if len(sitemaps) > 5:
                self.stdout.write(f'   ... и еще {len(sitemaps) - 5} подкарт')

            if invalid_urls:
                self.stdout.write(self.style.ERROR(f'   ⚠️ Найдено {len(invalid_urls)} относительных URL'))
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

            # Проверяем, что количество URL соответствует ожидаемому (1000)
            if len(urls) == 0:
                self.stdout.write(self.style.ERROR('   ❌ В sitemap НЕТ URL!'))
            elif len(urls) < 1000:
                self.stdout.write(self.style.WARNING(f'   ⚠️ МАЛО URL: {len(urls)} (ожидается 1000)'))
            elif len(urls) == 1000:
                self.stdout.write(self.style.SUCCESS('   ✅ Правильное количество URL: 1000'))
            else:
                self.stdout.write(self.style.WARNING(f'   ⚠️ МНОГО URL: {len(urls)} (ожидается 1000)'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка при анализе: {e}'))

    def check_games_sitemap(self, domain, protocol, verbose):
        """
        Проверяет подкарту games.

        Аргументы:
            domain: Домен или None для локального сервера
            protocol: Протокол (http/https)
            verbose: Флаг для отображения подробной информации
        """
        url_path = '/sitemap-games.xml'

        if domain:
            full_url = f'{protocol}://{domain}{url_path}'
            self.stdout.write(f'\n🔍 ПРОВЕРКА ПОДКАРТЫ GAMES: {full_url}')
            response = requests.get(full_url, timeout=30)
        else:
            self.stdout.write('\n🔍 ПРОВЕРКА ПОДКАРТЫ GAMES')
            from django.test import Client
            client = Client()
            response = client.get(url_path)

        self.stdout.write(f'   Статус: {response.status_code}')

        try:
            content = response.text

            if '<urlset' in content:
                root = ET.fromstring(content)
                ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                urls = root.findall('sitemap:url', ns)
                self.stdout.write(self.style.SUCCESS(f'   ✅ Найдено URL: {len(urls)}'))

                # Проверяем количество URL
                if len(urls) == 0:
                    self.stdout.write(self.style.ERROR('   ❌ НЕТ URL!'))
                elif len(urls) < 1000:
                    self.stdout.write(self.style.WARNING(f'   ⚠️ МАЛО URL: {len(urls)} (ожидается 1000)'))
                elif len(urls) == 1000:
                    self.stdout.write(self.style.SUCCESS('   ✅ Правильное количество URL: 1000'))
                else:
                    self.stdout.write(self.style.WARNING(f'   ⚠️ МНОГО URL: {len(urls)} (ожидается 1000)'))

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

    def check_similar_sitemap(self, domain, protocol):
        """
        Проверяет подкарту similar.

        Аргументы:
            domain: Домен или None для локального сервера
            protocol: Протокол (http/https)
        """
        url_path = '/sitemap-similar.xml'

        if domain:
            full_url = f'{protocol}://{domain}{url_path}'
            self.stdout.write(f'\n🔍 ПРОВЕРКА ПОДКАРТЫ SIMILAR: {full_url}')
            response = requests.get(full_url, timeout=30)
        else:
            self.stdout.write('\n🔍 ПРОВЕРКА ПОДКАРТЫ SIMILAR')
            from django.test import Client
            client = Client()
            response = client.get(url_path)

        self.stdout.write(f'   Статус: {response.status_code}')

        try:
            content = response.text

            if '<urlset' in content:
                root = ET.fromstring(content)
                ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                urls = root.findall('sitemap:url', ns)
                self.stdout.write(self.style.SUCCESS(f'   ✅ Найдено URL: {len(urls)}'))

                # Проверяем количество URL
                if len(urls) == 0:
                    self.stdout.write(self.style.ERROR('   ❌ НЕТ URL!'))
                elif len(urls) < 1000:
                    self.stdout.write(self.style.WARNING(f'   ⚠️ МАЛО URL: {len(urls)} (ожидается 1000)'))
                elif len(urls) == 1000:
                    self.stdout.write(self.style.SUCCESS('   ✅ Правильное количество URL: 1000'))
                else:
                    self.stdout.write(self.style.WARNING(f'   ⚠️ МНОГО URL: {len(urls)} (ожидается 1000)'))
            else:
                self.stdout.write(self.style.ERROR('   ❌ НЕ URLSET'))

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

    def check_pagination(self, total_pages, domain, protocol):
        """
        Проверяет пагинацию sitemap.

        Аргументы:
            total_pages: Общее количество страниц для проверки
            domain: Домен или None для локального сервера
            protocol: Протокол (http/https)
        """
        self.stdout.write('\n📄 ПРОВЕРКА ПАГИНАЦИИ')
        self.stdout.write('   Проверяем страницы sitemap-games.xml...')

        pages_to_check = [1, 2]
        if total_pages > 2:
            pages_to_check.append(total_pages)

        for page in pages_to_check:
            url_path = f'/sitemap-games.xml?page={page}'

            if domain:
                full_url = f'{protocol}://{domain}{url_path}'
                response = requests.get(full_url, timeout=30)
            else:
                from django.test import Client
                client = Client()
                response = client.get(url_path)

            if response.status_code == 200:
                try:
                    content = response.text
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

    def check_page_counts(self, total_pages, domain, protocol):
        """
        Проверяет количество URL на каждой странице sitemap.
        Должно быть 1000 URL на страницу (кроме последней).

        Аргументы:
            total_pages: Общее количество страниц
            domain: Домен или None для локального сервера
            protocol: Протокол (http/https)
        """
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('🔍 ПРОВЕРКА КОЛИЧЕСТВА URL НА СТРАНИЦАХ')
        self.stdout.write('=' * 60)

        games_per_page = 1000
        game_count = Game.objects.count()

        # Проверяем страницы games
        self.stdout.write('\n📄 SITEMAP-GAMES:')
        self.stdout.write('-' * 40)

        errors = []
        warning_pages = []

        for page in range(1, total_pages + 1):
            url_path = f'/sitemap-games.xml?page={page}'

            if domain:
                full_url = f'{protocol}://{domain}{url_path}'
                response = requests.get(full_url, timeout=30)
            else:
                from django.test import Client
                client = Client()
                response = client.get(url_path)

            if response.status_code == 200:
                try:
                    content = response.text
                    if '<urlset' in content:
                        root = ET.fromstring(content)
                        ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                        urls = root.findall('sitemap:url', ns)
                        url_count = len(urls)

                        # Проверяем количество
                        expected = games_per_page
                        if page == total_pages:
                            # Последняя страница может содержать меньше
                            expected = game_count - (total_pages - 1) * games_per_page
                            if expected <= 0:
                                expected = games_per_page

                        if url_count == expected:
                            if page <= 5 or page == total_pages:
                                self.stdout.write(self.style.SUCCESS(f'   Страница {page:2d}: {url_count:4d} URL ✅'))
                        elif url_count == 0:
                            self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: {url_count:4d} URL ❌ НЕТ URL!'))
                            errors.append(page)
                        elif url_count < expected:
                            self.stdout.write(self.style.WARNING(
                                f'   Страница {page:2d}: {url_count:4d} URL ⚠️ МАЛО (ожидается {expected})'))
                            warning_pages.append(page)
                        else:
                            self.stdout.write(self.style.WARNING(
                                f'   Страница {page:2d}: {url_count:4d} URL ⚠️ МНОГО (ожидается {expected})'))
                            warning_pages.append(page)
                    else:
                        self.stdout.write(self.style.ERROR(f'   Страница {page}: НЕ URLSET'))
                        errors.append(page)
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'   Страница {page}: ОШИБКА: {e}'))
                    errors.append(page)
            else:
                self.stdout.write(self.style.ERROR(f'   Страница {page}: СТАТУС {response.status_code}'))
                errors.append(page)

        # Проверяем страницы similar (только первые 5 и последнюю для краткости)
        self.stdout.write('\n📄 SITEMAP-SIMILAR:')
        self.stdout.write('-' * 40)

        similar_pages_to_check = [1, 2, 3, 4, 5]
        if total_pages > 5:
            similar_pages_to_check.append(total_pages)

        for page in similar_pages_to_check:
            url_path = f'/sitemap-similar.xml?page={page}'

            if domain:
                full_url = f'{protocol}://{domain}{url_path}'
                response = requests.get(full_url, timeout=30)
            else:
                from django.test import Client
                client = Client()
                response = client.get(url_path)

            if response.status_code == 200:
                try:
                    content = response.text
                    if '<urlset' in content:
                        root = ET.fromstring(content)
                        ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                        urls = root.findall('sitemap:url', ns)
                        url_count = len(urls)

                        expected = games_per_page
                        if page == total_pages:
                            expected = game_count - (total_pages - 1) * games_per_page
                            if expected <= 0:
                                expected = games_per_page

                        if url_count == expected:
                            self.stdout.write(self.style.SUCCESS(f'   Страница {page:2d}: {url_count:4d} URL ✅'))
                        elif url_count == 0:
                            self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: {url_count:4d} URL ❌ НЕТ URL!'))
                        else:
                            self.stdout.write(self.style.WARNING(
                                f'   Страница {page:2d}: {url_count:4d} URL ⚠️ (ожидается {expected})'))
                    else:
                        self.stdout.write(self.style.ERROR(f'   Страница {page}: НЕ URLSET'))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'   Страница {page}: ОШИБКА: {e}'))
            else:
                self.stdout.write(self.style.ERROR(f'   Страница {page}: СТАТУС {response.status_code}'))

        # Итог проверки
        self.stdout.write('\n' + '=' * 60)
        if errors:
            self.stdout.write(self.style.ERROR(f'❌ Найдено {len(errors)} страниц с ошибками: {errors}'))
        elif warning_pages:
            self.stdout.write(self.style.WARNING(
                f'⚠️ Найдено {len(warning_pages)} страниц с неправильным количеством URL: {warning_pages}'))
        else:
            self.stdout.write(self.style.SUCCESS('✅ Все страницы содержат правильное количество URL (1000)'))
        self.stdout.write('=' * 60)

    def print_summary(self, domain, protocol):
        """
        Выводит итоговую информацию о состоянии sitemap.

        Аргументы:
            domain: Домен или None для локального сервера
            protocol: Протокол (http/https)
        """
        url_path = '/sitemap.xml'

        if domain:
            full_url = f'{protocol}://{domain}{url_path}'
            response = requests.get(full_url, timeout=30)
        else:
            from django.test import Client
            client = Client()
            response = client.get(url_path)

        self.stdout.write('\n' + '=' * 60)

        content = response.text

        if '<sitemapindex' in content:
            self.stdout.write(self.style.SUCCESS('✅ SITEMAP РАБОТАЕТ КОРРЕКТНО'))

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