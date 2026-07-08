"""
Команда для проверки и диагностики sitemap.

Исправленная версия, которая корректно обрабатывает редиректы на VPS.
"""

from django.core.management.base import BaseCommand
from games.models import Game
from django.test import Client
import xml.etree.ElementTree as ET
import requests
import os


class Command(BaseCommand):
    help = 'Проверяет sitemap и выводит диагностическую информацию'

    def add_arguments(self, parser):
        parser.add_argument(
            '--check-counts',
            action='store_true',
            help='Проверяет количество URL на каждой странице sitemap'
        )
        parser.add_argument(
            '--domain',
            type=str,
            help='Проверяет sitemap на реальном домене'
        )
        parser.add_argument(
            '--protocol',
            type=str,
            default='https',
            help='Протокол (http/https)'
        )

    def get_response(self, url_path, domain=None, protocol='https', follow=True):
        """
        Получает ответ на запрос.
        Если указан domain - использует requests.
        Иначе использует Django Test Client с поддержкой редиректов.
        """
        if domain:
            full_url = f'{protocol}://{domain}{url_path}'
            try:
                return requests.get(full_url, timeout=30)
            except requests.exceptions.RequestException as e:
                self.stdout.write(self.style.ERROR(f'Ошибка: {e}'))
                return None
        else:
            client = Client()
            return client.get(url_path, follow=follow)

    def handle(self, *args, **options):
        domain = options.get('domain')
        protocol = options.get('protocol', 'https')
        check_counts = options.get('check_counts', False)

        self.stdout.write(self.style.SUCCESS('=' * 60))
        if domain:
            self.stdout.write(self.style.SUCCESS(f'ПРОВЕРКА SITEMAP НА ДОМЕНЕ: {domain}'))
        else:
            self.stdout.write(self.style.SUCCESS('ПРОВЕРКА SITEMAP (ЛОКАЛЬНО)'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        # Количество игр
        game_count = Game.objects.count()
        self.stdout.write(f'\n📊 Всего игр в БД: {game_count}')
        total_pages = (game_count + 999) // 1000
        self.stdout.write(f'📄 Страниц в sitemap: {total_pages} (по 1000 игр)')

        # Проверка главного sitemap
        self.check_main_sitemap(domain, protocol)

        # Проверка подкарты games (первая страница)
        self.check_games_sitemap(domain, protocol)

        # Проверка подкарты similar
        self.check_similar_sitemap(domain, protocol)

        # Проверка количества URL на каждой странице
        if check_counts:
            self.check_page_counts(total_pages, domain, protocol)

        self.print_summary(domain, protocol)

    def check_main_sitemap(self, domain, protocol):
        """Проверяет главный sitemap.xml."""
        self.stdout.write('\n🔍 ПРОВЕРКА ГЛАВНОГО SITEMAP (sitemap.xml)')

        response = self.get_response('/sitemap.xml', domain, protocol)
        if response is None:
            return

        self.stdout.write(f'   Статус: {response.status_code}')

        if response.status_code == 301 or response.status_code == 302:
            redirect_url = response.headers.get('Location', 'не указан')
            self.stdout.write(self.style.WARNING(f'   ⚠️ РЕДИРЕКТ: {redirect_url}'))
            # Если это редирект и мы используем test client, он уже следовал за ним
            # Проверяем итоговый статус
            if hasattr(response, 'redirect_chain') and response.redirect_chain:
                final_status = response.status_code
                self.stdout.write(f'   Статус после редиректа: {final_status}')

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
                # Показываем первые 200 символов для диагностики
                self.stdout.write(f'   Содержимое: {content[:200]}')
        except UnicodeDecodeError:
            self.stdout.write(self.style.ERROR('   ❌ ОШИБКА ДЕКОДИРОВАНИЯ'))

    def check_games_sitemap(self, domain, protocol):
        """Проверяет подкарту games."""
        self.stdout.write('\n🔍 ПРОВЕРКА ПОДКАРТЫ GAMES')

        response = self.get_response('/sitemap-games.xml', domain, protocol)
        if response is None:
            return

        self.stdout.write(f'   Статус: {response.status_code}')
        self.check_urlset_content(response)

    def check_similar_sitemap(self, domain, protocol):
        """Проверяет подкарту similar."""
        self.stdout.write('\n🔍 ПРОВЕРКА ПОДКАРТЫ SIMILAR')

        response = self.get_response('/sitemap-similar.xml', domain, protocol)
        if response is None:
            return

        self.stdout.write(f'   Статус: {response.status_code}')
        self.check_urlset_content(response)

    def check_urlset_content(self, response):
        """Проверяет содержимое urlset."""
        if response.status_code != 200:
            self.stdout.write(self.style.ERROR(f'   ❌ ОШИБКА: статус {response.status_code}'))
            return

        try:
            content = response.content.decode('utf-8')
            if '<urlset' in content:
                root = ET.fromstring(content)
                ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                urls = root.findall('sitemap:url', ns)
                url_count = len(urls)

                if url_count > 0:
                    self.stdout.write(self.style.SUCCESS(f'   ✅ Найдено URL: {url_count}'))

                    # Проверяем, абсолютные ли URL
                    sample_url = root.find('sitemap:url/sitemap:loc', ns)
                    if sample_url is not None and sample_url.text:
                        if sample_url.text.startswith(('http://', 'https://')):
                            self.stdout.write(self.style.SUCCESS('   ✅ URL АБСОЛЮТНЫЕ'))
                        else:
                            self.stdout.write(self.style.WARNING(f'   ⚠️ ОТНОСИТЕЛЬНЫЙ URL: {sample_url.text[:50]}...'))
                else:
                    self.stdout.write(self.style.ERROR('   ❌ НЕТ URL!'))
            else:
                self.stdout.write(self.style.ERROR('   ❌ НЕ URLSET'))
                # Показываем первые 200 символов для диагностики
                self.stdout.write(f'   Содержимое: {content[:200]}')
        except ET.ParseError as e:
            self.stdout.write(self.style.ERROR(f'   ❌ ОШИБКА ПАРСИНГА: {e}'))
        except UnicodeDecodeError:
            self.stdout.write(self.style.ERROR('   ❌ ОШИБКА ДЕКОДИРОВАНИЯ'))

    def print_sitemap_index_info(self, content):
        """Выводит информацию о sitemap index."""
        try:
            root = ET.fromstring(content)
            ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            sitemaps = root.findall('sitemap:sitemap', ns)
            self.stdout.write(f'   📂 Найдено подкарт: {len(sitemaps)}')

            absolute_count = 0
            relative_count = 0
            for sitemap in sitemaps[:5]:
                loc = sitemap.find('sitemap:loc', ns)
                if loc is not None:
                    url_text = loc.text
                    if url_text.startswith(('http://', 'https://')):
                        absolute_count += 1
                    else:
                        relative_count += 1

            if len(sitemaps) > 5:
                self.stdout.write(f'   ... и еще {len(sitemaps) - 5} подкарт')

            if relative_count > 0:
                self.stdout.write(self.style.WARNING(f'   ⚠️ Найдено {relative_count} относительных URL'))
            if absolute_count > 0:
                self.stdout.write(self.style.SUCCESS(f'   ✅ Найдено {absolute_count} абсолютных URL'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

    def print_urlset_info(self, content):
        """Выводит информацию о urlset."""
        try:
            root = ET.fromstring(content)
            ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            urls = root.findall('sitemap:url', ns)
            self.stdout.write(f'   🔗 URL в sitemap: {len(urls)}')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

    def check_page_counts(self, total_pages, domain, protocol):
        """Проверяет количество URL на каждой странице."""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('🔍 ПРОВЕРКА КОЛИЧЕСТВА URL НА СТРАНИЦАХ')
        self.stdout.write('=' * 60)

        errors = 0
        self.stdout.write('\n📄 SITEMAP-GAMES:')
        self.stdout.write('-' * 40)

        for page in range(1, min(total_pages + 1, 46)):  # Проверяем первые 45 страниц
            url_path = f'/sitemap-games.xml?page={page}'
            response = self.get_response(url_path, domain, protocol, follow=True)

            if response is None:
                self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: ОШИБКА ❌'))
                errors += 1
                continue

            if response.status_code == 200:
                try:
                    content = response.content.decode('utf-8')
                    if '<urlset' in content:
                        root = ET.fromstring(content)
                        ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                        urls = root.findall('sitemap:url', ns)
                        url_count = len(urls)

                        if url_count == 1000 or (page == total_pages and url_count > 0):
                            self.stdout.write(self.style.SUCCESS(f'   Страница {page:2d}: {url_count:4d} URL ✅'))
                        elif url_count == 0:
                            self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: {url_count:4d} URL ❌'))
                            errors += 1
                        else:
                            self.stdout.write(self.style.WARNING(f'   Страница {page:2d}: {url_count:4d} URL ⚠️'))
                    else:
                        self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: НЕ URLSET ❌'))
                        errors += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: ОШИБКА: {e}'))
                    errors += 1
            else:
                self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: СТАТУС {response.status_code} ❌'))
                errors += 1

        # Проверяем similar страницы
        self.stdout.write('\n📄 SITEMAP-SIMILAR:')
        self.stdout.write('-' * 40)

        for page in range(1, min(total_pages + 1, 46)):
            url_path = f'/sitemap-similar.xml?page={page}'
            response = self.get_response(url_path, domain, protocol, follow=True)

            if response is None:
                self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: ОШИБКА ❌'))
                errors += 1
                continue

            if response.status_code == 200:
                try:
                    content = response.content.decode('utf-8')
                    if '<urlset' in content:
                        root = ET.fromstring(content)
                        ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                        urls = root.findall('sitemap:url', ns)
                        url_count = len(urls)

                        if url_count > 0:
                            self.stdout.write(self.style.SUCCESS(f'   Страница {page:2d}: {url_count:4d} URL ✅'))
                        elif url_count == 0:
                            self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: {url_count:4d} URL ❌'))
                            errors += 1
                        else:
                            self.stdout.write(self.style.WARNING(f'   Страница {page:2d}: {url_count:4d} URL ⚠️'))
                    else:
                        self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: НЕ URLSET ❌'))
                        errors += 1
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: ОШИБКА: {e}'))
                    errors += 1
            else:
                self.stdout.write(self.style.ERROR(f'   Страница {page:2d}: СТАТУС {response.status_code} ❌'))
                errors += 1

        self.stdout.write('\n' + '=' * 60)
        if errors > 0:
            self.stdout.write(self.style.ERROR(f'❌ Найдено {errors} страниц с ошибками'))
        else:
            self.stdout.write(self.style.SUCCESS('✅ Все страницы загружены корректно'))
        self.stdout.write('=' * 60)

    def print_summary(self, domain, protocol):
        """Выводит итоговую информацию."""
        self.stdout.write('\n' + '=' * 60)

        if domain:
            self.stdout.write(self.style.SUCCESS('✅ ПРОВЕРКА ЗАВЕРШЕНА'))
            self.stdout.write(f'   Домен: {domain}')
        else:
            self.stdout.write(self.style.SUCCESS('✅ ПРОВЕРКА ЗАВЕРШЕНА'))

        self.stdout.write('=' * 60)