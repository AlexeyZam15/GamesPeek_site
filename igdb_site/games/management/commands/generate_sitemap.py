"""
Команда для генерации статических sitemap файлов.

Генерирует sitemap с правильной пагинацией:
- games: 45 страниц (44 по 1000 URL + 1 страница с 756 URL)
- similar: 45 страниц (44 по 1000 URL + 1 страница с 756 URL)

Запуск: python manage.py generate_sitemap
"""

import os
from django.core.management.base import BaseCommand
from django.template import loader
from django.utils import timezone
from django.conf import settings
from games.models import Game
from games.sitemap import GameSitemap
from games.sitemap_similar_games import SimilarGamesSitemap


class Command(BaseCommand):
    """
    Генерирует статические файлы sitemap с правильной пагинацией.

    Создаёт:
    - sitemap.xml - индексный файл со ссылками на все подкарты
    - sitemap-games.xml, sitemap-games.xml?page=2, ... (45 страниц)
    - sitemap-similar.xml, sitemap-similar.xml?page=2, ... (45 страниц)

    Сохраняет их в STATIC_ROOT/sitemaps/ для прямой отдачи сервером.
    """

    help = 'Генерирует статические sitemap файлы с правильной пагинацией'

    def handle(self, *args, **options):
        self.stdout.write('[' + timezone.now().strftime('%Y-%m-%d %H:%M:%S') + '] Начало генерации sitemap...')

        # Убеждаемся, что директория для sitemap существует
        sitemap_dir = os.path.join(settings.STATIC_ROOT, 'sitemaps')
        os.makedirs(sitemap_dir, exist_ok=True)

        # Получаем данные для генерации
        game_count = Game.objects.count()
        games_per_page = 1000
        total_pages = (game_count + games_per_page - 1) // games_per_page

        self.stdout.write(f'📊 Всего игр в БД: {game_count}')
        self.stdout.write(f'📄 Страниц в sitemap: {total_pages} (по {games_per_page} игр)')

        # 1. Генерация всех страниц sitemap-games
        self.stdout.write('\n📄 Генерация sitemap-games...')
        self._generate_games_sitemap_pages(sitemap_dir, total_pages, games_per_page, game_count)

        # 2. Генерация всех страниц sitemap-similar
        self.stdout.write('\n📄 Генерация sitemap-similar...')
        self._generate_similar_sitemap_pages(sitemap_dir, total_pages, games_per_page, game_count)

        # 3. Генерация индексного sitemap.xml
        self.stdout.write('\n📄 Генерация индексного sitemap.xml...')
        self._generate_sitemap_index(sitemap_dir, total_pages)

        self.stdout.write(
            self.style.SUCCESS(
                '\n✅ [' + timezone.now().strftime('%Y-%m-%d %H:%M:%S') + '] Генерация завершена успешно!')
        )
        self.stdout.write(f'📁 Файлы сохранены в: {sitemap_dir}')

    def _generate_games_sitemap_pages(self, sitemap_dir, total_pages, games_per_page, game_count):
        """
        Генерирует все страницы sitemap-games.xml.

        Аргументы:
            sitemap_dir: Директория для сохранения файлов
            total_pages: Общее количество страниц
            games_per_page: Количество игр на странице
            game_count: Общее количество игр
        """
        sitemap = GameSitemap()
        all_items = sitemap.items()

        for page_num in range(1, total_pages + 1):
            # Определяем элементы для текущей страницы
            start_idx = (page_num - 1) * games_per_page
            end_idx = min(start_idx + games_per_page, len(all_items))
            page_items = all_items[start_idx:end_idx]

            # Генерируем URL для каждого элемента
            urls = []
            for item in page_items:
                location = sitemap.location(item)
                urls.append({
                    'location': f'https://gamespeek.dpdns.org{location}',
                    'changefreq': sitemap.changefreq,
                    'priority': sitemap.priority,
                })

            # Формируем имя файла
            if page_num == 1:
                filename = 'sitemap-games.xml'
            else:
                filename = f'sitemap-games.xml?page={page_num}'

            # Сохраняем файл
            file_path = os.path.join(sitemap_dir, filename.replace('?', '_'))
            self._save_sitemap_page(file_path, urls)

            self.stdout.write(f'  Страница {page_num}: {len(urls)} URL → {filename}')

    def _generate_similar_sitemap_pages(self, sitemap_dir, total_pages, games_per_page, game_count):
        """
        Генерирует все страницы sitemap-similar.xml.

        Аргументы:
            sitemap_dir: Директория для сохранения файлов
            total_pages: Общее количество страниц
            games_per_page: Количество игр на странице
            game_count: Общее количество игр
        """
        sitemap = SimilarGamesSitemap()
        all_items = sitemap.items()

        for page_num in range(1, total_pages + 1):
            # Определяем элементы для текущей страницы
            start_idx = (page_num - 1) * games_per_page
            end_idx = min(start_idx + games_per_page, len(all_items))
            page_items = all_items[start_idx:end_idx]

            # Генерируем URL для каждого элемента
            urls = []
            for item in page_items:
                location = sitemap.location(item)
                urls.append({
                    'location': f'https://gamespeek.dpdns.org{location}',
                    'changefreq': sitemap.changefreq,
                    'priority': sitemap.priority,
                })

            # Формируем имя файла
            if page_num == 1:
                filename = 'sitemap-similar.xml'
            else:
                filename = f'sitemap-similar.xml?page={page_num}'

            # Сохраняем файл
            file_path = os.path.join(sitemap_dir, filename.replace('?', '_'))
            self._save_sitemap_page(file_path, urls)

            self.stdout.write(f'  Страница {page_num}: {len(urls)} URL → {filename}')

    def _save_sitemap_page(self, file_path, urls):
        """
        Сохраняет одну страницу sitemap.

        Аргументы:
            file_path: Путь для сохранения файла
            urls: Список URL для страницы
        """
        template = loader.get_template('sitemap_template.xml')
        content = template.render({'urlset': urls})

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

    def _generate_sitemap_index(self, sitemap_dir, total_pages):
        """
        Генерирует индексный файл sitemap.xml со ссылками на все подкарты.

        Аргументы:
            sitemap_dir: Директория для сохранения файла
            total_pages: Общее количество страниц
        """
        sitemap_urls = []
        protocol = 'https'
        domain = 'gamespeek.dpdns.org'

        # Добавляем ссылки на sitemap-games
        for page_num in range(1, total_pages + 1):
            if page_num == 1:
                location = f'/sitemaps/sitemap-games.xml'
            else:
                location = f'/sitemaps/sitemap-games.xml?page={page_num}'
            sitemap_urls.append({
                'location': f'{protocol}://{domain}{location}'
            })

        # Добавляем ссылки на sitemap-similar
        for page_num in range(1, total_pages + 1):
            if page_num == 1:
                location = f'/sitemaps/sitemap-similar.xml'
            else:
                location = f'/sitemaps/sitemap-similar.xml?page={page_num}'
            sitemap_urls.append({
                'location': f'{protocol}://{domain}{location}'
            })

        # Сохраняем индексный файл
        template = loader.get_template('sitemap_index.xml')
        content = template.render({'sitemaps': sitemap_urls})

        file_path = os.path.join(settings.STATIC_ROOT, 'sitemap.xml')
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(content)

        self.stdout.write(f'  Сохранён: {file_path} ({len(sitemap_urls)} подкарт)')