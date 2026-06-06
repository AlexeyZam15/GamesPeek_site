import os
from django.core.management.base import BaseCommand
from django.contrib.sitemaps import GenericSitemap
from django.template import loader
from django.utils import timezone
from games.models import Game
from games.sitemap import GameSitemap, StaticViewSitemap
from games.sitemap_similar_games import SimilarGamesSitemap
from django.conf import settings


class Command(BaseCommand):
    """
    Генерирует статические файлы sitemap.xml и sitemap_similar_games.xml
    Сохраняет их в STATIC_ROOT для прямой отдачи сервером.
    Запуск: python manage.py generate_sitemap
    """

    help = 'Генерирует статические sitemap файлы в статическую директорию'

    def handle(self, *args, **options):
        self.stdout.write('[' + timezone.now().strftime('%Y-%m-%d %H:%M:%S') + '] Начало генерации sitemap...')

        # Убеждаемся, что директория для sitemap существует
        sitemap_dir = os.path.join(settings.STATIC_ROOT, 'sitemaps')
        os.makedirs(sitemap_dir, exist_ok=True)

        # 1. Генерация основного sitemap.xml
        self.stdout.write('Генерация sitemap.xml...')
        main_sitemap = self._generate_main_sitemap()
        main_file_path = os.path.join(settings.STATIC_ROOT, 'sitemap.xml')

        with open(main_file_path, 'w', encoding='utf-8') as f:
            f.write(main_sitemap)

        self.stdout.write(self.style.SUCCESS(f'  Сохранён: {main_file_path}'))

        # 2. Генерация sitemap_similar_games.xml
        self.stdout.write('Генерация sitemap_similar_games.xml...')
        similar_sitemap = self._generate_similar_sitemap()
        similar_file_path = os.path.join(settings.STATIC_ROOT, 'sitemap_similar_games.xml')

        with open(similar_file_path, 'w', encoding='utf-8') as f:
            f.write(similar_sitemap)

        self.stdout.write(self.style.SUCCESS(f'  Сохранён: {similar_file_path}'))

        self.stdout.write(
            self.style.SUCCESS('[' + timezone.now().strftime('%Y-%m-%d %H:%M:%S') + '] Генерация завершена успешно!'))

    def _generate_main_sitemap(self):
        """
        Генерирует основной sitemap с играми и статическими страницами.
        """
        # Получаем все URL из GameSitemap
        game_sitemap = GameSitemap()
        game_items = game_sitemap.items()
        game_urls = []

        for item in game_items:
            location = game_sitemap.location(item)
            game_urls.append({
                'location': f'https://gamespeek.dpdns.org{location}',
                'changefreq': game_sitemap.changefreq,
                'priority': game_sitemap.priority,
                'lastmod': None,
            })

        # Получаем все URL из StaticViewSitemap
        static_sitemap = StaticViewSitemap()
        static_items = static_sitemap.items()
        static_urls = []

        for item in static_items:
            location = static_sitemap.location(item)
            static_urls.append({
                'location': f'https://gamespeek.dpdns.org{location}',
                'changefreq': static_sitemap.changefreq,
                'priority': static_sitemap.priority,
                'lastmod': None,
            })

        # Объединяем все URL
        all_urls = game_urls + static_urls

        # Рендерим XML шаблон
        template = loader.get_template('sitemap_template.xml')
        return template.render({'urlset': all_urls})

    def _generate_similar_sitemap(self):
        """
        Генерирует sitemap для страниц поиска похожих игр.
        """
        similar_sitemap = SimilarGamesSitemap()
        items = similar_sitemap.items()
        urls = []

        for item in items:
            location = similar_sitemap.location(item)
            urls.append({
                'location': f'https://gamespeek.dpdns.org{location}',
                'changefreq': similar_sitemap.changefreq,
                'priority': similar_sitemap.priority,
                'lastmod': None,
            })

        template = loader.get_template('sitemap_template.xml')
        return template.render({'urlset': urls})