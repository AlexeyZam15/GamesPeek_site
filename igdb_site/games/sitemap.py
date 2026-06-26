from django.contrib.sitemaps import Sitemap
from django.urls import reverse
from .models import Game


class GameSitemap(Sitemap):
    """
    Карта сайта для страниц игр.
    Генерирует URL для детальных страниц каждой игры.
    """
    changefreq = "weekly"
    priority = 0.8
    protocol = 'https'  # Явно указываем как строку, а не метод

    def items(self):
        """
        Возвращает все игры для включения в sitemap.
        Оптимизировано: выбираем только id для уменьшения нагрузки на БД.
        """
        return Game.objects.all().only('id')

    def location(self, item):
        """
        Генерирует абсолютный URL для страницы игры.
        Использует reverse() для получения относительного пути,
        protocol='https' добавляется автоматически базовым классом.
        """
        return reverse('game_detail', kwargs={'pk': item.id})


class StaticViewSitemap(Sitemap):
    """
    Карта сайта для статических страниц.
    Включает главную страницу и список игр.
    """
    changefreq = "daily"
    priority = 0.5
    protocol = 'https'  # Явно указываем как строку, а не метод

    def items(self):
        """
        Возвращает имена статических страниц для включения в sitemap.
        """
        return ['home', 'game_list']

    def location(self, item):
        """
        Генерирует URL для статической страницы.
        Для 'game_list' использует прямой путь, для остальных - reverse().
        """
        if item == 'game_list':
            return '/games/'
        return reverse(item)