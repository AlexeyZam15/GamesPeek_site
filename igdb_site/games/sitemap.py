from django.contrib.sitemaps import Sitemap
from django.urls import reverse
from .models import Game


class GameSitemap(Sitemap):
    changefreq = "weekly"
    priority = 0.8

    def items(self):
        return Game.objects.all()

    def location(self, item):
        return reverse('game_detail', kwargs={'pk': item.id})


class StaticViewSitemap(Sitemap):
    """
    Карта сайта для статических страниц.
    """
    changefreq = "daily"
    priority = 0.5

    def items(self):
        # ДОБАВЛЯЕМ 'game_list' в список
        return ['home', 'game_list']

    def location(self, item):
        # Возвращаем правильные URL для каждого элемента
        if item == 'game_list':
            return '/games/'
        return reverse(item)