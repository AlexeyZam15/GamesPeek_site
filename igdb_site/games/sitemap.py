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
    changefreq = "daily"
    priority = 0.5

    def items(self):
        return ['home', 'game_list']

    def location(self, item):
        return reverse(item)
