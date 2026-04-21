from django.contrib.sitemaps import Sitemap
from django.urls import reverse
from .models import Game


class GameSitemap(Sitemap):
    changefreq = "weekly"
    priority = 0.8

    def items(self):
        return Game.objects.all()

    def lastmod(self, obj):
        return obj.updated_at


class StaticViewSitemap(Sitemap):
    changefreq = "daily"
    priority = 0.5

    def items(self):
        return ['home', 'about', 'contact']

    def location(self, item):
        return reverse(item)