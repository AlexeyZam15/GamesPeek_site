from django.contrib.sitemaps import Sitemap
from django.urls import reverse
from django.core.paginator import Paginator
from .models import Game


class GameSitemap(Sitemap):
    """
    Карта сайта для страниц игр с пагинацией.
    Разбивает все игры на части по 1000 записей для ускорения индексации.
    """
    changefreq = "weekly"
    priority = 0.8
    protocol = 'https'

    def items(self):
        """
        Возвращает все игры с оптимизацией запроса.
        Использует only('id') для уменьшения нагрузки на базу данных.
        """
        return Game.objects.all().only('id').order_by('id')

    def location(self, item):
        """
        Генерирует абсолютный URL для страницы игры.
        Использует reverse() для получения пути.
        """
        return reverse('game_detail', kwargs={'pk': item.id})

    @property
    def paginator(self):
        """
        Создает пагинатор для разбивки на несколько sitemap файлов.
        Каждый файл содержит не более 1000 записей.
        """
        if not hasattr(self, '_paginator'):
            self._paginator = Paginator(self.items(), 1000)
        return self._paginator