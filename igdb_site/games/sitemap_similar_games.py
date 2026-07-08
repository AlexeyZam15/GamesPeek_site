from django.contrib.sitemaps import Sitemap
from django.core.paginator import Paginator
from .models import Game


class SimilarGamesSitemap(Sitemap):
    """
    Отдельный sitemap для страниц поиска похожих игр с пагинацией.
    Разбивает на части по 1000 записей.
    """
    changefreq = "weekly"
    priority = 0.6

    def items(self):
        """
        Возвращает QuerySet со всеми ID игр.
        """
        return Game.objects.all().only('id').order_by('id')

    def location(self, item):
        """
        Генерирует URL для поиска похожих игр.
        """
        return f'/games/?find_similar=1&source_game={item.id}'

    @property
    def paginator(self):
        """
        Создает пагинатор для разбивки на несколько sitemap файлов.
        """
        if not hasattr(self, '_paginator'):
            self._paginator = Paginator(self.items(), 1000)
        return self._paginator