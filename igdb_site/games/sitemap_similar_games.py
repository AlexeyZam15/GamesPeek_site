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

    def __init__(self):
        """
        Инициализация с предварительным созданием списка ID.
        """
        super().__init__()
        # Создаём список ID один раз при инициализации
        self._items = list(Game.objects.all().order_by('id').values_list('id', flat=True))
        self._paginator = Paginator(self._items, 1000)

    def items(self):
        """
        Возвращает список ID всех игр для пагинации.
        """
        return self._items

    def location(self, item):
        """
        Генерирует URL для поиска похожих игр.

        Аргументы:
            item: ID игры (целое число)

        Возвращает:
            str: URL страницы поиска похожих игр
        """
        return f'/games/?find_similar=1&source_game={item}'

    def get_urls(self, page=1, site=None, protocol=None):
        """
        Переопределяет метод get_urls для правильной пагинации.

        Аргументы:
            page: Номер страницы
            site: Объект Site
            protocol: Протокол (http/https)

        Возвращает:
            list: Список словарей с URL для текущей страницы
        """
        try:
            page_obj = self._paginator.page(page)
            items = page_obj.object_list
        except Exception:
            items = []

        urls = []
        for item in items:
            loc = self.location(item)
            url_info = {
                'location': loc,
                'changefreq': self.changefreq,
                'priority': self.priority,
            }
            urls.append(url_info)

        return urls

    @property
    def paginator(self):
        """
        Возвращает пагинатор.
        """
        return self._paginator