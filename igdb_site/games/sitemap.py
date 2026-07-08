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
        Генерирует абсолютный URL для страницы игры.

        Аргументы:
            item: ID игры (целое число)

        Возвращает:
            str: URL страницы игры
        """
        return reverse('game_detail', kwargs={'pk': item})

    def get_urls(self, page=1, site=None, protocol=None):
        """
        Переопределяет метод get_urls для правильной пагинации.

        Этот метод используется Django Sitemap для генерации URL на странице.
        Мы используем наш Paginator вместо стандартного limit/offset.

        Аргументы:
            page: Номер страницы
            site: Объект Site
            protocol: Протокол (http/https)

        Возвращает:
            list: Список словарей с URL для текущей страницы
        """
        from django.contrib.sitemaps import Sitemap as BaseSitemap

        # Получаем элементы для текущей страницы через Paginator
        try:
            page_obj = self._paginator.page(page)
            items = page_obj.object_list
        except Exception:
            items = []

        # Генерируем URL для каждого элемента
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