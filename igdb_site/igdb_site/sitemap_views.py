from django.contrib.sitemaps import views as sitemap_views
from django.http import HttpResponse
from django.core.cache import cache
import hashlib


def sitemap_without_noindex(request, sitemaps, section=None, template_name='sitemap.xml',
                            content_type='application/xml'):
    """
    Кастомная view для sitemap без заголовка X-Robots-Tag noindex.
    """
    # Если section не передан - это запрос индекса
    if section is None:
        # Генерируем индекс через стандартную view
        response = sitemap_views.index(request, sitemaps, template_name, content_type)
    else:
        # Генерируем конкретную подкарту
        response = sitemap_views.sitemap(request, sitemaps, section, template_name, content_type)

    # Удаляем noindex заголовок
    if 'X-Robots-Tag' in response:
        del response['X-Robots-Tag']

    return response