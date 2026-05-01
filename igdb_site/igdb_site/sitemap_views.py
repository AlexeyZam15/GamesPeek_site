from django.contrib.sitemaps import views as sitemap_views
from django.http import HttpResponse


def sitemap_without_noindex(request, sitemaps, section=None, template_name='sitemap.xml',
                            content_type='application/xml'):
    """
    Кастомная view для sitemap без заголовка X-Robots-Tag noindex.
    """
    response = sitemap_views.sitemap(request, sitemaps, section, template_name, content_type)
    # Удаляем проблемный заголовок
    if 'X-Robots-Tag' in response:
        del response['X-Robots-Tag']
    return response