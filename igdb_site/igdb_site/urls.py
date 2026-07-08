from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.http import HttpResponse

from igdb_site.sitemap_views import sitemap_without_noindex

from games.sitemap import GameSitemap
from games.sitemap_similar_games import SimilarGamesSitemap
import os

sitemaps = {
    'games': GameSitemap,
    'similar': SimilarGamesSitemap,
}


def serve_static_sitemap(request, filename):
    """
    Отдаёт статический sitemap файл из STATIC_ROOT.
    Используется для прямой отдачи заранее сгенерированных sitemap.xml файлов.
    """
    file_path = os.path.join(settings.STATIC_ROOT, filename)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        return HttpResponse(content, content_type='application/xml')
    return HttpResponse(status=404)


def serve_indexnow_key(request, key):
    """
    Serve IndexNow key file for Bing/Yandex verification.
    Проверяет существование файла с ключом и отдаёт его содержимое.
    """
    key_file_path = os.path.join(settings.BASE_DIR, f'{key}.txt')
    if os.path.exists(key_file_path):
        with open(key_file_path, 'r') as f:
            content = f.read()
        return HttpResponse(content, content_type='text/plain')
    return HttpResponse(status=404)


def serve_robots_txt(request):
    """
    Serve robots.txt from static files directory.
    Отдаёт файл robots.txt из директории статических файлов.
    """
    robots_path = os.path.join(settings.STATIC_ROOT, 'robots.txt')
    if os.path.exists(robots_path):
        with open(robots_path, 'r') as f:
            content = f.read()
        return HttpResponse(content, content_type='text/plain')
    return HttpResponse(status=404)


INDEXNOW_KEY = os.getenv('INDEXNOW_KEY', '')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('games.urls')),

    # ГЛАВНЫЙ SITEMAP - ДЛЯ ИНДЕКСА
    # Теперь Django видит, что это индекс, потому что section не передан
    path('sitemap.xml', sitemap_without_noindex, {'sitemaps': sitemaps}, name='sitemap_index'),

    # ПОДКАРТЫ ДЛЯ КОНКРЕТНЫХ СЕКЦИЙ - С ПАГИНАЦИЕЙ
    path('sitemap-<section>.xml', sitemap_without_noindex, {'sitemaps': sitemaps},
         name='django.contrib.sitemaps.views.sitemap'),

    path('robots.txt', serve_robots_txt, name='robots_txt'),
]

if INDEXNOW_KEY:
    urlpatterns.insert(0, path(f'{INDEXNOW_KEY}.txt', lambda request: serve_indexnow_key(request, INDEXNOW_KEY)))

if settings.DEBUG or os.getenv('DESKTOP_MODE') == '1':
    from django.urls import re_path
    from django.views.static import serve as static_serve

    urlpatterns += [
        re_path(r'^static/(?P<path>.*)$', static_serve, {
            'document_root': settings.STATIC_ROOT,
            'show_indexes': True,
        }),
    ]

    if hasattr(settings, 'MEDIA_URL') and hasattr(settings, 'MEDIA_ROOT'):
        urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)

if settings.DEBUG and os.getenv('DESKTOP_MODE') != '1':
    try:
        import debug_toolbar

        urlpatterns = [
                          path('__debug__/', include(debug_toolbar.urls)),
                      ] + urlpatterns
    except ImportError:
        print("⚠️ Django Debug Toolbar не установлен. Установите: pip install django-debug-toolbar")