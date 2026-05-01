from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.views.static import serve
from django.http import HttpResponse
from games.sitemap import GameSitemap, StaticViewSitemap
from games.sitemap_similar_games import SimilarGamesSitemap
from .sitemap_views import sitemap_without_noindex
import os

sitemaps = {
    'games': GameSitemap,
    'static': StaticViewSitemap,
}

sitemaps_similar = {
    'similar_games': SimilarGamesSitemap,
}


def serve_indexnow_key(request, key):
    """Serve IndexNow key file for Bing/Yandex verification."""
    key_file_path = os.path.join(settings.BASE_DIR, f'{key}.txt')
    if os.path.exists(key_file_path):
        with open(key_file_path, 'r') as f:
            content = f.read()
        return HttpResponse(content, content_type='text/plain')
    return HttpResponse(status=404)


# Get IndexNow key from environment variable
INDEXNOW_KEY = os.getenv('INDEXNOW_KEY', '')

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('games.urls')),
    path('sitemap.xml', sitemap_without_noindex, {'sitemaps': sitemaps}, name='django.contrib.sitemaps.views.sitemap'),
    path('sitemap_similar_games.xml', sitemap_without_noindex, {'sitemaps': sitemaps_similar},
         name='sitemap_similar_games'),
]

# Add IndexNow key route only if key is configured
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