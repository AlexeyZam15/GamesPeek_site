from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static
from django.views.static import serve
import os

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('games.urls')),
    path('sitemap.xml', include('django.contrib.sitemaps.urls')),
]

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
