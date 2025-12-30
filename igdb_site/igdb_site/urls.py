from django.contrib import admin
from django.urls import path, include
from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('', include('games.urls')),  # Добавляем URLs из приложения games
]

# ============================================
# ВСЁ НИЖЕ ТОЛЬКО ДЛЯ РЕЖИМА РАЗРАБОТКИ (DEBUG=True)
# ============================================

if settings.DEBUG:
    # 1. Добавляем маршруты для Debug Toolbar (ПЕРВЫМИ!)
    try:
        import debug_toolbar

        # Важно: добавляем ПЕРВЫМИ, чтобы перехватывать все запросы
        urlpatterns = [
                          path('__debug__/', include(debug_toolbar.urls)),
                      ] + urlpatterns
    except ImportError:
        # Если debug_toolbar не установлен, выводим предупреждение
        print("⚠️ Django Debug Toolbar не установлен. Установите: pip install django-debug-toolbar")

    # 2. Добавляем статические файлы (если нужно)
    urlpatterns += static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)

    # 3. Добавляем медиа файлы (если в settings.py настроено MEDIA_URL и MEDIA_ROOT)
    if hasattr(settings, 'MEDIA_URL') and hasattr(settings, 'MEDIA_ROOT'):
        urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
    else:
        print("ℹ️ MEDIA_URL/MEDIA_ROOT не настроены. Медиафайлы не будут обслуживаться.")