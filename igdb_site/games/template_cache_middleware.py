"""
Middleware для кэширования шаблонов в production.
"""

from django.middleware.common import MiddlewareMixin
from django.template import engines


class TemplateCacheMiddleware(MiddlewareMixin):
    """Middleware для управления кэшированием шаблонов."""

    def process_request(self, request):
        # В production включаем кэширование шаблонов
        from django.conf import settings

        if not settings.DEBUG:
            # Кэшируем шаблоны в памяти
            for engine in engines.all():
                if hasattr(engine, 'engine'):
                    engine.engine.template_cache = {}

        return None