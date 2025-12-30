"""
Middleware для оптимизации производительности.
"""

import time
import logging
from django.core.cache import cache
from django.utils.deprecation import MiddlewareMixin

logger = logging.getLogger('performance')


class PerformanceMiddleware(MiddlewareMixin):
    """Middleware для мониторинга производительности."""

    def process_request(self, request):
        request.start_time = time.time()
        return None

    def process_response(self, request, response):
        if hasattr(request, 'start_time'):
            duration = time.time() - request.start_time

            # Логируем медленные запросы к главной странице
            if request.path == '/' and duration > 0.5:
                logger.warning(
                    f"Slow home page: {duration:.3f}s"
                )

            # Добавляем заголовок для отладки
            if response.status_code == 200:
                response['X-Response-Time'] = f"{duration:.3f}s"

                # Для главной страницы добавляем дополнительные заголовки
                if request.path == '/':
                    from django.db import connection
                    query_count = len(connection.queries)
                    response['X-DB-Queries'] = str(query_count)

        return response


class CacheControlMiddleware(MiddlewareMixin):
    """Middleware для управления кэшированием."""

    CACHEABLE_PATHS = [
        '/games/list/',
        '/games/platform/',
        '/games/similar/',
        '/games/search/',
    ]

    def process_response(self, request, response):
        if response.status_code == 200 and request.method == 'GET':
            # Для статических страниц устанавливаем кэширование
            for path in self.CACHEABLE_PATHS:
                if request.path.startswith(path):
                    response['Cache-Control'] = 'public, max-age=300'  # 5 минут
                    break

        return response


class QueryCountMiddleware(MiddlewareMixin):
    """Middleware для подсчета SQL запросов."""

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        from django.db import connection

        # Сброс счетчика
        connection.queries_log.clear()

        response = self.get_response(request)

        # Подсчет запросов
        query_count = len(connection.queries)

        if query_count > 20:  # Больше 20 запросов - потенциальная проблема
            logger.warning(
                f"High query count: {request.path} - {query_count} queries"
            )

        response['X-Query-Count'] = str(query_count)

        return response