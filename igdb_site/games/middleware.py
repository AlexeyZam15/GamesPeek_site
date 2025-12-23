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

        # Игнорируем статику и медиа
        if request.path.startswith(('/static/', '/media/', '/favicon.ico')):
            return None

        return None

    def process_response(self, request, response):
        if hasattr(request, 'start_time'):
            duration = time.time() - request.start_time

            # Логируем медленные запросы
            if duration > 1.0:  # Больше 1 секунды
                logger.warning(
                    f"Slow request: {request.method} {request.path} - "
                    f"{duration:.3f}s, User: {request.user}, "
                    f"Params: {dict(request.GET)}"
                )

            # Добавляем заголовок для отладки
            if response.status_code == 200:
                response['X-Response-Time'] = f"{duration:.3f}s"
                response['X-Cache-Hit'] = 'True' if getattr(request, 'cache_hit', False) else 'False'

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