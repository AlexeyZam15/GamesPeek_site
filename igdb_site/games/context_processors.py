# games\context_processors.py
from django.conf import settings


def debug_context(request):
    """Добавляет переменную debug в контекст шаблонов."""
    return {
        'debug': settings.DEBUG,
    }