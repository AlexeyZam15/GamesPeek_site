from django import template
from django.core.cache import cache

register = template.Library()


@register.simple_tag
def get_cached_count(model_name):
    """Кэшированный счетчик"""
    cache_key = f'count_{model_name}'
    count = cache.get(cache_key)
    if count is None:
        from ..models import Game, Genre, Keyword, Platform
        if model_name == 'games':
            count = Game.objects.count()
        elif model_name == 'genres':
            count = Genre.objects.count()
        elif model_name == 'keywords':
            count = Keyword.objects.filter(cached_usage_count__gt=0).count()
        elif model_name == 'platforms':
            count = Platform.objects.annotate(gc=Count('game')).filter(gc__gt=0).count()
        else:
            count = 0
        cache.set(cache_key, count, 300)  # 5 минут
    return count