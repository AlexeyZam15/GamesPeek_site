from django import template
from django.urls import reverse
from urllib.parse import urlencode

register = template.Library()


@register.simple_tag
def get_find_similar_url(game):
    """Генерирует компактный URL для поиска похожих игр"""
    genre_ids = [str(genre.id) for genre in game.genres.all()]
    keyword_ids = [str(keyword.id) for keyword in game.keywords.all()]

    params = {
        'find_similar': '1',
        'g': ','.join(genre_ids),
        'k': ','.join(keyword_ids)
    }

    return f"{reverse('game_list')}?{urlencode(params)}"