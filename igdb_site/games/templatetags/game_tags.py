from django import template
from django.urls import reverse
from urllib.parse import urlencode
from django import template

register = template.Library()

@register.simple_tag
def get_find_similar_url(game):
    """
    Генерирует URL для поиска похожих игр с передачей исходной игры
    """
    from django.urls import reverse
    from urllib.parse import urlencode

    # Получаем ВСЕ жанры и ключевые слова игры (не только популярные)
    genres = game.genres.all()
    keywords = game.keywords.all()

    params = {
        'find_similar': '1',
        'source_game': game.id,  # Передаем ID исходной игры
    }

    # Добавляем ВСЕ жанры
    if genres:
        params['g'] = ','.join(str(g.id) for g in genres)

    # Добавляем ВСЕ ключевые слова (не только популярные)
    if keywords:
        params['k'] = ','.join(str(k.id) for k in keywords)

    base_url = reverse('game_list')
    return f"{base_url}?{urlencode(params)}"