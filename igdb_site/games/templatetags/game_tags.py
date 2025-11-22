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

    # Получаем жанры и ключевые слова игры
    genres = game.genres.all()
    keywords = game.keywords.all()

    # Берем только популярные ключевые слова (например, с usage_count > 5)
    popular_keywords = [k for k in keywords if k.usage_count > 5][:10]

    params = {
        'find_similar': '1',
        'source_game': game.id,  # ← ДОБАВИТЬ ИСХОДНУЮ ИГРУ
    }

    # Добавляем жанры
    if genres:
        params['g'] = ','.join(str(g.id) for g in genres)

    # Добавляем ключевые слова
    if popular_keywords:
        params['k'] = ','.join(str(k.id) for k in popular_keywords)

    base_url = reverse('game_list')
    return f"{base_url}?{urlencode(params)}"