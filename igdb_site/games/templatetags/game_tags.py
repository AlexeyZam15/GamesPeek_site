from django import template
from django.urls import reverse
from urllib.parse import urlencode

register = template.Library()


@register.simple_tag
def get_find_similar_url(game):
    """
    Генерирует URL для поиска похожих игр с передачей ВСЕХ критериев исходной игры
    """
    from django.urls import reverse
    from urllib.parse import urlencode

    # Получаем ВСЕ критерии игры
    genres = game.genres.all()
    keywords = game.keywords.all()
    themes = game.themes.all()
    perspectives = game.player_perspectives.all()
    developers = game.developers.all()

    params = {
        'find_similar': '1',
        'source_game': game.id,  # Передаем ID исходной игры для сравнения
    }

    # Добавляем ВСЕ критерии
    if genres:
        params['g'] = ','.join(str(g.id) for g in genres)

    if keywords:
        params['k'] = ','.join(str(k.id) for k in keywords)

    if themes:
        params['t'] = ','.join(str(t.id) for t in themes)

    if perspectives:
        params['pp'] = ','.join(str(p.id) for p in perspectives)

    if developers:
        params['d'] = ','.join(str(d.id) for d in developers)

    base_url = reverse('game_list')
    return f"{base_url}?{urlencode(params)}"


@register.simple_tag
def get_comparison_url(source_game, target_game):
    """
    Генерирует URL для сравнения игры с передачей ВСЕХ критериев исходной игры
    """
    from django.urls import reverse
    from urllib.parse import urlencode

    # Получаем ВСЕ критерии исходной игры
    genres = source_game.genres.all()
    keywords = source_game.keywords.all()
    themes = source_game.themes.all()
    perspectives = source_game.player_perspectives.all()
    developers = source_game.developers.all()

    params = {
        'source_game': source_game.id,
    }

    # Добавляем ВСЕ критерии
    if genres:
        params['g'] = ','.join(str(g.id) for g in genres)

    if keywords:
        params['k'] = ','.join(str(k.id) for k in keywords)

    if themes:
        params['t'] = ','.join(str(t.id) for t in themes)

    if perspectives:
        params['pp'] = ','.join(str(p.id) for p in perspectives)

    if developers:
        params['d'] = ','.join(str(d.id) for d in developers)

    base_url = reverse('game_comparison', args=[target_game.id])
    return f"{base_url}?{urlencode(params)}"
