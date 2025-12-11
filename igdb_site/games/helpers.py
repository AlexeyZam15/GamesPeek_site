# games/helpers.py
from django.urls import reverse
from urllib.parse import urlencode


def generate_compact_url_params(find_similar=False, genres=None, keywords=None, platforms=None,
                                themes=None, perspectives=None, developers=None, game_modes=None, sort=None):
    """
    Генерирует компактные параметры URL с ВСЕМИ критериями
    """
    params = {}

    if find_similar:
        params['find_similar'] = '1'

    if genres:
        params['g'] = ','.join(str(g) for g in genres)

    if keywords:
        params['k'] = ','.join(str(k) for k in keywords)

    if platforms:
        params['p'] = ','.join(str(p) for p in platforms)

    if themes:
        params['t'] = ','.join(str(t) for t in themes)

    if perspectives:
        params['pp'] = ','.join(str(pp) for pp in perspectives)

    if developers:
        params['d'] = ','.join(str(d) for d in developers)

    if game_modes:
        params['gm'] = ','.join(str(gm) for gm in game_modes)

    if sort:
        params['sort'] = sort

    return params


def get_compact_game_list_url(find_similar=False, genres=None, keywords=None, platforms=None,
                              themes=None, perspectives=None, developers=None, sort=None):
    """
    Вспомогательная функция для генерации полного URL с компактными параметрами
    """
    params = generate_compact_url_params(
        find_similar=find_similar,
        genres=genres,
        keywords=keywords,
        platforms=platforms,
        themes=themes,
        perspectives=perspectives,
        developers=developers,
        sort=sort
    )

    base_url = reverse('game_list')
    if params:
        return f"{base_url}?{urlencode(params)}"
    return base_url
