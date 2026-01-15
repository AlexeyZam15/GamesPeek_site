# games/helpers.py
from django.urls import reverse
from urllib.parse import urlencode


def generate_compact_url_params(find_similar=False, genres=None, keywords=None, platforms=None,
                                themes=None, perspectives=None, developers=None,
                                game_modes=None, game_types=None, sort=None):
    """
    Генерирует компактные параметры URL с ВСЕМИ критериями
    game_types - теперь поисковый фильтр (не влияет на find_similar)
    """
    params = {}

    # Поисковые фильтры (не влияют на find_similar)
    if platforms:
        params['p'] = ','.join(str(p) for p in platforms)

    if game_types:  # Поисковый фильтр, не включает find_similar автоматически
        params['gt'] = ','.join(str(gt) for gt in game_types)

    # Режим похожести
    if find_similar:
        params['find_similar'] = '1'

    # Критерии похожести
    if genres:
        params['g'] = ','.join(str(g) for g in genres)

    if keywords:
        params['k'] = ','.join(str(k) for k in keywords)

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
                              themes=None, perspectives=None, developers=None,
                              game_modes=None, game_types=None, sort=None):
    """
    Вспомогательная функция для генерации полного URL с компактными параметрами
    game_types - теперь поисковый фильтр
    """
    params = generate_compact_url_params(
        find_similar=find_similar,
        genres=genres,
        keywords=keywords,
        platforms=platforms,
        themes=themes,
        perspectives=perspectives,
        developers=developers,
        game_modes=game_modes,
        game_types=game_types,  # ОСТАЕТСЯ, но не влияет на find_similar
        sort=sort
    )

    base_url = reverse('game_list')
    if params:
        return f"{base_url}?{urlencode(params)}"
    return base_url
