# games/helpers.py
from django.urls import reverse
from urllib.parse import urlencode


def generate_compact_url_params(find_similar=False, genres=None, keywords=None, platforms=None,
                                themes=None, perspectives=None, developers=None,
                                game_modes=None, game_types=None,
                                release_year_start=None, release_year_end=None,  # ДОБАВЛЕНО
                                sort=None):
    """
    Генерирует компактные параметры URL с ВСЕМИ критериями
    """
    params = {}

    # Поисковые фильтры (не влияют на find_similar)
    if platforms:
        params['p'] = ','.join(str(p) for p in platforms)

    if game_types:
        params['gt'] = ','.join(str(gt) for gt in game_types)

    # Фильтр по дате выхода
    if release_year_start is not None or release_year_end is not None:
        if release_year_start is not None and release_year_end is not None:
            # Используем комбинированный параметр yr
            params['yr'] = f"{release_year_start}-{release_year_end}"
        else:
            # Используем отдельные параметры
            if release_year_start is not None:
                params['ys'] = str(release_year_start)
            if release_year_end is not None:
                params['ye'] = str(release_year_end)

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
                              game_modes=None, game_types=None,
                              release_year_start=None, release_year_end=None,  # ДОБАВЛЕНО
                              sort=None):
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
        game_modes=game_modes,
        game_types=game_types,
        release_year_start=release_year_start,  # ДОБАВЛЕНО
        release_year_end=release_year_end,  # ДОБАВЛЕНО
        sort=sort
    )

    base_url = reverse('game_list')
    if params:
        return f"{base_url}?{urlencode(params)}"
    return base_url
