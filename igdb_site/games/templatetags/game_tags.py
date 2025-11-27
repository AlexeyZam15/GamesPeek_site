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
    game_modes = game.game_modes.all()  # ← ДОБАВИТЬ

    params = {
        'find_similar': '1',
        'source_game': game.id,
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

    # ДОБАВИТЬ Game Modes (только для фильтрации)
    if game_modes:
        params['gm'] = ','.join(str(gm.id) for gm in game_modes)

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
    game_modes = source_game.game_modes.all()  # ← ДОБАВИТЬ

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

    # ДОБАВИТЬ Game Modes
    if game_modes:
        params['gm'] = ','.join(str(gm.id) for gm in game_modes)

    base_url = reverse('game_comparison', args=[target_game.id])
    return f"{base_url}?{urlencode(params)}"


@register.simple_tag
def get_card_comparison_url(source_game, target_game, selected_genres=None, selected_keywords=None,
                            selected_themes=None, selected_perspectives=None, selected_developers=None, selected_game_modes=None):  # ← ДОБАВИТЬ
    """
    Генерирует URL для сравнения из карточки игры
    """
    from django.urls import reverse
    from urllib.parse import urlencode

    params = {}

    # Определяем какие критерии использовать
    if source_game and hasattr(source_game, 'source_game_id') and source_game.source_game_id:
        # Если есть исходная игра (из game_detail)
        params['source_game'] = source_game.source_game_id

        # Используем критерии из source_game
        if hasattr(source_game, 'genres_ids') and source_game.genres_ids:
            params['g'] = ','.join(str(g) for g in source_game.genres_ids)
        elif hasattr(source_game, 'genres') and source_game.genres:
            params['g'] = ','.join(str(g) for g in source_game.genres)

        if hasattr(source_game, 'keywords_ids') and source_game.keywords_ids:
            params['k'] = ','.join(str(k) for k in source_game.keywords_ids)
        elif hasattr(source_game, 'keywords') and source_game.keywords:
            params['k'] = ','.join(str(k) for k in source_game.keywords)

        if hasattr(source_game, 'themes_ids') and source_game.themes_ids:
            params['t'] = ','.join(str(t) for t in source_game.themes_ids)
        elif hasattr(source_game, 'themes') and source_game.themes:
            params['t'] = ','.join(str(t) for t in source_game.themes)

        if hasattr(source_game, 'perspectives_ids') and source_game.perspectives_ids:
            params['pp'] = ','.join(str(p) for p in source_game.perspectives_ids)
        elif hasattr(source_game, 'perspectives') and source_game.perspectives:
            params['pp'] = ','.join(str(p) for p in source_game.perspectives)

        if hasattr(source_game, 'developers_ids') and source_game.developers_ids:
            params['d'] = ','.join(str(d) for d in source_game.developers_ids)
        elif hasattr(source_game, 'developers') and source_game.developers:
            params['d'] = ','.join(str(d) for d in source_game.developers)

        # ДОБАВИТЬ Game Modes
        if hasattr(source_game, 'game_modes_ids') and source_game.game_modes_ids:
            params['gm'] = ','.join(str(gm) for gm in source_game.game_modes_ids)
        elif hasattr(source_game, 'game_modes') and source_game.game_modes:
            params['gm'] = ','.join(str(gm) for gm in source_game.game_modes)
    else:
        # Используем переданные критерии поиска (из game_list)
        if selected_genres:
            params['g'] = ','.join(str(g) for g in selected_genres)
        if selected_keywords:
            params['k'] = ','.join(str(k) for k in selected_keywords)
        if selected_themes:
            params['t'] = ','.join(str(t) for t in selected_themes)
        if selected_perspectives:
            params['pp'] = ','.join(str(pp) for pp in selected_perspectives)
        if selected_developers:
            params['d'] = ','.join(str(d) for d in selected_developers)
        # ДОБАВИТЬ Game Modes
        if selected_game_modes:
            params['gm'] = ','.join(str(gm) for gm in selected_game_modes)

    base_url = reverse('game_comparison', args=[target_game.id])
    return f"{base_url}?{urlencode(params)}"
