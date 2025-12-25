from django import template
from django.urls import reverse
from urllib.parse import urlencode

register = template.Library()


@register.simple_tag
def get_find_similar_url(game):
    """Генерирует URL без запросов к БД."""
    # Получаем ID связанных объектов из prefetched данных
    genre_ids = [str(g.id) for g in game.genres.all()]
    keyword_ids = [str(k.id) for k in game.keywords.all()]
    theme_ids = [str(t.id) for t in game.themes.all()]

    params = {
        'find_similar': '1',
        'source_game': game.id,
    }

    if genre_ids:
        params['g'] = ','.join(genre_ids)  # Ограничиваем

    base_url = reverse('game_list')
    return f"{base_url}?{urlencode(params)}"


@register.simple_tag
def get_comparison_url(source_game, target_game):
    """Генерирует URL для сравнения двух конкретных игр."""
    params = {
        'source_game': source_game.id,
    }

    # Получаем ВСЕ критерии исходной игры
    genres = source_game.genres.all()
    keywords = source_game.keywords.all()
    themes = source_game.themes.all()
    perspectives = source_game.player_perspectives.all()
    developers = source_game.developers.all()
    game_modes = source_game.game_modes.all()

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
    if game_modes:
        params['gm'] = ','.join(str(gm.id) for gm in game_modes)

    base_url = reverse('game_comparison', args=[target_game.id])
    return f"{base_url}?{urlencode(params)}"


@register.simple_tag
def get_card_comparison_url(source_game, target_game, selected_genres=None, selected_keywords=None,
                            selected_themes=None, selected_perspectives=None, selected_developers=None,
                            selected_game_modes=None):
    """
    Упрощенная версия: генерирует URL для сравнения из карточки игры.
    """
    params = {}

    # Проверяем тип source_game
    if isinstance(source_game, dict):
        # Если source_game - это словарь с критериями
        if 'id' in source_game:
            params['source_game'] = source_game['id']
    elif hasattr(source_game, 'id'):
        # Если source_game - это объект Game
        params['source_game'] = source_game.id
    elif source_game is not None:
        # Если source_game существует, но не Game объект
        try:
            params['source_game'] = str(source_game)
        except:
            pass

    # Если нет source_game, используем текущие критерии фильтрации
    if not params.get('source_game'):
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
        if selected_game_modes:
            params['gm'] = ','.join(str(gm) for gm in selected_game_modes)

    base_url = reverse('game_comparison', args=[target_game.id])
    return f"{base_url}?{urlencode(params)}"