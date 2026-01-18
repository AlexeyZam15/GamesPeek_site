from django import template
from django.urls import reverse
from urllib.parse import urlencode

register = template.Library()


@register.simple_tag
def get_find_similar_url(game):
    """Генерирует URL для поиска похожих игр со всеми критериями."""
    params = {
        'find_similar': '1',
    }

    # Проверяем тип game
    if hasattr(game, 'id') and game.id is not None:
        # Если это реальная игра
        params['source_game'] = game.id

        # Жанры - ВСЕ (для алгоритма похожести)
        genre_ids = [str(g.id) for g in game.genres.all()]
        if genre_ids:
            params['g'] = ','.join(genre_ids)

        # Темы - ВСЕ (для алгоритма похожести)
        theme_ids = [str(t.id) for t in game.themes.all()]
        if theme_ids:
            params['t'] = ','.join(theme_ids)

        # Перспективы - ВСЕ (для алгоритма похожести)
        perspective_ids = [str(p.id) for p in game.player_perspectives.all()]
        if perspective_ids:
            params['pp'] = ','.join(perspective_ids)

        # Режимы игры - ВСЕ (для алгоритма похожести)
        game_mode_ids = [str(gm.id) for gm in game.game_modes.all()]
        if game_mode_ids:
            params['gm'] = ','.join(game_mode_ids)

        # Ключевые слова - ВСЕ (для алгоритма похожести)
        keyword_ids = [str(k.id) for k in game.keywords.all()]
        if keyword_ids:
            params['k'] = ','.join(keyword_ids)

        # Разработчики - ВСЕ (для алгоритма похожести)
        developer_ids = [str(d.id) for d in game.developers.all()]
        if developer_ids:
            params['d'] = ','.join(developer_ids)
    else:
        # Если это виртуальная игра (критерии поиска)
        # Используем сохраненные критерии через специальные методы
        if hasattr(game, 'genres_list'):
            genres = game.genres_list()
            if genres:
                params['g'] = ','.join(str(g) for g in genres)

        if hasattr(game, 'keywords_list'):
            keywords = game.keywords_list()
            if keywords:
                params['k'] = ','.join(str(k) for k in keywords)

        if hasattr(game, 'themes_list'):
            themes = game.themes_list()
            if themes:
                params['t'] = ','.join(str(t) for t in themes)

        if hasattr(game, 'perspectives_list'):
            perspectives = game.perspectives_list()
            if perspectives:
                params['pp'] = ','.join(str(p) for p in perspectives)

        if hasattr(game, 'developers_list'):
            developers = game.developers_list()
            if developers:
                params['d'] = ','.join(str(d) for d in developers)

        if hasattr(game, 'game_modes_list'):
            game_modes = game.game_modes_list()
            if game_modes:
                params['gm'] = ','.join(str(gm) for gm in game_modes)

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
    Полностью поддерживает SimpleSourceGame.
    """
    params = {}

    # Определяем ID исходной игры
    source_game_id = None

    if source_game is None:
        # Если source_game не передан, используем критерии фильтрации
        pass
    elif hasattr(source_game, 'is_game'):
        # Это SimpleSourceGame объект
        if source_game.is_game and hasattr(source_game, 'id'):
            source_game_id = source_game.id
    elif hasattr(source_game, 'id') and source_game.id is not None:
        # Это обычный Game объект
        source_game_id = source_game.id
    elif isinstance(source_game, dict) and 'id' in source_game:
        # Это словарь с ID
        source_game_id = source_game['id']
    else:
        # Пробуем получить ID другим способом
        try:
            source_game_id = int(str(source_game))
        except (ValueError, TypeError):
            pass

    # Если есть ID исходной игры, добавляем в параметры
    if source_game_id:
        params['source_game'] = source_game_id

    # Если нет source_game, используем текущие критерии фильтрации
    if not params.get('source_game'):
        # Если у нас есть SimpleSourceGame с критериями
        if hasattr(source_game, 'is_game') and not source_game.is_game:
            # Виртуальная игра - используем ее критерии
            if hasattr(source_game, 'genres_list'):
                genres = source_game.genres_list()
                if genres:
                    params['g'] = ','.join(str(g) for g in genres)

            if hasattr(source_game, 'keywords_list'):
                keywords = source_game.keywords_list()
                if keywords:
                    params['k'] = ','.join(str(k) for k in keywords)

            if hasattr(source_game, 'themes_list'):
                themes = source_game.themes_list()
                if themes:
                    params['t'] = ','.join(str(t) for t in themes)

            if hasattr(source_game, 'perspectives_list'):
                perspectives = source_game.perspectives_list()
                if perspectives:
                    params['pp'] = ','.join(str(pp) for pp in perspectives)

            if hasattr(source_game, 'developers_list'):
                developers = source_game.developers_list()
                if developers:
                    params['d'] = ','.join(str(d) for d in developers)

            if hasattr(source_game, 'game_modes_list'):
                game_modes = source_game.game_modes_list()
                if game_modes:
                    params['gm'] = ','.join(str(gm) for gm in game_modes)
        else:
            # Используем переданные критерии
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


@register.simple_tag
def get_comparison_url_from_criteria(criteria_virtual_game, target_game):
    """
    Генерирует URL для сравнения критериев с игрой.
    Для использования с виртуальными играми (критериями).
    """
    params = {}

    # Используем критерии из виртуальной игры
    if hasattr(criteria_virtual_game, 'genres_list'):
        genres = criteria_virtual_game.genres_list()
        if genres:
            params['g'] = ','.join(str(g) for g in genres)

    if hasattr(criteria_virtual_game, 'keywords_list'):
        keywords = criteria_virtual_game.keywords_list()
        if keywords:
            params['k'] = ','.join(str(k) for k in keywords)

    if hasattr(criteria_virtual_game, 'themes_list'):
        themes = criteria_virtual_game.themes_list()
        if themes:
            params['t'] = ','.join(str(t) for t in themes)

    if hasattr(criteria_virtual_game, 'perspectives_list'):
        perspectives = criteria_virtual_game.perspectives_list()
        if perspectives:
            params['pp'] = ','.join(str(p) for p in perspectives)

    if hasattr(criteria_virtual_game, 'developers_list'):
        developers = criteria_virtual_game.developers_list()
        if developers:
            params['d'] = ','.join(str(d) for d in developers)

    if hasattr(criteria_virtual_game, 'game_modes_list'):
        game_modes = criteria_virtual_game.game_modes_list()
        if game_modes:
            params['gm'] = ','.join(str(gm) for gm in game_modes)

    base_url = reverse('game_comparison', args=[target_game.id])
    return f"{base_url}?{urlencode(params)}"
