# games/analyze/utils.py
"""
Вспомогательные функции для анализа игр
"""

from typing import Dict, Any, List
from games.models import Game


def prepare_text_for_display(text: str, analysis_result: Dict = None, mode: str = 'criteria') -> str:
    """
    Подготавливает текст для отображения с подсветкой
    """
    if not text:
        return ''

    if not analysis_result or not analysis_result.get('pattern_info'):
        return text

    # Используем уже созданную функцию highlight_matches_in_text
    from .analyze_views import highlight_matches_in_text  # Или перенесите функцию в utils
    return highlight_matches_in_text(text, analysis_result, mode)


def get_description_stats(game: Game) -> Dict[str, Dict]:
    """
    Возвращает статистику по всем описаниям игры
    """
    descriptions = {
        'summary': game.summary or '',
        'storyline': game.storyline or '',
        'rawg': game.rawg_description or '',
        'wiki': game.wiki_description or '',
    }

    stats = {}
    for key, text in descriptions.items():
        if text.strip():
            stats[key] = {
                'length': len(text),
                'word_count': len(text.split()),
                'lines': text.count('\n') + 1,
                'has_content': True
            }
        else:
            stats[key] = {
                'length': 0,
                'word_count': 0,
                'lines': 0,
                'has_content': False
            }

    return stats


def get_game_text(game: Game, source: str = 'default') -> str:
    """
    Получает текст игры в зависимости от источника

    Args:
        game: Объект игры
        source: Источник текста

    Returns:
        Текст для анализа
    """
    if source == 'wiki' and game.wiki_description:
        return game.wiki_description
    elif source == 'rawg' and game.rawg_description:
        return game.rawg_description
    elif source == 'storyline' and game.storyline:
        return game.storyline
    elif source == 'all':
        # Объединяем все доступные тексты
        texts = []
        if game.summary:
            texts.append(game.summary)
        if game.storyline:
            texts.append(game.storyline)
        if game.rawg_description:
            texts.append(game.rawg_description)
        if game.wiki_description:
            texts.append(game.wiki_description)
        return ' '.join(texts) if texts else ''
    else:
        # По умолчанию: предпочитаем описание, потом сторилайн
        if game.summary:
            return game.summary
        elif game.storyline:
            return game.storyline
        elif game.rawg_description:
            return game.rawg_description
        elif game.wiki_description:
            return game.wiki_description
        else:
            return ''


def update_game_criteria(game: Game, results: Dict, is_keywords: bool) -> bool:
    """
    Обновляет критерии игры в базе данных

    Args:
        game: Объект игры
        results: Результаты анализа
        is_keywords: Обновлять ключевые слова или критерии

    Returns:
        True если были обновления
    """
    try:
        if is_keywords:
            keywords = results.get('keywords', {}).get('items', [])
            if keywords:
                # Получаем объекты Keyword
                keyword_ids = [k['id'] for k in keywords]
                from games.models import Keyword
                keyword_objects = Keyword.objects.filter(id__in=keyword_ids)
                game.keywords.add(*keyword_objects)
                return True
        else:
            updated = False
            # Жанры
            genres = results.get('genres', {}).get('items', [])
            if genres:
                genre_ids = [g['id'] for g in genres]
                from games.models import Genre
                genre_objects = Genre.objects.filter(id__in=genre_ids)
                game.genres.add(*genre_objects)
                updated = True

            # Темы
            themes = results.get('themes', {}).get('items', [])
            if themes:
                theme_ids = [t['id'] for t in themes]
                from games.models import Theme
                theme_objects = Theme.objects.filter(id__in=theme_ids)
                game.themes.add(*theme_objects)
                updated = True

            # Перспективы
            perspectives = results.get('perspectives', {}).get('items', [])
            if perspectives:
                perspective_ids = [p['id'] for p in perspectives]
                from games.models import PlayerPerspective
                perspective_objects = PlayerPerspective.objects.filter(id__in=perspective_ids)
                game.player_perspectives.add(*perspective_objects)
                updated = True

            # Режимы игры
            game_modes = results.get('game_modes', {}).get('items', [])
            if game_modes:
                mode_ids = [m['id'] for m in game_modes]
                from games.models import GameMode
                mode_objects = GameMode.objects.filter(id__in=mode_ids)
                game.game_modes.add(*mode_objects)
                updated = True

            return updated
    except Exception as e:
        # Логируем ошибку, но не падаем
        import logging
        logging.error(f"Ошибка при обновлении игры {game.id}: {e}")
        return False

    return False


def format_game_response(game: Game, text: str, text_source: str) -> Dict[str, Any]:
    """
    Форматирует базовую информацию об игре для ответа

    Args:
        game: Объект игры
        text: Текст который был проанализирован
        text_source: Источник текста

    Returns:
        Форматированная информация об игре
    """
    return {
        'game': {
            'id': game.id,
            'name': game.name,
            'slug': game.slug,
            'release_date': game.release_date.isoformat() if game.release_date else None
        },
        'analysis_context': {
            'text_source': text_source,
            'text_length': len(text),
            'has_text': len(text) > 0
        }
    }


def create_error_response(error_type: str, message: str, **kwargs) -> Dict[str, Any]:
    """
    Создает структурированный ответ об ошибке

    Args:
        error_type: Тип ошибки
        message: Сообщение об ошибке
        **kwargs: Дополнительные данные

    Returns:
        Ответ об ошибке
    """
    import time
    response = {
        'success': False,
        'error_type': error_type,
        'error_message': message,
        'timestamp': time.time()
    }
    response.update(kwargs)
    return response
