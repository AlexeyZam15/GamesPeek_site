"""Views for game detail page."""

from django.shortcuts import render, get_object_or_404
from django.http import HttpRequest, HttpResponse
from django.db.models import Prefetch

from ..models import (
    Game, Genre, Platform, Theme, Company,
    PlayerPerspective, GameMode, Keyword
)


def game_detail(request: HttpRequest, pk: int) -> HttpResponse:
    """
    Страница деталей игры с оптимизированными запросами и похожими играми.

    Похожие игры сортируются по убыванию процента схожести (самые похожие — первые).
    """
    from .game_list_views import get_similar_games_for_game

    game = get_object_or_404(
        Game.objects.prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
            Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
            Prefetch('developers', queryset=Company.objects.only('id', 'name')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
            Prefetch('publishers', queryset=Company.objects.only('id', 'name')),
        ),
        pk=pk
    )

    seo_text = _generate_game_seo_text(game)

    similar_games_data, total_count = get_similar_games_for_game(
        game_obj=game,
        selected_platforms=[],
        search_filters=None
    )

    similar_games = []
    for item in similar_games_data:
        if isinstance(item, dict):
            similar_game = item.get('game')
            similarity = item.get('similarity', 0)
        else:
            similar_game = item
            similarity = getattr(item, 'similarity', 0)

        if similar_game and similar_game.id != game.id:
            setattr(similar_game, 'similarity', similarity)
            similar_games.append(similar_game)

    # СОРТИРОВКА ПО УБЫВАНИЮ ПРОЦЕНТА СХОЖЕСТИ
    # Самые похожие игры — первые в списке
    similar_games.sort(key=lambda x: getattr(x, 'similarity', 0), reverse=True)

    # Ограничиваем до 12 самых похожих игр
    similar_games = similar_games[:12]

    return render(request, 'games/game_detail.html', {
        'game': game,
        'seo_text': seo_text,
        'similar_games': similar_games,
        'current_page': 1,
    })


def _generate_game_seo_text(game: Game) -> str:
    """
    Генерирует уникальный SEO-текст на основе данных игры.

    Текст включает название, жанры, год релиза, разработчиков,
    режимы игры, перспективы, платформы, рейтинг, темы и рекомендацию.
    """
    parts = []

    # Основное предложение с названием
    parts.append(f"{game.name} is a ")

    # Перечисление жанров
    if game.genres.all():
        genre_names = [g.name for g in game.genres.all()]
        parts.append(f"{', '.join(genre_names)} game ")

    # Год релиза
    if game.first_release_date:
        parts.append(f"released in {game.first_release_date.year} ")

    # Разработчики (первые два)
    if game.developers.all():
        dev_names = [d.name for d in game.developers.all()[:2]]
        parts.append(f"developed by {', '.join(dev_names)} ")

    # Переход к описанию игрового процесса
    parts.append("that offers ")

    # Режимы игры (первые два)
    if game.game_modes.all():
        mode_names = [m.name for m in game.game_modes.all()[:2]]
        parts.append(f"{', '.join(mode_names)} gameplay ")

    # Перспектива (первые две)
    if game.player_perspectives.all():
        persp_names = [p.name for p in game.player_perspectives.all()[:2]]
        parts.append(f"from a {', '.join(persp_names).lower()} perspective ")

    # Платформы (первые три)
    if game.platforms.all():
        platform_names = [p.name for p in game.platforms.all()[:3]]
        parts.append(f"playable on {', '.join(platform_names)} ")

    # Рейтинг с округлением до 1 знака
    rounded_rating = round(game.rating or 0, 1)
    parts.append(f"with {game.rating_count or 0} user ratings averaging {rounded_rating}/100.")

    # Абзац с темами (первые три)
    if game.themes.all():
        theme_names = [t.name for t in game.themes.all()[:3]]
        parts.append(f" The game explores themes like {', '.join(theme_names)}.")

    # Абзац-рекомендация с похожими играми
    parts.append(f" If you enjoy {game.name}, you might also like similar games in our database of 45,000+ titles.")

    return " ".join(parts)