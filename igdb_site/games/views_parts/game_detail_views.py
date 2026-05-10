"""Views for game detail page."""

from django.shortcuts import render, get_object_or_404
from django.http import HttpRequest, HttpResponse
from django.db.models import Prefetch

from ..models import (
    Game, Genre, Platform, Theme, Company,
    PlayerPerspective, GameMode, Keyword
)


def game_detail(request: HttpRequest, pk: int) -> HttpResponse:
    """Game detail page with optimized queries."""
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

    # Генерируем уникальный SEO-текст на основе данных игры
    seo_text = _generate_game_seo_text(game)

    return render(request, 'games/game_detail.html', {
        'game': game,
        'seo_text': seo_text,
    })


def _generate_game_seo_text(game: Game) -> str:
    """Generate unique SEO text based on game data."""
    parts = []

    # Основное предложение
    parts.append(f"{game.name} is a ")

    # Жанры
    if game.genres.all():
        genre_names = [g.name for g in game.genres.all()]
        parts.append(f"{', '.join(genre_names)} game ")

    # Год
    if game.first_release_date:
        parts.append(f"released in {game.first_release_date.year} ")

    # Разработчик
    if game.developers.all():
        dev_names = [d.name for d in game.developers.all()[:2]]
        parts.append(f"developed by {', '.join(dev_names)} ")

    # Первое предложение
    parts.append(f"that offers ")

    # Режимы игры
    if game.game_modes.all():
        mode_names = [m.name for m in game.game_modes.all()[:2]]
        parts.append(f"{', '.join(mode_names)} gameplay ")

    # Перспектива
    if game.player_perspectives.all():
        persp_names = [p.name for p in game.player_perspectives.all()[:2]]
        parts.append(f"from a {', '.join(persp_names).lower()} perspective ")

    # Платформы
    if game.platforms.all():
        platform_names = [p.name for p in game.platforms.all()[:3]]
        parts.append(f"playable on {', '.join(platform_names)} ")

    # Округляем рейтинг до 1 десятичного знака
    rounded_rating = round(game.rating or 0, 1)
    parts.append(f"with {game.rating_count or 0} user ratings averaging {rounded_rating}/100.")

    # Дополнительный абзац с темами
    if game.themes.all():
        theme_names = [t.name for t in game.themes.all()[:3]]
        parts.append(f" The game explores themes like {', '.join(theme_names)}.")

    # Третий абзац с рекомендацией
    parts.append(f" If you enjoy {game.name}, you might also like similar games in our database of 45,000+ titles.")

    return " ".join(parts)
