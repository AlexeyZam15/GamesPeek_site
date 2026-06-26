# games/views_parts/game_detail_views.py

from django.shortcuts import render, get_object_or_404
from django.http import HttpRequest, HttpResponse
from django.db.models import Prefetch

from ..models import (
    Game, Genre, Platform, Theme, Company,
    PlayerPerspective, GameMode, Keyword
)
from ..breadcrumb import generate_game_breadcrumb


def game_detail(request: HttpRequest, pk: int) -> HttpResponse:
    """
    Страница деталей игры с оптимизированными запросами и похожими играми.

    Похожие игры берутся из поля similar_game_ids, которое хранит ID
    самых похожих игр в порядке убывания схожести.
    """
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

    # Получаем похожие игры из поля similar_game_ids
    similar_games = []
    if game.similar_game_ids:
        similar_games = list(
            Game.objects.filter(id__in=game.similar_game_ids).prefetch_related(
                Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
                Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug')),
                Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            ).only(
                'id', 'name', 'rating', 'rating_count',
                'first_release_date', 'cover_url', 'game_type'
            )
        )
        # Сохраняем порядок как в similar_game_ids
        id_to_game = {g.id: g for g in similar_games}
        similar_games = [id_to_game[gid] for gid in game.similar_game_ids if gid in id_to_game]

    # Получаем первый жанр и первую платформу для навигационной цепочки
    first_genre = game.genres.first()
    genre_name = first_genre.name if first_genre else None

    first_platform = game.platforms.first()
    platform_name = first_platform.name if first_platform else None

    # Генерируем JSON-LD для навигационной цепочки BreadcrumbList
    breadcrumb_json_ld = generate_game_breadcrumb(
        game_title=game.name,
        platform_name=platform_name,
        genre_name=genre_name,
        base_url="https://gamespeek.dpdns.org"
    )

    # Получаем первый жанр и первую платформу для навигационной цепочки
    first_genre = game.genres.first()
    genre_name = first_genre.name if first_genre else None

    first_platform = game.platforms.first()
    platform_name = first_platform.name if first_platform else None

    # Генерируем JSON-LD для навигационной цепочки BreadcrumbList
    breadcrumb_json_ld = generate_game_breadcrumb(
        game_title=game.name,
        platform_name=platform_name,
        genre_name=genre_name,
        base_url="https://gamespeek.dpdns.org"
    )

    return render(request, 'games/game_detail.html', {
        'game': game,
        'seo_text': seo_text,
        'similar_games': similar_games,
        'current_page': 1,
        'breadcrumb_json_ld': breadcrumb_json_ld,
    })


def _generate_game_seo_text(game: Game) -> str:
    """
    Генерирует уникальный SEO-текст на основе данных игры.
    """
    parts = []

    parts.append(f"{game.name} is a ")

    if game.genres.all():
        genre_names = [g.name for g in game.genres.all()]
        parts.append(f"{', '.join(genre_names)} game ")

    if game.first_release_date:
        parts.append(f"released in {game.first_release_date.year} ")

    if game.developers.all():
        dev_names = [d.name for d in game.developers.all()[:2]]
        parts.append(f"developed by {', '.join(dev_names)} ")

    parts.append("that offers ")

    if game.game_modes.all():
        mode_names = [m.name for m in game.game_modes.all()[:2]]
        parts.append(f"{', '.join(mode_names)} gameplay ")

    if game.player_perspectives.all():
        persp_names = [p.name for p in game.player_perspectives.all()[:2]]
        parts.append(f"from a {', '.join(persp_names).lower()} perspective ")

    if game.platforms.all():
        platform_names = [p.name for p in game.platforms.all()[:3]]
        parts.append(f"playable on {', '.join(platform_names)} ")

    rounded_rating = round(game.rating or 0, 1)
    parts.append(f"with {game.rating_count or 0} user ratings averaging {rounded_rating}/100.")

    if game.themes.all():
        theme_names = [t.name for t in game.themes.all()[:3]]
        parts.append(f" The game explores themes like {', '.join(theme_names)}.")

    parts.append(f" If you enjoy {game.name}, you might also like similar games in our database of 45,000+ titles.")

    return " ".join(parts)