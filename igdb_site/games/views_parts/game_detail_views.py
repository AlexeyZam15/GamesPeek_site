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
    Generates unique SEO text based on game data.

    The text is generated programmatically from game attributes and is unique
    for each page, as the combination of genres, platforms, developers, and
    rating is unique for each game.

    Args:
        game: Game object with preloaded related data

    Returns:
        Unique descriptive text for the game page
    """
    parts = []

    # Game name and type
    game_type_display = game.get_game_type_display
    if game_type_display and game_type_display != "No game type":
        parts.append(f"{game.name} is a {game_type_display.lower()}")
    else:
        parts.append(f"{game.name} is a game")

    # Genres
    genres = list(game.genres.all())
    if genres:
        genre_names = [g.name for g in genres[:5]]
        if len(genre_names) > 3:
            parts.append(f"in {', '.join(genre_names[:-1])} and {genre_names[-1]} genres")
        else:
            parts.append(f"in the {', '.join(genre_names)} genre")

    # Release year
    if game.first_release_date:
        parts.append(f"released in {game.first_release_date.year}")

    # Developers
    developers = list(game.developers.all())
    if developers:
        dev_names = [d.name for d in developers[:2]]
        if len(dev_names) == 1:
            parts.append(f"developed by {dev_names[0]}")
        else:
            parts.append(f"developed by {', '.join(dev_names)}")

    # Platforms
    platforms = list(game.platforms.all())
    if platforms:
        platform_names = [p.name for p in platforms[:4]]
        display_names = []
        for name in platform_names:
            name_lower = name.lower()
            if 'microsoft windows' in name_lower or name_lower == 'windows':
                display_names.append('PC')
            elif 'nintendo switch' in name_lower:
                display_names.append('Nintendo Switch')
            elif 'playstation' in name_lower:
                display_names.append(name.replace('PlayStation', 'PS'))
            elif 'xbox' in name_lower:
                display_names.append(name.replace('Xbox', 'XB'))
            else:
                display_names.append(name)

        if len(display_names) == 1:
            parts.append(f"available on {display_names[0]}")
        else:
            parts.append(f"available on {', '.join(display_names)}")

    # Game modes
    modes = list(game.game_modes.all())
    if modes:
        mode_names = [m.name for m in modes[:3]]
        if len(mode_names) == 1:
            parts.append(f"with {mode_names[0]} mode")
        else:
            parts.append(f"with {', '.join(mode_names)} modes")

    # Rating
    if game.rating:
        rounded_rating = round(game.rating, 1)
        parts.append(f"with a rating of {rounded_rating}/100 based on {game.rating_count or 0} user votes")

    # Themes
    themes = list(game.themes.all())
    if themes:
        theme_names = [t.name for t in themes[:3]]
        if len(theme_names) == 1:
            parts.append(f"featuring {theme_names[0]} theme")
        else:
            parts.append(f"featuring {', '.join(theme_names)} themes")

    # Series information
    if game.is_part_of_series and game.main_series:
        series_name = game.main_series.name
        if game.series_order:
            parts.append(f"part of the {series_name} series (Part {game.series_order})")
        else:
            parts.append(f"part of the {series_name} series")

    # Call to action
    parts.append(
        f"Browse our database of 45,000+ games and find more titles similar to {game.name} in the 'Similar Games' section below.")

    # Capitalize first letter
    full_text = " ".join(parts)
    if full_text:
        full_text = full_text[0].upper() + full_text[1:]

    return full_text
