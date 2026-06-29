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
        id_to_game = {g.id: g for g in similar_games}
        similar_games = [id_to_game[gid] for gid in game.similar_game_ids if gid in id_to_game]

    first_genre = game.genres.first()
    genre_name = first_genre.name if first_genre else None

    first_platform = game.platforms.first()
    platform_name = first_platform.name if first_platform else None

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
    Генерирует краткий SEO-текст для страницы игры (максимум 155 символов).

    Args:
        game: Объект игры с предварительно загруженными связанными данными

    Returns:
        Описательный текст для страницы игры (макс. 155 символов)
    """
    parts = []

    # Название игры
    parts.append(game.name)

    # Тип игры
    game_type_display = game.get_game_type_display
    if game_type_display and game_type_display != "No game type":
        parts.append(f"is a {game_type_display.lower()}")
    else:
        parts.append("is a game")

    # Жанры (только первые 2)
    genres = list(game.genres.all())
    if genres:
        genre_names = [g.name for g in genres[:2]]
        if len(genre_names) == 1:
            parts.append(f"in the {genre_names[0]} genre")
        else:
            parts.append(f"in {genre_names[0]} and {genre_names[1]} genres")

    # Год релиза
    if game.first_release_date:
        parts.append(f"from {game.first_release_date.year}")

    # Рейтинг
    if game.rating:
        parts.append(f"rated {round(game.rating, 1)}/100")

    # Разработчик (только первый)
    developers = list(game.developers.all())
    if developers:
        parts.append(f"by {developers[0].name}")

    # Платформы (только первые 2)
    platforms = list(game.platforms.all())
    if platforms:
        platform_names = []
        for p in platforms[:2]:
            name_lower = p.name.lower()
            if 'microsoft windows' in name_lower or name_lower == 'windows':
                platform_names.append('PC')
            elif 'nintendo switch' in name_lower:
                platform_names.append('Switch')
            elif 'playstation' in name_lower:
                platform_names.append(p.name.replace('PlayStation', 'PS'))
            elif 'xbox' in name_lower:
                platform_names.append(p.name.replace('Xbox', 'XB'))
            else:
                platform_names.append(p.name)

        if len(platform_names) == 1:
            parts.append(f"on {platform_names[0]}")
        else:
            parts.append(f"on {platform_names[0]} and {platform_names[1]}")

    # Призыв к действию (коротко)
    parts.append("Find similar games in our database")

    full_text = " ".join(parts)

    # Обрезаем до 155 символов, если длиннее
    if len(full_text) > 155:
        full_text = full_text[:152] + "..."

    return full_text