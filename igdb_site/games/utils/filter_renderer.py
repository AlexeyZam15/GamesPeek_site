"""Utility functions for rendering filter sections with caching."""

import logging
from typing import Dict, List, Any, Optional, Callable
from django.template.loader import render_to_string
from django.utils.safestring import mark_safe

from ..models import Genre, Keyword, Platform, Theme, PlayerPerspective, GameMode, GameEngine
from ..models_parts.enums import GameTypeEnum

logger = logging.getLogger(__name__)


class FilterRenderer:
    """Класс для рендеринга секций фильтров без кэширования."""

    @staticmethod
    def render_search_platforms(platforms: List[Platform], selected_ids: List[int]) -> str:
        """Рендерит секцию платформ для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_platforms_section.html', {
            'platforms': platforms,
            'search_selected_platforms': selected_ids,
            'search_selected_platforms_objects': [
                p for p in platforms if p.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_search_game_types(game_type_choices, selected_ids: List[int]) -> str:
        """Рендерит секцию типов игр для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_game_types_section.html', {
            'game_type_choices': game_type_choices,
            'search_selected_game_types': selected_ids,
        })
        return mark_safe(html)

    @staticmethod
    def render_search_genres(genres: List[Genre], selected_ids: List[int]) -> str:
        """Рендерит секцию жанров для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_genres_section.html', {
            'genres': genres,
            'search_selected_genres': selected_ids,
            'search_selected_genres_objects': [
                g for g in genres if g.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_search_keywords(keywords: List[Keyword], selected_ids: List[int]) -> str:
        """Рендерит секцию ключевых слов для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_keywords_section.html', {
            'keywords': keywords,
            'search_selected_keywords': selected_ids,
            'search_selected_keywords_objects': [
                k for k in keywords if k.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_search_themes(themes: List[Theme], selected_ids: List[int]) -> str:
        """Рендерит секцию тем для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_themes_section.html', {
            'themes': themes,
            'search_selected_themes': selected_ids,
            'search_selected_themes_objects': [
                t for t in themes if t.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_search_perspectives(perspectives: List[PlayerPerspective], selected_ids: List[int]) -> str:
        """Рендерит секцию перспектив для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_perspectives_section.html', {
            'perspectives': perspectives,
            'search_selected_perspectives': selected_ids,
            'search_selected_perspectives_objects': [
                p for p in perspectives if p.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_search_game_modes(game_modes: List[GameMode], selected_ids: List[int]) -> str:
        """Рендерит секцию режимов игры для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_game_modes_section.html', {
            'game_modes': game_modes,
            'search_selected_game_modes': selected_ids,
            'search_selected_game_modes_objects': [
                gm for gm in game_modes if gm.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_search_engines(engines: List[GameEngine], selected_ids: List[int]) -> str:
        """Рендерит секцию движков для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_engines_section.html', {
            'engines': engines,
            'search_selected_engines': selected_ids,
            'search_selected_engines_objects': [
                e for e in engines if e.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_search_date_filter(year_start: Optional[int], year_end: Optional[int],
                                  min_year: int, max_year: int, current_year: int) -> str:
        """Рендерит секцию фильтра даты для поиска напрямую."""
        html = render_to_string('games/game_list/filter_sections/_search_date_section.html', {
            'search_selected_release_year_start': year_start,
            'search_selected_release_year_end': year_end,
            'years_range': {'min_year': min_year, 'max_year': max_year},
            'current_year': current_year
        })
        return mark_safe(html)

    @staticmethod
    def render_similarity_genres(genres: List[Genre], selected_ids: List[int]) -> str:
        """Рендерит секцию жанров для похожести напрямую."""
        html = render_to_string('games/game_list/filter_sections/_similarity_genres_section.html', {
            'genres': genres,
            'similarity_selected_genres': selected_ids,
            'similarity_selected_genres_objects': [
                g for g in genres if g.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_similarity_keywords(keywords: List[Keyword], selected_ids: List[int]) -> str:
        """Рендерит секцию ключевых слов для похожести напрямую."""
        html = render_to_string('games/game_list/filter_sections/_similarity_keywords_section.html', {
            'keywords': keywords,
            'similarity_selected_keywords': selected_ids,
            'similarity_selected_keywords_objects': [
                k for k in keywords if k.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_similarity_themes(themes: List[Theme], selected_ids: List[int]) -> str:
        """Рендерит секцию тем для похожести напрямую."""
        html = render_to_string('games/game_list/filter_sections/_similarity_themes_section.html', {
            'themes': themes,
            'similarity_selected_themes': selected_ids,
            'similarity_selected_themes_objects': [
                t for t in themes if t.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_similarity_perspectives(perspectives: List[PlayerPerspective], selected_ids: List[int]) -> str:
        """Рендерит секцию перспектив для похожести напрямую."""
        html = render_to_string('games/game_list/filter_sections/_similarity_perspectives_section.html', {
            'perspectives': perspectives,
            'similarity_selected_perspectives': selected_ids,
            'similarity_selected_perspectives_objects': [
                p for p in perspectives if p.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_similarity_game_modes(game_modes: List[GameMode], selected_ids: List[int]) -> str:
        """Рендерит секцию режимов игры для похожести напрямую."""
        html = render_to_string('games/game_list/filter_sections/_similarity_game_modes_section.html', {
            'game_modes': game_modes,
            'similarity_selected_game_modes': selected_ids,
            'similarity_selected_game_modes_objects': [
                gm for gm in game_modes if gm.id in selected_ids
            ]
        })
        return mark_safe(html)

    @staticmethod
    def render_similarity_engines(engines: List[GameEngine], selected_ids: List[int]) -> str:
        """Рендерит секцию движков для похожести напрямую."""
        html = render_to_string('games/game_list/filter_sections/_similarity_engines_section.html', {
            'engines': engines,
            'similarity_selected_engines': selected_ids,
            'similarity_selected_engines_objects': [
                e for e in engines if e.id in selected_ids
            ]
        })
        return mark_safe(html)
