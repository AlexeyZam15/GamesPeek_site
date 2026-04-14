"""Utility functions for rendering filter sections with caching."""

import logging
from typing import Dict, List, Any, Optional, Callable
from django.template.loader import render_to_string
from django.utils.safestring import mark_safe

from ..models_parts.filter_cache import FilterSectionCache
from ..models import Genre, Keyword, Platform, Theme, PlayerPerspective, GameMode, GameEngine
from ..models_parts.enums import GameTypeEnum

logger = logging.getLogger(__name__)


class FilterRenderer:
    """Класс для рендеринга секций фильтров с кэшированием."""

    @staticmethod
    def _log_cache_status(section_type: str, was_cached: bool, selected_ids: List[int] = None):
        """Логирует статус кэша секции."""
        ids_str = f" IDs: {selected_ids}" if selected_ids else ""
        if was_cached:
            logger.info(f"✅ CACHE HIT: {section_type}{ids_str} - using cached HTML")
        else:
            logger.info(f"🔄 CACHE MISS: {section_type}{ids_str} - rendering new HTML")

    @staticmethod
    def render_search_platforms(platforms: List[Platform], selected_ids: List[int]) -> str:
        """Рендерит секцию платформ для поиска."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_platforms_section.html', {
                'platforms': platforms,
                'search_selected_platforms': selected_ids,
                'search_selected_platforms_objects': [
                    p for p in platforms if p.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_platforms',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('search_platforms', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_search_game_types(game_type_choices, selected_ids: List[int]) -> str:
        """Рендерит секцию типов игр для поиска."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_game_types_section.html', {
                'game_type_choices': game_type_choices,
                'search_selected_game_types': selected_ids,
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_game_types',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('search_game_types', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_search_genres(genres: List[Genre], selected_ids: List[int]) -> str:
        """Рендерит секцию жанров для поиска."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_genres_section.html', {
                'genres': genres,
                'search_selected_genres': selected_ids,
                'search_selected_genres_objects': [
                    g for g in genres if g.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_genres',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('search_genres', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_search_keywords(keywords: List[Keyword], selected_ids: List[int]) -> str:
        """Рендерит секцию ключевых слов для поиска."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_keywords_section.html', {
                'keywords': keywords,
                'search_selected_keywords': selected_ids,
                'search_selected_keywords_objects': [
                    k for k in keywords if k.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_keywords',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('search_keywords', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_search_themes(themes: List[Theme], selected_ids: List[int]) -> str:
        """Рендерит секцию тем для поиска."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_themes_section.html', {
                'themes': themes,
                'search_selected_themes': selected_ids,
                'search_selected_themes_objects': [
                    t for t in themes if t.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_themes',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('search_themes', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_search_perspectives(perspectives: List[PlayerPerspective], selected_ids: List[int]) -> str:
        """Рендерит секцию перспектив для поиска."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_perspectives_section.html', {
                'perspectives': perspectives,
                'search_selected_perspectives': selected_ids,
                'search_selected_perspectives_objects': [
                    p for p in perspectives if p.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_perspectives',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('search_perspectives', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_search_game_modes(game_modes: List[GameMode], selected_ids: List[int]) -> str:
        """Рендерит секцию режимов игры для поиска."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_game_modes_section.html', {
                'game_modes': game_modes,
                'search_selected_game_modes': selected_ids,
                'search_selected_game_modes_objects': [
                    gm for gm in game_modes if gm.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_game_modes',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('search_game_modes', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_search_engines(engines: List[GameEngine], selected_ids: List[int]) -> str:
        """Рендерит секцию движков для поиска."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_engines_section.html', {
                'engines': engines,
                'search_selected_engines': selected_ids,
                'search_selected_engines_objects': [
                    e for e in engines if e.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_engines',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('search_engines', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_search_date_filter(year_start: Optional[int], year_end: Optional[int],
                                  min_year: int, max_year: int, current_year: int) -> str:
        """Рендерит секцию фильтра даты для поиска."""
        context_data = {
            'release_year_start': year_start,
            'release_year_end': year_end
        }

        def render_func():
            return render_to_string('games/game_list/filter_sections/_search_date_section.html', {
                'search_selected_release_year_start': year_start,
                'search_selected_release_year_end': year_end,
                'years_range': {'min_year': min_year, 'max_year': max_year},
                'current_year': current_year
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='search_date',
            render_func=render_func,
            context_data=context_data
        )
        FilterRenderer._log_cache_status('search_date', was_cached)
        return mark_safe(html)

    @staticmethod
    def render_similarity_genres(genres: List[Genre], selected_ids: List[int]) -> str:
        """Рендерит секцию жанров для похожести."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_similarity_genres_section.html', {
                'genres': genres,
                'similarity_selected_genres': selected_ids,
                'similarity_selected_genres_objects': [
                    g for g in genres if g.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='similarity_genres',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('similarity_genres', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_similarity_keywords(keywords: List[Keyword], selected_ids: List[int]) -> str:
        """Рендерит секцию ключевых слов для похожести."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_similarity_keywords_section.html', {
                'keywords': keywords,
                'similarity_selected_keywords': selected_ids,
                'similarity_selected_keywords_objects': [
                    k for k in keywords if k.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='similarity_keywords',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('similarity_keywords', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_similarity_themes(themes: List[Theme], selected_ids: List[int]) -> str:
        """Рендерит секцию тем для похожести."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_similarity_themes_section.html', {
                'themes': themes,
                'similarity_selected_themes': selected_ids,
                'similarity_selected_themes_objects': [
                    t for t in themes if t.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='similarity_themes',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('similarity_themes', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_similarity_perspectives(perspectives: List[PlayerPerspective], selected_ids: List[int]) -> str:
        """Рендерит секцию перспектив для похожести."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_similarity_perspectives_section.html', {
                'perspectives': perspectives,
                'similarity_selected_perspectives': selected_ids,
                'similarity_selected_perspectives_objects': [
                    p for p in perspectives if p.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='similarity_perspectives',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('similarity_perspectives', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_similarity_game_modes(game_modes: List[GameMode], selected_ids: List[int]) -> str:
        """Рендерит секцию режимов игры для похожести."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_similarity_game_modes_section.html', {
                'game_modes': game_modes,
                'similarity_selected_game_modes': selected_ids,
                'similarity_selected_game_modes_objects': [
                    gm for gm in game_modes if gm.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='similarity_game_modes',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('similarity_game_modes', was_cached, selected_ids)
        return mark_safe(html)

    @staticmethod
    def render_similarity_engines(engines: List[GameEngine], selected_ids: List[int]) -> str:
        """Рендерит секцию движков для похожести."""

        def render_func():
            return render_to_string('games/game_list/filter_sections/_similarity_engines_section.html', {
                'engines': engines,
                'similarity_selected_engines': selected_ids,
                'similarity_selected_engines_objects': [
                    e for e in engines if e.id in selected_ids
                ]
            })

        html, was_cached = FilterSectionCache.get_or_create_section(
            section_type='similarity_engines',
            render_func=render_func,
            selected_ids=selected_ids
        )
        FilterRenderer._log_cache_status('similarity_engines', was_cached, selected_ids)
        return mark_safe(html)