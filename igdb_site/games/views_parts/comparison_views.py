"""Views for game comparison."""

import logging
from typing import Dict, List, Any, Optional
from django.shortcuts import render, get_object_or_404
from django.http import HttpRequest, HttpResponse, HttpResponseServerError
from django.db.models import Prefetch

from .base_views import (
    convert_params_to_lists, SimpleSourceGame,
    GameSimilarity, VirtualGame
)
# Импортируем все нужные модели
from ..models import (
    Game, Genre, Keyword, Theme, PlayerPerspective,
    Company, GameMode, Platform
)

logger = logging.getLogger(__name__)


def _get_game2_object(pk2: int) -> Game:
    """Get second game object with optimized prefetching."""
    return get_object_or_404(
        Game.objects.prefetch_related(
            Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
            Prefetch('platforms', queryset=Platform.objects.only('id', 'name')),
            Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
            Prefetch('developers', queryset=Company.objects.only('id', 'name')),
            Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
            Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
        ),
        pk=pk2
    )


def _get_game1_or_criteria(request: HttpRequest, selected_criteria: dict) -> tuple:
    """
    Get first game object or criteria as source.

    Returns:
        tuple: (source_object, is_criteria_comparison, game1_object)
    """
    source_game_id = request.GET.get('source_game')
    game1 = None

    if source_game_id and source_game_id.strip() and source_game_id.strip().lower() != 'none':
        try:
            game1 = Game.objects.only(
                'id', 'name', 'game_type'
            ).prefetch_related(
                'genres', 'themes',
                'developers', 'player_perspectives', 'game_modes'
            ).get(pk=int(source_game_id))
            return game1, False, game1
        except (Game.DoesNotExist, ValueError):
            game1 = None

    # Return criteria-based source
    source = VirtualGame(
        genre_ids=selected_criteria['genres'],
        keyword_ids=selected_criteria['keywords'],
        theme_ids=selected_criteria['themes'],
        perspective_ids=selected_criteria['perspectives'],
        developer_ids=selected_criteria['developers'],
        game_mode_ids=selected_criteria['game_modes']
    )
    return source, True, None


def _load_criteria_objects(selected_criteria: dict) -> dict:
    """Load all criteria objects from database."""
    return {
        'genres': list(Genre.objects.filter(id__in=selected_criteria['genres']).only('id', 'name')) if
        selected_criteria['genres'] else [],
        'keywords': list(Keyword.objects.filter(id__in=selected_criteria['keywords']).only('id', 'name')) if
        selected_criteria['keywords'] else [],
        'themes': list(Theme.objects.filter(id__in=selected_criteria['themes']).only('id', 'name')) if
        selected_criteria['themes'] else [],
        'perspectives': list(
            PlayerPerspective.objects.filter(id__in=selected_criteria['perspectives']).only('id', 'name')) if
        selected_criteria['perspectives'] else [],
        'developers': list(Company.objects.filter(id__in=selected_criteria['developers']).only('id', 'name')) if
        selected_criteria['developers'] else [],
        'game_modes': list(GameMode.objects.filter(id__in=selected_criteria['game_modes']).only('id', 'name')) if
        selected_criteria['game_modes'] else [],
    }


def _calculate_similarity_if_needed(source, target_game, saved_similarity: float, is_criteria: bool) -> tuple:
    """
    Calculate similarity score and breakdown data.

    Returns:
        tuple: (similarity_score, similarity_data, breakdown)
    """
    similarity_engine = GameSimilarity()

    if saved_similarity > 0:
        try:
            similarity_data = similarity_engine.get_similarity_formula(source, target_game)
            breakdown = similarity_engine.get_similarity_breakdown(source,
                                                                   target_game) if saved_similarity > 0 else None
            return saved_similarity, similarity_data, breakdown
        except Exception as e:
            logger.error(f"Error getting similarity data: {e}")
            similarity_data = {
                'criteria': [],
                'bonus': None,
                'total': saved_similarity,
                'total_from_criteria': saved_similarity,
                'error': str(e)
            }
            return saved_similarity, similarity_data, None

    # Calculate from scratch
    similarity_data = similarity_engine.get_similarity_formula(source, target_game)
    similarity_score = similarity_data['total'] if similarity_data else 0
    breakdown = similarity_engine.get_similarity_breakdown(source, target_game) if similarity_score > 0 else None

    print(f"Comparison similarity calculated via formula: {similarity_score}")
    direct_similarity = similarity_engine.calculate_similarity(source, target_game)
    print(f"Direct calculate_similarity result: {direct_similarity}")

    if not is_criteria and similarity_score > 0:
        breakdown = similarity_engine.get_similarity_breakdown(source, target_game)
        print(f"Breakdown total similarity: {breakdown.get('total_similarity', 0)}")

    return similarity_score, similarity_data, breakdown


def _calculate_shared_items(game1, game2, selected_criteria: dict, is_criteria: bool, breakdown: dict = None) -> dict:
    """Calculate shared items between two sources using breakdown data if available."""
    fields_to_compare = ['genres', 'keywords', 'themes', 'perspectives', 'developers', 'game_modes']
    shared_items = {}

    # Если есть breakdown с common_elements, используем его напрямую
    if breakdown and breakdown.get('keywords', {}).get('common_elements'):
        # Загружаем объекты по ID из breakdown
        keyword_ids = breakdown['keywords']['common_elements']
        if keyword_ids:
            shared_items['keywords'] = list(Keyword.objects.filter(id__in=keyword_ids).only('id', 'name'))
        else:
            shared_items['keywords'] = []

        # Для остальных полей тоже можно использовать breakdown если нужно
        for field in fields_to_compare:
            if field != 'keywords' and breakdown.get(field, {}).get('common_elements'):
                field_ids = breakdown[field]['common_elements']
                if field_ids:
                    model_map = {
                        'genres': Genre,
                        'themes': Theme,
                        'perspectives': PlayerPerspective,
                        'developers': Company,
                        'game_modes': GameMode
                    }
                    if field in model_map:
                        shared_items[field] = list(model_map[field].objects.filter(id__in=field_ids).only('id', 'name'))
                    else:
                        shared_items[field] = []
                else:
                    shared_items[field] = []
            elif field not in shared_items:
                shared_items[field] = []

    # Если breakdown нет, вычисляем обычным способом
    else:
        if is_criteria:
            for field in fields_to_compare:
                if field == 'perspectives':
                    game_field = game2.player_perspectives.all()
                else:
                    game_field = getattr(game2, field).all()

                criteria_ids = selected_criteria[field]
                if criteria_ids:
                    model_map = {
                        'genres': Genre,
                        'keywords': Keyword,
                        'themes': Theme,
                        'perspectives': PlayerPerspective,
                        'developers': Company,
                        'game_modes': GameMode
                    }
                    model = model_map[field]
                    criteria_objects = model.objects.filter(id__in=criteria_ids)
                    shared_items[field] = list(game_field & criteria_objects)
                else:
                    shared_items[field] = []
        else:
            for field in fields_to_compare:
                if field == 'perspectives':
                    field1 = game1.player_perspectives.all()
                    field2 = game2.player_perspectives.all()
                    shared_items[field] = list(field1 & field2)
                elif field == 'keywords':
                    game1_keyword_ids = set(game1.keyword_ids or [])
                    game2_keyword_ids = set(game2.keyword_ids or [])
                    common_ids = game1_keyword_ids & game2_keyword_ids

                    if common_ids:
                        shared_items[field] = list(Keyword.objects.filter(igdb_id__in=common_ids).only('id', 'name'))
                    else:
                        shared_items[field] = []
                else:
                    field1 = getattr(game1, field).all()
                    field2 = getattr(game2, field).all()
                    shared_items[field] = list(field1 & field2)

    # Debug output
    print(f"\n=== SHARED ITEMS DEBUG ===")
    for field in fields_to_compare:
        print(f"{field}: {len(shared_items.get(field, []))} shared items")
    print(f"==========================\n")

    return shared_items


def _build_comparison_context(game1, game2, selected_criteria: dict, criteria_objects: dict,
                              similarity_score: float, similarity_data: dict, breakdown: dict,
                              shared_items: dict, is_criteria: bool) -> dict:
    """Build context dictionary for comparison template."""
    context = {
        'game1': game1,
        'game2': game2,
        'similarity_score': similarity_score,
        'is_criteria_comparison': is_criteria,
        'breakdown': breakdown,
        'similarity_data': similarity_data,
        'selected_criteria': selected_criteria,

        'criteria_genres': criteria_objects['genres'],
        'criteria_keywords': criteria_objects['keywords'],
        'criteria_themes': criteria_objects['themes'],
        'criteria_perspectives': criteria_objects['perspectives'],
        'criteria_developers': criteria_objects['developers'],
        'criteria_game_modes': criteria_objects['game_modes'],

        'criteria_genres_ids': selected_criteria['genres'],
        'criteria_keywords_ids': selected_criteria['keywords'],
        'criteria_themes_ids': selected_criteria['themes'],
        'criteria_perspectives_ids': selected_criteria['perspectives'],
        'criteria_developers_ids': selected_criteria['developers'],
        'criteria_game_modes_ids': selected_criteria['game_modes'],
    }

    # Add criteria virtual game wrapper for criteria comparison
    if is_criteria:
        class CriteriaWrapper:
            def __init__(self, criteria):
                self.id = None
                self.name = "Search Criteria"
                self.genres = criteria['genres']
                self.keywords = criteria['keywords']
                self.themes = criteria['themes']
                self.player_perspectives = criteria['perspectives']
                self.developers = criteria['developers']
                self.game_modes = criteria['game_modes']

            def genres_list(self):
                return self.genres

            def keywords_list(self):
                return self.keywords

            def themes_list(self):
                return self.themes

            def perspectives_list(self):
                return self.player_perspectives

            def developers_list(self):
                return self.developers

            def game_modes_list(self):
                return self.game_modes

        context['criteria_virtual_game'] = CriteriaWrapper(selected_criteria)
    else:
        context['criteria_virtual_game'] = game1

    # Add shared items to context
    for field, items in shared_items.items():
        context[f'shared_{field}'] = items
        context[f'shared_{field}_count'] = len(items)

    # Add selected criteria objects
    context.update({
        'selected_genres': selected_criteria['genres'],
        'selected_keywords': selected_criteria['keywords'],
        'selected_themes': selected_criteria['themes'],
        'selected_perspectives': selected_criteria['perspectives'],
        'selected_developers': selected_criteria['developers'],
        'selected_game_modes': selected_criteria['game_modes'],

        'selected_genres_objects': criteria_objects['genres'],
        'selected_keywords_objects': criteria_objects['keywords'],
        'selected_themes_objects': criteria_objects['themes'],
        'selected_perspectives_objects': criteria_objects['perspectives'],
        'selected_developers_objects': criteria_objects['developers'],
        'selected_game_modes_objects': criteria_objects['game_modes'],
    })

    # Set average similarity for criteria comparison
    context['average_similarity'] = similarity_score if is_criteria else None

    # Debug output
    print(f"Context shared_perspectives_count: {context.get('shared_perspectives_count', 0)}")
    print(f"Context shared_game_modes_count: {context.get('shared_game_modes_count', 0)}")
    print(f"Context shared_developers_count: {context.get('shared_developers_count', 0)}")
    print(f"Final similarity_score for template: {similarity_score}")
    print(f"========================================\n")

    return context


def game_comparison(request: HttpRequest, pk2: int) -> HttpResponse:
    """Universal comparison: game-game or criteria-game."""
    try:
        # Get second game
        game2 = _get_game2_object(pk2)

        # Parse criteria from request
        selected_criteria = convert_params_to_lists(request.GET)

        # Get source (game or criteria)
        source, is_criteria_comparison, game1 = _get_game1_or_criteria(request, selected_criteria)

        # Load criteria objects for display
        criteria_objects = _load_criteria_objects(selected_criteria)

        # Get saved similarity score if exists
        saved_similarity = request.GET.get('similarity', 0)
        try:
            saved_similarity = float(saved_similarity)
        except (ValueError, TypeError):
            saved_similarity = 0

        # Calculate similarity
        similarity_score, similarity_data, breakdown = _calculate_similarity_if_needed(
            source, game2, saved_similarity, is_criteria_comparison
        )

        # Calculate shared items (передаем breakdown)
        shared_items = _calculate_shared_items(game1, game2, selected_criteria, is_criteria_comparison, breakdown)

        # Build context
        context = _build_comparison_context(
            game1, game2, selected_criteria, criteria_objects,
            similarity_score, similarity_data, breakdown,
            shared_items, is_criteria_comparison
        )

        return render(request, 'games/game_comparison.html', context)

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Error in comparison: {str(e)}")
        logger.error(f"Details: {error_details}")
        return HttpResponseServerError(f"Error in comparison: {str(e)}")
