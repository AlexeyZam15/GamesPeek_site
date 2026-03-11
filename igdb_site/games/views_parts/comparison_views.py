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

def game_comparison(request: HttpRequest, pk2: int) -> HttpResponse:
    """Universal comparison: game-game or criteria-game."""
    try:
        keyword_prefetch = Prefetch('keywords', queryset=Keyword.objects.select_related('category'))

        game2 = get_object_or_404(
            Game.objects.prefetch_related(
                keyword_prefetch,
                Prefetch('genres', queryset=Genre.objects.only('id', 'name')),
                Prefetch('platforms', queryset=Platform.objects.only('id', 'name')),
                Prefetch('themes', queryset=Theme.objects.only('id', 'name')),
                Prefetch('developers', queryset=Company.objects.only('id', 'name')),
                Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name')),
                Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name')),
            ),
            pk=pk2
        )

        source_game_id = request.GET.get('source_game')
        game1 = None

        if source_game_id and source_game_id.strip() and source_game_id.strip().lower() != 'none':
            try:
                game1 = Game.objects.only(
                    'id', 'name', 'game_type'
                ).prefetch_related(
                    'genres', 'keywords', 'themes',
                    'developers', 'player_perspectives', 'game_modes'
                ).get(pk=int(source_game_id))
            except (Game.DoesNotExist, ValueError):
                game1 = None

        selected_criteria = convert_params_to_lists(request.GET)

        saved_similarity = request.GET.get('similarity')
        if saved_similarity:
            try:
                similarity_score = float(saved_similarity)
            except (ValueError, TypeError):
                similarity_score = 0
        else:
            similarity_score = 0

        criteria_genres_objs = Genre.objects.filter(id__in=selected_criteria['genres']).only('id', 'name') if \
            selected_criteria['genres'] else []
        criteria_keywords_objs = Keyword.objects.filter(id__in=selected_criteria['keywords']).only('id', 'name') if \
            selected_criteria['keywords'] else []
        criteria_themes_objs = Theme.objects.filter(id__in=selected_criteria['themes']).only('id', 'name') if \
            selected_criteria['themes'] else []
        criteria_perspectives_objs = PlayerPerspective.objects.filter(id__in=selected_criteria['perspectives']).only(
            'id', 'name') if selected_criteria['perspectives'] else []
        criteria_developers_objs = Company.objects.filter(id__in=selected_criteria['developers']).only('id', 'name') if \
            selected_criteria['developers'] else []
        criteria_game_modes_objs = GameMode.objects.filter(id__in=selected_criteria['game_modes']).only('id', 'name') if \
            selected_criteria['game_modes'] else []

        if game1:
            is_criteria_comparison = False
            source = game1
        else:
            is_criteria_comparison = True
            source = VirtualGame(
                genre_ids=selected_criteria['genres'],
                keyword_ids=selected_criteria['keywords'],
                theme_ids=selected_criteria['themes'],
                perspective_ids=selected_criteria['perspectives'],
                developer_ids=selected_criteria['developers'],
                game_mode_ids=selected_criteria['game_modes']
            )

        if similarity_score == 0:
            similarity_engine = GameSimilarity()

            if is_criteria_comparison:
                virtual_game = VirtualGame(
                    genre_ids=selected_criteria['genres'],
                    keyword_ids=selected_criteria['keywords'],
                    theme_ids=selected_criteria['themes'],
                    perspective_ids=selected_criteria['perspectives'],
                    developer_ids=selected_criteria['developers'],
                    game_mode_ids=selected_criteria['game_modes']
                )

                similar_games = similarity_engine.find_similar_games(
                    source_game=virtual_game,
                    min_similarity=0,
                    limit=1000
                )

                for game_data in similar_games:
                    if isinstance(game_data, dict) and game_data.get('game') and game_data['game'].id == game2.id:
                        similarity_score = game_data.get('similarity', 0)
                        break
                    elif hasattr(game_data, 'id') and game_data.id == game2.id:
                        similarity_score = getattr(game_data, 'similarity', 0)
                        break

                if similarity_score == 0:
                    similarity_score = similarity_engine.calculate_similarity(virtual_game, game2)
            else:
                similar_games = similarity_engine.find_similar_games(
                    source_game=game1,
                    min_similarity=0,
                    limit=1000
                )

                for game_data in similar_games:
                    if isinstance(game_data, dict) and game_data.get('game') and game_data['game'].id == game2.id:
                        similarity_score = game_data.get('similarity', 0)
                        break
                    elif hasattr(game_data, 'id') and game_data.id == game2.id:
                        similarity_score = getattr(game_data, 'similarity', 0)
                        break

                if similarity_score == 0:
                    similarity_score = similarity_engine.calculate_similarity(game1, game2)

        breakdown = None
        similarity_data = None
        if similarity_score > 0:
            similarity_engine = GameSimilarity()
            breakdown = similarity_engine.get_similarity_breakdown(source if is_criteria_comparison else game1, game2)
            try:
                similarity_data = similarity_engine.get_similarity_formula(source if is_criteria_comparison else game1,
                                                                           game2)
            except Exception as e:
                logger.error(f"Error getting similarity data: {e}")
                similarity_data = {
                    'criteria': [],
                    'bonus': None,
                    'total': similarity_score,
                    'total_from_criteria': similarity_score,
                    'error': str(e)
                }

        shared_items = {}
        fields_to_compare = ['genres', 'keywords', 'themes', 'perspectives', 'developers', 'game_modes']

        if is_criteria_comparison:
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
                else:
                    field1 = getattr(game1, field).all()
                    field2 = getattr(game2, field).all()

                shared_items[field] = list(field1 & field2)

        context = {
            'game1': game1,
            'game2': game2,
            'similarity_score': similarity_score,
            'is_criteria_comparison': is_criteria_comparison,
            'breakdown': breakdown,
            'similarity_data': similarity_data,
            'selected_criteria': selected_criteria,

            'criteria_genres': list(criteria_genres_objs),
            'criteria_keywords': list(criteria_keywords_objs),
            'criteria_themes': list(criteria_themes_objs),
            'criteria_perspectives': list(criteria_perspectives_objs),
            'criteria_developers': list(criteria_developers_objs),
            'criteria_game_modes': list(criteria_game_modes_objs),

            'criteria_genres_ids': selected_criteria['genres'],
            'criteria_keywords_ids': selected_criteria['keywords'],
            'criteria_themes_ids': selected_criteria['themes'],
            'criteria_perspectives_ids': selected_criteria['perspectives'],
            'criteria_developers_ids': selected_criteria['developers'],
            'criteria_game_modes_ids': selected_criteria['game_modes'],
        }

        if is_criteria_comparison:
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

        for field, items in shared_items.items():
            context[f'shared_{field}'] = items
            context[f'shared_{field}_count'] = len(items)

        context.update({
            'selected_genres': selected_criteria['genres'],
            'selected_keywords': selected_criteria['keywords'],
            'selected_themes': selected_criteria['themes'],
            'selected_perspectives': selected_criteria['perspectives'],
            'selected_developers': selected_criteria['developers'],
            'selected_game_modes': selected_criteria['game_modes'],

            'selected_genres_objects': list(criteria_genres_objs),
            'selected_keywords_objects': list(criteria_keywords_objs),
            'selected_themes_objects': list(criteria_themes_objs),
            'selected_perspectives_objects': list(criteria_perspectives_objs),
            'selected_developers_objects': list(criteria_developers_objs),
            'selected_game_modes_objects': list(criteria_game_modes_objs),
        })

        if is_criteria_comparison:
            context['average_similarity'] = similarity_score
        else:
            context['average_similarity'] = None

        return render(request, 'games/game_comparison.html', context)

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Error in comparison: {str(e)}")
        logger.error(f"Details: {error_details}")
        return HttpResponseServerError(f"Error in comparison: {str(e)}")
