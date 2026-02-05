"""Optimized views for game similarity search with updated models."""

# Re-export all views from parts
from .views_parts.game_list_views import (
    ajax_load_games_page,
    game_list,
    get_similar_games_for_criteria,
    get_similar_games_for_game,
    get_source_game,
)
from .views_parts.game_detail_views import (
    game_detail,
)
from .views_parts.comparison_views import (
    game_comparison,
)
from .views_parts.other_views import (
    home,
    keyword_category_view,
    game_search,
    platform_list,
    platform_games,
    auto_login_admin,
)
from .views_parts.analyze_views import (
    analyze_single_game,
    clear_analysis_results,
    is_staff_or_superuser,
    delete_keyword,
    get_current_keywords,
    get_found_keywords,
)
from .views_parts.base_views import (
    # Helper classes
    SimpleSourceGame,

    # Cache management
    warm_cache_for_home_page,
    clear_game_list_cache,
    get_cache_stats,

    # Cache decorators
    cache_view,
    cache_method,

    # Cache functions
    get_cache_key,
    cache_get_or_set,
    invalidate_cache_group,
    cache_multi_get,
    cache_multi_set,

    # Helper functions
    extract_request_params,
    convert_params_to_lists,
    _apply_filters,
    get_objects_by_ids,

    # Year range
    get_release_years_range,
)

# Export everything
__all__ = [
    # Views
    'ajax_load_games_page',
    'game_list',
    'game_detail',
    'game_comparison',
    'home',
    'keyword_category_view',
    'game_search',
    'platform_list',
    'platform_games',
    'auto_login_admin',
    'analyze_single_game',
    'clear_analysis_results',
    'is_staff_or_superuser',
    'delete_keyword',
    'get_current_keywords',
    'get_found_keywords',

    # Helper classes
    'SimpleSourceGame',

    # Cache management
    'warm_cache_for_home_page',
    'clear_game_list_cache',
    'get_cache_stats',

    # Cache decorators
    'cache_view',
    'cache_method',

    # Cache functions
    'get_cache_key',
    'cache_get_or_set',
    'invalidate_cache_group',
    'cache_multi_get',
    'cache_multi_set',

    # Helper functions
    'extract_request_params',
    'convert_params_to_lists',
    '_apply_filters',
    'get_objects_by_ids',

    # Similarity functions
    'get_similar_games_for_criteria',
    'get_similar_games_for_game',
    'get_source_game',

    # Year range
    'get_release_years_range',
]