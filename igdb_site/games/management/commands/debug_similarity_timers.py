"""
Django management command for debugging similarity search with detailed timers.
Supports searching by game name (finds the most popular game matching the name).
"""
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db.models import Q
from games.models import Game, GameMode
from games.similarity import GameSimilarity
import time
import logging
from collections import OrderedDict

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Debug similarity search with detailed timers for each stage'

    def add_arguments(self, parser):
        # Game identification (mutually exclusive)
        parser.add_argument('--game-id', type=int, help='ID of the source game')
        parser.add_argument('--game-name', type=str,
                            help='Name of the source game - finds the most popular game matching this name')

        # Virtual game mode
        parser.add_argument('--virtual', action='store_true', help='Use virtual game with criteria')

        # Criteria for virtual game
        parser.add_argument('--genres', type=str, help='Comma-separated genre IDs')
        parser.add_argument('--keywords', type=str, help='Comma-separated keyword IDs')
        parser.add_argument('--themes', type=str, help='Comma-separated theme IDs')
        parser.add_argument('--perspectives', type=str, help='Comma-separated perspective IDs')
        parser.add_argument('--game-modes', type=str, help='Comma-separated game mode IDs')
        parser.add_argument('--engines', type=str, help='Comma-separated engine IDs')

        # Search options
        parser.add_argument('--min-similarity', type=float, default=None, help='Minimum similarity threshold')
        parser.add_argument('--limit', type=int, default=500, help='Result limit')
        parser.add_argument('--exact', action='store_true', help='Use exact name match instead of partial')

    def handle(self, *args, **options):
        """Main command handler."""
        # Store timers at the VERY BEGINNING to ensure they exist
        timers = OrderedDict()
        name_search_time = 0
        total_start = time.time()

        game_id = options.get('game_id')
        game_name = options.get('game_name')
        virtual = options.get('virtual', False)
        exact_match = options.get('exact', False)

        # Parse criteria for virtual game
        genre_ids = self._parse_ids(options.get('genres'))
        keyword_ids = self._parse_ids(options.get('keywords'))
        theme_ids = self._parse_ids(options.get('themes'))
        perspective_ids = self._parse_ids(options.get('perspectives'))
        game_mode_ids = self._parse_ids(options.get('game_modes'))
        engine_ids = self._parse_ids(options.get('engines'))

        min_similarity = options.get('min_similarity')
        limit = options.get('limit', 500)

        self.stdout.write(self.style.SUCCESS('=' * 80))
        self.stdout.write(self.style.SUCCESS('DEBUG SIMILARITY SEARCH WITH TIMERS'))
        self.stdout.write(self.style.SUCCESS('=' * 80))

        # Validate input
        if not any([game_id, game_name, virtual]):
            self.stdout.write(self.style.ERROR('Please provide --game-id, --game-name, or --virtual'))
            self.stdout.write('Examples:')
            self.stdout.write('  python manage.py debug_similarity_timers --game-id 2828')
            self.stdout.write('  python manage.py debug_similarity_timers --game-name "GTA"')
            self.stdout.write('  python manage.py debug_similarity_timers --game-name "Grand Theft Auto" --exact')
            self.stdout.write('  python manage.py debug_similarity_timers --virtual --genres 5,12')
            return

        # Get source game
        source_game = None

        if virtual:
            source_game = self._create_virtual_game(
                genre_ids, keyword_ids, theme_ids,
                perspective_ids, game_mode_ids, engine_ids
            )
            self.stdout.write(f"Source: VIRTUAL GAME")
            self.stdout.write(f"  Genres: {genre_ids}")
            self.stdout.write(f"  Keywords: {keyword_ids}")
            self.stdout.write(f"  Themes: {theme_ids}")
            self.stdout.write(f"  Perspectives: {perspective_ids}")
            self.stdout.write(f"  Game modes: {game_mode_ids}")
            self.stdout.write(f"  Engines: {engine_ids}")

        elif game_id:
            name_search_start = time.time()
            try:
                source_game = Game.objects.get(id=game_id)
                name_search_time = time.time() - name_search_start
                timers['0. Get game by ID'] = name_search_time
                self._display_game_info(source_game, "Source")
            except Game.DoesNotExist:
                self.stdout.write(self.style.ERROR(f"Game with ID {game_id} not found"))
                self._display_all_timers(timers, name_search_time, total_start)
                return

        elif game_name:
            # Search for games by name
            name_search_start = time.time()
            self.stdout.write(f"Searching for games matching: '{game_name}'")

            if exact_match:
                games = Game.objects.filter(name__iexact=game_name)
            else:
                games = Game.objects.filter(name__icontains=game_name)

            # Order by popularity (rating_count) and get the most popular
            games = games.filter(rating_count__gt=0).order_by('-rating_count')

            count = games.count()
            name_search_time = time.time() - name_search_start
            timers['0. Name search'] = name_search_time

            if count == 0:
                self.stdout.write(self.style.ERROR(f"No games found matching '{game_name}'"))
                self._display_all_timers(timers, name_search_time, total_start)
                return

            # Show all matching games
            self.stdout.write(f"\nFound {count} game(s) matching '{game_name}' (search took {name_search_time:.4f}s):")
            for i, game in enumerate(games[:10]):  # Show first 10
                self.stdout.write(
                    f"  {i + 1}. {game.name} (ID: {game.id}) - Rating count: {game.rating_count}, Rating: {game.rating}")

            if count > 10:
                self.stdout.write(f"  ... and {count - 10} more")

            # Take the most popular one
            source_game = games.first()
            self.stdout.write("\n" + "-" * 40)
            self.stdout.write(self.style.SUCCESS(f"Selected MOST POPULAR game:"))
            self._display_game_info(source_game, "Selected")

        self.stdout.write('-' * 80)

        # Initialize similarity engine
        similarity_engine = GameSimilarity()

        # STAGE 1: Prepare source data
        stage_start = time.time()
        source_data, single_player_info = similarity_engine._prepare_source_data(source_game)
        stage_time = time.time() - stage_start
        timers['1. _prepare_source_data'] = stage_time
        self.stdout.write(f"STAGE 1: _prepare_source_data - {stage_time:.4f} seconds")
        self.stdout.write(f"  Genre count: {source_data['genre_count']}")
        self.stdout.write(f"  Keyword count: {source_data['keyword_count']}")
        self.stdout.write(f"  Theme count: {source_data['theme_count']}")
        self.stdout.write(f"  Perspective count: {source_data['perspective_count']}")
        self.stdout.write(f"  Game mode count: {source_data['game_mode_count']}")
        self.stdout.write(f"  Engine count: {source_data['engine_count']}")
        self.stdout.write(f"  Has single player: {single_player_info['has_single_player']}")
        self.stdout.write(f"  Min common genres: {single_player_info['dynamic_min_common_genres']}")

        # Show source data IDs for verification
        self.stdout.write(f"\n  Source IDs:")
        self.stdout.write(
            f"    Genres: {source_data['genre_ids'][:20]}{'...' if len(source_data['genre_ids']) > 20 else ''}")
        self.stdout.write(
            f"    Keywords: {source_data['keyword_ids'][:20]}{'...' if len(source_data['keyword_ids']) > 20 else ''}")
        self.stdout.write(
            f"    Themes: {source_data['theme_ids'][:20]}{'...' if len(source_data['theme_ids']) > 20 else ''}")
        self.stdout.write(
            f"    Perspectives: {source_data['perspective_ids'][:20]}{'...' if len(source_data['perspective_ids']) > 20 else ''}")
        self.stdout.write(
            f"    Game modes: {source_data['game_mode_ids'][:20]}{'...' if len(source_data['game_mode_ids']) > 20 else ''}")
        self.stdout.write(
            f"    Engines: {source_data['engine_ids'][:20]}{'...' if len(source_data['engine_ids']) > 20 else ''}")

        # STAGE 2: Get candidate IDs
        stage_start = time.time()
        candidate_ids = similarity_engine._get_candidate_ids_new(
            source_data, single_player_info, min_similarity
        )
        stage_time = time.time() - stage_start
        timers['2. _get_candidate_ids_new'] = stage_time
        self.stdout.write(f"\nSTAGE 2: _get_candidate_ids_new - {stage_time:.4f} seconds")
        self.stdout.write(f"  Candidates found: {len(candidate_ids)}")
        if len(candidate_ids) > 0:
            self.stdout.write(f"  First 20 candidates: {candidate_ids[:20]}")

        # Check if source game is in candidates
        if hasattr(source_game, 'id') and source_game.id in candidate_ids[:20]:
            self.stdout.write(f"  ✅ Source game ID {source_game.id} is in candidates")
        elif hasattr(source_game, 'id'):
            self.stdout.write(f"  ❌ Source game ID {source_game.id} NOT in first 20 candidates")

        # STAGE 3: Prepare candidate data
        stage_start = time.time()
        games_data = similarity_engine._prepare_candidate_data(candidate_ids)
        stage_time = time.time() - stage_start
        timers['3. _prepare_candidate_data'] = stage_time
        self.stdout.write(f"\nSTAGE 3: _prepare_candidate_data - {stage_time:.4f} seconds")
        self.stdout.write(f"  Games prepared: {len(games_data)}")

        # STAGE 4: Calculate common elements
        stage_start = time.time()
        games_data = similarity_engine._calculate_common_elements_new(
            games_data, source_data, candidate_ids
        )
        stage_time = time.time() - stage_start
        timers['4. _calculate_common_elements_new'] = stage_time
        self.stdout.write(f"\nSTAGE 4: _calculate_common_elements_new - {stage_time:.4f} seconds")

        # Show sample of common elements
        if games_data:
            # Show source game data first if available
            if hasattr(source_game, 'id') and source_game.id in games_data:
                source_sample = games_data[source_game.id]
                self.stdout.write(f"\n  Source game ID {source_game.id} ({source_game.name}):")
                self.stdout.write(f"    Common genres: {source_sample.get('common_genres', 0)}")
                self.stdout.write(f"    Common keywords: {source_sample.get('common_keywords', 0)}")
                self.stdout.write(f"    Common themes: {source_sample.get('common_themes', 0)}")
                self.stdout.write(f"    Common perspectives: {source_sample.get('common_perspectives', 0)}")
                self.stdout.write(f"    Common game modes: {source_sample.get('common_game_modes', 0)}")
                self.stdout.write(f"    Common engines: {source_sample.get('common_engines', 0)}")
                self.stdout.write(f"    Has single player: {source_sample.get('has_single_player', False)}")

            # Show a random candidate
            sample_id = next((id for id in games_data.keys() if id != getattr(source_game, 'id', None)), None)
            if sample_id:
                sample = games_data[sample_id]
                sample_game = Game.objects.filter(id=sample_id).first()
                self.stdout.write(
                    f"\n  Sample candidate ID {sample_id} ({sample_game.name if sample_game else 'Unknown'}):")
                self.stdout.write(f"    Common genres: {sample.get('common_genres', 0)}")
                self.stdout.write(f"    Common keywords: {sample.get('common_keywords', 0)}")
                self.stdout.write(f"    Common themes: {sample.get('common_themes', 0)}")
                self.stdout.write(f"    Common perspectives: {sample.get('common_perspectives', 0)}")
                self.stdout.write(f"    Common game modes: {sample.get('common_game_modes', 0)}")
                self.stdout.write(f"    Common engines: {sample.get('common_engines', 0)}")
                self.stdout.write(f"    Has single player: {sample.get('has_single_player', False)}")

        # STAGE 5: Calculate similarity for candidates
        stage_start = time.time()
        similar_games = similarity_engine._calculate_similarity_for_candidates(
            games_data, source_data, source_game, single_player_info
        )
        stage_time = time.time() - stage_start
        timers['5. _calculate_similarity_for_candidates'] = stage_time
        self.stdout.write(f"\nSTAGE 5: _calculate_similarity_for_candidates - {stage_time:.4f} seconds")
        self.stdout.write(
            f"  Games above threshold ({min_similarity or similarity_engine.DEFAULT_MIN_SIMILARITY}%): {len(similar_games)}")

        # STAGE 6: Sort results
        stage_start = time.time()
        similar_games.sort(key=lambda x: x['similarity'], reverse=True)
        stage_time = time.time() - stage_start
        timers['6. Sort results'] = stage_time
        self.stdout.write(f"\nSTAGE 6: Sort results - {stage_time:.4f} seconds")

        # STAGE 7: Load full objects
        stage_start = time.time()
        final_results = similarity_engine._load_full_objects(similar_games[:limit])
        stage_time = time.time() - stage_start
        timers['7. _load_full_objects'] = stage_time
        self.stdout.write(f"\nSTAGE 7: _load_full_objects - {stage_time:.4f} seconds")
        self.stdout.write(f"  Final results: {len(final_results)}")

        # Calculate total time
        total_time = time.time() - total_start
        timers['TOTAL (all stages)'] = total_time

        # Show top results
        self.stdout.write('\n' + '=' * 80)
        self.stdout.write(self.style.SUCCESS("TOP 20 RESULTS:"))

        for i, result in enumerate(final_results[:20]):
            game = result['game']
            similarity = result['similarity']
            common_genres = result.get('common_genres_count', 0)
            common_keywords = result.get('common_keywords_count', 0)
            common_themes = result.get('common_themes_count', 0)
            common_perspectives = result.get('common_perspectives_count', 0)
            common_game_modes = result.get('common_game_modes_count', 0)
            common_engines = result.get('common_engines_count', 0)

            # Mark source game
            source_marker = " (SOURCE)" if hasattr(source_game, 'id') and game.id == source_game.id else ""

            self.stdout.write(f"{i + 1:2d}. {game.name}{source_marker} (ID: {game.id}) - {similarity:.2f}%")
            self.stdout.write(f"     Common: G:{common_genres} K:{common_keywords} T:{common_themes} "
                              f"P:{common_perspectives} M:{common_game_modes} E:{common_engines}")

            # Show rating info for context
            if game.rating_count:
                self.stdout.write(f"     Rating: {game.rating:.1f} ({game.rating_count} votes)")

        # Show similarity distribution
        if similar_games:
            self.stdout.write('-' * 80)
            self.stdout.write(self.style.SUCCESS("SIMILARITY DISTRIBUTION:"))

            similarity_values = [g['similarity'] for g in similar_games]
            if similarity_values:
                self.stdout.write(f"  Min: {min(similarity_values):.2f}%")
                self.stdout.write(f"  Max: {max(similarity_values):.2f}%")
                self.stdout.write(f"  Avg: {sum(similarity_values) / len(similarity_values):.2f}%")
                self.stdout.write(f"  Median: {sorted(similarity_values)[len(similarity_values) // 2]:.2f}%")

        # Cache check
        self.stdout.write('-' * 80)
        self.stdout.write(self.style.SUCCESS("CACHE ANALYSIS:"))

        # Check if result was cached
        import json
        import hashlib

        cache_key_data = {
            'type': 'game' if hasattr(source_game, 'id') else 'virtual',
            'id': getattr(source_game, 'id', 'virtual'),
            'genres': sorted(source_data['genre_ids']),
            'keywords': sorted(source_data['keyword_ids']),
            'themes': sorted(source_data['theme_ids']),
            'perspectives': sorted(source_data['perspective_ids']),
            'game_modes': sorted(source_data['game_mode_ids']),
            'engines': sorted(source_data['engine_ids']),
            'min_similarity': min_similarity or similarity_engine.DEFAULT_MIN_SIMILARITY,
            'has_single_player': single_player_info['has_single_player'],
            'only_released': True,
            'limit': limit,
            'version': 'v15_similar_with_engines_no_limits'
        }

        cache_key = f'game_similarity_{hashlib.md5(json.dumps(cache_key_data, sort_keys=True).encode()).hexdigest()}'

        from django.core.cache import cache
        cached_result = cache.get(cache_key)

        if cached_result:
            cache_age = time.time() - cached_result.get('timestamp', 0)
            self.stdout.write(f"  ✅ Cache HIT!")
            self.stdout.write(f"  Cache key: {cache_key}")
            self.stdout.write(f"  Cache age: {cache_age:.2f} seconds")
            self.stdout.write(f"  Cached results: {len(cached_result.get('games', []))}")
        else:
            self.stdout.write(f"  ❌ Cache MISS!")
            self.stdout.write(f"  Cache key would be: {cache_key}")

        # DISPLAY ALL TIMERS AT THE VERY END (GUARANTEED LAST)
        self._display_all_timers(timers, name_search_time, total_start)

        self.stdout.write(self.style.SUCCESS('=' * 80))

    def _display_all_timers(self, timers, name_search_time, total_start):
        """Display all timers in a nice formatted table."""
        self.stdout.write('\n' + '=' * 80)
        self.stdout.write(self.style.SUCCESS('ALL TIMERS SUMMARY'))
        self.stdout.write('=' * 80)

        if not timers:
            self.stdout.write(self.style.WARNING('No timers collected'))
            return

        # Calculate total time including name search
        total_time = time.time() - total_start

        # Find the longest stage name for alignment
        max_name_length = max(len(name) for name in timers.keys()) + 2

        # Display each timer with bar chart
        for stage, timer_value in timers.items():
            if stage == 'TOTAL (all stages)':
                continue  # Skip total as we'll show it separately

            percentage = (timer_value / total_time) * 100 if total_time > 0 else 0
            bar_length = int(percentage / 2)  # Scale for 50 chars max
            bar = '█' * bar_length + '░' * (50 - bar_length)
            self.stdout.write(f"{stage:{max_name_length}} {timer_value:8.4f}s [{bar}] {percentage:5.1f}%")

        self.stdout.write('-' * 80)

        # Show total
        total_percentage = 100.0
        self.stdout.write(
            f"{'TOTAL (all stages)':{max_name_length}} {total_time:8.4f}s {'█' * 50} {total_percentage:5.1f}%")

        # Show breakdown by percentage groups
        self.stdout.write('\n' + self.style.SUCCESS('PERFORMANCE BREAKDOWN:'))

        # Sort timers by time descending (excluding TOTAL)
        sorted_timers = sorted([(t, n) for n, t in timers.items() if n != 'TOTAL (all stages)'], reverse=True)

        for timer_value, stage in sorted_timers[:3]:  # Show top 3 slowest
            percentage = (timer_value / total_time) * 100
            self.stdout.write(f"  • {stage}: {percentage:.1f}% ({timer_value:.4f}s)")

    def _parse_ids(self, id_string):
        """Parse comma-separated IDs into list of integers."""
        if not id_string:
            return []
        try:
            return [int(x.strip()) for x in id_string.split(',') if x.strip()]
        except ValueError:
            return []

    def _create_virtual_game(self, genre_ids, keyword_ids, theme_ids,
                             perspective_ids, game_mode_ids, engine_ids):
        """Create a VirtualGame object with the given criteria."""
        from games.similarity import VirtualGame

        return VirtualGame(
            genre_ids=genre_ids,
            keyword_ids=keyword_ids,
            theme_ids=theme_ids,
            perspective_ids=perspective_ids,
            game_mode_ids=game_mode_ids,
            engine_ids=engine_ids
        )

    def _display_game_info(self, game, label="Game"):
        """Display detailed information about a game."""
        self.stdout.write(f"{label}: {game.name} (ID: {game.id})")

        # Get all related data
        genres = list(game.genres.all().values_list('id', 'name'))
        keywords = list(game.keywords.all().values_list('id', 'name')[:10])  # Limit to 10 for readability
        themes = list(game.themes.all().values_list('id', 'name'))
        perspectives = list(game.player_perspectives.all().values_list('id', 'name'))
        game_modes = list(game.game_modes.all().values_list('id', 'name'))
        engines = list(game.engines.all().values_list('id', 'name'))

        self.stdout.write(f"  Genres: {len(genres)} - {[f'{id}:{name}' for id, name in genres]}")
        self.stdout.write(
            f"  Keywords: {game.keywords.count()} total - first 10: {[f'{id}:{name}' for id, name in keywords]}{'...' if game.keywords.count() > 10 else ''}")
        self.stdout.write(f"  Themes: {len(themes)} - {[f'{id}:{name}' for id, name in themes]}")
        self.stdout.write(f"  Perspectives: {len(perspectives)} - {[f'{id}:{name}' for id, name in perspectives]}")
        self.stdout.write(f"  Game modes: {len(game_modes)} - {[f'{id}:{name}' for id, name in game_modes]}")
        self.stdout.write(f"  Engines: {len(engines)} - {[f'{id}:{name}' for id, name in engines]}")
        self.stdout.write(f"  Rating: {game.rating} ({game.rating_count} votes)")