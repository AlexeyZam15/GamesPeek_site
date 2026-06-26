# games/management/commands/update_similar_ids.py
"""Максимально ускоренная команда обновления similar_game_ids с предзагрузкой."""

from django.core.management.base import BaseCommand
from django.db import connection
from django.db.models import Q
from games.models import Game
from games.similarity import GameSimilarity
import time
import sys
import signal
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock


class Command(BaseCommand):
    """Update similar_game_ids field for specified games with maximum speed."""

    help = 'Update similar_game_ids for games (specify game IDs or update all)'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interrupted = False
        self.processed = 0
        self.total = 0
        self.updated = 0
        self.errors = 0
        self.lock = Lock()
        self.similarity = GameSimilarity(verbose=False)
        self.dry_run = False
        self.force = False
        self.random_mode = False
        self.game_cache = {}

    def add_arguments(self, parser):
        parser.add_argument(
            '--game-id',
            type=int,
            nargs='+',
            help='Specific game IDs to update'
        )
        parser.add_argument(
            '--threads',
            type=int,
            default=8,
            help='Number of threads for parallel processing (default: 8)'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Maximum number of games to process'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be updated without actually updating'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Batch size for database updates (default: 100)'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force update even if similar_game_ids already exists'
        )
        parser.add_argument(
            '--random',
            action='store_true',
            help='Process one random game with empty similar_game_ids and show result'
        )

    def signal_handler(self, signum, frame):
        if not self.interrupted:
            self.interrupted = True
            sys.stderr.write("\n\n⚠️  Прерывание... Сохраняем обработанные игры...\n")
            sys.stderr.flush()

    def preload_games(self, game_ids):
        """Предзагрузка всех игр в память одним запросом."""
        self.stdout.write("📚 Preloading games into memory...")
        start = time.time()

        games = Game.objects.filter(id__in=game_ids).only(
            'id', 'name', 'similar_game_ids', 'genre_ids',
            'keyword_ids', 'theme_ids', 'engine_ids', 'platform_ids',
            'rating', 'rating_count', 'first_release_date', 'cover_url', 'game_type'
        )

        self.game_cache = {g.id: g for g in games}
        self.stdout.write(f"   ✅ Loaded {len(self.game_cache)} games in {time.time() - start:.2f}s")

    def process_game(self, game_id, force=False):
        """Обрабатывает одну игру и удаляет её из кэша."""
        try:
            game = self.game_cache.pop(game_id, None)
            if not game:
                return (game_id, None, [], [], "Game not found in cache", False)

            similar_data = self.similarity.find_similar_games(
                source_game=game,
                min_similarity=40,
                limit=500,
                search_filters=None
            )

            game_similarity_pairs = []
            for item in similar_data:
                if isinstance(item, dict):
                    similar_game = item.get('game')
                    similarity_score = item.get('similarity', 0)
                else:
                    similar_game = item
                    similarity_score = getattr(item, 'similarity', 0)

                if similar_game and similar_game.id != game.id:
                    game_similarity_pairs.append((similar_game.id, similarity_score))

            game_similarity_pairs.sort(key=lambda x: x[1], reverse=True)
            new_ids = [gid for gid, _ in game_similarity_pairs[:12]]
            old_ids = game.similar_game_ids or []

            if force or set(new_ids) != set(old_ids):
                return (game.id, game.name, new_ids, old_ids, None, True)
            else:
                return (game.id, game.name, new_ids, old_ids, None, False)

        except Exception as e:
            self.game_cache.pop(game_id, None)
            return (game_id, None, [], [], str(e), False)

    def save_batch(self, batch):
        """Сохраняет батч обновлений."""
        if not batch or self.dry_run:
            return

        with connection.cursor() as cursor:
            for game_id, new_ids in batch:
                cursor.execute(
                    "UPDATE games_game SET similar_game_ids = %s WHERE id = %s",
                    [new_ids, game_id]
                )

    def handle_random(self):
        """Обрабатывает одну случайную игру с пустым similar_game_ids."""
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('RANDOM GAME TEST'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        game = Game.objects.filter(
            Q(similar_game_ids=[]) | Q(similar_game_ids__isnull=True)
        ).order_by('?').first()

        if not game:
            self.stdout.write(self.style.WARNING('\n⚠️  No games with empty similar_game_ids found'))
            return

        self.stdout.write(f"\n🎯 Selected game:")
        self.stdout.write(f"   ID: {game.id}")
        self.stdout.write(f"   Name: {game.name}")
        self.stdout.write(f"   Current similar_game_ids: {game.similar_game_ids}")
        self.stdout.write(f"   Current count: {len(game.similar_game_ids or [])}")
        self.stdout.write("\n" + "-" * 60)

        start_time = time.time()

        try:
            self.preload_games([game.id])

            result = self.process_game(game.id, force=True)
            game_id, game_name, new_ids, old_ids, error, need_update = result

            elapsed = time.time() - start_time

            if error:
                self.stdout.write(self.style.ERROR(f"\n❌ Error: {error}"))
                return

            self.stdout.write(f"\n✅ Processed in {elapsed:.2f}s")
            self.stdout.write(f"\n📊 Result:")
            self.stdout.write(f"   Game ID: {game_id}")
            self.stdout.write(f"   Game Name: {game_name}")
            self.stdout.write(f"   Old similar_game_ids: {old_ids}")
            self.stdout.write(f"   Old count: {len(old_ids)}")
            self.stdout.write(f"   New similar_game_ids: {new_ids}")
            self.stdout.write(f"   New count: {len(new_ids)}")

            if need_update and not self.dry_run:
                self.save_batch([(game_id, new_ids)])
                self.stdout.write(self.style.SUCCESS(f"\n✅ Saved successfully!"))
            elif need_update and self.dry_run:
                self.stdout.write(self.style.WARNING(f"\n⚠️  Dry run - would save {len(new_ids)} IDs"))
            else:
                self.stdout.write(f"\nℹ️  No changes needed")

            if new_ids:
                self.stdout.write(f"\n📋 First 5 similar games:")
                with connection.cursor() as cursor:
                    placeholders = ','.join(['%s'] * min(5, len(new_ids)))
                    cursor.execute(
                        f"SELECT id, name FROM games_game WHERE id IN ({placeholders}) LIMIT 5",
                        new_ids[:5]
                    )
                    for row in cursor.fetchall():
                        self.stdout.write(f"   - {row[1]} (ID: {row[0]})")

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ Error: {str(e)}"))

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 60))

    def handle(self, *args, **options):
        original_handler = signal.signal(signal.SIGINT, self.signal_handler)

        self.random_mode = options.get('random', False)

        if self.random_mode:
            self.handle_random()
            signal.signal(signal.SIGINT, original_handler)
            return

        game_ids = options.get('game_id')
        threads = min(options.get('threads', 8), 32)
        limit = options.get('limit')
        self.dry_run = options.get('dry_run', False)
        batch_size = options.get('batch_size', 100)
        self.force = options.get('force', False)

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('UPDATE SIMILAR GAME IDS (MAX SPEED)'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        if self.force:
            self.stdout.write(self.style.WARNING('\n⚠️  FORCE MODE - Will overwrite existing similar_game_ids'))
        else:
            self.stdout.write('\nℹ️  SKIP MODE - Will skip games that already have similar_game_ids')

        if game_ids:
            game_id_list = game_ids
            self.stdout.write(f"\n🎯 Updating specific games: {game_ids}")
        else:
            if not self.force:
                game_id_list = list(
                    Game.objects
                    .filter(Q(similar_game_ids=[]) | Q(similar_game_ids__isnull=True))
                    .only('id')
                    .values_list('id', flat=True)
                )
                self.stdout.write("\n🎯 Updating ONLY games with empty similar_game_ids")
            else:
                game_id_list = list(Game.objects.all().only('id').values_list('id', flat=True))
                self.stdout.write("\n🎯 Updating ALL games")

        if limit:
            game_id_list = game_id_list[:limit]
            self.stdout.write(f"   Limit: {limit} games")

        self.total = len(game_id_list)
        self.stdout.write(f"   Total games to process: {self.total}")
        self.stdout.write(f"   Threads: {threads}")
        self.stdout.write(f"   Batch size: {batch_size}")
        self.stdout.write(f"   Dry run: {self.dry_run}")
        self.stdout.write("\n" + "-" * 60)

        if self.total == 0:
            self.stdout.write(self.style.WARNING("\n⚠️  No games to process"))
            self.stdout.write(self.style.SUCCESS('=' * 60))
            signal.signal(signal.SIGINT, original_handler)
            return

        if self.dry_run:
            self.stdout.write(self.style.WARNING("\n⚠️  DRY RUN MODE - No changes will be saved"))

        self.preload_games(game_id_list)

        start_time = time.time()
        processed_count = 0
        to_update_batch = []

        with ThreadPoolExecutor(max_workers=threads) as executor:
            futures = {executor.submit(self.process_game, gid, self.force): gid for gid in game_id_list}

            for future in as_completed(futures):
                if self.interrupted:
                    executor.shutdown(wait=False, cancel_futures=True)
                    if to_update_batch:
                        self.save_batch(to_update_batch)
                        to_update_batch = []
                    break

                try:
                    result = future.result(timeout=60)
                    processed_count += 1

                    game_id, game_name, new_ids, old_ids, error, need_update = result
                    with self.lock:
                        self.processed = processed_count
                        if error is None and need_update:
                            to_update_batch.append((game_id, new_ids))
                            self.updated += 1
                        elif error is not None:
                            self.errors += 1

                    if len(to_update_batch) >= batch_size:
                        self.save_batch(to_update_batch)
                        to_update_batch = []

                except Exception as e:
                    with self.lock:
                        self.errors += 1

                if processed_count % 50 == 0 or processed_count == self.total or self.interrupted:
                    percent = (processed_count / self.total) * 100 if self.total > 0 else 0
                    elapsed = time.time() - start_time
                    speed = processed_count / elapsed if elapsed > 0 else 0
                    eta = (self.total - processed_count) / speed if speed > 0 else 0

                    bar_length = 30
                    filled = int(bar_length * processed_count / self.total) if self.total > 0 else 0
                    bar = '█' * filled + '░' * (bar_length - filled)

                    if eta < 60:
                        eta_str = f"{eta:.0f}s"
                    elif eta < 3600:
                        eta_str = f"{eta / 60:.1f}m"
                    else:
                        eta_str = f"{eta / 3600:.1f}h"

                    status = "⚠️ INTERRUPTED" if self.interrupted else ""
                    sys.stderr.write(
                        f"\r[{processed_count:>{len(str(self.total))}}/{self.total}] "
                        f"{bar} {percent:5.1f}% "
                        f"Updated:{self.updated} Errors:{self.errors} "
                        f"{speed:.1f}/s ETA:{eta_str:>6} {status}"
                    )
                    sys.stderr.flush()

        if to_update_batch:
            self.save_batch(to_update_batch)

        sys.stderr.write("\n")

        elapsed_time = time.time() - start_time
        self.stdout.write("\n" + "-" * 60)
        self.stdout.write(self.style.SUCCESS("\n📊 SUMMARY"))
        self.stdout.write(f"   Total games processed: {self.processed}")
        self.stdout.write(f"   Games updated: {self.updated}")
        self.stdout.write(f"   Errors: {self.errors}")
        if self.interrupted:
            self.stdout.write(self.style.WARNING("   ⚠️  Interrupted by user (saved partial results)"))
        if self.force:
            self.stdout.write(self.style.WARNING("   ⚠️  FORCE MODE was enabled"))
        else:
            self.stdout.write("   ℹ️  SKIP MODE - only updated empty similar_game_ids")
        self.stdout.write(f"   Total time: {elapsed_time:.2f} seconds")
        if self.processed > 0:
            self.stdout.write(f"   Speed: {self.processed / elapsed_time:.1f} games/sec")
        self.stdout.write(f"   Database queries: {len(connection.queries)}")

        if self.dry_run:
            self.stdout.write(self.style.WARNING("\n⚠️  DRY RUN COMPLETE - No changes were saved"))
        else:
            self.stdout.write(self.style.SUCCESS("\n✅ UPDATE COMPLETE"))

        self.stdout.write(self.style.SUCCESS('=' * 60))
        signal.signal(signal.SIGINT, original_handler)