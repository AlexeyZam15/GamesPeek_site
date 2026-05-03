import time
import signal
import json
import os
import threading
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.db import connection
from games.models import Game
from games.similarity import GameSimilarity

if sys.platform == 'win32':
    PROGRESS_FILE = os.path.join(tempfile.gettempdir(), 'similar_games_cache_progress.json')
else:
    PROGRESS_FILE = '/tmp/similar_games_cache_progress.json'

BATCH_SIZE = 500
THREAD_BATCH_SIZE = 50


class ProgressBar:
    def __init__(self, total, width=50):
        self.total = total
        self.width = width
        self.start_time = time.time()
        self.last_update_time = self.start_time
        self.last_processed = 0
        self.current = 0
        self.lock = threading.Lock()
        self.last_line_len = 0
        self.last_update_call = 0
        self.avg_speed = 0
        self.speed_samples = []

    def update(self, current):
        with self.lock:
            now = time.time()

            if now - self.last_update_call < 0.33 and current < self.total:
                return
            self.last_update_call = now

            self.current = current
            elapsed = now - self.start_time
            percent = current / self.total if self.total > 0 else 0

            if current > self.last_processed and now - self.last_update_time > 0.5:
                instant_speed = (current - self.last_processed) / (now - self.last_update_time)
                self.speed_samples.append(instant_speed)
                if len(self.speed_samples) > 5:
                    self.speed_samples.pop(0)
                self.avg_speed = sum(self.speed_samples) / len(self.speed_samples)
                self.last_processed = current
                self.last_update_time = now

            speed = self.avg_speed if self.avg_speed > 0 else 0

            if speed > 0 and current > 0 and current < self.total:
                remaining_seconds = (self.total - current) / speed
                remaining_str = str(timedelta(seconds=int(remaining_seconds)))
            else:
                remaining_str = "calculating..."

            elapsed_str = str(timedelta(seconds=int(elapsed)))
            filled = int(self.width * percent)
            bar = '█' * filled + '░' * (self.width - filled)

            line = f"\r[{bar}] {percent * 100:5.1f}% | {current:6d}/{self.total} | {speed:5.2f} g/s | ETA: {remaining_str:>15} | Time: {elapsed_str:>8}    "

            if self.last_line_len > 0:
                sys.stdout.write('\r' + ' ' * self.last_line_len + '\r')

            sys.stdout.write(line)
            sys.stdout.flush()
            self.last_line_len = len(line)

    def finish(self):
        sys.stdout.write('\n')
        sys.stdout.flush()
        total_time = time.time() - self.start_time
        print(f"\n✅ Done! Time: {str(timedelta(seconds=int(total_time)))}")


class Command(BaseCommand):
    help = 'ULTRA FAST pre-cache similar games'

    def add_arguments(self, parser):
        parser.add_argument('--threads', type=int, default=8, help='Threads (default: 8)')
        parser.add_argument('--min-rating-count', type=int, default=0, help='Min rating (default: 0)')
        parser.add_argument('--clear', action='store_true', help='Clear cache before start')
        parser.add_argument('--resume', action='store_true', help='Resume from progress')
        parser.add_argument('--quiet', action='store_true', help='Suppress all output except progress bar')
        parser.add_argument('--verbose', action='store_true', help='Verbose output with detailed logging')

    def __init__(self):
        super().__init__()
        self.stop_flag = False
        self.processed = set()
        self.lock = threading.Lock()
        self.quiet = False
        self.verbose = False
        self.executor = None
        self.progress = None
        self._signal_handler_called = False
        # Устанавливаем обработчик сигнала здесь
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, sig, frame):
        if self._signal_handler_called:
            return
        self._signal_handler_called = True

        # Используем sys.stderr.write вместо print, чтобы избежать reentrant call
        sys.stderr.write("\n\n⚠️ Stopping... Saving progress...\n")
        sys.stderr.flush()

        self.stop_flag = True
        if self.executor:
            self.executor.shutdown(wait=False, cancel_futures=True)
        self.save_progress()

        sys.stderr.write(f"Progress saved to {PROGRESS_FILE}\n")
        sys.stderr.write("Use --resume to continue\n")
        sys.stderr.flush()

        # Немедленный выход без вызова дополнительных обработчиков
        os._exit(0)

    def save_progress(self):
        data = {'processed': list(self.processed), 'ts': datetime.now().isoformat()}
        try:
            os.makedirs(os.path.dirname(PROGRESS_FILE), exist_ok=True)
            with open(PROGRESS_FILE, 'w') as f:
                json.dump(data, f)
        except Exception as e:
            if not self.quiet:
                print(f"Warning: Could not save progress: {e}")

    def load_progress(self):
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r') as f:
                    data = json.load(f)
                    self.processed = set(data.get('processed', []))
                    if not self.quiet:
                        print(f"🔄 Resuming: {len(self.processed)} games already cached")
                    return True
            except Exception as e:
                if not self.quiet:
                    print(f"Warning: Could not load progress: {e}")
        return False

    def clear_progress(self):
        if os.path.exists(PROGRESS_FILE):
            try:
                os.remove(PROGRESS_FILE)
            except Exception as e:
                if not self.quiet:
                    print(f"Warning: Could not clear progress: {e}")

    def get_game_ids_raw_sql(self, min_rating):
        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT id
                           FROM games_game
                           WHERE rating_count >= %s
                           ORDER BY rating_count DESC, id
                           """, [min_rating])
            return [row[0] for row in cursor.fetchall()]

    def clear_cache_raw_sql(self):
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE games_gamesimilaritycache RESTART IDENTITY")
        if not self.quiet:
            print("🗑️ Cache cleared (TRUNCATE)")

    def bulk_insert_results(self, results):
        if not results:
            return

        with connection.cursor() as cursor:
            game1_ids = [r[0] for r in results]
            game2_ids = [r[1] for r in results]
            similarities = [r[2] for r in results]

            cursor.execute("""
                           INSERT INTO games_gamesimilaritycache
                           (game1_id, game2_id, similarity_score, calculated_at, algorithm_version)
                           SELECT unnest(%s::integer[]),
                                  unnest(%s::integer[]),
                                  unnest(%s::double precision[]),
                                  NOW(),
                                  7 ON CONFLICT (game1_id, game2_id) 
                DO
                           UPDATE SET similarity_score = EXCLUDED.similarity_score,
                               calculated_at = NOW()
                           """, [game1_ids, game2_ids, similarities])

            connection.commit()

    def process_single_game(self, game_id):
        similarity_engine = GameSimilarity()
        similarity_engine.stop_flag = self.stop_flag
        similarity_engine.verbose = False
        batch_results = []

        if self.stop_flag:
            return batch_results

        with self.lock:
            if game_id in self.processed:
                return batch_results

        try:
            game = Game.objects.filter(id=game_id).only(
                'id', 'name', 'keyword_ids', 'genre_ids', 'theme_ids',
                'perspective_ids', 'game_mode_ids', 'engine_ids', 'developer_ids'
            ).first()

            if not game:
                with self.lock:
                    self.processed.add(game_id)
                return batch_results

            similar = similarity_engine.find_similar_games(
                source_game=game,
                min_similarity=0,
                limit=500,
                search_filters=None
            )

            for item in similar:
                if isinstance(item, dict):
                    target = item.get('game')
                    sim = item.get('similarity', 0)
                else:
                    target = item
                    sim = getattr(item, 'similarity', 0)

                if target and hasattr(target, 'id') and target.id != game_id:
                    batch_results.append((game_id, target.id, sim))

            with self.lock:
                self.processed.add(game_id)

        except Exception as e:
            with self.lock:
                self.processed.add(game_id)

        return batch_results

    def handle(self, *args, **options):
        threads = options['threads']
        min_rating = options['min_rating_count']
        clear = options['clear']
        resume = options['resume']
        self.quiet = options['quiet']
        self.verbose = options.get('verbose', False)

        start_time = time.time()

        signal.signal(signal.SIGINT, self.signal_handler)

        if not self.quiet:
            print("=" * 70)
            print("⚡ ULTRA FAST SIMILAR GAMES CACHING (ALGO v7)")
            print("=" * 70)
            print(f"Threads: {threads}")
            print(f"Min rating count: {min_rating}")
            print(f"Progress file: {PROGRESS_FILE}")
            print("=" * 70)

        if not self.quiet:
            print("\n📊 STEP 1/4: Loading game IDs...")

        step_start = time.time()
        game_ids = self.get_game_ids_raw_sql(min_rating)
        total = len(game_ids)
        step_time = time.time() - step_start

        if not self.quiet:
            print(f"✅ {total} games loaded in {step_time:.2f}s")

        if clear:
            if not self.quiet:
                print("\n🗑️ Clearing cache...")
            self.clear_cache_raw_sql()
            self.clear_progress()
            self.processed.clear()
        elif resume:
            self.load_progress()

        remaining = [gid for gid in game_ids if gid not in self.processed]

        if not self.quiet:
            print(f"\n📌 Games to process: {len(remaining)}/{total}")

        if not remaining:
            if not self.quiet:
                print("✅ All done!")
            return

        if not self.quiet:
            print(f"\n🚀 STEP 3/4: Processing {len(remaining)} games...")
            print("-" * 40)

        self.progress = ProgressBar(total, width=40)
        self.progress.update(len(self.processed))

        all_results = []
        processed_count = len(self.processed)
        last_save = processed_count

        batch_size = THREAD_BATCH_SIZE
        game_batches = [remaining[i:i + batch_size] for i in range(0, len(remaining), batch_size)]

        self.executor = ThreadPoolExecutor(max_workers=threads)

        try:
            for batch in game_batches:
                if self.stop_flag:
                    break

                futures = []
                for game_id in batch:
                    if self.stop_flag:
                        break
                    futures.append(self.executor.submit(self.process_single_game, game_id))

                for future in as_completed(futures):
                    if self.stop_flag:
                        break

                    batch_results = future.result()
                    all_results.extend(batch_results)

                    with self.lock:
                        processed_count = len(self.processed)

                    # Обновляем прогресс-бар
                    self.progress.update(processed_count)

                    if processed_count - last_save >= 100:
                        self.save_progress()
                        last_save = processed_count

                    if len(all_results) >= BATCH_SIZE:
                        self.bulk_insert_results(all_results[:BATCH_SIZE])
                        all_results = all_results[BATCH_SIZE:]

        except KeyboardInterrupt:
            self.stop_flag = True
        finally:
            self.executor.shutdown(wait=True, cancel_futures=True)

        if all_results:
            self.bulk_insert_results(all_results)

        self.progress.finish()
        self.save_progress()

        if len(self.processed) >= total and not self.stop_flag:
            if not self.quiet:
                print("\n🔧 STEP 4/4: Optimizing database...")

            with connection.cursor() as cursor:
                cursor.execute("ANALYZE games_gamesimilaritycache;")
                cursor.execute("VACUUM games_gamesimilaritycache;")

            if not self.quiet:
                print("✅ ANALYZE and VACUUM completed")

        total_time = time.time() - start_time

        if not self.quiet:
            print("\n" + "=" * 70)
            if self.stop_flag:
                print("⚠️ PROCESS PAUSED - Use --resume to continue")
            else:
                print("✅ COMPLETE!")
            print(f"⏱️  Time: {str(timedelta(seconds=int(total_time)))}")
            print(f"📊 Processed: {len(self.processed)}/{total} games")
            if total > 0:
                print(f"⚡ Average speed: {total / total_time:.1f} games/sec")
            print("=" * 70)

        if len(self.processed) >= total and not self.stop_flag:
            self.clear_progress()