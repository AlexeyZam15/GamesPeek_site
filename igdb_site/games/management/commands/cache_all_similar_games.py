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

BATCH_SIZE = 5000
THREAD_BATCH_SIZE = 200


class ProgressBar:
    __slots__ = ('total', 'start_time', 'last_update_time', 'last_processed',
                 'current', 'lock', 'last_line_len', 'start_time_absolute')

    def __init__(self, total):
        self.total = total
        self.start_time = time.time()
        self.start_time_absolute = self.start_time
        self.last_update_time = self.start_time
        self.last_processed = 0
        self.current = 0
        self.lock = threading.Lock()
        self.last_line_len = 0

    def update(self, current):
        with self.lock:
            now = time.time()
            # Обновляем не чаще раза в секунду
            if now - self.last_update_time < 1.0:
                return

            self.current = current
            elapsed = now - self.start_time_absolute
            percent = current / self.total if self.total > 0 else 0

            # Расчет скорости за всё время (общая, не мгновенная)
            if elapsed > 0 and current > 0:
                # Общая средняя скорость с самого начала
                avg_speed = current / elapsed
                # ETA на основе общей скорости
                remaining_seconds = (self.total - current) / avg_speed if avg_speed > 0 else 0
                eta = str(timedelta(seconds=int(remaining_seconds)))
                speed_display = int(avg_speed) if avg_speed >= 1 else round(avg_speed, 1)
            else:
                eta = "calculating..."
                speed_display = 0

            self.last_update_time = now
            self.last_processed = current

            # Прогресс-бар
            filled = int(30 * percent)
            bar = '█' * filled + '░' * (30 - filled)

            time_str = str(timedelta(seconds=int(elapsed)))

            line = f"\r[{bar}] {percent * 100:5.1f}% | {current:6d}/{self.total} | {speed_display:>5} g/s | ETA: {eta:>12} | Time: {time_str:>8}"

            if self.last_line_len > len(line):
                sys.stdout.write('\r' + ' ' * self.last_line_len + '\r')
            sys.stdout.write(line)
            sys.stdout.flush()
            self.last_line_len = len(line)

    def finish(self):
        sys.stdout.write('\n')
        sys.stdout.flush()


class Command(BaseCommand):
    help = 'MAX SPEED pre-cache similar games'

    def add_arguments(self, parser):
        parser.add_argument('--threads', type=int, default=32, help='Threads (default: 32)')
        parser.add_argument('--min-rating-count', type=int, default=0, help='Min rating (default: 0)')
        parser.add_argument('--clear', action='store_true', help='Clear cache before start')
        parser.add_argument('--resume', action='store_true', help='Resume from progress')
        parser.add_argument('--quiet', action='store_true', help='Suppress all output')

    def __init__(self):
        super().__init__()
        self.stop_flag = False
        self.processed = set()
        self.lock = threading.Lock()
        self.quiet = False
        self.executor = None
        self._signal_called = False
        signal.signal(signal.SIGINT, self.signal_handler)

    def signal_handler(self, sig, frame):
        if hasattr(self, '_signal_called') and self._signal_called:
            return
        self._signal_called = True

        sys.stderr.write("\n\n⚠️ Stopping... Saving progress...\n")
        sys.stderr.flush()

        self.stop_flag = True

        # Просто сохраняем прогресс и выходим, не дожидаясь потоков
        self.save_progress()

        sys.stderr.write(f"✅ Progress saved to {PROGRESS_FILE}\n")
        sys.stderr.write("▶️  Use --resume to continue\n")
        sys.stderr.flush()

        # Немедленный выход
        os._exit(0)

    def save_progress(self):
        # Загружаем существующий прогресс, если есть
        existing_processed = set()
        if os.path.exists(PROGRESS_FILE):
            try:
                with open(PROGRESS_FILE, 'r') as f:
                    existing_data = json.load(f)
                    existing_processed = set(existing_data.get('processed', []))
            except:
                pass

        # Объединяем с новыми
        all_processed = existing_processed | self.processed

        data = {'processed': list(all_processed), 'ts': datetime.now().isoformat()}
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
                        print(f"🔄 Loaded {len(self.processed)} already processed games")
                    return True
            except Exception as e:
                if not self.quiet:
                    print(f"Warning: Could not load progress: {e}")
        return False

    def clear_progress(self):
        if os.path.exists(PROGRESS_FILE):
            try:
                os.remove(PROGRESS_FILE)
            except:
                pass

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
                                  7 ON CONFLICT (game1_id, game2_id) DO
                           UPDATE
                               SET similarity_score = EXCLUDED.similarity_score, calculated_at = NOW()
                           """, [game1_ids, game2_ids, similarities])
            connection.commit()

    def process_single_game(self, game_id):
        if self.stop_flag:
            return []

        # Быстрая проверка без лока
        if game_id in self.processed:
            return []

        try:
            # Минимальная загрузка данных
            game = Game.objects.filter(id=game_id).only('id', 'name', 'keyword_ids', 'genre_ids',
                                                        'theme_ids', 'perspective_ids', 'game_mode_ids', 'engine_ids',
                                                        'developer_ids').first()
            if not game:
                with self.lock:
                    self.processed.add(game_id)
                return []

            sim = GameSimilarity()
            sim.stop_flag = self.stop_flag
            sim.verbose = False

            similar = sim.find_similar_games(game, min_similarity=0, limit=500, search_filters=None)

            results = []
            for item in similar:
                if isinstance(item, dict):
                    target = item.get('game')
                    s = item.get('similarity', 0)
                else:
                    target = item
                    s = getattr(item, 'similarity', 0)
                if target and hasattr(target, 'id') and target.id != game_id:
                    results.append((game_id, target.id, s))

            with self.lock:
                self.processed.add(game_id)
            return results
        except:
            with self.lock:
                self.processed.add(game_id)
            return []

    def handle(self, *args, **options):
        threads = options['threads']
        min_rating = options['min_rating_count']
        clear = options['clear']
        resume = options['resume']
        self.quiet = options['quiet']

        start_time = time.time()

        if not self.quiet:
            print("=" * 50)
            print(f"MAX SPEED | Threads: {threads} | Min rating: {min_rating}")
            print("=" * 50)

        # Загружаем ВСЕ ID игр
        all_game_ids = self.get_game_ids_raw_sql(min_rating)
        total_all = len(all_game_ids)

        if not self.quiet:
            print(f"Total games in DB: {total_all}")

        # Очистка или восстановление
        if clear:
            self.clear_cache_raw_sql()
            self.clear_progress()
            self.processed.clear()
            remaining = all_game_ids
            already_processed = 0
            # В handle при resume
        elif resume:
            self.load_progress()
            remaining = [gid for gid in all_game_ids if gid not in self.processed]
            already_processed = len(self.processed)
        else:
            remaining = all_game_ids
            already_processed = 0

        total_remaining = len(remaining)

        if not self.quiet:
            print(f"Already processed: {already_processed}")
            print(f"Remaining to process: {total_remaining}")

        if total_remaining == 0:
            if not self.quiet:
                print("All done!")
            return

        if not self.quiet:
            print(f"\n🚀 Processing {total_remaining} games...")
            print("-" * 40)

        # Прогресс-бар для ОСТАВШИХСЯ игр (начинаем с 0)
        progress = ProgressBar(total_remaining)
        progress.update(0)

        all_results = []
        processed_count = 0  # счётчик для оставшихся игр
        last_save = 0

        batch_size = THREAD_BATCH_SIZE
        game_batches = [remaining[i:i + batch_size] for i in range(0, total_remaining, batch_size)]

        self.executor = ThreadPoolExecutor(max_workers=threads)

        try:
            for batch in game_batches:
                if self.stop_flag:
                    break

                futures = [self.executor.submit(self.process_single_game, gid) for gid in batch]

                for future in as_completed(futures):
                    if self.stop_flag:
                        break

                    batch_results = future.result()
                    all_results.extend(batch_results)

                    processed_count += 1

                    # Обновляем прогресс-бар (0% = 0 из remaining)
                    progress.update(processed_count)

                    if processed_count - last_save >= 500:
                        self.save_progress()
                        last_save = processed_count

                    if len(all_results) >= BATCH_SIZE:
                        self.bulk_insert_results(all_results[:BATCH_SIZE])
                        all_results = all_results[BATCH_SIZE:]

        except:
            pass
        finally:
            self.executor.shutdown(wait=True, cancel_futures=True)

        if all_results:
            self.bulk_insert_results(all_results)

        progress.finish()
        self.save_progress()

        total_time = time.time() - start_time
        if not self.quiet:
            print(f"\nDone! Time: {str(timedelta(seconds=int(total_time)))}")
            print(f"Processed: {processed_count}/{total_remaining} games")

        if processed_count >= total_remaining and not self.stop_flag:
            self.clear_progress()
