# games/management/commands/stress_test_igdb.py
import time
import statistics
import concurrent.futures
import threading
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from games.igdb_api import make_igdb_request


class Command(BaseCommand):
    """Стресс-тест IGDB API с максимальными оптимизациями"""

    help = 'Стресс-тест IGDB API для определения оптимальных параметров'

    def add_arguments(self, parser):
        parser.add_argument('--duration', type=int, default=60,
                            help='Продолжительность теста в секундах (по умолчанию: 60)')
        parser.add_argument('--max-workers', type=int, default=6,
                            help='Максимальное количество параллельных потоков (по умолчанию: 6)')
        parser.add_argument('--batch-size', type=int, default=10,
                            help='Размер пачки запроса (по умолчанию: 10)')
        parser.add_argument('--requests-per-second', type=int, default=0,
                            help='Целевое количество запросов в секунду (0 = без ограничений)')
        parser.add_argument('--test-game-ids', type=str, default='',
                            help='Список ID игр для теста через запятую')

    def handle(self, *args, **options):
        self.stdout.write('🔥 СТРЕСС-ТЕСТ IGDB API С МАКСИМАЛЬНЫМИ ОПТИМИЗАЦИЯМИ')
        self.stdout.write('=' * 70)

        test_duration = options['duration']
        max_workers = options['max_workers']
        batch_size = min(options['batch_size'], 10)  # Максимум 10
        target_rps = options['requests_per_second']

        # Подготовка тестовых данных
        test_ids = self.prepare_test_ids(options['test_game_ids'])

        # Статистика
        stats = {
            'total_requests': 0,
            'successful_requests': 0,
            'failed_requests': 0,
            'response_times': [],
            'errors': [],
            'start_time': time.time(),
            'end_time': 0,
            'thread_stats': {}
        }

        # Ограничитель скорости
        rate_limiter = RateLimiter(target_rps) if target_rps > 0 else None

        try:
            self.run_stress_test(
                test_ids=test_ids,
                test_duration=test_duration,
                max_workers=max_workers,
                batch_size=batch_size,
                stats=stats,
                rate_limiter=rate_limiter
            )
        except KeyboardInterrupt:
            self.stdout.write('\n\n🛑 ТЕСТ ПРЕРВАН ПОЛЬЗОВАТЕЛЕМ')
        finally:
            self.print_final_stats(stats, max_workers, batch_size)

    def prepare_test_ids(self, custom_ids):
        """Подготавливает список ID для теста"""
        if custom_ids:
            return [int(id.strip()) for id in custom_ids.split(',') if id.strip()]

        # Большой список популярных игр для теста (50+ ID)
        return [
            # AAA игры
            1942,  # The Witcher 3
            11902,  # Cyberpunk 2077
            1020,  # Grand Theft Auto V
            7346,  # The Legend of Zelda: Breath of the Wild
            18122,  # Elden Ring
            2909,  # Red Dead Redemption 2
            11169,  # God of War
            113313,  # Baldur's Gate 3
            2777,  # Dark Souls III
            10746,  # Horizon Zero Dawn

            # Популярные инди-игры
            11208,  # Hollow Knight
            19560,  # Celeste
            20634,  # Hades
            11117,  # Stardew Valley
            7826,  # Cuphead
            103298,  # Among Us
            12598,  # Undertale
            11557,  # Minecraft
            7331,  # Terraria
            13205,  # Rocket League

            # Дополнительные игры для разнообразия
            28050,  # Apex Legends
            115,  # Counter-Strike: Global Offensive
            2597,  # Dota 2
            5118,  # League of Legends
            19486,  # Valorant
            106987,  # Fortnite
            7332,  # Overwatch
            107242,  # Overwatch 2
            12020,  # Destiny 2
            18992,  # Call of Duty: Warzone

            # Стратегии и RPG
            5527,  # Civilization VI
            28019,  # Total War: Warhammer III
            11247,  # Divinity: Original Sin 2
            27993,  # Pathfinder: Wrath of the Righteous
            12948,  # XCOM 2
            22263,  # Crusader Kings III
            28833,  # Victoria 3
            28540,  # Company of Heroes 3

            # Симуляторы
            7264,  # The Sims 4
            109462,  # Microsoft Flight Simulator
            11619,  # Euro Truck Simulator 2
            26921,  # Cities: Skylines II

            # Хорроры
            19698,  # Resident Evil Village
            119266,  # Resident Evil 4 Remake
            19514,  # Dead Space Remake
            28081,  # Alan Wake 2

            # Для пачек по 10
            2454,  # Fallout 4
            19719,  # Death Stranding
            1029,  # Portal 2
            13005,  # Doom Eternal
        ]

    def run_stress_test(self, test_ids, test_duration, max_workers, batch_size, stats, rate_limiter):
        """Запускает стресс-тест"""
        self.stdout.write(f'\n📊 ПАРАМЕТРЫ ТЕСТА:')
        self.stdout.write(f'   • Продолжительность: {test_duration} сек')
        self.stdout.write(f'   • Максимум потоков: {max_workers}')
        self.stdout.write(f'   • Размер пачки: {batch_size}')
        self.stdout.write(f'   • Тестовых ID: {len(test_ids)}')
        if rate_limiter:
            self.stdout.write(f'   • Целевой RPS: {rate_limiter.target_rps}')

        self.stdout.write(f'\n🚀 ЗАПУСК ТЕСТА...')
        self.stdout.write(f'   Начало: {datetime.now().strftime("%H:%M:%S")}\n')

        end_time = time.time() + test_duration
        lock = threading.Lock()
        stop_event = threading.Event()
        last_progress_time = [0]

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            interrupted = False  # Флаг прерывания

            try:
                while time.time() < end_time and not stop_event.is_set():
                    with lock:
                        if stats.get('rate_limit_hit'):
                            print()
                            self.stdout.write(f'\n🔴 Обнаружен лимит API после {stats["total_requests"]} запросов')
                            break

                    if rate_limiter:
                        rate_limiter.wait()

                    future = executor.submit(
                        self.make_test_request,
                        test_ids=test_ids,
                        batch_size=batch_size,
                        stats=stats,
                        lock=lock,
                        stop_event=stop_event
                    )
                    futures.append(future)

                    if len(futures) >= max_workers * 2:
                        try:
                            self.process_completed_futures(futures, stats, lock, test_duration, last_progress_time)
                        except RateLimitExceeded as e:
                            print()
                            self.stdout.write(f'\n⚠️  {str(e)}')
                            stop_event.set()
                            break

                    if time.time() - last_progress_time[0] > 0.5:
                        self._print_progress(stats, test_duration, last_progress_time)

            except KeyboardInterrupt:
                interrupted = True
                print()
                self.stdout.write('\n🛑 ТЕСТ ПРЕРВАН ПОЛЬЗОВАТЕЛЕМ')
                stop_event.set()
            except RateLimitExceeded as e:
                print()
                self.stdout.write(f'\n⚠️  {str(e)}')
                stop_event.set()
            finally:
                # Устанавливаем флаг остановки для всех потоков
                stop_event.set()

                # Останавливаем ThreadPoolExecutor
                executor.shutdown(wait=False, cancel_futures=True)

                # Отменяем все фьючерсы
                for future in futures:
                    if not future.done():
                        future.cancel()

                # Быстрая очистка
                try:
                    # Ждем максимум 2 секунды на завершение
                    done, not_done = concurrent.futures.wait(
                        futures,
                        timeout=2.0,
                        return_when=concurrent.futures.ALL_COMPLETED
                    )
                except Exception:
                    pass

                # Финальный прогресс если не было прерывания
                if not interrupted:
                    self._print_final_progress(stats, test_duration)

        stats['end_time'] = time.time()

        if not interrupted:
            print()
            self.print_final_stats(stats, max_workers, batch_size)

    def _print_progress(self, stats, test_duration, last_progress_time):
        """Печатает прогресс в одной строке"""
        current_time = time.time()
        elapsed = current_time - stats['start_time']
        remaining = max(0, test_duration - elapsed)

        progress_percent = min(100, (elapsed / test_duration) * 100) if test_duration > 0 else 0

        # Прогресс-бар (50 символов)
        filled_length = int(50 * progress_percent / 100)
        progress_bar = "█" * filled_length + "░" * (50 - filled_length)

        # RPS расчет
        rps = stats['total_requests'] / elapsed if elapsed > 0 else 0

        # Форматируем вывод
        if stats['total_requests'] > 0:
            success_rate = (stats['successful_requests'] / stats['total_requests']) * 100
            success_str = f'{success_rate:.1f}%'
        else:
            success_str = '0%'

        print(
            f'\r📊 {stats["total_requests"]} запр | '
            f'✅ {stats["successful_requests"]} | '
            f'❌ {stats["failed_requests"]} ({success_str}) | '
            f'⏱️ {elapsed:.0f}/{test_duration}с | '
            f'📈 {rps:.1f} RPS | '
            f'[{progress_bar}] {progress_percent:.0f}% ',
            end='',
            flush=True
        )

        last_progress_time[0] = current_time

    def _print_final_progress(self, stats, test_duration):
        """Печатает финальный прогресс"""
        elapsed = time.time() - stats['start_time']
        progress_percent = min(100, (elapsed / test_duration) * 100) if test_duration > 0 else 100

        # Прогресс-бар полностью заполненный
        progress_bar = "█" * 50

        if stats['total_requests'] > 0:
            rps = stats['total_requests'] / elapsed if elapsed > 0 else 0
            success_rate = (stats['successful_requests'] / stats['total_requests']) * 100
            success_str = f'{success_rate:.1f}%'
        else:
            rps = 0
            success_str = '0%'

        print(
            f'\r📊 {stats["total_requests"]} запр | '
            f'✅ {stats["successful_requests"]} | '
            f'❌ {stats["failed_requests"]} ({success_str}) | '
            f'⏱️ {elapsed:.0f}/{test_duration}с | '
            f'📈 {rps:.1f} RPS | '
            f'[{progress_bar}] {progress_percent:.0f}% ✓',
            end='\n',
            flush=True
        )

    def make_test_request(self, test_ids, batch_size, stats, lock, stop_event=None):
        """Выполняет тестовый запрос"""
        import random

        # Проверяем флаг остановки
        if stop_event and stop_event.is_set():
            raise concurrent.futures.CancelledError()

        batch_ids = random.sample(test_ids, min(batch_size, len(test_ids)))
        id_list = ','.join(str(id) for id in batch_ids)
        query = f'fields id,name,cover.image_id; where id = ({id_list});'

        start_time = time.time()
        try:
            # Дополнительная проверка
            if stop_event and stop_event.is_set():
                raise concurrent.futures.CancelledError()

            response = make_igdb_request('games', query, debug=False)
            elapsed = time.time() - start_time

            with lock:
                stats['total_requests'] += 1
                stats['successful_requests'] += 1
                stats['response_times'].append(elapsed)
                stats['thread_stats'][threading.current_thread().name] = \
                    stats['thread_stats'].get(threading.current_thread().name, 0) + 1

            return True, elapsed, len(response) if response else 0

        except Exception as e:
            # Проверяем не отменен ли запрос
            if stop_event and stop_event.is_set():
                raise concurrent.futures.CancelledError()

            elapsed = time.time() - start_time
            error_msg = str(e)

            with lock:
                stats['total_requests'] += 1
                stats['failed_requests'] += 1
                stats['response_times'].append(elapsed)
                stats['errors'].append(error_msg)

                if '429' in error_msg or 'too many' in error_msg.lower():
                    stats['rate_limit_hit'] = True
                    raise RateLimitExceeded(f"Rate limit hit after {stats['total_requests']} requests")

            return False, elapsed, 0

    def process_completed_futures(self, futures, stats, lock, test_duration, last_progress_time):
        """Обрабатывает завершенные фьючерсы"""
        completed = []
        for future in futures:
            if future.done():
                try:
                    future.result()
                except RateLimitExceeded as e:
                    with lock:
                        stats['rate_limit_hit'] = True
                    # Пробрасываем исключение, чтобы прервать тест
                    raise e
                except concurrent.futures.CancelledError:
                    pass
                except Exception:
                    pass
                completed.append(future)

        # Удаляем завершенные фьючерсы
        for future in completed:
            try:
                futures.remove(future)
            except ValueError:
                pass

    def print_final_stats(self, stats, max_workers, batch_size):
        """Выводит финальную статистику"""
        total_time = stats['end_time'] - stats['start_time']

        self.stdout.write(f'\n\n📊 ФИНАЛЬНАЯ СТАТИСТИКА ТЕСТА')
        self.stdout.write('=' * 70)

        self.stdout.write(f'📈 ОБЩАЯ СТАТИСТИКА:')
        self.stdout.write(f'   • Общее время: {total_time:.1f} сек')
        self.stdout.write(f'   • Всего запросов: {stats["total_requests"]}')
        self.stdout.write(f'   • Успешных: {stats["successful_requests"]}')
        self.stdout.write(f'   • Неудачных: {stats["failed_requests"]}')

        if stats['total_requests'] > 0:
            success_rate = (stats['successful_requests'] / stats['total_requests']) * 100
            self.stdout.write(f'   • Успешность: {success_rate:.1f}%')

        if total_time > 0:
            rps = stats['total_requests'] / total_time
            self.stdout.write(f'   • Запросов в секунду: {rps:.2f}')

        # Статистика времени ответа
        if stats['response_times']:
            avg_time = statistics.mean(stats['response_times'])
            min_time = min(stats['response_times'])
            max_time = max(stats['response_times'])

            if len(stats['response_times']) > 1:
                p95 = sorted(stats['response_times'])[int(len(stats['response_times']) * 0.95)]
                p99 = sorted(stats['response_times'])[int(len(stats['response_times']) * 0.99)]
            else:
                p95 = p99 = avg_time

            self.stdout.write(f'\n⏱️  ВРЕМЯ ОТВЕТА:')
            self.stdout.write(f'   • Среднее: {avg_time:.3f} сек')
            self.stdout.write(f'   • Минимум: {min_time:.3f} сек')
            self.stdout.write(f'   • Максимум: {max_time:.3f} сек')
            self.stdout.write(f'   • 95-й перцентиль: {p95:.3f} сек')
            self.stdout.write(f'   • 99-й перцентиль: {p99:.3f} сек')

        # Распределение по потокам
        if stats['thread_stats']:
            self.stdout.write(f'\n🧵 РАСПРЕДЕЛЕНИЕ ПО ПОТОКАМ:')
            total_thread_req = sum(stats['thread_stats'].values())
            for thread_name, count in sorted(stats['thread_stats'].items(), key=lambda x: x[1], reverse=True):
                percentage = (count / total_thread_req) * 100 if total_thread_req > 0 else 0
                self.stdout.write(f'   • {thread_name}: {count} запросов ({percentage:.1f}%)')

        # Ошибки если есть
        if stats['errors']:
            self.stdout.write(f'\n⚠️  ОШИБКИ ({len(stats["errors"])}):')
            error_counts = {}
            for error in stats['errors']:
                error_msg = str(error).split(':')[0] if ':' in str(error) else str(error)
                error_counts[error_msg] = error_counts.get(error_msg, 0) + 1

            for error_msg, count in list(error_counts.items())[:5]:
                self.stderr.write(f'   • {error_msg}: {count} раз')

        # РЕКОМЕНДАЦИИ
        self.stdout.write(f'\n💡 РЕКОМЕНДАЦИИ ДЛЯ ПРОИЗВОДСТВЕННОЙ СИСТЕМЫ:')

        # Оптимальный таймаут
        if stats['response_times']:
            recommended_timeout = max(max_time * 2, 10.0)
            self.stdout.write(f'   • Таймаут запроса: {recommended_timeout:.1f} сек')

        # Параллелизм
        optimal_workers = min(max_workers, 5)  # IGDB может ограничивать
        self.stdout.write(f'   • Оптимальное количество потоков: {optimal_workers}')

        # Размер пачки
        self.stdout.write(f'   • Размер пачки: {batch_size} (максимум API)')

        # RPS ограничения
        if stats['total_requests'] > 0 and total_time > 0:
            actual_rps = stats['total_requests'] / total_time
            safe_rps = actual_rps * 0.7  # 70% от достигнутого
            self.stdout.write(f'   • Безопасный RPS: {safe_rps:.1f}')

        # Паузы
        self.stdout.write(f'   • Пауза между запросами: 0.1-0.2 сек')

        # Обработка ошибок
        self.stdout.write(f'   • Повтор попытки при ошибке: 2-3 раза')
        self.stdout.write(f'   • Экспоненциальная задержка при повторе: 1, 2, 4 сек')


class RateLimiter:
    """Ограничитель скорости запросов"""

    def __init__(self, target_rps):
        self.target_rps = target_rps
        self.min_interval = 1.0 / target_rps if target_rps > 0 else 0
        self.last_request_time = 0
        self.lock = threading.Lock()

    def wait(self):
        """Ждет разрешения на следующий запрос"""
        if self.target_rps <= 0:
            return

        with self.lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time

            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)

            self.last_request_time = time.time()


class RateLimitExceeded(Exception):
    """Исключение при достижении лимита запросов"""
    pass