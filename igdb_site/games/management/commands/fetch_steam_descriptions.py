# games/management/commands/fetch_steam_descriptions.py

"""
Django management command для получения описаний игр из Steam API.
Автоматически перезапускается с новым оффсетом для каждой итерации.
Поддерживает поиск игры по названию и автоматические паузы при ошибках.
"""

import requests
import time
import logging
import re
import traceback
import signal
import sys
import os
import subprocess
import random
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime, timedelta
from pathlib import Path
from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
from django.db.models import Q
from django.conf import settings
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from threading import Lock
from games.models_parts.game import Game
from games.models_parts.simple_models import Platform

logger = logging.getLogger(__name__)


class SteamRateLimiter:
    """Класс для управления rate limiting и ошибками."""

    def __init__(self, max_consecutive_failures=10, base_wait_time=60, max_wait_time=300):
        self.consecutive_failures = 0
        self.consecutive_403 = 0
        self.max_consecutive_failures = max_consecutive_failures
        self.base_wait_time = base_wait_time
        self.max_wait_time = max_wait_time
        self.last_failure_time = None
        self.total_failures = 0
        self.total_403 = 0
        self.wait_history = []
        self.lock = Lock()
        self.in_backoff = False
        self.backoff_until = None
        self.pause_start_time = None

    def record_failure(self, error_type: str = "unknown") -> float:
        """Запись любой неудачной попытки. Возвращает время ожидания если нужно."""
        try:
            acquired = self.lock.acquire(timeout=2)
            if not acquired:
                return 0

            try:
                self.consecutive_failures += 1
                self.total_failures += 1
                self.last_failure_time = datetime.now()

                if error_type == "403":
                    self.consecutive_403 += 1
                    self.total_403 += 1

                if self.consecutive_failures >= self.max_consecutive_failures:
                    return self._calculate_wait_time()
            finally:
                self.lock.release()
        except Exception:
            pass

        return 0

    def record_success(self):
        """Сброс счетчика при успешном запросе."""
        try:
            acquired = self.lock.acquire(timeout=2)
            if not acquired:
                return

            try:
                if self.consecutive_failures > 0:
                    self.consecutive_failures = 0
                    self.consecutive_403 = 0
                    self.in_backoff = False
                    self.backoff_until = None
                    self.pause_start_time = None
            finally:
                self.lock.release()
        except Exception:
            pass

    def _calculate_wait_time(self) -> float:
        """Расчет времени ожидания с экспоненциальной задержкой."""
        exponent = self.consecutive_failures - self.max_consecutive_failures + 1
        wait_time = min(
            self.base_wait_time * (2 ** max(0, exponent)),
            self.max_wait_time
        )

        wait_time = wait_time * (0.8 + 0.4 * random.random())

        self.in_backoff = True
        self.backoff_until = datetime.now() + timedelta(seconds=wait_time)
        self.pause_start_time = datetime.now()

        self.wait_history.append({
            'time': datetime.now(),
            'wait': wait_time,
            'consecutive': self.consecutive_failures,
            'reason': f"{self.consecutive_failures} ошибок подряд"
        })

        return wait_time

    def should_backoff(self) -> bool:
        """Проверка нужно ли делать backoff."""
        try:
            acquired = self.lock.acquire(timeout=1)
            if not acquired:
                return False

            try:
                if self.in_backoff and self.backoff_until:
                    if datetime.now() < self.backoff_until:
                        return True
                    else:
                        self.in_backoff = False
                        self.backoff_until = None
                        self.pause_start_time = None
                return False
            finally:
                self.lock.release()
        except Exception:
            return False

    def get_wait_time_remaining(self) -> float:
        """Сколько осталось ждать."""
        try:
            acquired = self.lock.acquire(timeout=1)
            if not acquired:
                return 0

            try:
                if self.in_backoff and self.backoff_until:
                    remaining = (self.backoff_until - datetime.now()).total_seconds()
                    return max(0, remaining)
                return 0
            finally:
                self.lock.release()
        except Exception:
            return 0

    def get_status(self) -> Dict:
        """Получение статуса rate limiter."""
        try:
            acquired = self.lock.acquire(timeout=2)
            if not acquired:
                return {
                    'consecutive_failures': self.consecutive_failures,
                    'in_backoff': False,
                    'pause_start': None
                }

            try:
                return {
                    'consecutive_failures': self.consecutive_failures,
                    'in_backoff': self.in_backoff,
                    'pause_start': self.pause_start_time.isoformat() if self.pause_start_time else None
                }
            finally:
                self.lock.release()
        except Exception:
            return {
                'consecutive_failures': 0,
                'in_backoff': False,
                'pause_start': None
            }


class Command(BaseCommand):
    """Steam descriptions fetcher с автоматическим перезапуском и обработкой ошибок."""

    help = 'Получение описаний из Steam с автоматическим перезапуском для каждого оффсета'

    def add_arguments(self, parser: CommandParser) -> None:
        """Добавление аргументов команды."""
        parser.add_argument(
            '--limit',
            type=int,
            default=10000,
            help='Максимальное количество игр для обработки (по умолчанию: 10000)'
        )

        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Смещение от начала списка (по умолчанию: 0)'
        )

        parser.add_argument(
            '--game-name',
            type=str,
            help='Название игры для поиска (будет найдена самая популярная)'
        )

        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Тестовый запуск без сохранения в базу данных'
        )

        parser.add_argument(
            '--batch-size',
            type=int,
            default=30,
            help='Размер итерации (по умолчанию: 30)'
        )

        parser.add_argument(
            '--iteration-pause',
            type=int,
            default=1,
            help='Пауза между итерациями в секундах (по умолчанию: 1)'
        )

        parser.add_argument(
            '--workers',
            type=int,
            default=3,
            help='Количество параллельных воркеров (по умолчанию: 3)'
        )

        parser.add_argument(
            '--delay',
            type=float,
            default=0.5,
            help='Задержка между запросами в секундах (по умолчанию: 0.5)'
        )

        parser.add_argument(
            '--timeout',
            type=float,
            default=5,
            help='Таймаут запроса в секундах (по умолчанию: 5)'
        )

        parser.add_argument(
            '--output-file',
            type=str,
            default='steam_descriptions_all.txt',
            help='Выходной TXT файл для описаний'
        )

        parser.add_argument(
            '--not-found-file',
            type=str,
            default='steam_not_found.txt',
            help='Файл для списка не найденных игр'
        )

        parser.add_argument(
            '--process-not-found',
            action='store_true',
            help='Обрабатывать только игры из файла не найденных'
        )

        parser.add_argument(
            '--skip-not-found',
            action='store_true',
            default=True,
            help='Пропускать игры из файла не найденных (по умолчанию: True)'
        )

        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительное обновление даже если описание уже есть'
        )

        parser.add_argument(
            '--skip-search',
            action='store_true',
            help='Пропустить поиск в Steam, использовать название игры напрямую'
        )

        parser.add_argument(
            '--debug',
            action='store_true',
            default=True,
            help='Показывать детальные сообщения об ошибках (по умолчанию: True)'
        )

        parser.add_argument(
            '--verbose',
            action='store_true',
            default=True,
            help='Показывать подробный вывод включая API запросы (по умолчанию: True)'
        )

        parser.add_argument(
            '--no-restart',
            action='store_true',
            help='Не перезапускать команду для следующих оффсетов'
        )

        parser.add_argument(
            '--output-dir',
            type=str,
            default='fetch_steam_descriptions',
            help='Директория для сохранения файлов (по умолчанию: fetch_steam_descriptions)'
        )

        parser.add_argument(
            '--max-consecutive-failures',
            type=int,
            default=3,
            help='Максимальное количество ЛЮБЫХ ошибок подряд перед паузой (по умолчанию: 3)'
        )

        parser.add_argument(
            '--base-wait',
            type=int,
            default=60,
            help='Базовая пауза при ошибках в секундах (по умолчанию: 60)'
        )

        parser.add_argument(
            '--max-wait',
            type=int,
            default=180,
            help='Максимальная пауза при ошибках в секундах (по умолчанию: 180)'
        )

        parser.add_argument(
            '--batch-failure-threshold',
            type=float,
            default=0.3,
            help='Порог неудач в батче для паузы (0.0-1.0, по умолчанию: 0.3)'
        )

        parser.add_argument(
            '--processed',
            type=int,
            default=0,
            help='Количество уже обработанных игр (для продолжения)'
        )

        parser.add_argument(
            '--log-file',
            type=str,
            default='steam_fetcher_timeline.log',
            help='Файл для лога временной шкалы (по умолчанию: steam_fetcher_timeline.log)'
        )

        parser.add_argument(
            '--stats-file',
            type=str,
            help='Файл с накопленной статистикой для загрузки'
        )

    def __init__(self, *args, **kwargs):
        """Инициализация команды."""
        super().__init__(*args, **kwargs)
        self.stats_lock = Lock()
        self.output_lock = Lock()
        self.descriptions_buffer = []
        self.not_found_buffer = []
        self.buffer_size = 50
        self.session = self._create_session()
        self.pc_platform = None
        self.debug = False
        self.verbose = False
        self.interrupted = False
        self.total_stats = {
            'success': 0,
            'not_found': 0,
            'no_description': 0,
            'error': 0,
            'error_403': 0,
            'error_timeout': 0,
            'error_other': 0,
            'iterations': 0,
            'backoff_pauses': 0,
            'skipped_not_found': 0
        }
        self.output_dir = None
        self.full_output_path = None
        self.not_found_file_path = None
        self.log_file_path = None
        self.not_found_games = set()
        self.current_offset = 0
        self.error_log = []
        self.rate_limiter = None
        self.batch_failure_threshold = 0.3
        self.batch_size = 30
        self.workers = 3
        self.delay = 0.5
        self.timeout = 5
        self.output_file = 'steam_descriptions_all.txt'
        self.not_found_file = 'steam_not_found.txt'
        self.log_file = 'steam_fetcher_timeline.log'
        self.start_time = None
        self.processed_before_pause = 0  # Сколько игр обработано до паузы
        self._pause_active = False  # Флаг активной паузы

    def _initialize_parameters(self, options: Dict, pc: Platform) -> Optional[Tuple]:
        """Инициализация параметров из options."""
        limit_from_args = options['limit']
        force = options['force']
        game_name = options['game_name']
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        iteration_pause = options['iteration_pause']
        self.workers = options['workers']
        self.delay = options['delay']
        self.timeout = options['timeout']
        self.batch_size = batch_size
        output_file = options['output_file']
        not_found_file = options['not_found_file']
        process_not_found = options['process_not_found']
        skip_not_found = options['skip_not_found']
        skip_search = options['skip_search']
        self.debug = options['debug']
        self.verbose = options['verbose']
        no_restart = options['no_restart']
        output_dir = options['output_dir']
        log_file = options['log_file']
        self.output_file = output_file
        self.not_found_file = not_found_file
        self.log_file = log_file

        processed_total = options.get('processed', 0)

        # Создаем директорию
        self.output_dir = Path(output_dir)
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.log_debug("Ошибка при создании выходной директории", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Не удалось создать директорию {output_dir}: {e}'))
            return None

        # Устанавливаем полные пути к файлам
        self.full_output_path = self.output_dir / output_file
        self.not_found_file_path = self.output_dir / not_found_file
        self.log_file_path = self.output_dir / log_file

        # Создаем файл не найденных игр ТОЛЬКО если его нет
        if not self.not_found_file_path.exists():
            try:
                with open(self.not_found_file_path, 'w', encoding='utf-8') as f:
                    header = f"{'=' * 80}\n"
                    header += f"STEAM NOT FOUND GAMES\n"
                    header += f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    header += f"{'=' * 80}\n\n"
                    f.write(header)
                self.stdout.write(self.style.SUCCESS(f'📁 Создан файл не найденных игр: {self.not_found_file_path}'))
            except Exception as e:
                self.log_debug("Ошибка при создании файла не найденных игр", error=e)

        # Очищаем лог-файл при первом запуске
        if processed_total == 0:
            try:
                with open(self.log_file_path, 'w', encoding='utf-8') as f:
                    f.write(f"=== ЛОГ ВРЕМЕННОЙ ШКАЛЫ ===\n")
                    f.write(f"Программа: fetch_steam_descriptions\n")
                    f.write(f"{'=' * 50}\n\n")
            except Exception:
                pass

        # Загружаем not_found_games
        self.not_found_games = self.load_not_found_games()

        if not game_name:
            base_queryset = Game.objects.filter(platforms=pc)

            if not force:
                base_queryset = base_queryset.filter(
                    Q(rawg_description__isnull=True) | Q(rawg_description='')
                )

            if skip_not_found is True and self.not_found_games:
                base_queryset = base_queryset.exclude(id__in=self.not_found_games)
                self.stdout.write(self.style.WARNING(f'📊 Исключено {len(self.not_found_games)} ранее не найденных игр'))

            if process_not_found is True and self.not_found_games:
                base_queryset = Game.objects.filter(id__in=self.not_found_games)
                self.stdout.write(
                    self.style.WARNING(f'📊 Обработка только {len(self.not_found_games)} ранее не найденных игр'))

            total_to_process = base_queryset.count()

            self.stdout.write(self.style.SUCCESS(f'📊 Всего игр на PC: {Game.objects.filter(platforms=pc).count()}'))
            self.stdout.write(self.style.SUCCESS(f'📊 Игр для обработки: {total_to_process}'))

            if limit_from_args == 10000:
                limit = total_to_process
                self.stdout.write(self.style.WARNING(f'🔄 Автоматически установлен limit = {limit} игр'))
            else:
                limit = min(limit_from_args, total_to_process)
                self.stdout.write(self.style.WARNING(f'📊 Ручной limit = {limit} из {total_to_process} доступных'))
            target_game = None
        else:
            result = self._find_specific_game(game_name, pc)
            if not result:
                return None
            target_game, limit, batch_size, no_restart = result
            self.batch_size = batch_size
            total_to_process = 1
            processed_total = 0
            process_not_found = False
            skip_not_found = False

        self.current_offset = options['offset']

        return (limit, total_to_process, target_game, processed_total,
                batch_size, iteration_pause, output_file, output_dir,
                dry_run, force, skip_search, no_restart, process_not_found, skip_not_found)

    def save_not_found_buffer(self, is_first: bool = False):
        """Сохранение буфера не найденных игр в файл."""
        if not self.not_found_buffer:
            return

        file_path = self.not_found_file_path

        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.log_debug("Ошибка создания директории", error=e)
            return

        with self.output_lock:
            try:
                # Всегда открываем в режиме append, так как файл уже создан
                with open(file_path, 'a', encoding='utf-8') as f:
                    for game_info in self.not_found_buffer:
                        f.write(game_info)
                        f.write("\n")

                self.log_debug(f"Добавлено {len(self.not_found_buffer)} игр в файл не найденных")
                self.not_found_buffer = []

            except IOError as e:
                self.log_debug("Ошибка записи файла не найденных игр", error=e)

    def load_not_found_games(self) -> set:
        """Загрузка списка не найденных игр из файла."""
        not_found_set = set()
        file_path = self.not_found_file_path

        if file_path and file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        # Пропускаем заголовки и пустые строки
                        if (line and not line.startswith('=') and
                                not line.startswith('STEAM NOT FOUND GAMES') and
                                not line.startswith('Created:') and
                                'Game ID:' in line):
                            try:
                                game_id = int(line.split('Game ID:')[1].split('-')[0].strip())
                                not_found_set.add(game_id)
                            except (ValueError, IndexError):
                                pass
                self.stdout.write(
                    self.style.SUCCESS(f'📂 Загружено {len(not_found_set)} не найденных игр из {file_path}'))
            except Exception as e:
                self.log_debug(f"Ошибка при загрузке файла не найденных игр", error=e)
        else:
            self.stdout.write(self.style.WARNING(f'📂 Файл не найденных игр не существует: {file_path}'))

        return not_found_set

    def add_to_not_found(self, game: Game, reason: str = "not_found"):
        """Добавление игры в список не найденных."""
        # Проверяем, нет ли уже игры в списке
        if game.id in self.not_found_games:
            return

        game_info = f"Game ID: {game.id} - {game.name} (Rating: {game.rating or 'N/A'}) - {reason}"
        self.not_found_buffer.append(game_info)

        # Сразу добавляем в множество, чтобы исключить при текущем запуске
        self.not_found_games.add(game.id)

        # Сохраняем в файл
        self.save_not_found_buffer()

        self.log_debug(f"Добавлена не найденная игра: {game.name} (ID: {game.id})")

    def _create_session(self) -> requests.Session:
        """Создание HTTP сессии без повторов."""
        session = requests.Session()

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=30,
            pool_maxsize=30,
            max_retries=0
        )

        session.mount('http://', adapter)
        session.mount('https://', adapter)

        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Cache-Control': 'no-cache',
            'Pragma': 'no-cache'
        })

        return session

    def log_timeline(self, event_type: str, games_processed: int = None):
        """
        Логирование событий временной шкалы.
        event_type: START, STOP, RESUME, END
        games_processed: количество обработанных игр (для STOP и END)
        """
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if event_type == "START":
            message = f"[{timestamp}] НАЧАЛО РАБОТЫ"
        elif event_type == "STOP":
            message = f"[{timestamp}] ПАУЗА - обработано игр: {games_processed}"
        elif event_type == "RESUME":
            message = f"[{timestamp}] ВОЗОБНОВЛЕНИЕ"
        elif event_type == "END":
            message = f"[{timestamp}] ЗАВЕРШЕНИЕ - всего обработано игр: {games_processed}"
        else:
            return

        # Выводим в консоль
        if event_type == "START" or event_type == "RESUME":
            self.stdout.write(self.style.SUCCESS(message))
        elif event_type == "STOP":
            self.stdout.write(self.style.ERROR(message))
        elif event_type == "END":
            self.stdout.write(self.style.SUCCESS(message))

        # Сохраняем в файл
        if self.log_file_path:
            try:
                with open(self.log_file_path, 'a', encoding='utf-8') as f:
                    f.write(message + "\n")
            except Exception:
                pass

    def signal_handler(self, signum, frame):
        """Обработчик сигнала прерывания (Ctrl+C)."""
        self.stdout.write(self.style.ERROR('\n\n⚠️  Получен сигнал прерывания (Ctrl+C)'))
        self.interrupted = True
        os._exit(130)

    def log_debug(self, message: str, game_name: str = None, error: Exception = None):
        """Логирование debug сообщений."""
        if self.debug:
            prefix = f"[DEBUG] {game_name}: " if game_name else "[DEBUG] "

            if error:
                error_type = type(error).__name__
                error_msg = str(error)
                trace = traceback.format_exc().split('\n')[-3:-1]
                trace_str = ' '.join(trace)

                full_message = f'{prefix}{message}\n'
                full_message += f'  🔴 Тип: {error_type}\n'
                full_message += f'  📝 Сообщение: {error_msg}\n'
                full_message += f'  🔍 Трейс: {trace_str}'

                self.error_log.append(full_message)
            else:
                full_message = f'{prefix}{message}'

            self.stdout.write(self.style.WARNING(full_message))

    def log_verbose(self, message: str, game_name: str = None, response=None):
        """Логирование verbose сообщений."""
        if self.verbose:
            prefix = f"[VERBOSE] {game_name}: " if game_name else "[VERBOSE] "

            if response:
                status = response.status_code
                url = response.url
                size = len(response.content) if response.content else 0

                full_message = f'{prefix}{message}\n'
                full_message += f'  🌐 URL: {url}\n'
                full_message += f'  📊 Статус: {status}\n'
                full_message += f'  📦 Размер: {size} байт'

                if status == 403:
                    full_message += f'\n  🚫 ДОСТУП ЗАПРЕЩЕН (403)'
                elif status == 404:
                    full_message += f'\n  🔍 НЕ НАЙДЕНО (404)'
                elif status != 200:
                    full_message += f'\n  ⚠️ Тело: {response.text[:200]}...'
            else:
                full_message = f'{prefix}{message}'

            self.stdout.write(self.style.NOTICE(full_message))

    def check_rate_limit(self) -> bool:
        """Проверка rate limiting и выполнение backoff при необходимости."""
        # Если пауза уже активна, не начинаем новую
        if hasattr(self, '_pause_active') and self._pause_active:
            return True

        if self.rate_limiter and self.rate_limiter.should_backoff():
            remaining = self.rate_limiter.get_wait_time_remaining()
            if remaining > 0:
                status = self.rate_limiter.get_status()

                consecutive = status["consecutive_failures"]

                # Устанавливаем флаг активной паузы
                self._pause_active = True

                # Получаем общее количество обработанных игр
                total_processed = self.processed_total if hasattr(self, 'processed_total') else 0

                # Логируем начало паузы
                self.log_timeline("STOP", total_processed)

                self.stdout.write(self.style.ERROR(
                    f'\n🚫 ОБНАРУЖЕНО {consecutive} НЕУДАЧ ПОДРЯД!'
                ))
                self.stdout.write(self.style.ERROR(
                    f'⏳ Пауза на {remaining:.1f}с для снятия блокировки Steam...'
                ))

                try:
                    self.session.close()
                except:
                    pass

                last_display = 0
                last_remaining = remaining
                pause_start_time = time.time()

                while remaining > 0 and not self.interrupted:
                    try:
                        current_remaining = self.rate_limiter.get_wait_time_remaining()

                        if int(current_remaining) != int(last_remaining) or current_remaining <= 5:
                            if time.time() - last_display > 0.2:
                                print(f'\r   Осталось {int(current_remaining)}с...   ', end='', flush=True)
                                last_display = time.time()
                                last_remaining = current_remaining

                        time.sleep(0.1)
                        remaining = current_remaining

                    except KeyboardInterrupt:
                        self.interrupted = True
                        break
                    except Exception:
                        pass

                print()

                if self.interrupted:
                    self.stdout.write(self.style.WARNING('\n⚠️ Пауза прервана пользователем'))
                    self._pause_active = False
                    return False

                # Логируем возобновление работы
                self.log_timeline("RESUME")

                try:
                    self.stdout.write(self.style.WARNING('🔄 Пересоздание HTTP сессии...'))
                    self.session = self._create_session()
                    self.stdout.write(self.style.SUCCESS('✅ Сессия пересоздана'))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'❌ Ошибка при создании сессии: {e}'))

                self.stdout.write(self.style.SUCCESS(
                    f'✅ Пауза завершена, возобновляем работу'
                ))

                with self.stats_lock:
                    self.total_stats['backoff_pauses'] += 1

                # Снимаем флаг активной паузы
                self._pause_active = False

                return True
        return False

    def get_pc_platform(self) -> Optional[Platform]:
        """Получение платформы PC."""
        if self.pc_platform is None:
            try:
                self.log_debug("Поиск платформы PC")

                queries = [
                    Q(name__iexact='PC'),
                    Q(name__iexact='PC (Microsoft Windows)'),
                    Q(name__icontains='windows'),
                    Q(name__icontains='win')
                ]

                query = Q()
                for q in queries:
                    query |= q

                self.pc_platform = Platform.objects.filter(query).first()

                if self.pc_platform:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f'✅ Найдена платформа PC: {self.pc_platform.name} (ID: {self.pc_platform.id})')
                    )
                else:
                    self.stdout.write(self.style.ERROR('❌ Платформа PC не найдена!'))

                    if self.debug:
                        all_platforms = Platform.objects.all()[:10]
                        self.stdout.write(self.style.WARNING('📋 Доступные платформы:'))
                        for p in all_platforms:
                            self.stdout.write(f'  - {p.name} (ID: {p.id})')

            except Exception as e:
                self.log_debug("Ошибка при получении платформы PC", error=e)
                return None

        return self.pc_platform

    def get_games_batch(self, offset: int, batch_size: int, force: bool,
                        skip_not_found: bool = True, not_found_set: set = None) -> List[Game]:
        """Получение батча игр с учетом не найденных."""
        pc = self.get_pc_platform()
        if not pc:
            return []

        try:
            queryset = Game.objects.filter(platforms=pc)

            if not force:
                queryset = queryset.filter(
                    Q(rawg_description__isnull=True) |
                    Q(rawg_description='')
                )

            if skip_not_found is True and not_found_set:
                queryset = queryset.exclude(id__in=not_found_set)

            games = list(queryset.order_by('-rating_count', 'id')[offset:offset + batch_size])

            if games:
                self.stdout.write(
                    self.style.SUCCESS(f'📊 Батч: игры {offset + 1}-{offset + len(games)} (смещение {offset})')
                )

                if self.debug:
                    for i, game in enumerate(games[:3], 1):
                        has_desc = bool(game.rawg_description)
                        desc_status = "есть описание" if has_desc else "нет описания"
                        not_found_status = " (была не найдена)" if game.id in (not_found_set or set()) else ""
                        self.stdout.write(f'    {i}. {game.name} (ID: {game.id}, {desc_status}{not_found_status})')

            return games

        except Exception as e:
            self.log_debug("Ошибка при получении батча игр", error=e)
            return []

    def clean_html(self, text: Optional[str]) -> Optional[str]:
        """Очистка HTML тегов."""
        if not text:
            return text
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def search_steam(self, game_name: str, timeout: float) -> Tuple[Optional[int], Optional[str]]:
        """Поиск игры в Steam. Возвращает (app_id, error_type)."""
        if self.rate_limiter and self.rate_limiter.should_backoff():
            self.check_rate_limit()
            if self.rate_limiter.should_backoff():
                return None, 'backoff'

        search_name = re.sub(r'[^\w\s-]', '', game_name)
        search_name = re.sub(r'\s+', ' ', search_name).strip()

        if not search_name:
            return None, 'invalid_name'

        self.log_verbose(f"Поиск в Steam: '{search_name}'", game_name=game_name)

        try:
            self.session.headers.update({'Connection': 'keep-alive'})
        except (AttributeError, OSError, Exception):
            self.log_debug(f"Пересоздание поврежденной сессии", game_name=game_name)
            self.session = self._create_session()

        try:
            url = "https://store.steampowered.com/api/storesearch"
            params = {
                'term': search_name[:50],
                'l': 'english',
                'cc': 'us'
            }

            response = self.session.get(url, params=params, timeout=timeout)
            self.log_verbose(f"Ответ от Steam Search API", game_name=game_name, response=response)

            if response.status_code != 200:
                error_type = 'unknown'
                if response.status_code == 403:
                    error_type = '403'
                    with self.stats_lock:
                        self.total_stats['error_403'] += 1
                elif response.status_code == 404:
                    error_type = '404'
                elif response.status_code == 429:
                    error_type = '429'
                elif response.status_code >= 500:
                    error_type = '5xx'

                wait_time = self.rate_limiter.record_failure(error_type)
                if wait_time > 0:
                    self.check_rate_limit()

                return None, error_type
            else:
                self.rate_limiter.record_success()

            data = response.json()
            if data.get('total', 0) > 0:
                items = data.get('items', [])
                if items:
                    app_id = items[0].get('id')
                    self.log_verbose(f"Найден Steam ID: {app_id}", game_name=game_name)
                    return app_id, None
                else:
                    return None, 'not_found'
            else:
                self.log_debug(f"Нет результатов для '{search_name}'", game_name=game_name)
                return None, 'not_found'

        except requests.Timeout:
            self.log_debug(f"Таймаут при поиске", game_name=game_name)
            self.rate_limiter.record_failure("timeout")
            return None, 'timeout'
        except requests.ConnectionError as e:
            self.log_debug(f"Ошибка соединения", game_name=game_name, error=e)
            self.rate_limiter.record_failure("connection")
            return None, 'connection'
        except OSError as e:
            self.log_debug(f"Ошибка сокета, пересоздание сессии", game_name=game_name, error=e)
            self.rate_limiter.record_failure("socket_error")
            try:
                self.session = self._create_session()
                response = self.session.get(url, params=params, timeout=timeout)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('total', 0) > 0 and data.get('items'):
                        app_id = data['items'][0].get('id')
                        return app_id, None
            except:
                pass
            return None, 'socket_error'
        except Exception as e:
            self.log_debug(f"Неизвестная ошибка", game_name=game_name, error=e)
            self.rate_limiter.record_failure("exception")
            return None, 'exception'

    def fetch_description(self, app_id: int, timeout: float, game_name: str = None) -> Tuple[
        Optional[str], Optional[str]]:
        """Получение описания игры. Возвращает (description, error_type)."""
        if self.rate_limiter and self.rate_limiter.should_backoff():
            self.check_rate_limit()
            if self.rate_limiter.should_backoff():
                return None, 'backoff'

        self.log_verbose(f"Запрос описания для App ID: {app_id}", game_name=game_name)

        try:
            url = "https://store.steampowered.com/api/appdetails"
            params = {
                'appids': app_id,
                'l': 'english',
                'cc': 'us'
            }

            response = self.session.get(url, params=params, timeout=timeout)
            self.log_verbose(f"Ответ от Steam AppDetails API", game_name=game_name, response=response)

            if response.status_code != 200:
                error_type = 'unknown'
                if response.status_code == 403:
                    error_type = '403'
                    with self.stats_lock:
                        self.total_stats['error_403'] += 1
                elif response.status_code == 404:
                    error_type = '404'
                elif response.status_code == 429:
                    error_type = '429'
                elif response.status_code >= 500:
                    error_type = '5xx'

                wait_time = self.rate_limiter.record_failure(error_type)
                if wait_time > 0:
                    self.check_rate_limit()

                return None, error_type
            else:
                self.rate_limiter.record_success()

            try:
                data = response.json()
            except ValueError:
                self.log_debug(f"Ошибка парсинга JSON", game_name=game_name)
                return None, 'json_error'

            if str(app_id) not in data or not data[str(app_id)]['success']:
                self.log_debug(f"App ID {app_id} не найден в ответе", game_name=game_name)
                return None, 'not_found'

            game_data = data[str(app_id)]['data']

            for field in ['detailed_description', 'about_the_game', 'short_description']:
                desc = game_data.get(field, '')
                if desc:
                    self.log_verbose(f"Найдено описание в поле '{field}'", game_name=game_name)
                    return self.clean_html(desc), None

            self.log_debug(f"Нет описания ни в одном из полей", game_name=game_name)
            return None, 'no_description'

        except requests.Timeout:
            self.log_debug(f"Таймаут при получении описания", game_name=game_name)
            self.rate_limiter.record_failure("timeout")
            return None, 'timeout'
        except requests.ConnectionError as e:
            self.log_debug(f"Ошибка соединения", game_name=game_name, error=e)
            self.rate_limiter.record_failure("connection")
            return None, 'connection'
        except OSError as e:
            self.log_debug(f"Ошибка сокета", game_name=game_name, error=e)
            self.rate_limiter.record_failure("socket_error")
            return None, 'socket_error'
        except Exception as e:
            self.log_debug(f"Неизвестная ошибка", game_name=game_name, error=e)
            self.rate_limiter.record_failure("exception")
            return None, 'exception'

    def format_for_file(self, game: Game, description: str, app_id: int) -> str:
        """Форматирование для файла."""
        try:
            try:
                genres = [g.name for g in game.genres.all()[:3]] if hasattr(game, 'genres') else ['N/A']
            except Exception:
                genres = ['ERROR']

            lines = [
                f"Game: {game.name}",
                f"ID: {game.id}",
                f"Steam App ID: {app_id}",
                f"Rating: {game.rating or 'N/A'}",
                f"Genres: {', '.join(genres)}",
                "",
                "DESCRIPTION:",
                description
            ]
            return "\n".join(lines)

        except Exception as e:
            self.log_debug(f"Ошибка форматирования", game_name=game.name, error=e)
            return f"Game: {game.name}\nERROR: {str(e)}"

    def save_buffer(self, output_file: str, is_first: bool = False):
        """Сохранение буфера описаний в файл."""
        if not self.descriptions_buffer:
            return

        file_path = (self.output_dir / output_file) if self.output_dir else Path(output_file)

        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.log_debug("Ошибка создания директории", error=e)
            return

        with self.output_lock:
            try:
                mode = 'w' if is_first else 'a'

                with open(file_path, mode, encoding='utf-8') as f:
                    if is_first:
                        header = f"{'=' * 80}\n"
                        header += f"STEAM GAME DESCRIPTIONS\n"
                        header += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        header += f"Offset: {self.current_offset}\n"
                        header += f"{'=' * 80}\n\n"
                        f.write(header)

                    for desc in self.descriptions_buffer:
                        f.write(desc)
                        f.write("\n" + "-" * 80 + "\n\n")

                self.descriptions_buffer = []
                self.full_output_path = file_path

            except IOError as e:
                self.log_debug("Ошибка записи файла", error=e)

    def process_batch(self, games: List[Game], skip_search: bool, timeout: float,
                      delay: float, output_file: str, dry_run: bool,
                      batch_stats: Dict, is_first: bool = False) -> List[Game]:
        """Обработка одного батча игр."""
        games_to_update = []
        batch_failures = 0
        batch_total = len(games)

        self.stdout.write(f'  🕒 Таймаут: {timeout}с, воркеров: {self.workers}')

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {}
            for game in games:
                future = executor.submit(
                    self.process_game,
                    game, skip_search, timeout, delay,
                    output_file, dry_run, batch_stats
                )
                futures[future] = game

            completed = 0
            error_details = []
            error_403_count = 0
            timeout_count = 0

            processed_games = set()

            while completed < batch_total and not self.interrupted:
                if self.rate_limiter and self.rate_limiter.should_backoff():
                    if not self.check_rate_limit():
                        break

                for future in list(futures.keys()):
                    if future in processed_games:
                        continue

                    if future.done():
                        game = futures[future]
                        processed_games.add(future)
                        completed += 1

                        try:
                            result = future.result(timeout=1)

                            if result['success']:
                                self.stdout.write(
                                    f'  [{completed}/{batch_total}] ✓ {game.name[:40]:40} (Steam: {result["app_id"]})'
                                )
                                if not dry_run:
                                    game.rawg_description = result['description']
                                    games_to_update.append(game)
                            else:
                                batch_failures += 1

                                if result.get('error_type') == 'error_403':
                                    error_403_count += 1
                                elif result.get('error_type') == 'timeout':
                                    timeout_count += 1

                                icon = {
                                    'not_found': '🔍',
                                    'no_description': '📄',
                                    'exception': '💥',
                                    'error_403': '🚫',
                                    'timeout': '⌛',
                                }.get(result.get('error_type'), '⏭️')

                                self.stdout.write(
                                    f'  [{completed}/{batch_total}] {icon} {game.name[:40]:40} - {result.get("error_message", "Ошибка")}'
                                )

                                if result.get('error_details') and self.debug:
                                    error_details.append({
                                        'game': game.name,
                                        'type': result['error_type'],
                                        'details': result['error_details']
                                    })

                        except TimeoutError:
                            batch_failures += 1
                            timeout_count += 1
                            self.stdout.write(
                                f'  [{completed}/{batch_total}] ⌛ {game.name[:40]:40} - ТАЙМАУТ ПОЛУЧЕНИЯ РЕЗУЛЬТАТА'
                            )
                            with self.stats_lock:
                                batch_stats['error'] += 1
                                self.total_stats['error'] += 1
                                self.total_stats['error_timeout'] += 1

                        except Exception as e:
                            batch_failures += 1
                            self.stdout.write(
                                f'  [{completed}/{batch_total}] 💥 {game.name[:40]:40} - Ошибка: {str(e)[:50]}'
                            )
                            with self.stats_lock:
                                batch_stats['error'] += 1
                                self.total_stats['error'] += 1
                                self.total_stats['error_other'] += 1

                time.sleep(0.1)

                if completed < batch_total:
                    current_time = time.time()
                    for future, game in list(futures.items()):
                        if future not in processed_games:
                            if hasattr(future, '_start_time'):
                                elapsed = current_time - future._start_time
                                if elapsed > timeout * 3:
                                    future.cancel()
                                    processed_games.add(future)
                                    completed += 1
                                    batch_failures += 1
                                    timeout_count += 1
                                    self.stdout.write(
                                        f'  [{completed}/{batch_total}] ⌛ {game.name[:40]:40} - ПРЕВЫШЕНО ВРЕМЯ ОЖИДАНИЯ'
                                    )
                                    with self.stats_lock:
                                        batch_stats['error'] += 1
                                        self.total_stats['error'] += 1
                                        self.total_stats['error_timeout'] += 1
                            else:
                                future._start_time = current_time

                if self.interrupted:
                    self.stdout.write(
                        self.style.WARNING('\n  ⚠️ Получен сигнал прерывания, завершаю обработку батча...'))
                    for future in list(futures.keys()):
                        if future not in processed_games and not future.done():
                            future.cancel()
                    break

            if error_403_count > 0:
                self.stdout.write(self.style.ERROR(
                    f'  🚫 403 ошибок в батче: {error_403_count}'
                ))

            if timeout_count > 0:
                self.stdout.write(self.style.WARNING(
                    f'  ⌛ Таймаутов в батче: {timeout_count}'
                ))

            if batch_total > 0:
                failure_ratio = batch_failures / batch_total

                if failure_ratio >= self.batch_failure_threshold:
                    self.stdout.write(self.style.ERROR(
                        f'\n  ⚠️ ВЫСОКИЙ УРОВЕНЬ ОШИБОК В БАТЧЕ: {failure_ratio * 100:.1f}%'
                    ))

                    wait_time = self.rate_limiter.record_failure("batch_failure")

                    if wait_time > 0:
                        self.stdout.write(self.style.ERROR(
                            f'  🚫 Слишком много ошибок, инициирую паузу...'
                        ))
                        self.check_rate_limit()

        if self.descriptions_buffer:
            self.save_buffer(output_file, is_first)

        return games_to_update

    def process_game(self, game: Game, skip_search: bool, timeout: float,
                     delay: float, output_file: str, dry_run: bool, stats: Dict) -> Dict:
        """Обработка одной игры."""
        result = {
            'success': False,
            'skipped': False,
            'description': None,
            'app_id': None,
            'error_type': None,
            'error_message': None,
            'error_details': None,
            'should_retry': False
        }

        try:
            if delay > 0:
                time.sleep(delay)

            if self.rate_limiter and self.rate_limiter.should_backoff():
                if not self.check_rate_limit():
                    result['skipped'] = True
                    result['error_type'] = 'backoff'
                    result['error_message'] = 'Пауза из-за ошибок'
                    result['should_retry'] = True
                    return result

            app_id = None
            search_error = None
            if not skip_search:
                app_id, search_error = self.search_steam(game.name, timeout)

            if not app_id:
                with self.stats_lock:
                    if search_error == 'not_found':
                        stats['not_found'] += 1
                        self.total_stats['not_found'] += 1
                        result['error_type'] = 'not_found'
                        result['error_message'] = 'Не найдена в Steam'

                        if not dry_run:
                            self.add_to_not_found(game, "not_found")
                    else:
                        stats['error'] += 1
                        self.total_stats['error'] += 1
                        if search_error == '403':
                            self.total_stats['error_403'] += 1
                        elif search_error == 'timeout':
                            self.total_stats['error_timeout'] += 1
                        else:
                            self.total_stats['error_other'] += 1
                        result['error_type'] = search_error or 'search_error'
                        result['error_message'] = f'Ошибка поиска: {search_error}'
                        result['should_retry'] = True

                result['skipped'] = True
                return result

            description, desc_error = self.fetch_description(app_id, timeout, game.name)

            if not description:
                with self.stats_lock:
                    if desc_error == 'no_description':
                        stats['no_description'] += 1
                        self.total_stats['no_description'] += 1
                        result['error_type'] = 'no_description'
                        result['error_message'] = 'Нет описания'
                    else:
                        stats['error'] += 1
                        self.total_stats['error'] += 1
                        if desc_error == '403':
                            self.total_stats['error_403'] += 1
                        elif desc_error == 'timeout':
                            self.total_stats['error_timeout'] += 1
                        else:
                            self.total_stats['error_other'] += 1
                        result['error_type'] = desc_error or 'fetch_error'
                        result['error_message'] = f'Ошибка получения: {desc_error}'
                        result['should_retry'] = True

                result['skipped'] = True
                return result

            result['success'] = True
            result['description'] = description
            result['app_id'] = app_id

            if output_file:
                formatted = self.format_for_file(game, description, app_id)
                with self.output_lock:
                    self.descriptions_buffer.append(formatted)
                    if len(self.descriptions_buffer) >= self.buffer_size:
                        self.save_buffer(output_file, is_first=(self.current_offset == 0))

            with self.stats_lock:
                stats['success'] += 1
                self.total_stats['success'] += 1

            return result

        except Exception as e:
            self.log_debug("Критическая ошибка", game_name=game.name, error=e)

            with self.stats_lock:
                stats['error'] += 1
                self.total_stats['error'] += 1
                self.total_stats['error_other'] += 1

            result['skipped'] = True
            result['error_type'] = 'exception'
            result['error_message'] = str(e)[:50]
            result['error_details'] = traceback.format_exc()
            result['should_retry'] = True
            return result

    def handle(self, *args: Any, **options: Any) -> None:
        """Основной метод выполнения."""
        self.start_time = datetime.now()

        signal.signal(signal.SIGINT, self.signal_handler)

        # Загрузка накопленной статистики если есть
        stats_file = options.get('stats_file')
        if stats_file and os.path.exists(stats_file):
            try:
                import json
                with open(stats_file, 'r', encoding='utf-8') as f:
                    loaded_stats = json.load(f)
                    # Обновляем статистику
                    for key, value in loaded_stats.items():
                        if key in self.total_stats:
                            self.total_stats[key] = value
                self.stdout.write(self.style.SUCCESS(f'📊 Загружена статистика из {stats_file}'))
                # Удаляем файл после загрузки
                try:
                    os.remove(stats_file)
                except:
                    pass
            except Exception as e:
                self.log_debug("Ошибка при загрузке статистики", error=e)

        # Инициализация rate limiter
        self.rate_limiter = SteamRateLimiter(
            max_consecutive_failures=options['max_consecutive_failures'],
            base_wait_time=options['base_wait'],
            max_wait_time=options['max_wait']
        )
        self.batch_failure_threshold = options['batch_failure_threshold']

        # Получаем PC платформу
        pc = self.get_pc_platform()
        if not pc:
            self.stdout.write(self.style.ERROR('❌ Критическая ошибка: платформа PC не найдена'))
            return

        # Инициализация параметров
        params = self._initialize_parameters(options, pc)
        if not params:
            return

        (limit, total_to_process, target_game, processed_total,
         batch_size, iteration_pause, output_file, output_dir,
         dry_run, force, skip_search, no_restart, process_not_found, skip_not_found) = params

        # Логируем начало работы ТОЛЬКО если это первый запуск (processed_total == 0)
        if processed_total == 0:
            self.log_timeline("START")

        # Вывод параметров запуска
        self._print_startup_info(limit, total_to_process, processed_total,
                                 batch_size, iteration_pause, options)

        # Основной цикл обработки
        processed_total, games_per_second = self._main_processing_loop(
            target_game, limit, batch_size, iteration_pause,
            skip_search, output_file, dry_run,
            force, no_restart, processed_total, options,
            process_not_found, skip_not_found
        )

        # Финальная статистика
        self._print_final_stats()

    def _find_specific_game(self, game_name: str, pc: Platform) -> Optional[Tuple]:
        """Поиск конкретной игры по названию."""
        self.stdout.write(f'🔍 Поиск игры: "{game_name}"')
        self.log_debug(f"Начат поиск игры по названию: {game_name}")

        self.stdout.write(f'  Шаг 1: Поиск точного совпадения...')
        exact_games = Game.objects.filter(
            Q(name__iexact=game_name) &
            Q(platforms=pc)
        ).order_by('-rating_count')

        exact_count = exact_games.count()
        self.stdout.write(f'  Найдено точных совпадений: {exact_count}')

        if exact_count > 0:
            target_game = exact_games.first()
            self.stdout.write(self.style.SUCCESS(
                f'  ✅ Точное совпадение: {target_game.name} (ID: {target_game.id})'
            ))
        else:
            self.stdout.write(f'  Шаг 2: Поиск частичных совпадений...')

            all_matches = Game.objects.filter(
                Q(name__icontains=game_name) &
                Q(platforms=pc)
            ).order_by('-rating_count')

            total_matches = all_matches.count()
            self.stdout.write(f'  Всего найдено частичных совпадений: {total_matches}')

            if total_matches > 0:
                self.stdout.write(f'  Топ-5 по рейтингу:')
                for i, g in enumerate(all_matches[:5], 1):
                    self.stdout.write(f'    {i}. {g.name} (ID: {g.id}, рейтинг: {g.rating})')

                target_game = all_matches.first()
                self.stdout.write(self.style.SUCCESS(
                    f'  ✅ Выбрана: {target_game.name} (ID: {target_game.id})'
                ))
            else:
                self.stdout.write(self.style.ERROR(f'❌ Игра "{game_name}" не найдена'))

                if self.debug:
                    similar = Game.objects.filter(platforms=pc)[:10]
                    self.stdout.write(self.style.WARNING('📋 Доступные игры (первые 10):'))
                    for g in similar:
                        self.stdout.write(f'  - {g.name}')
                return None

        return target_game, 1, 1, True

    def _print_startup_info(self, limit: int, total_to_process: int,
                            processed_total: int, batch_size: int,
                            iteration_pause: int, options: Dict) -> None:
        """Вывод информации о запуске."""
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('STEAM DESCRIPTIONS FETCHER'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        self.stdout.write(f'\n📊 Параметры запуска:')
        self.stdout.write(f'  Всего для обработки: {total_to_process} игр')
        self.stdout.write(f'  Лимит за запуск: {limit}')
        self.stdout.write(f'  Текущий offset: {self.current_offset}')
        self.stdout.write(f'  Уже обработано: {processed_total} игр')
        self.stdout.write(f'  Размер итерации: {batch_size}')
        self.stdout.write(f'  Пауза между итерациями: {iteration_pause}с')
        self.stdout.write(f'  Воркеров: {self.workers}')
        self.stdout.write(f'  Задержка: {self.delay}с')
        self.stdout.write(f'  Таймаут: {self.timeout}с')
        self.stdout.write(f'  Dry run: {options["dry_run"]}')
        self.stdout.write(f'  Force: {options["force"]}')
        self.stdout.write(f'  Skip search: {options["skip_search"]}')
        self.stdout.write(f'  Process only not found: {options["process_not_found"]}')
        self.stdout.write(f'  Skip not found: {options["skip_not_found"]}')
        self.stdout.write(f'  Not found file: {self.not_found_file}')
        self.stdout.write(f'  Log file: {self.log_file}')
        self.stdout.write(f'  Loaded not found games: {len(self.not_found_games)}')

        self.stdout.write(self.style.WARNING(f'\n🚫 Настройки защиты от ошибок:'))
        self.stdout.write(f'  Макс. ошибок подряд: {options["max_consecutive_failures"]}')
        self.stdout.write(f'  Базовая пауза: {options["base_wait"]}с')
        self.stdout.write(f'  Макс. пауза: {options["max_wait"]}с')
        self.stdout.write(f'  Порог ошибок в батче: {options["batch_failure_threshold"] * 100}%')

        self.stdout.write('=' * 60)
        self.stdout.write(self.style.WARNING('⚠️  Нажмите Ctrl+C для завершения'))
        self.stdout.write('=' * 60)

    def _main_processing_loop(self, target_game: Optional[Game], limit: int,
                              batch_size: int, iteration_pause: int,
                              skip_search: bool, output_file: str, dry_run: bool,
                              force: bool, no_restart: bool, processed_total: int,
                              options: Dict,
                              process_not_found: bool, skip_not_found: bool) -> Tuple[int, float]:
        """Основной цикл обработки игр."""
        iteration = 0
        games_per_second = 0
        loop_start_time = time.time()

        # Сохраняем processed_total как атрибут класса для доступа из других методов
        self.processed_total = processed_total

        while self.current_offset < limit and not self.interrupted:
            iteration += 1
            iteration_start_time = time.time()

            self.stdout.write(self.style.SUCCESS(f'\n🔄 ИТЕРАЦИЯ {iteration} (offset {self.current_offset})'))

            # Проверяем rate limiting
            if self.rate_limiter and self.rate_limiter.should_backoff():
                if not self.check_rate_limit():
                    break

            # Получаем игры для обработки
            games_to_process = self._get_games_to_process(target_game, force,
                                                          process_not_found, skip_not_found)
            if not games_to_process:
                self.stdout.write(self.style.WARNING('⚠️ Нет игр для обработки в этой итерации'))
                break

            # Обрабатываем батч
            batch_stats, games_to_update = self._process_games_batch(
                games_to_process, skip_search, self.timeout, self.delay,
                output_file, dry_run, self.current_offset == 0 and iteration == 1 and self.processed_total == 0
            )

            # Обновляем статистику
            self.processed_total += len(games_to_process)
            games_per_second = self._update_speed_eta(iteration_start_time, len(games_to_process), games_per_second)

            # Обновляем БД
            self._update_database(games_to_update, dry_run)

            # Сохраняем буфер не найденных игр
            if self.not_found_buffer:
                self.save_not_found_buffer()

            # Выводим прогресс и статистику
            self._print_progress_and_stats(
                self.processed_total, limit, loop_start_time, games_per_second,
                batch_stats, iteration
            )

            # Подготовка к следующей итерации
            self.current_offset += batch_size

            # Перезапуск если нужно
            if self._should_restart(no_restart, target_game, self.current_offset, limit):
                self._restart_process(
                    limit, self.current_offset, batch_size, iteration_pause,
                    output_file, str(self.output_dir), dry_run, force, skip_search,
                    self.processed_total, options, process_not_found, skip_not_found
                )

        return self.processed_total, games_per_second

    def _get_games_to_process(self, target_game: Optional[Game], force: bool,
                              process_not_found: bool, skip_not_found: bool) -> List[Game]:
        """Получение списка игр для обработки."""
        if target_game:
            return [target_game]

        if process_not_found is True and self.not_found_games:
            games = list(Game.objects.filter(id__in=self.not_found_games).order_by('id')
                         [self.current_offset:self.current_offset + self.batch_size])
            self.stdout.write(self.style.WARNING(f'📊 Обработка не найденных игр: {len(games)}'))
            return games

        return self.get_games_batch(self.current_offset, self.batch_size, force,
                                    skip_not_found, self.not_found_games)

    def _process_games_batch(self, games: List[Game], skip_search: bool,
                             timeout: float, delay: float, output_file: str,
                             dry_run: bool, is_first: bool) -> Tuple[Dict, List[Game]]:
        """Обработка батча игр."""
        batch_stats = {
            'success': 0,
            'not_found': 0,
            'no_description': 0,
            'error': 0
        }

        games_to_update = self.process_batch(
            games, skip_search, timeout, delay,
            output_file, dry_run, batch_stats, is_first
        )

        return batch_stats, games_to_update

    def _update_speed_eta(self, iteration_start_time: float,
                          games_processed: int, current_speed: float) -> float:
        """Обновление скорости обработки."""
        iteration_time = time.time() - iteration_start_time
        current_games_per_second = games_processed / max(iteration_time, 0.1)

        if current_speed == 0:
            return current_games_per_second
        return current_speed * 0.7 + current_games_per_second * 0.3

    def _update_database(self, games_to_update: List[Game], dry_run: bool) -> None:
        """Обновление базы данных."""
        if games_to_update and not dry_run:
            try:
                with transaction.atomic():
                    Game.objects.bulk_update(games_to_update, ['rawg_description'])
                self.stdout.write(self.style.SUCCESS(f'✅ Обновлено {len(games_to_update)} игр в БД'))
            except Exception as e:
                self.log_debug("Ошибка при обновлении БД", error=e)

    def _print_progress_and_stats(self, processed_total: int, limit: int,
                                  start_time: float, games_per_second: float,
                                  batch_stats: Dict, iteration: int) -> None:
        """Вывод прогресса и детальной статистики."""
        progress = (processed_total / limit * 100) if limit > 0 else 0
        elapsed = time.time() - start_time

        elapsed_str = self._format_time(elapsed)

        remaining = limit - processed_total
        eta_str = self._format_eta(remaining, games_per_second)

        bar_length = 40
        filled = int(bar_length * progress / 100)
        bar = '█' * filled + '░' * (bar_length - filled)

        self.stdout.write(self.style.SUCCESS(f'\n📊 ПРОГРЕСС: [{bar}] {progress:.1f}%'))
        self.stdout.write(self.style.SUCCESS(f'📊 Обработано: {processed_total}/{limit} игр'))
        self.stdout.write(self.style.SUCCESS(
            f'⏱️ Прошло: {elapsed_str} | ⏳ Осталось: {eta_str} | ⚡ Скорость: {games_per_second:.1f} игр/с'))

        self._print_detailed_stats()

        self._print_iteration_stats(batch_stats, iteration)

    def _format_time(self, seconds: float) -> str:
        """Форматирование времени в читаемый вид."""
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        secs = int(seconds % 60)

        if hours > 0:
            return f"{hours}ч {minutes}м {secs}с"
        elif minutes > 0:
            return f"{minutes}м {secs}с"
        else:
            return f"{secs}с"

    def _format_eta(self, remaining: int, speed: float) -> str:
        """Форматирование ETA."""
        if remaining <= 0 or speed <= 0:
            return "завершение"

        eta_seconds = remaining / speed
        return self._format_time(eta_seconds)

    def _print_detailed_stats(self) -> None:
        """Вывод детальной статистики за все время."""
        total = (self.total_stats['success'] +
                 self.total_stats['not_found'] +
                 self.total_stats['no_description'] +
                 self.total_stats['error'])

        if total == 0:
            return

        success_pct = (self.total_stats['success'] / total * 100)
        not_found_pct = (self.total_stats['not_found'] / total * 100)
        no_desc_pct = (self.total_stats['no_description'] / total * 100)
        error_pct = (self.total_stats['error'] / total * 100)

        self.stdout.write(self.style.SUCCESS(f'\n📊 ДЕТАЛЬНАЯ СТАТИСТИКА ЗА ВСЕ ВРЕМЯ:'))
        self.stdout.write(f'  ✅ Успешно обновлено: {self.total_stats["success"]} ({success_pct:.1f}%)')
        self.stdout.write(f'  🔍 Не найдено в Steam: {self.total_stats["not_found"]} ({not_found_pct:.1f}%)')
        self.stdout.write(f'  📄 Нет описания: {self.total_stats["no_description"]} ({no_desc_pct:.1f}%)')
        self.stdout.write(f'  💥 Ошибок запросов: {self.total_stats["error"]} ({error_pct:.1f}%)')

        error_details = []
        if self.total_stats['error_403'] > 0:
            error_details.append(f'403: {self.total_stats["error_403"]}')
        if self.total_stats['error_timeout'] > 0:
            error_details.append(f'Таймаут: {self.total_stats["error_timeout"]}')
        if self.total_stats['error_other'] > 0:
            error_details.append(f'Другие: {self.total_stats["error_other"]}')

        if error_details:
            self.stdout.write(self.style.WARNING(f'     └─ {", ".join(error_details)}'))

        if self.not_found_file_path and self.not_found_file_path.exists():
            size = self.not_found_file_path.stat().st_size
            lines = 0
            try:
                with open(self.not_found_file_path, 'r', encoding='utf-8') as f:
                    lines = sum(1 for line in f if line.strip() and not line.startswith('=') and not line.startswith(
                        'STEAM') and not line.startswith('Generated') and not line.startswith('Offset'))
            except:
                pass
            self.stdout.write(
                self.style.WARNING(f'  📁 Не найденные игры: {self.not_found_file_path} ({lines} игр, {size} байт)'))

    def _print_iteration_stats(self, batch_stats: Dict, iteration: int) -> None:
        """Вывод статистики текущей итерации."""
        total = sum(batch_stats.values())
        success_rate = (batch_stats['success'] / total * 100) if total > 0 else 0

        self.stdout.write(self.style.SUCCESS(f'\n📊 ИТОГ ИТЕРАЦИИ {iteration}:'))
        self.stdout.write(f'  ✓ Успешно: {batch_stats["success"]} ({success_rate:.1f}%)')
        self.stdout.write(f'  🔍 Не найдено в Steam: {batch_stats["not_found"]}')
        self.stdout.write(f'  📄 Нет описания: {batch_stats["no_description"]}')

        if batch_stats['error'] > 0:
            self.stdout.write(self.style.ERROR(f'  💥 Ошибок запросов: {batch_stats["error"]}'))

    def _should_restart(self, no_restart: bool, target_game: Optional[Game],
                        current_offset: int, limit: int) -> bool:
        """Проверка необходимости перезапуска."""
        return (not no_restart and not target_game and
                current_offset < limit and not self.interrupted)

    def _restart_process(self, limit: int, next_offset: int, batch_size: int,
                         iteration_pause: int, output_file: str, output_dir: str,
                         dry_run: bool, force: bool, skip_search: bool,
                         processed_total: int, options: Dict,
                         process_not_found: bool, skip_not_found: bool) -> None:
        """Перезапуск процесса для следующей итерации с передачей накопленной статистики."""
        self.stdout.write(self.style.WARNING(f'\n🔄 Следующая итерация через {iteration_pause}с...'))

        for i in range(iteration_pause, 0, -1):
            if i > 1:
                time.sleep(1)
                self.stdout.write(self.style.WARNING(f'   Осталось {i}с...'))
            else:
                time.sleep(1)
                break

        cmd = [
            sys.executable, "manage.py", "fetch_steam_descriptions",
            f"--limit={limit}",
            f"--offset={next_offset}",
            f"--batch-size={batch_size}",
            f"--iteration-pause={iteration_pause}",
            f"--workers={self.workers}",
            f"--delay={self.delay}",
            f"--timeout={self.timeout}",
            f"--output-file={output_file}",
            f"--not-found-file={self.not_found_file}",
            f"--output-dir={output_dir}",
            f"--log-file={self.log_file}",
            f"--max-consecutive-failures={options['max_consecutive_failures']}",
            f"--base-wait={options['base_wait']}",
            f"--max-wait={options['max_wait']}",
            f"--batch-failure-threshold={options['batch_failure_threshold']}",
            f"--processed={processed_total}",
        ]

        # Добавляем накопленную статистику как аргументы
        if hasattr(self, 'total_stats'):
            # Сохраняем статистику во временный файл для передачи между процессами
            stats_file = self.output_dir / 'steam_fetcher_stats.json'
            try:
                import json
                with open(stats_file, 'w', encoding='utf-8') as f:
                    json.dump(self.total_stats, f, ensure_ascii=False, indent=2)
                cmd.append(f"--stats-file={stats_file}")
                self.stdout.write(self.style.SUCCESS(f'📊 Сохранена статистика в {stats_file}'))
            except Exception as e:
                self.log_debug("Ошибка при сохранении статистики", error=e)

        if dry_run:
            cmd.append("--dry-run")
        if force:
            cmd.append("--force")
        if skip_search:
            cmd.append("--skip-search")
        if self.debug:
            cmd.append("--debug")
        if self.verbose:
            cmd.append("--verbose")
        if process_not_found:
            cmd.append("--process-not-found")
        if skip_not_found:
            cmd.append("--skip-not-found")

        self.stdout.write(self.style.SUCCESS(f'🚀 Запуск: {" ".join(cmd)}'))

        try:
            # Сохраняем буферы перед выходом
            if self.descriptions_buffer:
                self.save_buffer(output_file, is_first=False)
            if self.not_found_buffer:
                self.save_not_found_buffer(is_first=False)

            self.stdout.write(self.style.WARNING('🔄 Запуск нового процесса...'))
            subprocess.Popen(cmd, shell=False)
            self.stdout.write(self.style.SUCCESS('✅ Новый процесс запущен'))
            self.stdout.write(self.style.SUCCESS('✅ Текущий процесс завершен'))
            sys.exit(0)

        except Exception as e:
            self.log_debug("Ошибка при перезапуске команды", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка при перезапуске: {e}'))

    def _print_final_stats(self) -> None:
        """Вывод финальной статистики."""
        end_time = datetime.now()
        elapsed_time = (end_time - self.start_time).total_seconds() if self.start_time else 0

        total_processed = (self.total_stats['success'] +
                           self.total_stats['not_found'] +
                           self.total_stats['no_description'] +
                           self.total_stats['error'])

        # Логируем завершение работы
        self.log_timeline("END", total_processed)

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 60))
        self.stdout.write(self.style.SUCCESS('📊 ИТОГОВАЯ СТАТИСТИКА'))
        self.stdout.write('=' * 60)

        self.stdout.write(f'  ✅ Успешно обновлено: {self.total_stats["success"]}')
        self.stdout.write(f'  🔍 Не найдено в Steam: {self.total_stats["not_found"]}')
        self.stdout.write(f'  📄 Нет описания: {self.total_stats["no_description"]}')
        self.stdout.write(f'  💥 Ошибок запросов: {self.total_stats["error"]}')
        self.stdout.write(
            f'  └─ 403: {self.total_stats["error_403"]}, Таймаут: {self.total_stats["error_timeout"]}, Другие: {self.total_stats["error_other"]}')
        self.stdout.write(f'  ⏸️ Пауз из-за ошибок: {self.total_stats["backoff_pauses"]}')
        self.stdout.write(f'  🔄 Выполнено итераций: {self.total_stats["iterations"]}')
        self.stdout.write(f'  ⏱️ Общее время работы: {self._format_time(elapsed_time)}')
        self.stdout.write(f'  📊 Всего обработано: {total_processed} игр')

        # Информация о файлах
        self._print_file_info()

        self.stdout.write('=' * 60)

    def _print_file_info(self) -> None:
        """Вывод информации о выходных файлах."""
        if self.full_output_path and self.full_output_path.exists():
            size = self.full_output_path.stat().st_size
            self.stdout.write(self.style.SUCCESS(f'\n📁 Описания: {self.full_output_path}'))
            self.stdout.write(self.style.SUCCESS(f'📊 Размер: {size:,} байт ({size / 1024:.1f} КБ)'))
        else:
            expected_path = self.output_dir / self.output_file
            if expected_path.exists():
                size = expected_path.stat().st_size
                self.stdout.write(self.style.SUCCESS(f'\n📁 Описания: {expected_path}'))
                self.stdout.write(self.style.SUCCESS(f'📊 Размер: {size:,} байт ({size / 1024:.1f} КБ)'))

        if self.not_found_file_path and self.not_found_file_path.exists():
            size = self.not_found_file_path.stat().st_size
            lines = 0
            try:
                with open(self.not_found_file_path, 'r', encoding='utf-8') as f:
                    lines = sum(1 for line in f if line.strip() and not line.startswith('=') and not line.startswith(
                        'STEAM') and not line.startswith('Generated') and not line.startswith('Offset'))
            except:
                pass
            self.stdout.write(self.style.WARNING(f'📁 Не найдено: {self.not_found_file_path}'))
            self.stdout.write(self.style.WARNING(f'📊 Размер: {size:,} байт ({size / 1024:.1f} КБ), игр: {lines}'))

        if self.log_file_path and self.log_file_path.exists():
            size = self.log_file_path.stat().st_size
            self.stdout.write(self.style.WARNING(f'📋 Лог: {self.log_file_path} ({size} байт)'))