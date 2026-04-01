# games/management/commands/fetch_steam_descriptions.py

"""
Django management command для получения описаний игр из Steam API.
Поддерживает пакетную обработку с сохранением прогресса и оценкой времени.
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
import csv
import datetime as dt
import statistics
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
import json

logger = logging.getLogger(__name__)


class SteamRateLimiter:
    """Класс для управления rate limiting и ошибками с адаптивной задержкой."""

    def __init__(self, max_consecutive_failures=10, base_wait_time=60, max_wait_time=300):
        self.consecutive_failures = 0
        self.consecutive_403 = 0
        self.consecutive_429 = 0
        self.max_consecutive_failures = max_consecutive_failures
        self.base_wait_time = base_wait_time
        self.max_wait_time = max_wait_time
        self.last_failure_time = None
        self.total_failures = 0
        self.total_403 = 0
        self.total_429 = 0
        self.wait_history = []
        self.lock = Lock()
        self.in_backoff = False
        self.backoff_until = None
        self.pause_start_time = None
        self.last_429_time = None
        self.successful_requests = 0
        self.last_success_time = None

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
                self.successful_requests = 0

                if error_type == "403":
                    self.consecutive_403 += 1
                    self.total_403 += 1
                elif error_type == "429":
                    self.consecutive_429 += 1
                    self.total_429 += 1
                    self.last_429_time = datetime.now()

                    if self.consecutive_429 >= 3:
                        wait_time = self.base_wait_time
                        self.in_backoff = True
                        self.backoff_until = datetime.now() + timedelta(seconds=wait_time)
                        self.pause_start_time = datetime.now()

                        self.wait_history.append({
                            'time': datetime.now(),
                            'wait': wait_time,
                            'consecutive': self.consecutive_failures,
                            'consecutive_429': self.consecutive_429,
                            'reason': f"{self.consecutive_429} ошибок 429"
                        })

                        return wait_time

                if self.consecutive_failures >= self.max_consecutive_failures:
                    return self._calculate_wait_time()

            finally:
                self.lock.release()
        except Exception:
            pass

        return 0

    def record_success(self):
        """Сброс счетчика при успешном запросе с адаптивным уменьшением задержки."""
        try:
            acquired = self.lock.acquire(timeout=2)
            if not acquired:
                return

            try:
                self.successful_requests += 1
                self.last_success_time = datetime.now()

                if self.consecutive_failures > 0:
                    self.consecutive_failures = 0
                    self.consecutive_403 = 0
                    self.in_backoff = False
                    self.backoff_until = None
                    self.pause_start_time = None

                if self.consecutive_429 > 0:
                    self.consecutive_429 = max(0, self.consecutive_429 - 1)

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
            'consecutive_429': self.consecutive_429,
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
                    'pause_start': self.pause_start_time.isoformat() if self.pause_start_time else None,
                    'successful_requests': self.successful_requests,
                    'last_success': self.last_success_time.isoformat() if self.last_success_time else None
                }
            finally:
                self.lock.release()
        except Exception:
            return {
                'consecutive_failures': 0,
                'in_backoff': False,
                'pause_start': None
            }


def get_request(url: str, parameters: Dict = None, timeout: float = 10, retries: int = 3) -> Optional[Dict]:
    """
    Универсальная функция для выполнения GET запросов с обработкой ошибок.
    При ошибке SSL или отсутствии ответа выполняет повторные попытки.
    Использует глобальную сессию для пула соединений.
    """
    global _session
    if '_session' not in globals():
        import requests
        _session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=50,
            pool_maxsize=50,
            max_retries=0
        )
        _session.mount('http://', adapter)
        _session.mount('https://', adapter)
        _session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        })

    for attempt in range(retries):
        try:
            response = _session.get(url=url, params=parameters, timeout=timeout)

            if response:
                if response.status_code == 200:
                    try:
                        return response.json()
                    except Exception as e:
                        print(f'\n⚠️ Ошибка парсинга JSON: {e}')
                        if attempt < retries - 1:
                            time.sleep(2 ** attempt)
                        continue
                elif response.status_code == 429:
                    wait_time = 30 * (attempt + 1) + random.uniform(0, 5)
                    print(f'\n🚫 Rate limit (429), ждем {wait_time:.1f}с...')
                    time.sleep(wait_time)
                    continue
                elif response.status_code == 403:
                    wait_time = 60 * (attempt + 1)
                    print(f'\n🚫 Forbidden (403), ждем {wait_time:.1f}с...')
                    time.sleep(wait_time)
                    continue
                else:
                    print(f'\n⚠️ HTTP {response.status_code} для {url}')
                    if attempt < retries - 1:
                        wait_time = 2 ** attempt + random.uniform(0, 1)
                        time.sleep(wait_time)
                    continue
            else:
                print(f'\n⚠️ Нет ответа от {url}, попытка {attempt + 1}/{retries}')
                time.sleep(5)

        except requests.exceptions.SSLError as e:
            print(f'\n🔒 SSL ошибка: {e}')
            for i in range(5, 0, -1):
                print(f'\r   Ожидание... ({i})', end='')
                time.sleep(1)
            print('\r   Повторная попытка.      ')

        except requests.exceptions.Timeout:
            print(f'\n⏰ Таймаут для {url}, попытка {attempt + 1}/{retries}')
            time.sleep(2 ** attempt + random.uniform(0, 1))

        except requests.exceptions.ConnectionError:
            print(f'\n🔌 Ошибка соединения, попытка {attempt + 1}/{retries}')
            time.sleep(2 ** attempt)

        except Exception as e:
            print(f'\n💥 Ошибка запроса: {e}')
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
            else:
                return None

    return None


class Command(BaseCommand):
    """Steam descriptions fetcher с пакетной обработкой и сохранением прогресса."""

    # Константа лимита запросов в минуту
    REQUESTS_PER_MINUTE = 50

    help = 'Получение описаний из Steam с пакетной обработкой и сохранением прогресса'

    def __init__(self, *args, **kwargs):
        """Инициализация команды."""
        super().__init__(*args, **kwargs)
        self.stats_lock = Lock()
        self.output_lock = Lock()
        self.descriptions_buffer = []
        self.not_found_buffer = []
        self.found_buffer = []
        self.buffer_size = 50
        self.session = self._create_session()
        self.pc_platform = None
        self.debug = False
        self.verbose = False
        self.interrupted = False
        self.no_description_file = 'steam_no_description.txt'
        self.found_file = 'steam_found.txt'
        self.not_found_file = 'steam_not_found.txt'
        self.log_file = 'steam_fetcher_timeline.log'
        self.cache_file = 'steam_cache.json'
        self.stats_file = 'steam_stats.json'
        self.progress_file = 'steam_progress.txt'
        self.no_description_file_path = None
        self.found_file_path = None
        self.not_found_file_path = None
        self.log_file_path = None
        self.cache_file_path = None
        self.stats_file_path = None
        self.progress_file_path = None
        self.no_description_buffer = []
        self.no_description_games = set()
        self.found_games = set()
        self.app_id_dict = {}
        self.cache_data = {}
        self.total_stats = {
            'success': 0,
            'not_found': 0,
            'no_description': 0,
            'error': 0,
            'error_403': 0,
            'error_429': 0,
            'error_timeout': 0,
            'error_other': 0,
            'iterations': 0,
            'backoff_pauses': 0,
            'skipped_not_found': 0,
            'skipped_no_description': 0
        }
        self.output_dir = None
        self.full_output_path = None
        self.not_found_games = set()
        self.current_offset = 0
        self.error_log = []
        self.rate_limiter = None
        self.batch_failure_threshold = 0.3
        self.batch_size = 30
        self.workers = 3
        self.delay = 0.5
        self.timeout = 10
        self.output_file = 'steam_descriptions_all.txt'
        self.start_time = None
        self.processed_before_pause = 0
        self._pause_active = False
        self.processed_total = 0
        self.limit = 0
        self.only_found = False
        self.force = False
        self.dry_run = False
        self.create_backup = True
        self.batch_times = []
        self.loop_start_time = None

        # Инициализация ограничителя запросов
        self._init_rate_limiter()

    def _init_rate_limiter(self):
        """Инициализация потокобезопасного ограничителя запросов."""
        import threading
        self._request_counter = 0
        self._request_counter_lock = threading.Lock()
        self._minute_start = int(time.time() / 60)
        self._max_requests_per_minute = self.REQUESTS_PER_MINUTE
        self._pause_active = False
        self._pause_shown = False

    def add_arguments(self, parser: CommandParser) -> None:
        """Добавление аргументов команды."""
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Максимальное количество игр для обработки (по умолчанию: все игры)'
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
            default=40,
            help='Размер пакета для обработки (по умолчанию: 40)'
        )

        parser.add_argument(
            '--iteration-pause',
            type=int,
            default=0,
            help='Пауза между пакетами в секундах (по умолчанию: 0)'
        )

        parser.add_argument(
            '--workers',
            type=int,
            default=8,
            help='Количество параллельных воркеров (по умолчанию: 8)'
        )

        parser.add_argument(
            '--delay',
            type=float,
            default=0.1,
            help='Задержка между запросами в секундах (по умолчанию: 0.1)'
        )

        parser.add_argument(
            '--timeout',
            type=float,
            default=10.0,
            help='Таймаут запроса в секундах (по умолчанию: 10.0)'
        )

        parser.add_argument(
            '--output-file',
            type=str,
            default='steam_descriptions_all.txt',
            help='Выходной TXT файл для описаний'
        )

        parser.add_argument(
            '--output-dir',
            type=str,
            default='steam_data',
            help='Директория для сохранения файлов (по умолчанию: steam_data)'
        )

        parser.add_argument(
            '--max-consecutive-failures',
            type=int,
            default=15,
            help='Максимальное количество ошибок подряд перед паузой (по умолчанию: 15)'
        )

        parser.add_argument(
            '--base-wait',
            type=int,
            default=180,
            help='Базовая пауза при ошибках в секундах (по умолчанию: 180)'
        )

        parser.add_argument(
            '--max-wait',
            type=int,
            default=300,
            help='Максимальная пауза при ошибках в секундах (по умолчанию: 300)'
        )

        parser.add_argument(
            '--batch-failure-threshold',
            type=float,
            default=0.8,
            help='Порог неудач в батче для паузы (0.0-1.0, по умолчанию: 0.8)'
        )

        parser.add_argument(
            '--processed',
            type=int,
            default=0,
            help='Количество уже обработанных игр (для продолжения)'
        )

        parser.add_argument(
            '--clear-descriptions',
            action='store_true',
            help='Очистить все существующие rawg_description перед началом работы'
        )

        parser.add_argument(
            '--clear-logs',
            action='store_true',
            help='Удалить всю папку с логами и файлами перед началом работы'
        )

        parser.add_argument(
            '--only-found',
            action='store_true',
            help='Обрабатывать только игры из файла найденных (steam_found.txt)'
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
            '--process-no-description',
            action='store_true',
            help='Обрабатывать только игры из файла без описания'
        )

        parser.add_argument(
            '--skip-no-description',
            action='store_true',
            default=True,
            help='Пропускать игры из файла без описания (по умолчанию: True)'
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
            help='Показывать детальные сообщения об ошибках'
        )

        parser.add_argument(
            '--verbose',
            action='store_true',
            default=True,
            help='Показывать подробный вывод включая API запросы'
        )

        parser.add_argument(
            '--no-restart',
            action='store_true',
            help='Не перезапускать команду для следующих оффсетов'
        )

        parser.add_argument(
            '--no-backup',
            action='store_true',
            help='Не создавать резервные копии файлов'
        )

        parser.add_argument(
            '--reset-progress',
            action='store_true',
            help='Сбросить индекс прогресса (начать заново)'
        )

        parser.add_argument(
            '--stat-success',
            type=int,
            default=0,
            help='Внутренний аргумент: количество успешных операций из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-not-found',
            type=int,
            default=0,
            help='Внутренний аргумент: количество не найденных игр из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-no-description',
            type=int,
            default=0,
            help='Внутренний аргумент: количество игр без описания из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-error',
            type=int,
            default=0,
            help='Внутренний аргумент: количество ошибок из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-error-403',
            type=int,
            default=0,
            help='Внутренний аргумент: количество ошибок 403 из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-error-429',
            type=int,
            default=0,
            help='Внутренний аргумент: количество ошибок 429 из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-error-timeout',
            type=int,
            default=0,
            help='Внутренний аргумент: количество таймаутов из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-error-other',
            type=int,
            default=0,
            help='Внутренний аргумент: количество прочих ошибок из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-backoff-pauses',
            type=int,
            default=0,
            help='Внутренний аргумент: количество пауз из предыдущего процесса'
        )
        parser.add_argument(
            '--stat-iterations',
            type=int,
            default=0,
            help='Внутренний аргумент: количество итераций из предыдущего процесса'
        )

    def _display_progress_during_pause(self, remaining_seconds: float):
        """
        Отображает прогресс обработки во время паузы.
        Использует сохраненные данные о прогрессе.
        """
        if not hasattr(self, '_last_progress_display'):
            self._last_progress_display = {}

        total_limit = getattr(self, 'limit', 0)
        processed_total = getattr(self, 'processed_total', 0)

        if total_limit <= 0:
            return

        # Расчет прогресса
        progress = (processed_total / total_limit * 100) if total_limit > 0 else 0

        # Создаем прогресс-бар
        bar_length = 40
        filled = int(bar_length * progress / 100)
        bar = '█' * filled + '░' * (bar_length - filled)

        # Расчет ETA
        if hasattr(self, '_load_times') and self._load_times and len(self._load_times) > 0:
            avg_time = sum(self._load_times[-100:]) / min(len(self._load_times), 100)
            remaining_games = total_limit - processed_total
            eta_seconds = remaining_games * avg_time + remaining_seconds
        else:
            eta_seconds = 0

        # Форматирование времени
        if eta_seconds > 0:
            hours = int(eta_seconds // 3600)
            minutes = int((eta_seconds % 3600) // 60)
            secs = int(eta_seconds % 60)
            if hours > 0:
                eta_str = f"{hours}ч {minutes}м {secs}с"
            elif minutes > 0:
                eta_str = f"{minutes}м {secs}с"
            else:
                eta_str = f"{secs}с"
        else:
            eta_str = "расчет..."

        # Расчет скорости
        if hasattr(self, '_load_times') and self._load_times and len(self._load_times) > 0:
            avg_time = sum(self._load_times[-100:]) / min(len(self._load_times), 100)
            games_per_second = 1 / avg_time if avg_time > 0 else 0
        else:
            games_per_second = 0

        # Вывод прогресса
        self.stdout.write(
            self.style.NOTICE(
                f'\r📊 ПРОГРЕСС: [{bar}] {progress:.1f}%\n'
                f'📊 Обработано: {processed_total}/{total_limit} игр\n'
                f'⏱️ Пауза: {remaining_seconds:.1f}с | ⏳ Осталось: {eta_str} | ⚡ Скорость: {games_per_second:.1f} игр/с   '
            ),
            ending=''
        )
        self.stdout.flush()

    def _wait_for_rate_limit(self):
        """
        Ожидает, соблюдая лимит REQUESTS_PER_MINUTE запросов в минуту.
        Делает ровно REQUESTS_PER_MINUTE запросов максимально быстро, затем пауза до конца минуты.
        Использует блокировку для корректного подсчета параллельных запросов.
        """
        max_requests = self._max_requests_per_minute

        with self._request_counter_lock:
            current_minute = int(time.time() / 60)

            # Если минута изменилась, сбрасываем счетчик
            if current_minute != self._minute_start:
                self._request_counter = 0
                self._minute_start = current_minute
                self._pause_active = False
                self._pause_shown = False

            # Если пауза уже активна, ждем
            if self._pause_active:
                end_of_minute = (self._minute_start + 1) * 60
                wait_seconds = end_of_minute - time.time()

                if wait_seconds > 0:
                    self._request_counter_lock.release()

                    wait_start = time.time()
                    remaining = wait_seconds
                    last_display = 0

                    try:
                        while remaining > 0:
                            if self.interrupted:
                                raise KeyboardInterrupt()

                            # Обновляем прогресс каждые 0.5 секунды
                            if time.time() - last_display >= 0.5:
                                # Выводим информацию о паузе с обратным отсчетом
                                self.stdout.write(
                                    self.style.WARNING(
                                        f'\r  🚫 Пауза {remaining:.1f}с | Лимит {max_requests} запросов/мин   '
                                    ),
                                    ending=''
                                )
                                self.stdout.flush()
                                last_display = time.time()

                            time.sleep(0.1)
                            remaining = wait_seconds - (time.time() - wait_start)

                        self.stdout.write('', ending='')

                    finally:
                        self._request_counter_lock.acquire()

                    # Сбрасываем для новой минуты
                    self._request_counter = 0
                    self._minute_start = int(time.time() / 60)
                    self._pause_active = False
                    self._pause_shown = False

            # Если достигли лимита, включаем паузу
            if self._request_counter >= max_requests:
                self._pause_active = True
                end_of_minute = (self._minute_start + 1) * 60
                wait_seconds = end_of_minute - time.time()

                if wait_seconds > 0:
                    if not self._pause_shown:
                        self._pause_shown = True
                        self.stdout.write('', ending='')
                        self.stdout.write(
                            self.style.WARNING(
                                f'\n  🚫 Достигнут лимит {max_requests} запросов/мин, пауза {wait_seconds:.1f}с...'
                            )
                        )

                    self._request_counter_lock.release()

                    wait_start = time.time()
                    remaining = wait_seconds
                    last_display = 0

                    try:
                        while remaining > 0:
                            if self.interrupted:
                                raise KeyboardInterrupt()

                            # Обновляем прогресс каждые 0.5 секунды
                            if time.time() - last_display >= 0.5:
                                self.stdout.write(
                                    self.style.WARNING(
                                        f'\r  🚫 Пауза {remaining:.1f}с | Лимит {max_requests} запросов/мин   '
                                    ),
                                    ending=''
                                )
                                self.stdout.flush()
                                last_display = time.time()

                            time.sleep(0.1)
                            remaining = wait_seconds - (time.time() - wait_start)

                        self.stdout.write('', ending='')

                    finally:
                        self._request_counter_lock.acquire()

                    # Сбрасываем для новой минуты
                    self._request_counter = 0
                    self._minute_start = int(time.time() / 60)
                    self._pause_active = False
                    self._pause_shown = False

            # Увеличиваем счетчик
            self._request_counter += 1

    def _print_load_time_statistics(self) -> None:
        """
        Выводит статистику времени загрузки игр.
        """
        if not hasattr(self, '_load_times') or not self._load_times:
            return

        load_times = self._load_times

        self.stdout.write(self.style.SUCCESS(f'\n⏱️ СТАТИСТИКА ВРЕМЕНИ ЗАГРУЗКИ:'))
        self.stdout.write(f'  📊 Всего загружено: {len(load_times)} игр')
        self.stdout.write(f'  ⚡ Среднее время: {sum(load_times) / len(load_times):.2f}с')
        self.stdout.write(f'  🚀 Минимальное: {min(load_times):.2f}с')
        self.stdout.write(f'  🐢 Максимальное: {max(load_times):.2f}с')

        # Медианное время
        sorted_times = sorted(load_times)
        median = sorted_times[len(sorted_times) // 2]
        self.stdout.write(f'  📈 Медианное: {median:.2f}с')

        # Процентили
        p95 = sorted_times[int(len(sorted_times) * 0.95)]
        p99 = sorted_times[int(len(sorted_times) * 0.99)]
        self.stdout.write(f'  📊 95-й перцентиль: {p95:.2f}с')
        self.stdout.write(f'  📊 99-й перцентиль: {p99:.2f}с')

        # Распределение по источникам
        if hasattr(self, '_load_sources'):
            csv_count = self._load_sources.get('csv', 0)
            api_count = self._load_sources.get('steam_api', 0)
            if csv_count > 0 or api_count > 0:
                self.stdout.write(f'\n📂 Источники:')
                self.stdout.write(f'  📁 CSV: {csv_count} ({csv_count / len(load_times) * 100:.1f}%)')
                self.stdout.write(f'  🌐 Steam API: {api_count} ({api_count / len(load_times) * 100:.1f}%)')

    def save_steam_cache(self, cache_file_path: Path):
        """Сохранение общего кэша Steam в файл."""
        try:
            import json

            # Создаем бэкап только один раз за всю сессию
            if self.create_backup and cache_file_path.exists():
                backup_path = cache_file_path.with_suffix(f'.backup.json')
                import shutil
                shutil.copy2(cache_file_path, backup_path)
                self.log_debug(f"Создан бэкап кэша: {backup_path}")
                self.create_backup = False

            with open(cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка сохранения кэша: {e}'))

    def save_found_buffer(self):
        """Сохранение буфера успешно найденных игр в файл."""
        if not self.found_buffer:
            return

        file_path = self.found_file_path

        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.log_debug("Ошибка создания директории", error=e)
            return

        with self.output_lock:
            try:
                # Создаем бэкап только один раз за всю сессию
                if self.create_backup and file_path.exists() and file_path.stat().st_size > 0:
                    backup_path = file_path.with_suffix(f'.backup.txt')
                    import shutil
                    shutil.copy2(file_path, backup_path)
                    self.log_debug(f"Создан бэкап файла найденных: {backup_path}")
                    self.create_backup = False

                with open(file_path, 'a', encoding='utf-8') as f:
                    for game_info in self.found_buffer:
                        f.write(game_info)
                        f.write("\n")

                self.log_debug(f"Добавлено {len(self.found_buffer)} игр в файл найденных")
                self.found_buffer = []

            except IOError as e:
                self.log_debug("Ошибка записи файла найденных игр", error=e)

    def save_not_found_buffer(self):
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
                # Создаем бэкап только один раз за всю сессию
                if self.create_backup and file_path.exists() and file_path.stat().st_size > 0:
                    backup_path = file_path.with_suffix(f'.backup.txt')
                    import shutil
                    shutil.copy2(file_path, backup_path)
                    self.log_debug(f"Создан бэкап файла не найденных: {backup_path}")
                    self.create_backup = False

                with open(file_path, 'a', encoding='utf-8') as f:
                    for game_info in self.not_found_buffer:
                        f.write(game_info)
                        f.write("\n")

                self.log_debug(f"Добавлено {len(self.not_found_buffer)} игр в файл не найденных")
                self.not_found_buffer = []

            except IOError as e:
                self.log_debug("Ошибка записи файла не найденных игр", error=e)

    def save_no_description_buffer(self):
        """Сохранение буфера игр без описания в файл."""
        if not self.no_description_buffer:
            return

        file_path = self.no_description_file_path

        try:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.log_debug("Ошибка создания директории", error=e)
            return

        with self.output_lock:
            try:
                # Создаем бэкап только один раз за всю сессию
                if self.create_backup and file_path.exists() and file_path.stat().st_size > 0:
                    backup_path = file_path.with_suffix(f'.backup.txt')
                    import shutil
                    shutil.copy2(file_path, backup_path)
                    self.log_debug(f"Создан бэкап файла без описания: {backup_path}")
                    self.create_backup = False

                with open(file_path, 'a', encoding='utf-8') as f:
                    for game_info in self.no_description_buffer:
                        f.write(game_info)
                        f.write("\n")

                self.log_debug(f"Добавлено {len(self.no_description_buffer)} игр в файл без описания")
                self.no_description_buffer = []

            except IOError as e:
                self.log_debug("Ошибка записи файла игр без описания", error=e)

    def save_buffer(self, output_file: str, is_first: bool = False):
        """Сохранение буфера описаний в файл (кэш)."""
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
                # Создаем бэкап только один раз за всю сессию (не для первого сохранения)
                if self.create_backup and not is_first and file_path.exists() and file_path.stat().st_size > 0:
                    backup_path = file_path.with_suffix(f'.backup.txt')
                    import shutil
                    shutil.copy2(file_path, backup_path)
                    self.log_debug(f"Создан бэкап кэш-файла описаний: {backup_path}")
                    self.create_backup = False

                add_header = False
                if not file_path.exists() or file_path.stat().st_size == 0:
                    add_header = True

                with open(file_path, 'a', encoding='utf-8') as f:
                    if add_header:
                        header = f"{'=' * 80}\n"
                        header += f"STEAM GAME DESCRIPTIONS\n"
                        header += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        header += f"{'=' * 80}\n\n"
                        f.write(header)

                    for desc in self.descriptions_buffer:
                        f.write(desc)
                        f.write("\n" + "-" * 80 + "\n\n")

                self.descriptions_buffer = []
                self.full_output_path = file_path

            except IOError as e:
                self.log_debug("Ошибка записи файла", error=e)

    def load_descriptions_from_csv(self) -> Dict[str, str]:
        """
        Загрузка описаний из CSV-файла steam_games_data.csv.
        Возвращает словарь {название_игры: описание}.
        """
        descriptions = {}
        csv_file_path = Path.cwd() / 'steam_games_data.csv'

        if not csv_file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 CSV-файл не найден: {csv_file_path}'))
            return descriptions

        self.stdout.write(self.style.WARNING(f'📂 Загрузка описаний из CSV: {csv_file_path}'))

        try:
            import sys
            csv.field_size_limit(sys.maxsize)

            with open(csv_file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                loaded_count = 0
                for row in reader:
                    game_name = row.get('name', '').strip()
                    description = row.get('detailed_description', '').strip()

                    if game_name and description:
                        descriptions[game_name.lower()] = description
                        loaded_count += 1

            self.stdout.write(self.style.SUCCESS(f'✅ Загружено {loaded_count} описаний из CSV'))

        except Exception as e:
            self.log_debug("Ошибка при загрузке CSV-файла", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка загрузки CSV: {e}'))

        return descriptions

    def load_descriptions_from_csv_to_db(self, csv_descriptions: Dict[str, str]) -> int:
        """
        Загрузка описаний из CSV в базу данных.
        Загружает только игры без описания.
        Использует массовые запросы для оптимизации скорости.
        Поддерживает прерывание (Ctrl+C).
        """
        if not csv_descriptions:
            self.stdout.write(self.style.WARNING('📂 CSV-словарь пуст'))
            return 0

        self.stdout.write(self.style.WARNING('📂 Загрузка описаний из CSV в базу данных...'))

        # Получаем список всех названий игр из CSV
        game_names_list = list(csv_descriptions.keys())

        self.stdout.write(f'   🔍 Массовый поиск игр для обновления...')

        games_to_update = []

        # Разбиваем на пачки по 1000 названий для избежания слишком длинных запросов
        batch_size = 1000
        total_batches = (len(game_names_list) + batch_size - 1) // batch_size
        processed_batches = 0

        for i in range(0, len(game_names_list), batch_size):
            # Проверяем прерывание перед каждой пачкой
            if self.interrupted:
                self.stdout.write(self.style.WARNING('\n   ⚠️ Прерывание загрузки CSV...'))
                break

            batch_names = game_names_list[i:i + batch_size]

            # Массовый запрос к базе данных
            games = Game.objects.filter(
                name__in=batch_names,
                rawg_description__isnull=True
            ).exclude(rawg_description='')

            # Создаем словарь для быстрого поиска
            name_to_game = {game.name.lower(): game for game in games}

            # Для названий, которые не нашлись по точному совпадению, ищем по регистронезависимому
            not_found_names = [name for name in batch_names if name not in name_to_game]

            if not_found_names:
                # Массовый поиск по регистронезависимому совпадению
                import django.db.models.functions as func
                case_insensitive_games = Game.objects.annotate(
                    lower_name=func.Lower('name')
                ).filter(
                    lower_name__in=not_found_names,
                    rawg_description__isnull=True
                ).exclude(rawg_description='')

                # Добавляем найденные игры в словарь
                for game in case_insensitive_games:
                    name_to_game[game.name.lower()] = game

            # Формируем список для обновления
            for name_lower, description in csv_descriptions.items():
                # Проверяем прерывание внутри цикла
                if self.interrupted:
                    break

                if name_lower in name_to_game:
                    game = name_to_game[name_lower]
                    game.rawg_description = description
                    games_to_update.append(game)

            # Проверяем прерывание после формирования списка
            if self.interrupted:
                break

            processed_batches += 1
            if processed_batches % 10 == 0 or processed_batches == total_batches:
                self.stdout.write(
                    f'   📥 Обработано {min(i + batch_size, len(game_names_list))}/{len(game_names_list)} названий, найдено {len(games_to_update)} игр')

        # Если было прерывание, сообщаем о частичной загрузке
        if self.interrupted:
            self.stdout.write(self.style.WARNING(
                f'\n   ⚠️ Загрузка CSV прервана. Найдено {len(games_to_update)} игр для обновления.'))
            if not games_to_update:
                self.stdout.write(self.style.WARNING('   📂 Не было найдено игр для обновления до прерывания'))
                return 0

        if not games_to_update:
            self.stdout.write(self.style.WARNING('📂 Нет игр для обновления из CSV'))
            return 0

        self.stdout.write(f'   💾 Сохранение {len(games_to_update)} описаний в базу данных...')

        try:
            with transaction.atomic():
                # Сохраняем пачками по 500 записей
                save_batch_size = 500
                saved_count = 0
                total_to_save = len(games_to_update)

                for i in range(0, total_to_save, save_batch_size):
                    # Проверяем прерывание перед каждой пачкой сохранения
                    if self.interrupted:
                        self.stdout.write(self.style.WARNING(
                            f'\n   ⚠️ Прерывание сохранения. Сохранено {saved_count}/{total_to_save}'))
                        break

                    batch = games_to_update[i:i + save_batch_size]
                    Game.objects.bulk_update(batch, ['rawg_description'])
                    saved_count += len(batch)

                    # Вычисляем проценты для прогресс-бара
                    percent = (saved_count / total_to_save) * 100
                    bar_width = 40
                    filled = int(bar_width * saved_count / total_to_save)
                    bar = '█' * filled + '░' * (bar_width - filled)

                    # Формируем строку прогресс-бара в одну строку
                    self.stdout.write(f'\r      [{bar}] {percent:>5.1f}% | {saved_count:>6}/{total_to_save:<6}',
                                      ending='')
                    self.stdout.flush()

                # Переходим на новую строку после завершения
                self.stdout.write('')

            if saved_count > 0:
                self.stdout.write(self.style.SUCCESS(f'✅ Загружено {saved_count} описаний из CSV в БД'))
            else:
                self.stdout.write(self.style.WARNING('⚠️ Не сохранено ни одного описания'))

            return saved_count

        except Exception as e:
            self.log_debug("Ошибка при сохранении описаний из CSV", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка сохранения: {e}'))
            return 0

    def get_description_from_csv(self, game_name: str, csv_descriptions: Dict[str, str]) -> Optional[str]:
        """
        Получение описания игры из CSV-словаря.
        Возвращает описание или None, если не найдено.
        """
        if not csv_descriptions:
            return None

        game_name_lower = game_name.lower()
        return csv_descriptions.get(game_name_lower)

    def process_game(self, game: Game, skip_search: bool, timeout: float,
                     delay: float, output_file: str, dry_run: bool, stats: Dict,
                     csv_descriptions: Dict[str, str] = None) -> Dict:
        """
        Обработка одной игры.
        ВНИМАНИЕ: Проверка rate limit выполняется на уровне пачки (process_batch).
        Внутри этого метода проверка НЕ производится для максимальной скорости.
        """
        result = {
            'success': False,
            'skipped': False,
            'description': None,
            'app_id': None,
            'error_type': None,
            'error_message': None,
            'error_details': None,
            'should_retry': False,
            'load_time': None,
            'source': None
        }

        try:
            # Проверка rate limiting (backoff) - только для блокировок, не для лимита REQUESTS_PER_MINUTE
            if self.rate_limiter and self.rate_limiter.should_backoff():
                if not self.check_rate_limit():
                    result['skipped'] = True
                    result['error_type'] = 'backoff'
                    result['error_message'] = 'Пауза из-за ошибок'
                    result['should_retry'] = True
                    return result

            # Засекаем время начала обработки игры
            game_start_time = time.time()

            if delay > 0:
                time.sleep(delay)

            description_from_csv = None
            if csv_descriptions:
                description_from_csv = self.get_description_from_csv(game.name, csv_descriptions)

            if description_from_csv:
                result['success'] = True
                result['description'] = description_from_csv
                result['app_id'] = None
                result['source'] = 'csv'
                result['load_time'] = time.time() - game_start_time

                if not dry_run:
                    game_info = f"Game ID: {game.id} - {game.name} (Source: CSV file)"
                    with self.output_lock:
                        self.found_buffer.append(game_info)
                        self.found_games.add(game.id)
                    self.save_found_buffer()

                if output_file:
                    formatted = self.format_for_file(game, description_from_csv, 0)
                    with self.output_lock:
                        self.descriptions_buffer.append(formatted)
                        if len(self.descriptions_buffer) >= self.buffer_size:
                            self.save_buffer(output_file, is_first=(self.current_offset == 0))

                with self.stats_lock:
                    stats['success'] += 1
                    self.total_stats['success'] += 1

                return result

            # Поиск в Steam
            app_id = None
            search_error = None

            if game.id in self.app_id_dict:
                app_id = self.app_id_dict[game.id]
                self.log_debug(f"Использую сохраненный Steam App ID: {app_id}", game_name=game.name)
            elif not skip_search:
                app_id, search_error = self.search_steam(game.name, timeout)
            else:
                app_id = None
                search_error = 'no_app_id'

            if not app_id:
                result['load_time'] = time.time() - game_start_time
                with self.stats_lock:
                    if search_error in ['not_found', 'invalid_name', 'app_id_not_found', 'app_not_success',
                                        'no_app_id']:
                        stats['not_found'] += 1
                        self.total_stats['not_found'] += 1
                        result['error_type'] = 'not_found'
                        result[
                            'error_message'] = 'Не найдена в Steam' if search_error != 'no_app_id' else 'Нет Steam App ID'

                        if not dry_run and search_error != 'no_app_id':
                            self.add_to_not_found(game, "not_found")
                    else:
                        stats['error'] += 1
                        self.total_stats['error'] += 1
                        if search_error == '403':
                            self.total_stats['error_403'] += 1
                        elif search_error == '429':
                            self.total_stats['error_429'] += 1
                        elif search_error == 'timeout':
                            self.total_stats['error_timeout'] += 1
                        else:
                            self.total_stats['error_other'] += 1
                        result['error_type'] = search_error or 'search_error'
                        result['error_message'] = f'Ошибка поиска: {search_error}'
                        result['should_retry'] = True

                result['skipped'] = True
                return result

            # Получение описания
            description, desc_error = self.fetch_description(app_id, timeout, game.name)

            # Записываем время загрузки
            load_time = time.time() - game_start_time
            result['load_time'] = load_time

            if not description:
                with self.stats_lock:
                    if desc_error in ['app_id_not_found', 'app_not_success', 'invalid_app_id']:
                        stats['not_found'] += 1
                        self.total_stats['not_found'] += 1
                        result['error_type'] = 'not_found'
                        result['error_message'] = 'Не найдена в Steam'
                        if not dry_run:
                            self.add_to_not_found(game, "not_found")

                    elif desc_error and desc_error.startswith('not_game_'):
                        stats['not_found'] += 1
                        self.total_stats['not_found'] += 1
                        result['error_type'] = 'not_found'
                        result['error_message'] = f'Не игра (тип: {desc_error.replace("not_game_", "")})'
                        if not dry_run:
                            self.add_to_not_found(game, desc_error)

                    elif desc_error in ['no_description', 'empty_after_clean', 'empty_response', 'no_game_data']:
                        stats['no_description'] += 1
                        self.total_stats['no_description'] += 1
                        result['error_type'] = 'no_description'
                        result['error_message'] = 'Нет описания'
                        if not dry_run:
                            self.add_to_no_description(game, "no_description", f"App ID: {app_id}")

                    else:
                        stats['error'] += 1
                        self.total_stats['error'] += 1
                        if desc_error == '403':
                            self.total_stats['error_403'] += 1
                        elif desc_error == '429':
                            self.total_stats['error_429'] += 1
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
            result['source'] = 'steam_api'

            if not dry_run:
                self.add_to_found(game, app_id)

            if output_file:
                formatted = self.format_for_file(game, description, app_id)
                with self.output_lock:
                    self.descriptions_buffer.append(formatted)
                    if len(self.descriptions_buffer) >= self.buffer_size:
                        self.save_buffer(output_file, is_first=(self.current_offset == 0))

            with self.stats_lock:
                stats['success'] += 1
                self.total_stats['success'] += 1

                # Сохраняем статистику времени загрузки
                if not hasattr(self, '_load_times'):
                    self._load_times = []
                    self._load_sources = {'csv': 0, 'steam_api': 0}

                self._load_times.append(load_time)
                self._load_sources[result['source']] = self._load_sources.get(result['source'], 0) + 1

            return result

        except KeyboardInterrupt:
            self.interrupted = True
            result['skipped'] = True
            result['error_type'] = 'interrupted'
            result['error_message'] = 'Прервано пользователем'
            result['should_retry'] = False
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
            result['load_time'] = time.time() - game_start_time if 'game_start_time' in locals() else None
            return result

    def process_batch(self, games: List[Game], skip_search: bool, timeout: float,
                      delay: float, output_file: str, dry_run: bool,
                      batch_stats: Dict, is_first: bool = False,
                      csv_descriptions: Dict[str, str] = None) -> List[Game]:
        """Обработка одного батча игр с поддержкой прерывания."""
        games_to_update = []
        batch_failures = 0
        batch_total = len(games)
        executor = None

        self.stdout.write(f'  🕒 Таймаут: {timeout}с, воркеров: {self.workers}')

        try:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {}

                # ПЕРЕД КАЖДЫМ БАТЧЕМ ПРОВЕРЯЕМ ЛИМИТ ЗАПРОСОВ В МИНУТУ
                with self._request_counter_lock:
                    # Используем self.loop_start_time из цикла
                    if hasattr(self, 'loop_start_time') and self.loop_start_time:
                        elapsed_seconds = time.time() - self.loop_start_time
                    else:
                        elapsed_seconds = 1

                    # Общее количество сделанных запросов
                    total_requests_made = (self.total_stats['success'] +
                                           self.total_stats['not_found'] +
                                           self.total_stats['no_description'] +
                                           self.total_stats['error'])

                    # Если это не первая итерация И есть запросы, проверяем лимит
                    if total_requests_made > 0:
                        # Прогнозируемое количество запросов после добавления текущей пачки
                        predicted_total = total_requests_made + len(games)

                        # Рассчитываем, сколько нужно ждать, чтобы прогнозируемое количество запросов
                        # уложилось в лимит REQUESTS_PER_MINUTE в минуту
                        wait_needed = (predicted_total * 60 / self.REQUESTS_PER_MINUTE) - elapsed_seconds

                        if wait_needed > 0:
                            self._request_counter_lock.release()

                            try:
                                self.stdout.write(
                                    self.style.WARNING(
                                        f'\n  ⏱️ Сделано {total_requests_made} запросов, +{len(games)} = {predicted_total} | '
                                        f'лимит {self.REQUESTS_PER_MINUTE}/мин | нужно ждать {wait_needed:.1f}с...'
                                    )
                                )

                                while True:
                                    if self.interrupted:
                                        raise KeyboardInterrupt()

                                    # Пересчитываем оставшееся время
                                    if hasattr(self, 'loop_start_time') and self.loop_start_time:
                                        current_elapsed = time.time() - self.loop_start_time
                                    else:
                                        current_elapsed = 1

                                    current_total = (self.total_stats['success'] +
                                                     self.total_stats['not_found'] +
                                                     self.total_stats['no_description'] +
                                                     self.total_stats['error'])
                                    current_predicted = current_total + len(games)
                                    current_wait = (current_predicted * 60 / self.REQUESTS_PER_MINUTE) - current_elapsed

                                    if current_wait <= 0:
                                        break

                                    # Обновляем прогресс во время паузы
                                    total_processed = getattr(self, 'processed_total', 0)
                                    total_limit = getattr(self, 'limit', 0)

                                    if total_limit > 0:
                                        progress = (total_processed / total_limit * 100)
                                        bar_length = 40
                                        filled = int(bar_length * progress / 100)
                                        bar = '█' * filled + '░' * (bar_length - filled)

                                        self.stdout.write(
                                            self.style.NOTICE(
                                                f'\r📊 ПРОГРЕСС: [{bar}] {progress:.1f}%\n'
                                                f'📊 Обработано: {total_processed}/{total_limit} игр\n'
                                                f'⏱️ Пауза {current_wait:.1f}с | Будет {current_predicted} запросов   '
                                            ),
                                            ending=''
                                        )
                                    else:
                                        self.stdout.write(
                                            self.style.WARNING(
                                                f'\r  ⏱️ Пауза {current_wait:.1f}с | Будет {current_predicted} запросов   '
                                            ),
                                            ending=''
                                        )
                                    self.stdout.flush()

                                    time.sleep(0.2)

                                self.stdout.write('\n')

                            finally:
                                self._request_counter_lock.acquire()

                    # Текущая статистика после паузы
                    if hasattr(self, 'loop_start_time') and self.loop_start_time:
                        elapsed_seconds = time.time() - self.loop_start_time
                    else:
                        elapsed_seconds = 1

                    total_requests_made = (self.total_stats['success'] +
                                           self.total_stats['not_found'] +
                                           self.total_stats['no_description'] +
                                           self.total_stats['error'])
                    current_rate = total_requests_made / elapsed_seconds if elapsed_seconds > 0 else 0
                    target_rate = self.REQUESTS_PER_MINUTE / 60

                    self.stdout.write(f'  📊 Скорость: {current_rate:.2f} зап/с (лимит {target_rate:.2f}) | '
                                      f'сделано {total_requests_made} за {elapsed_seconds:.0f}с')

                # Отправляем задачи для текущего батча
                for game in games:
                    if self.interrupted:
                        break

                    future = executor.submit(
                        self.process_game,
                        game, skip_search, timeout, delay,
                        output_file, dry_run, batch_stats, csv_descriptions
                    )
                    futures[future] = game

                completed = 0
                error_403_count = 0
                timeout_count = 0

                processed_games = set()
                futures_list = list(futures.keys())
                total_futures = len(futures_list)

                if total_futures == 0:
                    self.stdout.write(self.style.WARNING('  ⚠️ Нет задач для выполнения'))
                    return []

                self.stdout.write(f'  📋 Отправлено задач: {total_futures}')

                while completed < total_futures and not self.interrupted:
                    if self.rate_limiter and self.rate_limiter.should_backoff():
                        if not self.check_rate_limit():
                            break

                    if self.interrupted:
                        break

                    for future in list(futures_list):
                        if future in processed_games:
                            continue

                        if future.done():
                            game = futures[future]
                            processed_games.add(future)
                            completed += 1

                            try:
                                result = future.result(timeout=1)

                                if result['success']:
                                    if result.get('app_id') is None:
                                        self.stdout.write(
                                            f'  [{completed}/{batch_total}] 📄 {game.name[:40]:40} (Из CSV)'
                                        )
                                    else:
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

                    time.sleep(0.05)

                    if self.interrupted:
                        for future in futures_list:
                            if future not in processed_games and not future.done():
                                future.cancel()
                        break

                if self.interrupted:
                    for future in futures_list:
                        if future not in processed_games and not future.done():
                            future.cancel()
                    for future in futures_list:
                        try:
                            future.result(timeout=0.5)
                        except:
                            pass
                    self.stdout.write(self.style.WARNING('  ✅ Все задачи отменены'))

            if error_403_count > 0:
                self.stdout.write(self.style.ERROR(f'  🚫 403 ошибок в батче: {error_403_count}'))

            if timeout_count > 0:
                self.stdout.write(self.style.WARNING(f'  ⌛ Таймаутов в батче: {timeout_count}'))

            if batch_total > 0:
                failure_ratio = batch_failures / batch_total
                if failure_ratio >= self.batch_failure_threshold:
                    self.stdout.write(self.style.ERROR(f'\n  ⚠️ ВЫСОКИЙ УРОВЕНЬ ОШИБОК: {failure_ratio * 100:.1f}%'))
                    wait_time = self.rate_limiter.record_failure("batch_failure")
                    if wait_time > 0:
                        self.check_rate_limit()

        finally:
            if executor:
                executor.shutdown(wait=False, cancel_futures=True)

        if self.descriptions_buffer:
            self.save_buffer(output_file, is_first)

        return games_to_update

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
                        skip_not_found: bool = True, not_found_set: set = None,
                        skip_no_description: bool = True, no_description_set: set = None) -> List[Game]:
        """Получение батча игр с учетом не найденных и без описания."""
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

            if skip_no_description is True and no_description_set:
                queryset = queryset.exclude(id__in=no_description_set)

            games = list(queryset.order_by('-rating_count', 'id')[offset:offset + batch_size])

            if games:
                self.stdout.write(
                    self.style.SUCCESS(f'📊 Пакет: игры {offset + 1}-{offset + len(games)} (смещение {offset})')
                )

            return games

        except Exception as e:
            self.log_debug("Ошибка при получении батча игр", error=e)
            return []

    def search_steam(self, game_name: str, timeout: float) -> Tuple[Optional[int], Optional[str]]:
        """Поиск игры в Steam с использованием универсальной функции get_request."""
        if self.rate_limiter and self.rate_limiter.should_backoff():
            self.check_rate_limit()
            if self.rate_limiter.should_backoff():
                return None, 'backoff'

        search_name = re.sub(r'[^\w\s-]', '', game_name)
        search_name = re.sub(r'\s+', ' ', search_name).strip()

        if not search_name:
            return None, 'invalid_name'

        self.log_verbose(f"Поиск в Steam: '{search_name}'", game_name=game_name)

        url = "https://store.steampowered.com/api/storesearch"
        params = {
            'term': search_name[:50],
            'l': 'english',
            'cc': 'us'
        }

        try:
            response_data = get_request(url, params, timeout, retries=2)

            if response_data is None:
                error_type = 'no_response'
                self.rate_limiter.record_failure(error_type)
                return None, error_type

            if response_data.get('total', 0) > 0:
                items = response_data.get('items', [])
                if items:
                    app_id = items[0].get('id')
                    self.log_verbose(f"Найден Steam ID: {app_id}", game_name=game_name)
                    self.rate_limiter.record_success()
                    return app_id, None
                else:
                    return None, 'not_found'
            else:
                self.log_debug(f"Нет результатов для '{search_name}'", game_name=game_name)
                return None, 'not_found'

        except Exception as e:
            self.log_debug(f"Ошибка при поиске", game_name=game_name, error=e)
            self.rate_limiter.record_failure("exception")
            return None, 'exception'

    def fetch_description(self, app_id: int, timeout: float, game_name: str = None) -> Tuple[
        Optional[str], Optional[str]]:
        """Получение описания игры с использованием универсальной функции get_request."""
        if self.rate_limiter and self.rate_limiter.should_backoff():
            self.check_rate_limit()
            if self.rate_limiter.should_backoff():
                return None, 'backoff'

        self.log_verbose(f"Запрос описания для App ID: {app_id}", game_name=game_name)

        if not app_id or app_id <= 0:
            self.log_debug(f"Некорректный App ID: {app_id}", game_name=game_name)
            return None, 'invalid_app_id'

        url = "https://store.steampowered.com/api/appdetails"
        params = {
            'appids': app_id,
            'l': 'english',
            'cc': 'us'
        }

        try:
            response_data = get_request(url, params, timeout, retries=2)

            if response_data is None:
                error_type = 'no_response'
                self.rate_limiter.record_failure(error_type)
                return None, error_type

            str_app_id = str(app_id)
            if str_app_id not in response_data:
                self.log_debug(f"App ID {app_id} отсутствует в ответе", game_name=game_name)
                return None, 'app_id_not_found'

            app_data = response_data[str_app_id]

            if not app_data.get('success'):
                self.log_debug(f"App ID {app_id} не успешен (success=False)", game_name=game_name)
                return None, 'app_not_success'

            game_data = app_data.get('data')
            if not game_data:
                self.log_debug(f"Нет данных для App ID {app_id}", game_name=game_name)
                return None, 'no_game_data'

            description = None
            description_source = None

            for field in ['detailed_description', 'about_the_game', 'short_description']:
                desc = game_data.get(field, '')
                if desc and isinstance(desc, str) and desc.strip():
                    description = desc
                    description_source = field
                    break

            if description:
                self.log_verbose(f"Найдено описание в поле '{description_source}'", game_name=game_name)
                cleaned_description = self.clean_html(description)

                if cleaned_description and cleaned_description.strip():
                    self.rate_limiter.record_success()
                    return cleaned_description, None
                else:
                    self.log_debug(f"Описание пустое после очистки HTML", game_name=game_name)
                    return None, 'empty_after_clean'

            if game_data.get('is_free') is False and not game_data.get('price_overview'):
                self.log_debug(f"Платная игра без описания", game_name=game_name)
                return None, 'no_description'

            if game_data.get('type') and game_data.get('type') != 'game':
                self.log_debug(f"Это не игра, а {game_data.get('type')}", game_name=game_name)
                return None, f"not_game_{game_data.get('type')}"

            self.log_debug(f"Нет описания ни в одном из полей", game_name=game_name)
            return None, 'no_description'

        except Exception as e:
            self.log_debug(f"Ошибка при получении описания", game_name=game_name, error=e)
            self.rate_limiter.record_failure("exception")
            return None, 'exception'

    def clean_html(self, text: Optional[str]) -> Optional[str]:
        """Очистка HTML тегов."""
        if not text:
            return text
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

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

    def add_to_found(self, game: Game, app_id: int):
        """Добавление игры в список успешно найденных."""
        if game.id in self.found_games:
            return

        game_info = f"Game ID: {game.id} - {game.name} (Steam App ID: {app_id})"
        self.found_buffer.append(game_info)
        self.found_games.add(game.id)
        self.app_id_dict[game.id] = app_id

        self.update_steam_cache(game.id, True, app_id, game.name)
        self.save_found_buffer()

    def add_to_not_found(self, game: Game, reason: str = "not_found"):
        """Добавление игры в список не найденных."""
        if game.id in self.not_found_games:
            return

        game_info = f"Game ID: {game.id} - {game.name} (Steam App ID: None) - не найдено в Steam"
        self.not_found_buffer.append(game_info)
        self.not_found_games.add(game.id)

        self.update_steam_cache(game.id, False, None, game.name)
        self.save_not_found_buffer()

    def add_to_no_description(self, game: Game, reason: str = "no_description", error_details: str = None):
        """Добавление игры в список без описания."""
        if game.id in self.no_description_games:
            return

        app_id = "None"
        if game.id in self.cache_data and self.cache_data[game.id].get('app_id'):
            app_id = self.cache_data[game.id]['app_id']

        error_info = f" - {error_details}" if error_details else ""
        game_info = f"Game ID: {game.id} - {game.name} (Steam App ID: {app_id}) - {reason}{error_info}"
        self.no_description_buffer.append(game_info)
        self.no_description_games.add(game.id)

        if game.id in self.cache_data:
            self.cache_data[game.id]['has_description'] = False

        self.save_no_description_buffer()

    def update_steam_cache(self, game_id: int, found: bool, app_id: int = None, game_name: str = None):
        """Обновление записи в кэше Steam."""
        self.cache_data[game_id] = {
            'found': found,
            'app_id': app_id,
            'checked_at': datetime.now().isoformat(),
            'game_name': game_name or 'Unknown',
            'has_description': False
        }

    def load_steam_cache(self, cache_file_path: Path) -> Dict[int, Dict[str, any]]:
        """Загрузка общего кэша Steam из файла."""
        cache = {}

        if not cache_file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 Кэш Steam не существует: {cache_file_path}'))
            return cache

        self.stdout.write(self.style.WARNING(f'📂 Загрузка кэша Steam...'))

        try:
            import json
            file_size = cache_file_path.stat().st_size
            self.stdout.write(f'   📦 Размер файла: {file_size:,} байт ({file_size / 1024:.1f} КБ)')

            with open(cache_file_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                cache = {int(k): v for k, v in cache.items()}

            self.stdout.write(self.style.SUCCESS(f'📂 Загружен кэш Steam: {len(cache)} игр'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка загрузки кэша: {e}'))

        return cache

    def load_found_games(self) -> Tuple[set, dict]:
        """Загрузка списка успешно найденных игр из файла."""
        found_set = set()
        app_id_dict = {}
        file_path = self.found_file_path

        if file_path and file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 Загрузка найденных игр...'))

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith('=') or line.startswith('STEAM FOUND GAMES') or line.startswith('Created:'):
                            continue

                        if 'Game ID:' in line:
                            try:
                                game_part = line.split('Game ID:')[1].strip()

                                if '-' in game_part:
                                    game_id_str = game_part.split('-')[0].strip()
                                    game_id = int(game_id_str)
                                    found_set.add(game_id)

                                    if 'Steam App ID:' in line:
                                        app_id_part = line.split('Steam App ID:')[1].strip()
                                        if ')' in app_id_part:
                                            app_id_part = app_id_part.split(')')[0]
                                        if app_id_part.isdigit():
                                            app_id_dict[game_id] = int(app_id_part)

                                else:
                                    game_id_str = game_part.split(' ')[0].strip()
                                    game_id = int(game_id_str)
                                    found_set.add(game_id)

                            except (ValueError, IndexError):
                                continue

            except Exception as e:
                self.log_debug(f"Ошибка при загрузке файла найденных игр", error=e)

        self.stdout.write(
            self.style.SUCCESS(f'📂 Загружено {len(found_set)} найденных игр, {len(app_id_dict)} с App ID'))
        return found_set, app_id_dict

    def load_not_found_games(self) -> set:
        """Загрузка списка не найденных игр из файла."""
        not_found_set = set()
        file_path = self.not_found_file_path

        if file_path and file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 Загрузка не найденных игр...'))

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith('=') or line.startswith('STEAM NOT FOUND GAMES') or line.startswith(
                                'Created:'):
                            continue

                        if 'Game ID:' in line:
                            try:
                                game_part = line.split('Game ID:')[1].strip()
                                if '-' in game_part:
                                    game_id_str = game_part.split('-')[0].strip()
                                    game_id = int(game_id_str)
                                    not_found_set.add(game_id)
                                else:
                                    game_id_str = game_part.split(' ')[0].strip()
                                    game_id = int(game_id_str)
                                    not_found_set.add(game_id)
                            except (ValueError, IndexError):
                                continue

            except Exception as e:
                self.log_debug(f"Ошибка при загрузке файла не найденных игр", error=e)

        self.stdout.write(self.style.SUCCESS(f'📂 Загружено {len(not_found_set)} не найденных игр'))
        return not_found_set

    def load_no_description_games(self) -> set:
        """Загрузка списка игр без описания из файла."""
        no_description_set = set()
        file_path = self.no_description_file_path

        if file_path and file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 Загрузка игр без описания...'))

            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        if line.startswith('=') or line.startswith(
                                'STEAM GAMES WITHOUT DESCRIPTION') or line.startswith('Created:'):
                            continue

                        if 'Game ID:' in line:
                            try:
                                game_part = line.split('Game ID:')[1].strip()
                                if '-' in game_part:
                                    game_id_str = game_part.split('-')[0].strip()
                                    game_id = int(game_id_str)
                                    no_description_set.add(game_id)
                                else:
                                    game_id_str = game_part.split(' ')[0].strip()
                                    game_id = int(game_id_str)
                                    no_description_set.add(game_id)
                            except (ValueError, IndexError):
                                continue

            except Exception as e:
                self.log_debug(f"Ошибка при загрузке файла игр без описания", error=e)

        self.stdout.write(self.style.SUCCESS(f'📂 Загружено {len(no_description_set)} игр без описания'))
        return no_description_set

    def save_progress(self, index: int):
        """Сохранение индекса прогресса в файл."""
        if self.progress_file_path:
            try:
                with open(self.progress_file_path, 'w') as f:
                    f.write(str(index))
            except Exception as e:
                self.log_debug(f"Ошибка сохранения прогресса: {e}")

    def load_progress(self) -> int:
        """Загрузка индекса прогресса из файла."""
        if self.progress_file_path and self.progress_file_path.exists():
            try:
                with open(self.progress_file_path, 'r') as f:
                    return int(f.read().strip())
            except Exception:
                pass
        return 0

    def reset_progress(self):
        """Сброс индекса прогресса."""
        if self.progress_file_path and self.progress_file_path.exists():
            try:
                self.progress_file_path.unlink()
                self.stdout.write(self.style.SUCCESS('✅ Прогресс сброшен'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Ошибка сброса прогресса: {e}'))

    def check_rate_limit(self) -> bool:
        """Проверка rate limiting и выполнение backoff при необходимости."""
        if hasattr(self, '_pause_active') and self._pause_active:
            return True

        if self.rate_limiter and self.rate_limiter.should_backoff():
            remaining = self.rate_limiter.get_wait_time_remaining()
            if remaining > 0:
                self._pause_active = True

                total_processed = self.processed_total if hasattr(self, 'processed_total') else 0
                self.log_timeline("STOP", total_processed)

                self.stdout.write('', ending='')
                self.stdout.write(self.style.ERROR(f'\n🚫 Пауза на {remaining:.1f}с для снятия блокировки...'))

                try:
                    self.session.close()
                except:
                    pass

                pause_start_time = time.time()
                last_display = 0

                while remaining > 0 and not self.interrupted:
                    try:
                        current_remaining = self.rate_limiter.get_wait_time_remaining()
                        if current_remaining <= 0:
                            break

                        # Обновляем прогресс каждые 2 секунды
                        if time.time() - last_display >= 2.0:
                            total_processed = getattr(self, 'processed_total', 0)
                            total_limit = getattr(self, 'limit', 0)

                            if total_limit > 0:
                                progress = (total_processed / total_limit * 100)
                                bar_length = 40
                                filled = int(bar_length * progress / 100)
                                bar = '█' * filled + '░' * (bar_length - filled)

                                self.stdout.write(
                                    self.style.NOTICE(
                                        f'\r📊 ПРОГРЕСС: [{bar}] {progress:.1f}% | '
                                        f'Обработано: {total_processed}/{total_limit} | '
                                        f'🚫 Пауза {current_remaining:.1f}с   '
                                    ),
                                    ending=''
                                )
                            else:
                                self.stdout.write(
                                    self.style.WARNING(
                                        f'\r  🚫 Пауза {current_remaining:.1f}с   '
                                    ),
                                    ending=''
                                )
                            self.stdout.flush()
                            last_display = time.time()

                        remaining = current_remaining
                        time.sleep(0.5)

                    except KeyboardInterrupt:
                        self.interrupted = True
                        break

                self.stdout.write('', ending='')
                self.stdout.write('\n')

                if self.interrupted:
                    self._pause_active = False
                    return False

                self.log_timeline("RESUME")

                try:
                    self.session = self._create_session()
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'❌ Ошибка создания сессии: {e}'))

                with self.stats_lock:
                    self.total_stats['backoff_pauses'] += 1

                self._pause_active = False
                return True
        return False

    def log_timeline(self, event_type: str, games_processed: int = None):
        """Логирование событий временной шкалы."""
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

        if event_type == "START" or event_type == "RESUME":
            self.stdout.write(self.style.SUCCESS(message))
        elif event_type == "STOP":
            self.stdout.write(self.style.ERROR(message))
        elif event_type == "END":
            self.stdout.write(self.style.SUCCESS(message))

        if self.log_file_path:
            try:
                with open(self.log_file_path, 'a', encoding='utf-8') as f:
                    f.write(message + "\n")
            except Exception:
                pass

    def log_debug(self, message: str, game_name: str = None, error: Exception = None):
        """Логирование debug сообщений."""
        if self.debug:
            prefix = f"[DEBUG] {game_name}: " if game_name else "[DEBUG] "
            if error:
                error_type = type(error).__name__
                error_msg = str(error)
                trace = traceback.format_exc().split('\n')[-3:-1]
                trace_str = ' '.join(trace)
                full_message = f'{prefix}{message}\n  🔴 Тип: {error_type}\n  📝 Сообщение: {error_msg}\n  🔍 Трейс: {trace_str}'
                self.error_log.append(full_message)
            else:
                full_message = f'{prefix}{message}'
            self.stdout.write(self.style.WARNING(full_message))

    def log_verbose(self, message: str, game_name: str = None, response=None):
        """Логирование verbose сообщений."""
        if self.verbose:
            prefix = f"[VERBOSE] {game_name}: " if game_name else "[VERBOSE] "
            if response:
                status = response.status_code if hasattr(response, 'status_code') else 'N/A'
                url = response.url if hasattr(response, 'url') else 'N/A'
                full_message = f'{prefix}{message}\n  🌐 URL: {url}\n  📊 Статус: {status}'
            else:
                full_message = f'{prefix}{message}'
            self.stdout.write(self.style.NOTICE(full_message))

    def signal_handler(self, signum, frame):
        """Обработчик сигнала прерывания."""
        self.stdout.write(self.style.ERROR('\n\n⚠️ Получен сигнал прерывания (Ctrl+C)'))
        self.interrupted = True

    def _clear_logs(self) -> None:
        """Очистка папки с логами."""
        self.stdout.write(self.style.WARNING('⚠️ НАЧАЛО ОЧИСТКИ ПАПКИ С ЛОГАМИ'))
        self.stdout.write(self.style.WARNING('=' * 60))

        try:
            output_dir = self.output_dir
            if not output_dir.exists():
                self.stdout.write(self.style.WARNING(f'📁 Папка не существует: {output_dir}'))
                return

            files = list(output_dir.iterdir())
            file_count = len(files)

            if file_count == 0:
                self.stdout.write(self.style.WARNING(f'📁 Папка уже пуста: {output_dir}'))
                return

            self.stdout.write(self.style.WARNING(f'📊 Найдено файлов: {file_count}'))

            response = input(f'⚠️ Удалить ВСЮ папку {output_dir}? (yes/no): ')
            if response.lower() != 'yes':
                self.stdout.write(self.style.WARNING('❌ Очистка отменена'))
                return

            backup_dir = output_dir.parent / f"{output_dir.name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            import shutil
            shutil.copytree(output_dir, backup_dir)
            self.stdout.write(self.style.WARNING(f'📋 Бэкап: {backup_dir}'))

            shutil.rmtree(output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)

            self.stdout.write(self.style.SUCCESS('✅ ОЧИСТКА ЛОГОВ ЗАВЕРШЕНА'))

        except Exception as e:
            self.log_debug("Ошибка при очистке логов", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка: {e}'))

    def _clear_descriptions(self, pc: Platform) -> None:
        """Очистка всех существующих rawg_description."""
        self.stdout.write(self.style.WARNING('⚠️ НАЧАЛО ОЧИСТКИ ВСЕХ ОПИСАНИЙ'))
        self.stdout.write(self.style.WARNING('=' * 60))

        try:
            games_to_clear = Game.objects.filter(
                platforms=pc,
                rawg_description__isnull=False
            ).exclude(rawg_description='')

            count = games_to_clear.count()
            if count == 0:
                self.stdout.write(self.style.SUCCESS('✅ Нет описаний для очистки'))
                return

            self.stdout.write(self.style.WARNING(f'📊 Найдено игр с описаниями: {count}'))

            response = input(f'⚠️ Удалить ВСЕ {count} описаний? (yes/no): ')
            if response.lower() != 'yes':
                self.stdout.write(self.style.WARNING('❌ Очистка отменена'))
                return

            backup_file = self.output_dir / f'descriptions_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            try:
                import json
                backup_data = []
                for game in games_to_clear[:1000]:
                    backup_data.append({
                        'id': game.id,
                        'name': game.name,
                        'rawg_description': game.rawg_description
                    })
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, ensure_ascii=False, indent=2)
                self.stdout.write(self.style.WARNING(f'📋 Бэкап: {backup_file}'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'⚠️ Ошибка бэкапа: {e}'))

            updated_count = games_to_clear.update(rawg_description=None)
            self.stdout.write(self.style.SUCCESS(f'✅ Удалено описаний: {updated_count}'))

            if self.full_output_path and self.full_output_path.exists():
                backup_desc_file = self.full_output_path.with_suffix(
                    f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
                import shutil
                shutil.copy2(self.full_output_path, backup_desc_file)
                with open(self.full_output_path, 'w', encoding='utf-8') as f:
                    header = f"{'=' * 80}\nSTEAM GAME DESCRIPTIONS\nGenerated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 80}\n\n"
                    f.write(header)

            self.stdout.write(self.style.SUCCESS('✅ ОЧИСТКА ОПИСАНИЙ ЗАВЕРШЕНА'))

        except Exception as e:
            self.log_debug("Ошибка при очистке описаний", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка: {e}'))

    def load_descriptions_from_cache(self) -> int:
        """
        Загрузка описаний из кэш-файла в базу данных.
        Загружает только игры без описания.
        """
        if not self.full_output_path or not self.full_output_path.exists():
            self.stdout.write(self.style.WARNING('📂 Кэш-файл не найден'))
            return 0

        self.stdout.write(self.style.WARNING('📂 Загрузка описаний из кэша...'))

        games_from_cache = []
        current_game = None
        current_description = []
        reading_description = False

        try:
            with open(self.full_output_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.rstrip()

                    if line.startswith('Game: '):
                        if current_game and current_description:
                            description_text = '\n'.join(current_description).strip()
                            if description_text:
                                games_from_cache.append((current_game, description_text))
                        game_name = line.replace('Game: ', '').strip()
                        current_game = game_name
                        current_description = []
                        reading_description = False

                    elif line == 'DESCRIPTION:' and current_game:
                        reading_description = True

                    elif reading_description and line != '-' * 80:
                        if line.strip():
                            current_description.append(line)

                    elif line == '-' * 80:
                        if current_game and current_description:
                            description_text = '\n'.join(current_description).strip()
                            if description_text:
                                games_from_cache.append((current_game, description_text))
                        current_game = None
                        current_description = []
                        reading_description = False

                if current_game and current_description:
                    description_text = '\n'.join(current_description).strip()
                    if description_text:
                        games_from_cache.append((current_game, description_text))

            if not games_from_cache:
                self.stdout.write(self.style.WARNING('📂 Кэш-файл пуст'))
                return 0

            self.stdout.write(self.style.SUCCESS(f'📊 Найдено {len(games_from_cache)} игр в кэше'))

            game_names = [game[0] for game in games_from_cache]

            games_with_description = set(
                Game.objects.filter(
                    name__in=game_names,
                    rawg_description__isnull=False
                ).exclude(rawg_description='').values_list('name', flat=True)
            )

            games_to_load = [g for g in games_from_cache if g[0] not in games_with_description]

            if not games_to_load:
                self.stdout.write(self.style.SUCCESS('✅ Все описания уже есть в БД'))
                return 0

            self.stdout.write(self.style.WARNING(
                f'📊 Будет загружено {len(games_to_load)} новых описаний (пропущено {len(games_with_description)})'))

            loaded_count = 0
            processed = 0
            for game_name, description in games_to_load:
                processed += 1
                if processed % 50 == 0 or processed == len(games_to_load):
                    self.stdout.write(f'   📥 Загружено {processed}/{len(games_to_load)} игр')

                if self._save_description_to_db(game_name, description.split('\n')):
                    loaded_count += 1

            if loaded_count > 0:
                self.stdout.write(self.style.SUCCESS(f'📂 Загружено {loaded_count} новых описаний'))

        except Exception as e:
            self.log_debug("Ошибка загрузки из кэша", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка: {e}'))

        return loaded_count

    def _save_description_to_db(self, game_name: str, description_lines: list) -> bool:
        """Сохранение описания в базу данных."""
        try:
            description = '\n'.join(description_lines).strip()
            if not description:
                return False

            game = Game.objects.filter(name__iexact=game_name).first()
            if not game:
                game = Game.objects.filter(name__icontains=game_name).first()

            if game:
                if not game.rawg_description or game.rawg_description != description:
                    game.rawg_description = description
                    game.save(update_fields=['rawg_description'])
                    return True
            return False

        except Exception as e:
            self.log_debug(f"Ошибка сохранения для {game_name}", error=e)
            return False

    def _save_stats_to_file(self):
        """Сохранение текущей статистики в файл."""
        try:
            import json
            if not self.stats_file_path:
                return

            stats_data = {
                'command': 'fetch_steam_descriptions',
                'started_at': self.start_time.isoformat() if self.start_time else None,
                'last_updated': datetime.now().isoformat(),
                'processed_total': self.processed_total,
                'limit': self.limit,
                'total_stats': self.total_stats,
                'batch_times': self.batch_times[-10:] if self.batch_times else []
            }

            with open(self.stats_file_path, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2, default=str)

        except Exception as e:
            self.log_debug("Ошибка сохранения статистики", error=e)

    def _init_stats_file(self):
        """Инициализация файла статистики."""
        try:
            import json
            self.stats_file_path = self.output_dir / 'steam_fetcher_stats.json'

            initial_stats = {
                'command': 'fetch_steam_descriptions',
                'started_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat(),
                'processed_total': 0,
                'limit': None,
                'total_stats': self.total_stats,
                'batch_times': []
            }

            with open(self.stats_file_path, 'w', encoding='utf-8') as f:
                json.dump(initial_stats, f, ensure_ascii=False, indent=2, default=str)

            self.stdout.write(self.style.SUCCESS(f'📊 Файл статистики создан: {self.stats_file_path}'))

        except Exception as e:
            self.log_debug("Ошибка при инициализации статистики", error=e)

    def _find_specific_game(self, game_name: str, pc: Platform) -> Optional[Tuple]:
        """Поиск конкретной игры по названию."""
        self.stdout.write(f'🔍 Поиск игры: "{game_name}"')

        exact_games = Game.objects.filter(
            Q(name__iexact=game_name) & Q(platforms=pc)
        ).order_by('-rating_count')

        exact_count = exact_games.count()
        self.stdout.write(f'  Точных совпадений: {exact_count}')

        if exact_count > 0:
            target_game = exact_games.first()
            self.stdout.write(self.style.SUCCESS(f'  ✅ Найдена: {target_game.name} (ID: {target_game.id})'))
        else:
            all_matches = Game.objects.filter(
                Q(name__icontains=game_name) & Q(platforms=pc)
            ).order_by('-rating_count')

            total_matches = all_matches.count()
            self.stdout.write(f'  Частичных совпадений: {total_matches}')

            if total_matches > 0:
                target_game = all_matches.first()
                self.stdout.write(self.style.SUCCESS(f'  ✅ Выбрана: {target_game.name} (ID: {target_game.id})'))
            else:
                self.stdout.write(self.style.ERROR(f'❌ Игра "{game_name}" не найдена'))
                return None

        return target_game, 1, 1, True

    def _print_startup_info(self, limit: int, total_to_process: int,
                            processed_total: int, batch_size: int,
                            iteration_pause: int, options: Dict,
                            process_not_found: bool, skip_not_found: bool,
                            process_no_description: bool, skip_no_description: bool) -> None:
        """Вывод информации о запуске."""
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('STEAM DESCRIPTIONS FETCHER'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        self.stdout.write(f'\n📊 Параметры:')
        self.stdout.write(f'  Всего для обработки: {total_to_process} игр')
        self.stdout.write(f'  Лимит: {limit}')
        self.stdout.write(f'  Уже обработано: {processed_total}')
        self.stdout.write(f'  Размер пакета: {batch_size}')
        self.stdout.write(f'  Пауза между пакетами: {iteration_pause}с')
        self.stdout.write(f'  Воркеров: {self.workers}')
        self.stdout.write(f'  Задержка: {self.delay}с')
        self.stdout.write(f'  Таймаут: {self.timeout}с')
        self.stdout.write(f'  Dry run: {options["dry_run"]}')
        self.stdout.write(f'  Force: {options["force"]}')
        self.stdout.write(f'  Only found: {options.get("only_found", False)}')
        self.stdout.write(f'  Загружено found: {len(self.found_games)}')
        self.stdout.write(f'  Загружено not found: {len(self.not_found_games)}')
        self.stdout.write(f'  Загружено no description: {len(self.no_description_games)}')

        self.stdout.write(self.style.WARNING(f'\n🚫 Защита:'))
        self.stdout.write(f'  Макс. ошибок: {options["max_consecutive_failures"]}')
        self.stdout.write(f'  Базовая пауза: {options["base_wait"]}с')
        self.stdout.write(f'  Порог ошибок: {options["batch_failure_threshold"] * 100}%')

        self.stdout.write('=' * 60)

    def _print_detailed_stats(self) -> None:
        """Вывод детальной статистики."""
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

        self.stdout.write(self.style.SUCCESS(f'\n📊 СТАТИСТИКА:'))
        self.stdout.write(f'  ✅ Успешно: {self.total_stats["success"]} ({success_pct:.1f}%)')
        self.stdout.write(f'  🔍 Не найдено: {self.total_stats["not_found"]} ({not_found_pct:.1f}%)')
        self.stdout.write(f'  📄 Нет описания: {self.total_stats["no_description"]} ({no_desc_pct:.1f}%)')
        self.stdout.write(f'  💥 Ошибок: {self.total_stats["error"]} ({error_pct:.1f}%)')

        error_details = []
        if self.total_stats.get('error_429', 0) > 0:
            error_details.append(f'429: {self.total_stats["error_429"]}')
        if self.total_stats['error_403'] > 0:
            error_details.append(f'403: {self.total_stats["error_403"]}')
        if self.total_stats['error_timeout'] > 0:
            error_details.append(f'Таймаут: {self.total_stats["error_timeout"]}')
        if self.total_stats['error_other'] > 0:
            error_details.append(f'Другие: {self.total_stats["error_other"]}')

        if error_details:
            self.stdout.write(self.style.WARNING(f'     └─ {", ".join(error_details)}'))

    def _print_final_stats(self) -> None:
        """Вывод финальной статистики."""
        end_time = datetime.now()
        elapsed_time = (end_time - self.start_time).total_seconds() if self.start_time else 0

        total_processed = (self.total_stats['success'] +
                           self.total_stats['not_found'] +
                           self.total_stats['no_description'] +
                           self.total_stats['error'])

        self.log_timeline("END", total_processed)

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 60))
        self.stdout.write(self.style.SUCCESS('📊 ИТОГОВАЯ СТАТИСТИКА'))
        self.stdout.write('=' * 60)

        if total_processed > 0:
            success_pct = self.total_stats['success'] / total_processed * 100
            not_found_pct = self.total_stats['not_found'] / total_processed * 100
            no_desc_pct = self.total_stats['no_description'] / total_processed * 100
            error_pct = self.total_stats['error'] / total_processed * 100
        else:
            success_pct = not_found_pct = no_desc_pct = error_pct = 0

        self.stdout.write(f'  ✅ Успешно: {self.total_stats["success"]} ({success_pct:.1f}%)')
        self.stdout.write(f'  🔍 Не найдено: {self.total_stats["not_found"]} ({not_found_pct:.1f}%)')
        self.stdout.write(f'  📄 Нет описания: {self.total_stats["no_description"]} ({no_desc_pct:.1f}%)')
        self.stdout.write(f'  💥 Ошибок: {self.total_stats["error"]} ({error_pct:.1f}%)')

        error_details = []
        if self.total_stats.get('error_429', 0) > 0:
            error_details.append(f'429: {self.total_stats["error_429"]}')
        if self.total_stats['error_403'] > 0:
            error_details.append(f'403: {self.total_stats["error_403"]}')
        if self.total_stats['error_timeout'] > 0:
            error_details.append(f'Таймаут: {self.total_stats["error_timeout"]}')
        if self.total_stats['error_other'] > 0:
            error_details.append(f'Другие: {self.total_stats["error_other"]}')

        if error_details:
            self.stdout.write(self.style.WARNING(f'     └─ {", ".join(error_details)}'))

        self.stdout.write(f'  ⏸️ Пауз: {self.total_stats["backoff_pauses"]}')
        self.stdout.write(f'  🔄 Итераций: {self.total_stats["iterations"]}')
        self.stdout.write(f'  ⏱️ Время: {self._format_time(elapsed_time)}')
        self.stdout.write(f'  📊 Всего обработано: {total_processed} игр')

        if self.batch_times:
            avg_time = statistics.mean(self.batch_times)
            self.stdout.write(f'  ⚡ Среднее время пакета: {self._format_time(avg_time)}')

        # Добавляем статистику времени загрузки
        self._print_load_time_statistics()

        self.stdout.write('=' * 60)

    def _format_time(self, seconds: float) -> str:
        """Форматирование времени."""
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

    def _update_database(self, games_to_update: List[Game], dry_run: bool) -> None:
        """Обновление базы данных."""
        if games_to_update and not dry_run:
            try:
                with transaction.atomic():
                    Game.objects.bulk_update(games_to_update, ['rawg_description'])
                self.stdout.write(self.style.SUCCESS(f'✅ Обновлено {len(games_to_update)} игр в БД'))
            except Exception as e:
                self.log_debug("Ошибка при обновлении БД", error=e)

    def _get_games_batch_for_offset(self, target_game: Optional[Game], force: bool,
                                    process_not_found: bool, skip_not_found: bool,
                                    process_no_description: bool, skip_no_description: bool,
                                    only_found: bool,
                                    start_offset: int, count: int) -> List[Game]:
        """Получение порции игр для обработки."""
        if target_game:
            return [target_game] if start_offset == 0 else []

        if only_found is True and self.found_games:
            games = list(Game.objects.filter(id__in=self.found_games).order_by('id')
                         [start_offset:start_offset + count])
            return games

        if process_not_found is True and self.not_found_games:
            games = list(Game.objects.filter(id__in=self.not_found_games).order_by('id')
                         [start_offset:start_offset + count])
            return games

        if process_no_description is True and self.no_description_games:
            games = list(Game.objects.filter(id__in=self.no_description_games).order_by('id')
                         [start_offset:start_offset + count])
            return games

        return self.get_games_batch(start_offset, count, force,
                                    skip_not_found, self.not_found_games,
                                    skip_no_description, self.no_description_games)

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
        self.debug = options['debug']
        self.verbose = options['verbose']
        no_restart = options['no_restart']
        output_dir = options['output_dir']
        skip_search = options.get('skip_search', False)

        self.create_backup = not options.get('no_backup', False)

        if options.get('stat_success', 0) > 0:
            self.total_stats['success'] = options['stat_success']
        if options.get('stat_not_found', 0) > 0:
            self.total_stats['not_found'] = options['stat_not_found']
        if options.get('stat_no_description', 0) > 0:
            self.total_stats['no_description'] = options['stat_no_description']
        if options.get('stat_error', 0) > 0:
            self.total_stats['error'] = options['stat_error']
        if options.get('stat_error_403', 0) > 0:
            self.total_stats['error_403'] = options['stat_error_403']
        if options.get('stat_error_429', 0) > 0:
            self.total_stats['error_429'] = options['stat_error_429']
        if options.get('stat_error_timeout', 0) > 0:
            self.total_stats['error_timeout'] = options['stat_error_timeout']
        if options.get('stat_error_other', 0) > 0:
            self.total_stats['error_other'] = options['stat_error_other']
        if options.get('stat_backoff_pauses', 0) > 0:
            self.total_stats['backoff_pauses'] = options['stat_backoff_pauses']
        if options.get('stat_iterations', 0) > 0:
            self.total_stats['iterations'] = options['stat_iterations']

        self.output_file = output_file
        self.found_file = 'steam_found.txt'
        self.not_found_file = 'steam_not_found.txt'
        self.no_description_file = 'steam_no_description.txt'
        self.log_file = 'steam_fetcher_timeline.log'
        self.cache_file = 'steam_cache.json'
        self.stats_file = 'steam_stats.json'
        self.progress_file = 'steam_progress.txt'

        processed_total = options.get('processed', 0)

        self.output_dir = Path(output_dir)
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            self.log_debug("Ошибка создания директории", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Не удалось создать директорию {output_dir}: {e}'))
            return None

        self.full_output_path = self.output_dir / self.output_file
        self.found_file_path = self.output_dir / self.found_file
        self.not_found_file_path = self.output_dir / self.not_found_file
        self.no_description_file_path = self.output_dir / self.no_description_file
        self.log_file_path = self.output_dir / self.log_file
        self.cache_file_path = self.output_dir / self.cache_file
        self.stats_file_path = self.output_dir / self.stats_file
        self.progress_file_path = self.output_dir / self.progress_file

        self.cache_data = self.load_steam_cache(self.cache_file_path)

        if not self.found_file_path.exists():
            try:
                with open(self.found_file_path, 'w', encoding='utf-8') as f:
                    header = f"{'=' * 80}\nSTEAM FOUND GAMES\nCreated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 80}\n\n"
                    f.write(header)
            except Exception as e:
                self.log_debug("Ошибка создания файла найденных", error=e)

        if not self.not_found_file_path.exists():
            try:
                with open(self.not_found_file_path, 'w', encoding='utf-8') as f:
                    header = f"{'=' * 80}\nSTEAM NOT FOUND GAMES\nCreated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 80}\n\n"
                    f.write(header)
            except Exception as e:
                self.log_debug("Ошибка создания файла не найденных", error=e)

        if not self.no_description_file_path.exists():
            try:
                with open(self.no_description_file_path, 'w', encoding='utf-8') as f:
                    header = f"{'=' * 80}\nSTEAM GAMES WITHOUT DESCRIPTION\nCreated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n{'=' * 80}\n\n"
                    f.write(header)
            except Exception as e:
                self.log_debug("Ошибка создания файла без описания", error=e)

        if processed_total == 0:
            try:
                with open(self.log_file_path, 'w', encoding='utf-8') as f:
                    f.write(f"=== ЛОГ ВРЕМЕННОЙ ШКАЛЫ ===\nПрограмма: fetch_steam_descriptions\n{'=' * 50}\n\n")
            except Exception:
                pass

        self.found_games, self.app_id_dict = self.load_found_games()
        self.not_found_games = self.load_not_found_games()
        self.no_description_games = self.load_no_description_games()

        # Инициализируем пустой словарь для App ID (больше не загружаем из API)
        self.steam_app_dict = {}

        # ========== ОПТИМИЗАЦИЯ: отсеиваем игры с уже существующими описаниями ==========
        if self.found_games:
            self.stdout.write(
                self.style.WARNING(f'📊 Проверка наличия описаний у {len(self.found_games)} найденных игр...'))

            games_with_description = set(
                Game.objects.filter(
                    id__in=self.found_games,
                    rawg_description__isnull=False
                ).exclude(rawg_description='').values_list('id', flat=True)
            )

            if games_with_description:
                original_count = len(self.found_games)
                self.found_games = self.found_games - games_with_description

                for game_id in games_with_description:
                    if game_id in self.app_id_dict:
                        del self.app_id_dict[game_id]

                self.stdout.write(self.style.SUCCESS(
                    f'✅ Отсеяно {len(games_with_description)} игр с уже существующими описаниями. '
                    f'Осталось {len(self.found_games)} игр для обработки'
                ))
            else:
                self.stdout.write(self.style.SUCCESS(f'✅ Все {len(self.found_games)} найденных игр не имеют описаний'))
        # =============================================================================

        only_found = options.get('only_found', False)
        process_not_found = options.get('process_not_found', False)
        process_no_description = options.get('process_no_description', False)
        skip_not_found = options.get('skip_not_found', True)
        skip_no_description = options.get('skip_no_description', True)

        if not game_name:
            base_queryset = Game.objects.filter(platforms=pc)

            if not force:
                base_queryset = base_queryset.filter(
                    Q(rawg_description__isnull=True) | Q(rawg_description='')
                )

            if only_found and self.found_games:
                base_queryset = Game.objects.filter(id__in=self.found_games)
                self.stdout.write(self.style.WARNING(f'📊 Только {len(self.found_games)} найденных игр'))
            elif skip_not_found and self.not_found_games:
                base_queryset = base_queryset.exclude(id__in=self.not_found_games)
                self.stdout.write(self.style.WARNING(f'📊 Исключено {len(self.not_found_games)} не найденных'))

            if skip_no_description and self.no_description_games:
                base_queryset = base_queryset.exclude(id__in=self.no_description_games)
                self.stdout.write(self.style.WARNING(f'📊 Исключено {len(self.no_description_games)} без описания'))

            if process_not_found and self.not_found_games:
                base_queryset = Game.objects.filter(id__in=self.not_found_games)
                self.stdout.write(self.style.WARNING(f'📊 Только {len(self.not_found_games)} не найденных'))

            if process_no_description and self.no_description_games:
                base_queryset = Game.objects.filter(id__in=self.no_description_games)
                self.stdout.write(self.style.WARNING(f'📊 Только {len(self.no_description_games)} без описания'))

            total_available = base_queryset.count()

            self.stdout.write(self.style.SUCCESS(f'📊 Всего игр на PC: {Game.objects.filter(platforms=pc).count()}'))
            self.stdout.write(self.style.SUCCESS(f'📊 Доступно для обработки: {total_available}'))

            if limit_from_args is None:
                total_limit = total_available
                self.stdout.write(self.style.WARNING(f'🔄 Обрабатываем все {total_limit} игр'))
            else:
                total_limit = min(limit_from_args, total_available)
                self.stdout.write(self.style.WARNING(f'📊 Лимит = {total_limit} из {total_available}'))

            target_game = None
        else:
            result = self._find_specific_game(game_name, pc)
            if not result:
                return None
            target_game, total_limit, batch_size, no_restart = result
            self.batch_size = batch_size
            total_available = 1
            processed_total = 0
            only_found = False
            process_not_found = False
            process_no_description = False

        self.current_offset = options['offset']

        if options.get('reset_progress', False):
            self.reset_progress()
            self.current_offset = 0

        progress_offset = self.load_progress()
        if progress_offset > self.current_offset:
            self.current_offset = progress_offset
            self.stdout.write(self.style.WARNING(f'📊 Восстановлен прогресс: offset {self.current_offset}'))

        self.stdout.write(self.style.SUCCESS(f'📊 Будет обработано: {total_limit} игр'))
        self.stdout.write(self.style.SUCCESS(f'📊 Размер пакета: {batch_size} игр'))
        self.stdout.write(self.style.SUCCESS(f'📊 Итераций: {(total_limit + batch_size - 1) // batch_size}'))

        return (total_limit, total_available, target_game, processed_total,
                batch_size, iteration_pause, output_file, output_dir,
                dry_run, force, skip_search, no_restart,
                process_not_found, skip_not_found,
                process_no_description, skip_no_description,
                only_found)

    def _main_processing_loop(self, target_game: Optional[Game], total_limit: int,
                              batch_size: int, iteration_pause: int,
                              skip_search: bool, output_file: str, dry_run: bool,
                              force: bool, no_restart: bool, processed_total: int,
                              options: Dict,
                              process_not_found: bool, skip_not_found: bool,
                              process_no_description: bool, skip_no_description: bool,
                              only_found: bool,
                              csv_descriptions: Dict[str, str] = None) -> Tuple[int, float]:
        """Основной цикл обработки игр."""
        iteration = 0
        games_per_second = 0
        self.loop_start_time = time.time()

        self.processed_total = processed_total
        self.only_found = only_found
        self.force = force
        self.dry_run = dry_run

        games_processed_this_session = 0

        self.stdout.write(self.style.SUCCESS(f'\n🚀 НАЧАЛО ОБРАБОТКИ {total_limit} ИГР'))

        try:
            while not self.interrupted:
                if games_processed_this_session >= total_limit:
                    self.stdout.write(self.style.SUCCESS(f'\n✅ Достигнут лимит {total_limit} игр'))
                    break

                remaining_limit = total_limit - games_processed_this_session
                current_batch_size = min(batch_size, remaining_limit)

                games_to_process = self._get_games_batch_for_offset(
                    target_game, force,
                    process_not_found, skip_not_found,
                    process_no_description, skip_no_description,
                    only_found,
                    self.current_offset,
                    current_batch_size
                )

                if not games_to_process:
                    self.stdout.write(self.style.WARNING('⚠️ Нет больше игр для обработки'))
                    break

                iteration += 1
                with self.stats_lock:
                    self.total_stats['iterations'] = iteration

                iteration_start_time = time.time()

                if self.interrupted:
                    break

                self.stdout.write(self.style.SUCCESS(f'\n🔄 ИТЕРАЦИЯ {iteration}'))
                self.stdout.write(f'  📍 Смещение: {self.current_offset}')
                self.stdout.write(f'  📍 Игр: {len(games_to_process)}')

                if self.rate_limiter and self.rate_limiter.should_backoff():
                    self.check_rate_limit()
                    if self.interrupted:
                        break

                if self.interrupted:
                    break

                batch_stats, games_to_update = self._process_games_batch(
                    games_to_process, skip_search, self.timeout, self.delay,
                    output_file, dry_run,
                    iteration == 1 and games_processed_this_session == 0,
                    csv_descriptions
                )

                if self.interrupted:
                    break

                games_processed_this_batch = len(games_to_process)
                games_processed_this_session += games_processed_this_batch
                self.processed_total += games_processed_this_batch

                self.current_offset += games_processed_this_batch
                self.save_progress(self.current_offset)

                iteration_time = time.time() - iteration_start_time
                self.batch_times.append(iteration_time)
                if len(self.batch_times) > 20:
                    self.batch_times.pop(0)

                current_games_per_second = games_processed_this_batch / max(iteration_time, 0.1)
                if games_per_second == 0:
                    games_per_second = current_games_per_second
                else:
                    games_per_second = games_per_second * 0.7 + current_games_per_second * 0.3

                self._update_database(games_to_update, dry_run)

                if self.not_found_buffer:
                    self.save_not_found_buffer()
                if self.no_description_buffer:
                    self.save_no_description_buffer()
                if self.found_buffer:
                    self.save_found_buffer()

                if self.cache_file_path:
                    self.save_steam_cache(self.cache_file_path)

                if self.interrupted:
                    break

                current_processed = games_processed_this_session
                progress = min(current_processed / total_limit * 100, 100) if total_limit > 0 else 0

                elapsed = time.time() - self.loop_start_time
                elapsed_str = self._format_time(elapsed)

                if self.batch_times:
                    avg_batch_time = statistics.mean(self.batch_times)
                    remaining_batches = ((total_limit - current_processed) + batch_size - 1) // batch_size
                    eta_seconds = remaining_batches * avg_batch_time
                    eta_str = self._format_time(eta_seconds)
                else:
                    eta_str = "расчет..."

                bar_length = 40
                filled = int(bar_length * progress / 100)
                bar = '█' * filled + '░' * (bar_length - filled)

                self.stdout.write(self.style.SUCCESS(f'\n📊 ПРОГРЕСС: [{bar}] {progress:.1f}%'))
                self.stdout.write(f'📊 Обработано: {current_processed}/{total_limit} игр')
                self.stdout.write(
                    f'⏱️ Прошло: {elapsed_str} | ⏳ Осталось: {eta_str} | ⚡ Скорость: {games_per_second:.1f} игр/с')

                self._print_detailed_stats()
                self._save_stats_to_file()

                if iteration_pause > 0 and not self.interrupted:
                    for i in range(iteration_pause, 0, -1):
                        if self.interrupted:
                            break
                        if i % 10 == 0 or i <= 5:
                            self.stdout.write(f'\r   Пауза {i}с...', end='')
                        time.sleep(1)
                    if not self.interrupted:
                        self.stdout.write('\r   Продолжение...      ')

        except KeyboardInterrupt:
            self.interrupted = True
            self.stdout.write(self.style.ERROR('\n\n⚠️ Прерывание (Ctrl+C)'))
            self._save_stats_to_file()

        finally:
            self.stdout.write(self.style.WARNING('\n🔄 Завершение...'))

            if self.descriptions_buffer:
                self.save_buffer(output_file, is_first=False)
            if self.not_found_buffer:
                self.save_not_found_buffer()
            if self.no_description_buffer:
                self.save_no_description_buffer()
            if self.found_buffer:
                self.save_found_buffer()

            if self.cache_file_path:
                self.save_steam_cache(self.cache_file_path)

            self._save_stats_to_file()
            self.save_progress(self.current_offset)

            if self.interrupted:
                self.stdout.write(
                    self.style.WARNING(f'  💾 Прогресс сохранен: {games_processed_this_session}/{total_limit} игр'))

        return games_processed_this_session, games_per_second

    def _process_games_batch(self, games: List[Game], skip_search: bool,
                             timeout: float, delay: float, output_file: str,
                             dry_run: bool, is_first: bool,
                             csv_descriptions: Dict[str, str] = None) -> Tuple[Dict, List[Game]]:
        """Обработка батча игр."""
        batch_stats = {
            'success': 0,
            'not_found': 0,
            'no_description': 0,
            'error': 0
        }

        if self.interrupted:
            self.stdout.write(self.style.WARNING('  ⚠️ Обработка отменена'))
            return batch_stats, []

        games_to_update = self.process_batch(
            games, skip_search, timeout, delay,
            output_file, dry_run, batch_stats, is_first, csv_descriptions
        )

        return batch_stats, games_to_update

    def handle(self, *args: Any, **options: Any) -> None:
        """Основной метод выполнения."""
        self.start_time = datetime.now()

        signal.signal(signal.SIGINT, self.signal_handler)

        self.rate_limiter = SteamRateLimiter(
            max_consecutive_failures=options['max_consecutive_failures'],
            base_wait_time=options['base_wait'],
            max_wait_time=options['max_wait']
        )
        self.batch_failure_threshold = options['batch_failure_threshold']

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('🚀 STEAM DESCRIPTIONS FETCHER'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        self.stdout.write(self.style.WARNING('📋 Шаг 1/8: Поиск платформы PC...'))
        pc = self.get_pc_platform()
        if not pc:
            self.stdout.write(self.style.ERROR('❌ Платформа PC не найдена'))
            return
        self.stdout.write(self.style.SUCCESS(f'✅ Платформа: {pc.name} (ID: {pc.id})'))

        self.output_dir = Path(options['output_dir'])

        if options['clear_logs']:
            self.stdout.write(self.style.WARNING('📋 Шаг 2/8: Очистка логов...'))
            self._clear_logs()
            return

        self.stdout.write(self.style.WARNING('📋 Шаг 2/8: Инициализация параметров...'))
        params = self._initialize_parameters(options, pc)
        if not params:
            return

        (limit, total_to_process, target_game, processed_total,
         batch_size, iteration_pause, output_file, output_dir,
         dry_run, force, skip_search, no_restart,
         process_not_found, skip_not_found,
         process_no_description, skip_no_description,
         only_found) = params

        if processed_total == 0:
            self.stdout.write(self.style.WARNING('📋 Шаг 3/8: Создание статистики...'))
            self._init_stats_file()

            self.stdout.write(self.style.WARNING('📋 Шаг 4/8: Загрузка описаний из кэш-файла...'))
            loaded_from_cache = self.load_descriptions_from_cache()
            if loaded_from_cache > 0:
                self.stdout.write(self.style.SUCCESS(f'✅ Загружено {loaded_from_cache} описаний из кэша'))

            self.stdout.write(self.style.WARNING('📋 Шаг 5/8: Загрузка описаний из CSV-файла...'))
            csv_descriptions = self.load_descriptions_from_csv()

            if csv_descriptions:
                if only_found and self.found_games:
                    self.stdout.write(self.style.WARNING(
                        f'📊 Загрузка CSV-описаний только для {len(self.found_games)} найденных игр...'))
                    filtered_csv = {}
                    for game_id in self.found_games:
                        game = Game.objects.filter(id=game_id).first()
                        if game and game.name.lower() in csv_descriptions:
                            filtered_csv[game.name.lower()] = csv_descriptions[game.name.lower()]
                    loaded_from_csv = self.load_descriptions_from_csv_to_db(filtered_csv)
                else:
                    loaded_from_csv = self.load_descriptions_from_csv_to_db(csv_descriptions)

                if loaded_from_csv > 0:
                    self.stdout.write(self.style.SUCCESS(f'✅ Загружено {loaded_from_csv} описаний из CSV'))

                self.csv_descriptions = csv_descriptions
            else:
                self.csv_descriptions = {}

            # Дополнительное отсеивание после загрузки
            self.stdout.write(self.style.WARNING('📋 Шаг 5.1/8: Повторная проверка и отсеивание игр с описаниями...'))

            if self.found_games:
                games_with_description = set(
                    Game.objects.filter(
                        id__in=self.found_games,
                        rawg_description__isnull=False
                    ).exclude(rawg_description='').values_list('id', flat=True)
                )

                if games_with_description:
                    self.found_games = self.found_games - games_with_description

                    for game_id in games_with_description:
                        if game_id in self.app_id_dict:
                            del self.app_id_dict[game_id]

                    self.stdout.write(self.style.SUCCESS(
                        f'✅ Отсеяно {len(games_with_description)} игр с описаниями. '
                        f'Осталось {len(self.found_games)} игр для обработки'
                    ))

            if self.not_found_games:
                games_with_description = set(
                    Game.objects.filter(
                        id__in=self.not_found_games,
                        rawg_description__isnull=False
                    ).exclude(rawg_description='').values_list('id', flat=True)
                )

                if games_with_description:
                    self.not_found_games = self.not_found_games - games_with_description
                    self.stdout.write(self.style.SUCCESS(
                        f'✅ Отсеяно {len(games_with_description)} не найденных игр с описаниями'
                    ))

            if self.no_description_games:
                games_with_description = set(
                    Game.objects.filter(
                        id__in=self.no_description_games,
                        rawg_description__isnull=False
                    ).exclude(rawg_description='').values_list('id', flat=True)
                )

                if games_with_description:
                    self.no_description_games = self.no_description_games - games_with_description
                    self.stdout.write(self.style.SUCCESS(
                        f'✅ Отсеяно {len(games_with_description)} игр без описания, которые теперь имеют описание'
                    ))

            # Пересчитываем total_to_process для only_found режима
            if only_found and self.found_games:
                total_to_process = len(self.found_games)
                limit = total_to_process
                self.stdout.write(self.style.SUCCESS(
                    f'📊 Пересчитано количество игр для обработки: {total_to_process}'
                ))

        if options['clear_descriptions']:
            self.stdout.write(self.style.WARNING('📋 Шаг 6/8: Очистка описаний...'))
            self._clear_descriptions(pc)
            if not force and not only_found:
                return

        if processed_total == 0:
            self.log_timeline("START")

        self.stdout.write(self.style.WARNING('📋 Шаг 7/8: Подготовка...'))
        self._print_startup_info(limit, total_to_process, processed_total,
                                 batch_size, iteration_pause, options,
                                 process_not_found, skip_not_found,
                                 process_no_description, skip_no_description)

        self.stdout.write(self.style.WARNING('📋 Шаг 8/8: Запуск обработки...'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        processed_total, games_per_second = self._main_processing_loop(
            target_game, limit, batch_size, iteration_pause,
            skip_search, output_file, dry_run,
            force, no_restart, processed_total, options,
            process_not_found, skip_not_found,
            process_no_description, skip_no_description,
            only_found,
            getattr(self, 'csv_descriptions', {})
        )

        self.stdout.write(self.style.WARNING('💾 Сохранение финальных данных...'))
        if self.cache_file_path:
            self.save_steam_cache(self.cache_file_path)

        self._save_stats_to_file()

        self._print_final_stats()

        if self.interrupted:
            self.stdout.write(self.style.WARNING('\n⚠️ Работа прервана пользователем. Завершение.'))
            return

        if not no_restart and not self.interrupted and limit is not None and processed_total < limit:
            self.stdout.write(self.style.WARNING(f'\n🔄 Перезапуск для следующего оффсета...'))
            new_offset = self.current_offset
            self.stdout.write(self.style.WARNING(f'📊 Новый оффсет: {new_offset}'))

            import sys
            import subprocess

            script_path = sys.argv[0]
            new_args = []

            for arg in sys.argv[1:]:
                if arg.startswith('--offset'):
                    continue
                if arg.startswith('--processed'):
                    continue
                if arg.startswith('--stat-success'):
                    continue
                if arg.startswith('--stat-not-found'):
                    continue
                if arg.startswith('--stat-no-description'):
                    continue
                if arg.startswith('--stat-error'):
                    continue
                if arg.startswith('--stat-error-403'):
                    continue
                if arg.startswith('--stat-error-429'):
                    continue
                if arg.startswith('--stat-error-timeout'):
                    continue
                if arg.startswith('--stat-error-other'):
                    continue
                if arg.startswith('--stat-backoff-pauses'):
                    continue
                if arg.startswith('--stat-iterations'):
                    continue
                new_args.append(arg)

            new_args.append(f'--offset={new_offset}')
            new_args.append(f'--processed={processed_total}')
            new_args.append(f'--stat-success={self.total_stats["success"]}')
            new_args.append(f'--stat-not-found={self.total_stats["not_found"]}')
            new_args.append(f'--stat-no-description={self.total_stats["no_description"]}')
            new_args.append(f'--stat-error={self.total_stats["error"]}')
            new_args.append(f'--stat-error-403={self.total_stats["error_403"]}')
            new_args.append(f'--stat-error-429={self.total_stats["error_429"]}')
            new_args.append(f'--stat-error-timeout={self.total_stats["error_timeout"]}')
            new_args.append(f'--stat-error-other={self.total_stats["error_other"]}')
            new_args.append(f'--stat-backoff-pauses={self.total_stats["backoff_pauses"]}')
            new_args.append(f'--stat-iterations={self.total_stats["iterations"]}')

            if '--no-restart' not in new_args:
                new_args.append('--no-restart')

            self.stdout.write(self.style.WARNING(f'🔄 Выполняется перезапуск...'))
            try:
                subprocess.run([sys.executable, script_path] + new_args)
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Ошибка перезапуска: {e}'))
