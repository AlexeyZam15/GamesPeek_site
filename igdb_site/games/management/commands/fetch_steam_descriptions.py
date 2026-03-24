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
        self.consecutive_429 = 0  # Счетчик для ошибок 429
        self.max_consecutive_failures = max_consecutive_failures
        self.base_wait_time = base_wait_time
        self.max_wait_time = max_wait_time
        self.last_failure_time = None
        self.total_failures = 0
        self.total_403 = 0
        self.total_429 = 0  # Общий счетчик 429 для статистики
        self.wait_history = []
        self.lock = Lock()
        self.in_backoff = False
        self.backoff_until = None
        self.pause_start_time = None
        self.last_429_time = None  # Время последней ошибки 429

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

                # Специальная обработка для разных типов ошибок
                if error_type == "403":
                    self.consecutive_403 += 1
                    self.total_403 += 1
                elif error_type == "429":
                    self.consecutive_429 += 1
                    self.total_429 += 1
                    self.last_429_time = datetime.now()

                    # 429 требует немедленной паузы при превышении порога
                    if self.consecutive_429 >= 3:  # 3 ошибки 429 = пауза
                        wait_time = self.base_wait_time  # Используем базовое время
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

                # Стандартная проверка для остальных ошибок
                if self.consecutive_failures >= self.max_consecutive_failures:
                    return self._calculate_wait_time()

            finally:
                self.lock.release()
        except Exception:
            pass

        return 0

    def record_success(self):
        """Сброс счетчика при успешном запросе, но НЕ для 429."""
        try:
            acquired = self.lock.acquire(timeout=2)
            if not acquired:
                return

            try:
                # Сбрасываем обычные ошибки, но оставляем счетчик 429
                if self.consecutive_failures > 0:
                    self.consecutive_failures = 0
                    self.consecutive_403 = 0
                    self.in_backoff = False
                    self.backoff_until = None
                    self.pause_start_time = None

                # НЕ сбрасываем consecutive_429!
                # Они сбрасываются только после успешной паузы или вручную

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
            help='Размер итерации (по умолчанию: 40)'
        )

        parser.add_argument(
            '--iteration-pause',
            type=int,
            default=0,
            help='Пауза между итерациями в секундах (по умолчанию: 0)'
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
            default=4.0,
            help='Таймаут запроса в секундах (по умолчанию: 4.0)'
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
            help='Максимальное количество ЛЮБЫХ ошибок подряд перед паузой (по умолчанию: 15)'
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
            '--no-backup',
            action='store_true',
            help='Не создавать резервные копии файлов'
        )

        # Аргументы для передачи статистики между процессами
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
        self.no_description_file_path = None
        self.found_file_path = None
        self.not_found_file_path = None
        self.log_file_path = None
        self.cache_file_path = None
        self.stats_file_path = None
        self.no_description_buffer = []
        self.no_description_games = set()
        self.found_games = set()
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
        self.timeout = 5
        self.output_file = 'steam_descriptions_all.txt'
        self.start_time = None
        self.processed_before_pause = 0
        self._pause_active = False
        self.processed_total = 0
        self.limit = 0
        self.only_found = False
        self.force = False
        self.dry_run = False
        self.create_backup = True  # По умолчанию создаем бэкапы

    def _save_description_to_db(self, game_name: str, description_lines: list) -> bool:
        """
        Сохранение описания в базу данных по названию игры.
        Возвращает True если успешно, False если игра не найдена.
        """
        try:
            description = '\n'.join(description_lines).strip()
            if not description:
                return False

            # Ищем игру по названию
            game = Game.objects.filter(name__iexact=game_name).first()
            if not game:
                # Пробуем частичное совпадение
                game = Game.objects.filter(name__icontains=game_name).first()

            if game:
                if not game.rawg_description or game.rawg_description != description:
                    game.rawg_description = description
                    game.save(update_fields=['rawg_description'])
                    return True
            return False

        except Exception as e:
            self.log_debug(f"Ошибка сохранения описания для {game_name}", error=e)
            return False

    def load_descriptions_from_cache(self) -> int:
        """
        Загрузка описаний из файла steam_descriptions_all.txt в базу данных.
        Загружает только игры, у которых еще нет описания.
        Возвращает количество загруженных описаний.
        """
        if not self.full_output_path or not self.full_output_path.exists():
            self.stdout.write(self.style.WARNING('📂 Кэш-файл описаний не найден, пропускаем загрузку'))
            return 0

        self.stdout.write(self.style.WARNING('📂 Загрузка описаний из кэш-файла...'))

        # Сначала собираем все игры из кэш-файла с их описаниями
        games_from_cache = []  # список кортежей (game_name, description)
        current_game = None
        current_description = []
        reading_description = False

        try:
            with open(self.full_output_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.rstrip()

                    # Начало записи игры
                    if line.startswith('Game: '):
                        if current_game and current_description:
                            description_text = '\n'.join(current_description).strip()
                            if description_text:
                                games_from_cache.append((current_game, description_text))

                        game_name = line.replace('Game: ', '').strip()
                        current_game = game_name
                        current_description = []
                        reading_description = False

                    # Начало описания
                    elif line == 'DESCRIPTION:' and current_game:
                        reading_description = True

                    # Читаем описание
                    elif reading_description and line != '-' * 80:
                        if line.strip():
                            current_description.append(line)

                    # Конец записи
                    elif line == '-' * 80:
                        if current_game and current_description:
                            description_text = '\n'.join(current_description).strip()
                            if description_text:
                                games_from_cache.append((current_game, description_text))
                        current_game = None
                        current_description = []
                        reading_description = False

                # Сохраняем последнюю игру
                if current_game and current_description:
                    description_text = '\n'.join(current_description).strip()
                    if description_text:
                        games_from_cache.append((current_game, description_text))

            if not games_from_cache:
                self.stdout.write(self.style.WARNING('📂 Кэш-файл не содержит описаний'))
                return 0

            self.stdout.write(self.style.SUCCESS(f'📊 Найдено {len(games_from_cache)} игр в кэш-файле'))

            # Получаем ID игр, у которых уже есть описание в БД
            game_names = [game[0] for game in games_from_cache]

            self.stdout.write('   🔍 Проверка наличия описаний в БД...')

            # Находим игры, у которых уже есть описание
            games_with_description = set(
                Game.objects.filter(
                    name__in=game_names,
                    rawg_description__isnull=False
                ).exclude(rawg_description='').values_list('name', flat=True)
            )

            games_to_load = [g for g in games_from_cache if g[0] not in games_with_description]

            if not games_to_load:
                self.stdout.write(self.style.SUCCESS('✅ Все описания из кэша уже есть в БД'))
                return 0

            self.stdout.write(self.style.WARNING(
                f'📊 Будет загружено {len(games_to_load)} новых описаний (пропущено {len(games_with_description)} уже существующих)'))

            # Загружаем только те, у которых нет описания
            loaded_count = 0
            processed = 0
            last_percent = -1

            for game_name, description in games_to_load:
                processed += 1
                percent = int(processed / len(games_to_load) * 100)
                if percent != last_percent and (percent % 10 == 0 or percent == 100):
                    last_percent = percent
                    self.stdout.write(f'   📥 Загружено {processed}/{len(games_to_load)} игр ({percent}%)')

                # Сохраняем описание (внутри метода уже есть проверка на наличие)
                if self._save_description_to_db(game_name, description.split('\n')):
                    loaded_count += 1

            if loaded_count > 0:
                self.stdout.write(self.style.SUCCESS(f'📂 Загружено {loaded_count} новых описаний из кэш-файла'))

        except Exception as e:
            self.log_debug("Ошибка загрузки описаний из кэша", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка загрузки из кэша: {e}'))

        return loaded_count

    def add_to_found(self, game: Game, app_id: int):
        """Добавление игры в список успешно найденных.
        Формат: Game ID: 12345 - Название игры (Steam App ID: 67890)
        """
        if game.id in self.found_games:
            return

        game_info = f"Game ID: {game.id} - {game.name} (Steam App ID: {app_id})"
        self.found_buffer.append(game_info)
        self.found_games.add(game.id)

        # Обновляем кэш
        self.update_steam_cache(game.id, True, app_id, game.name)

        self.save_found_buffer()
        self.log_debug(f"Добавлена найденная игра: {game.name} (ID: {game.id}, Steam ID: {app_id})")

    def _init_stats_file(self):
        """Инициализация файла статистики при запуске команды."""
        try:
            import json
            from datetime import datetime

            # Создаем файл с начальной статистикой
            self.stats_file_path = self.output_dir / 'steam_fetcher_stats.json'

            self.stdout.write(self.style.WARNING(f'📊 Создание файла статистики: {self.stats_file_path}...'))

            initial_stats = {
                'command': 'fetch_steam_descriptions',
                'started_at': datetime.now().isoformat(),
                'last_updated': datetime.now().isoformat(),
                'processed_total': 0,
                'limit': None,
                'total_stats': self.total_stats,
                'options': {}
            }

            with open(self.stats_file_path, 'w', encoding='utf-8') as f:
                json.dump(initial_stats, f, ensure_ascii=False, indent=2, default=str)

                # Получаем размер записанных данных
                bytes_written = f.tell()
                self.stdout.write(self.style.SUCCESS(f'   ✅ Записано {bytes_written:,} байт'))

            self.stdout.write(self.style.SUCCESS(f'📊 Файл статистики создан: {self.stats_file_path}'))

        except Exception as e:
            self.log_debug("Ошибка при инициализации файла статистики", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка создания файла статистики: {e}'))

    def _save_stats_to_file(self):
        """Сохранение текущей статистики в файл (перезапись)."""
        try:
            import json
            from datetime import datetime

            if not self.stats_file_path:
                return

            stats_data = {
                'command': 'fetch_steam_descriptions',
                'started_at': self.start_time.isoformat() if self.start_time else None,
                'last_updated': datetime.now().isoformat(),
                'processed_total': self.processed_total if hasattr(self, 'processed_total') else 0,
                'limit': self.limit if hasattr(self, 'limit') else None,
                'total_stats': self.total_stats,
                'options': {
                    'only_found': getattr(self, 'only_found', False),
                    'force': getattr(self, 'force', False),
                    'dry_run': getattr(self, 'dry_run', False),
                    'batch_size': self.batch_size,
                    'workers': self.workers,
                    'delay': self.delay,
                    'timeout': self.timeout
                }
            }

            with open(self.stats_file_path, 'w', encoding='utf-8') as f:
                json.dump(stats_data, f, ensure_ascii=False, indent=2, default=str)

        except Exception as e:
            self.log_debug("Ошибка при сохранении статистики", error=e)

    def update_steam_cache(self, game_id: int, found: bool, app_id: int = None, game_name: str = None):
        """
        Обновление записи в кэше Steam.
        """
        self.cache_data[game_id] = {
            'found': found,
            'app_id': app_id,
            'checked_at': datetime.now().isoformat(),
            'game_name': game_name or 'Unknown',
            'has_description': False  # Будет обновлено при получении описания
        }

    def save_steam_cache(self, cache_file_path: Path):
        """
        Сохранение общего кэша Steam в файл.
        """
        try:
            import json
            # Создаем резервную копию только если create_backup = True
            if self.create_backup and cache_file_path.exists():
                backup_path = cache_file_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
                import shutil
                shutil.copy2(cache_file_path, backup_path)
                self.log_debug(f"Создан бэкап кэша: {backup_path}")
                self.create_backup = False  # После создания бэкапа отключаем для следующих итераций

            with open(cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2, default=str)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка сохранения кэша: {e}'))

    def load_steam_cache(self, cache_file_path: Path) -> Dict[int, Dict[str, any]]:
        """
        Загрузка общего кэша Steam из файла.
        Использует тот же кэш, что и check_steam_games.
        """
        cache = {}

        if not cache_file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 Кэш Steam не существует: {cache_file_path}'))
            return cache

        self.stdout.write(self.style.WARNING(f'📂 Загрузка кэша Steam из {cache_file_path}...'))

        try:
            import json

            # Получаем размер файла для прогресса
            file_size = cache_file_path.stat().st_size
            self.stdout.write(f'   📦 Размер файла: {file_size:,} байт ({file_size / 1024:.1f} КБ)')

            with open(cache_file_path, 'r', encoding='utf-8') as f:
                # Показываем, что читаем
                self.stdout.write('   🔄 Чтение файла...')
                cache = json.load(f)
                # Преобразуем ключи из строк в числа
                self.stdout.write('   🔄 Преобразование данных...')
                cache = {int(k): v for k, v in cache.items()}

            self.stdout.write(self.style.SUCCESS(f'📂 Загружен кэш Steam: {len(cache)} игр'))
        except json.JSONDecodeError as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка парсинга JSON: {e}'))
            self.stdout.write(self.style.WARNING('⚠️ Кэш поврежден, будет создан новый'))
            cache = {}
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка загрузки кэша: {e}'))
            cache = {}

        return cache

    def _clear_logs(self) -> None:
        """Очистка папки с логами и файлами с созданием бэкапа."""
        self.stdout.write(self.style.WARNING('⚠️  НАЧАЛО ОЧИСТКИ ПАПКИ С ЛОГАМИ'))
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

            self.stdout.write(self.style.WARNING(f'📊 Найдено файлов в папке: {file_count}'))

            self.stdout.write(self.style.WARNING(f'📋 Файлы:'))
            for i, file in enumerate(files[:10], 1):
                size = file.stat().st_size if file.is_file() else 0
                self.stdout.write(f'    {i}. {file.name} ({size} байт)')

            if file_count > 10:
                self.stdout.write(f'    ... и еще {file_count - 10} файлов')

            response = input(f'⚠️  Вы уверены, что хотите удалить ВСЮ папку {output_dir}? (yes/no): ')

            if response.lower() != 'yes':
                self.stdout.write(self.style.WARNING('❌ Очистка отменена'))
                return

            # Создаем бэкап всей папки
            backup_dir = output_dir.parent / f"{output_dir.name}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            import shutil
            shutil.copytree(output_dir, backup_dir)
            self.stdout.write(self.style.WARNING(f'📋 Создана резервная копия всей папки: {backup_dir}'))

            shutil.rmtree(output_dir)

            self.stdout.write(self.style.SUCCESS(f'✅ Папка удалена: {output_dir}'))

            output_dir.mkdir(parents=True, exist_ok=True)
            self.stdout.write(self.style.SUCCESS(f'✅ Папка создана заново: {output_dir}'))

            self.stdout.write(self.style.SUCCESS('=' * 60))
            self.stdout.write(self.style.SUCCESS('✅ ОЧИСТКА ЛОГОВ ЗАВЕРШЕНА'))

        except Exception as e:
            self.log_debug("Ошибка при очистке логов", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка при очистке: {e}'))

    def load_found_games(self) -> tuple:
        """
        Загрузка списка успешно найденных игр из файла.
        Возвращает: (found_set, app_id_dict)
        """
        found_set = set()
        app_id_dict = {}
        file_path = self.found_file_path

        if file_path and file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 Загрузка найденных игр из {file_path}...'))

            try:
                # Сначала подсчитываем количество строк для прогресса
                with open(file_path, 'r', encoding='utf-8') as f:
                    total_lines = sum(1 for _ in f)

                processed = 0
                last_percent = -1

                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()

                        # Обновляем прогресс каждые 100 строк или 5%
                        processed += 1
                        if total_lines > 0:
                            percent = int(processed / total_lines * 100)
                            if percent != last_percent and percent % 5 == 0:
                                last_percent = percent
                                self.stdout.write(f'   📖 Загружено {processed}/{total_lines} строк ({percent}%)')

                        if not line:
                            continue
                        if line.startswith('=') or line.startswith('STEAM FOUND GAMES') or line.startswith('Created:'):
                            continue

                        if 'Game ID:' in line:
                            try:
                                # Парсим формат: Game ID: 12345 - Название игры (Steam App ID: 67890)
                                game_part = line.split('Game ID:')[1].strip()

                                if '-' in game_part:
                                    game_id_str = game_part.split('-')[0].strip()
                                    game_id = int(game_id_str)
                                    found_set.add(game_id)

                                    # Парсим Steam App ID
                                    if 'Steam App ID:' in line:
                                        app_id_part = line.split('Steam App ID:')[1].strip()
                                        if ')' in app_id_part:
                                            app_id_part = app_id_part.split(')')[0]
                                        if app_id_part.isdigit():
                                            app_id_dict[game_id] = int(app_id_part)

                                else:
                                    # Старый формат без дефиса
                                    game_id_str = game_part.split(' ')[0].strip()
                                    game_id = int(game_id_str)
                                    found_set.add(game_id)

                            except (ValueError, IndexError) as e:
                                if line_num <= 10:
                                    self.log_debug(f"Ошибка парсинга строки {line_num}: {line[:100]}", error=e)
                                continue

                self.stdout.write(self.style.SUCCESS(f'📂 Загружено {len(found_set)} найденных игр из {file_path}'))
                if app_id_dict:
                    self.stdout.write(self.style.SUCCESS(f'📂 Загружено {len(app_id_dict)} Steam App ID из {file_path}'))
            except Exception as e:
                self.log_debug(f"Ошибка при загрузке файла найденных игр", error=e)
        else:
            self.stdout.write(self.style.WARNING(f'📂 Файл найденных игр не существует: {file_path}'))

        return found_set, app_id_dict

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
                # Создаем бэкап только если create_backup = True
                if self.create_backup and file_path.exists() and file_path.stat().st_size > 0:
                    backup_path = file_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
                    import shutil
                    shutil.copy2(file_path, backup_path)
                    self.log_debug(f"Создан бэкап файла найденных: {backup_path}")
                    self.create_backup = False  # После создания бэкапа отключаем для следующих итераций

                with open(file_path, 'a', encoding='utf-8') as f:
                    for game_info in self.found_buffer:
                        f.write(game_info)
                        f.write("\n")

                self.log_debug(f"Добавлено {len(self.found_buffer)} игр в файл найденных")
                self.found_buffer = []

            except IOError as e:
                self.log_debug("Ошибка записи файла найденных игр", error=e)

    def _backup_file(self, file_path: Path) -> Path:
        """
        Создание резервной копии файла.
        Возвращает путь к созданной копии.
        """
        if not file_path.exists():
            return None

        backup_path = file_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}{file_path.suffix}')

        try:
            import shutil
            shutil.copy2(file_path, backup_path)
            self.stdout.write(self.style.WARNING(f'📋 Создана резервная копия: {backup_path}'))
            return backup_path
        except Exception as e:
            self.log_debug(f"Ошибка создания резервной копии {file_path}", error=e)
            return None

    def add_to_not_found(self, game: Game, reason: str = "not_found"):
        """Добавление игры в список не найденных.
        Формат: Game ID: 12345 - Название игры (Steam App ID: None) - не найдено в Steam
        """
        if game.id in self.not_found_games:
            return

        game_info = f"Game ID: {game.id} - {game.name} (Steam App ID: None) - не найдено в Steam"
        self.not_found_buffer.append(game_info)
        self.not_found_games.add(game.id)

        # Обновляем кэш
        self.update_steam_cache(game.id, False, None, game.name)

        self.save_not_found_buffer()
        self.log_debug(f"Добавлена не найденная игра: {game.name} (ID: {game.id}) - {reason}")

    def _clear_descriptions(self, pc: Platform) -> None:
        """Очистка всех существующих rawg_description с созданием бэкапа и очисткой кэш-файла."""
        self.stdout.write(self.style.WARNING('⚠️  НАЧАЛО ОЧИСТКИ ВСЕХ ОПИСАНИЙ'))
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

            response = input(f'⚠️  Вы уверены, что хотите удалить ВСЕ {count} описаний? (yes/no): ')

            if response.lower() != 'yes':
                self.stdout.write(self.style.WARNING('❌ Очистка отменена'))
                return

            # Создаем бэкап данных перед очисткой
            backup_data = []
            for game in games_to_clear[:1000]:
                backup_data.append({
                    'id': game.id,
                    'name': game.name,
                    'rawg_description': game.rawg_description
                })

            backup_file = self.output_dir / f'descriptions_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            try:
                import json
                with open(backup_file, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, ensure_ascii=False, indent=2, default=str)
                self.stdout.write(self.style.WARNING(f'📋 Создана резервная копия описаний: {backup_file}'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'⚠️ Не удалось создать бэкап: {e}'))

            updated_count = games_to_clear.update(rawg_description=None)

            self.stdout.write(self.style.SUCCESS(f'✅ Удалено описаний: {updated_count}'))

            # Очищаем файл steam_descriptions_all.txt (кэш)
            if self.full_output_path and self.full_output_path.exists():
                backup_desc_file = self.full_output_path.with_suffix(
                    f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
                try:
                    import shutil
                    shutil.copy2(self.full_output_path, backup_desc_file)
                    self.stdout.write(
                        self.style.WARNING(f'📋 Создана резервная копия кэш-файла описаний: {backup_desc_file}'))

                    # Очищаем файл (записываем только заголовок)
                    with open(self.full_output_path, 'w', encoding='utf-8') as f:
                        header = f"{'=' * 80}\n"
                        header += f"STEAM GAME DESCRIPTIONS\n"
                        header += f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                        header += f"{'=' * 80}\n\n"
                        f.write(header)
                    self.stdout.write(self.style.SUCCESS(f'✅ Очищен кэш-файл описаний: {self.full_output_path}'))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'❌ Ошибка очистки кэш-файла: {e}'))

            # Бэкап и очистка файла кэша
            if self.cache_file_path and self.cache_file_path.exists():
                backup_cache_file = self.cache_file_path.with_suffix(
                    f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
                try:
                    import shutil
                    shutil.copy2(self.cache_file_path, backup_cache_file)
                    self.stdout.write(self.style.WARNING(f'📋 Создана резервная копия кэша: {backup_cache_file}'))
                    self.cache_file_path.unlink()
                    self.stdout.write(self.style.SUCCESS(f'✅ Удален файл кэша: {self.cache_file_path}'))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f'❌ Ошибка удаления кэша: {e}'))

            self.stdout.write(self.style.SUCCESS('=' * 60))
            self.stdout.write(self.style.SUCCESS('✅ ОЧИСТКА ОПИСАНИЙ ЗАВЕРШЕНА'))
            self.stdout.write(self.style.WARNING('📁 Файлы найденных и не найденных игр сохранены'))

        except Exception as e:
            self.log_debug("Ошибка при очистке описаний", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Ошибка при очистке: {e}'))

    def _get_games_batch_for_offset(self, target_game: Optional[Game], force: bool,
                                    process_not_found: bool, skip_not_found: bool,
                                    process_no_description: bool, skip_no_description: bool,
                                    only_found: bool,
                                    start_offset: int, count: int) -> List[Game]:
        """Получение конкретной порции игр для обработки внутри offset."""
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

    def add_to_no_description(self, game: Game, reason: str = "no_description", error_details: str = None):
        """Добавление игры в список без описания.
        Формат: Game ID: 12345 - Название игры (Steam App ID: 67890) - причина
        """
        if game.id in self.no_description_games:
            return

        # Получаем app_id из кэша если есть
        app_id = "None"
        if game.id in self.cache_data and self.cache_data[game.id].get('app_id'):
            app_id = self.cache_data[game.id]['app_id']

        error_info = f" - {error_details}" if error_details else ""
        game_info = f"Game ID: {game.id} - {game.name} (Steam App ID: {app_id}) - {reason}{error_info}"
        self.no_description_buffer.append(game_info)
        self.no_description_games.add(game.id)

        # Обновляем кэш (описание не получено)
        if game.id in self.cache_data:
            self.cache_data[game.id]['has_description'] = False

        self.save_no_description_buffer()
        self.log_debug(f"Добавлена игра без описания: {game.name} (ID: {game.id}) - {reason}")

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
                # Создаем бэкап только если create_backup = True
                if self.create_backup and file_path.exists() and file_path.stat().st_size > 0:
                    backup_path = file_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
                    import shutil
                    shutil.copy2(file_path, backup_path)
                    self.log_debug(f"Создан бэкап файла без описания: {backup_path}")
                    self.create_backup = False  # После создания бэкапа отключаем для следующих итераций

                with open(file_path, 'a', encoding='utf-8') as f:
                    for game_info in self.no_description_buffer:
                        f.write(game_info)
                        f.write("\n")

                self.log_debug(f"Добавлено {len(self.no_description_buffer)} игр в файл без описания")
                self.no_description_buffer = []

            except IOError as e:
                self.log_debug("Ошибка записи файла игр без описания", error=e)

    def load_no_description_games(self) -> set:
        """Загрузка списка игр без описания из файла.
        Единый формат: Game ID: 12345 - Название игры (Steam App ID: 67890) - причина
        """
        no_description_set = set()
        file_path = self.no_description_file_path

        if file_path and file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 Загрузка игр без описания из {file_path}...'))

            try:
                # Сначала подсчитываем количество строк для прогресса
                with open(file_path, 'r', encoding='utf-8') as f:
                    total_lines = sum(1 for _ in f)

                processed = 0
                last_percent = -1

                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()

                        # Обновляем прогресс каждые 100 строк или 5%
                        processed += 1
                        if total_lines > 0:
                            percent = int(processed / total_lines * 100)
                            if percent != last_percent and percent % 5 == 0:
                                last_percent = percent
                                self.stdout.write(f'   📖 Загружено {processed}/{total_lines} строк ({percent}%)')

                        if not line:
                            continue
                        if line.startswith('=') or line.startswith(
                                'STEAM GAMES WITHOUT DESCRIPTION') or line.startswith('Created:'):
                            continue

                        if 'Game ID:' in line:
                            try:
                                # Парсим формат: Game ID: 12345 - Название игры (Steam App ID: 67890) - причина
                                game_part = line.split('Game ID:')[1].strip()

                                if '-' in game_part:
                                    game_id_str = game_part.split('-')[0].strip()
                                    game_id = int(game_id_str)
                                    no_description_set.add(game_id)
                                else:
                                    # Старый формат без дефиса
                                    game_id_str = game_part.split(' ')[0].strip()
                                    game_id = int(game_id_str)
                                    no_description_set.add(game_id)

                            except (ValueError, IndexError) as e:
                                if line_num <= 10:
                                    self.log_debug(f"Ошибка парсинга строки {line_num}: {line[:100]}", error=e)
                                continue

                self.stdout.write(
                    self.style.SUCCESS(f'📂 Загружено {len(no_description_set)} игр без описания из {file_path}'))
            except Exception as e:
                self.log_debug(f"Ошибка при загрузке файла игр без описания", error=e)
        else:
            self.stdout.write(self.style.WARNING(f'📂 Файл игр без описания не существует: {file_path}'))

        return no_description_set

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

        # Устанавливаем флаг создания бэкапов (по умолчанию True, если не указан --no-backup)
        self.create_backup = not options.get('no_backup', False)

        # Загружаем накопленную статистику из аргументов командной строки
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

        # Используем фиксированные имена файлов для общей папки
        self.output_file = output_file
        self.found_file = 'steam_found.txt'
        self.not_found_file = 'steam_not_found.txt'
        self.no_description_file = 'steam_no_description.txt'
        self.log_file = 'steam_fetcher_timeline.log'
        self.cache_file = 'steam_cache.json'
        self.stats_file = 'steam_stats.json'

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
        self.full_output_path = self.output_dir / self.output_file
        self.found_file_path = self.output_dir / self.found_file
        self.not_found_file_path = self.output_dir / self.not_found_file
        self.no_description_file_path = self.output_dir / self.no_description_file
        self.log_file_path = self.output_dir / self.log_file
        self.cache_file_path = self.output_dir / self.cache_file
        self.stats_file_path = self.output_dir / self.stats_file

        # Загружаем кэш Steam
        self.cache_data = self.load_steam_cache(self.cache_file_path)

        # Создаем файл найденных игр ТОЛЬКО если его нет
        if not self.found_file_path.exists():
            try:
                with open(self.found_file_path, 'w', encoding='utf-8') as f:
                    header = f"{'=' * 80}\n"
                    header += f"STEAM FOUND GAMES\n"
                    header += f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    header += f"{'=' * 80}\n\n"
                    f.write(header)
                self.stdout.write(self.style.SUCCESS(f'📁 Создан файл найденных игр: {self.found_file_path}'))
            except Exception as e:
                self.log_debug("Ошибка при создании файла найденных игр", error=e)

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

        # Создаем файл игр без описания ТОЛЬКО если его нет
        if not self.no_description_file_path.exists():
            try:
                with open(self.no_description_file_path, 'w', encoding='utf-8') as f:
                    header = f"{'=' * 80}\n"
                    header += f"STEAM GAMES WITHOUT DESCRIPTION\n"
                    header += f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    header += f"{'=' * 80}\n\n"
                    f.write(header)
                self.stdout.write(
                    self.style.SUCCESS(f'📁 Создан файл игр без описания: {self.no_description_file_path}'))
            except Exception as e:
                self.log_debug("Ошибка при создании файла игр без описания", error=e)

        # Очищаем лог-файл при первом запуске
        if processed_total == 0:
            try:
                with open(self.log_file_path, 'w', encoding='utf-8') as f:
                    f.write(f"=== ЛОГ ВРЕМЕННОЙ ШКАЛЫ ===\n")
                    f.write(f"Программа: fetch_steam_descriptions\n")
                    f.write(f"{'=' * 50}\n\n")
            except Exception:
                pass

        # Загружаем found_games и app_id_dict
        self.found_games, self.app_id_dict = self.load_found_games()

        # Загружаем not_found_games
        self.not_found_games = self.load_not_found_games()

        # Загружаем no_description_games
        self.no_description_games = self.load_no_description_games()

        # Определяем режим работы
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
                self.stdout.write(
                    self.style.WARNING(f'📊 Обработка только {len(self.found_games)} ранее найденных игр'))
            elif skip_not_found and self.not_found_games:
                base_queryset = base_queryset.exclude(id__in=self.not_found_games)
                self.stdout.write(self.style.WARNING(f'📊 Исключено {len(self.not_found_games)} ранее не найденных игр'))

            if skip_no_description and self.no_description_games:
                base_queryset = base_queryset.exclude(id__in=self.no_description_games)
                self.stdout.write(
                    self.style.WARNING(f'📊 Исключено {len(self.no_description_games)} ранее игр без описания'))

            if process_not_found and self.not_found_games:
                base_queryset = Game.objects.filter(id__in=self.not_found_games)
                self.stdout.write(
                    self.style.WARNING(f'📊 Обработка только {len(self.not_found_games)} ранее не найденных игр'))

            if process_no_description and self.no_description_games:
                base_queryset = Game.objects.filter(id__in=self.no_description_games)
                self.stdout.write(
                    self.style.WARNING(f'📊 Обработка только {len(self.no_description_games)} ранее игр без описания'))

            total_available = base_queryset.count()

            self.stdout.write(self.style.SUCCESS(f'📊 Всего игр на PC: {Game.objects.filter(platforms=pc).count()}'))
            self.stdout.write(self.style.SUCCESS(f'📊 Доступно для обработки: {total_available}'))

            # Определяем общий лимит (сколько всего игр обработать за все время)
            if limit_from_args is None:
                total_limit = total_available
                self.stdout.write(self.style.WARNING(f'🔄 Общий лимит не указан, обрабатываем все {total_limit} игр'))
            else:
                total_limit = min(limit_from_args, total_available)
                self.stdout.write(self.style.WARNING(f'📊 Общий лимит = {total_limit} из {total_available} доступных'))

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

        # Выводим информацию о том, сколько игр будет обработано
        self.stdout.write(self.style.SUCCESS(f'📊 ИТОГО БУДЕТ ОБРАБОТАНО: {total_limit} игр'))
        self.stdout.write(self.style.SUCCESS(f'📊 Размер батча: {batch_size} игр'))
        self.stdout.write(self.style.SUCCESS(f'📊 Количество итераций: {(total_limit + batch_size - 1) // batch_size}'))
        if self.create_backup:
            self.stdout.write(self.style.WARNING(f'📋 Создание резервных копий файлов: ВКЛЮЧЕНО'))
        else:
            self.stdout.write(self.style.WARNING(f'📋 Создание резервных копий файлов: ОТКЛЮЧЕНО'))

        return (total_limit, total_available, target_game, processed_total,
                batch_size, iteration_pause, output_file, output_dir,
                dry_run, force, skip_search, no_restart,
                process_not_found, skip_not_found,
                process_no_description, skip_no_description,
                only_found)

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
                # Создаем бэкап только если create_backup = True
                if self.create_backup and file_path.exists() and file_path.stat().st_size > 0:
                    backup_path = file_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
                    import shutil
                    shutil.copy2(file_path, backup_path)
                    self.log_debug(f"Создан бэкап файла не найденных: {backup_path}")
                    self.create_backup = False  # После создания бэкапа отключаем для следующих итераций

                with open(file_path, 'a', encoding='utf-8') as f:
                    for game_info in self.not_found_buffer:
                        f.write(game_info)
                        f.write("\n")

                self.log_debug(f"Добавлено {len(self.not_found_buffer)} игр в файл не найденных")
                self.not_found_buffer = []

            except IOError as e:
                self.log_debug("Ошибка записи файла не найденных игр", error=e)

    def load_not_found_games(self) -> set:
        """Загрузка списка не найденных игр из файла.
        Единый формат: Game ID: 12345 - Название игры (Steam App ID: None) - не найдено в Steam
        """
        not_found_set = set()
        file_path = self.not_found_file_path

        if file_path and file_path.exists():
            self.stdout.write(self.style.WARNING(f'📂 Загрузка не найденных игр из {file_path}...'))

            try:
                # Сначала подсчитываем количество строк для прогресса
                with open(file_path, 'r', encoding='utf-8') as f:
                    total_lines = sum(1 for _ in f)

                processed = 0
                last_percent = -1

                with open(file_path, 'r', encoding='utf-8') as f:
                    for line_num, line in enumerate(f, 1):
                        line = line.strip()

                        # Обновляем прогресс каждые 100 строк или 5%
                        processed += 1
                        if total_lines > 0:
                            percent = int(processed / total_lines * 100)
                            if percent != last_percent and percent % 5 == 0:
                                last_percent = percent
                                self.stdout.write(f'   📖 Загружено {processed}/{total_lines} строк ({percent}%)')

                        if not line:
                            continue
                        if line.startswith('=') or line.startswith('STEAM NOT FOUND GAMES') or line.startswith(
                                'Created:'):
                            continue

                        if 'Game ID:' in line:
                            try:
                                # Парсим формат: Game ID: 12345 - Название игры (Steam App ID: None) - не найдено в Steam
                                game_part = line.split('Game ID:')[1].strip()

                                if '-' in game_part:
                                    game_id_str = game_part.split('-')[0].strip()
                                    game_id = int(game_id_str)
                                    not_found_set.add(game_id)
                                else:
                                    # Старый формат без дефиса
                                    game_id_str = game_part.split(' ')[0].strip()
                                    game_id = int(game_id_str)
                                    not_found_set.add(game_id)

                            except (ValueError, IndexError) as e:
                                if line_num <= 10:
                                    self.log_debug(f"Ошибка парсинга строки {line_num}: {line[:100]}", error=e)
                                continue

                self.stdout.write(
                    self.style.SUCCESS(f'📂 Загружено {len(not_found_set)} не найденных игр из {file_path}'))
            except Exception as e:
                self.log_debug(f"Ошибка при загрузке файла не найденных игр", error=e)
        else:
            self.stdout.write(self.style.WARNING(f'📂 Файл не найденных игр не существует: {file_path}'))

        return not_found_set

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
        """Обработчик сигнала прерывания (Ctrl+C) для graceful shutdown."""
        self.stdout.write(self.style.ERROR('\n\n⚠️ Получен сигнал прерывания (Ctrl+C)'))
        self.interrupted = True
        # НЕ используем os._exit()

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
        """Проверка rate limiting и выполнение backoff при необходимости. Возвращает True если была пауза."""
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

                # Разбиваем паузу на маленькие интервалы для проверки прерывания
                while remaining > 0 and not self.interrupted:
                    try:
                        current_remaining = self.rate_limiter.get_wait_time_remaining()

                        if int(current_remaining) != int(last_remaining) or current_remaining <= 5:
                            if time.time() - last_display > 0.2:
                                print(f'\r   Осталось {int(current_remaining)}с...   ', end='', flush=True)
                                last_display = time.time()
                                last_remaining = current_remaining

                        # Разбиваем на маленькие интервалы для проверки прерывания
                        time.sleep(0.5)
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

                # Проверяем прерывание перед пересозданием сессии
                if self.interrupted:
                    return False

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

                return True  # Была пауза
        return False  # Не было паузы

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
                    self.style.SUCCESS(f'📊 Батч: игры {offset + 1}-{offset + len(games)} (смещение {offset})')
                )

                if self.debug:
                    for i, game in enumerate(games[:3], 1):
                        has_desc = bool(game.rawg_description)
                        desc_status = "есть описание" if has_desc else "нет описания"
                        not_found_status = " (была не найдена)" if game.id in (not_found_set or set()) else ""
                        no_desc_status = " (была без описания)" if game.id in (no_description_set or set()) else ""
                        self.stdout.write(
                            f'    {i}. {game.name} (ID: {game.id}, {desc_status}{not_found_status}{no_desc_status})')

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
            # Проверка на корректность app_id
            if not app_id or app_id <= 0:
                self.log_debug(f"Некорректный App ID: {app_id}", game_name=game_name)
                return None, 'invalid_app_id'

            url = "https://store.steampowered.com/api/appdetails"
            params = {
                'appids': app_id,
                'l': 'english',
                'cc': 'us'
            }

            # Проверяем сессию перед запросом
            try:
                self.session.headers.update({'Connection': 'keep-alive'})
            except (AttributeError, OSError, Exception):
                self.log_debug(f"Пересоздание поврежденной сессии", game_name=game_name)
                self.session = self._create_session()

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
                    with self.stats_lock:
                        self.total_stats['error_429'] += 1
                elif response.status_code >= 500:
                    error_type = '5xx'

                wait_time = self.rate_limiter.record_failure(error_type)
                if wait_time > 0:
                    self.check_rate_limit()

                return None, error_type
            else:
                self.rate_limiter.record_success()

            # Парсим JSON ответ
            try:
                data = response.json()
            except ValueError as e:
                self.log_debug(f"Ошибка парсинга JSON: {e}", game_name=game_name)
                return None, 'json_error'
            except Exception as e:
                self.log_debug(f"Неизвестная ошибка парсинга: {e}", game_name=game_name)
                return None, 'json_parse_error'

            # Проверяем структуру ответа
            if not data:
                self.log_debug(f"Пустой ответ от API", game_name=game_name)
                return None, 'empty_response'

            # Проверяем наличие данных для нашего app_id
            str_app_id = str(app_id)
            if str_app_id not in data:
                self.log_debug(f"App ID {app_id} отсутствует в ответе", game_name=game_name)
                return None, 'app_id_not_found'

            app_data = data[str_app_id]

            # Проверяем success флаг
            if not app_data.get('success'):
                self.log_debug(f"App ID {app_id} не успешен (success=False)", game_name=game_name)
                return None, 'app_not_success'

            # Получаем данные игры
            game_data = app_data.get('data')
            if not game_data:
                self.log_debug(f"Нет данных для App ID {app_id}", game_name=game_name)
                return None, 'no_game_data'

            # Ищем описание в различных полях
            description = None
            description_source = None

            # Приоритет полей: detailed_description -> about_the_game -> short_description
            for field in ['detailed_description', 'about_the_game', 'short_description']:
                desc = game_data.get(field, '')
                if desc and isinstance(desc, str) and desc.strip():
                    description = desc
                    description_source = field
                    break

            # Если нашли описание
            if description:
                self.log_verbose(f"Найдено описание в поле '{description_source}'", game_name=game_name)
                cleaned_description = self.clean_html(description)

                # Проверяем, не пустое ли описание после очистки
                if cleaned_description and cleaned_description.strip():
                    return cleaned_description, None
                else:
                    self.log_debug(f"Описание пустое после очистки HTML", game_name=game_name)
                    return None, 'empty_after_clean'

            # Проверяем, есть ли игра вообще (может быть платная или недоступная)
            if game_data.get('is_free') is False and not game_data.get('price_overview'):
                # Платная игра без описания - все равно считаем как no_description
                self.log_debug(f"Платная игра без описания", game_name=game_name)
                return None, 'no_description'

            # Проверяем, не демо ли это или другой контент
            if game_data.get('type') and game_data.get('type') != 'game':
                self.log_debug(f"Это не игра, а {game_data.get('type')}", game_name=game_name)
                return None, f"not_game_{game_data.get('type')}"

            # Если дошли сюда - описания нет
            self.log_debug(f"Нет описания ни в одном из полей", game_name=game_name)
            return None, 'no_description'

        except requests.Timeout:
            self.log_debug(f"Таймаут при получении описания", game_name=game_name)
            self.rate_limiter.record_failure("timeout")
            with self.stats_lock:
                self.total_stats['error_timeout'] += 1
            return None, 'timeout'

        except requests.ConnectionError as e:
            self.log_debug(f"Ошибка соединения", game_name=game_name, error=e)
            self.rate_limiter.record_failure("connection")
            return None, 'connection'

        except requests.TooManyRedirects as e:
            self.log_debug(f"Слишком много редиректов", game_name=game_name, error=e)
            self.rate_limiter.record_failure("too_many_redirects")
            return None, 'too_many_redirects'

        except requests.RequestException as e:
            self.log_debug(f"Общая ошибка запроса", game_name=game_name, error=e)
            self.rate_limiter.record_failure("request_exception")
            return None, 'request_exception'

        except OSError as e:
            self.log_debug(f"Ошибка сокета/ОС", game_name=game_name, error=e)
            self.rate_limiter.record_failure("socket_error")
            # Пробуем пересоздать сессию
            try:
                self.session = self._create_session()
            except:
                pass
            return None, 'socket_error'

        except MemoryError as e:
            self.log_debug(f"Ошибка памяти", game_name=game_name, error=e)
            return None, 'memory_error'

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
                # Создаем бэкап только если create_backup = True и не is_first
                if self.create_backup and not is_first and file_path.exists() and file_path.stat().st_size > 0:
                    backup_path = file_path.with_suffix(f'.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
                    import shutil
                    shutil.copy2(file_path, backup_path)
                    self.log_debug(f"Создан бэкап кэш-файла описаний: {backup_path}")
                    self.create_backup = False

                # Всегда используем append, так как файл может содержать существующие описания
                # При is_first=False просто добавляем, при is_first=True но файл существует - тоже добавляем
                # Очистка файла происходит только в _clear_descriptions
                mode = 'a'

                # Проверяем, нужно ли добавить заголовок если файл пустой или только что создан
                add_header = False
                if not file_path.exists() or file_path.stat().st_size == 0:
                    add_header = True
                elif is_first and file_path.exists() and file_path.stat().st_size > 0:
                    # Если is_first=True но файл не пустой, значит это продолжение, не добавляем заголовок
                    add_header = False

                with open(file_path, mode, encoding='utf-8') as f:
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
                self.stdout.write(self.style.SUCCESS(f'💾 Сохранено {len(self.descriptions_buffer)} описаний в кэш'))

            except IOError as e:
                self.log_debug("Ошибка записи файла", error=e)

    def process_batch(self, games: List[Game], skip_search: bool, timeout: float,
                      delay: float, output_file: str, dry_run: bool,
                      batch_stats: Dict, is_first: bool = False) -> List[Game]:
        """Обработка одного батча игр с поддержкой прерывания."""
        games_to_update = []
        batch_failures = 0
        batch_total = len(games)
        executor = None

        self.stdout.write(f'  🕒 Таймаут: {timeout}с, воркеров: {self.workers}')

        try:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {}
                for game in games:
                    # Проверяем прерывание перед отправкой задач
                    if self.interrupted:
                        self.stdout.write(self.style.WARNING('  ⚠️ Прерывание обнаружено, отмена отправки задач...'))
                        break
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
                futures_list = list(futures.keys())
                total_futures = len(futures_list)

                # Если нет задач, выходим
                if total_futures == 0:
                    self.stdout.write(self.style.WARNING('  ⚠️ Нет задач для выполнения'))
                    return []

                self.stdout.write(f'  📋 Отправлено задач: {total_futures}')

                while completed < total_futures and not self.interrupted:
                    # Проверяем rate limit
                    if self.rate_limiter and self.rate_limiter.should_backoff():
                        if not self.check_rate_limit():
                            break

                    # Проверяем прерывание перед каждой итерацией
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

                    # Проверяем прерывание между проверками
                    if self.interrupted:
                        self.stdout.write(self.style.ERROR('  ⚠️ Прерывание во время обработки батча!'))
                        break

                    # Небольшая пауза между проверками для снижения нагрузки CPU
                    time.sleep(0.05)

                    # Отмена долгих задач при прерывании
                    if self.interrupted:
                        for future in futures_list:
                            if future not in processed_games and not future.done():
                                future.cancel()
                        break

                # Если было прерывание, отменяем оставшиеся задачи
                if self.interrupted:
                    self.stdout.write(self.style.WARNING('  ⚠️ Отмена оставшихся задач...'))
                    for future in futures_list:
                        if future not in processed_games and not future.done():
                            future.cancel()

                    # Ждем завершения отмены с таймаутом
                    for future in futures_list:
                        try:
                            future.result(timeout=0.5)
                        except:
                            pass

                    self.stdout.write(self.style.WARNING('  ✅ Все задачи отменены'))

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

        finally:
            if executor:
                executor.shutdown(wait=False, cancel_futures=True)
                self.stdout.write(self.style.WARNING('  ✅ Пул потоков завершен'))

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

            # Проверяем, есть ли app_id в загруженном словаре для этой игры
            if game.id in getattr(self, 'app_id_dict', {}):
                app_id = self.app_id_dict[game.id]
                self.log_debug(f"Использую сохраненный Steam App ID: {app_id}", game_name=game.name)
            elif not skip_search:
                app_id, search_error = self.search_steam(game.name, timeout)
            else:
                # Если skip_search и нет app_id в словаре, нельзя получить описание
                app_id = None
                search_error = 'no_app_id'

            if not app_id:
                with self.stats_lock:
                    if search_error in ['not_found', 'invalid_name', 'app_id_not_found', 'app_not_success',
                                        'no_app_id']:
                        stats['not_found'] += 1
                        self.total_stats['not_found'] += 1
                        result['error_type'] = 'not_found'

                        if search_error == 'no_app_id':
                            result['error_message'] = 'Нет Steam App ID в кэше'
                        elif search_error == 'not_found':
                            result['error_message'] = 'Не найдена в Steam'
                        elif search_error == 'invalid_name':
                            result['error_message'] = 'Некорректное название'
                        elif search_error == 'app_id_not_found':
                            result['error_message'] = 'App ID не найден'
                        elif search_error == 'app_not_success':
                            result['error_message'] = 'Игра не доступна'
                        else:
                            result['error_message'] = 'Не найдена в Steam'

                        if not dry_run and search_error != 'no_app_id':
                            error_reason = "not_found"
                            if search_error == 'invalid_name':
                                error_reason = "invalid_name"
                            elif search_error == 'app_id_not_found':
                                error_reason = "app_id_not_found"
                            elif search_error == 'app_not_success':
                                error_reason = "app_not_success"
                            else:
                                error_reason = "not_found"
                            self.add_to_not_found(game, error_reason)

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

            description, desc_error = self.fetch_description(app_id, timeout, game.name)

            if not description:
                with self.stats_lock:
                    if desc_error in ['app_id_not_found', 'app_not_success', 'invalid_app_id']:
                        stats['not_found'] += 1
                        self.total_stats['not_found'] += 1
                        result['error_type'] = 'not_found'

                        if desc_error == 'app_id_not_found':
                            result['error_message'] = 'App ID не найден'
                            error_reason = "app_id_not_found"
                        elif desc_error == 'app_not_success':
                            result['error_message'] = 'Игра не доступна'
                            error_reason = "app_not_success"
                        elif desc_error == 'invalid_app_id':
                            result['error_message'] = 'Некорректный App ID'
                            error_reason = "invalid_app_id"
                        else:
                            result['error_message'] = 'Не найдена в Steam'
                            error_reason = "not_found"

                        if not dry_run:
                            self.add_to_not_found(game, error_reason)

                    elif desc_error and desc_error.startswith('not_game_'):
                        stats['not_found'] += 1
                        self.total_stats['not_found'] += 1
                        result['error_type'] = 'not_found'

                        game_type = desc_error.replace('not_game_', '')
                        result['error_message'] = f'Не игра (тип: {game_type})'

                        if not dry_run:
                            self.add_to_not_found(game, f"not_game_{game_type}")

                    elif desc_error in ['no_description', 'None', None, '', 'empty_after_clean', 'empty_response',
                                        'no_game_data']:
                        stats['no_description'] += 1
                        self.total_stats['no_description'] += 1
                        result['error_type'] = 'no_description'

                        if desc_error == 'no_description':
                            result['error_message'] = 'Нет описания'
                            error_reason = "no_description"
                        elif desc_error == 'empty_after_clean':
                            result['error_message'] = 'Пусто после очистки HTML'
                            error_reason = "empty_after_clean"
                        elif desc_error == 'empty_response':
                            result['error_message'] = 'Пустой ответ API'
                            error_reason = "empty_response"
                        elif desc_error == 'no_game_data':
                            result['error_message'] = 'Нет данных игры'
                            error_reason = "no_game_data"
                        else:
                            result['error_message'] = f'Нет описания ({desc_error or "None"})'
                            error_reason = f"no_description_{desc_error or 'none'}"

                        if not dry_run:
                            self.add_to_no_description(game, error_reason, f"App ID: {app_id}")

                    else:
                        stats['error'] += 1
                        self.total_stats['error'] += 1

                        if desc_error == '403':
                            self.total_stats['error_403'] += 1
                            error_display = '403'
                        elif desc_error == '429':
                            self.total_stats['error_429'] += 1
                            error_display = '429'
                        elif desc_error == 'timeout':
                            self.total_stats['error_timeout'] += 1
                            error_display = 'таймаут'
                        elif desc_error == 'connection':
                            self.total_stats['error_other'] += 1
                            error_display = 'ошибка соединения'
                        elif desc_error == 'socket_error':
                            self.total_stats['error_other'] += 1
                            error_display = 'ошибка сокета'
                        elif desc_error == 'json_error' or desc_error == 'json_parse_error':
                            self.total_stats['error_other'] += 1
                            error_display = 'ошибка JSON'
                        else:
                            self.total_stats['error_other'] += 1
                            error_display = desc_error or 'неизвестная ошибка'

                        result['error_type'] = desc_error or 'fetch_error'
                        result['error_message'] = f'Ошибка получения: {error_display}'
                        result['should_retry'] = True

                result['skipped'] = True
                return result

            result['success'] = True
            result['description'] = description
            result['app_id'] = app_id

            # Добавляем в файл НАЙДЕННЫХ игр
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
        self.stats_file_path = None

        signal.signal(signal.SIGINT, self.signal_handler)

        # Инициализация rate limiter
        self.rate_limiter = SteamRateLimiter(
            max_consecutive_failures=options['max_consecutive_failures'],
            base_wait_time=options['base_wait'],
            max_wait_time=options['max_wait']
        )
        self.batch_failure_threshold = options['batch_failure_threshold']

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('🚀 ЗАПУСК STEAM DESCRIPTIONS FETCHER'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        # Получаем PC платформу
        self.stdout.write(self.style.WARNING('📋 Шаг 1/7: Поиск платформы PC...'))
        pc = self.get_pc_platform()
        if not pc:
            self.stdout.write(self.style.ERROR('❌ Критическая ошибка: платформа PC не найдена'))
            return
        self.stdout.write(self.style.SUCCESS(f'✅ Платформа PC найдена: {pc.name} (ID: {pc.id})'))

        # Сохраняем output_dir для очистки логов
        self.output_dir = Path(options['output_dir'])

        # Очистка логов если указан флаг
        if options['clear_logs']:
            self.stdout.write(self.style.WARNING('📋 Шаг 2/7: Очистка логов...'))
            self._clear_logs()
            self.stdout.write(self.style.SUCCESS('✅ Команда завершена после очистки логов'))
            return

        # Инициализация параметров
        self.stdout.write(self.style.WARNING('📋 Шаг 2/7: Инициализация параметров...'))
        params = self._initialize_parameters(options, pc)
        if not params:
            return

        (limit, total_to_process, target_game, processed_total,
         batch_size, iteration_pause, output_file, output_dir,
         dry_run, force, skip_search, no_restart,
         process_not_found, skip_not_found,
         process_no_description, skip_no_description,
         only_found) = params

        self.stdout.write(self.style.SUCCESS('✅ Параметры инициализированы'))

        # Создаем файл статистики при первом запуске
        if processed_total == 0:
            self.stdout.write(self.style.WARNING('📋 Шаг 3/7: Создание файла статистики...'))
            self._init_stats_file()

            # Загружаем описания из кэш-файла при первом запуске
            self.stdout.write(self.style.WARNING('📋 Шаг 4/7: Загрузка описаний из кэша...'))
            loaded = self.load_descriptions_from_cache()
            if loaded > 0:
                self.stdout.write(self.style.SUCCESS(f'✅ Загружено {loaded} описаний из кэша'))
        else:
            self.stdout.write(self.style.WARNING('📋 Шаг 3-4/7: Пропуск (продолжение работы)...'))

        # Очистка описаний если указан флаг (очищает и БД, и кэш-файл)
        if options['clear_descriptions']:
            self.stdout.write(self.style.WARNING('📋 Шаг 5/7: Очистка описаний...'))
            self._clear_descriptions(pc)
            if not force and not only_found:
                self.stdout.write(
                    self.style.WARNING('💡 Для повторного сбора описаний запустите команду без --clear-descriptions'))
                return

        # Логируем начало работы
        if processed_total == 0:
            self.log_timeline("START")

        # Вывод параметров запуска
        self.stdout.write(self.style.WARNING('📋 Шаг 6/7: Подготовка к обработке...'))
        self._print_startup_info(limit, total_to_process, processed_total,
                                 batch_size, iteration_pause, options,
                                 process_not_found, skip_not_found,
                                 process_no_description, skip_no_description)

        # Основной цикл обработки
        self.stdout.write(self.style.WARNING('📋 Шаг 7/7: Запуск основного цикла обработки...'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        processed_total, games_per_second = self._main_processing_loop(
            target_game, limit, batch_size, iteration_pause,
            skip_search, output_file, dry_run,
            force, no_restart, processed_total, options,
            process_not_found, skip_not_found,
            process_no_description, skip_no_description,
            only_found
        )

        # Сохраняем финальную версию кэша
        self.stdout.write(self.style.WARNING('💾 Сохранение финального кэша...'))
        if self.cache_file_path:
            self.save_steam_cache(self.cache_file_path)

        # Сохраняем финальную статистику
        self.stdout.write(self.style.WARNING('💾 Сохранение финальной статистики...'))
        self._save_stats_to_file()

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
                            iteration_pause: int, options: Dict,
                            process_not_found: bool, skip_not_found: bool,
                            process_no_description: bool, skip_no_description: bool) -> None:
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
        self.stdout.write(f'  Only found: {options.get("only_found", False)}')
        self.stdout.write(f'  Process only not found: {process_not_found}')
        self.stdout.write(f'  Skip not found: {skip_not_found}')
        self.stdout.write(f'  Process only no description: {process_no_description}')
        self.stdout.write(f'  Skip no description: {skip_no_description}')
        self.stdout.write(f'  Found file: {self.found_file}')
        self.stdout.write(f'  Not found file: {self.not_found_file}')
        self.stdout.write(f'  No description file: {self.no_description_file}')
        self.stdout.write(f'  Log file: {self.log_file}')
        self.stdout.write(f'  Loaded found games: {len(self.found_games)}')
        self.stdout.write(f'  Loaded not found games: {len(self.not_found_games)}')
        self.stdout.write(f'  Loaded no description games: {len(self.no_description_games)}')

        self.stdout.write(self.style.WARNING(f'\n🚫 Настройки защиты от ошибок:'))
        self.stdout.write(f'  Макс. ошибок подряд: {options["max_consecutive_failures"]}')
        self.stdout.write(f'  Базовая пауза: {options["base_wait"]}с')
        self.stdout.write(f'  Макс. пауза: {options["max_wait"]}с')
        self.stdout.write(f'  Порог ошибок в батче: {options["batch_failure_threshold"] * 100}%')

        self.stdout.write('=' * 60)
        self.stdout.write(self.style.WARNING('⚠️  Нажмите Ctrl+C для завершения'))
        self.stdout.write('=' * 60)

    def _main_processing_loop(self, target_game: Optional[Game], total_limit: int,
                              batch_size: int, iteration_pause: int,
                              skip_search: bool, output_file: str, dry_run: bool,
                              force: bool, no_restart: bool, processed_total: int,
                              options: Dict,
                              process_not_found: bool, skip_not_found: bool,
                              process_no_description: bool, skip_no_description: bool,
                              only_found: bool) -> Tuple[int, float]:
        """Основной цикл обработки игр."""
        iteration = 0
        games_per_second = 0
        loop_start_time = time.time()

        self.processed_total = processed_total
        self.only_found = only_found
        self.force = force
        self.dry_run = dry_run

        games_processed_this_session = 0

        self.stdout.write(self.style.SUCCESS(f'\n🚀 НАЧАЛО ОБРАБОТКИ {total_limit} ИГР'))
        self.stdout.write(self.style.SUCCESS(f'📊 Батч-сайз: {batch_size} игр за итерацию'))

        try:
            while not self.interrupted:
                # Проверяем, не достигли ли общего лимита
                if games_processed_this_session >= total_limit:
                    self.stdout.write(self.style.SUCCESS(f'\n✅ Достигнут общий лимит {total_limit} игр, завершаем'))
                    break

                # Вычисляем сколько еще можно обработать
                remaining_limit = total_limit - games_processed_this_session
                current_batch_size = min(batch_size, remaining_limit)

                # Получаем игры для обработки
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
                pause_occurred = False

                if self.interrupted:
                    break

                self.stdout.write(
                    self.style.SUCCESS(f'\n🔄 ИТЕРАЦИЯ {iteration}/{((total_limit + batch_size - 1) // batch_size)}'))
                self.stdout.write(self.style.SUCCESS(f'  📍 Смещение: {self.current_offset}'))
                self.stdout.write(self.style.SUCCESS(f'  📍 Игр в этой итерации: {len(games_to_process)}'))

                if self.rate_limiter and self.rate_limiter.should_backoff():
                    pause_occurred = self.check_rate_limit()
                    if pause_occurred and self.interrupted:
                        break

                if self.interrupted:
                    break

                batch_stats, games_to_update = self._process_games_batch(
                    games_to_process, skip_search, self.timeout, self.delay,
                    output_file, dry_run,
                    iteration == 1 and games_processed_this_session == 0
                )

                if self.interrupted:
                    break

                games_processed_this_batch = len(games_to_process)
                games_processed_this_session += games_processed_this_batch
                self.processed_total += games_processed_this_batch

                self.current_offset += games_processed_this_batch

                iteration_time = time.time() - iteration_start_time
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
                if total_limit > 0:
                    progress = min(current_processed / total_limit * 100, 100)
                else:
                    progress = 0

                elapsed = time.time() - loop_start_time
                elapsed_str = self._format_time(elapsed)

                remaining = max(0, total_limit - current_processed)
                eta_str = self._format_eta(remaining, games_per_second)

                bar_length = 40
                filled = int(bar_length * progress / 100)
                bar = '█' * filled + '░' * (bar_length - filled)

                self.stdout.write(self.style.SUCCESS(f'\n📊 ПРОГРЕСС: [{bar}] {progress:.1f}%'))
                self.stdout.write(self.style.SUCCESS(f'📊 Обработано: {current_processed}/{total_limit} игр'))
                self.stdout.write(self.style.SUCCESS(
                    f'⏱️ Прошло: {elapsed_str} | ⏳ Осталось: {eta_str} | ⚡ Скорость: {games_per_second:.1f} игр/с'))

                self._print_detailed_stats()
                self._save_stats_to_file()

                if pause_occurred and iteration_pause > 0 and not self.interrupted:
                    self.stdout.write(
                        self.style.WARNING(f'\n⏸️ Дополнительная пауза {iteration_pause}с после ошибок...'))
                    for _ in range(iteration_pause):
                        if self.interrupted:
                            break
                        time.sleep(1)

        except KeyboardInterrupt:
            self.interrupted = True
            self.stdout.write(self.style.ERROR('\n\n⚠️ Получено прерывание (Ctrl+C)'))
            self._save_stats_to_file()

        finally:
            self.stdout.write(self.style.WARNING('\n🔄 Завершение работы, очистка ресурсов...'))

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

            if self.interrupted:
                self.stdout.write(
                    self.style.WARNING(f'  💾 Прогресс сохранен: {games_processed_this_session}/{total_limit} игр'))
                sys.stdout.flush()

        return games_processed_this_session, games_per_second

    def _get_games_to_process(self, target_game: Optional[Game], force: bool,
                              process_not_found: bool, skip_not_found: bool,
                              process_no_description: bool, skip_no_description: bool) -> List[Game]:
        """Получение списка игр для обработки."""
        if target_game:
            return [target_game]

        if process_not_found is True and self.not_found_games:
            games = list(Game.objects.filter(id__in=self.not_found_games).order_by('id')
                         [self.current_offset:self.current_offset + self.batch_size])
            self.stdout.write(self.style.WARNING(f'📊 Обработка не найденных игр: {len(games)}'))
            return games

        if process_no_description is True and self.no_description_games:
            games = list(Game.objects.filter(id__in=self.no_description_games).order_by('id')
                         [self.current_offset:self.current_offset + self.batch_size])
            self.stdout.write(self.style.WARNING(f'📊 Обработка игр без описания: {len(games)}'))
            return games

        return self.get_games_batch(self.current_offset, self.batch_size, force,
                                    skip_not_found, self.not_found_games,
                                    skip_no_description, self.no_description_games)

    def _process_games_batch(self, games: List[Game], skip_search: bool,
                             timeout: float, delay: float, output_file: str,
                             dry_run: bool, is_first: bool) -> Tuple[Dict, List[Game]]:
        """Обработка батча игр с проверкой прерывания."""
        batch_stats = {
            'success': 0,
            'not_found': 0,
            'no_description': 0,
            'error': 0
        }

        # Если прервано, не обрабатываем
        if self.interrupted:
            self.stdout.write(self.style.WARNING('  ⚠️ Обработка батча отменена из-за прерывания'))
            return batch_stats, []

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

    def _print_progress_and_stats(self, processed_total: int, total_to_process: int,
                                  start_time: float, games_per_second: float) -> None:
        """Вывод прогресса и статистики."""
        # processed_total - сколько обработано всего (с учетом предыдущих запусков)
        # total_to_process - сколько нужно обработать в этом запуске

        if total_to_process > 0:
            # Прогресс считаем от общего количества для этого запуска
            # Но processed_total может быть больше total_to_process, если были предыдущие запуски
            current_in_this_run = self.processed_total - (self.processed_total - processed_total)
            progress = min(current_in_this_run / total_to_process * 100, 100)
        else:
            progress = 0

        elapsed = time.time() - start_time
        elapsed_str = self._format_time(elapsed)

        remaining = max(0, total_to_process - (self.processed_total - (self.processed_total - processed_total)))
        eta_str = self._format_eta(remaining, games_per_second)

        bar_length = 40
        filled = int(bar_length * progress / 100)
        bar = '█' * filled + '░' * (bar_length - filled)

        self.stdout.write(self.style.SUCCESS(f'\n📊 ПРОГРЕСС: [{bar}] {progress:.1f}%'))
        self.stdout.write(self.style.SUCCESS(
            f'📊 Обработано в этом запуске: {int(self.processed_total - (self.processed_total - processed_total))}/{total_to_process} игр'))
        self.stdout.write(self.style.SUCCESS(
            f'⏱️ Прошло: {elapsed_str} | ⏳ Осталось: {eta_str} | ⚡ Скорость: {games_per_second:.1f} игр/с'))

        # Выводим статистику за все время из self.total_stats
        self._print_detailed_stats()

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
                         process_not_found: bool, skip_not_found: bool,
                         process_no_description: bool, skip_no_description: bool,
                         only_found: bool) -> None:
        """Перезапуск процесса для следующей итерации."""
        if self.interrupted:
            self.stdout.write(self.style.WARNING('\n⚠️ Прерывание обнаружено, перезапуск отменен'))
            return

        self.stdout.write(self.style.WARNING(f'\n🔄 Следующая итерация через {iteration_pause}с...'))

        for i in range(iteration_pause, 0, -1):
            if self.interrupted:
                self.stdout.write(self.style.WARNING('\n⚠️ Прерывание во время паузы, перезапуск отменен'))
                return
            if i > 1:
                time.sleep(1)
                self.stdout.write(self.style.WARNING(f'   Осталось {i}с...'))
            else:
                time.sleep(1)
                break

        if self.interrupted:
            self.stdout.write(self.style.WARNING('\n⚠️ Прерывание обнаружено, перезапуск отменен'))
            return

        # Сохраняем статистику перед перезапуском
        self._save_stats_to_file()

        # Сохраняем кэш перед перезапуском (без бэкапа, так как это итерация)
        self.create_backup = False  # Отключаем бэкап для итерации
        if self.cache_file_path:
            self.save_steam_cache(self.cache_file_path)

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
            f"--output-dir={output_dir}",
            f"--max-consecutive-failures={options['max_consecutive_failures']}",
            f"--base-wait={options['base_wait']}",
            f"--max-wait={options['max_wait']}",
            f"--batch-failure-threshold={options['batch_failure_threshold']}",
            f"--processed={processed_total}",
            f"--no-backup",  # Добавляем флаг для отключения бэкапов в дочернем процессе
        ]

        # Передаем накопленную статистику
        if self.total_stats['success'] > 0:
            cmd.append(f"--stat-success={self.total_stats['success']}")
        if self.total_stats['not_found'] > 0:
            cmd.append(f"--stat-not-found={self.total_stats['not_found']}")
        if self.total_stats['no_description'] > 0:
            cmd.append(f"--stat-no-description={self.total_stats['no_description']}")
        if self.total_stats['error'] > 0:
            cmd.append(f"--stat-error={self.total_stats['error']}")
        if self.total_stats['error_403'] > 0:
            cmd.append(f"--stat-error-403={self.total_stats['error_403']}")
        if self.total_stats['error_429'] > 0:
            cmd.append(f"--stat-error-429={self.total_stats['error_429']}")
        if self.total_stats['error_timeout'] > 0:
            cmd.append(f"--stat-error-timeout={self.total_stats['error_timeout']}")
        if self.total_stats['error_other'] > 0:
            cmd.append(f"--stat-error-other={self.total_stats['error_other']}")
        if self.total_stats['backoff_pauses'] > 0:
            cmd.append(f"--stat-backoff-pauses={self.total_stats['backoff_pauses']}")
        if self.total_stats['iterations'] > 0:
            cmd.append(f"--stat-iterations={self.total_stats['iterations']}")

        if only_found:
            cmd.append("--only-found")
        if process_not_found:
            cmd.append("--process-not-found")
        if process_no_description:
            cmd.append("--process-no-description")
        if skip_not_found:
            cmd.append("--skip-not-found")
        if skip_no_description:
            cmd.append("--skip-no-description")
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

        self.stdout.write(self.style.SUCCESS(f'🚀 Запуск: {" ".join(cmd)}'))

        try:
            if self.descriptions_buffer:
                self.save_buffer(output_file, is_first=False)
            if self.not_found_buffer:
                self.save_not_found_buffer()
            if self.no_description_buffer:
                self.save_no_description_buffer()
            if self.found_buffer:
                self.save_found_buffer()

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

        self.stdout.write(f'  ✅ Успешно обновлено: {self.total_stats["success"]} ({success_pct:.1f}%)')
        self.stdout.write(f'  🔍 Не найдено в Steam: {self.total_stats["not_found"]} ({not_found_pct:.1f}%)')
        self.stdout.write(f'  📄 Нет описания: {self.total_stats["no_description"]} ({no_desc_pct:.1f}%)')
        self.stdout.write(f'  💥 Ошибок запросов: {self.total_stats["error"]} ({error_pct:.1f}%)')

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

        if self.found_file_path and self.found_file_path.exists():
            size = self.found_file_path.stat().st_size
            lines = 0
            try:
                with open(self.found_file_path, 'r', encoding='utf-8') as f:
                    lines = sum(1 for line in f if line.strip() and not line.startswith('=') and not line.startswith(
                        'STEAM') and not line.startswith('Generated') and not line.startswith('Offset'))
            except:
                pass
            self.stdout.write(self.style.SUCCESS(f'📁 Найдено: {self.found_file_path}'))
            self.stdout.write(self.style.SUCCESS(f'📊 Размер: {size:,} байт ({size / 1024:.1f} КБ), игр: {lines}'))

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

        if self.no_description_file_path and self.no_description_file_path.exists():
            size = self.no_description_file_path.stat().st_size
            lines = 0
            try:
                with open(self.no_description_file_path, 'r', encoding='utf-8') as f:
                    lines = sum(1 for line in f if line.strip() and not line.startswith('=') and not line.startswith(
                        'STEAM') and not line.startswith('Generated') and not line.startswith('Offset'))
            except:
                pass
            self.stdout.write(self.style.WARNING(f'📁 Без описания: {self.no_description_file_path}'))
            self.stdout.write(self.style.WARNING(f'📊 Размер: {size:,} байт ({size / 1024:.1f} КБ), игр: {lines}'))

        if self.log_file_path and self.log_file_path.exists():
            size = self.log_file_path.stat().st_size
            self.stdout.write(self.style.WARNING(f'📋 Лог: {self.log_file_path} ({size} байт)'))
