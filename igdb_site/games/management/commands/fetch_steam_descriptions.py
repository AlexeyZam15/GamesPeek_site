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
        self.consecutive_failures = 0  # Считаем ЛЮБЫЕ неудачные запросы подряд
        self.consecutive_403 = 0  # Считаем только 403 для информации
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

    def record_failure(self, error_type: str = "unknown") -> float:
        """Запись любой неудачной попытки. Возвращает время ожидания если нужно."""
        try:
            # Используем таймаут для блокировки
            acquired = self.lock.acquire(timeout=2)
            if not acquired:
                print(f"⚠️ ТАЙМАУТ record_failure: не удалось получить блокировку")
                return 0

            try:
                self.consecutive_failures += 1
                self.total_failures += 1
                self.last_failure_time = datetime.now()

                if error_type == "403":
                    self.consecutive_403 += 1
                    self.total_403 += 1

                # Проверяем нужно ли делать паузу
                if self.consecutive_failures >= self.max_consecutive_failures:
                    return self._calculate_wait_time()
            finally:
                self.lock.release()
        except Exception as e:
            print(f"⚠️ ОШИБКА record_failure: {e}")

        return 0

    def record_success(self):
        """Сброс счетчика при успешном запросе."""
        try:
            acquired = self.lock.acquire(timeout=2)
            if not acquired:
                print(f"⚠️ ТАЙМАУТ record_success: не удалось получить блокировку")
                return

            try:
                if self.consecutive_failures > 0:
                    print(f"✅ Сброс счетчика после {self.consecutive_failures} ошибок")
                    self.consecutive_failures = 0
                    self.consecutive_403 = 0
                    self.in_backoff = False
                    self.backoff_until = None
            finally:
                self.lock.release()
        except Exception as e:
            print(f"⚠️ ОШИБКА record_success: {e}")

    def _calculate_wait_time(self) -> float:
        """Расчет времени ожидания с экспоненциальной задержкой."""
        # Экспоненциальная задержка: 60, 120, 240, 300...
        exponent = self.consecutive_failures - self.max_consecutive_failures + 1
        wait_time = min(
            self.base_wait_time * (2 ** max(0, exponent)),
            self.max_wait_time
        )

        # Добавляем случайную составляющую (±20%)
        wait_time = wait_time * (0.8 + 0.4 * random.random())

        self.in_backoff = True
        self.backoff_until = datetime.now() + timedelta(seconds=wait_time)

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
            # Пытаемся получить блокировку с таймаутом
            acquired = self.lock.acquire(timeout=1)
            if not acquired:
                # Если не можем получить блокировку, предполагаем что не в backoff
                return False

            try:
                if self.in_backoff and self.backoff_until:
                    if datetime.now() < self.backoff_until:
                        return True
                    else:
                        # Время вышло, сбрасываем флаг
                        self.in_backoff = False
                        self.backoff_until = None
                return False
            finally:
                self.lock.release()
        except Exception as e:
            print(f"⚠️ ОШИБКА should_backoff: {e}")
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
        except Exception as e:
            print(f"⚠️ ОШИБКА get_wait_time_remaining: {e}")
            return 0

    def get_status(self) -> Dict:
        """Получение статуса rate limiter."""
        try:
            # Используем таймаут для получения блокировки
            acquired = self.lock.acquire(timeout=2)
            if not acquired:
                # Если не можем получить блокировку, возвращаем статус по умолчанию
                print(f"⚠️ ТАЙМАУТ get_status: не удалось получить блокировку")
                return {
                    'consecutive_failures': self.consecutive_failures,
                    'consecutive_403': self.consecutive_403,
                    'total_failures': self.total_failures,
                    'total_403': self.total_403,
                    'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
                    'waits': len(self.wait_history),
                    'in_backoff': False,
                    'backoff_remaining': 0
                }

            try:
                return {
                    'consecutive_failures': self.consecutive_failures,
                    'consecutive_403': self.consecutive_403,
                    'total_failures': self.total_failures,
                    'total_403': self.total_403,
                    'last_failure': self.last_failure_time.isoformat() if self.last_failure_time else None,
                    'waits': len(self.wait_history),
                    'in_backoff': self.in_backoff,
                    'backoff_remaining': self.get_wait_time_remaining()
                }
            finally:
                self.lock.release()
        except Exception as e:
            print(f"⚠️ ОШИБКА get_status: {e}")
            return {
                'consecutive_failures': 0,
                'consecutive_403': 0,
                'total_failures': 0,
                'total_403': 0,
                'last_failure': None,
                'waits': 0,
                'in_backoff': False,
                'backoff_remaining': 0
            }


class Command(BaseCommand):
    """Steam descriptions fetcher с автоматическим перезапуском и обработкой ошибок."""

    help = 'Получение описаний из Steam с автоматическим перезапуском для каждого оффсета'

    def add_arguments(self, parser: CommandParser) -> None:
        """Добавление аргументов команды."""
        parser.add_argument(
            '--limit',
            type=int,
            default=100000,
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
            default='steam_fetcher_logs',
            help='Директория для сохранения файлов'
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

    def __init__(self, *args, **kwargs):
        """Инициализация команды."""
        super().__init__(*args, **kwargs)
        self.stats_lock = Lock()
        self.output_lock = Lock()
        self.descriptions_buffer = []
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
            'backoff_pauses': 0
        }
        self.output_dir = None
        self.full_output_path = None
        self.current_offset = 0
        self.error_log = []
        self.rate_limiter = None
        self.batch_failure_threshold = 0.8

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

    def check_rate_limit(self) -> bool:
        """Проверка rate limiting и выполнение backoff при необходимости."""
        if self.rate_limiter and self.rate_limiter.should_backoff():
            remaining = self.rate_limiter.get_wait_time_remaining()
            if remaining > 0:
                status = self.rate_limiter.get_status()

                self.stdout.write(self.style.ERROR(
                    f'\n🚫 ОБНАРУЖЕНО {status["consecutive_failures"]} НЕУДАЧ ПОДРЯД!'
                ))
                self.stdout.write(self.style.ERROR(
                    f'⏳ Пауза на {remaining:.1f}с для снятия блокировки Steam...'
                ))

                # Закрываем старую сессию перед паузой
                try:
                    self.session.close()
                except:
                    pass

                # Обратный отсчет
                last_display = 0
                last_remaining = remaining

                while remaining > 0 and not self.interrupted:
                    try:
                        # Обновляем оставшееся время
                        current_remaining = self.rate_limiter.get_wait_time_remaining()

                        # Выводим только когда изменилось целое число секунд
                        if int(current_remaining) != int(last_remaining) or current_remaining <= 5:
                            if time.time() - last_display > 0.2:
                                # Используем print для надежности
                                print(f'\r   Осталось {int(current_remaining)}с...   ', end='', flush=True)
                                last_display = time.time()
                                last_remaining = current_remaining

                        time.sleep(0.1)
                        remaining = current_remaining

                    except KeyboardInterrupt:
                        self.interrupted = True
                        break
                    except Exception as e:
                        # Игнорируем ошибки во время паузы
                        pass

                # Переход на новую строку после завершения отсчета
                print()

                if self.interrupted:
                    self.stdout.write(self.style.WARNING('\n⚠️ Пауза прервана пользователем'))
                    return False

                # Создаем новую сессию после паузы
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

                return True
        return False

    def signal_handler(self, signum, frame):
        """Обработчик сигнала прерывания (Ctrl+C)."""
        self.stdout.write(self.style.ERROR('\n\n⚠️  Получен сигнал прерывания (Ctrl+C)'))

        # Устанавливаем флаг прерывания
        self.interrupted = True

        # Показываем статистику по ошибкам
        if self.rate_limiter:
            status = self.rate_limiter.get_status()
            self.stdout.write(self.style.ERROR(f'📊 Статистика ошибок:'))
            self.stdout.write(self.style.ERROR(f'  Всего неудач: {status["total_failures"]}'))
            self.stdout.write(self.style.ERROR(f'  403 ошибок: {status["total_403"]}'))
            self.stdout.write(self.style.ERROR(f'  Подряд: {status["consecutive_failures"]}'))
            self.stdout.write(self.style.ERROR(f'  Пауз: {status["waits"]}'))

        # Выводим накопленные ошибки
        if self.error_log and self.debug:
            self.stdout.write(self.style.ERROR('\n📋 ПОСЛЕДНИЕ ОШИБКИ:'))
            for i, error in enumerate(self.error_log[-10:], 1):
                self.stdout.write(self.style.ERROR(f'  {i}. {error[:100]}...'))

        self.stdout.write(self.style.ERROR('🛑 ФОРСИРОВАННОЕ ЗАВЕРШЕНИЕ...'))

        # Принудительно завершаем процесс
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

    def get_games_batch(self, offset: int, batch_size: int, force: bool) -> List[Game]:
        """Получение батча игр."""
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

            games = list(queryset.order_by('-rating_count', 'id')[offset:offset + batch_size])

            if games:
                self.stdout.write(
                    self.style.SUCCESS(f'📊 Батч: игры {offset + 1}-{offset + len(games)} (смещение {offset})')
                )

                if self.debug:
                    for i, game in enumerate(games[:3], 1):
                        has_desc = bool(game.rawg_description)
                        desc_status = "есть описание" if has_desc else "нет описания"
                        self.stdout.write(f'    {i}. {game.name} (ID: {game.id}, {desc_status})')

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
        # Проверяем не в паузе ли мы
        if self.rate_limiter and self.rate_limiter.should_backoff():
            self.check_rate_limit()
            if self.rate_limiter.should_backoff():
                return None, 'backoff'

        search_name = re.sub(r'[^\w\s-]', '', game_name)
        search_name = re.sub(r'\s+', ' ', search_name).strip()

        if not search_name:
            return None, 'invalid_name'

        self.log_verbose(f"Поиск в Steam: '{search_name}'", game_name=game_name)

        # Проверяем сессию и пересоздаем если нужно
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
            # Пересоздаем сессию и пробуем еще раз
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
        # Проверяем не в паузе ли мы
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

            # Словарь для отслеживания обработанных игр
            processed_games = set()

            while completed < batch_total and not self.interrupted:
                # Проверяем не в паузе ли мы
                if self.rate_limiter and self.rate_limiter.should_backoff():
                    if not self.check_rate_limit():
                        break

                # Собираем завершенные future
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

                # Небольшая пауза чтобы не нагружать процессор
                time.sleep(0.1)

                # Проверяем не зависли ли future
                if completed < batch_total:
                    current_time = time.time()
                    for future, game in list(futures.items()):
                        if future not in processed_games:
                            # Если задача выполняется дольше timeout * 3, считаем её зависшей
                            if hasattr(future, '_start_time'):
                                elapsed = current_time - future._start_time
                                if elapsed > timeout * 3:
                                    # Принудительно отменяем
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
                                # Запоминаем время старта
                                future._start_time = current_time

                # Проверяем не был ли получен сигнал прерывания
                if self.interrupted:
                    self.stdout.write(
                        self.style.WARNING('\n  ⚠️ Получен сигнал прерывания, завершаю обработку батча...'))
                    # Отменяем все незавершенные задачи
                    for future in list(futures.keys()):
                        if future not in processed_games and not future.done():
                            future.cancel()
                    break

            # Статистика по ошибкам для батча
            if error_403_count > 0:
                self.stdout.write(self.style.ERROR(
                    f'  🚫 403 ошибок в батче: {error_403_count}'
                ))

            if timeout_count > 0:
                self.stdout.write(self.style.WARNING(
                    f'  ⌛ Таймаутов в батче: {timeout_count}'
                ))

            # Анализ результатов батча
            if batch_total > 0:
                failure_ratio = batch_failures / batch_total

                if failure_ratio >= self.batch_failure_threshold:
                    self.stdout.write(self.style.ERROR(
                        f'\n  ⚠️ ВЫСОКИЙ УРОВЕНЬ ОШИБОК В БАТЧЕ: {failure_ratio * 100:.1f}%'
                    ))

                    # Принудительно записываем неудачу в rate limiter
                    wait_time = self.rate_limiter.record_failure("batch_failure")

                    if wait_time > 0:
                        self.stdout.write(self.style.ERROR(
                            f'  🚫 Слишком много ошибок, инициирую паузу...'
                        ))
                        self.check_rate_limit()

        # Сохраняем буфер
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
            'error_details': None
        }

        try:
            if delay > 0:
                time.sleep(delay)

            # Проверяем не в паузе ли мы
            if self.rate_limiter and self.rate_limiter.should_backoff():
                if not self.check_rate_limit():
                    result['skipped'] = True
                    result['error_type'] = 'backoff'
                    result['error_message'] = 'Пауза из-за ошибок'
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

                result['skipped'] = True
                return result

            result['success'] = True
            result['description'] = description
            result['app_id'] = app_id

            # Сохраняем в файл
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
            return result

    def handle(self, *args: Any, **options: Any) -> None:
        """Основной метод выполнения."""
        self.stdout.write(self.style.WARNING('\n🔍 ДИАГНОСТИКА: Начало handle()'))

        signal.signal(signal.SIGINT, self.signal_handler)

        start_time = time.time()

        # Инициализация rate limiter
        self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Инициализация rate limiter'))
        self.rate_limiter = SteamRateLimiter(
            max_consecutive_failures=options['max_consecutive_failures'],
            base_wait_time=options['base_wait'],
            max_wait_time=options['max_wait']
        )
        self.batch_failure_threshold = options['batch_failure_threshold']

        # Получаем PC платформу сначала для расчета limit
        self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Получение PC платформы для расчета limit'))
        pc = self.get_pc_platform()
        if not pc:
            self.stdout.write(self.style.ERROR('❌ Критическая ошибка: платформа PC не найдена'))
            return

        # Автоматический расчет количества игр для PC
        force = options['force']
        total_games = Game.objects.filter(platforms=pc).count()

        if not force:
            # Считаем игры без описаний
            games_without_desc = Game.objects.filter(
                platforms=pc,
                rawg_description__isnull=True
            ) | Game.objects.filter(
                platforms=pc,
                rawg_description=''
            )
            games_without_desc = games_without_desc.distinct().count()
            self.stdout.write(self.style.SUCCESS(f'📊 Всего игр на PC: {total_games}'))
            self.stdout.write(self.style.SUCCESS(f'📊 Игр без описаний: {games_without_desc}'))
            auto_limit = games_without_desc
        else:
            # При force обрабатываем все игры
            auto_limit = total_games
            self.stdout.write(self.style.SUCCESS(f'📊 Всего игр на PC: {total_games} (режим force)'))

        limit = options['limit']
        # Если limit не указан (равен дефолтному 10000) или равен 0, используем автоматический расчет
        if limit == 10000 or limit == 0:
            limit = auto_limit
            self.stdout.write(self.style.WARNING(f'🔄 Автоматически установлен limit = {limit} игр'))
        else:
            self.stdout.write(self.style.WARNING(f'📊 Ручной limit = {limit} игр'))

        self.current_offset = options['offset']
        game_name = options['game_name']
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        iteration_pause = options['iteration_pause']
        self.workers = options['workers']
        delay = options['delay']
        timeout = options['timeout']
        output_file = options['output_file']
        force = options['force']
        skip_search = options['skip_search']
        self.debug = options['debug']
        self.verbose = options['verbose']
        no_restart = options['no_restart']
        output_dir = options['output_dir']

        # Создаем директорию
        self.stdout.write(self.style.WARNING(f'🔍 ДИАГНОСТИКА: Создание директории {output_dir}'))
        self.output_dir = Path(output_dir)
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.log_debug(f"Создана выходная директория: {self.output_dir}")
        except Exception as e:
            self.log_debug("Ошибка при создании выходной директории", error=e)
            self.stdout.write(self.style.ERROR(f'❌ Не удалось создать директорию {output_dir}: {e}'))
            return

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('STEAM DESCRIPTIONS FETCHER'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        # Поиск конкретной игры
        target_game = None
        if game_name:
            self.stdout.write(f'🔍 Поиск игры: "{game_name}"')
            self.log_debug(f"Начат поиск игры по названию: {game_name}")

            # Сначала точный поиск
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
                # Поиск по частичному совпадению
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

                    # Диагностика: показываем похожие игры
                    if self.debug:
                        similar = Game.objects.filter(platforms=pc)[:10]
                        self.stdout.write(self.style.WARNING('📋 Доступные игры (первые 10):'))
                        for g in similar:
                            self.stdout.write(f'  - {g.name}')
                    return

            # Устанавливаем параметры для одной игры
            limit = 1
            self.current_offset = 0
            batch_size = 1
            no_restart = True

        # Вывод параметров
        self.stdout.write(f'\n📊 Параметры запуска:')
        self.stdout.write(f'  Лимит: {limit}')
        self.stdout.write(f'  Текущий offset: {self.current_offset}')
        self.stdout.write(f'  Размер итерации: {batch_size}')
        self.stdout.write(f'  Пауза между итерациями: {iteration_pause}с')
        self.stdout.write(f'  Воркеров: {self.workers}')
        self.stdout.write(f'  Задержка: {delay}с')
        self.stdout.write(f'  Таймаут: {timeout}с')
        self.stdout.write(f'  Dry run: {dry_run}')
        self.stdout.write(f'  Force: {force}')
        self.stdout.write(f'  Skip search: {skip_search}')

        self.stdout.write(self.style.WARNING(f'\n🚫 Настройки защиты от ошибок:'))
        self.stdout.write(f'  Макс. ошибок подряд: {options["max_consecutive_failures"]}')
        self.stdout.write(f'  Базовая пауза: {options["base_wait"]}с')
        self.stdout.write(f'  Макс. пауза: {options["max_wait"]}с')
        self.stdout.write(f'  Порог ошибок в батче: {options["batch_failure_threshold"] * 100}%')

        self.stdout.write('=' * 60)
        self.stdout.write(self.style.WARNING('⚠️  Нажмите Ctrl+C для завершения'))
        self.stdout.write('=' * 60)

        # Основной цикл
        iteration = 0
        processed_total = 0
        last_eta_update = time.time()
        games_per_second = 0

        self.stdout.write(self.style.WARNING(
            f'🔍 ДИАГНОСТИКА: Вход в основной цикл, current_offset={self.current_offset}, limit={limit}'))

        while self.current_offset < limit and not self.interrupted:
            iteration += 1
            iteration_start_time = time.time()

            self.stdout.write(
                self.style.WARNING(f'\n🔍 ДИАГНОСТИКА: НАЧАЛО ИТЕРАЦИИ {iteration}, offset={self.current_offset}'))
            self.stdout.write(self.style.SUCCESS(f'\n🔄 ИТЕРАЦИЯ {iteration} (offset {self.current_offset})'))

            # Проверяем rate limiting перед каждой итерацией
            self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Проверка rate limiting'))
            if self.rate_limiter and self.rate_limiter.should_backoff():
                self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Обнаружен backoff, вызываю check_rate_limit()'))
                if not self.check_rate_limit():
                    self.stdout.write(
                        self.style.WARNING('🔍 ДИАГНОСТИКА: check_rate_limit() вернул False, прерываю цикл'))
                    break

            # Получаем игры
            self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Получение игр для обработки'))
            games_to_process = []
            if target_game:
                games_to_process = [target_game]
                self.stdout.write(self.style.SUCCESS(f'📊 Обработка конкретной игры: {target_game.name}'))
            else:
                games_to_process = self.get_games_batch(self.current_offset, batch_size, force)

            self.stdout.write(self.style.WARNING(f'🔍 ДИАГНОСТИКА: Получено {len(games_to_process)} игр'))

            if not games_to_process:
                self.stdout.write(self.style.WARNING('⚠️ Нет игр для обработки в этой итерации'))
                self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Нет игр, прерываю цикл'))
                break

            # Статистика батча
            batch_stats = {
                'success': 0,
                'not_found': 0,
                'no_description': 0,
                'error': 0
            }

            # Обрабатываем батч
            self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Вызов process_batch()'))
            games_to_update = self.process_batch(
                games_to_process, skip_search, timeout, delay,
                output_file, dry_run, batch_stats,
                is_first=(self.current_offset == 0 and iteration == 1)
            )
            self.stdout.write(
                self.style.WARNING(f'🔍 ДИАГНОСТИКА: process_batch() вернул {len(games_to_update)} игр для обновления'))

            # Обновляем processed_total
            processed_total += len(games_to_process)

            # Расчет скорости и ETA
            iteration_time = time.time() - iteration_start_time
            current_games_per_second = len(games_to_process) / max(iteration_time, 0.1)

            # Сглаживаем скорость (экспоненциальное скользящее среднее)
            if games_per_second == 0:
                games_per_second = current_games_per_second
            else:
                games_per_second = games_per_second * 0.7 + current_games_per_second * 0.3

            remaining_games = limit - self.current_offset - len(games_to_process)
            eta_seconds = remaining_games / max(games_per_second, 0.01) if remaining_games > 0 else 0

            # Форматируем ETA
            if eta_seconds > 0:
                eta_hours = int(eta_seconds // 3600)
                eta_minutes = int((eta_seconds % 3600) // 60)
                eta_seconds_display = int(eta_seconds % 60)

                if eta_hours > 0:
                    eta_str = f"{eta_hours}ч {eta_minutes}м {eta_seconds_display}с"
                elif eta_minutes > 0:
                    eta_str = f"{eta_minutes}м {eta_seconds_display}с"
                else:
                    eta_str = f"{eta_seconds_display}с"
            else:
                eta_str = "завершение"

            # Сохраняем буфер
            self.stdout.write(
                self.style.WARNING(f'🔍 ДИАГНОСТИКА: Проверка буфера, размер={len(self.descriptions_buffer)}'))
            if self.descriptions_buffer:
                self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Сохранение буфера'))
                self.save_buffer(output_file, is_first=(self.current_offset == 0 and iteration == 1))

            # Обновляем БД
            if games_to_update and not dry_run:
                self.stdout.write(self.style.WARNING(f'🔍 ДИАГНОСТИКА: Обновление {len(games_to_update)} игр в БД'))
                try:
                    with transaction.atomic():
                        Game.objects.bulk_update(games_to_update, ['rawg_description'])
                    self.stdout.write(self.style.SUCCESS(f'✅ Обновлено {len(games_to_update)} игр в БД'))
                except Exception as e:
                    self.log_debug("Ошибка при обновлении БД", error=e)
                    self.stdout.write(self.style.ERROR(f'🔍 ДИАГНОСТИКА: Ошибка при обновлении БД: {e}'))

            # Прогресс бар и статистика
            progress = (processed_total / limit * 100) if limit > 0 else 0
            elapsed_time = time.time() - start_time
            elapsed_hours = int(elapsed_time // 3600)
            elapsed_minutes = int((elapsed_time % 3600) // 60)
            elapsed_seconds = int(elapsed_time % 60)

            if elapsed_hours > 0:
                elapsed_str = f"{elapsed_hours}ч {elapsed_minutes}м {elapsed_seconds}с"
            elif elapsed_minutes > 0:
                elapsed_str = f"{elapsed_minutes}м {elapsed_seconds}с"
            else:
                elapsed_str = f"{elapsed_seconds}с"

            # Рисуем прогресс бар
            bar_length = 40
            filled_length = int(bar_length * progress / 100)
            bar = '█' * filled_length + '░' * (bar_length - filled_length)

            self.stdout.write(self.style.SUCCESS(f'\n📊 ПРОГРЕСС: [{bar}] {progress:.1f}%'))
            self.stdout.write(self.style.SUCCESS(f'📊 Обработано: {processed_total}/{limit} игр'))
            self.stdout.write(self.style.SUCCESS(
                f'⏱️ Прошло: {elapsed_str} | ⏳ Осталось: {eta_str} | ⚡ Скорость: {games_per_second:.1f} игр/с'))

            # Статистика итерации
            total_in_batch = sum(batch_stats.values())
            success_rate = (batch_stats['success'] / total_in_batch * 100) if total_in_batch > 0 else 0

            self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Вывод статистики итерации'))
            self.stdout.write(self.style.SUCCESS(f'\n📊 ИТОГ ИТЕРАЦИИ {iteration}:'))
            self.stdout.write(f'  ✓ Успешно: {batch_stats["success"]} ({success_rate:.1f}%)')
            self.stdout.write(f'  🔍 Не найдено в Steam: {batch_stats["not_found"]}')
            self.stdout.write(f'  📄 Нет описания: {batch_stats["no_description"]}')

            if batch_stats['error'] > 0:
                self.stdout.write(self.style.ERROR(f'  💥 Ошибок запросов: {batch_stats["error"]}'))

                # Показываем детализацию ошибок если есть
                error_details = []
                if self.total_stats['error_403'] > 0:
                    error_details.append(f'403: {self.total_stats["error_403"]}')
                if self.total_stats['error_timeout'] > 0:
                    error_details.append(f'Таймаут: {self.total_stats["error_timeout"]}')
                if self.total_stats['error_other'] > 0:
                    error_details.append(f'Другие: {self.total_stats["error_other"]}')

                if error_details:
                    self.stdout.write(self.style.WARNING(f'     └─ {", ".join(error_details)}'))

            # Показываем статус rate limiter
            if self.rate_limiter:
                rl_status = self.rate_limiter.get_status()
                if rl_status['total_failures'] > 0:
                    self.stdout.write(self.style.WARNING(
                        f'  📊 Статистика ошибок: всего {rl_status["total_failures"]}, подряд {rl_status["consecutive_failures"]}'
                    ))

            self.total_stats['iterations'] += 1

            # Подготовка к следующей итерации
            next_offset = self.current_offset + batch_size

            if not no_restart and not target_game and next_offset < limit and not self.interrupted:
                self.stdout.write(self.style.WARNING(f'\n🔄 Следующая итерация через {iteration_pause}с...'))

                # Обратный отсчет
                for i in range(iteration_pause, 0, -1):
                    if i > 1:
                        time.sleep(1)
                        self.stdout.write(self.style.WARNING(f'   Осталось {i}с...'))
                    else:
                        time.sleep(1)
                        break

                # Формируем команду для следующего оффсета
                cmd = [
                    sys.executable, "manage.py", "fetch_steam_descriptions",
                    f"--limit={limit}",
                    f"--offset={next_offset}",
                    f"--batch-size={batch_size}",
                    f"--iteration-pause={iteration_pause}",
                    f"--workers={self.workers}",
                    f"--delay={delay}",
                    f"--timeout={timeout}",
                    f"--output-file={output_file}",
                    f"--output-dir={output_dir}",
                    f"--max-consecutive-failures={options['max_consecutive_failures']}",
                    f"--base-wait={options['base_wait']}",
                    f"--max-wait={options['max_wait']}",
                    f"--batch-failure-threshold={options['batch_failure_threshold']}",
                ]

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
                self.stdout.write('')

                try:
                    # Показываем время выполнения текущей итерации
                    elapsed = time.time() - start_time
                    self.stdout.write(self.style.SUCCESS(f'⏱️ Время выполнения текущей итерации: {elapsed:.1f}с'))

                    # Запускаем дочерний процесс и выходим
                    self.stdout.write(self.style.WARNING('🔄 Завершаю текущий процесс, продолжение в новом...'))

                    # Сохраняем буфер перед выходом
                    if self.descriptions_buffer:
                        self.save_buffer(output_file, is_first=(self.current_offset == 0 and iteration == 1))

                    # Запускаем новый процесс
                    subprocess.Popen(cmd, shell=False)

                    # Завершаем текущий процесс
                    self.stdout.write(self.style.SUCCESS('✅ Текущий процесс завершен, продолжение в новом процессе'))
                    sys.exit(0)

                except Exception as e:
                    self.log_debug("Ошибка при перезапуске команды", error=e)
                    self.stdout.write(self.style.ERROR(f'❌ Ошибка при перезапуске: {e}'))

                    # Пробуем продолжить в текущем процессе
                    self.current_offset = next_offset
                    continue
            else:
                # Завершаем цикл
                if next_offset >= limit:
                    self.stdout.write(self.style.SUCCESS('\n✅ Достигнут лимит, все итерации завершены'))
                elif no_restart:
                    self.stdout.write(self.style.WARNING('\n⏹️ Автоперезапуск отключен (--no-restart)'))
                elif target_game:
                    self.stdout.write(self.style.SUCCESS('\n✅ Обработка конкретной игры завершена'))
                break

        # Финальная статистика
        self.print_final_stats(start_time)

    def print_final_stats(self, start_time: float) -> None:
        """Вывод финальной статистики."""
        self.stdout.write(self.style.WARNING('\n🔍 ДИАГНОСТИКА: print_final_stats()'))

        elapsed_time = time.time() - start_time

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 60))
        self.stdout.write(self.style.SUCCESS('📊 ИТОГОВАЯ СТАТИСТИКА'))
        self.stdout.write('=' * 60)
        self.stdout.write(f'  ✓ Успешно обработано: {self.total_stats["success"]}')
        self.stdout.write(f'  🔍 Не найдено в Steam: {self.total_stats["not_found"]}')
        self.stdout.write(f'  📄 Нет описания: {self.total_stats["no_description"]}')
        self.stdout.write(f'  🚫 403 ошибок: {self.total_stats["error_403"]}')
        self.stdout.write(f'  ⌛ Таймаутов: {self.total_stats["error_timeout"]}')

        if self.total_stats['error_other'] > 0:
            self.stdout.write(self.style.ERROR(f'  💥 Прочих ошибок: {self.total_stats["error_other"]}'))

        if self.total_stats['backoff_pauses'] > 0:
            self.stdout.write(self.style.WARNING(f'  ⏸️ Пауз из-за ошибок: {self.total_stats["backoff_pauses"]}'))

        self.stdout.write(f'  🔄 Выполнено итераций: {self.total_stats["iterations"]}')
        self.stdout.write(f'  ⏱️ Общее время: {elapsed_time:.1f}с')

        # Финальный вывод о файле
        if self.full_output_path and self.full_output_path.exists():
            size = self.full_output_path.stat().st_size
            line_count = 0
            try:
                with open(self.full_output_path, 'r', encoding='utf-8') as f:
                    line_count = sum(1 for _ in f)
            except:
                pass

            self.stdout.write(self.style.SUCCESS(f'\n📁 Результаты сохранены в файл: {self.full_output_path}'))
            self.stdout.write(self.style.SUCCESS(f'📊 Размер файла: {size:,} байт ({size / 1024:.1f} КБ)'))
            self.stdout.write(self.style.SUCCESS(f'📝 Строк в файле: ~{line_count}'))
        else:
            # Проверяем есть ли файл в директории
            expected_path = self.output_dir / output_file
            if expected_path.exists():
                size = expected_path.stat().st_size
                self.stdout.write(self.style.SUCCESS(f'\n📁 Результаты сохранены в файл: {expected_path}'))
                self.stdout.write(self.style.SUCCESS(f'📊 Размер файла: {size:,} байт ({size / 1024:.1f} КБ)'))
            else:
                self.stdout.write(self.style.WARNING(f'\n📁 Файл не создан (нет данных или dry run)'))

        # Выводим все ошибки если были
        if self.error_log and self.debug:
            self.stdout.write(self.style.WARNING('\n📋 ПОЛНЫЙ ЛОГ ОШИБОК:'))
            for i, error in enumerate(self.error_log, 1):
                self.stdout.write(self.style.WARNING(f'  {i}. {error}'))

        self.stdout.write('=' * 60)
        self.stdout.write(self.style.WARNING('🔍 ДИАГНОСТИКА: Конец print_final_stats()'))

        # Принудительное завершение на всякий случай
        sys.stdout.flush()
        os._exit(0)
