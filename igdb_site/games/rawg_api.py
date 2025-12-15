"""
Модуль для работы с RAWG API с кэшированием и управлением лимитами запросов.
"""
import time
import requests
import re
import hashlib
import sqlite3
import threading
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from django.conf import settings


class RAWGClient:
    def __init__(self, api_key=None):
        """
        Инициализация клиента RAWG API.
        """
        self.api_key = api_key or getattr(settings, 'RAWG_API_KEY', None)
        if not self.api_key:
            raise ValueError("RAWG API key is not configured. Set RAWG_API_KEY in .env file.")

        self.request_timestamps = []
        self.api_stats = defaultdict(int)
        self.local = threading.local()

        # Атрибуты для баланса
        self.requests_remaining = None
        self.monthly_limit = None
        self.reset_date = None
        self.last_balance_check = None
        self.balance_error = None
        self.balance_valid = True
        self.balance_exceeded = False  # Добавляем флаг
        self.balance_checked = False  # Флаг что баланс уже проверен

        # Только инициализация кэша, НЕ проверка баланса
        self.init_cache_structure()

    def check_balance(self, force=False):
        """
        Проверка баланса API ключа.

        Args:
            force (bool): Принудительная проверка, даже если уже проверяли
        """
        # Если уже проверяли и не force - пропускаем
        if self.balance_checked and not force:
            return {
                'status': 'already_checked',
                'balance_exceeded': self.balance_exceeded,
                'balance_error': self.balance_error,
                'requests_remaining': self.requests_remaining,
                'monthly_limit': self.monthly_limit,
                'reset_date': self.reset_date
            }

        try:
            url = "https://api.rawg.io/api/games"
            params = {
                'key': self.api_key,
                'page_size': 1
            }

            response = requests.get(url, params=params, timeout=10)
            self.last_balance_check = datetime.now()
            self.balance_checked = True  # Отмечаем что проверили

            if response.status_code == 200:
                headers = response.headers
                self.requests_remaining = headers.get('X-RateLimit-Remaining')
                self.monthly_limit = headers.get('X-RateLimit-Limit')
                self.reset_date = headers.get('X-RateLimit-Reset')
                self.balance_error = None
                self.balance_valid = True
                self.balance_exceeded = False

                return {
                    'status': 'ok',
                    'requests_remaining': self.requests_remaining,
                    'monthly_limit': self.monthly_limit,
                    'reset_date': self.reset_date
                }

            elif response.status_code == 429:
                self.balance_error = "🚫 Превышен лимит запросов (429). Лимит API ключа исчерпан."
                self.balance_valid = False
                self.balance_exceeded = True
                return {'status': 'rate_limit_exceeded', 'message': self.balance_error}

            elif response.status_code == 401:
                self.balance_error = "🚫 Неверный API ключ (401)."
                self.balance_valid = False
                self.balance_exceeded = True
                return {'status': 'invalid_key', 'message': self.balance_error}

            elif response.status_code == 403:
                self.balance_error = "🚫 Доступ запрещен (403). Возможно, лимит запросов исчерпан."
                self.balance_valid = False
                self.balance_exceeded = True
                return {'status': 'forbidden', 'message': self.balance_error}

            else:
                self.balance_error = f"Ошибка API: {response.status_code}"
                self.balance_valid = False
                return {
                    'status': 'api_error',
                    'message': self.balance_error,
                    'status_code': response.status_code
                }

        except requests.exceptions.Timeout:
            self.balance_error = "Таймаут при проверке баланса API."
            self.balance_valid = False
            return {'status': 'timeout', 'message': self.balance_error}
        except Exception as e:
            self.balance_error = f"Ошибка при проверке баланса: {str(e)}"
            self.balance_valid = False
            return {'status': 'error', 'message': self.balance_error}

    def enforce_rate_limit(self, delay=0.3):
        """
        Обеспечение соблюдения лимитов запросов к API.

        Args:
            delay (float): Базовая задержка между запросами.
        """
        # Проверяем, не исчерпан ли баланс (без вывода сообщения)
        if self.balance_exceeded:
            raise ValueError(f"Лимит API ключа исчерпан: {self.balance_error}")

        now = time.time()
        self.request_timestamps = [t for t in self.request_timestamps if now - t < 1.0]

        if len(self.request_timestamps) >= 2:
            sleep_time = 1.0 - (now - self.request_timestamps[0])
            if sleep_time > 0:
                time.sleep(sleep_time)

        self.request_timestamps.append(now)

        if delay > 0:
            time.sleep(delay)

    def search_game(self, game_name, delay=0.3):
        """
        Поиск игры в RAWG API.
        """
        try:
            self.enforce_rate_limit(delay)

            url = "https://api.rawg.io/api/games"
            params = {
                'key': self.api_key,
                'search': game_name,
                'page_size': 1,
                'search_precise': True,
                'search_exact': False
            }

            response = requests.get(url, params=params, timeout=8)

            if response.status_code == 429:
                self.api_stats['rate_limited'] += 1
                self.balance_exceeded = True
                error_msg = "🚫 Превышен лимит запросов (429). Лимит API ключа исчерпан."
                self.balance_error = error_msg
                raise ValueError(error_msg)

            if response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                if results:
                    return results[0]

            return None

        except requests.exceptions.Timeout:
            print(f'   ⏰ Таймаут поиска: {game_name}')
            return None
        except Exception as e:
            print(f'   ⚠️ Ошибка поиска: {game_name} - {e}')
            return None

    def get_game_details(self, rawg_id, delay=0.3):
        """
        Получение детальной информации об игре.
        """
        try:
            self.enforce_rate_limit(delay)

            url = f"https://api.rawg.io/api/games/{rawg_id}"
            params = {'key': self.api_key}

            response = requests.get(url, params=params, timeout=8)

            if response.status_code == 429:
                self.api_stats['rate_limited'] += 1
                self.balance_exceeded = True
                error_msg = "🚫 Превышен лимит запросов (429). Лимит API ключа исчерпан."
                self.balance_error = error_msg
                raise ValueError(error_msg)

            if response.status_code == 200:
                return response.json()

            return None

        except requests.exceptions.Timeout:
            print(f'   ⏰ Таймаут деталей: {rawg_id}')
            return None
        except Exception as e:
            print(f'   ⚠️ Ошибка получения деталей: {rawg_id} - {e}')
            return None

    def get_game_description(self, game_name, min_length=1, delay=0.3,
                             use_cache=True, cache_ttl=30):
        """
        Получение описания игры из RAWG с использованием кэша.

        ВСЕГДА возвращает словарь с ключом 'status'
        """
        try:
            # ТОЛЬКО если баланс еще не проверяли - проверяем один раз
            if not self.balance_checked:
                balance_info = self.check_balance(force=True)

                if self.balance_exceeded:
                    return {
                        'status': 'balance_exceeded',
                        'error': f"Проблема с балансом API: {self.balance_error}",
                        'balance_info': balance_info
                    }

            # Если баланс исчерпан - сразу возвращаем ошибку
            if self.balance_exceeded:
                return {
                    'status': 'balance_exceeded',
                    'error': f"Баланс API ключа исчерпан: {self.balance_error}",
                    'balance_exceeded': True
                }

            normalized = self.normalize_game_name(game_name)
            game_hash = normalized['hash']

            # 1. Пробуем получить из кэша
            if use_cache:
                cached = self.get_from_cache(game_hash, cache_ttl)
                if cached:
                    if cached['found'] and cached['description']:
                        self.api_stats['cache_hits'] += 1
                        return {
                            'status': 'found',
                            'description': cached['description'],
                            'source': 'cache',
                            'rawg_id': cached['rawg_id']
                        }
                    elif not cached['found']:
                        self.api_stats['cache_hits'] += 1
                        return {
                            'status': 'not_found',
                            'source': 'cache'
                        }

            self.api_stats['cache_misses'] += 1

            # 2. Поиск в RAWG API
            search_result = self.search_game(normalized['search'], delay)
            self.api_stats['search_requests'] += 1

            # Если поиск вернул None из-за проблем с балансом
            if search_result is None and self.balance_exceeded:
                return {
                    'status': 'balance_exceeded',
                    'error': f"Проблема с балансом API: {self.balance_error}"
                }

            if not search_result:
                # Сохраняем в кэш как ненайденную игру
                self.save_to_cache(
                    game_name, game_hash,
                    found=False,
                    source='not_found_search'
                )
                return {'status': 'not_found', 'source': 'search'}

            # 3. Проверяем описание в результатах поиска
            description = (
                    search_result.get('description') or
                    search_result.get('description_raw') or
                    ''
            )

            if description and len(description.strip()) >= min_length:
                description = self.clean_description(description)
                self.save_to_cache(
                    game_name, game_hash,
                    rawg_id=search_result.get('id'),
                    description=description,
                    source='search',
                    found=True
                )
                return {
                    'status': 'found',
                    'description': description,
                    'source': 'search',
                    'rawg_id': search_result.get('id')
                }

            # 4. Получаем детали, если описание не найдено в поиске
            game_id = search_result.get('id')
            if game_id:
                details = self.get_game_details(game_id, delay)
                self.api_stats['detail_requests'] += 1

                if details:
                    description = (
                            details.get('description') or
                            details.get('description_raw') or
                            ''
                    )
                    if description and len(description.strip()) >= min_length:
                        description = self.clean_description(description)
                        self.save_to_cache(
                            game_name, game_hash,
                            rawg_id=game_id,
                            description=description,
                            source='details',
                            found=True
                        )
                        return {
                            'status': 'found',
                            'description': description,
                            'source': 'details',
                            'rawg_id': game_id
                        }

            # 5. Слишком короткое или пустое описание
            if description:
                description = self.clean_description(description)
                self.save_to_cache(
                    game_name, game_hash,
                    rawg_id=search_result.get('id'),
                    description=description,
                    source='short',
                    found=True
                )
                return {
                    'status': 'found',
                    'description': description,
                    'source': 'short',
                    'rawg_id': search_result.get('id')
                }
            else:
                self.save_to_cache(
                    game_name, game_hash,
                    rawg_id=search_result.get('id'),
                    description='',
                    source='empty',
                    found=True
                )
                return {
                    'status': 'found',
                    'description': '',
                    'source': 'empty',
                    'rawg_id': search_result.get('id')
                }

        except ValueError as e:
            # Проверяем, не ошибка ли баланса
            if "лимит исчерпан" in str(e) or "429" in str(e) or "баланс" in str(e).lower():
                self.balance_exceeded = True
                return {
                    'status': 'balance_exceeded',
                    'error': str(e),
                    'balance_exceeded': True
                }
            else:
                return {
                    'status': 'error',
                    'error': str(e),
                    'game_name': game_name
                }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'game_name': game_name
            }

    def get_cache_connection(self):
        """Получение соединения с кэшем для текущего потока."""
        if not hasattr(self.local, 'cache_conn'):
            try:
                cache_dir = Path('cache')
                cache_dir.mkdir(exist_ok=True)

                self.local.cache_conn = sqlite3.connect(
                    cache_dir / 'rawg_cache.db',
                    timeout=10,
                    check_same_thread=False  # Разрешаем использование в разных потоках
                )
                self.local.cache_conn.execute('PRAGMA journal_mode = WAL')
                self.local.cache_conn.execute('PRAGMA synchronous = NORMAL')

                # Создаем таблицы если их нет
                cursor = self.local.cache_conn.cursor()
                cursor.execute('''
                               CREATE TABLE IF NOT EXISTS rawg_cache
                               (
                                   game_hash
                                   TEXT
                                   PRIMARY
                                   KEY,
                                   game_name
                                   TEXT
                                   NOT
                                   NULL,
                                   rawg_id
                                   INTEGER,
                                   description
                                   TEXT,
                                   description_source
                                   TEXT,
                                   found
                                   BOOLEAN
                                   DEFAULT
                                   1,
                                   created_at
                                   TIMESTAMP
                                   DEFAULT
                                   CURRENT_TIMESTAMP,
                                   updated_at
                                   TIMESTAMP
                                   DEFAULT
                                   CURRENT_TIMESTAMP,
                                   request_count
                                   INTEGER
                                   DEFAULT
                                   1,
                                   last_balance_check
                                   TIMESTAMP,
                                   balance_status
                                   TEXT
                                   DEFAULT
                                   'unknown'
                               )
                               ''')

                cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_hash ON rawg_cache(game_hash)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_name ON rawg_cache(game_name)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_updated_at ON rawg_cache(updated_at)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_balance_status ON rawg_cache(balance_status)')

                cursor.execute('''
                               CREATE TABLE IF NOT EXISTS normalized_names
                               (
                                   original_name
                                   TEXT
                                   PRIMARY
                                   KEY,
                                   normalized_name
                                   TEXT
                                   NOT
                                   NULL,
                                   created_at
                                   TIMESTAMP
                                   DEFAULT
                                   CURRENT_TIMESTAMP
                               )
                               ''')

                # Таблица для хранения истории проверок баланса
                cursor.execute('''
                               CREATE TABLE IF NOT EXISTS balance_history
                               (
                                   id
                                   INTEGER
                                   PRIMARY
                                   KEY
                                   AUTOINCREMENT,
                                   check_date
                                   TIMESTAMP
                                   DEFAULT
                                   CURRENT_TIMESTAMP,
                                   status_code
                                   INTEGER,
                                   response_text
                                   TEXT,
                                   balance_status
                                   TEXT,
                                   requests_remaining
                                   INTEGER,
                                   monthly_limit
                                   INTEGER,
                                   reset_date
                                   TEXT
                               )
                               ''')

                self.local.cache_conn.commit()

            except Exception as e:
                print(f'⚠️ Ошибка инициализации кэша для потока: {e}')
                self.local.cache_conn = None

        return self.local.cache_conn

    def init_cache_structure(self):
        """Инициализация структуры кэша (однократно при запуске)."""
        try:
            cache_dir = Path('cache')
            cache_dir.mkdir(exist_ok=True)

            # Создаем временное соединение для инициализации структуры
            conn = sqlite3.connect(cache_dir / 'rawg_cache.db', timeout=10)
            conn.execute('PRAGMA journal_mode = WAL')
            conn.execute('PRAGMA synchronous = NORMAL')

            cursor = conn.cursor()

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS rawg_cache
                           (
                               game_hash
                               TEXT
                               PRIMARY
                               KEY,
                               game_name
                               TEXT
                               NOT
                               NULL,
                               rawg_id
                               INTEGER,
                               description
                               TEXT,
                               description_source
                               TEXT,
                               found
                               BOOLEAN
                               DEFAULT
                               1,
                               created_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP,
                               updated_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP,
                               request_count
                               INTEGER
                               DEFAULT
                               1,
                               last_balance_check
                               TIMESTAMP,
                               balance_status
                               TEXT
                               DEFAULT
                               'unknown'
                           )
                           ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_hash ON rawg_cache(game_hash)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_name ON rawg_cache(game_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_updated_at ON rawg_cache(updated_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_balance_status ON rawg_cache(balance_status)')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS normalized_names
                           (
                               original_name
                               TEXT
                               PRIMARY
                               KEY,
                               normalized_name
                               TEXT
                               NOT
                               NULL,
                               created_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP
                           )
                           ''')

            # Таблица для хранения истории проверок баланса
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS balance_history
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               check_date
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP,
                               status_code
                               INTEGER,
                               response_text
                               TEXT,
                               balance_status
                               TEXT,
                               requests_remaining
                               INTEGER,
                               monthly_limit
                               INTEGER,
                               reset_date
                               TEXT
                           )
                           ''')

            conn.commit()
            conn.close()

        except Exception as e:
            print(f'⚠️ Ошибка инициализации структуры кэша: {e}')

    def init_stats_db_structure(self):
        """Инициализация структуры БД для статистики."""
        try:
            stats_dir = Path('stats')
            stats_dir.mkdir(exist_ok=True)

            # Создаем временное соединение для инициализации структуры
            conn = sqlite3.connect(stats_dir / 'api_stats.db', timeout=10)
            cursor = conn.cursor()

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS request_stats
                           (
                               date
                               TEXT
                               PRIMARY
                               KEY,
                               total_requests
                               INTEGER
                               DEFAULT
                               0,
                               cache_hits
                               INTEGER
                               DEFAULT
                               0,
                               cache_misses
                               INTEGER
                               DEFAULT
                               0,
                               search_requests
                               INTEGER
                               DEFAULT
                               0,
                               detail_requests
                               INTEGER
                               DEFAULT
                               0,
                               rate_limited
                               INTEGER
                               DEFAULT
                               0,
                               balance_exceeded
                               INTEGER
                               DEFAULT
                               0,
                               avg_response_time
                               REAL
                               DEFAULT
                               0
                           )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS game_stats
                           (
                               game_hash
                               TEXT
                               PRIMARY
                               KEY,
                               game_name
                               TEXT
                               NOT
                               NULL,
                               found_count
                               INTEGER
                               DEFAULT
                               0,
                               not_found_count
                               INTEGER
                               DEFAULT
                               0,
                               error_count
                               INTEGER
                               DEFAULT
                               0,
                               last_checked
                               TIMESTAMP,
                               first_found
                               TIMESTAMP
                           )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS efficiency_stats
                           (
                               timestamp
                               TEXT
                               PRIMARY
                               KEY,
                               cache_efficiency
                               REAL,
                               requests_saved
                               INTEGER,
                               avg_requests_per_game
                               REAL,
                               balance_status
                               TEXT
                           )
                           ''')

            conn.commit()
            conn.close()

        except Exception as e:
            print(f'⚠️ Ошибка инициализации структуры статистики: {e}')

    def normalize_game_name(self, game_name):
        """
        Нормализация названия игры для поиска и кэширования.

        Args:
            game_name (str): Оригинальное название игры.

        Returns:
            dict: Хэш и нормализованное название.
        """
        normalized = game_name.lower().strip()
        normalized = re.sub(r'\s+', ' ', normalized)

        # Для поиска удаляем версии и года в скобках
        search_name = re.sub(r'\([^)]*\)', '', normalized)
        search_name = re.sub(
            r'\b(remastered|definitive edition|game of the year|goty|enhanced edition)\b',
            '', search_name, flags=re.IGNORECASE
        )
        search_name = search_name.strip()

        # Хэш для уникальности
        name_hash = hashlib.md5(game_name.encode('utf-8')).hexdigest()

        return {
            'hash': name_hash,
            'original': game_name,
            'search': search_name if search_name else normalized
        }

    def _save_balance_check(self, response):
        """Сохранение информации о проверке баланса в БД."""
        cache_conn = self.get_cache_connection()
        if not cache_conn:
            return

        try:
            cursor = cache_conn.cursor()

            # Определяем статус баланса на основе кода ответа
            if response.status_code == 200:
                balance_status = 'ok'
            elif response.status_code == 429:
                balance_status = 'rate_limit_exceeded'
            elif response.status_code in [401, 403]:
                balance_status = 'access_denied'
            else:
                balance_status = 'error'

            # Извлекаем информацию о лимитах из заголовков
            headers = response.headers
            remaining = headers.get('X-RateLimit-Remaining')
            limit = headers.get('X-RateLimit-Limit')
            reset = headers.get('X-RateLimit-Reset')

            # Сохраняем в историю
            cursor.execute('''
                           INSERT INTO balance_history
                           (status_code, response_text, balance_status, requests_remaining, monthly_limit, reset_date)
                           VALUES (?, ?, ?, ?, ?, ?)
                           ''', (
                               response.status_code,
                               response.text[:500] if response.text else '',
                               balance_status,
                               remaining,
                               limit,
                               reset
                           ))

            # Обновляем статус в основной таблице кэша
            cursor.execute('''
                           UPDATE rawg_cache
                           SET last_balance_check = CURRENT_TIMESTAMP,
                               balance_status     = ?
                           WHERE game_hash IN (SELECT game_hash FROM rawg_cache LIMIT 1)
                           ''', (balance_status,))

            cache_conn.commit()

        except Exception as e:
            print(f'⚠️ Ошибка сохранения проверки баланса: {e}')

    def get_from_cache(self, game_hash, cache_ttl=30):
        """
        Получение данных из кэша.

        Args:
            game_hash (str): Хэш названия игры.
            cache_ttl (int): Время жизни кэша в днях.

        Returns:
            dict or None: Кэшированные данные или None.
        """
        cache_conn = self.get_cache_connection()
        if not cache_conn:
            return None

        try:
            cursor = cache_conn.cursor()

            ttl_condition = ""
            if cache_ttl > 0:
                ttl_condition = f"AND updated_at > datetime('now', '-{cache_ttl} days')"

            cursor.execute(f'''
                SELECT rawg_id, description, description_source, found, request_count, balance_status
                FROM rawg_cache 
                WHERE game_hash = ? {ttl_condition}
            ''', (game_hash,))

            result = cursor.fetchone()
            if result:
                cursor.execute('''
                               UPDATE rawg_cache
                               SET request_count = request_count + 1,
                                   updated_at    = CURRENT_TIMESTAMP
                               WHERE game_hash = ?
                               ''', (game_hash,))
                cache_conn.commit()

                return {
                    'rawg_id': result[0],
                    'description': result[1],
                    'source': result[2],
                    'found': bool(result[3]),
                    'request_count': result[4],
                    'balance_status': result[5]
                }

            return None

        except Exception as e:
            print(f'   ⚠️ Ошибка получения из кэша: {e}')
            return None

    def save_to_cache(self, game_name, game_hash, rawg_id=None,
                      description=None, source=None, found=True):
        """
        Сохранение данных в кэш.

        Args:
            game_name (str): Название игры.
            game_hash (str): Хэш названия.
            rawg_id (int): ID игры в RAWG.
            description (str): Описание игры.
            source (str): Источник описания.
            found (bool): Найдена ли игра.
        """
        cache_conn = self.get_cache_connection()
        if not cache_conn:
            return

        try:
            cursor = cache_conn.cursor()

            # Проверяем текущий статус баланса
            balance_info = self.check_balance()
            balance_status = balance_info.get('status', 'unknown') if balance_info else 'unknown'

            cursor.execute('SELECT 1 FROM rawg_cache WHERE game_hash = ?', (game_hash,))
            exists = cursor.fetchone()

            if exists:
                cursor.execute('''
                               UPDATE rawg_cache
                               SET rawg_id            = COALESCE(?, rawg_id),
                                   description        = COALESCE(?, description),
                                   description_source = COALESCE(?, description_source),
                                   found              = ?,
                                   updated_at         = CURRENT_TIMESTAMP,
                                   request_count      = request_count + 1,
                                   last_balance_check = CURRENT_TIMESTAMP,
                                   balance_status     = ?
                               WHERE game_hash = ?
                               ''', (rawg_id, description, source, found, balance_status, game_hash))
            else:
                cursor.execute('''
                               INSERT INTO rawg_cache
                               (game_hash, game_name, rawg_id, description, description_source, found,
                                request_count, last_balance_check, balance_status)
                               VALUES (?, ?, ?, ?, ?, ?, 1, CURRENT_TIMESTAMP, ?)
                               ''', (game_hash, game_name, rawg_id, description, source, found, balance_status))

            cache_conn.commit()

        except Exception as e:
            print(f'   ⚠️ Ошибка сохранения в кэш: {e}')

    @staticmethod
    def clean_description(text):
        """
        Очистка HTML-тегов и лишних пробелов из описания.

        Args:
            text (str): Исходный текст.

        Returns:
            str: Очищенный текст.
        """
        if not text:
            return ''

        text = re.sub(r'<[^>]+>', '', text)
        text = re.sub(r'\s+', ' ', text).strip()

        return text[:15000]

    def get_balance_history(self, limit=10):
        """
        Получение истории проверок баланса.

        Args:
            limit (int): Количество записей для получения.

        Returns:
            list: Список записей истории баланса.
        """
        cache_conn = self.get_cache_connection()
        if not cache_conn:
            return []

        try:
            cursor = cache_conn.cursor()
            cursor.execute('''
                           SELECT check_date, status_code, balance_status, requests_remaining, monthly_limit, reset_date
                           FROM balance_history
                           ORDER BY check_date DESC LIMIT ?
                           ''', (limit,))

            history = []
            for row in cursor.fetchall():
                history.append({
                    'check_date': row[0],
                    'status_code': row[1],
                    'balance_status': row[2],
                    'requests_remaining': row[3],
                    'monthly_limit': row[4],
                    'reset_date': row[5]
                })

            return history

        except Exception as e:
            print(f'⚠️ Ошибка получения истории баланса: {e}')
            return []

    def print_balance_info(self):
        """
        Вывод информации о текущем балансе.
        """
        print("\n" + "=" * 60)
        print("📊 ИНФОРМАЦИЯ О БАЛАНСЕ API КЛЮЧА RAWG")
        print("=" * 60)

        if self.balance_error:
            print(f"❌ {self.balance_error}")
            print("\nРекомендации:")
            print("1. Проверьте лимит запросов в личном кабинете RAWG")
            print("2. Если лимит исчерпан, дождитесь сброса (обычно 1-е число месяца)")
            print("3. Используйте другой API ключ")
        else:
            print(f"✅ API ключ активен")

            if self.requests_remaining:
                print(f"📈 Осталось запросов: {self.requests_remaining}")

                # Преобразуем в числа для сравнения
                try:
                    remaining = int(self.requests_remaining)
                    if self.monthly_limit:
                        limit = int(self.monthly_limit)
                        percentage = (remaining / limit) * 100
                        print(f"📊 Загруженность: {percentage:.1f}% ({remaining}/{limit})")

                        # Предупреждение при низком остатке
                        if percentage < 10:
                            print(f"⚠️  Внимание: осталось менее 10% лимита!")
                        elif percentage < 30:
                            print(f"⚠️  Осталось менее 30% лимита")
                except (ValueError, TypeError):
                    pass

            if self.monthly_limit:
                print(f"🎯 Месячный лимит: {self.monthly_limit}")
            if self.reset_date:
                print(f"🔄 Дата сброса: {self.reset_date}")
            if self.last_balance_check:
                print(f"🕐 Проверено: {self.last_balance_check.strftime('%Y-%m-%d %H:%M:%S')}")

        print("=" * 60)

    def get_stats(self):
        """
        Получение статистики использования API.
        """
        return {
            'cache_hits': self.api_stats.get('cache_hits', 0),
            'cache_misses': self.api_stats.get('cache_misses', 0),
            'search_requests': self.api_stats.get('search_requests', 0),
            'detail_requests': self.api_stats.get('detail_requests', 0),
            'rate_limited': self.api_stats.get('rate_limited', 0),
            'balance_valid': self.balance_valid,
            'balance_error': self.balance_error,
            'requests_remaining': self.requests_remaining,
            'monthly_limit': self.monthly_limit,
            'reset_date': self.reset_date,
            'total_requests': self.api_stats.get('search_requests', 0) +
                              self.api_stats.get('detail_requests', 0)
        }
