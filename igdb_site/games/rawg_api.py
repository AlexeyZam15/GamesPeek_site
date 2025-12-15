# FILE: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\rawg_api.py
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
    """Клиент для работы с RAWG API с кэшированием и управлением лимитами."""

    def __init__(self, api_key=None):
        """
        Инициализация клиента RAWG API.

        Args:
            api_key (str): Ключ API RAWG. Если не указан, берется из settings.RAWG_API_KEY.
        """
        self.api_key = api_key or getattr(settings, 'RAWG_API_KEY', None)
        if not self.api_key:
            raise ValueError("RAWG API key is not configured. Set RAWG_API_KEY in .env file.")

        self.request_timestamps = []
        self.api_stats = defaultdict(int)
        self.local = threading.local()  # Хранилище для потоковых данных

        # Инициализируем глобальные структуры
        self.init_cache_structure()
        self.init_stats_db_structure()

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
                                   1
                               )
                               ''')

                cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_hash ON rawg_cache(game_hash)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_name ON rawg_cache(game_name)')
                cursor.execute('CREATE INDEX IF NOT EXISTS idx_updated_at ON rawg_cache(updated_at)')

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
                               1
                           )
                           ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_hash ON rawg_cache(game_hash)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_name ON rawg_cache(game_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_updated_at ON rawg_cache(updated_at)')

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
                               REAL
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

    def enforce_rate_limit(self, delay=0.3):
        """
        Обеспечение соблюдения лимитов запросов к API.

        Args:
            delay (float): Базовая задержка между запросами.
        """
        now = time.time()

        # Удаляем старые временные метки
        self.request_timestamps = [t for t in self.request_timestamps if now - t < 1.0]

        # Если слишком много запросов в последнюю секунду - ждем
        if len(self.request_timestamps) >= 2:  # Макс. 2 запроса в секунду
            sleep_time = 1.0 - (now - self.request_timestamps[0])
            if sleep_time > 0:
                time.sleep(sleep_time)

        self.request_timestamps.append(now)

        # Базовая задержка
        if delay > 0:
            time.sleep(delay)

    def search_game(self, game_name, delay=0.3):
        """
        Поиск игры в RAWG API.

        Args:
            game_name (str): Название игры для поиска.
            delay (float): Задержка между запросами.

        Returns:
            dict or None: Результат поиска или None.
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
                time.sleep(2)
                return None

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

        Args:
            rawg_id (int): ID игры в RAWG.
            delay (float): Задержка между запросами.

        Returns:
            dict or None: Детали игры или None.
        """
        try:
            self.enforce_rate_limit(delay)

            url = f"https://api.rawg.io/api/games/{rawg_id}"
            params = {'key': self.api_key}

            response = requests.get(url, params=params, timeout=8)

            if response.status_code == 429:
                self.api_stats['rate_limited'] += 1
                time.sleep(2)
                return None

            if response.status_code == 200:
                return response.json()

            return None

        except requests.exceptions.Timeout:
            print(f'   ⏰ Таймаут деталей: {rawg_id}')
            return None
        except Exception as e:
            print(f'   ⚠️ Ошибка получения деталей: {rawg_id} - {e}')
            return None

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
                SELECT rawg_id, description, description_source, found, request_count
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
                    'request_count': result[4]
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
                                   request_count      = request_count + 1
                               WHERE game_hash = ?
                               ''', (rawg_id, description, source, found, game_hash))
            else:
                cursor.execute('''
                               INSERT INTO rawg_cache
                               (game_hash, game_name, rawg_id, description, description_source, found, request_count)
                               VALUES (?, ?, ?, ?, ?, ?, 1)
                               ''', (game_hash, game_name, rawg_id, description, source, found))

            cache_conn.commit()

        except Exception as e:
            print(f'   ⚠️ Ошибка сохранения в кэш: {e}')

    def get_game_description(self, game_name, min_length=1, delay=0.3,
                             use_cache=True, cache_ttl=30):
        """
        Получение описания игры из RAWG с использованием кэша.

        Args:
            game_name (str): Название игры.
            min_length (int): Минимальная длина описания.
            delay (float): Задержка между запросами.
            use_cache (bool): Использовать ли кэш.
            cache_ttl (int): Время жизни кэша.

        Returns:
            dict: Результат с описанием и метаданными.
        """
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
                    return {'status': 'not_found', 'source': 'cache'}

        self.api_stats['cache_misses'] += 1

        try:
            # 2. Поиск в RAWG API
            search_result = self.search_game(normalized['search'], delay)
            self.api_stats['search_requests'] += 1

            if not search_result:
                self.save_to_cache(game_name, game_hash, found=False)
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

        except Exception as e:
            print(f'   ⚠️ Ошибка получения описания для {game_name}: {e}')
            return {'status': 'error', 'error': str(e)}

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

    def get_stats(self):
        """
        Получение статистики использования API.

        Returns:
            dict: Статистика запросов и кэширования.
        """
        return {
            'cache_hits': self.api_stats.get('cache_hits', 0),
            'cache_misses': self.api_stats.get('cache_misses', 0),
            'search_requests': self.api_stats.get('search_requests', 0),
            'detail_requests': self.api_stats.get('detail_requests', 0),
            'rate_limited': self.api_stats.get('rate_limited', 0),
            'total_requests': self.api_stats.get('search_requests', 0) +
                              self.api_stats.get('detail_requests', 0)
        }