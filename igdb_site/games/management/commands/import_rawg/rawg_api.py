import requests
import time
import sqlite3
import hashlib
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from django.conf import settings


class RAWGClient:
    """Клиент для работы с RAWG API с кэшированием и сохранением баланса"""

    def __init__(self, api_key: Optional[str] = None, api_limit: Optional[int] = None):
        self.api_key = api_key or getattr(settings, 'RAWG_API_KEY', None)
        self.base_url = "https://api.rawg.io/api"
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'search_requests': 0,
            'detail_requests': 0,
            'rate_limited': 0,
        }

        self.debug = False
        self.session = self._init_session()
        self.cache_conn = None
        self.init_cache()

        # Инициализируем баланс - ВСЕГДА СБРАСЫВАЕМ при указании api_limit
        self.api_limit = api_limit or 20000  # По умолчанию 20000 если не указано
        self.used_requests = 0  # ВСЕГДА НАЧИНАЕМ С 0 при указании нового лимита

        # Загружаем из кэша только если api_limit НЕ указан
        if api_limit is None:
            cached_balance = self._load_balance_from_cache()
            self.api_limit = cached_balance.get('api_limit', 20000)
            self.used_requests = cached_balance.get('used_requests', 0)
        else:
            # Если указан новый лимит - СБРАСЫВАЕМ счетчик
            self.used_requests = 0

        # Сохраняем в кэш (особенно если сбросили)
        self.save_balance_to_cache()

        if self.debug:
            print(f'🔧 Режим отладки включен')
            print(f'📊 Лимит API: {self.api_limit}')
            print(f'📊 Использовано запросов: {self.used_requests}')

        if not self.api_key:
            raise ValueError("RAWG API ключ не найден. Укажите через параметр или добавьте RAWG_API_KEY в настройки.")

    def update_balance(self, new_used: int = None, new_limit: int = None):
        """Обновляет баланс API"""
        if new_used is not None:
            self.used_requests = new_used
            if self.debug:
                print(f'🔄 Обновлено использованных запросов: {self.used_requests}')

        if new_limit is not None:
            self.api_limit = new_limit
            # ПРИ ИЗМЕНЕНИИ ЛИМИТА СБРАСЫВАЕМ СЧЕТЧИК
            if new_limit != self.api_limit:
                self.used_requests = 0
                if self.debug:
                    print(f'🔄 Обновлен лимит API: {self.api_limit}, счетчик сброшен')
            else:
                if self.debug:
                    print(f'🔄 Обновлен лимит API: {self.api_limit}')

        # Сохраняем в кэш
        self.save_balance_to_cache()

        # Показываем новый баланс
        balance = self.check_balance()
        if self.debug:
            print(f'📊 Новый баланс: использовано {balance["used"]}/{balance["limit"]}, осталось {balance["remaining"]}')

        return balance

    def set_debug(self, debug: bool):
        """Устанавливает режим отладки"""
        self.debug = debug
        if self.debug:
            print(f'🔧 Режим отладки включен')
            print(f'📊 Лимит API: {self.api_limit}')
            print(f'📊 Использовано запросов: {self.used_requests}')

    def _init_balance(self):
        """Инициализирует баланс из кэша (теперь вызывается только при отсутствии api_limit)"""
        cached_balance = self._load_balance_from_cache()

        self.api_limit = cached_balance.get('api_limit', 20000)
        self.used_requests = cached_balance.get('used_requests', 0)

        if self.debug:
            remaining = self.api_limit - self.used_requests
            print(f'📊 Баланс из кэша: использовано {self.used_requests}/{self.api_limit}, осталось {remaining}')

    def _init_session(self):
        """Инициализация HTTP сессии с оптимизациями"""
        session = requests.Session()

        retry_strategy = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            respect_retry_after_header=True
        )

        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=100,
            pool_block=False
        )

        session.mount("https://", adapter)
        session.mount("http://", adapter)

        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; RAWG-Importer/1.0)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })

        return session

    def init_cache(self):
        """Инициализация кэша SQLite"""
        try:
            cache_dir = Path('cache')
            cache_dir.mkdir(exist_ok=True)

            cache_path = cache_dir / 'rawg_cache.db'

            self.cache_conn = sqlite3.connect(
                str(cache_path),
                timeout=30,
                check_same_thread=False
            )

            self.cache_conn.execute('PRAGMA journal_mode = WAL')
            self.cache_conn.execute('PRAGMA synchronous = NORMAL')
            self.cache_conn.execute('PRAGMA cache_size = -10000')
            self.cache_conn.execute('PRAGMA temp_store = MEMORY')

            cursor = self.cache_conn.cursor()

            # Таблица для кэша игр
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
                               description
                               TEXT,
                               found
                               INTEGER
                               DEFAULT
                               1,
                               created_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP,
                               updated_at
                               TIMESTAMP
                               DEFAULT
                               CURRENT_TIMESTAMP
                           )
                           ''')

            # Таблица для баланса API
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS api_balance
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               CHECK
                           (
                               id =
                               1
                           ),
                               api_limit INTEGER DEFAULT 20000,
                               used_requests INTEGER DEFAULT 0,
                               last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                               )
                           ''')

            cursor.execute('CREATE INDEX IF NOT EXISTS idx_game_name ON rawg_cache(game_name)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_found ON rawg_cache(found)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_updated_at ON rawg_cache(updated_at)')

            self.cache_conn.commit()

            if self.debug:
                print(f'✅ Кэш инициализирован: {cache_path}')

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка инициализации кэша: {e}')
            self.cache_conn = None

    def _load_balance_from_cache(self) -> Dict[str, Any]:
        """Загружает баланс из кэша"""
        if not self.cache_conn:
            return {'api_limit': 20000, 'used_requests': 0}

        try:
            cursor = self.cache_conn.cursor()

            cursor.execute("""
                           SELECT api_limit, used_requests
                           FROM api_balance
                           WHERE id = 1
                           """)

            result = cursor.fetchone()
            if result:
                api_limit, used_requests = result
                return {
                    'api_limit': api_limit,
                    'used_requests': used_requests
                }

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка загрузки баланса из кэша: {e}')

        return {'api_limit': 20000, 'used_requests': 0}

    def save_balance_to_cache(self):
        """Сохраняет баланс в кэш"""
        if not self.cache_conn:
            return

        try:
            cursor = self.cache_conn.cursor()

            # Вставляем или обновляем баланс
            cursor.execute("""
                INSERT OR REPLACE INTO api_balance (id, api_limit, used_requests, last_updated)
                VALUES (1, ?, ?, CURRENT_TIMESTAMP)
            """, (self.api_limit, self.used_requests))

            self.cache_conn.commit()

            if self.debug:
                remaining = self.api_limit - self.used_requests
                print(f'💾 Баланс сохранен: {self.used_requests}/{self.api_limit}, осталось {remaining}')

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка сохранения баланса в кэш: {e}')

    def increment_used_requests(self, count: int = 1):
        """Увеличивает счетчик использованных запросов и сохраняет в кэш"""
        self.used_requests += count

        # Сохраняем после каждого увеличения
        self.save_balance_to_cache()

        if self.debug:
            remaining = self.api_limit - self.used_requests
            print(f'📊 Запросов использовано: {self.used_requests}, осталось: {remaining}')

    def check_balance(self) -> Dict[str, Any]:
        """Проверяет оставшийся баланс запросов"""
        remaining = self.api_limit - self.used_requests

        # Предупреждение при 20% остатка
        warning_threshold = self.api_limit * 0.2
        is_low = remaining <= warning_threshold and remaining > 0

        # Превышен ли лимит
        exceeded = self.used_requests >= self.api_limit

        return {
            'used': self.used_requests,
            'limit': self.api_limit,
            'remaining': max(0, remaining),
            'is_low': is_low,
            'exceeded': exceeded,
            'percentage': (self.used_requests / self.api_limit * 100) if self.api_limit > 0 else 0
        }

    def reset_balance(self, new_limit: Optional[int] = None):
        """Сбрасывает счетчик использованных запросов"""
        self.used_requests = 0  # ВСЕГДА СБРАСЫВАЕМ В 0

        if new_limit is not None:
            self.api_limit = new_limit

        self.save_balance_to_cache()

        if self.debug:
            print(f'🔄 Баланс сброшен: лимит {self.api_limit}, использовано 0')

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None,
                      timeout: int = 10, retry_count: int = 0) -> Dict[str, Any]:
        """Выполняет запрос к API с проверкой баланса"""
        if params is None:
            params = {}

        params['key'] = self.api_key

        url = f"{self.base_url}/{endpoint}"

        try:
            # ПРОВЕРЯЕМ БАЛАНС ПЕРЕД ЗАПРОСОМ - ВАЖНО!
            balance = self.check_balance()
            if balance['exceeded']:
                # ВОЗВРАЩАЕМ СПЕЦИАЛЬНЫЙ РЕЗУЛЬТАТ, ЧТОБЫ ОСТАНОВИТЬ ПРОЦЕСС
                raise ValueError(f"🚫 ЛИМИТ API ИСЧЕРПАН: использовано {self.used_requests}/{self.api_limit} запросов. "
                                 f"Остановка обработки.")

            self.stats['total_requests'] += 1

            if self.debug:
                print(f'🌐 Запрос #{self.stats["total_requests"]} к {endpoint}')

            response = self.session.get(
                url,
                params=params,
                timeout=timeout,
                allow_redirects=True
            )

            # Обработка ответа
            if response.status_code == 200:
                # Увеличиваем счетчик запросов для поиска и деталей
                if endpoint == 'games' and 'search' in params:
                    self.stats['search_requests'] += 1
                    self.increment_used_requests()
                elif 'games/' in endpoint:
                    self.stats['detail_requests'] += 1
                    self.increment_used_requests()

                return response.json()

            elif response.status_code == 429:
                # Rate limit
                self.stats['rate_limited'] += 1
                retry_after = response.headers.get('Retry-After', '60')

                if self.debug:
                    print(f'🚫 Rate limit: ждем {retry_after} секунд')

                # Ждем указанное время
                time.sleep(int(retry_after))

                # Пробуем снова
                if retry_count < 2:
                    return self._make_request(endpoint, params, timeout, retry_count + 1)
                else:
                    raise ValueError(f"Rate limit после {retry_count + 1} попыток")

            elif response.status_code == 401:
                raise ValueError("Неверный API ключ RAWG")

            elif response.status_code == 403:
                raise ValueError("Доступ запрещен (403). Проверьте API ключ и права доступа.")

            else:
                response.raise_for_status()

        except requests.exceptions.Timeout:
            if self.debug:
                print(f'⚠️ Таймаут запроса к {endpoint}')

            if retry_count < 2:
                backoff_time = 1 * (retry_count + 1)
                time.sleep(backoff_time)
                return self._make_request(endpoint, params, timeout * 1.5, retry_count + 1)

            raise ValueError(f"Таймаут запроса к RAWG API после {retry_count + 1} попыток")

        except requests.exceptions.ConnectionError:
            if self.debug:
                print(f'⚠️ Ошибка соединения с {endpoint}')

            if retry_count < 2:
                time.sleep(2)
                return self._make_request(endpoint, params, timeout, retry_count + 1)

            raise ValueError("Ошибка соединения с RAWG API. Проверьте интернет-подключение.")

        except Exception as e:
            if self.debug:
                print(f'❌ Ошибка запроса к {endpoint}: {e}')
            raise

    def get_cache_connection(self):
        """Возвращает соединение с кэшем"""
        if not self.cache_conn:
            self.init_cache()
        return self.cache_conn

    def _hash_game_name(self, game_name: str) -> str:
        """Создает хэш для имени игры"""
        return hashlib.md5(game_name.lower().encode('utf-8')).hexdigest()

    def _get_from_cache(self, game_hash: str) -> Optional[Dict[str, Any]]:
        """Получает данные из кэша"""
        if not self.cache_conn:
            return None

        try:
            cursor = self.cache_conn.cursor()
            cursor.execute(
                'SELECT description, found, updated_at FROM rawg_cache WHERE game_hash = ?',
                (game_hash,)
            )
            result = cursor.fetchone()

            if result:
                self.stats['cache_hits'] += 1

                description, found, updated_at = result
                return {
                    'description': description,
                    'found': bool(found),
                    'updated_at': updated_at,
                    'source': 'cache'
                }

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка чтения из кэша: {e}')

        self.stats['cache_misses'] += 1
        return None

    def _save_to_cache(self, game_hash: str, game_name: str,
                       description: str = None, found: bool = True):
        """Сохраняет данные в кэш"""
        if not self.cache_conn:
            return

        try:
            cursor = self.cache_conn.cursor()

            if description:
                cursor.execute('''
                    INSERT OR REPLACE INTO rawg_cache 
                    (game_hash, game_name, description, found, updated_at) 
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (game_hash, game_name, description, 1 if found else 0))
            else:
                cursor.execute('''
                    INSERT OR REPLACE INTO rawg_cache 
                    (game_hash, game_name, found, updated_at) 
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ''', (game_hash, game_name, 1 if found else 0))

            self.cache_conn.commit()

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка сохранения в кэш: {e}')

    def search_games(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Ищет игры по названию"""
        try:
            params = {
                'search': query,
                'page_size': limit,
                'search_precise': True
            }

            data = self._make_request('games', params)
            return data.get('results', [])

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка поиска игр: {e}')
            return []

    def get_game_details(self, game_id: int) -> Optional[Dict[str, Any]]:
        """Получает детальную информацию об игре"""
        try:
            data = self._make_request(f'games/{game_id}')
            return data

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка получения деталей игры {game_id}: {e}')
            return None

    def get_game_description(self, game_name: str, min_length: int = 1,
                             delay: float = 0.5, use_cache: bool = True,
                             cache_ttl: int = 30, timeout: int = 10) -> Dict[str, Any]:
        """Получает описание игры - ГАРАНТИРУЕМ ВОЗВРАТ СТАТУСА"""
        game_hash = self._hash_game_name(game_name)

        # Проверяем кэш
        if use_cache:
            cached = self._get_from_cache(game_hash)
            if cached:
                if self.debug:
                    print(f'💾 Кэш найден для: {game_name}')

                # Проверяем TTL
                if cache_ttl > 0 and cached.get('updated_at'):
                    try:
                        updated_at = datetime.fromisoformat(cached['updated_at'].replace('Z', '+00:00'))
                        if datetime.now(updated_at.tzinfo) - updated_at > timedelta(days=cache_ttl):
                            if self.debug:
                                print(f'🔄 Запись устарела ({cache_ttl} дней), обновляю...')
                        else:
                            # ГАРАНТИРУЕМ СТАТУС В КЭШЕ
                            if 'status' not in cached:
                                cached['status'] = 'found' if cached.get('found') else 'not_found'
                            return cached
                    except:
                        pass
                else:
                    # ГАРАНТИРУЕМ СТАТУС В КЭШЕ
                    if 'status' not in cached:
                        cached['status'] = 'found' if cached.get('found') else 'not_found'
                    return cached

        # Пауза для соблюдения rate limit
        if delay > 0:
            time.sleep(delay)

        if self.debug:
            print(f'🔍 Поиск игры: {game_name}')

        try:
            # Ищем игру
            search_results = self.search_games(game_name)

            if not search_results:
                if self.debug:
                    print(f'❌ Игра не найдена: {game_name}')

                result = {
                    'status': 'not_found',
                    'description': None,
                    'source': 'search',
                    'found': False
                }

                if use_cache:
                    self._save_to_cache(game_hash, game_name, None, False)

                return result

            # Берем наиболее релевантный результат
            best_match = search_results[0]
            game_id = best_match.get('id')

            if not game_id:
                result = {
                    'status': 'not_found',
                    'description': None,
                    'source': 'search',
                    'found': False
                }

                if use_cache:
                    self._save_to_cache(game_hash, game_name, None, False)

                return result

            # Получаем детальную информацию
            if self.debug:
                print(f'📄 Получение деталей для игры ID: {game_id}')

            game_details = self.get_game_details(game_id)

            if not game_details:
                result = {
                    'status': 'error',
                    'error': 'Не удалось получить детали игры',
                    'source': 'details',
                    'found': False
                }
                return result

            # Извлекаем описание
            description = game_details.get('description', '')

            if not description or len(description.strip()) < min_length:
                # Пробуем альтернативные источники описания
                description = (
                        game_details.get('description_raw') or
                        best_match.get('description') or
                        ''
                )

            # Определяем статус
            if not description or len(description.strip()) == 0:
                status = 'empty'
                found_in_rawg = False
            elif len(description.strip()) < min_length:
                status = 'short'
                found_in_rawg = False
            else:
                status = 'found'
                found_in_rawg = True

            # Подготавливаем результат
            if status == 'found':
                result = {
                    'status': 'found',
                    'description': description.strip(),
                    'source': 'details',
                    'found': True,
                    'rawg_id': game_id,
                    'rawg_data': {
                        'name': game_details.get('name'),
                        'released': game_details.get('released'),
                        'rating': game_details.get('rating'),
                        'ratings_count': game_details.get('ratings_count')
                    }
                }

                if self.debug:
                    print(f'✅ Найдено описание длиной {len(description)} символов')

                if use_cache:
                    self._save_to_cache(game_hash, game_name, description.strip(), True)
            else:
                result = {
                    'status': status,
                    'description': description.strip() if description else None,
                    'source': 'details',
                    'found': found_in_rawg,
                    'rawg_id': game_id
                }

                if self.debug:
                    print(f'⚠️ Статус: {status}')

                if use_cache:
                    self._save_to_cache(game_hash, game_name, None, False)

            return result

        except Exception as e:
            # ЛОВИМ ЛЮБЫЕ ИСКЛЮЧЕНИЯ И ВОЗВРАЩАЕМ РЕЗУЛЬТАТ СО СТАТУСОМ
            error_msg = str(e)
            if self.debug:
                print(f'❌ Ошибка при получении описания для {game_name}: {error_msg}')

            return {
                'status': 'error',
                'error': error_msg[:200],
                'source': 'exception',
                'found': False
            }

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику использования"""
        stats = self.stats.copy()
        stats['api_limit'] = self.api_limit
        stats['used_requests'] = self.used_requests
        stats['remaining_requests'] = self.api_limit - self.used_requests
        stats['cache_efficiency'] = (self.stats['cache_hits'] / max(1, self.stats['total_requests'])) * 100
        return stats

    def close(self):
        """Закрывает соединения"""
        if self.cache_conn:
            try:
                # Сохраняем баланс перед закрытием
                self.save_balance_to_cache()

                cursor = self.cache_conn.cursor()
                cursor.execute('PRAGMA optimize')
                self.cache_conn.commit()
                self.cache_conn.close()
                if self.debug:
                    print('✅ Кэш закрыт и оптимизирован')
            except Exception as e:
                if self.debug:
                    print(f'⚠️ Ошибка закрытия кэша: {e}')

        if self.session:
            self.session.close()
            if self.debug:
                print('✅ HTTP сессия закрыта')

    def __del__(self):
        """Деструктор - закрывает соединения"""
        self.close()