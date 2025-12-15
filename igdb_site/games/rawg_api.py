# games/rawg_api.py
import requests
import time
import sqlite3
import hashlib
import json
import os
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from urllib.parse import quote
import concurrent.futures
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from django.conf import settings


class RAWGClient:
    """Клиент для работы с RAWG API с кэшированием"""

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or getattr(settings, 'RAWG_API_KEY', None)
        self.base_url = "https://api.rawg.io/api"
        self.stats = {
            'total_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'search_requests': 0,
            'detail_requests': 0,
            'rate_limited': 0,
            'balance_exceeded': False,
            'balance_error': None,
            'last_balance_check': None,
            'requests_remaining': None
        }

        # Флаг отладки (можно установить через метод)
        self.debug = False

        # Инициализация сессии с оптимизациями
        self.session = self._init_session()

        # Инициализация кэша
        self.cache_conn = None
        self.init_cache()

        if not self.api_key:
            raise ValueError("RAWG API ключ не найден. Укажите через параметр или добавьте RAWG_API_KEY в настройки.")

    def set_debug(self, debug: bool):
        """Устанавливает режим отладки"""
        self.debug = debug

    def _init_session(self):
        """Инициализация HTTP сессии с оптимизациями"""
        session = requests.Session()

        # Оптимизированная стратегия retry
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

        # Оптимизированные заголовки
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (compatible; RAWG-Importer/1.0)',
            'Accept': 'application/json',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive'
        })

        return session

    def init_cache(self):
        """Инициализация кэша SQLite с исправленной структурой"""
        try:
            cache_dir = Path('cache')
            cache_dir.mkdir(exist_ok=True)

            cache_path = cache_dir / 'rawg_cache.db'

            # Если файл существует и есть ошибка структуры, удаляем его
            if cache_path.exists():
                try:
                    # Пробуем подключиться и проверить структуру
                    test_conn = sqlite3.connect(str(cache_path), timeout=10)
                    cursor = test_conn.cursor()
                    cursor.execute(
                        "SELECT game_hash, game_name, description, found, created_at, updated_at FROM rawg_cache LIMIT 1")
                    test_conn.close()
                except sqlite3.OperationalError as e:
                    if "no such column" in str(e):
                        # Удаляем файл с плохой структурой
                        if self.debug:
                            print(f'⚠️ Удален поврежденный файл кэша: {e}')
                        os.remove(cache_path)
                    else:
                        # Другие ошибки
                        if self.debug:
                            print(f'⚠️ Ошибка проверки кэша: {e}')
                        # Пересоздаем файл
                        os.remove(cache_path)

            # Используем более быстрые настройки SQLite
            self.cache_conn = sqlite3.connect(
                str(cache_path),
                timeout=30,
                check_same_thread=False
            )

            # Включаем оптимизации
            self.cache_conn.execute('PRAGMA journal_mode = WAL')
            self.cache_conn.execute('PRAGMA synchronous = NORMAL')
            self.cache_conn.execute('PRAGMA cache_size = -10000')
            self.cache_conn.execute('PRAGMA temp_store = MEMORY')

            cursor = self.cache_conn.cursor()

            # Создаем УПРОЩЕННУЮ таблицу без проблемных колонок
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

            # Только основные индексы
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

    def get_cache_connection(self):
        """Возвращает соединение с кэшем"""
        if not self.cache_conn:
            self.init_cache()
        return self.cache_conn

    def get_cache_size(self):
        """Возвращает количество записей в кэше"""
        try:
            if not self.cache_conn:
                return None

            cursor = self.cache_conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM rawg_cache')
            return cursor.fetchone()[0]
        except:
            return None

    def _make_request(self, endpoint: str, params: Dict[str, Any] = None,
                      timeout: int = 10, retry_count: int = 0) -> Dict[str, Any]:
        """Выполняет запрос к API с обработкой ошибок"""
        if params is None:
            params = {}

        # Добавляем API ключ
        params['key'] = self.api_key

        url = f"{self.base_url}/{endpoint}"

        try:
            self.stats['total_requests'] += 1

            if self.debug:
                print(f'🌐 Запрос к {endpoint} с параметрами: {params}')

            # Используем keep-alive соединения
            response = self.session.get(
                url,
                params=params,
                timeout=timeout,
                allow_redirects=True
            )

            if response.status_code == 200:
                if self.debug:
                    print(f'✅ Ответ получен: {response.status_code}')
                return response.json()

            elif response.status_code == 429:
                # Rate limit - ЛИМИТ ИСЧЕРПАН!
                self.stats['rate_limited'] += 1

                # Получаем информацию о времени ожидания
                retry_after = response.headers.get('Retry-After')

                if retry_after:
                    wait_time = int(retry_after)
                    error_msg = f"Превышен лимит запросов. Следующий запрос через {wait_time} секунд"
                else:
                    # Если нет Retry-After заголовка, скорее всего лимит на сутки
                    wait_time = 86400  # 24 часа
                    error_msg = f"ДНЕВНОЙ ЛИМИТ ИСЧЕРПАН! Следующий запрос через 24 часа"

                # Помечаем баланс как исчерпанный
                self.stats['balance_exceeded'] = True
                self.stats['balance_error'] = error_msg

                if self.debug:
                    print(f'🚫 ЛИМИТ API: {error_msg}')

                # Бросаем специальное исключение для остановки процесса
                raise ValueError(f"ЛИМИТ API ИСЧЕРПАН: {error_msg}")

            elif response.status_code == 401:
                # Неверный API ключ
                error_msg = "Неверный API ключ RAWG"
                self.stats['balance_exceeded'] = True
                self.stats['balance_error'] = error_msg

                if self.debug:
                    print(f'❌ Неверный API ключ')

                raise ValueError(error_msg)

            else:
                if self.debug:
                    print(f'❌ Ошибка {response.status_code}: {response.text[:100]}')
                response.raise_for_status()

        except requests.exceptions.Timeout:
            if self.debug:
                print(f'⚠️ Таймаут запроса к {endpoint}')

            if retry_count < 2:
                time.sleep(1)
                return self._make_request(endpoint, params, timeout * 1.5, retry_count + 1)
            raise ValueError("Таймаут запроса к RAWG API")

        except requests.exceptions.RequestException as e:
            if self.debug:
                print(f'❌ Ошибка запроса к {endpoint}: {e}')
            raise ValueError(f"Ошибка запроса к RAWG API: {str(e)}")

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
                # Обновляем время последнего использования
                cursor.execute(
                    'UPDATE rawg_cache SET last_used = CURRENT_TIMESTAMP WHERE game_hash = ?',
                    (game_hash,)
                )
                self.cache_conn.commit()

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

        return None

    def _save_to_cache(self, game_hash: str, game_name: str,
                       description: str = None, found: bool = True):
        """Сохраняет данные в кэш (упрощенная версия)"""
        if not self.cache_conn:
            return

        try:
            cursor = self.cache_conn.cursor()

            # Упрощенный INSERT без проблемных колонок
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
            # Если ошибка, пересоздаем таблицу
            try:
                cursor.execute('DROP TABLE IF EXISTS rawg_cache')
                cursor.execute('''
                               CREATE TABLE rawg_cache
                               (
                                   game_hash   TEXT PRIMARY KEY,
                                   game_name   TEXT NOT NULL,
                                   description TEXT,
                                   found       INTEGER   DEFAULT 1,
                                   created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                   updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                               )
                               ''')
                self.cache_conn.commit()
            except:
                pass

    def _cleanup_old_cache(self, ttl_days: int = 30):
        """Очищает старые записи из кэша"""
        if not self.cache_conn:
            return

        try:
            cursor = self.cache_conn.cursor()

            # Удаляем записи старше ttl_days дней
            cursor.execute('''
                           DELETE
                           FROM rawg_cache
                           WHERE updated_at < datetime('now', ?)
                           ''', (f'-{ttl_days} days',))

            deleted = cursor.rowcount
            self.cache_conn.commit()

            if self.debug and deleted > 0:
                print(f'🧹 Удалено {deleted} старых записей из кэша')

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка очистки кэша: {e}')

    def search_games(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """Ищет игры по названию"""
        self.stats['search_requests'] += 1

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
        self.stats['detail_requests'] += 1

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
        """Получает описание игры с правильной обработкой пустых описаний"""
        game_hash = self._hash_game_name(game_name)

        # Проверяем кэш
        if use_cache:
            cached = self._get_from_cache(game_hash)
            if cached:
                self.stats['cache_hits'] += 1

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
                            # Возвращаем из кэша
                            return cached
                    except:
                        # Если ошибка парсинга даты, продолжаем
                        pass
                else:
                    # TTL отключен, возвращаем из кэша
                    return cached
            else:
                self.stats['cache_misses'] += 1
                if self.debug:
                    print(f'❌ Кэш не найден для: {game_name}')

        # Пауза для соблюдения rate limit
        if delay > 0:
            time.sleep(delay)

        # Ищем игру
        if self.debug:
            print(f'🔍 Поиск игры: {game_name}')

        search_results = self.search_games(game_name)

        if not search_results:
            # Игра не найдена
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
            # ОПИСАНИЯ НЕТ ВООБЩЕ
            status = 'empty'
            found_in_rawg = False  # ВАЖНО: хоть игра и найдена, описания нет
        elif len(description.strip()) < min_length:
            # Описание есть, но слишком короткое
            status = 'short'
            found_in_rawg = False
        else:
            # Описание нормальное
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
            # Нет описания или слишком короткое
            result = {
                'status': status,
                'description': description.strip() if description else None,
                'source': 'details',
                'found': found_in_rawg,  # found=False если нет описания
                'rawg_id': game_id
            }

            if self.debug:
                print(f'⚠️ Статус: {status}')

            if use_cache:
                # Сохраняем в кэш что игра не имеет описания
                self._save_to_cache(game_hash, game_name, None, False)

        return result

    def get_game_descriptions_batch(self, game_names: List[str], min_length: int = 1,
                                    delay: float = 0.5, use_cache: bool = True,
                                    cache_ttl: int = 30, max_workers: int = 4) -> Dict[str, Dict[str, Any]]:
        """Получает описания для нескольких игр параллельно"""
        results = {}

        if self.debug:
            print(f'🚀 Начинаем batch обработку {len(game_names)} игр с {max_workers} потоками')

        # Сначала проверяем кэш для всех игр
        if use_cache:
            cached_results = {}
            to_fetch = []

            for game_name in game_names:
                game_hash = self._hash_game_name(game_name)
                cached = self._get_from_cache(game_hash)

                if cached:
                    # Проверяем TTL
                    if cache_ttl > 0:
                        updated_at = datetime.fromisoformat(cached['updated_at'].replace('Z', '+00:00'))
                        if datetime.now(updated_at.tzinfo) - updated_at > timedelta(days=cache_ttl):
                            to_fetch.append(game_name)
                        else:
                            cached_results[game_name] = cached
                            self.stats['cache_hits'] += 1
                    else:
                        cached_results[game_name] = cached
                        self.stats['cache_hits'] += 1
                else:
                    to_fetch.append(game_name)
                    self.stats['cache_misses'] += 1

            results.update(cached_results)

            if self.debug:
                print(f'💾 Из кэша получено: {len(cached_results)} игр')
                print(f'🔍 Требуется запросов: {len(to_fetch)} игр')
        else:
            to_fetch = game_names
            self.stats['cache_misses'] += len(game_names)

        # Параллельно получаем описания для оставшихся игр
        if to_fetch:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Создаем задачи
                future_to_game = {}
                for i, game_name in enumerate(to_fetch):
                    # Добавляем задержку между запусками задач
                    if i > 0 and delay > 0:
                        time.sleep(delay / max_workers)

                    future = executor.submit(
                        self.get_game_description,
                        game_name=game_name,
                        min_length=min_length,
                        delay=0,  # Задержка уже учтена выше
                        use_cache=use_cache,
                        cache_ttl=cache_ttl
                    )
                    future_to_game[future] = game_name

                # Обрабатываем результаты
                completed = 0
                total = len(to_fetch)

                for future in concurrent.futures.as_completed(future_to_game):
                    game_name = future_to_game[future]
                    try:
                        result = future.result(timeout=30)
                        results[game_name] = result

                        completed += 1
                        if self.debug and completed % 10 == 0:
                            print(f'📊 Batch прогресс: {completed}/{total} ({completed / total * 100:.1f}%)')

                    except Exception as e:
                        if self.debug:
                            print(f'⚠️ Ошибка получения описания для {game_name}: {e}')
                        results[game_name] = {
                            'status': 'error',
                            'error': str(e),
                            'source': 'batch',
                            'found': False
                        }

        if self.debug:
            print(f'✅ Batch обработка завершена. Всего результатов: {len(results)}')

        return results

    def check_balance(self, force: bool = False, quick_check: bool = False) -> Dict[str, Any]:
        """Проверяет баланс/лимиты API"""
        # Кэшируем проверку баланса на 5 минут
        if not force and self.stats['last_balance_check']:
            last_check = datetime.fromisoformat(self.stats['last_balance_check'])
            if datetime.now() - last_check < timedelta(minutes=5):
                if self.debug:
                    print(f'💾 Использую кэшированную проверку баланса')
                return {
                    'balance_exceeded': self.stats['balance_exceeded'],
                    'error': self.stats['balance_error'],
                    'requests_remaining': self.stats['requests_remaining']
                }

        try:
            # Быстрая проверка - просто тестовый запрос
            if quick_check:
                params = {'key': self.api_key, 'page_size': 1}
                response = self.session.get(
                    f"{self.base_url}/games",
                    params=params,
                    timeout=5
                )

                if response.status_code == 200:
                    self.stats['balance_exceeded'] = False
                    self.stats['balance_error'] = None

                    # Пытаемся получить информацию о лимитах из заголовков
                    remaining = response.headers.get('X-RateLimit-Remaining')
                    if remaining:
                        self.stats['requests_remaining'] = int(remaining)

                    if self.debug:
                        print(f'✅ Баланс проверен, осталось запросов: {self.stats["requests_remaining"]}')

                elif response.status_code == 429:
                    self.stats['balance_exceeded'] = True
                    self.stats['balance_error'] = "Превышен лимит запросов"
                    if self.debug:
                        print(f'❌ Превышен лимит запросов')
                elif response.status_code == 401:
                    self.stats['balance_exceeded'] = True
                    self.stats['balance_error'] = "Неверный API ключ"
                    if self.debug:
                        print(f'❌ Неверный API ключ')
                else:
                    response.raise_for_status()
            else:
                # Полная проверка с запросом к API
                data = self._make_request('games', {'page_size': 1})
                self.stats['balance_exceeded'] = False
                self.stats['balance_error'] = None

            self.stats['last_balance_check'] = datetime.now().isoformat()

            return {
                'balance_exceeded': self.stats['balance_exceeded'],
                'error': self.stats['balance_error'],
                'requests_remaining': self.stats['requests_remaining']
            }

        except ValueError as e:
            self.stats['balance_exceeded'] = True
            self.stats['balance_error'] = str(e)
            self.stats['last_balance_check'] = datetime.now().isoformat()

            if self.debug:
                print(f'❌ Ошибка проверки баланса: {e}')

            return {
                'balance_exceeded': True,
                'error': str(e),
                'requests_remaining': None
            }
        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка проверки баланса: {e}')

            return {
                'balance_exceeded': False,
                'error': None,
                'requests_remaining': None
            }

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику использования"""
        # Очищаем старый кэш при запросе статистики
        self._cleanup_old_cache(30)

        return self.stats.copy()

    def close(self):
        """Закрывает соединения"""
        if self.cache_conn:
            try:
                # Оптимизируем базу данных перед закрытием
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