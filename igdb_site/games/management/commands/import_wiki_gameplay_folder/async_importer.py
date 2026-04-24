import asyncio
import aiohttp
import re
import time
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class AsyncWikiImporter:
    """Асинхронный импортер Wikipedia с правильным rate limiting"""

    def __init__(self, lang: str = 'en', max_concurrent: int = 3, username: str = None, password: str = None):
        self.lang = lang
        self.max_concurrent = max_concurrent
        self.username = username
        self.password = password
        self.session = None
        self.semaphore = None

        # Rate limiting: не более 2 запросов в секунду
        self.request_times = []
        self.max_requests_per_second = 2

        # Статистика
        self.api_calls = 0
        self.cache_hits = 0
        self.failed_requests = 0
        self.rate_limit_wait_count = 0

        # Кэш
        self._cache = {}
        self._cache_lock = asyncio.Lock()

    async def __aenter__(self):
        """Инициализация асинхронной сессии"""
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            limit_per_host=self.max_concurrent,
            ttl_dns_cache=300
        )

        # Тот же User-Agent что и в тесте
        headers = {
            'User-Agent': 'GameWikiBot/2.0 (https://github.com/your-repo; contact@your-email.com; Python aiohttp)'
        }

        self.session = aiohttp.ClientSession(
            connector=connector,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        )

        self.semaphore = asyncio.Semaphore(self.max_concurrent)

        # Аутентификация
        if self.username and self.password:
            await self.authenticate()

        return self

    async def authenticate(self):
        """Аутентификация через Bot Password для высоких лимитов"""
        if not self.username or not self.password:
            print(f"⚠️ Не указаны логин/пароль для аутентификации")
            return False

        try:
            # Шаг 1: Получаем login token
            url = f"https://{self.lang}.wikipedia.org/w/api.php"

            params = {
                'action': 'query',
                'meta': 'tokens',
                'type': 'login',
                'format': 'json'
            }

            async with self.session.get(url, params=params) as response:
                data = await response.json()
                login_token = data.get('query', {}).get('tokens', {}).get('logintoken')

                if not login_token:
                    print(f"⚠️ Не удалось получить login token")
                    print(f"   Ответ: {data}")
                    return False

            # Шаг 2: Выполняем login
            # Важно: используем POST запрос с данными формы
            login_data = {
                'action': 'login',
                'lgname': self.username,
                'lgpassword': self.password,
                'lgtoken': login_token,
                'format': 'json'
            }

            async with self.session.post(url, data=login_data) as response:
                data = await response.json()

                if data.get('login', {}).get('result') == 'Success':
                    print(f"✅ Аутентификация успешна! Лимиты повышены.")
                    return True
                else:
                    error = data.get('login', {}).get('reason', 'Неизвестная ошибка')
                    print(f"❌ Ошибка аутентификации: {error}")
                    print(f"   Используйте логин: AlexeyZam15@gamespeek")
                    print(f"   Или: AlexeyZam15")
                    return False

        except Exception as e:
            print(f"❌ Ошибка при аутентификации: {e}")
            return False

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    async def rate_limit_wait(self):
        """Ожидание для соблюдения rate limiting"""
        now = time.time()
        self.request_times = [t for t in self.request_times if now - t < 1.0]

        if len(self.request_times) >= self.max_requests_per_second:
            wait_time = 1.0 - (now - self.request_times[0])
            if wait_time > 0:
                self.rate_limit_wait_count += 1
                await asyncio.sleep(wait_time)

        self.request_times.append(time.time())

    async def fetch_with_retry(self, url: str, params: Dict, max_retries: int = 3) -> Optional[Dict]:
        """Асинхронный запрос с повторными попытками и maxlag"""
        # Добавляем maxlag ко всем запросам
        if 'maxlag' not in params:
            params['maxlag'] = 5

        for attempt in range(max_retries):
            try:
                async with self.semaphore:
                    async with self.session.get(url, params=params) as response:
                        if response.status == 200:
                            self.api_calls += 1
                            data = await response.json()
                            # Проверяем на maxlag ошибку в теле ответа
                            if 'error' in data and data['error'].get('code') == 'maxlag':
                                lag = data['error'].get('lag', 5)
                                wait_time = min(lag + 5, 30)  # Ждем лаг + 5 сек, но не более 30
                                await asyncio.sleep(wait_time)
                                continue
                            return data
                        elif response.status == 429:
                            wait_time = 2 ** (attempt + 1)
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            response.raise_for_status()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == max_retries - 1:
                    self.failed_requests += 1
                await asyncio.sleep(0.5 * (attempt + 1))
        return None

    async def search_wikipedia_titles(self, game_name: str, year: int = None) -> List[str]:
        """Поиск заголовков страниц"""
        cache_key = f"search:{game_name}:{year if year else 'no_year'}"

        cached = await self.get_from_cache('search', cache_key)
        if cached:
            return cached.split('|') if cached else []

        search_variants = []
        if year:
            search_variants = [
                f'"{game_name}" ({year} video game)',
                f'"{game_name}" video game',
                f'{game_name} video game'
            ]
        else:
            search_variants = [f'"{game_name}" video game', f'{game_name} video game']

        all_results = []

        for search_query in search_variants:
            url = f"https://{self.lang}.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'list': 'search',
                'srsearch': search_query,
                'format': 'json',
                'srlimit': 5,
                'srprop': 'snippet'
            }

            data = await self.fetch_with_retry(url, params)

            if data and 'query' in data and 'search' in data['query']:
                for result in data['query']['search']:
                    title = result['title']
                    if title not in all_results:
                        all_results.append(title)

            if len(all_results) >= 3:
                break

        if all_results and cache_key:
            await self.set_to_cache('search', cache_key, '|'.join(all_results[:5]))

        return all_results[:5]

    async def get_from_cache(self, key_type: str, value: str) -> Optional[str]:
        """Получить из кэша"""
        cache_key = f"{self.lang}:{key_type}:{value}"
        async with self._cache_lock:
            if cache_key in self._cache:
                self.cache_hits += 1
                return self._cache[cache_key]
        return None

    async def set_to_cache(self, key_type: str, value: str, data: str):
        """Сохранить в кэш"""
        cache_key = f"{self.lang}:{key_type}:{value}"
        async with self._cache_lock:
            self._cache[cache_key] = data

    async def get_page_content(self, title: str, get_full_text: bool = False) -> Optional[str]:
        """Получение содержимого страницы"""
        cache_key = f"content:{title}"

        cached = await self.get_from_cache('content', cache_key)
        if cached:
            return cached

        url = f"https://{self.lang}.wikipedia.org/w/api.php"
        params = {
            'action': 'query',
            'prop': 'extracts',
            'titles': title,
            'explaintext': 1,
            'format': 'json',
            'exsectionformat': 'wiki'
        }

        if not get_full_text:
            params['exintro'] = 1
            params['exsentences'] = 8

        data = await self.fetch_with_retry(url, params)

        if not data:
            return None

        pages = data.get('query', {}).get('pages', {})
        for page_id, page_data in pages.items():
            if page_id != '-1':
                content = page_data.get('extract', '')
                if content:
                    formatted = self.add_paragraphs_to_content(content)
                    await self.set_to_cache('content', cache_key, formatted)
                    return formatted
        return None

    @staticmethod
    def add_paragraphs_to_content(content: str) -> str:
        """Форматирование контента с абзацами"""
        if not content:
            return ""

        lines = content.split('\n')
        result_lines = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith('==') and line.endswith('=='):
                if result_lines and result_lines[-1] != '':
                    result_lines.append('')
                result_lines.append(line)
                result_lines.append('')
            else:
                result_lines.append(line)
                if len(line) > 100 and line.endswith('.'):
                    result_lines.append('')

        cleaned = []
        for i, line in enumerate(result_lines):
            if line != '' or (i > 0 and result_lines[i - 1] != ''):
                cleaned.append(line)

        return '\n'.join(cleaned)

    @staticmethod
    def extract_gameplay_fast(content: str) -> Optional[str]:
        """Извлечение секции Gameplay"""
        if not content:
            return None

        lines = content.split('\n')

        gameplay_start = -1
        for i, line in enumerate(lines):
            if 'Gameplay' in line and line.strip().startswith('==') and line.strip().endswith('=='):
                gameplay_start = i + 1
                break

        if gameplay_start == -1:
            return None

        paragraphs = []
        current_paragraph = []

        for i in range(gameplay_start, len(lines)):
            line = lines[i].rstrip()

            if line.strip().startswith('==') and line.strip().endswith('=='):
                if len(line.strip().replace('=', '').strip()) > 0:
                    if not any(x in line.lower() for x in ['gameplay', 'mechanics']):
                        break

            if line:
                current_paragraph.append(line)
            elif current_paragraph:
                paragraphs.append('\n'.join(current_paragraph))
                current_paragraph = []

        if current_paragraph:
            paragraphs.append('\n'.join(current_paragraph))

        if not paragraphs:
            return None

        result = '\n\n'.join(paragraphs)
        result = re.sub(r'\[\d+\]', '', result)

        return result

    async def get_game_description(self, game_name: str, game_year: int = None) -> Optional[str]:
        """Получить описание игры с учетом года выпуска"""
        try:
            # Пробуем прямой доступ по точному названию
            exact_title = game_name.replace(' ', '_')
            content = await self.get_page_content(exact_title, get_full_text=True)

            if content:
                description = self.extract_gameplay_fast(content)
                if description:
                    return description

            # Поиск через API
            search_titles = await self.search_wikipedia_titles(game_name, game_year)

            for title in search_titles:
                content = await self.get_page_content(title.replace(' ', '_'), get_full_text=True)
                if content:
                    description = self.extract_gameplay_fast(content)
                    if description:
                        return description

            return None

        except Exception as e:
            return None

    async def process_batch(self, games_batch: List[Dict]) -> Dict[int, str]:
        """Обработка батча игр"""
        results = {}

        tasks = []
        for game in games_batch:
            game_year = game.get('release_year')
            task = asyncio.create_task(self.get_game_description(game['name'], game_year))
            tasks.append((game['id'], task))

        for game_id, task in tasks:
            try:
                description = await asyncio.wait_for(task, timeout=45)
                if description:
                    results[game_id] = description
            except asyncio.TimeoutError:
                pass
            except Exception:
                pass

        return results
