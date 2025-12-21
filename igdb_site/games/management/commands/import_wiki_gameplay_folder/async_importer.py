import asyncio
import aiohttp
import re
import time
from typing import Dict, List, Optional, Tuple
import logging

logger = logging.getLogger(__name__)


class AsyncWikiImporter:
    """Исправленный асинхронный импортер Wikipedia"""

    def __init__(self, lang: str = 'en', max_concurrent: int = 50):
        self.lang = lang
        self.max_concurrent = max_concurrent
        self.session = None
        self.semaphore = None

        # Статистика
        self.api_calls = 0
        self.cache_hits = 0
        self.failed_requests = 0

        # Простой кэш в памяти
        self._cache = {}
        self._cache_lock = asyncio.Lock()

    async def __aenter__(self):
        """Инициализация асинхронной сессии"""
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent,
            limit_per_host=self.max_concurrent,
            ttl_dns_cache=300
        )
        self.session = aiohttp.ClientSession(
            connector=connector,
            headers={'User-Agent': 'GameWikiBot/10.0 (https://github.com/your-repo)'},
            timeout=aiohttp.ClientTimeout(total=30, connect=10, sock_read=20)
        )
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие сессии"""
        if self.session:
            await self.session.close()

    def _get_cache_key(self, key_type: str, value: str) -> str:
        """Получить ключ кэша"""
        return f"{self.lang}:{key_type}:{value}"

    async def get_from_cache(self, key_type: str, value: str) -> Optional[str]:
        """Получить из кэша"""
        cache_key = self._get_cache_key(key_type, value)
        async with self._cache_lock:
            if cache_key in self._cache:
                self.cache_hits += 1
                return self._cache[cache_key]
        return None

    async def set_to_cache(self, key_type: str, value: str, data: str, ttl: int = 3600):
        """Сохранить в кэш"""
        cache_key = self._get_cache_key(key_type, value)
        async with self._cache_lock:
            self._cache[cache_key] = data

    async def fetch_with_retry(self, url: str, params: Dict, max_retries: int = 2) -> Optional[Dict]:
        """Асинхронный запрос с повторными попытками"""
        for attempt in range(max_retries):
            try:
                async with self.session.get(url, params=params) as response:
                    if response.status == 200:
                        self.api_calls += 1
                        return await response.json()
                    elif response.status == 429:  # Too Many Requests
                        wait_time = 2 ** (attempt + 1)
                        logger.debug(f"Rate limited, waiting {wait_time}s")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        response.raise_for_status()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == max_retries - 1:
                    self.failed_requests += 1
                    logger.debug(f"Request failed after {max_retries} attempts: {e}")
                    return None
                await asyncio.sleep(0.5 * (attempt + 1))
        return None

    async def search_wikipedia_titles(self, game_name: str, year: int = None) -> List[str]:
        """Улучшенный поиск заголовков страниц с поддержкой года"""
        # Формируем ключ кэша с учетом года
        cache_key = f"search:{game_name}:{year if year else 'no_year'}"

        cached = await self.get_from_cache('search', cache_key)
        if cached:
            return cached.split('|') if cached else []

        # Формируем поисковый запрос
        if year:
            # Пробуем несколько вариантов запроса с годом
            search_variants = [
                f'{game_name} ({year} video game)',
                f'{game_name} {year} video game',
                f'{game_name} video game {year}',
                f'{game_name} video game'
            ]
        else:
            search_variants = [f'{game_name} video game']

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

            async with self.semaphore:
                data = await self.fetch_with_retry(url, params)

            if data and 'query' in data and 'search' in data['query']:
                for result in data['query']['search']:
                    title = result['title']
                    if title not in all_results:  # Убираем дубликаты
                        all_results.append(title)

            # Если нашли достаточно результатов, останавливаемся
            if len(all_results) >= 3:
                break

        # Сортируем результаты: сначала те, что содержат год
        if year:
            def sort_key(title):
                score = 0
                if str(year) in title:
                    score += 10
                if 'video game' in title.lower():
                    score += 5
                if game_name.lower() in title.lower():
                    score += 3
                return -score  # Отрицательное для сортировки по убыванию

            all_results.sort(key=sort_key)

        # Сохраняем в кэш
        if all_results:
            await self.set_to_cache('search', cache_key, '|'.join(all_results))

        return all_results[:5]  # Возвращаем не более 5 результатов

    async def get_page_content(self, title: str) -> Optional[str]:
        """Получение содержимого страницы"""
        # Проверяем кэш
        cached = await self.get_from_cache('content', title)
        if cached:
            return cached

        url = f"https://{self.lang}.wikipedia.org/w/api.php"
        params = {
            'action': 'query',
            'prop': 'extracts',
            'titles': title,
            'explaintext': 1,
            'exsectionformat': 'plain',
            'exintro': 1,
            'exsentences': 8,
            'format': 'json'
        }

        async with self.semaphore:
            data = await self.fetch_with_retry(url, params)

        if not data:
            return None

        pages = data.get('query', {}).get('pages', {})
        for page_id, page_data in pages.items():
            if page_id != '-1':
                content = page_data.get('extract', '')
                if content:
                    # Сохраняем в кэш
                    await self.set_to_cache('content', title, content)
                return content

        return None

    @staticmethod
    def extract_gameplay_fast(content: str) -> Optional[str]:
        """Быстрое извлечение раздела Gameplay"""
        if not content:
            return None

        # Быстрый поиск раздела Gameplay
        lines = content.split('\n')

        # Ищем заголовок Gameplay
        gameplay_start = -1
        for i, line in enumerate(lines):
            if any(marker in line.lower() for marker in ['==gameplay==', '== game ==', '==game mechanics==']):
                gameplay_start = i + 1
                break

        # Если нашли Gameplay
        if gameplay_start != -1:
            gameplay_lines = []
            for i in range(gameplay_start, min(gameplay_start + 10, len(lines))):
                line = lines[i].strip()
                if line and not line.startswith('=='):
                    gameplay_lines.append(line)
                elif line.startswith('=='):
                    break

            if gameplay_lines:
                text = ' '.join(gameplay_lines[:5])  # Берем первые 5 строк
                text = re.sub(r'\[\d+\]', '', text)
                text = ' '.join(text.split()[:80])
                return text[:600]

        # Если не нашли Gameplay, берем первые абзацы
        for line in lines:
            line = line.strip()
            if line and len(line) > 60 and not line.startswith('=='):
                line = re.sub(r'\[\d+\]', '', line)
                line = ' '.join(line.split()[:60])
                return line[:400]

        return None

    async def get_game_description(self, game_name: str, game_year: int = None) -> Optional[str]:
        """Получить описание игры с учетом года выпуска"""
        try:
            # 1. Прямые запросы к возможным названиям страниц
            variants = []

            if game_year:
                variants.append(f"{game_name} ({game_year} video game)")
                variants.append(f"{game_name} ({game_year})")
                variants.append(f"{game_name} (video game, {game_year})")

            variants.extend([
                f"{game_name} (video game)",
                game_name,
                game_name.replace(':', ''),
            ])

            # Пробуем все варианты
            for variant in variants:
                content = await self.get_page_content(variant)
                if content:
                    # Проверяем, подходит ли по году
                    if not game_year or str(game_year) in content:
                        description = self.extract_gameplay_fast(content)
                        if description and len(description) >= 40:
                            return description

            # 2. Если прямые запросы не сработали, ищем через поиск
            titles = await self.search_wikipedia_titles(game_name, game_year)

            for title in titles:
                content = await self.get_page_content(title)
                if content:
                    # Проверяем, что это действительно игра (а не фильм/книга)
                    if any(keyword in content.lower() for keyword in ['game', 'video game', 'play', 'player', 'level']):
                        description = self.extract_gameplay_fast(content)
                        if description and len(description) >= 40:
                            return description

            return None

        except Exception as e:
            logger.debug(f"Error getting description for {game_name}: {e}")
            return None

    async def process_batch(self, games_batch: List[Dict]) -> Dict[int, str]:
        """Обработка батча игр с учетом года выпуска"""
        results = {}

        # Создаем задачи для всех игр
        tasks = []
        for game in games_batch:
            # Получаем год из данных игры (нужно передавать в games_batch)
            game_year = game.get('release_year')
            task = asyncio.create_task(self.get_game_description(game['name'], game_year))
            tasks.append((game['id'], task))

        # Обрабатываем задачи по мере завершения
        for game_id, task in tasks:
            try:
                description = await asyncio.wait_for(task, timeout=30)
                if description:
                    results[game_id] = description
            except asyncio.TimeoutError:
                logger.debug(f"Timeout processing game {game_id}")
            except Exception as e:
                logger.debug(f"Error processing game {game_id}: {e}")

        return results
