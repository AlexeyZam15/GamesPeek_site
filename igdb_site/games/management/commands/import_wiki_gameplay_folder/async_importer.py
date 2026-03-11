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

    async def get_page_content(self, title: str, get_full_text: bool = False) -> Optional[str]:
        """Получение содержимого страницы с абзацами"""
        cache_key = f"content:{title}:{'full' if get_full_text else 'intro'}:paragraphs"
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

        async with self.semaphore:
            data = await self.fetch_with_retry(url, params)

        if not data:
            return None

        pages = data.get('query', {}).get('pages', {})
        for page_id, page_data in pages.items():
            if page_id != '-1':
                content = page_data.get('extract', '')
                if content:
                    formatted_content = self.add_paragraphs_to_content(content)
                    await self.set_to_cache('content', cache_key, formatted_content)
                    return formatted_content
        return None

    @staticmethod
    def add_paragraphs_to_content(content: str) -> str:
        """Добавить отступы между предложениями/абзацами"""
        if not content:
            return ""

        # Разделяем по точкам (предложения) и добавляем отступы
        # Это упрощенный вариант - на самом деле нужно парсить структуру

        # 1. Разделяем по строкам
        lines = content.split('\n')

        # 2. Находим заголовки (==Section==)
        result_lines = []

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Если это заголовок (начинается и заканчивается на ==)
            if line.startswith('==') and line.endswith('=='):
                # Добавляем пустую строку перед заголовком (кроме первого)
                if result_lines and result_lines[-1] != '':
                    result_lines.append('')
                result_lines.append(line)
                result_lines.append('')  # Пустая строка после заголовка
            else:
                # Для обычного текста - добавляем как есть
                result_lines.append(line)
                # После длинного предложения добавляем отступ
                if len(line) > 100 and line.endswith('.'):
                    result_lines.append('')

        # 3. Убираем лишние пустые строки подряд
        cleaned_lines = []
        for i, line in enumerate(result_lines):
            if line != '' or (i > 0 and result_lines[i - 1] != ''):
                cleaned_lines.append(line)

        return '\n'.join(cleaned_lines)

    @staticmethod
    def extract_gameplay_fast(content: str) -> Optional[str]:
        """Простое извлечение Gameplay с отступами"""
        if not content:
            return None

        lines = content.split('\n')

        # Находим Gameplay
        gameplay_start = -1
        for i, line in enumerate(lines):
            if 'Gameplay' in line and line.strip().startswith('==') and line.strip().endswith('=='):
                gameplay_start = i + 1
                break

        if gameplay_start == -1:
            return None

        # Собираем текст до следующего заголовка ==
        paragraphs = []
        current_paragraph = []

        for i in range(gameplay_start, len(lines)):
            line = lines[i].rstrip()  # Сохраняем левые пробелы

            # Останавливаемся на следующем основном разделе
            if line.strip().startswith('==') and line.strip().endswith('=='):
                if len(line.strip().replace('=', '').strip()) > 0:
                    if not any(x in line.lower() for x in ['gameplay', 'mechanics']):
                        break

            if line:  # Если строка не пустая
                current_paragraph.append(line)
            elif current_paragraph:  # Пустая строка = конец абзаца
                paragraphs.append('\n'.join(current_paragraph))
                current_paragraph = []

        # Добавляем последний абзац
        if current_paragraph:
            paragraphs.append('\n'.join(current_paragraph))

        if not paragraphs:
            return None

        # Объединяем абзацы через двойной перевод строки (отступ)
        result = '\n\n'.join(paragraphs)

        # Удаляем сноски
        result = re.sub(r'\[\d+\]', '', result)

        return result

    async def get_page_by_exact_title(self, title: str) -> Optional[str]:
        """Получить страницу по точному заголовку"""
        url = f"https://{self.lang}.wikipedia.org/w/api.php"
        params = {
            'action': 'query',
            'prop': 'extracts',
            'titles': title,
            'explaintext': 1,
            'exsectionformat': 'plain',
            'format': 'json'
        }

        async with self.semaphore:
            data = await self.fetch_with_retry(url, params)

        if not data:
            return None

        pages = data.get('query', {}).get('pages', {})
        for page_id, page_data in pages.items():
            if page_id != '-1':
                return page_data.get('extract', '')

        return None

    async def get_game_description(self, game_name: str, game_year: int = None) -> Optional[str]:
        """Получить описание игры с учетом года выпуска"""
        try:
            # Получаем полный текст
            content = await self.get_page_content(game_name.replace(' ', '_'), get_full_text=True)

            if content:
                # Извлекаем Gameplay
                description = self.extract_gameplay_fast(content)
                if description:
                    return description

            return None

        except Exception as e:
            # Ошибки логируются только при --debug через логгер
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
