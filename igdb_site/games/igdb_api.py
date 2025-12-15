# igdb_api.py
import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from functools import lru_cache
from django.conf import settings
from django.core.cache import cache

# Глобальные переменные
DEBUG = False
_session = None  # Глобальная сессия для всех запросов


def set_debug_mode(debug_enabled):
    """Установить режим отладки"""
    global DEBUG
    DEBUG = debug_enabled


def debug_print(message):
    """Печать только в режиме отладки"""
    if DEBUG:
        print(message)


def get_session():
    """Получить или создать сессию с настройками retry"""
    global _session

    if _session is None:
        session = requests.Session()

        # Настройка retry стратегии
        retry_strategy = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )

        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=100)
        session.mount("https://", adapter)
        session.mount("http://", adapter)

        _session = session

    return _session


# Кэш для токена с использованием Django cache
def get_igdb_access_token():
    """Получает access token для IGDB API с кэшированием."""
    cache_key = 'igdb_access_token'
    token_data = cache.get(cache_key)

    if token_data and token_data.get('expiry', 0) > time.time():
        debug_print("🔑 Using cached access token")
        return token_data['token']

    debug_print("🔑 Getting new IGDB access token...")

    session = get_session()
    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': settings.IGDB_CLIENT_ID,
        'client_secret': settings.IGDB_CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }

    try:
        response = session.post(url, params=params, timeout=10)
        debug_print(f"Token request status: {response.status_code}")

        if response.status_code != 200:
            debug_print(f"❌ Error response: {response.text}")
            response.raise_for_status()

        data = response.json()
        access_token = data['access_token']

        # Кэшируем токен на 29 дней (минус 1 день на всякий случай)
        expiry_time = time.time() + (29 * 24 * 60 * 60)
        cache.set(cache_key, {'token': access_token, 'expiry': expiry_time}, timeout=30 * 24 * 60 * 60)

        debug_print("✅ Successfully got new access token")
        return access_token

    except requests.exceptions.RequestException as e:
        debug_print(f"❌ Failed to get IGDB access token: {e}")
        raise Exception(f"Failed to get IGDB access token: {e}")


# Кэш для часто запрашиваемых данных
def get_cached_igdb_data(cache_key, endpoint, query, ttl=3600):
    """Получает данные из кэша или делает запрос к IGDB."""
    cached_data = cache.get(cache_key)
    if cached_data is not None:
        debug_print(f"📦 Cache hit for {cache_key}")
        return cached_data

    data = make_igdb_request(endpoint, query)
    cache.set(cache_key, data, timeout=ttl)
    return data


def make_igdb_request(endpoint, query, debug=None):
    """Выполняет запрос к IGDB API с отладкой."""
    local_debug = debug if debug is not None else DEBUG

    if local_debug:
        print(f"🚀 Making IGDB request to {endpoint}...")
        print(f"Query: {query}")

    access_token = get_igdb_access_token()
    headers = {
        'Client-ID': settings.IGDB_CLIENT_ID,
        'Authorization': f'Bearer {access_token}',
    }

    url = f'https://api.igdb.com/v4/{endpoint}'
    session = get_session()

    try:
        response = session.post(url, headers=headers, data=query, timeout=30)

        if local_debug:
            print(f"Response status: {response.status_code}")

        if response.status_code != 200:
            print(f"❌ Error response: {response.text}")
            response.raise_for_status()

        result = response.json()

        if local_debug:
            print(f"✅ Successfully got {len(result)} results")

        return result

    except requests.exceptions.RequestException as e:
        print(f"❌ IGDB API request failed: {e}")
        raise Exception(f"IGDB API request failed: {e}")


# Оптимизированные функции с кэшированием
def get_companies(company_ids):
    """Получает данные о компаниях из IGDB с кэшированием"""
    if not company_ids:
        return []

    # Создаем уникальный ключ кэша
    cache_key = f'igdb_companies_{hash(tuple(sorted(company_ids)))}'
    fields = "id,name,description,logo.url,website"
    query = f'fields {fields}; where id = ({",".join(map(str, company_ids))});'

    return get_cached_igdb_data(cache_key, 'companies', query, ttl=24 * 3600)


def get_themes(theme_ids):
    """Получает данные о темах из IGDB с кэшированием"""
    if not theme_ids:
        return []

    cache_key = f'igdb_themes_{hash(tuple(sorted(theme_ids)))}'
    fields = "id,name"
    query = f'fields {fields}; where id = ({",".join(map(str, theme_ids))});'

    return get_cached_igdb_data(cache_key, 'themes', query, ttl=24 * 3600)


def get_player_perspectives(perspective_ids):
    """Получает данные о перспективах игрока из IGDB с кэшированием"""
    if not perspective_ids:
        return []

    cache_key = f'igdb_perspectives_{hash(tuple(sorted(perspective_ids)))}'
    fields = "id,name"
    query = f'fields {fields}; where id = ({",".join(map(str, perspective_ids))});'

    return get_cached_igdb_data(cache_key, 'player_perspectives', query, ttl=24 * 3600)


def get_game_modes(mode_ids):
    """Получает данные о режимах игры из IGDB с кэшированием"""
    if not mode_ids:
        return []

    cache_key = f'igdb_modes_{hash(tuple(sorted(mode_ids)))}'
    fields = "id,name"
    query = f'fields {fields}; where id = ({",".join(map(str, mode_ids))});'

    return get_cached_igdb_data(cache_key, 'game_modes', query, ttl=24 * 3600)


def get_series(series_ids):
    """Получает данные о сериях из IGDB с кэшированием"""
    if not series_ids:
        return []

    cache_key = f'igdb_series_{hash(tuple(sorted(series_ids)))}'
    fields = "id,name,description,created_at"
    query = f'fields {fields}; where id = ({",".join(map(str, series_ids))});'

    return get_cached_igdb_data(cache_key, 'collections', query, ttl=24 * 3600)
