# igdb_api.py
import requests
import time  # ← ДОБАВЬТЕ ЭТУ СТРОКУ
from django.conf import settings

# Глобальная переменная для управления выводом
DEBUG = False

# Кэш для токена
_access_token = None
_token_expiry = None


def set_debug_mode(debug_enabled):
    """Установить режим отладки"""
    global DEBUG
    DEBUG = debug_enabled


def debug_print(message):
    """Печать только в режиме отладки"""
    if DEBUG:
        print(message)


def get_igdb_access_token():
    """Получает access token для IGDB API с кэшированием."""
    global _access_token, _token_expiry

    # Если токен еще действителен, возвращаем его
    if _access_token and _token_expiry and time.time() < _token_expiry:
        debug_print("🔑 Using cached access token")
        return _access_token

    debug_print("🔑 Getting new IGDB access token...")

    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': settings.IGDB_CLIENT_ID,
        'client_secret': settings.IGDB_CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }

    try:
        response = requests.post(url, params=params)
        debug_print(f"Token request status: {response.status_code}")

        if response.status_code != 200:
            debug_print(f"❌ Error response: {response.text}")
            response.raise_for_status()

        data = response.json()
        _access_token = data['access_token']
        # Токены обычно действительны 60 дней, но будем консервативны - 30 дней
        _token_expiry = time.time() + (30 * 24 * 60 * 60) - 3600  # минус 1 час на всякий случай

        debug_print("✅ Successfully got new access token")
        return _access_token

    except requests.exceptions.RequestException as e:
        debug_print(f"❌ Failed to get IGDB access token: {e}")
        raise Exception(f"Failed to get IGDB access token: {e}")


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

    try:
        response = requests.post(url, headers=headers, data=query)

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


def get_companies(company_ids):
    """Получает данные о компаниях из IGDB"""
    fields = "id,name,description,country,logo.url,website"
    query = f'fields {fields}; where id = ({",".join(map(str, company_ids))});'
    return make_igdb_request('companies', query)


def get_themes(theme_ids):
    """Получает данные о темах из IGDB"""
    fields = "id,name"
    query = f'fields {fields}; where id = ({",".join(map(str, theme_ids))});'
    return make_igdb_request('themes', query)


def get_player_perspectives(perspective_ids):
    """Получает данные о перспективах игрока из IGDB"""
    fields = "id,name"
    query = f'fields {fields}; where id = ({",".join(map(str, perspective_ids))});'
    return make_igdb_request('player_perspectives', query)


def get_game_modes(mode_ids):
    """Получает данные о режимах игры из IGDB"""
    fields = "id,name"
    query = f'fields {fields}; where id = ({",".join(map(str, mode_ids))});'
    return make_igdb_request('game_modes', query)


def get_series(series_ids):
    """Получает данные о сериях из IGDB"""
    fields = "id,name,description,created_at"
    query = f'fields {fields}; where id = ({",".join(map(str, series_ids))});'
    return make_igdb_request('collections', query)  # В IGDB серии называются collections
