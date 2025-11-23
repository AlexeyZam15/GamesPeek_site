# igdb_api.py
import requests
from django.conf import settings

# Глобальная переменная для управления выводом
DEBUG = False


def set_debug_mode(debug_enabled):
    """Установить режим отладки"""
    global DEBUG
    DEBUG = debug_enabled


def debug_print(message):
    """Печать только в режиме отладки"""
    if DEBUG:
        print(message)


def get_igdb_access_token():
    """Получает access token для IGDB API с отладкой."""
    debug_print("🔑 Getting IGDB access token...")

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
        token = data['access_token']
        debug_print("✅ Successfully got access token")
        return token

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