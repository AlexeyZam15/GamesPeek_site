import requests
from django.conf import settings
import time


def get_igdb_access_token():
    """Получает access token для IGDB API с отладкой."""
    print("🔑 Getting IGDB access token...")

    url = 'https://id.twitch.tv/oauth2/token'
    params = {
        'client_id': settings.IGDB_CLIENT_ID,
        'client_secret': settings.IGDB_CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }

    print(f"Client ID: {settings.IGDB_CLIENT_ID[:10]}...")  # Покажем первые 10 символов
    print(f"Client Secret: {settings.IGDB_CLIENT_SECRET[:10]}...")  # Покажем первые 10 символов

    try:
        response = requests.post(url, params=params)
        print(f"Token request status: {response.status_code}")

        if response.status_code != 200:
            print(f"❌ Error response: {response.text}")
            response.raise_for_status()

        data = response.json()
        token = data['access_token']
        print("✅ Successfully got access token")
        return token

    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to get IGDB access token: {e}")
        raise Exception(f"Failed to get IGDB access token: {e}")


def make_igdb_request(endpoint, data):
    """Выполняет запрос к IGDB API с отладкой."""
    print(f"🚀 Making IGDB request to {endpoint}...")

    access_token = get_igdb_access_token()
    headers = {
        'Client-ID': settings.IGDB_CLIENT_ID,
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'text/plain'
    }

    url = f'https://api.igdb.com/v4/{endpoint}'
    print(f"Request URL: {url}")
    print(f"Request data: {data[:100]}...")  # Покажем первые 100 символов

    try:
        response = requests.post(url, headers=headers, data=data)
        print(f"API response status: {response.status_code}")

        if response.status_code != 200:
            print(f"❌ API Error: {response.text}")
            response.raise_for_status()

        result = response.json()
        print(f"✅ Successfully got {len(result)} results")
        return result

    except requests.exceptions.RequestException as e:
        print(f"❌ IGDB API request failed: {e}")
        raise Exception(f"IGDB API request failed: {e}")