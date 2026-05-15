"""
Проверка обоих sitemap: sitemap.xml и sitemap_similar_games.xml
Запуск: python check_sitemap.py
"""

import os
import sys
import django
from xml.etree import ElementTree as ET

# Настройка Django
BASE_DIR = r'P:\Users\Alexey\Desktop\igdb_site\igdb_site'
sys.path.insert(0, BASE_DIR)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
os.environ['DJANGO_ALLOW_ASYNC_UNSAFE'] = 'true'

django.setup()

from django.test.client import Client


def check_sitemap(url, name):
    """Проверяет отдельный sitemap"""
    print("\n" + "=" * 60)
    print(f"ПРОВЕРКА: {name}")
    print("=" * 60)

    client = Client()

    response = client.get(url)

    if response.status_code != 200:
        print(f"❌ {name} не доступен (статус: {response.status_code})")
        return None

    print(f"✅ Статус: {response.status_code}")

    content = response.content.decode('utf-8')

    # Проверяем, это XML или индекс sitemap
    if 'sitemapindex' in content:
        print("   Тип: Индекс sitemap (содержит ссылки на другие sitemap)")

        try:
            root = ET.fromstring(content)
            ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            sitemaps_list = root.findall('sitemap:sitemap', ns)

            print(f"\n   Вложенных sitemap: {len(sitemaps_list)}")

            for sitemap_elem in sitemaps_list[:5]:
                loc = sitemap_elem.find('sitemap:loc', ns)
                if loc is not None:
                    print(f"      - {loc.text}")

            if len(sitemaps_list) > 5:
                print(f"      ... и ещё {len(sitemaps_list) - 5}")

        except ET.ParseError as e:
            print(f"   Ошибка парсинга: {e}")

        return None

    # Парсим обычный sitemap
    try:
        root = ET.fromstring(content)
        ns = {'sitemap': 'http://www.sitemaps.org/schemas/sitemap/0.9'}

        urls = root.findall('sitemap:url', ns)

        print(f"\n1. Всего URL в sitemap: {len(urls)}")

        # Категоризируем URL
        game_urls = []
        static_urls = []
        similar_urls = []

        for url_elem in urls:
            loc = url_elem.find('sitemap:loc', ns)
            if loc is not None and loc.text:
                url_text = loc.text

                # Проверяем на find_similar (с параметром в URL)
                if 'find_similar=1' in url_text or '?find_similar' in url_text:
                    similar_urls.append(url_text)
                elif '/games/' in url_text:
                    # Проверяем, это страница игры или список
                    parts = url_text.rstrip('/').split('/')
                    if len(parts) > 0 and parts[-1].isdigit():
                        game_urls.append(url_text)
                    else:
                        static_urls.append(url_text)
                else:
                    static_urls.append(url_text)

        print(f"   - Страниц игр (/games/ID/): {len(game_urls)}")
        print(f"   - Статических страниц: {len(static_urls)}")
        print(f"   - Страниц find_similar: {len(similar_urls)}")

        # Показываем статические страницы
        print("\n2. Статические страницы в sitemap:")
        if static_urls:
            for url in static_urls:
                print(f"      {url}")
        else:
            print("      (нет)")

        # Проверяем наличие /games/
        has_games_list = any('/games/' in url and 'find_similar' not in url and not url.split('/')[-1].isdigit()
                             for url in static_urls)

        print("\n3. Страница /games/ в sitemap:", end=" ")
        if has_games_list:
            print("✅ ДА")
        else:
            print("❌ НЕТ")

        # Показываем примеры find_similar страниц
        print("\n4. Примеры find_similar страниц (первые 5):")
        if similar_urls:
            for url in similar_urls[:5]:
                print(f"      {url}")
            if len(similar_urls) > 5:
                print(f"      ... и ещё {len(similar_urls) - 5}")
        else:
            print("      (нет)")

        # Показываем примеры игр
        if not similar_urls:
            print("\n5. Примеры игр в sitemap (первые 5):")
            for url in game_urls[:5]:
                print(f"      {url}")
            if len(game_urls) > 5:
                print(f"      ... и ещё {len(game_urls) - 5}")

        return {
            'name': name,
            'total': len(urls),
            'games': len(game_urls),
            'static': len(static_urls),
            'similar': len(similar_urls),
            'has_games_list': has_games_list
        }

    except ET.ParseError as e:
        print(f"❌ Ошибка парсинга XML: {e}")
        return None


def main():
    print("\n" + "=" * 60)
    print("ПРОВЕРКА ОБОИХ SITEMAP")
    print("=" * 60)

    results = []

    # Проверяем основной sitemap
    result1 = check_sitemap('/sitemap.xml', 'sitemap.xml')
    if result1:
        results.append(result1)

    # Проверяем sitemap похожих игр
    result2 = check_sitemap('/sitemap_similar_games.xml', 'sitemap_similar_games.xml')
    if result2:
        results.append(result2)

    # Итоги
    print("\n" + "=" * 60)
    print("СВОДКА")
    print("=" * 60)

    for r in results:
        print(f"\n{r['name']}:")
        print(f"   Всего URL: {r['total']}")
        print(f"   Игры: {r['games']}")
        print(f"   Статические: {r['static']}")
        print(f"   Find_similar: {r['similar']}")
        print(f"   /games/ в sitemap: {'✅' if r['has_games_list'] else '❌'}")


if __name__ == "__main__":
    main()