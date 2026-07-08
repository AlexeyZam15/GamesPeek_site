"""
Кастомные представления для генерации sitemap с правильными абсолютными URL.

Основные функции:
- Генерация sitemap index с абсолютными URL для всех подкарт
- Кэширование подкарт с инвалидацией при изменении количества игр
- Отсутствие noindex заголовков для правильной индексации
"""

from django.contrib.sitemaps import views as sitemap_views
from django.http import HttpResponse
from django.core.cache import cache
from django.template import loader
from django.contrib.sites.shortcuts import get_current_site
import hashlib


def get_full_url(request, location):
    """
    Преобразует относительный URL в абсолютный на основе текущего сайта.

    Аргументы:
        request: HTTP запрос для определения домена и протокола
        location: Относительный путь (например: /sitemap-games.xml)

    Возвращает:
        str: Полный абсолютный URL (https://gamespeek.dpdns.org/sitemap-games.xml)
    """
    current_site = get_current_site(request)
    protocol = 'https' if request.is_secure() else 'http'
    return f"{protocol}://{current_site.domain}{location}"


def get_sitemap_cache_key(section=None):
    """
    Генерирует ключ для кэша sitemap с учётом количества игр.

    Кэш автоматически инвалидируется при добавлении или удалении игр,
    потому что ключ содержит общее количество записей в базе данных.

    Аргументы:
        section: Название секции sitemap ('games', 'similar' или None)

    Возвращает:
        str: MD5 хеш ключа для кэширования
    """
    from games.models import Game

    game_count = Game.objects.count()
    section_name = section or 'main'
    key_data = f"sitemap_{section_name}_v1_{game_count}"
    return hashlib.md5(key_data.encode()).hexdigest()


def sitemap_without_noindex(request, sitemaps, section=None, template_name='sitemap.xml',
                            content_type='application/xml'):
    """
    Генерирует sitemap index или подкарту без заголовков noindex.

    Эта функция заменяет стандартную django.contrib.sitemaps.views.sitemap
    и добавляет следующие улучшения:
    1. Генерация абсолютных URL в sitemap index
    2. Кэширование подкарт до изменения количества игр
    3. Отсутствие X-Robots-Tag: noindex для правильной индексации

    Аргументы:
        request: HTTP запрос
        sitemaps: Словарь с картами сайта
        section: Название секции для подкарты или None для индекса
        template_name: Имя шаблона для рендеринга
        content_type: MIME тип ответа

    Возвращает:
        HttpResponse: Сгенерированная карта сайта
    """
    # Генерация sitemap index (главного файла)
    if section is None:
        print("[SITEMAP] Generating INDEX manually")

        sitemap_urls = []

        for section_name, site in sitemaps.items():
            sitemap_instance = site()

            if hasattr(sitemap_instance, 'paginator'):
                paginator = sitemap_instance.paginator

                for page_num in range(1, paginator.num_pages + 1):
                    if page_num == 1:
                        relative_url = f"/sitemap-{section_name}.xml"
                    else:
                        relative_url = f"/sitemap-{section_name}.xml?page={page_num}"

                    absolute_url = get_full_url(request, relative_url)
                    sitemap_urls.append({'location': absolute_url})
            else:
                relative_url = f"/sitemap-{section_name}.xml"
                absolute_url = get_full_url(request, relative_url)
                sitemap_urls.append({'location': absolute_url})

        print(f"[SITEMAP] Generated {len(sitemap_urls)} sitemap URLs with absolute paths")

        template = loader.get_template('sitemap_index.xml')
        response = HttpResponse(
            template.render({'sitemaps': sitemap_urls}),
            content_type=content_type
        )

        if 'X-Robots-Tag' in response:
            del response['X-Robots-Tag']

        return response

    # Генерация подкарты с кэшированием
    cache_key = get_sitemap_cache_key(section)
    cached_content = cache.get(cache_key)

    if cached_content:
        print(f"[SITEMAP CACHE HIT] {cache_key}")
        return HttpResponse(cached_content, content_type=content_type)

    print(f"[SITEMAP CACHE MISS] {cache_key} - generating...")

    response = sitemap_views.sitemap(request, sitemaps, section, template_name, content_type)

    response.render()
    cache.set(cache_key, response.content, 60 * 60 * 24 * 30)  # 30 дней
    print(f"[SITEMAP CACHED] {cache_key}")

    if 'X-Robots-Tag' in response:
        del response['X-Robots-Tag']

    return response


def clear_sitemap_cache(request=None):
    """
    Принудительно очищает кэш всех sitemap.

    Используется при обновлении контента для немедленного
    отражения изменений в карте сайта.

    Аргументы:
        request: HTTP запрос (опционально)

    Возвращает:
        dict: Статус очистки с количеством игр в базе
    """
    from games.models import Game

    game_count = Game.objects.count()

    for section_name in ['games', 'similar']:
        key_data = f"sitemap_{section_name}_v1_{game_count}"
        cache_key = hashlib.md5(key_data.encode()).hexdigest()
        cache.delete(cache_key)

    return {"status": "cleared", "game_count": game_count}