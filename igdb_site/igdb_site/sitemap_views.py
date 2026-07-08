"""
Кастомные представления для генерации sitemap с правильными абсолютными URL.

Основные функции:
- Генерация sitemap index с абсолютными URL для всех подкарт
- Генерация подкарт с правильной пагинацией (последняя страница = 756 URL)
- Кэширование каждой страницы отдельно
- Отсутствие noindex заголовков для правильной индексации
"""

from django.http import HttpResponse
from django.core.cache import cache
from django.template import loader
from django.contrib.sites.shortcuts import get_current_site
import hashlib
import os


def get_full_url(request, location):
    """
    Преобразует относительный URL в абсолютный на основе окружения.

    Аргументы:
        request: HTTP запрос для определения домена и протокола
        location: Относительный путь (например: /sitemap-games.xml)

    Возвращает:
        str: Полный абсолютный URL
    """
    production_domain = os.getenv('PRODUCTION_DOMAIN', '')

    if production_domain:
        protocol = 'https'
        return f"{protocol}://{production_domain}{location}"
    else:
        current_site = get_current_site(request)
        protocol = 'https' if request.is_secure() else 'http'
        return f"{protocol}://{current_site.domain}{location}"


def get_sitemap_cache_key(section=None, page=None):
    """
    Генерирует ключ для кэша sitemap.

    Аргументы:
        section: Название секции sitemap ('games', 'similar' или None)
        page: Номер страницы (1, 2, 3, ...) или None для индекса

    Возвращает:
        str: MD5 хеш ключа для кэширования
    """
    from games.models import Game

    game_count = Game.objects.count()
    section_name = section or 'main'
    page_part = f"_page_{page}" if page is not None else ""
    key_data = f"sitemap_{section_name}{page_part}_v1_{game_count}"
    return hashlib.md5(key_data.encode()).hexdigest()


def generate_sitemap_page(request, section, sitemap_instance, page=1):
    """
    Генерирует одну страницу sitemap с правильной пагинацией.

    Аргументы:
        request: HTTP запрос
        section: Название секции ('games' или 'similar')
        sitemap_instance: Экземпляр Sitemap класса
        page: Номер страницы (1, 2, 3, ...)

    Возвращает:
        HttpResponse: Сгенерированная страница sitemap
    """
    # Получаем элементы для текущей страницы через Paginator
    paginator = sitemap_instance.paginator
    page_obj = paginator.page(page)
    items = page_obj.object_list

    # Генерируем URL для каждого элемента
    urls = []
    for item in items:
        loc = sitemap_instance.location(item)
        url_dict = {
            'location': loc,
            'changefreq': sitemap_instance.changefreq,
            'priority': sitemap_instance.priority,
        }

        # Добавляем lastmod если есть
        if hasattr(sitemap_instance, 'lastmod'):
            lastmod = sitemap_instance.lastmod(item)
            if lastmod:
                url_dict['lastmod'] = lastmod.isoformat() if hasattr(lastmod, 'isoformat') else str(lastmod)

        urls.append(url_dict)

    # Рендерим шаблон
    template = loader.get_template('sitemap_template.xml')
    content = template.render({'urlset': urls})

    return HttpResponse(content, content_type='application/xml')


def sitemap_without_noindex(request, sitemaps, section=None, template_name='sitemap.xml',
                            content_type='application/xml'):
    """
    Генерирует sitemap index или подкарту без заголовков noindex.

    Аргументы:
        request: HTTP запрос
        sitemaps: Словарь с картами сайта
        section: Название секции для подкарты или None для индекса
        template_name: Имя шаблона для рендеринга (не используется для подкарт)
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

        print(f"[SITEMAP] Generated {len(sitemap_urls)} sitemap URLs")

        template = loader.get_template('sitemap_index.xml')
        response = HttpResponse(
            template.render({'sitemaps': sitemap_urls}),
            content_type=content_type
        )

        if 'X-Robots-Tag' in response:
            del response['X-Robots-Tag']

        return response

    # Генерация подкарты
    # Определяем номер страницы из GET параметра
    page = request.GET.get('page', 1)
    try:
        page = int(page)
    except (ValueError, TypeError):
        page = 1

    # Проверяем кэш
    cache_key = get_sitemap_cache_key(section, page)
    cached_content = cache.get(cache_key)

    if cached_content:
        print(f"[SITEMAP CACHE HIT] {cache_key}")
        response = HttpResponse(cached_content, content_type=content_type)
        if 'X-Robots-Tag' in response:
            del response['X-Robots-Tag']
        return response

    print(f"[SITEMAP CACHE MISS] {cache_key} - generating...")

    # Получаем экземпляр sitemap
    if section in sitemaps:
        sitemap_instance = sitemaps[section]()
    else:
        return HttpResponse(status=404)

    # Генерируем страницу с правильной пагинацией
    response = generate_sitemap_page(request, section, sitemap_instance, page)

    # Кэшируем содержимое (не response, а его content)
    content = response.content
    cache.set(cache_key, content, 60 * 60 * 24 * 30)  # 30 дней
    print(f"[SITEMAP CACHED] {cache_key}")

    if 'X-Robots-Tag' in response:
        del response['X-Robots-Tag']

    return response


def clear_sitemap_cache(request=None):
    """
    Принудительно очищает кэш всех sitemap.

    Аргументы:
        request: HTTP запрос (опционально)

    Возвращает:
        dict: Статус очистки с количеством игр в базе
    """
    from games.models import Game

    game_count = Game.objects.count()
    cleared_count = 0

    for section_name in ['games', 'similar']:
        for page in range(1, 46):
            key_data = f"sitemap_{section_name}_page_{page}_v1_{game_count}"
            cache_key = hashlib.md5(key_data.encode()).hexdigest()
            if cache.delete(cache_key):
                cleared_count += 1

    return {"status": "cleared", "game_count": game_count, "cleared_keys": cleared_count}