from django.contrib.sitemaps import views as sitemap_views
from django.http import HttpResponse
from django.core.cache import cache
from django.template import loader
import hashlib


def get_sitemap_cache_key(section=None):
    """
    Генерирует ключ для кэша sitemap.
    Кэш сбрасывается только когда меняется количество игр в базе.
    """
    from games.models import Game

    game_count = Game.objects.count()

    section_name = section or 'main'

    key_data = f"sitemap_{section_name}_v1_{game_count}"
    return hashlib.md5(key_data.encode()).hexdigest()


def sitemap_without_noindex(request, sitemaps, section=None, template_name='sitemap.xml',
                            content_type='application/xml'):
    """
    Кастомная view для sitemap без заголовка X-Robots-Tag noindex.
    С кэшированием до изменения количества игр.
    Поддерживает пагинацию (sitemap index).
    """
    # Если section не передан - создаём индекс вручную
    if section is None:
        print("[SITEMAP] Generating INDEX manually")

        # Создаём список URL для подкарт
        sitemap_urls = []
        for section_name, site in sitemaps.items():
            # Создаём экземпляр sitemap, чтобы получить paginator
            sitemap_instance = site()

            # Проверяем, есть ли пагинация
            if hasattr(sitemap_instance, 'paginator'):
                # Вызываем property, чтобы получить объект Paginator
                paginator = sitemap_instance.paginator
                # Добавляем все страницы
                for page_num in range(1, paginator.num_pages + 1):
                    if page_num == 1:
                        # Первая страница без параметра page
                        location = f"/sitemap-{section_name}.xml"
                    else:
                        location = f"/sitemap-{section_name}.xml?page={page_num}"
                    sitemap_urls.append({'location': location})
            else:
                # Без пагинации - одна ссылка
                sitemap_urls.append({'location': f"/sitemap-{section_name}.xml"})

        print(f"[SITEMAP] Found {len(sitemap_urls)} sitemap URLs")

        # Загружаем шаблон для индекса
        template = loader.get_template('sitemap_index.xml')
        response = HttpResponse(template.render({'sitemaps': sitemap_urls}), content_type=content_type)

        # Удаляем noindex заголовок
        if 'X-Robots-Tag' in response:
            del response['X-Robots-Tag']

        return response

    # Для подкарт - используем стандартную логику с кэшированием
    cache_key = get_sitemap_cache_key(section)
    cached_content = cache.get(cache_key)

    if cached_content:
        print(f"[SITEMAP CACHE HIT] {cache_key}")
        return HttpResponse(cached_content, content_type=content_type)

    print(f"[SITEMAP CACHE MISS] {cache_key} - generating...")

    # Генерируем конкретную подкарту
    response = sitemap_views.sitemap(request, sitemaps, section, template_name, content_type)

    # Кэшируем подкарту
    response.render()
    cache.set(cache_key, response.content, 60 * 60 * 24 * 30)
    print(f"[SITEMAP CACHED] {cache_key}")

    # Удаляем noindex заголовок
    if 'X-Robots-Tag' in response:
        del response['X-Robots-Tag']

    return response


def clear_sitemap_cache(request=None):
    """
    Принудительная очистка кэша sitemap.
    """
    from games.models import Game

    game_count = Game.objects.count()

    for section_name in ['main', 'similar_games']:
        key_data = f"sitemap_{section_name}_v1_{game_count}"
        cache_key = hashlib.md5(key_data.encode()).hexdigest()
        cache.delete(cache_key)

    return {"status": "cleared", "game_count": game_count}