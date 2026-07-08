from django.contrib.sitemaps import views as sitemap_views
from django.http import HttpResponse
from django.core.cache import cache
import hashlib


def get_sitemap_cache_key(section=None):
    """
    Генерирует ключ для кэша sitemap.
    Кэш сбрасывается только когда меняется количество игр в базе.
    """
    from games.models import Game

    game_count = Game.objects.count()

    # Учитываем section, чтобы ключи для разных sitemap не пересекались
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
    cache_key = get_sitemap_cache_key(section)

    # Пробуем взять из кэша
    cached_content = cache.get(cache_key)

    if cached_content:
        print(f"[SITEMAP CACHE HIT] {cache_key}")
        return HttpResponse(cached_content, content_type=content_type)

    print(f"[SITEMAP CACHE MISS] {cache_key} - generating...")

    # Генерируем sitemap
    response = sitemap_views.sitemap(request, sitemaps, section, template_name, content_type)

    # Удаляем проблемный заголовок
    if 'X-Robots-Tag' in response:
        del response['X-Robots-Tag']

    # Принудительно рендерим ответ
    response.render()

    # Получаем содержимое
    response_content = response.content

    # Кэшируем содержимое на 30 дней
    cache.set(cache_key, response_content, 60 * 60 * 24 * 30)

    print(f"[SITEMAP CACHED] {cache_key}")

    return response


def clear_sitemap_cache(request=None):
    """
    Принудительная очистка кэша sitemap.
    """
    from games.models import Game

    game_count = Game.objects.count()

    # Очищаем ключи для обоих sitemap
    for section_name in ['main', 'similar_games']:
        key_data = f"sitemap_{section_name}_v1_{game_count}"
        cache_key = hashlib.md5(key_data.encode()).hexdigest()
        cache.delete(cache_key)

    return {"status": "cleared", "game_count": game_count}