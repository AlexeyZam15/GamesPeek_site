# color_tags.py
import base64
import hashlib
import colorsys
from django import template
from django.utils.safestring import mark_safe
from django.core.cache import cache

register = template.Library()

# Время жизни кэша (в секундах) - 1 час
CACHE_TIMEOUT = 3600


def get_svg_cache_key(rating=None, similarity_percent=None, color=None, text=None, size=60):
    """Создает уникальный ключ для кэша SVG"""
    if rating is not None:
        # Для рейтинга
        rating_str = f"{float(rating):.1f}"
        cache_data = f"rating_{rating_str}_{color}_{size}"
    elif similarity_percent is not None:
        # Для схожести
        similarity_str = f"{float(similarity_percent):.0f}"
        cache_data = f"similarity_{similarity_str}_{color}_{size}"
    else:
        # Общий случай
        cache_data = f"svg_{color}_{text}_{size}"

    # Создаем хэш для ключа
    return f'svg_cache_{hashlib.md5(cache_data.encode()).hexdigest()}'


@register.simple_tag
def get_rating_color(rating):
    """
    Возвращает цвет в формате hex для рейтинга от 0 до 10
    От зеленого (высокий рейтинг) к красному (низкий рейтинг)
    """
    # Нормализуем рейтинг от 0 до 1
    normalized = max(0, min(10, float(rating))) / 10

    # Инвертируем для перехода от зеленого к красному
    # 0 = красный, 1 = зеленый
    hue = normalized * 120  # 0-120 градусов в HSL

    # Настройки насыщенности и яркости
    saturation = 0.7
    lightness = 0.5

    # Конвертируем HSL в RGB
    rgb = colorsys.hls_to_rgb(hue / 360, lightness, saturation)

    # Конвертируем в hex
    hex_color = '#{:02x}{:02x}{:02x}'.format(
        int(rgb[0] * 255),
        int(rgb[1] * 255),
        int(rgb[2] * 255)
    )

    return hex_color


@register.simple_tag
def get_similarity_color(similarity_percent):
    """
    Возвращает цвет в формате hex для процента схожести
    """
    if similarity_percent >= 80:
        return '#10b981'  # зеленый
    elif similarity_percent >= 60:
        return '#f59e0b'  # желтый/оранжевый
    else:
        return '#3b82f6'  # синий


@register.simple_tag
def rating_star_svg(rating, size=60):
    """
    Генерирует SVG звезду с рейтингом с кэшированием
    """
    color = get_rating_color(rating)
    rating_text = f"{float(rating):.1f}"

    # Создаем ключ кэша
    cache_key = get_svg_cache_key(rating=rating, color=color, size=size)

    # Пробуем получить из кэша
    cached_img = cache.get(cache_key)
    if cached_img:
        return mark_safe(cached_img)

    # Рассчитываем размер шрифта пропорционально размеру изображения
    font_size = max(10, 14 * size // 60)

    # Генерируем SVG
    svg_content = f'''
    <svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 60 60">
        <path d="M30 5L36.77 20.65L54 23L41.5 35.35L44.54 52L30 44.5L15.46 52L18.5 35.35L6 23L23.23 20.65L30 5Z" 
              fill="{color}" stroke="{color}" stroke-width="1"/>
        <text x="30" y="35" text-anchor="middle" fill="white" 
              font-family="Arial, sans-serif" font-weight="bold" font-size="{font_size}"
              style="text-shadow: 1px 1px 2px rgba(0,0,0,0.8);">
            {rating_text}
        </text>
    </svg>
    '''

    # Кодируем в base64
    svg_encoded = base64.b64encode(svg_content.encode('utf-8')).decode('utf-8')

    # Создаем тег img
    img_tag = (
        f'<img src="data:image/svg+xml;base64,{svg_encoded}" width="{size}" height="{size}" '
        f'alt="Rating: {rating_text}" title="Rating" '
        f'class="rating-svg">'
    )

    # Сохраняем в кэш
    cache.set(cache_key, img_tag, CACHE_TIMEOUT)

    return mark_safe(img_tag)


@register.simple_tag
def similarity_pattern_svg(similarity_percent, size=60):
    """
    Генерирует SVG узор с процентом схожести с кэшированием
    """
    color = get_similarity_color(similarity_percent)
    similarity_text = f"{float(similarity_percent):.0f}%"

    # Создаем ключ кэша
    cache_key = get_svg_cache_key(similarity_percent=similarity_percent, color=color, size=size)

    # Пробуем получить из кэша
    cached_img = cache.get(cache_key)
    if cached_img:
        return mark_safe(cached_img)

    # Рассчитываем размеры пропорционально
    font_size = max(9, 12 * size // 60)
    circle_radius = max(15, 20 * size // 60)

    # Генерируем SVG
    svg_content = f'''
    <svg xmlns="http://www.w3.org/2000/svg" width="{size}" height="{size}" viewBox="0 0 60 60">
        <!-- Центральный круг -->
        <circle cx="30" cy="30" r="{circle_radius}" fill="{color}" fill-opacity="0.9"/>

        <!-- Внешние выступы -->
        <path d="M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z"
              fill="{color}" fill-opacity="0.8" transform="rotate(0,30,30)"/>
        <path d="M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z"
              fill="{color}" fill-opacity="0.8" transform="rotate(45,30,30)"/>
        <path d="M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z"
              fill="{color}" fill-opacity="0.8" transform="rotate(90,30,30)"/>
        <path d="M30 3.75L33 16.25L43.75 18L36.25 25.5L38.75 36.25L30 31.25L21.25 36.25L23.75 25.5L16.25 18L27 16.25Z"
              fill="{color}" fill-opacity="0.8" transform="rotate(135,30,30)"/>

        <!-- Текст процента -->
        <text x="30" y="35" text-anchor="middle" fill="white" 
              font-family="Arial, sans-serif" font-weight="bold" font-size="{font_size}"
              style="text-shadow: 1px 1px 2px rgba(0,0,0,0.8);">
            {similarity_text}
        </text>
    </svg>
    '''

    # Кодируем в base64
    svg_encoded = base64.b64encode(svg_content.encode('utf-8')).decode('utf-8')

    # Создаем тег img
    img_tag = (
        f'<img src="data:image/svg+xml;base64,{svg_encoded}" width="{size}" height="{size}" '
        f'alt="Similarity: {similarity_text}" title="Similarity" '
        f'class="similarity-svg">'
    )

    # Сохраняем в кэш
    cache.set(cache_key, img_tag, CACHE_TIMEOUT)

    return mark_safe(img_tag)