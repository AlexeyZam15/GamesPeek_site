"""
Модуль для генерации микроразметки BreadcrumbList в формате JSON-LD.
Используется для формирования навигационных цепочек в результатах поиска Яндекса и Google.
"""

from typing import List, Dict, Any


def generate_breadcrumb_list(
        items: List[Dict[str, str]],
        base_url: str = "https://gamespeek.dpdns.org"
) -> str:
    """
    Генерирует JSON-LD разметку для навигационной цепочки на основе Schema.org BreadcrumbList.

    Параметры:
        items: Список словарей с ключами 'name' и 'url'.
               name - название элемента (минимум 4 символа, без эмодзи)
               url - относительный или абсолютный путь к разделу
        base_url: Базовый URL сайта (по умолчанию https://gamespeek.dpdns.org)

    Возвращает:
        Строку с JSON-LD разметкой для вставки в <head> или перед </body>

    Пример использования:
        breadcrumb_items = [
            {"name": "Главная", "url": "/"},
            {"name": "Игры", "url": "/games"},
            {"name": "The Witcher 3", "url": ""}
        ]
        json_ld = generate_breadcrumb_list(breadcrumb_items)
    """

    # Словарь для валидации - исключаем нежелательные элементы
    reserved_names = {"главная", "home", "main", "главная страница"}

    # Фильтруем и подготавливаем элементы навигационной цепочки
    filtered_items = []
    position_counter = 1

    for idx, item in enumerate(items, start=1):
        name = item.get("name", "").strip()
        url = item.get("url", "").strip()

        # Пропускаем элементы с коротким именем (менее 4 символов без пробелов)
        name_without_spaces = name.replace(" ", "")
        if len(name_without_spaces) < 4:
            continue

        # Пропускаем слишком длинные имена (более 100 символов)
        if len(name) > 100:
            continue

        # Пропускаем зарезервированные названия (главная страница)
        if name.lower() in reserved_names:
            continue

        # Ограничиваем цепочку максимум 5 элементами (Яндекс рекомендует до 3)
        if len(filtered_items) >= 5:
            break

        # Формируем абсолютный URL, если передан относительный путь
        absolute_url = None
        if url and url != "/":
            if url.startswith("/"):
                absolute_url = f"{base_url}{url}"
            elif url.startswith("http://") or url.startswith("https://"):
                # Проверяем, что домен совпадает с base_url
                if base_url in url:
                    absolute_url = url
                else:
                    absolute_url = None
            else:
                absolute_url = f"{base_url}/{url}"
        elif url == "/":
            absolute_url = base_url

        # Для последнего элемента URL может быть пустым - не включаем его в разметку
        is_last_item = (idx == len(items))

        item_data = {
            "@type": "ListItem",
            "position": position_counter,
            "name": name
        }

        # Добавляем URL только для не последних элементов
        if not is_last_item and absolute_url:
            item_data["item"] = absolute_url

        filtered_items.append(item_data)
        position_counter += 1

    # Если после фильтрации осталось меньше 2 элементов, цепочка не имеет смысла
    if len(filtered_items) < 2:
        return ""

    # Формируем полную JSON-LD структуру
    breadcrumb_dict = {
        "@context": "https://schema.org",
        "@type": "BreadcrumbList",
        "itemListElement": filtered_items
    }

    # Импортируем json внутри функции для избежания циклических зависимостей
    import json
    return f'<script type="application/ld+json">\n{json.dumps(breadcrumb_dict, ensure_ascii=False, indent=2)}\n</script>'


def generate_game_breadcrumb(
        game_title: str,
        platform_name: str = None,
        genre_name: str = None,
        base_url: str = "https://gamespeek.dpdns.org"
) -> str:
    """
    Генерирует навигационную цепочку для страницы игры.

    Параметры:
        game_title: Название игры (минимум 4 символа)
        platform_name: Название платформы (опционально)
        genre_name: Название жанра (опционально)
        base_url: Базовый URL сайта

    Возвращает:
        JSON-LD разметку для страницы игры
    """
    breadcrumb_items = []

    # Первый элемент - ссылка на главную (не включается в финальную разметку, если имя "Главная")
    breadcrumb_items.append({"name": "Каталог игр", "url": "/"})

    # Второй элемент - раздел игр по платформе (если указана)
    if platform_name and len(platform_name.replace(" ", "")) >= 4:
        platform_slug = platform_name.lower().replace(" ", "_").replace("-", "_")
        breadcrumb_items.append({
            "name": platform_name,
            "url": f"/platforms/{platform_slug}"
        })

    # Третий элемент - раздел игр по жанру (если указан и отличается от платформы)
    if genre_name and len(genre_name.replace(" ", "")) >= 4:
        if not platform_name or genre_name.lower() != platform_name.lower():
            genre_slug = genre_name.lower().replace(" ", "_").replace("-", "_")
            breadcrumb_items.append({
                "name": genre_name,
                "url": f"/genres/{genre_slug}"
            })

    # Последний элемент - текущая игра (без URL)
    if len(game_title.replace(" ", "")) >= 4:
        breadcrumb_items.append({"name": game_title, "url": ""})

    return generate_breadcrumb_list(breadcrumb_items, base_url)


def generate_category_breadcrumb(
        category_name: str,
        parent_category: str = None,
        base_url: str = "https://gamespeek.dpdns.org"
) -> str:
    """
    Генерирует навигационную цепочку для страницы категории (список игр).

    Параметры:
        category_name: Название текущей категории
        parent_category: Название родительской категории (опционально)
        base_url: Базовый URL сайта

    Возвращает:
        JSON-LD разметку для страницы категории
    """
    breadcrumb_items = []

    # Первый элемент - корневой раздел
    breadcrumb_items.append({"name": "Каталог игр", "url": "/"})

    # Второй элемент - родительская категория (если указана)
    if parent_category and len(parent_category.replace(" ", "")) >= 4:
        parent_slug = parent_category.lower().replace(" ", "_").replace("-", "_")
        breadcrumb_items.append({
            "name": parent_category,
            "url": f"/categories/{parent_slug}"
        })

    # Текущая категория (без URL)
    if len(category_name.replace(" ", "")) >= 4:
        breadcrumb_items.append({"name": category_name, "url": ""})

    return generate_breadcrumb_list(breadcrumb_items, base_url)


def generate_review_breadcrumb(
        game_title: str,
        review_title: str,
        base_url: str = "https://gamespeek.dpdns.org"
) -> str:
    """
    Генерирует навигационную цепочку для страницы обзора игры.

    Параметры:
        game_title: Название игры
        review_title: Заголовок обзора
        base_url: Базовый URL сайта

    Возвращает:
        JSON-LD разметку для страницы обзора
    """
    breadcrumb_items = []

    # Хлебные крошки: Каталог > Игра > Обзор
    if len(game_title.replace(" ", "")) >= 4:
        game_slug = game_title.lower().replace(" ", "_").replace("-", "_")
        breadcrumb_items.append({"name": "Каталог игр", "url": "/"})
        breadcrumb_items.append({"name": game_title, "url": f"/games/{game_slug}"})

        if len(review_title.replace(" ", "")) >= 4:
            breadcrumb_items.append({"name": review_title, "url": ""})

    return generate_breadcrumb_list(breadcrumb_items, base_url)


def generate_similar_games_breadcrumb(
        game_title: str,
        base_url: str = "https://gamespeek.dpdns.org"
) -> str:
    """
    Генерирует навигационную цепочку для страницы похожих игр.

    Параметры:
        game_title: Название исходной игры
        base_url: Базовый URL сайта

    Возвращает:
        JSON-LD разметку для страницы со списком похожих игр
    """
    breadcrumb_items = []

    # Первый элемент - каталог игр
    breadcrumb_items.append({"name": "Каталог игр", "url": "/"})

    # Второй элемент - страница текущей игры
    if len(game_title.replace(" ", "")) >= 4:
        game_slug = game_title.lower().replace(" ", "_").replace("-", "_")
        breadcrumb_items.append({
            "name": game_title,
            "url": f"/games/{game_slug}"
        })

    # Третий элемент - страница похожих игр (без URL)
    breadcrumb_items.append({"name": "Похожие игры", "url": ""})

    return generate_breadcrumb_list(breadcrumb_items, base_url)