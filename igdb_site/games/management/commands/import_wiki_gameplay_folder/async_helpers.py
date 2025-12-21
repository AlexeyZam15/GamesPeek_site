"""
Асинхронные хелперы для работы с Django ORM
"""

import asyncio
from typing import Dict, List, Optional, Set, Tuple  # Добавить Set
from django.db import connection
from asgiref.sync import sync_to_async
from django.db import transaction
from games.models import Game


# =========== Асинхронные ORM запросы ===========

@sync_to_async
def get_total_games_count() -> int:
    """Получить общее количество игр"""
    return Game.objects.count()


@sync_to_async
def get_games_without_wiki_count() -> int:
    """Получить количество игр без описания Wikipedia"""
    return Game.objects.filter(wiki_description__isnull=True).count()


@sync_to_async
def get_games_without_any_desc_count() -> int:
    """Получить количество игр без любого описания"""
    return Game.objects.filter(
        wiki_description__isnull=True,
        rawg_description__isnull=True,
        summary__isnull=True
    ).count()


@sync_to_async
def get_game_by_id_async(game_id: int) -> Optional[Dict]:
    """Получить игру по ID"""
    try:
        game = Game.objects.get(id=game_id)
        return {'id': game.id, 'name': game.name}
    except Game.DoesNotExist:
        return None


@sync_to_async
def get_games_for_processing(
        skip_existing: bool = True,
        only_empty: bool = False,
        skip_failed: bool = False,
        failed_ids: List[int] = None,
        limit: int = 0,
        include_not_found: bool = False,
        not_found_ids: Set[int] = None
) -> List[Dict]:
    """Получить игры для обработки с годом выпуска"""
    queryset = Game.objects.all()

    if skip_existing:
        queryset = queryset.filter(wiki_description__isnull=True)

    if only_empty:
        queryset = queryset.filter(
            wiki_description__isnull=True,
            rawg_description__isnull=True,
            summary__isnull=True
        )

    if skip_failed and failed_ids:
        queryset = queryset.exclude(id__in=failed_ids)

    if not include_not_found and not_found_ids:
        queryset = queryset.exclude(id__in=not_found_ids)

    queryset = queryset.order_by('id')

    if limit > 0:
        queryset = queryset[:limit]

    # Используем правильное название поля - first_release_date вместо release_date
    games = list(queryset.values_list('id', 'name', 'first_release_date'))
    return [{
        'id': g[0],
        'name': g[1],
        'release_year': g[2].year if g[2] else None
    } for g in games]


# =========== Асинхронное сохранение ===========

@sync_to_async
def save_batch_async(batch_results: Dict[int, str]) -> int:
    """Асинхронное сохранение батча результатов"""
    if not batch_results:
        return 0

    try:
        return _save_bulk_sync(batch_results)
    except Exception as e:
        print(f"❌ Ошибка bulk save: {e}")
        # Пробуем безопасное сохранение
        return _save_safe_sync(batch_results)


def _save_bulk_sync(batch_results: Dict[int, str]) -> int:
    """Синхронная массовая вставка"""
    with connection.cursor() as cursor:
        # Создаем временную таблицу
        cursor.execute("""
            CREATE TEMP TABLE temp_wiki_updates (
                game_id INTEGER PRIMARY KEY,
                description TEXT
            )
        """)

        # Вставляем данные пачками
        batch_size = 500
        items = list(batch_results.items())

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]
            if not batch:
                break

            # Используем executemany для безопасной вставки
            query = """
                INSERT INTO temp_wiki_updates (game_id, description) 
                VALUES (%s, %s)
            """

            batch_data = []
            for game_id, description in batch:
                # Используем параметризованные запросы для безопасности
                safe_desc = description
                batch_data.append((game_id, safe_desc))

            cursor.executemany(query, batch_data)

        # Обновляем основную таблицу
        cursor.execute("""
            UPDATE games_game 
            SET wiki_description = temp_wiki_updates.description 
            FROM temp_wiki_updates 
            WHERE games_game.id = temp_wiki_updates.game_id
        """)

        cursor.execute("DROP TABLE temp_wiki_updates")
        connection.commit()

        return len(batch_results)


def _save_safe_sync(batch_results: Dict[int, str]) -> int:
    """Синхронное безопасное сохранение по одному"""
    saved = 0

    for game_id, description in batch_results.items():
        try:
            with transaction.atomic():
                Game.objects.filter(id=game_id).update(wiki_description=description)
                saved += 1
        except Exception as e:
            print(f"   ❌ Ошибка сохранения игры {game_id}: {str(e)[:100]}...")

    return saved


# =========== Утилиты для параллельного сохранения ===========

class AsyncBatchSaver:
    """Асинхронный сохранитель батчей с поддержкой конкурентности"""

    def __init__(self, max_concurrent: int = 1):
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.total_saved = 0

    async def save_batch(self, batch_results: Dict[int, str]) -> int:
        """Сохранить батч с ограничением конкурентности"""
        async with self.semaphore:
            saved = await save_batch_async(batch_results)
            self.total_saved += saved
            return saved

    def get_total_saved(self) -> int:
        """Получить общее количество сохраненных"""
        return self.total_saved


async def save_multiple_batches(batches: list, max_concurrent: int = 1) -> int:
    """Сохранить несколько батчей параллельно"""
    saver = AsyncBatchSaver(max_concurrent=max_concurrent)

    tasks = []
    for batch in batches:
        if batch:
            task = asyncio.create_task(saver.save_batch(batch))
            tasks.append(task)

    # Ждем завершения всех задач
    if tasks:
        await asyncio.gather(*tasks)

    return saver.get_total_saved()