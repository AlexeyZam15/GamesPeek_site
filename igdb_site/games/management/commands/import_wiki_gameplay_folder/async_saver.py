"""
Модуль для асинхронного сохранения результатов
"""

import asyncio
from typing import Dict
from django.db import connection
from asgiref.sync import sync_to_async
from django.db import transaction
from games.models import Game


class AsyncBatchSaver:
    """Асинхронный сохранитель батчей"""

    def __init__(self, max_concurrent_saves: int = 1):
        self.max_concurrent = max_concurrent_saves
        self.total_saved = 0
        self.semaphore = None

    async def __aenter__(self):
        self.semaphore = asyncio.Semaphore(self.max_concurrent)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def save_batch(self, batch_results: Dict[int, str]) -> int:
        """Сохранить батч асинхронно"""
        if not batch_results:
            return 0

        async with self.semaphore:
            # Используем sync_to_async для синхронного кода
            saved = await self._save_batch_safe(batch_results)
            self.total_saved += saved
            return saved

    @sync_to_async
    def _save_batch_safe(self, batch_results: Dict[int, str]) -> int:
        """Безопасное синхронное сохранение (обернутое в sync_to_async)"""
        if not batch_results:
            return 0

        try:
            # Пробуем bulk save
            return self._save_bulk(batch_results)
        except Exception as e:
            print(f"❌ Ошибка bulk save: {e}")
            # Fallback на поштучное сохранение
            return self._save_one_by_one(batch_results)

    def _save_bulk(self, batch_results: Dict[int, str]) -> int:
        """Массовое сохранение"""
        with connection.cursor() as cursor:
            # Создаем временную таблицу
            cursor.execute("""
                           CREATE
                           TEMP TABLE temp_wiki_updates (
                    game_id INTEGER PRIMARY KEY,
                    description TEXT
                )
                           """)

            # Вставляем данные
            query = """
                    INSERT INTO temp_wiki_updates (game_id, description)
                    VALUES (%s, %s) \
                    """

            batch_data = [(game_id, desc) for game_id, desc in batch_results.items()]
            cursor.executemany(query, batch_data)

            # Обновляем основную таблицу
            cursor.execute("""
                           UPDATE games_game
                           SET wiki_description = temp_wiki_updates.description FROM temp_wiki_updates
                           WHERE games_game.id = temp_wiki_updates.game_id
                           """)

            cursor.execute("DROP TABLE temp_wiki_updates")
            connection.commit()

            return len(batch_results)

    def _save_one_by_one(self, batch_results: Dict[int, str]) -> int:
        """Сохранение по одному"""
        saved = 0
        for game_id, description in batch_results.items():
            try:
                with transaction.atomic():
                    Game.objects.filter(id=game_id).update(wiki_description=description)
                    saved += 1
            except Exception:
                pass

        return saved

    def get_total_saved(self) -> int:
        """Получить общее количество сохраненных записей"""
        return self.total_saved


# Простая асинхронная функция
async def save_batch_async(batch_results: Dict[int, str]) -> int:
    """Простая асинхронная функция сохранения батча"""
    if not batch_results:
        return 0

    # Обертка для sync_to_async
    @sync_to_async
    def sync_save():
        try:
            with connection.cursor() as cursor:
                cursor.execute("""
                               CREATE
                               TEMP TABLE temp_async_updates (
                        game_id INTEGER PRIMARY KEY,
                        description TEXT
                    )
                               """)

                query = "INSERT INTO temp_async_updates (game_id, description) VALUES (%s, %s)"
                data = [(game_id, desc) for game_id, desc in batch_results.items()]
                cursor.executemany(query, data)

                cursor.execute("""
                               UPDATE games_game
                               SET wiki_description = temp_async_updates.description FROM temp_async_updates
                               WHERE games_game.id = temp_async_updates.game_id
                               """)

                cursor.execute("DROP TABLE temp_async_updates")
                connection.commit()

                return len(batch_results)
        except Exception:
            # Fallback
            saved = 0
            for game_id, description in batch_results.items():
                try:
                    with transaction.atomic():
                        Game.objects.filter(id=game_id).update(wiki_description=description)
                        saved += 1
                except Exception:
                    pass
            return saved

    return await sync_save()