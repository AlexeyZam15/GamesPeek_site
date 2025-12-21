import time
from typing import Dict
from django.db import connection
from django.db import transaction
from games.models import Game


class BatchSaver:
    """Класс для пошагового сохранения результатов после каждого батча"""

    def __init__(self, batch_size: int = 200):
        self.batch_size = batch_size
        self.total_saved = 0
        self.start_time = time.time()
        self.temp_table_created = False

    def _ensure_temp_table(self, cursor):
        """Создать временную таблицу если не создана"""
        if not self.temp_table_created:
            cursor.execute("""
                           CREATE
                           TEMP TABLE IF NOT EXISTS batch_wiki_updates (
                    game_id INTEGER PRIMARY KEY,
                    description TEXT,
                    batch_num INTEGER DEFAULT 0
                )
                           """)
            self.temp_table_created = True

    def save_batch(self, batch_results: Dict[int, str], batch_num: int = 0) -> int:
        """Сохранить один батч результатов"""
        if not batch_results:
            return 0

        try:
            with connection.cursor() as cursor:
                self._ensure_temp_table(cursor)

                # Вставляем данные батча
                query = """
                        INSERT INTO batch_wiki_updates (game_id, description, batch_num)
                        VALUES (%s, %s, %s) ON CONFLICT (game_id) DO \
                        UPDATE \
                            SET description = EXCLUDED.description, \
                            batch_num = EXCLUDED.batch_num \
                        """

                batch_data = [(game_id, desc, batch_num)
                              for game_id, desc in batch_results.items()]

                cursor.executemany(query, batch_data)

                # Обновляем основную таблицу для этого батча
                cursor.execute("""
                               UPDATE games_game
                               SET wiki_description = batch_wiki_updates.description FROM batch_wiki_updates
                               WHERE games_game.id = batch_wiki_updates.game_id
                                 AND batch_wiki_updates.batch_num = %s
                               """, [batch_num])

                # Удаляем обработанные записи из временной таблицы
                cursor.execute("""
                               DELETE
                               FROM batch_wiki_updates
                               WHERE batch_num = %s
                               """, [batch_num])

                connection.commit()

                saved_count = len(batch_results)
                self.total_saved += saved_count

                return saved_count

        except Exception as e:
            print(f"Ошибка сохранения батча {batch_num}: {e}")
            # Fallback на безопасное сохранение
            return self._save_batch_safe(batch_results)

    def _save_batch_safe(self, batch_results: Dict[int, str]) -> int:
        """Безопасное сохранение батча по одному"""
        saved = 0
        for game_id, description in batch_results.items():
            try:
                with transaction.atomic():
                    Game.objects.filter(id=game_id).update(wiki_description=description)
                    saved += 1
            except Exception:
                pass
        self.total_saved += saved
        return saved

    def finalize(self):
        """Завершить работу, очистить временные таблицы"""
        if self.temp_table_created:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("DROP TABLE IF EXISTS batch_wiki_updates")
            except Exception:
                pass

    def get_stats(self):
        """Получить статистику сохранения"""
        elapsed = time.time() - self.start_time
        return {
            'total_saved': self.total_saved,
            'elapsed_time': elapsed,
            'speed': self.total_saved / elapsed if elapsed > 0 else 0
        }


# Простая функция для сохранения одного батча
def save_batch_simple(batch_results: Dict[int, str]) -> int:
    """Простое сохранение одного батча"""
    if not batch_results:
        return 0

    try:
        # Создаем список обновлений
        updates = []
        for game_id, description in batch_results.items():
            updates.append(
                Game(id=game_id, wiki_description=description)
            )

        # Массовое обновление
        Game.objects.bulk_update(updates, ['wiki_description'])
        return len(batch_results)

    except Exception as e:
        print(f"Ошибка простого сохранения батча: {e}")
        # Fallback
        saved = 0
        for game_id, description in batch_results.items():
            try:
                Game.objects.filter(id=game_id).update(wiki_description=description)
                saved += 1
            except Exception:
                pass
        return saved