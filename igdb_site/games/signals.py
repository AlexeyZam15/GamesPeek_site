"""
Сигналы Django для оптимизации отдельных соединений.
"""

from django.db.backends.signals import connection_created
from django.dispatch import receiver


@receiver(connection_created)
def optimize_sqlite_connection(sender, connection, **kwargs):
    """
    Легкая оптимизация каждого нового SQLite соединения.
    Не создает индексы, только настраивает параметры.
    """
    if connection.vendor == 'sqlite':
        try:
            cursor = connection.cursor()

            # Быстрые оптимизации без создания индексов
            cursor.execute("PRAGMA cache_size=-2000000;")
            cursor.execute("PRAGMA synchronous=NORMAL;")
            cursor.execute("PRAGMA temp_store=MEMORY;")
            cursor.execute("PRAGMA busy_timeout=30000;")

            cursor.close()
        except Exception:
            # Игнорируем ошибки - не критично
            pass