"""
Оптимизированный SQLite backend для Django.
"""

import sqlite3
from django.db.backends.sqlite3.base import (
    DatabaseWrapper as SQLiteDatabaseWrapper,
    DatabaseFeatures,
    DatabaseOperations,
    DatabaseCreation,
    DatabaseClient,
    DatabaseIntrospection,
)


class OptimizedDatabaseFeatures(DatabaseFeatures):
    """Расширенные возможности для SQLite."""
    supports_transactions = True
    uses_savepoints = True
    can_clone_databases = True
    supports_foreign_keys = True
    supports_column_check_constraints = True


class OptimizedDatabaseOperations(DatabaseOperations):
    """Оптимизированные операции для SQLite."""

    def bulk_batch_size(self, fields, objs):
        """Увеличиваем размер батча для bulk операций."""
        # Увеличиваем с 999 (стандарт) до 2000
        if len(fields) > 1:
            return 2000 // len(fields)
        return 2000


class OptimizedDatabaseWrapper(SQLiteDatabaseWrapper):
    """Главный класс с оптимизациями."""

    features_class = OptimizedDatabaseFeatures
    ops_class = OptimizedDatabaseOperations

    def get_new_connection(self, conn_params):
        """Создает новое соединение с оптимизациями."""
        # Создаем базовое соединение
        conn = super().get_new_connection(conn_params)

        # Применяем оптимизации
        self._optimize_connection(conn)

        return conn

    def _optimize_connection(self, conn):
        """Применяет все оптимизации к соединению."""
        cursor = conn.cursor()

        optimizations = [
            # Основные оптимизации
            ("PRAGMA journal_mode=WAL;", "WAL mode"),
            ("PRAGMA cache_size=-2000000;", "Cache size 2GB"),
            ("PRAGMA mmap_size=30000000000;", "MMAP 30GB"),
            ("PRAGMA synchronous=NORMAL;", "Normal sync"),
            ("PRAGMA temp_store=MEMORY;", "Temp in memory"),
            ("PRAGMA locking_mode=NORMAL;", "Normal locking"),
            ("PRAGMA busy_timeout=30000;", "Busy timeout 30s"),

            # Оптимизации запросов
            ("PRAGMA foreign_keys=ON;", "Foreign keys"),
            ("PRAGMA recursive_triggers=ON;", "Recursive triggers"),

            # Для производительности
            ("PRAGMA journal_size_limit=67108864;", "Journal limit 64MB"),  # 64MB
            ("PRAGMA page_size=4096;", "Page size 4KB"),

            # Статистика для оптимизатора
            ("PRAGMA analysis_limit=1000;", "Analysis limit"),
            ("PRAGMA optimize;", "Query optimization"),
        ]

        for sql, description in optimizations:
            try:
                cursor.execute(sql)
                if "journal_mode" in sql:
                    result = cursor.fetchone()
                    print(f"  ✅ {description}: {result[0] if result else 'OK'}")
            except Exception as e:
                print(f"  ⚠️ {description}: {e}")

        cursor.close()

    def create_cursor(self, name=None):
        """Создает курсор с оптимизациями."""
        cursor = super().create_cursor(name)

        # Настраиваем курсор для производительности
        cursor.arraysize = 1000  # Размер выборки по умолчанию

        return cursor