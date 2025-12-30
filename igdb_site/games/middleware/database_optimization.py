"""
Middleware для оптимизации базы данных при первом запросе.
Решает проблему доступа к БД во время инициализации.
"""

import threading
import time
from django.utils.deprecation import MiddlewareMixin
from django.db import connection
from django.conf import settings


class DatabaseOptimizationMiddleware(MiddlewareMixin):
    """
    Middleware для оптимизации SQLite базы данных.
    Выполняет оптимизацию один раз при первом HTTP запросе.
    """

    # Статические переменные для отслеживания состояния
    _optimization_started = False
    _optimization_completed = False
    _lock = threading.Lock()

    def process_request(self, request):
        """
        Обрабатывает каждый запрос, запускает оптимизацию при первом запросе.
        """
        # Только для SQLite и только в режиме DEBUG
        if not settings.DEBUG or connection.vendor != 'sqlite':
            return

        # Проверяем, нужно ли запускать оптимизацию
        if not self._optimization_started:
            with self._lock:
                if not self._optimization_started:
                    self._optimization_started = True

                    # Запускаем оптимизацию в отдельном потоке
                    thread = threading.Thread(
                        target=self._run_database_optimization,
                        daemon=True  # Демон-поток, завершится с основным
                    )
                    thread.start()

        return None

    def _run_database_optimization(self):
        """Запускает полную оптимизацию базы данных."""
        try:
            print("🔧 Запуск оптимизации базы данных...")
            start_time = time.time()

            # 1. Оптимизируем настройки SQLite
            self._optimize_sqlite_settings()

            # 2. Создаем индексы для производительности
            indexes_created = self._create_performance_indexes()

            # 3. Выполняем дополнительную оптимизацию
            self._run_additional_optimizations()

            elapsed_time = time.time() - start_time
            self._optimization_completed = True

            print(f"✅ Оптимизация завершена за {elapsed_time:.2f} секунд")
            print(f"📊 Создано индексов: {indexes_created}")

        except Exception as e:
            print(f"⚠️ Ошибка при оптимизации базы данных: {e}")

    def _optimize_sqlite_settings(self):
        """Применяет оптимизации настроек SQLite."""
        cursor = connection.cursor()

        try:
            print("  ⚙️  Настройка SQLite...")

            # Проверяем текущий режим журнала
            cursor.execute("PRAGMA journal_mode;")
            current_mode = cursor.fetchone()[0].lower()

            # Включаем WAL mode для лучшей конкурентности
            if current_mode != 'wal':
                cursor.execute("PRAGMA journal_mode=WAL;")
                print("    ✅ Включен WAL mode")

            # Устанавливаем большой кэш (2GB)
            cursor.execute("PRAGMA cache_size=-2000000;")

            # Оптимальный режим синхронизации
            cursor.execute("PRAGMA synchronous=NORMAL;")

            # Временные таблицы в памяти
            cursor.execute("PRAGMA temp_store=MEMORY;")

            # Увеличиваем таймаут для занятой БД
            cursor.execute("PRAGMA busy_timeout=30000;")

            # Используем mmap для больших файлов
            cursor.execute("PRAGMA mmap_size=30000000000;")

            # Включаем внешние ключи
            cursor.execute("PRAGMA foreign_keys=ON;")

            # Устанавливаем лимит журнала
            cursor.execute("PRAGMA journal_size_limit=67108864;")

        except Exception as e:
            print(f"    ⚠️  Ошибка настройки SQLite: {e}")
        finally:
            cursor.close()

    def _create_performance_indexes(self):
        """Создает индексы для повышения производительности."""
        cursor = connection.cursor()

        try:
            print("  📊 Проверка и создание индексов...")

            # Проверяем существующие индексы
            cursor.execute("""
                           SELECT name
                           FROM sqlite_master
                           WHERE type = 'index'
                             AND name LIKE 'idx_game_%'
                           """)
            existing_indexes = {row[0] for row in cursor.fetchall()}

            # Определяем какие индексы нужно создать
            indexes_to_check = [
                # Критические индексы для производительности
                {
                    'name': 'idx_game_rating_count_rating',
                    'sql': """
                           CREATE INDEX idx_game_rating_count_rating
                               ON games_game (rating_count DESC, rating DESC)
                           """,
                    'desc': "Сортировка по рейтингу"
                },
                {
                    'name': 'idx_game_first_release',
                    'sql': """
                           CREATE INDEX idx_game_first_release
                               ON games_game (first_release_date DESC)
                           """,
                    'desc': "Сортировка по дате релиза"
                },
                {
                    'name': 'idx_game_name_ci',
                    'sql': """
                           CREATE INDEX idx_game_name_ci
                               ON games_game (name COLLATE NOCASE)
                           """,
                    'desc': "Поиск по имени"
                },
                {
                    'name': 'idx_game_type',
                    'sql': """
                           CREATE INDEX idx_game_type
                               ON games_game (game_type)
                           """,
                    'desc': "Фильтрация по типу игры"
                },

                # M2M индексы
                {
                    'name': 'idx_game_genres_game',
                    'sql': """
                           CREATE INDEX idx_game_genres_game
                               ON games_game_genres (game_id, genre_id)
                           """,
                    'desc': "Игры по жанрам"
                },
                {
                    'name': 'idx_game_genres_genre',
                    'sql': """
                           CREATE INDEX idx_game_genres_genre
                               ON games_game_genres (genre_id, game_id)
                           """,
                    'desc': "Жанры по играм"
                },
                {
                    'name': 'idx_game_platforms_game',
                    'sql': """
                           CREATE INDEX idx_game_platforms_game
                               ON games_game_platforms (game_id, platform_id)
                           """,
                    'desc': "Игры по платформам"
                },
                {
                    'name': 'idx_game_platforms_platform',
                    'sql': """
                           CREATE INDEX idx_game_platforms_platform
                               ON games_game_platforms (platform_id, game_id)
                           """,
                    'desc': "Платформы по играм"
                },
                {
                    'name': 'idx_game_keywords_game',
                    'sql': """
                           CREATE INDEX idx_game_keywords_game
                               ON games_game_keywords (game_id, keyword_id)
                           """,
                    'desc': "Игры по ключевым словам"
                },
            ]

            created_count = 0
            for index_info in indexes_to_check:
                if index_info['name'] not in existing_indexes:
                    try:
                        cursor.execute(index_info['sql'])
                        created_count += 1
                        print(f"    ✅ Создан: {index_info['desc']}")
                    except Exception as e:
                        print(f"    ⚠️  Ошибка создания {index_info['name']}: {e}")

            return created_count

        except Exception as e:
            print(f"    ⚠️  Ошибка при работе с индексами: {e}")
            return 0
        finally:
            cursor.close()

    def _run_additional_optimizations(self):
        """Выполняет дополнительные оптимизации."""
        cursor = connection.cursor()

        try:
            # Оптимизируем запросы
            cursor.execute("PRAGMA optimize;")

            # Анализируем базу для статистики
            cursor.execute("PRAGMA analysis_limit=1000;")

            # Проверяем целостность (только для DEBUG)
            if settings.DEBUG:
                cursor.execute("PRAGMA integrity_check;")
                result = cursor.fetchone()
                if result and result[0] == 'ok':
                    print("    ✅ Проверка целостности: OK")

        except Exception as e:
            # Игнорируем ошибки дополнительных оптимизаций
            pass
        finally:
            cursor.close()