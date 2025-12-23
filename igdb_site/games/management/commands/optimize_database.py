"""
Команда для ручной оптимизации базы данных.
"""

import time
from django.core.management.base import BaseCommand
from django.db import connection
from django.conf import settings


class Command(BaseCommand):
    help = 'Оптимизирует базу данных вручную (без middleware)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--quick',
            action='store_true',
            help='Быстрая оптимизация (только настройки, без индексов)'
        )
        parser.add_argument(
            '--vacuum',
            action='store_true',
            help='Выполнить VACUUM (дефрагментация)'
        )
        parser.add_argument(
            '--indexes-only',
            action='store_true',
            help='Только создать индексы'
        )

    def handle(self, *args, **options):
        self.stdout.write("🛠️  Ручная оптимизация базы данных...")

        if connection.vendor != 'sqlite':
            self.stdout.write(self.style.WARNING("⚠️ Поддерживается только SQLite"))
            return

        start_time = time.time()

        try:
            # Импортируем middleware для использования его методов
            from games.middleware.database_optimization import DatabaseOptimizationMiddleware

            middleware = DatabaseOptimizationMiddleware(get_response=None)

            if not options['indexes_only']:
                # Оптимизация настроек
                self.stdout.write("⚙️  Оптимизация настроек SQLite...")
                middleware._optimize_sqlite_settings()

            if not options['quick']:
                # Создание индексов
                self.stdout.write("📊 Создание индексов...")
                indexes_created = middleware._create_performance_indexes()
                self.stdout.write(f"    Создано индексов: {indexes_created}")

            # Дополнительные оптимизации
            if options['vacuum']:
                self.stdout.write("🌀 Выполнение VACUUM...")
                cursor = connection.cursor()
                cursor.execute("VACUUM;")
                cursor.close()
                self.stdout.write("    VACUUM выполнен")

            elapsed_time = time.time() - start_time
            self.stdout.write(self.style.SUCCESS(
                f"✅ Оптимизация завершена за {elapsed_time:.2f} секунд"
            ))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Ошибка: {e}"))