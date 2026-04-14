import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'igdb_site.settings')
django.setup()

from django.apps import apps
from django.db import connection
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Показывает реальные размеры всех таблиц в базе данных PostgreSQL'

    def handle(self, *args, **options):
        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80))
        self.stdout.write(self.style.SUCCESS('📊 РЕАЛЬНЫЕ РАЗМЕРЫ ТАБЛИЦ В БАЗЕ ДАННЫХ'))
        self.stdout.write(self.style.SUCCESS('=' * 80))

        table_sizes = []

        for model in apps.get_models():
            table_name = model._meta.db_table
            with connection.cursor() as cursor:
                cursor.execute("""
                    SELECT pg_total_relation_size(%s) / (1024 * 1024)
                """, [table_name])
                size_mb = cursor.fetchone()[0] or 0

                cursor.execute("SELECT COUNT(*) FROM %s" % table_name)
                row_count = cursor.fetchone()[0]

            if size_mb > 0 or row_count > 0:
                table_sizes.append((model.__name__, table_name, row_count, size_mb))

        table_sizes.sort(key=lambda x: x[3], reverse=True)

        total_size = 0
        total_rows = 0

        self.stdout.write(f"\n{'Модель':<35} {'Таблица':<35} {'Строк':>12} {'Размер MB':>12}")
        self.stdout.write('-' * 80)

        for name, table, rows, size in table_sizes:
            total_size += size
            total_rows += rows
            self.stdout.write(f"{name:<35} {table:<35} {rows:>12,} {size:>12,.1f}")

        self.stdout.write('-' * 80)
        self.stdout.write(f"{'ИТОГО':<35} {'':<35} {total_rows:>12,} {total_size:>12,.1f}")
        self.stdout.write('=' * 80)
        self.stdout.write(self.style.SUCCESS(f"\n✅ Всего таблиц: {len(table_sizes)}"))
        self.stdout.write(self.style.SUCCESS(f"📊 Всего строк: {total_rows:,}"))
        self.stdout.write(self.style.SUCCESS(f"💾 Общий размер БД: {total_size:.1f} MB"))
        self.stdout.write('=' * 80)