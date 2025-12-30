"""
Validate that prefetch is working correctly.
"""

from django.core.management.base import BaseCommand
from django.db import connection
from django.test import RequestFactory
from games.views import home


class Command(BaseCommand):
    help = 'Validate prefetch and count SQL queries'

    def handle(self, *args, **options):
        self.stdout.write("🔍 Validating prefetch optimization...")

        factory = RequestFactory()
        request = factory.get('/')

        # Очищаем кэш
        from django.core.cache import cache
        cache.delete('optimized_home_final_v1')

        # Сбрасываем счетчик
        connection.queries_log.clear()

        # Запускаем view
        response = home(request)

        # Анализируем запросы
        queries = connection.queries
        total_queries = len(queries)

        self.stdout.write(f"\n📊 Total SQL queries: {total_queries}")

        if total_queries < 10:
            self.stdout.write(self.style.SUCCESS("✅ Excellent! Less than 10 queries"))
        elif total_queries < 20:
            self.stdout.write(self.style.WARNING("⚠️  Acceptable: Less than 20 queries"))
        else:
            self.stdout.write(self.style.ERROR(f"❌ Problematic: {total_queries} queries"))

        # Группируем по типам запросов
        query_types = {}
        for query in queries:
            sql = query.get('sql', '').upper()

            if 'SELECT' in sql:
                if 'JOIN' in sql or 'FROM' in sql:
                    table = 'unknown'
                    if 'FROM' in sql:
                        parts = sql.split('FROM')
                        if len(parts) > 1:
                            table = parts[1].split()[0].strip('"`')

                    query_types[table] = query_types.get(table, 0) + 1

        self.stdout.write("\n📈 Queries by table:")
        for table, count in sorted(query_types.items(), key=lambda x: x[1], reverse=True):
            if count > 3:  # Показываем только проблемные
                self.stdout.write(f"  {table}: {count} queries")

        # Проверяем N+1 паттерны
        self.stdout.write("\n🔎 Checking for N+1 patterns...")
        nplus1_found = False

        for table, count in query_types.items():
            if count > 10:  # Более 10 запросов к одной таблице
                self.stdout.write(self.style.ERROR(f"  ❌ N+1 detected: {count} queries to {table}"))
                nplus1_found = True

        if not nplus1_found:
            self.stdout.write(self.style.SUCCESS("  ✅ No N+1 patterns detected"))

        self.stdout.write(f"\n⏱️  Page load time: {response.headers.get('X-Response-Time', 'N/A')}")
        self.stdout.write(f"📋 Header X-DB-Queries: {response.headers.get('X-DB-Queries', 'N/A')}")

        self.stdout.write("\n✅ Validation complete!")