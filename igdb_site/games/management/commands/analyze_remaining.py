"""
Analyze remaining 13 queries on home page.
"""

from django.core.management.base import BaseCommand
from django.db import connection
from django.test import RequestFactory
from games.views import home


class Command(BaseCommand):
    help = 'Analyze remaining queries on home page'

    def handle(self, *args, **options):
        self.stdout.write("=== Analyzing remaining 13 queries ===")

        # Очищаем кэш
        from django.core.cache import cache
        cache.delete('optimized_home_final_v4')

        # Создаем запрос
        factory = RequestFactory()
        request = factory.get('/')

        # Сбрасываем счетчик
        connection.queries_log.clear()

        # Выполняем view
        response = home(request)

        queries = connection.queries

        self.stdout.write(f"\n📊 Total queries: {len(queries)}")
        self.stdout.write("=" * 60)

        # Выводим ВСЕ запросы с нумерацией
        for i, query in enumerate(queries, 1):
            sql = query.get('sql', '')
            time_taken = float(query.get('time', 0))

            # Упрощаем для отображения
            simple_sql = sql.replace('\n', ' ').replace('  ', ' ')
            if len(simple_sql) > 100:
                simple_sql = simple_sql[:100] + "..."

            # Определяем тип
            query_type = "SELECT"
            if "UPDATE" in sql.upper():
                query_type = "UPDATE"
            elif "INSERT" in sql.upper():
                query_type = "INSERT"

            # Определяем таблицу
            table = "unknown"
            if "FROM" in sql.upper():
                parts = sql.upper().split("FROM")
                if len(parts) > 1:
                    table_part = parts[1].strip().split()[0]
                    table = table_part.strip('"`')

            self.stdout.write(f"{i:2d}. [{time_taken:.3f}s] {query_type} {table}: {simple_sql}")

        # Группируем для анализа
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("📈 ANALYSIS BY QUERY TYPE:")

        query_types = {}
        for query in queries:
            sql = query.get('sql', '').upper()
            if "SELECT" in sql:
                # Детализируем SELECT запросы
                if "JOIN" in sql:
                    key = "SELECT (with JOIN)"
                elif "COUNT" in sql:
                    key = "SELECT (COUNT)"
                else:
                    key = "SELECT (simple)"
            elif "UPDATE" in sql:
                key = "UPDATE"
            elif "INSERT" in sql:
                key = "INSERT"
            else:
                key = "OTHER"

            query_types[key] = query_types.get(key, 0) + 1

        for query_type, count in sorted(query_types.items()):
            self.stdout.write(f"  {query_type}: {count}")

        # Анализ по таблицам
        self.stdout.write("\n📊 ANALYSIS BY TABLE:")
        table_counts = {}
        for query in queries:
            sql = query.get('sql', '').upper()

            # Ищем таблицы
            tables_found = []
            if "FROM" in sql:
                from_part = sql.split("FROM")[1]
                # Берем первую таблицу после FROM
                first_table = from_part.strip().split()[0].strip('"`')
                tables_found.append(first_table)

            if "JOIN" in sql:
                # Ищем таблицы в JOIN
                join_parts = sql.split("JOIN")
                for part in join_parts[1:]:
                    table_name = part.strip().split()[0].strip('"`')
                    tables_found.append(table_name)

            for table in tables_found:
                table_counts[table] = table_counts.get(table, 0) + 1

        for table, count in sorted(table_counts.items(), key=lambda x: x[1], reverse=True):
            self.stdout.write(f"  {table}: {count} queries")

        # Рекомендации
        self.stdout.write("\n💡 RECOMMENDATIONS:")

        if len(queries) <= 10:
            self.stdout.write("  ✅ Excellent! ≤10 queries is optimal")
        elif len(queries) <= 15:
            self.stdout.write("  ⚠️  Good: ≤15 queries is acceptable")
        else:
            self.stdout.write("  ❌ Needs improvement: >15 queries")

        # Проверяем prefetch эффективность
        join_queries = sum(1 for q in queries if "JOIN" in q.get('sql', '').upper())
        if join_queries >= 3:
            self.stdout.write("  ✅ Prefetch is working (JOIN queries present)")
        else:
            self.stdout.write("  ⚠️  Low JOIN count - check prefetch")

        self.stdout.write("\n✅ Analysis complete!")