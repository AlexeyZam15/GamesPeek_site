# games/management/commands/analyze_page.py

from django.core.management.base import BaseCommand
from django.test import Client
from django.db import connection
import sys


class Command(BaseCommand):
    help = 'Analyze SQL queries for a specific page'

    def add_arguments(self, parser):
        parser.add_argument(
            'url',
            type=str,
            help='URL to analyze (e.g., /, /games/, /games/list/)'
        )
        parser.add_argument(
            '--method',
            type=str,
            default='GET',
            help='HTTP method (default: GET)'
        )

    def handle(self, *args, **options):
        url = options['url']
        method = options['method']

        self.stdout.write(f"🔍 Analyzing {method} {url}...\n")

        # Создаем тестовый клиент
        client = Client()

        # Очищаем предыдущие запросы
        connection.queries_log.clear()

        # Делаем запрос
        if method.upper() == 'GET':
            response = client.get(url)
        elif method.upper() == 'POST':
            response = client.post(url)
        else:
            self.stderr.write(f"❌ Unsupported method: {method}")
            return

        # Анализируем запросы
        queries = connection.queries
        total_queries = len(queries)

        self.stdout.write(f"📊 Total queries: {total_queries}\n")

        if total_queries == 0:
            self.stdout.write("✅ No database queries detected")
            return

        # Группируем запросы по таблицам
        table_stats = {}
        query_types = {}

        for i, query in enumerate(queries, 1):
            sql = query['sql']

            # Определяем тип запроса
            query_type = 'OTHER'
            if sql.startswith('SELECT'):
                query_type = 'SELECT'
            elif sql.startswith('UPDATE'):
                query_type = 'UPDATE'
            elif sql.startswith('INSERT'):
                query_type = 'INSERT'
            elif sql.startswith('DELETE'):
                query_type = 'DELETE'

            query_types[query_type] = query_types.get(query_type, 0) + 1

            # Определяем таблицу (упрощенно)
            table = 'UNKNOWN'
            if 'FROM' in sql:
                # Пытаемся извлечь имя таблицы
                parts = sql.split()
                for j, part in enumerate(parts):
                    if part.upper() == 'FROM':
                        if j + 1 < len(parts):
                            table = parts[j + 1].replace('"', '').split('_')[0]
                            break

            table_stats[table] = table_stats.get(table, 0) + 1

            # Выводим первые 20 запросов
            if i <= 20:
                time_ms = float(query['time']) * 1000
                self.stdout.write(f"{i:3d}. [{time_ms:5.1f}ms] {sql[:100]}...")

        # Выводим статистику
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("📈 QUERY TYPE ANALYSIS:")
        for qtype, count in sorted(query_types.items()):
            percentage = (count / total_queries) * 100
            self.stdout.write(f"  {qtype:10s}: {count:4d} ({percentage:5.1f}%)")

        self.stdout.write("\n📊 TABLE ANALYSIS:")
        for table, count in sorted(table_stats.items(), key=lambda x: x[1], reverse=True):
            percentage = (count / total_queries) * 100
            self.stdout.write(f"  {table:20s}: {count:4d} ({percentage:5.1f}%)")

        # Рекомендации
        self.stdout.write("\n💡 RECOMMENDATIONS:")
        if total_queries > 30:
            self.stdout.write("  ❌ CRITICAL: Too many queries (>30)")
        elif total_queries > 15:
            self.stdout.write("  ⚠️  WARNING: High query count (>15)")
        else:
            self.stdout.write("  ✅ GOOD: Acceptable query count")

        if query_types.get('SELECT', 0) > total_queries * 0.8:
            self.stdout.write("  ⚠️  Many SELECT queries - check prefetch_related()")

        if 'UPDATE' in query_types:
            self.stdout.write(f"  ⚠️  UPDATE queries detected: {query_types['UPDATE']}")

        self.stdout.write(f"\n✅ Status code: {response.status_code}")
        self.stdout.write(f"✅ Response size: {len(response.content)} bytes")