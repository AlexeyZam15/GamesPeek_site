# games/management/commands/check_similar_ids.py
"""Команда для проверки заполненности similar_game_ids."""

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db import connection
from games.models import Game


class Command(BaseCommand):
    """Check similar_game_ids statistics."""

    help = 'Check how many games have non-empty similar_game_ids'

    def handle(self, *args, **options):
        total = Game.objects.count()
        with_ids = Game.objects.exclude(similar_game_ids=[]).exclude(similar_game_ids__isnull=True).count()
        empty_ids = Game.objects.filter(Q(similar_game_ids=[]) | Q(similar_game_ids__isnull=True)).count()

        # Статистика по длине списков
        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT array_length(similar_game_ids, 1) as length,
                                  COUNT(*) as count
                           FROM games_game
                           WHERE similar_game_ids IS NOT NULL AND array_length(similar_game_ids, 1) > 0
                           GROUP BY length
                           ORDER BY length
                           """)
            length_stats = cursor.fetchall()

        # Размер поля
        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT pg_size_pretty(SUM(pg_column_size(similar_game_ids))) as total_size,
                                  COUNT(*)                                              as total_with_ids
                           FROM games_game
                           WHERE similar_game_ids IS NOT NULL
                             AND array_length(similar_game_ids, 1) > 0
                           """)
            row = cursor.fetchone()
            similar_ids_size = row[0] if row else '0 bytes'
            games_with_ids = row[1] if row else 0

        self.stdout.write(self.style.SUCCESS('=' * 50))
        self.stdout.write(self.style.SUCCESS('SIMILAR_GAME_IDS STATISTICS'))
        self.stdout.write(self.style.SUCCESS('=' * 50))
        self.stdout.write(f"\n📊 GAMES:")
        self.stdout.write(f"   TOTAL GAMES: {total}")
        self.stdout.write(f"   ✅ With similar_game_ids: {with_ids} ({with_ids / total * 100:.1f}%)")
        self.stdout.write(f"   ❌ Empty similar_game_ids: {empty_ids} ({empty_ids / total * 100:.1f}%)")

        self.stdout.write(f"\n📊 LIST LENGTH DISTRIBUTION:")
        for length, count in length_stats:
            self.stdout.write(f"   {length} games: {count}")

        self.stdout.write(f"\n📊 SIMILAR_GAME_IDS FIELD:")
        self.stdout.write(f"   Total size: {similar_ids_size}")
        self.stdout.write(f"   Games with non-empty: {games_with_ids}")

        # Проверка: все ли списки по 12 ID
        all_twelve = all(length == 12 for length, count in length_stats)
        if all_twelve and length_stats:
            self.stdout.write(self.style.SUCCESS(f"\n✅ All {with_ids} games have exactly 12 IDs"))
        elif length_stats:
            self.stdout.write(self.style.WARNING(f"\n⚠️  Not all games have 12 IDs"))

        self.stdout.write(self.style.SUCCESS('=' * 50))