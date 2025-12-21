from django.core.management.base import BaseCommand
from django.db.models import Count
from games.models import Game, GameTypeEnum


class Command(BaseCommand):
    help = 'Check current game types status after migration'

    def handle(self, *args, **options):
        self.stdout.write(self.style.MIGRATE_HEADING("=" * 70))
        self.stdout.write(self.style.MIGRATE_HEADING("GAME TYPE STATUS CHECK"))
        self.stdout.write(self.style.MIGRATE_HEADING("=" * 70))

        try:
            # 1. Basic statistics
            self.show_basic_stats()

            # 2. Distribution by type
            self.show_type_distribution()

            # 3. Test properties
            self.test_properties()

            self.stdout.write(self.style.SUCCESS("\n✓ Check complete!"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n✗ Error: {e}"))
            import traceback
            self.stdout.write(self.style.ERROR(traceback.format_exc()))

    def show_basic_stats(self):
        """Show basic statistics"""
        self.stdout.write("\n" + self.style.MIGRATE_LABEL("1. BASIC STATISTICS"))

        total = Game.objects.count()
        with_type = Game.objects.filter(game_type__isnull=False).count()
        without_type = Game.objects.filter(game_type__isnull=True).count()

        self.stdout.write(f"   Total games: {total}")
        self.stdout.write(f"   Games with game_type: {with_type}")
        self.stdout.write(f"   Games without game_type: {without_type}")

        if total > 0:
            coverage = with_type / total * 100
            self.stdout.write(f"   Coverage: {coverage:.1f}%")

    def show_type_distribution(self):
        """Show distribution by game type"""
        self.stdout.write("\n" + self.style.MIGRATE_LABEL("2. TYPE DISTRIBUTION"))

        # Get counts using values() - безопасный способ
        counts = Game.objects.values('game_type').annotate(
            count=Count('game_type')
        ).order_by('game_type')

        total = Game.objects.count()

        self.stdout.write(f"   {'Type ID':<10} {'Type Name':<25} {'Count':<10} {'%':<10}")
        self.stdout.write(f"   {'-' * 10:<10} {'-' * 25:<25} {'-' * 10:<10} {'-' * 10:<10}")

        for item in counts:
            type_id = item['game_type']
            count = item['count']

            # Get type name
            type_name = "NULL"
            if type_id is not None:
                for choice_id, choice_name in GameTypeEnum.CHOICES:
                    if choice_id == type_id:
                        type_name = str(choice_name)
                        break
                else:
                    type_name = f"Unknown ({type_id})"

            # Calculate percentage
            percentage = count / total * 100 if total > 0 else 0

            # Format output
            type_id_str = str(type_id) if type_id is not None else "NULL"
            count_str = str(count)
            pct_str = f"{percentage:.1f}%"

            # Color coding
            if type_id is None:
                style = self.style.WARNING
            elif type_id in GameTypeEnum.PRIMARY_GAME_TYPES:
                style = self.style.HTTP_INFO
            else:
                style = self.style.HTTP_NOT_MODIFIED

            self.stdout.write(style(
                f"   {type_id_str:<10} {type_name:<25} {count_str:<10} {pct_str:<10}"
            ))

    def test_properties(self):
        """Test that game type properties work correctly"""
        self.stdout.write("\n" + self.style.MIGRATE_LABEL("3. TESTING PROPERTIES"))

        # Get 3 games with different types using safe query
        test_games = Game.objects.all()[:3]

        if not test_games:
            self.stdout.write(self.style.WARNING("   No games found to test!"))
            return

        for game in test_games:
            self.stdout.write(f"\n   Game: {self.style.HTTP_INFO(game.name)}")
            self.stdout.write(f"     ID: {game.id}, IGDB ID: {game.igdb_id}")

            if game.game_type is None:
                self.stdout.write(self.style.WARNING("     game_type: NULL"))
                self.stdout.write(self.style.WARNING("     Display: No game type"))
            else:
                # Test display method
                display = game.get_game_type_display()
                self.stdout.write(f"     game_type value: {game.game_type}")
                self.stdout.write(f"     Display: {display}")

                # Test properties - use safe approach
                try:
                    self.stdout.write(f"     game_type_name: {game.game_type_name}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"     Error getting game_type_name: {e}"))

                try:
                    self.stdout.write(f"     is_primary_game: {game.is_primary_game}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"     Error getting is_primary_game: {e}"))