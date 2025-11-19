from django.core.management.base import BaseCommand
from games.models import Keyword


class Command(BaseCommand):
    help = 'Reset keyword categories (set all to null)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be reset without actually doing it'
        )
        parser.add_argument(
            '--confirmed',
            action='store_true',
            help='Skip confirmation prompt'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        confirmed = options['confirmed']

        # Статистика
        total_keywords = Keyword.objects.count()
        classified_keywords = Keyword.objects.filter(category__isnull=False).count()
        unclassified_keywords = Keyword.objects.filter(category__isnull=True).count()

        self.stdout.write("🔄 Keyword Category Reset")
        self.stdout.write("=" * 40)
        self.stdout.write(f"📊 Current statistics:")
        self.stdout.write(f"   Total keywords: {total_keywords}")
        self.stdout.write(f"   Classified: {classified_keywords}")
        self.stdout.write(f"   Unclassified: {unclassified_keywords}")
        self.stdout.write("")

        if classified_keywords == 0:
            self.stdout.write("✅ No classified keywords found. Nothing to reset.")
            return

        if dry_run:
            self.stdout.write("🔍 DRY RUN - No changes will be made")
            self.stdout.write("📝 Keywords that would be reset:")

            classified_keywords_list = Keyword.objects.filter(category__isnull=False)
            for keyword in classified_keywords_list[:10]:  # Покажем первые 10
                self.stdout.write(f"   - {keyword.name} → {keyword.category.name}")

            if classified_keywords > 10:
                self.stdout.write(f"   ... and {classified_keywords - 10} more")

            self.stdout.write(f"\n🎯 Total to reset: {classified_keywords} keywords")
            return

        # Подтверждение
        if not confirmed:
            self.stdout.write(self.style.WARNING("⚠️  This will reset ALL keyword categories!"))
            confirmation = input("❓ Are you sure? Type 'yes' to continue: ")
            if confirmation.lower() != 'yes':
                self.stdout.write("❌ Reset cancelled.")
                return

        # Сбрасываем категории
        updated_count = Keyword.objects.filter(category__isnull=False).update(category=None)

        self.stdout.write(self.style.SUCCESS(f"✅ Successfully reset {updated_count} keywords!"))
        self.stdout.write(f"📊 New statistics:")
        self.stdout.write(f"   Total keywords: {total_keywords}")
        self.stdout.write(f"   Classified: 0")
        self.stdout.write(f"   Unclassified: {total_keywords}")