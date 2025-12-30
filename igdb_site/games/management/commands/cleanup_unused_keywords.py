"""
Management command to delete unused keywords (keywords not linked to any game).
"""
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.utils import timezone
from games.models import Keyword
import time


class Command(BaseCommand):
    help = 'Delete unused keywords (keywords not linked to any game)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Limit number of keywords to delete (0 for all)'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed output'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force deletion without confirmation'
        )
        parser.add_argument(
            '--category',
            type=str,
            help='Delete only from specific category'
        )
        parser.add_argument(
            '--min-id',
            type=int,
            help='Minimum keyword ID to process'
        )
        parser.add_argument(
            '--max-id',
            type=int,
            help='Maximum keyword ID to process'
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        limit = options['limit']
        verbose = options['verbose']
        force = options['force']
        category_name = options['category']
        min_id = options['min_id']
        max_id = options['max_id']

        self.stdout.write(self.style.HTTP_INFO("🔍 Starting cleanup of unused keywords..."))

        # Build base queryset
        keywords_qs = Keyword.objects.all()

        # Apply filters
        if category_name:
            keywords_qs = keywords_qs.filter(category__name=category_name)
            self.stdout.write(f"📁 Filtering by category: {category_name}")

        if min_id:
            keywords_qs = keywords_qs.filter(id__gte=min_id)
            self.stdout.write(f"📊 Minimum ID: {min_id}")

        if max_id:
            keywords_qs = keywords_qs.filter(id__lte=max_id)
            self.stdout.write(f"📊 Maximum ID: {max_id}")

        # Get unused keywords
        unused_keywords = keywords_qs.filter(game__isnull=True)

        total_count = unused_keywords.count()

        if limit and limit > 0:
            unused_keywords = unused_keywords[:limit]
            self.stdout.write(f"📊 Limiting to {limit} keywords")

        if not unused_keywords.exists():
            self.stdout.write(self.style.SUCCESS("✅ No unused keywords found!"))
            return

        # Display statistics
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(f"📊 STATISTICS:")
        self.stdout.write(f"   Total keywords in database: {Keyword.objects.count()}")
        self.stdout.write(f"   Unused keywords found: {total_count}")

        if limit and 0 < limit < total_count:
            self.stdout.write(f"   Will process: {min(limit, total_count)}")

        # Show unused keywords by category if verbose
        if verbose:
            self.stdout.write("\n📁 Unused keywords by category:")
            category_stats = unused_keywords.values(
                'category__name'
            ).annotate(
                count=models.Count('id')
            ).order_by('-count')

            for stat in category_stats:
                cat_name = stat['category__name'] or "No Category"
                self.stdout.write(f"   {cat_name}: {stat['count']} keywords")

        # Show sample of unused keywords
        if verbose:
            self.stdout.write("\n📝 Sample of unused keywords (first 10):")
            sample = unused_keywords.select_related('category').only('id', 'name', 'category__name')[:10]
            for kw in sample:
                cat_name = kw.category.name if kw.category else "No Category"
                self.stdout.write(f"   {kw.id}: {kw.name} ({cat_name})")

        if dry_run:
            self.stdout.write(self.style.WARNING("\n⚠️ DRY RUN MODE - No keywords will be deleted"))
            self.stdout.write(f"   Would delete {len(unused_keywords)} unused keywords")
            return

        # Ask for confirmation
        if not force:
            confirm = input(
                f"\n⚠️ Are you sure you want to delete {len(unused_keywords)} unused keywords? "
                f"(yes/no): "
            )
            if confirm.lower() != 'yes':
                self.stdout.write(self.style.WARNING("❌ Operation cancelled"))
                return

        # Delete unused keywords
        self.stdout.write("\n🗑️ Starting deletion...")
        start_time = time.time()

        try:
            with transaction.atomic():
                # Get the IDs before deletion for reporting
                deleted_ids = list(unused_keywords.values_list('id', flat=True))

                # Perform deletion
                deleted_count, _ = unused_keywords.delete()

                end_time = time.time()
                elapsed_time = end_time - start_time

                # Display results
                self.stdout.write(self.style.SUCCESS(f"\n✅ SUCCESS!"))
                self.stdout.write(f"   Deleted {deleted_count} unused keywords")
                self.stdout.write(f"   Time taken: {elapsed_time:.2f} seconds")

                if verbose and deleted_ids:
                    self.stdout.write(f"\n📋 Deleted keyword IDs (first 20):")
                    self.stdout.write(f"   {', '.join(map(str, deleted_ids[:20]))}")
                    if len(deleted_ids) > 20:
                        self.stdout.write(f"   ... and {len(deleted_ids) - 20} more")

                # Show updated statistics
                self.stdout.write(f"\n📊 UPDATED STATISTICS:")
                self.stdout.write(f"   Remaining keywords: {Keyword.objects.count()}")

        except Exception as e:
            self.stdout.write(self.style.ERROR(f"\n❌ ERROR: {str(e)}"))
            self.stdout.write("Transaction rolled back.")
            raise CommandError(f"Deletion failed: {str(e)}")

    def show_usage_statistics(self):
        """Show detailed keyword usage statistics."""
        from django.db.models import Count, Q

        self.stdout.write("\n📈 KEYWORD USAGE STATISTICS:")

        # Keywords with games
        used_keywords = Keyword.objects.filter(game__isnull=False)
        used_count = used_keywords.count()

        # Keywords without games
        unused_count = Keyword.objects.filter(game__isnull=True).count()

        total_count = Keyword.objects.count()

        self.stdout.write(f"   Total keywords: {total_count}")
        self.stdout.write(f"   Used keywords: {used_count}")
        self.stdout.write(f"   Unused keywords: {unused_count}")

        if total_count > 0:
            unused_percentage = (unused_count / total_count) * 100
            self.stdout.write(f"   Unused percentage: {unused_percentage:.1f}%")

        # Most used keywords
        top_keywords = Keyword.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('-game_count')[:5]

        if top_keywords:
            self.stdout.write("\n🏆 Most used keywords:")
            for kw in top_keywords:
                self.stdout.write(f"   {kw.name}: {kw.game_count} games")