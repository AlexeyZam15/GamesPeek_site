from django.core.management.base import BaseCommand
from games.models import Keyword


class Command(BaseCommand):
    help = 'Show unclassified keywords for manual classification'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Number of keywords to show'
        )
        parser.add_argument(
            '--all',
            action='store_true',
            help='Show ALL keywords (ignore limit)'
        )
        parser.add_argument(
            '--format',
            type=str,
            choices=['list', 'json', 'csv', 'comma'],
            default='comma',
            help='Output format'
        )

    def handle(self, *args, **options):
        keywords = Keyword.objects.filter(category__isnull=True)

        # Применяем лимит или показываем все
        if options['all']:
            self.stdout.write("📋 Showing ALL unclassified keywords")
        else:
            limit = options['limit']
            keywords = keywords[:limit]
            self.stdout.write(f"📝 Showing {limit} keywords (use --all for all)")

        format_type = options['format']
        total_unclassified = Keyword.objects.filter(category__isnull=True).count()
        showing_count = keywords.count()

        self.stdout.write(f"🔍 Found {total_unclassified} unclassified keywords")
        self.stdout.write(f"📊 Showing {showing_count} keywords:")
        self.stdout.write("=" * 50)

        if format_type == 'list':
            for i, keyword in enumerate(keywords, 1):
                self.stdout.write(f"{i}. {keyword.name}")

        elif format_type == 'json':
            import json
            keyword_list = [{'id': kw.id, 'name': kw.name} for kw in keywords]
            self.stdout.write(json.dumps(keyword_list, indent=2))

        elif format_type == 'csv':
            for keyword in keywords:
                self.stdout.write(f"{keyword.id},{keyword.name}")

        elif format_type == 'comma':
            keyword_names = [keyword.name for keyword in keywords]
            self.stdout.write(", ".join(keyword_names))

        # Показываем статистику если показываем не все
        if not options['all'] and total_unclassified > showing_count:
            self.stdout.write(f"\nℹ️  ... and {total_unclassified - showing_count} more unclassified keywords")
            self.stdout.write("💡 Use '--all' to see all keywords")