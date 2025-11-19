from django.core.management.base import BaseCommand
from games.models import Keyword


class Command(BaseCommand):
    help = 'Export keywords from database to file'

    def add_arguments(self, parser):
        parser.add_argument(
            '--unclassified-only',
            action='store_true',
            help='Export only unclassified keywords'
        )
        parser.add_argument(
            '--format',
            type=str,
            choices=['lines', 'comma'],
            default='lines',
            help='Output format: lines (one per line) or comma (comma-separated)'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='keywords.txt',
            help='Output file name'
        )

    def handle(self, *args, **options):
        unclassified_only = options['unclassified_only']
        output_file = options['output']
        format_type = options['format']

        # Выбираем ключевые слова
        if unclassified_only:
            keywords = Keyword.objects.filter(category__isnull=True).order_by('name')
            self.stdout.write(f"🔍 Exporting only UNCLASSIFIED keywords...")
        else:
            keywords = Keyword.objects.all().order_by('name')
            self.stdout.write(f"📝 Exporting ALL keywords...")

        # Экспортируем в файл
        with open(output_file, 'w', encoding='utf-8') as f:
            if format_type == 'lines':
                for kw in keywords:
                    f.write(f"{kw.name}\n")
            elif format_type == 'comma':
                keyword_names = [kw.name for kw in keywords]
                f.write(", ".join(keyword_names))

        self.stdout.write(f"✅ Exported {keywords.count()} keywords to {output_file} ({format_type} format)")

        if unclassified_only:
            total_keywords = Keyword.objects.count()
            classified_count = total_keywords - keywords.count()
            self.stdout.write(f"📊 Statistics:")
            self.stdout.write(f"   Total keywords: {total_keywords}")
            self.stdout.write(f"   Classified: {classified_count}")
            self.stdout.write(f"   Unclassified: {keywords.count()}")
            self.stdout.write(f"   Coverage: {classified_count / total_keywords * 100:.1f}%")