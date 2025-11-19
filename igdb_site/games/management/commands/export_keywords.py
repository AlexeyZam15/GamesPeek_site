from django.core.management.base import BaseCommand
from games.models import Keyword


class Command(BaseCommand):
    help = 'Export all keywords from database to file'

    def handle(self, *args, **options):
        keywords = Keyword.objects.all().order_by('name')

        with open('all_keywords.txt', 'w', encoding='utf-8') as f:
            for kw in keywords:
                f.write(f"{kw.name}\n")

        self.stdout.write(f"✅ Exported {keywords.count()} keywords to all_keywords.txt")