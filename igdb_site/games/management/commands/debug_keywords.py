from django.core.management.base import BaseCommand
import os
from games.models import Keyword


class Command(BaseCommand):
    help = 'Debug keyword matching between files and database'

    def handle(self, *args, **options):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        lists_dir = os.path.join(base_dir, 'games', 'keyword_lists')

        # Соберем все ключевые слова из файлов
        all_file_keywords = set()

        for filename in os.listdir(lists_dir):
            if filename.endswith('.txt'):
                file_path = os.path.join(lists_dir, filename)
                with open(file_path, 'r', encoding='utf-8') as f:
                    keywords = {line.strip().lower() for line in f if line.strip()}
                    all_file_keywords.update(keywords)

        # Соберем все ключевые слова из базы
        db_keywords = {kw.name.lower() for kw in Keyword.objects.all()}

        # Найдем различия
        missing_in_db = all_file_keywords - db_keywords
        missing_in_files = db_keywords - all_file_keywords

        self.stdout.write(f"📊 STATISTICS:")
        self.stdout.write(f"   In files: {len(all_file_keywords)}")
        self.stdout.write(f"   In DB: {len(db_keywords)}")
        self.stdout.write(f"   Missing in DB: {len(missing_in_db)}")
        self.stdout.write(f"   Missing in files: {len(missing_in_files)}")

        self.stdout.write(f"\n🔍 FIRST 10 MISSING IN DB:")
        for kw in list(missing_in_db)[:10]:
            self.stdout.write(f"   '{kw}'")

        self.stdout.write(f"\n🔍 FIRST 10 MISSING IN FILES:")
        for kw in list(missing_in_files)[:10]:
            self.stdout.write(f"   '{kw}'")