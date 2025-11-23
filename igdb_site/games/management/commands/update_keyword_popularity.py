# management/commands/update_keyword_popularity.py
from django.core.management.base import BaseCommand
from django.db import models
from games.models import Keyword


class Command(BaseCommand):
    help = 'Update keyword popularity scores based on usage'

    def add_arguments(self, parser):
        parser.add_argument(
            '--show-all',
            action='store_true',
            help='Показать все изменения (не только первые 10)'
        )

    def handle(self, *args, **options):
        show_all = options['show_all']
        keywords = Keyword.objects.all()
        total_keywords = keywords.count()

        self.stdout.write(f"🔄 Updating popularity for {total_keywords} keywords...")
        self.stdout.write("")

        updated_count = 0
        changes_shown = 0

        for i, keyword in enumerate(keywords, 1):
            # Показываем прогресс
            if i % 50 == 0 or i == total_keywords:
                progress = (i / total_keywords) * 100
                self.stdout.write(f"   📊 Progress: {i}/{total_keywords} ({progress:.1f}%)", ending='\r')

            old_count = keyword.usage_count
            keyword.update_popularity()

            if keyword.usage_count != old_count:
                updated_count += 1

                # Показываем изменения
                if show_all or changes_shown < 10:
                    changes_shown += 1
                    self.stdout.write(
                        f"   ✅ {keyword.name}: {old_count} → {keyword.usage_count} "
                        f"({keyword.popularity_level})"
                    )

        # Очищаем строку прогресса
        self.stdout.write(" " * 50, ending='\r')

        # Статистика
        popularity_stats = Keyword.objects.aggregate(
            low=models.Count('id', filter=models.Q(usage_count__lte=5)),
            medium=models.Count('id', filter=models.Q(usage_count__range=(6, 20))),
            high=models.Count('id', filter=models.Q(usage_count__range=(21, 100))),
            very_high=models.Count('id', filter=models.Q(usage_count__gt=100)),
        )

        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("📊 POPULARITY STATISTICS:")
        self.stdout.write("=" * 50)
        self.stdout.write(f"   🔴 Unused/Low (0-5): {popularity_stats['low']}")
        self.stdout.write(f"   🟡 Medium (6-20): {popularity_stats['medium']}")
        self.stdout.write(f"   🟢 High (21-100): {popularity_stats['high']}")
        self.stdout.write(f"   🔵 Very High (100+): {popularity_stats['very_high']}")
        self.stdout.write(f"   ✅ Updated: {updated_count} keywords")