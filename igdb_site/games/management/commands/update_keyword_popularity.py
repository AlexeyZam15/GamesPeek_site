from django.core.management.base import BaseCommand
from games.models import Keyword


class Command(BaseCommand):
    help = 'Update keyword popularity scores based on usage'

    def handle(self, *args, **options):
        keywords = Keyword.objects.all()
        total_keywords = keywords.count()

        self.stdout.write(f"🔄 Updating popularity for {total_keywords} keywords...")

        updated_count = 0
        for keyword in keywords:
            old_count = keyword.usage_count
            keyword.update_popularity()

            if keyword.usage_count != old_count:
                updated_count += 1
                if updated_count <= 10:  # Показываем первые 10 изменений
                    self.stdout.write(
                        f"   ✅ {keyword.name}: {old_count} → {keyword.usage_count} "
                        f"({keyword.popularity_level})"
                    )

        # Статистика
        popularity_stats = Keyword.objects.aggregate(
            low=models.Count('id', filter=models.Q(usage_count__lte=5)),
            medium=models.Count('id', filter=models.Q(usage_count__range=(6, 20))),
            high=models.Count('id', filter=models.Q(usage_count__range=(21, 100))),
            very_high=models.Count('id', filter=models.Q(usage_count__gt=100)),
        )

        self.stdout.write("\n📊 POPULARITY STATISTICS:")
        self.stdout.write(f"   🔴 Unused/Low (0-5): {popularity_stats['low']}")
        self.stdout.write(f"   🟡 Medium (6-20): {popularity_stats['medium']}")
        self.stdout.write(f"   🟢 High (21-100): {popularity_stats['high']}")
        self.stdout.write(f"   🔵 Very High (100+): {popularity_stats['very_high']}")
        self.stdout.write(f"   ✅ Updated: {updated_count} keywords")