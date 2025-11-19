from django.core.management.base import BaseCommand
from games.models import Keyword, KeywordCategory


class Command(BaseCommand):
    help = 'Interactive keyword classification - assign categories manually'

    def add_arguments(self, parser):
        parser.add_argument(
            '--unclassified-only',
            action='store_true',
            help='Only show unclassified keywords'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=50,
            help='Number of keywords to process'
        )

    def handle(self, *args, **options):
        self.stdout.write("🎮 GamesPeek Interactive Keyword Classification")
        self.stdout.write("=" * 50)

        # Категории для выбора
        CATEGORY_CHOICES = {
            '1': 'Gameplay',
            '2': 'Setting',
            '3': 'Graphics',
            '4': 'Other',
            '0': 'Skip',
            's': 'Show current category',
            'q': 'Quit'
        }

        # Получаем ключевые слова
        keywords = Keyword.objects.all()

        if options['unclassified_only']:
            keywords = keywords.filter(category__isnull=True)
            self.stdout.write("🔍 Showing only UNCLASSIFIED keywords")

        keywords = keywords[:options['limit']]
        total_keywords = keywords.count()

        if total_keywords == 0:
            self.stdout.write("❌ No keywords found!")
            return

        self.stdout.write(f"📝 Found {total_keywords} keywords to classify")
        self.stdout.write("\n🎯 Category Guide:")
        for key, category in CATEGORY_CHOICES.items():
            self.stdout.write(f"   {key} - {category}")

        self.stdout.write("\n" + "=" * 50)

        classified_count = 0
        skipped_count = 0

        for i, keyword in enumerate(keywords, 1):
            self.stdout.write(f"\n--- Keyword {i}/{total_keywords} ---")
            self.stdout.write(f"🔑 Keyword: {keyword.name}")

            if keyword.category:
                self.stdout.write(f"📌 Current category: {keyword.category.name}")

            while True:
                self.stdout.write("\n🎯 Categories: 1=Gameplay, 2=Setting, 3=Graphics, 4=Other")
                self.stdout.write("   Commands: 0=Skip, s=Show, q=Quit")

                user_input = input("🎯 Choose: ").strip().lower()

                if user_input == 'q':
                    self.stdout.write("👋 Classification stopped by user")
                    self.show_stats(classified_count, skipped_count, total_keywords)
                    return

                elif user_input == 's':
                    self.stdout.write("🎯 Categories: 1=Gameplay, 2=Setting, 3=Graphics, 4=Other")
                    self.stdout.write("   Commands: 0=Skip, s=Show, q=Quit")
                    continue

                elif user_input == '0':
                    self.stdout.write("⏭️ Skipped")
                    skipped_count += 1
                    break

                elif user_input in ['1', '2', '3', '4']:
                    category_name = CATEGORY_CHOICES[user_input]
                    category, created = KeywordCategory.objects.get_or_create(
                        name=category_name,
                        defaults={'description': f'{category_name} related keywords'}
                    )

                    keyword.category = category
                    keyword.save()

                    self.stdout.write(f"✅ Classified as: {category_name}")
                    classified_count += 1
                    break

                else:
                    self.stdout.write("❌ Invalid input! Please choose 1-4, 0, s, or q")

        self.show_stats(classified_count, skipped_count, total_keywords)

    def show_stats(self, classified, skipped, total):
        """Показывает статистику классификации"""
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("📊 CLASSIFICATION SUMMARY:")
        self.stdout.write(f"   ✅ Classified: {classified}")
        self.stdout.write(f"   ⏭️ Skipped: {skipped}")
        self.stdout.write(f"   📝 Processed: {total}")
        self.stdout.write(f"   📈 Progress: {classified}/{total} ({classified / total * 100:.1f}%)")