from django.core.management.base import BaseCommand
from games.keyword_classifier import KeywordClassifier
import time


class Command(BaseCommand):
    help = 'Automatically classify keywords into Gameplay and Setting categories'

    def add_arguments(self, parser):
        parser.add_argument(
            '--min-confidence',
            type=float,
            default=0.6,
            help='Minimum confidence threshold for classification (0.0-1.0)'
        )
        parser.add_argument(
            '--show-examples',
            action='store_true',
            help='Show examples of classified keywords'
        )

    def handle(self, *args, **options):
        min_confidence = options['min_confidence']
        show_examples = options['show_examples']

        self.stdout.write("🧠 Starting automatic keyword classification...")
        self.stdout.write(f"📊 Minimum confidence: {min_confidence}")

        classifier = KeywordClassifier()

        # Получаем все ключевые слова
        from games.models import Keyword
        keywords = Keyword.objects.all()
        total_keywords = keywords.count()

        self.stdout.write(f"📝 Processing {total_keywords} keywords...")

        if total_keywords == 0:
            self.stdout.write(self.style.WARNING("❌ No keywords found in database!"))
            self.stdout.write("   Run 'python manage.py fetch_games' first to load keywords")
            return

        start_time = time.time()
        examples = []

        # Получаем или создаем категории
        from games.models import KeywordCategory
        gameplay_category, _ = KeywordCategory.objects.get_or_create(
            name='Gameplay',
            defaults={'description': 'Keywords related to game mechanics and gameplay features'}
        )
        setting_category, _ = KeywordCategory.objects.get_or_create(
            name='Setting',
            defaults={'description': 'Keywords related to game world, environment and location'}
        )
        misc_category, _ = KeywordCategory.objects.get_or_create(
            name='Miscellaneous',
            defaults={'description': 'Unclassified or ambiguous keywords'}
        )

        stats = {
            'total': total_keywords,
            'classified': 0,
            'gameplay': 0,
            'setting': 0,
            'miscellaneous': 0,
            'low_confidence': 0
        }

        # Обрабатываем ключевые слова с прогрессом
        for i, keyword in enumerate(keywords, 1):
            category_name, confidence = classifier.classify_keyword(keyword.name)

            # Показываем прогресс каждые 10 ключевых слов или для первых 5
            if i % 10 == 0 or i <= 5:
                self.stdout.write(f"   🔍 [{i}/{total_keywords}] {keyword.name} → {category_name} ({confidence:.2f})")

            if confidence >= min_confidence:
                if category_name == 'Gameplay':
                    keyword.category = gameplay_category
                    stats['gameplay'] += 1
                    category_emoji = "🎮"
                elif category_name == 'Setting':
                    keyword.category = setting_category
                    stats['setting'] += 1
                    category_emoji = "🌍"
                else:
                    keyword.category = misc_category
                    stats['miscellaneous'] += 1
                    category_emoji = "📦"

                keyword.save()
                stats['classified'] += 1

                # Сохраняем примеры для показа
                if show_examples and len(examples) < 10:
                    examples.append(f"      {category_emoji} {keyword.name} → {category_name} ({confidence:.2f})")
            else:
                stats['low_confidence'] += 1
                if i <= 5:  # Показываем первые 5 low-confidence
                    self.stdout.write(
                        f"   ⚠️ [{i}/{total_keywords}] {keyword.name} → LOW CONFIDENCE ({confidence:.2f})")

        end_time = time.time()
        processing_time = end_time - start_time

        # Выводим результаты
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("📈 CLASSIFICATION RESULTS:")
        self.stdout.write("=" * 50)
        self.stdout.write(f"   Total keywords processed: {stats['total']}")
        self.stdout.write(f"   Processing time: {processing_time:.2f} seconds")
        self.stdout.write(f"   Avg time per keyword: {processing_time / stats['total']:.3f}s")
        self.stdout.write("")
        self.stdout.write(f"   🎮 Gameplay keywords: {stats['gameplay']}")
        self.stdout.write(f"   🌍 Setting keywords: {stats['setting']}")
        self.stdout.write(f"   📦 Miscellaneous: {stats['miscellaneous']}")
        self.stdout.write(f"   ⚠️ Low confidence (not classified): {stats['low_confidence']}")
        self.stdout.write(f"   ✅ Successfully classified: {stats['classified']}")

        success_rate = (stats['classified'] / stats['total']) * 100
        self.stdout.write(f"   📊 Success rate: {success_rate:.1f}%")

        if show_examples and examples:
            self.stdout.write("\n" + "🔍 CLASSIFICATION EXAMPLES:")
            for example in examples:
                self.stdout.write(example)

        if stats['classified'] > 0:
            self.stdout.write(
                self.style.SUCCESS(f"\n✅ Successfully classified {success_rate:.1f}% of keywords!")
            )
            self.stdout.write("   Check results in admin: http://127.0.0.1:8000/admin/games/keyword/")
        else:
            self.stdout.write(
                self.style.WARNING(f"\n⚠️ No keywords were classified. Try lowering --min-confidence")
            )