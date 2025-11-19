from django.core.management.base import BaseCommand
import os
from games.models import Keyword, KeywordCategory


class Command(BaseCommand):
    help = 'Load keyword categories from text files'

    def handle(self, *args, **options):
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        lists_dir = os.path.join(base_dir, 'games', 'keyword_lists')

        # Создаем категории
        categories = {
            'Gameplay': 'Game mechanics, systems, and gameplay features',
            'Setting': 'Game world, locations, time periods, and environments',
            'Graphics': 'Visual style, art direction, and graphics',
            'Audio': 'Sound, music, voice acting, and audio design',
            'Narrative': 'Story, plot, characters, and narrative elements',
            'Theme': 'Themes, atmosphere, mood, and tone',
            'Technical': 'Technical features, platforms, and specifications',
            'Other': 'Other keywords that dont fit main categories'
        }

        category_objects = {}
        for name, description in categories.items():
            category, created = KeywordCategory.objects.get_or_create(
                name=name,
                defaults={'description': description}
            )
            category_objects[name] = category
            if created:
                self.stdout.write(f"✅ Created category: {name}")

        # Загружаем ключевые слова из файлов
        total_classified = 0

        for category_name, category_obj in category_objects.items():
            file_path = os.path.join(lists_dir, f"{category_name.lower()}.txt")

            if not os.path.exists(file_path):
                self.stdout.write(f"⚠️ File not found: {file_path}")
                continue

            with open(file_path, 'r', encoding='utf-8') as f:
                keywords = [line.strip() for line in f if line.strip()]

            classified_count = 0
            not_found_count = 0

            for keyword_name in keywords:
                try:
                    # Ищем ключевое слово в базе (регистронезависимо)
                    keyword = Keyword.objects.filter(name__iexact=keyword_name).first()
                    if keyword:
                        keyword.category = category_obj
                        keyword.save()
                        classified_count += 1
                    else:
                        self.stdout.write(f"❌ Keyword not found in DB: '{keyword_name}'")
                        not_found_count += 1
                except Exception as e:
                    self.stdout.write(f"❌ Error with '{keyword_name}': {e}")

            total_classified += classified_count
            self.stdout.write(f"✅ {category_name}: {classified_count} keywords")
            if not_found_count > 0:
                self.stdout.write(f"   ⚠️ {not_found_count} keywords not found in database")

        # Статистика
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write(f"🎯 CLASSIFICATION SUMMARY:")
        self.stdout.write(f"   ✅ Total classified: {total_classified}")
        self.stdout.write(f"   📊 Remaining unclassified: {Keyword.objects.filter(category__isnull=True).count()}")
        self.stdout.write("\n💡 To see unclassified keywords: python manage.py show_keywords --all")