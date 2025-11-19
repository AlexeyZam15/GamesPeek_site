from django.core.management.base import BaseCommand
import os
from django.conf import settings
from games.models import Keyword, KeywordCategory


class Command(BaseCommand):
    help = 'Load keyword categories from text files'

    def handle(self, *args, **options):
        # Используем правильный путь через settings.BASE_DIR
        lists_dir = os.path.join(settings.BASE_DIR, 'games', 'keyword_lists')
        self.stdout.write(f"📁 Loading from: {lists_dir}")

        # Создаем категории
        categories = {
            'gameplay': 'Game mechanics, systems, and gameplay features',
            'setting': 'Game world, locations, time periods, and environments',
            'characters': 'Character types and characteristics',
            'genre': 'Game genres and subgenres',
            'multiplayer': 'Multiplayer aspects and features',
            'graphics': 'Visual style, art direction, and graphics',
            'audio': 'Sound, music, voice acting, and audio design',
            'narrative': 'Story, plot, characters, and narrative elements',
            'theme': 'Themes, atmosphere, mood, and tone',
            'technical': 'Technical features, platforms, and specifications',
            'awards': 'Awards, nominations, and achievements',
            'platform': 'Platforms, stores, and distribution',
            'other': 'Other keywords that dont fit main categories'
        }

        category_objects = {}
        for name, description in categories.items():
            # Используем правильное имя категории (с заглавной буквы для отображения)
            display_name = name.capitalize()
            category, created = KeywordCategory.objects.get_or_create(
                name=display_name,
                defaults={'description': description}
            )
            category_objects[name] = category
            if created:
                self.stdout.write(f"✅ Created category: {display_name}")
            else:
                self.stdout.write(f"📁 Using existing category: {display_name}")

        # Загружаем ключевые слова из файлов
        total_classified = 0
        used_keywords = set()

        for file_name, category_obj in category_objects.items():
            file_path = os.path.join(lists_dir, f"{file_name}.txt")

            if not os.path.exists(file_path):
                self.stdout.write(f"❌ File not found: {file_path}")
                continue

            self.stdout.write(f"\n📖 Processing {file_name}.txt...")

            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Парсим ключевые слова
            keywords = [kw.strip() for kw in content.split(',') if kw.strip()]
            if keywords and keywords[0].startswith('.'):
                keywords[0] = keywords[0][1:].strip()

            classified_count = 0
            not_found_count = 0
            duplicate_count = 0

            # Показываем прогресс каждые 10 ключевых слов
            for i, keyword_name in enumerate(keywords, 1):
                if not keyword_name:
                    continue

                if keyword_name in used_keywords:
                    if i <= 5:  # Показываем только первые 5 дубликатов
                        self.stdout.write(f"   ⚠️ [{i}] Duplicate: '{keyword_name}'")
                    duplicate_count += 1
                    continue

                try:
                    keyword = Keyword.objects.filter(name__iexact=keyword_name).first()
                    if keyword:
                        keyword.category = category_obj
                        keyword.save()
                        classified_count += 1
                        used_keywords.add(keyword_name)
                        total_classified += 1

                        # Показываем прогресс для первых 10 и каждых 20 ключевых слов
                        if i <= 10 or i % 20 == 0:
                            self.stdout.write(f"   ✅ [{i}] Classified: '{keyword_name}' → {file_name.capitalize()}")
                    else:
                        if i <= 5:  # Показываем только первые 5 ненайденных
                            self.stdout.write(f"   ❌ [{i}] Not found: '{keyword_name}'")
                        not_found_count += 1
                except Exception as e:
                    self.stdout.write(f"   💥 [{i}] Error with '{keyword_name}': {e}")

            # Итоги по файлу
            self.stdout.write(f"📊 {file_name.capitalize()} results:")
            self.stdout.write(f"   ✅ Classified: {classified_count}")
            if not_found_count > 0:
                self.stdout.write(f"   ❌ Not found: {not_found_count}")
            if duplicate_count > 0:
                self.stdout.write(f"   ⚠️ Duplicates: {duplicate_count}")

        # Финальная статистика
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("🎯 FINAL CLASSIFICATION SUMMARY:")
        self.stdout.write(f"   📁 Total files processed: {len(category_objects)}")
        self.stdout.write(f"   ✅ Total keywords classified: {total_classified}")
        self.stdout.write(f"   📊 Remaining unclassified: {Keyword.objects.filter(category__isnull=True).count()}")
        self.stdout.write(f"   📈 Database coverage: {total_classified / Keyword.objects.count() * 100:.1f}%")

        if total_classified > 0:
            self.stdout.write(self.style.SUCCESS(
                f"\n🎉 SUCCESS! Classified {total_classified} keywords across {len(category_objects)} categories!"))
        else:
            self.stdout.write(
                self.style.ERROR(f"\n❌ FAILED! No keywords were classified. Check file paths and keyword names."))