from django.core.management.base import BaseCommand
import os
from django.conf import settings
from games.models import Keyword, KeywordCategory


class Command(BaseCommand):
    help = 'Load keyword categories from text files (only for unclassified keywords)'

    def handle(self, *args, **options):
        # Используем правильный путь через settings.BASE_DIR
        lists_dir = os.path.join(settings.BASE_DIR, 'games', 'keyword_lists')
        self.stdout.write(f"📁 Loading from: {lists_dir}")

        # Создаем категории согласно новой структуре
        categories = {
            'genre': 'Game genres and subgenres',
            'setting': 'Game world, time periods, environments and atmosphere',
            'gameplay': 'Game mechanics, systems and gameplay features',
            'narrative': 'Story, plot, narrative elements and structure',
            'characters': 'Character types, roles and characteristics',
            'technical': 'Technical features, performance and specifications',
            'graphics': 'Visual style, art direction and graphics type',
            'platform': 'Platforms, stores and distribution methods',
            'multiplayer': 'Multiplayer, online and social features',
            'achievements': 'Achievements, awards and recognition systems',
            'audio': 'Sound, music, voice acting and audio design',
            'context': 'Cultural references, influences and real-world connections',
            'development': 'Development process, support and post-release updates'
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

        # Загружаем ключевые слова из файлов (ТОЛЬКО неклассифицированные)
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
            already_classified_count = 0

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
                        # ВАЖНО: Проверяем если ключевое слово уже классифицировано
                        if keyword.category is not None:
                            if i <= 3:  # Показываем только первые 3 уже классифицированных
                                self.stdout.write(
                                    f"   🔒 [{i}] Already classified: '{keyword_name}' → {keyword.category.name}")
                            already_classified_count += 1
                            continue

                        # Классифицируем ТОЛЬКО если категория пустая
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
            self.stdout.write(f"   ✅ Newly classified: {classified_count}")
            if already_classified_count > 0:
                self.stdout.write(f"   🔒 Already classified: {already_classified_count}")
            if not_found_count > 0:
                self.stdout.write(f"   ❌ Not found: {not_found_count}")
            if duplicate_count > 0:
                self.stdout.write(f"   ⚠️ Duplicates: {duplicate_count}")

        # Финальная статистика
        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("🎯 FINAL CLASSIFICATION SUMMARY:")
        self.stdout.write(f"   📁 Total files processed: {len(category_objects)}")
        self.stdout.write(f"   ✅ New keywords classified: {total_classified}")
        self.stdout.write(f"   📊 Remaining unclassified: {Keyword.objects.filter(category__isnull=True).count()}")

        total_keywords = Keyword.objects.count()
        if total_keywords > 0:
            classified_count = total_keywords - Keyword.objects.filter(category__isnull=True).count()
            coverage = classified_count / total_keywords * 100
            self.stdout.write(f"   📈 Database coverage: {coverage:.1f}%")
        else:
            self.stdout.write(f"   📈 Database coverage: 0.0%")

        if total_classified > 0:
            self.stdout.write(self.style.SUCCESS(f"\n🎉 SUCCESS! Newly classified {total_classified} keywords!"))
        else:
            self.stdout.write(
                self.style.WARNING(f"\nℹ️ No new keywords were classified. All keywords already have categories."))