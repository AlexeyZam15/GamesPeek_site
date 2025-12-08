# games/management/commands/check_categories.py
import sys
from django.core.management.base import BaseCommand
from games.models import Genre, Theme, PlayerPerspective, GameMode


class Command(BaseCommand):
    help = 'Проверяет наличие категорий в базе данных'

    def add_arguments(self, parser):
        parser.add_argument('--category', type=str, choices=['all', 'genres', 'themes', 'perspectives', 'modes'],
                            default='all', help='Категория для проверки')
        parser.add_argument('--name', type=str, help='Поиск конкретного названия')

    def handle(self, *args, **options):
        category = options['category']
        specific_name = options.get('name')

        # Определите ALL_CATEGORIES как атрибут класса или локальную переменную
        ALL_CATEGORIES = self.get_all_categories()

        if specific_name:
            self.search_specific_name(specific_name, ALL_CATEGORIES)
        elif category == 'all':
            self.check_all_categories(ALL_CATEGORIES)
        else:
            self.check_single_category(category, ALL_CATEGORIES[category])

    def get_all_categories(self):
        """Возвращает все категории"""
        return {
            'genres': {
                'model': Genre,
                'names': [
                    "Action", "Adventure", "Arcade", "Card & Board Game",
                    "Fighting", "Hack and slash/Beat 'em up", "Indie",
                    "MOBA", "Music", "Pinball", "Platform", "Point-and-click",
                    "Puzzle", "Quiz/Trivia", "Racing", "Real Time Strategy (RTS)",
                    "Role-playing (RPG)", "Shooter", "Simulator", "Sport",
                    "Strategy", "Tactical", "Turn-based strategy (TBS)",
                    "Visual Novel"
                ]
            },
            'themes': {
                'model': Theme,
                'names': [
                    "4X (explore, expand, exploit, and exterminate)", "Action",
                    "Business", "Comedy", "Drama", "Educational", "Erotic",
                    "Fantasy", "Historical", "Horror", "Kids", "Mystery",
                    "Non-fiction", "Open world", "Party", "Romance", "Sandbox",
                    "Science fiction", "Stealth", "Survival", "Thriller", "Warfare"
                ]
            },
            'perspectives': {
                'model': PlayerPerspective,
                'names': [
                    "Auditory", "Bird view / Isometric", "First person",
                    "Side view", "Text", "Third person", "Virtual Reality"
                ]
            },
            'modes': {
                'model': GameMode,
                'names': [
                    "Battle Royale", "Co-operative",
                    "Massively Multiplayer Online (MMO)", "Multiplayer",
                    "Single player", "Split screen"
                ]
            }
        }

    def search_specific_name(self, name, all_categories):
        """Ищет конкретное название во всех категориях"""
        self.stdout.write(f"🔍 Поиск '{name}' во всех категориях...")
        self.stdout.write("-" * 50)

        found = False
        for category_name, data in all_categories.items():
            model = data['model']
            try:
                obj = model.objects.get(name__iexact=name)
                self.stdout.write(f"✅ Найдено в {category_name}: {obj.name}")
                found = True
            except model.DoesNotExist:
                pass
            except model.MultipleObjectsReturned:
                objects = model.objects.filter(name__icontains=name)
                self.stdout.write(f"⚠️ Найдено несколько в {category_name}:")
                for obj in objects:
                    self.stdout.write(f"   • {obj.name}")
                found = True

        if not found:
            self.stdout.write(f"❌ '{name}' не найдено ни в одной категории")

    def check_all_categories(self, all_categories):
        """Проверяет все категории"""
        total_missing = 0

        for category_name, data in all_categories.items():
            model = data['model']
            expected_names = data['names']

            self.stdout.write(f"\n📋 {category_name.upper()} (ожидается: {len(expected_names)})")
            self.stdout.write("-" * 40)

            # Получаем все существующие записи
            existing_objects = model.objects.all()
            existing_names = {obj.name for obj in existing_objects}

            # Ищем отсутствующие
            missing = []
            for name in expected_names:
                if name not in existing_names:
                    # Проверяем без учета регистра
                    if not any(name.lower() == existing.lower() for existing in existing_names):
                        missing.append(name)

            # Выводим результаты
            self.stdout.write(f"✅ В базе: {len(existing_objects)}")

            if missing:
                self.stdout.write(f"❌ Отсутствуют ({len(missing)}):")
                for name in missing:
                    self.stdout.write(f"   • {name}")
                total_missing += len(missing)
            else:
                self.stdout.write("🎉 Все категории присутствуют!")

        self.stdout.write("\n" + "=" * 50)
        self.stdout.write(f"📊 ИТОГО: отсутствует {total_missing} категорий")

    def check_single_category(self, category_name, category_data):
        """Проверяет одну категорию"""
        model = category_data['model']
        expected_names = category_data['names']

        self.stdout.write(f"\n📋 Проверка {category_name.upper()}")
        self.stdout.write("-" * 40)

        # Получаем все записи
        existing_objects = list(model.objects.all().order_by('name'))
        existing_names = {obj.name for obj in existing_objects}

        # Выводим что есть
        self.stdout.write(f"📁 В базе ({len(existing_objects)}):")
        for obj in existing_objects:
            self.stdout.write(f"   • {obj.name}")

        # Ищем отсутствующие
        missing = []
        for name in expected_names:
            if name not in existing_names:
                # Проверяем без учета регистра
                if not any(name.lower() == existing.lower() for existing in existing_names):
                    missing.append(name)

        if missing:
            self.stdout.write(f"\n❌ Отсутствуют ({len(missing)} из {len(expected_names)}):")
            for name in missing:
                self.stdout.write(f"   • {name}")

            self.stdout.write(f"\n📈 Покрытие: {len(existing_objects)}/{len(expected_names)} "
                              f"({len(existing_objects) / len(expected_names) * 100:.1f}%)")
        else:
            self.stdout.write(f"\n🎉 Все {len(expected_names)} категорий присутствуют!")