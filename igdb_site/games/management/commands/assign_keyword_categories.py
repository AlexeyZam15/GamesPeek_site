# management/commands/assign_keyword_categories.py
from django.core.management.base import BaseCommand
from games.models import Keyword, KeywordCategory


class Command(BaseCommand):
    help = 'Назначить категории ключевым словам'

    def add_arguments(self, parser):
        parser.add_argument(
            '--auto-only',
            action='store_true',
            help='Назначить категории только ключевым словам без категории'
        )
        parser.add_argument(
            '--show-examples',
            action='store_true',
            help='Показать примеры назначения категорий'
        )

    def handle(self, *args, **options):
        auto_only = options['auto_only']
        show_examples = options['show_examples']

        self.stdout.write('🏷️ Назначение категорий ключевым словам')
        self.stdout.write('=' * 60)

        # Создаем категории если их нет
        categories_data = {
            'Gameplay': 'Геймплейные механики и особенности игрового процесса',
            'Setting': 'Сеттинг, атмосфера и место действия',
            'Genre': 'Жанровые особенности и классификации',
            'Narrative': 'Сюжет, нарратив и сторителлинг',
            'Characters': 'Персонажи, их характеристики и развитие',
            'Technical': 'Технические особенности и реализация',
            'Graphics': 'Графика, визуальный стиль и рендеринг',
            'Platform': 'Платформенные особенности и совместимость',
            'Multiplayer': 'Мультиплеерные возможности и режимы',
            'Achievements': 'Достижения, награды и система прогрессии',
            'Audio': 'Аудио, звуковые эффекты и музыка',
            'Context': 'Контекст, темы и культурные аспекты',
            'Development': 'Особенности разработки и издания'
        }

        # Создаем категории
        created_categories = 0
        for name, description in categories_data.items():
            category, created = KeywordCategory.objects.get_or_create(
                name=name,
                defaults={'description': description}
            )
            if created:
                created_categories += 1
                self.stdout.write(f'✅ Создана категория: {name}')

        if created_categories > 0:
            self.stdout.write(f'📁 Создано категорий: {created_categories}')

        # Определяем ключевые слова для обработки
        if auto_only:
            keywords_to_process = Keyword.objects.filter(category__isnull=True)
            self.stdout.write(f'🔑 Ключевых слов без категории: {keywords_to_process.count()}')
        else:
            keywords_to_process = Keyword.objects.all()
            self.stdout.write(f'🔑 Всего ключевых слов: {keywords_to_process.count()}')

        # Словарь для категоризации
        category_rules = {
            'Gameplay': [
                # Тактические и боевые системы
                'tactical', 'turn-based', 'combat', 'battle', 'strategy', 'tactics',
                'fighting', 'melee', 'ranged', 'shooter', 'action', 'stealth',
                'puzzle', 'exploration', 'platforming', 'racing', 'sports',

                # RPG системы
                'rpg', 'role-playing', 'leveling', 'experience', 'skill', 'ability',
                'class', 'stat', 'attribute', 'perk', 'talent', 'progression',

                # Экономика и крафтинг
                'crafting', 'building', 'economy', 'trading', 'shopping', 'market',
                'resource', 'inventory', 'loot', 'treasure', 'currency',

                # Игровые механики
                'quick-time', 'minigame', 'side-quest', 'mission', 'objective',
                'save', 'checkpoint', 'difficulty', 'challenge', 'achievement',
                'multiplayer', 'co-op', 'pvp', 'online', 'single-player'
            ],
            'Setting': [
                'medieval', 'fantasy', 'sci-fi', 'science-fiction', 'cyberpunk',
                'post-apocalyptic', 'apocalypse', 'historical', 'modern', 'contemporary',
                'future', 'space', 'alien', 'underwater', 'urban', 'city', 'rural',
                'western', 'noir', 'horror', 'comedy', 'drama', 'romance', 'war',
                'political', 'military', 'school', 'academy', 'hospital', 'prison',
                'zombie', 'supernatural', 'mythology', 'steampunk', 'dieselpunk'
            ],
            'Narrative': [
                'story', 'plot', 'narrative', 'dialogue', 'choice', 'consequence',
                'ending', 'branching', 'multiple-endings', 'linear', 'open-world',
                'sandbox', 'quest', 'mission', 'cutscene', 'cinematic', 'narration',
                'voice-acting', 'subtitle', 'translation', 'lore', 'world-building'
            ],
            'Characters': [
                'character', 'protagonist', 'hero', 'villain', 'antagonist', 'npc',
                'companion', 'party', 'crew', 'customization', 'creation', 'appearance',
                'personality', 'relationship', 'romance', 'friendship', 'rivalry',
                'dialogue', 'voice', 'gender', 'race', 'species', 'faction'
            ],
            'Graphics': [
                'graphics', 'visual', 'art', 'style', 'pixel', 'voxel', '3d', '2d',
                'resolution', 'texture', 'lighting', 'shadow', 'particle', 'effect',
                'animation', 'model', 'sprite', 'ui', 'interface', 'hud', 'menu'
            ],
            'Audio': [
                'sound', 'music', 'audio', 'voice', 'ost', 'soundtrack', 'score',
                'effect', 'ambient', 'dialogue', 'narration', 'volume', 'mixing'
            ],
            'Technical': [
                'engine', 'physics', 'ai', 'pathfinding', 'network', 'server',
                'client', 'mod', 'modding', 'script', 'programming', 'render',
                'performance', 'optimization', 'bug', 'patch', 'update', 'dlc'
            ]
        }

        # Назначаем категории
        assigned_count = 0
        reassigned_count = 0
        examples = []

        for keyword in keywords_to_process:
            new_category = self.determine_category(keyword.name, category_rules)
            old_category = keyword.category

            if new_category:
                if old_category != new_category:
                    keyword.category = new_category
                    keyword.save()

                    if old_category is None:
                        assigned_count += 1
                        if len(examples) < 10:  # Сохраняем примеры для показа
                            examples.append(f'✅ "{keyword.name}" -> {new_category.name}')
                    else:
                        reassigned_count += 1
                        if len(examples) < 10:
                            examples.append(f'🔄 "{keyword.name}" {old_category.name} -> {new_category.name}')

        # Выводим результаты
        self.stdout.write('')
        if examples and show_examples:
            self.stdout.write('📝 ПРИМЕРЫ НАЗНАЧЕНИЯ:')
            for example in examples:
                self.stdout.write(f'   {example}')

        self.stdout.write('=' * 60)
        self.stdout.write(self.style.SUCCESS('✅ КАТЕГОРИИ НАЗНАЧЕНЫ!'))
        self.stdout.write(f'• Назначено новым ключевым словам: {assigned_count}')
        self.stdout.write(f'• Переназначено: {reassigned_count}')

        # Статистика по категориям
        self.stdout.write('\n📊 СТАТИСТИКА ПО КАТЕГОРИЯМ:')
        for category in KeywordCategory.objects.all().order_by('name'):
            count = Keyword.objects.filter(category=category).count()
            if count > 0:
                self.stdout.write(f'   {category.name}: {count} ключевых слов')

        # Ключевые слова без категории
        uncategorized_count = Keyword.objects.filter(category__isnull=True).count()
        if uncategorized_count > 0:
            self.stdout.write(f'   ❓ Без категории: {uncategorized_count}')

    def determine_category(self, keyword_name, category_rules):
        """Определить категорию для ключевого слова"""
        keyword_lower = keyword_name.lower()

        # Специальные правила для конкретных ключевых слов
        special_cases = {
            'tactical turn-based combat': 'Gameplay',
            'turn-based tactics': 'Gameplay',
            'turn-based combat': 'Gameplay',
            'turn-based': 'Gameplay',
            'jrpg': 'Genre',
            'action-rpg': 'Genre',
            'strategy-rpg': 'Genre',
            'visual-novel': 'Genre',
            'roguelike': 'Genre',
            'metroidvania': 'Genre'
        }

        # Проверяем специальные случаи
        if keyword_name in special_cases:
            category_name = special_cases[keyword_name]
            return KeywordCategory.objects.get(name=category_name)

        # Проверяем по общим правилам
        for category_name, terms in category_rules.items():
            for term in terms:
                if term in keyword_lower:
                    return KeywordCategory.objects.get(name=category_name)

        # Если не нашли - назначаем Gameplay по умолчанию
        return KeywordCategory.objects.get(name='Gameplay')