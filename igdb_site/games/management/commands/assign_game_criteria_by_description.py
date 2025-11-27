# management/commands/assign_game_criteria_by_description.py
from django.core.management.base import BaseCommand, CommandError
from django.db.models import Q
import re
from games.models import Game, Genre, Theme, Keyword, GameMode, PlayerPerspective


class Command(BaseCommand):
    help = 'Анализирует описание игры и автоматически присваивает найденные критерии'

    def add_arguments(self, parser):
        parser.add_argument(
            '--name',
            type=str,
            help='Название игры для анализа (можно часть названия)',
        )
        parser.add_argument(
            '--exact',
            action='store_true',
            help='Точное совпадение названия',
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=5,
            help='Лимит результатов поиска (по умолчанию 5)',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать что будет присвоено без сохранения в БД',
        )
        parser.add_argument(
            '--min-confidence',
            type=str,
            choices=['low', 'medium', 'high'],
            default='low',
            help='Минимальная уверенность для присвоения (low, medium, high)',
        )

    def handle(self, *args, **options):
        game_name = options['name']
        exact_match = options['exact']
        limit = options['limit']
        dry_run = options['dry_run']
        min_confidence = options['min_confidence']

        if not game_name:
            raise CommandError('Необходимо указать название игры через --name')

        # Поиск игр
        games = self.find_games_by_name(game_name, exact_match, limit)

        if not games:
            self.stdout.write(
                self.style.WARNING(f'Игры с названием "{game_name}" не найдены')
            )
            return

        self.stdout.write(
            self.style.SUCCESS(f'Найдено игр: {len(games)}')
        )
        if dry_run:
            self.stdout.write(
                self.style.WARNING('⚡ РЕЖИМ ПРЕДПРОСМОТРА - изменения не сохраняются')
            )

        # Анализ и присвоение критериев каждой найденной игре
        for game in games:
            self.analyze_and_assign_criteria(game, dry_run, min_confidence)

    def find_games_by_name(self, name, exact_match, limit):
        """Поиск игр по названию"""
        if exact_match:
            games = Game.objects.filter(name__iexact=name)[:limit]
        else:
            games = Game.objects.filter(name__icontains=name)[:limit]
        return games

    def analyze_and_assign_criteria(self, game, dry_run, min_confidence):
        """Анализ и присвоение критериев игре"""
        analyzer = GameCriteriaAssigner()
        assignment_plan = analyzer.create_assignment_plan(game)

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(
            self.style.SUCCESS(f'Игра: {game.name} (ID: {game.id})')
        )
        self.stdout.write("=" * 60)

        # Выводим план присвоения
        self.print_assignment_plan(assignment_plan)

        # Проверяем уверенность
        confidence_levels = {'low': 0, 'medium': 1, 'high': 2}
        plan_confidence_level = confidence_levels[assignment_plan['confidence']]
        min_confidence_level = confidence_levels[min_confidence]

        if plan_confidence_level < min_confidence_level:
            self.stdout.write(
                self.style.WARNING(
                    f'❌ Уверенность слишком низкая ({assignment_plan["confidence"]}) < требуемой ({min_confidence}) - пропускаем'
                )
            )
            return

        # Применяем изменения (если не dry-run)
        if not dry_run:
            self.apply_assignment_plan(game, assignment_plan)
            self.stdout.write(
                self.style.SUCCESS('✅ Изменения сохранены в БД')
            )
        else:
            self.stdout.write(
                self.style.WARNING('💡 Режим предпросмотра - изменения НЕ сохранены')
            )

    def print_assignment_plan(self, plan):
        """Вывод плана присвоения критериев"""
        self.stdout.write(
            self.style.MIGRATE_HEADING('\n📋 ПЛАН ПРИСВОЕНИЯ КРИТЕРИЕВ:')
        )

        # Жанры
        if plan['genres_to_add']:
            self.stdout.write(
                self.style.SUCCESS(f"🎮 Жанры для добавления: {', '.join(plan['genres_to_add'])}")
            )
        else:
            self.stdout.write("🎮 Жанры для добавления: нет")

        # Темы
        if plan['themes_to_add']:
            self.stdout.write(
                self.style.SUCCESS(f"🎭 Темы для добавления: {', '.join(plan['themes_to_add'])}")
            )
        else:
            self.stdout.write("🎭 Темы для добавления: нет")

        # Ключевые слова
        if plan['keywords_to_add']:
            self.stdout.write(
                self.style.SUCCESS(f"🏷️ Ключевые слова для добавления: {', '.join(plan['keywords_to_add'])}")
            )
        else:
            self.stdout.write("🏷️ Ключевые слова для добавления: нет")

        # Режимы игры
        if plan['game_modes_to_add']:
            self.stdout.write(
                self.style.SUCCESS(f"👥 Режимы игры для добавления: {', '.join(plan['game_modes_to_add'])}")
            )
        else:
            self.stdout.write("👥 Режимы игры для добавления: нет")

        # Перспективы
        if plan['perspectives_to_add']:
            self.stdout.write(
                self.style.SUCCESS(f"👀 Перспективы для добавления: {', '.join(plan['perspectives_to_add'])}")
            )
        else:
            self.stdout.write("👀 Перспективы для добавления: нет")

        # Статистика
        self.stdout.write(
            self.style.MIGRATE_HEADING(f"\n📊 СТАТИСТИКА:")
        )
        self.stdout.write(f"• Уверенность анализа: {plan['confidence']}")
        self.stdout.write(f"• Всего критериев для добавления: {plan['total_to_add']}")

    def apply_assignment_plan(self, game, plan):
        """Применение плана присвоения к игре"""
        # Жанры
        if plan['genres_to_add']:
            genres_to_add = Genre.objects.filter(name__in=plan['genres_to_add'])
            game.genres.add(*genres_to_add)

        # Темы
        if plan['themes_to_add']:
            themes_to_add = Theme.objects.filter(name__in=plan['themes_to_add'])
            game.themes.add(*themes_to_add)

        # Ключевые слова
        if plan['keywords_to_add']:
            keywords_to_add = Keyword.objects.filter(name__in=plan['keywords_to_add'])
            game.keywords.add(*keywords_to_add)

        # Режимы игры
        if plan['game_modes_to_add']:
            game_modes_to_add = GameMode.objects.filter(name__in=plan['game_modes_to_add'])
            game.game_modes.add(*game_modes_to_add)

        # Перспективы
        if plan['perspectives_to_add']:
            perspectives_to_add = PlayerPerspective.objects.filter(name__in=plan['perspectives_to_add'])
            game.player_perspectives.add(*perspectives_to_add)


class GameCriteriaAssigner:
    """Класс для анализа и создания плана присвоения критериев"""

    def __init__(self):
        # Расширенные словари для сопоставления
        self.genre_mapping = {
            'Role-playing (RPG)': ['role-playing', 'rpg', 'level up', 'experience points', 'quest',
                                   'character progression', 'skill tree', 'dialogue', 'non-player character'],
            'Action': ['action', 'combat', 'fight', 'battle', 'shooter', 'shooting', 'melee', 'fast-paced', 'enemies',
                       'boss'],
            'Adventure': ['adventure', 'explore', 'puzzle', 'story-driven', 'exploration', 'narrative', 'storyline',
                          'mystery'],
            'Strategy': ['strategy', 'tactical', 'planning', 'resource management', 'base building', 'turn-based',
                         'real-time strategy'],
            'Simulation': ['simulation', 'sim', 'realistic', 'management', 'physics', 'realism', 'economy',
                           'simulator'],
            'Sports': ['sports', 'football', 'basketball', 'racing', 'soccer', 'athlete', 'sport', 'championship',
                       'tournament'],
            'Horror': ['horror', 'scary', 'terror', 'survival horror', 'fear', 'psychological', 'supernatural',
                       'monster'],
            'Platformer': ['platform', 'jump', 'platforming', 'precise controls', 'side-scrolling', 'platformer'],
            'Puzzle': ['puzzle', 'brain teaser', 'logic', 'solve', 'challenge', 'mind'],
        }

        self.theme_mapping = {
            'Fantasy': ['fantasy', 'magic', 'dragon', 'medieval', 'sword', 'wizard', 'orc', 'elf', 'dwarf', 'kingdom'],
            'Science fiction': ['sci-fi', 'science fiction', 'space', 'alien', 'future', 'cyberpunk', 'spaceship',
                                'robot', 'technology'],
            'Post-apocalyptic': ['post-apocalyptic', 'apocalypse', 'wasteland', 'survival', 'dystopian', 'catastrophe',
                                 'end of the world'],
            'Historical': ['historical', 'world war', 'ancient', 'history', 'historical', 'period', 'era', 'century'],
            'Modern': ['modern', 'contemporary', 'present day', 'current', 'today', 'real world'],
            'Steampunk': ['steampunk', 'victorian', 'industrial', 'clockwork'],
            'Noir': ['noir', 'detective', 'mystery', 'crime', 'investigation'],
        }

        self.game_mode_mapping = {
            'Single-player': ['single-player', 'single player', 'solo'],
            'Multiplayer': ['multiplayer', 'co-op', 'cooperative', 'pvp', 'online'],
        }

        self.perspective_mapping = {
            'First-person': ['first-person', 'first person', 'fps'],
            'Third-person': ['third-person', 'third person'],
            'Isometric': ['isometric', 'top-down'],
            'Side view': ['side view', 'side-scrolling'],
        }

    def create_assignment_plan(self, game):
        """Создает план присвоения критериев для игры"""
        text = self._prepare_text(game)

        # Анализ текста
        detected_genres = self._detect_items(text, self.genre_mapping)
        detected_themes = self._detect_items(text, self.theme_mapping)
        detected_game_modes = self._detect_items(text, self.game_mode_mapping)
        detected_perspectives = self._detect_items(text, self.perspective_mapping)
        detected_keywords = self._detect_keywords(text)

        # Получаем текущие значения
        current_genres = set(game.genres.values_list('name', flat=True))
        current_themes = set(game.themes.values_list('name', flat=True))
        current_keywords = set(game.keywords.values_list('name', flat=True))
        current_game_modes = set(game.game_modes.values_list('name', flat=True))
        current_perspectives = set(game.player_perspectives.values_list('name', flat=True))

        # Исключаем уже существующие критерии
        genres_to_add = list(detected_genres - current_genres)
        themes_to_add = list(detected_themes - current_themes)
        keywords_to_add = list(set(detected_keywords) - current_keywords)
        game_modes_to_add = list(detected_game_modes - current_game_modes)
        perspectives_to_add = list(detected_perspectives - current_perspectives)

        # Создаем план (ТОЛЬКО ДОБАВЛЕНИЕ новых критериев)
        plan = {
            # Для добавления (только новые)
            'genres_to_add': genres_to_add,
            'themes_to_add': themes_to_add,
            'game_modes_to_add': game_modes_to_add,
            'perspectives_to_add': perspectives_to_add,
            'keywords_to_add': keywords_to_add,

            # Мета-информация
            'confidence': self._calculate_confidence(
                len(detected_genres) + len(detected_themes) +
                len(detected_keywords) + len(detected_game_modes) +
                len(detected_perspectives)
            ),
            'total_to_add': len(genres_to_add) + len(themes_to_add) +
                            len(keywords_to_add) + len(game_modes_to_add) +
                            len(perspectives_to_add),
        }

        return plan

    def _prepare_text(self, game):
        """Подготовка текста для анализа"""
        description_parts = []
        if game.summary:
            description_parts.append(game.summary.lower())
        if game.storyline:
            description_parts.append(game.storyline.lower())
        return ' '.join(description_parts)

    def _detect_items(self, text, mapping_dict):
        """Обнаружение элементов по словарю"""
        detected = set()
        for item_name, keywords in mapping_dict.items():
            for keyword in keywords:
                pattern = r'\b' + re.escape(keyword) + r'\b'
                if re.search(pattern, text):
                    detected.add(item_name)
                    break
        return detected

    def _detect_keywords(self, text):
        """Обнаружение ключевых слов из базы"""
        detected_keywords = []
        all_keywords = Keyword.objects.all()

        for keyword in all_keywords:
            pattern = r'\b' + re.escape(keyword.name.lower()) + r'\b'
            if re.search(pattern, text):
                detected_keywords.append(keyword.name)

        return detected_keywords

    def _calculate_confidence(self, total_matches):
        """Расчет уверенности"""
        if total_matches >= 5:
            return 'high'
        elif total_matches >= 2:
            return 'medium'
        else:
            return 'low'