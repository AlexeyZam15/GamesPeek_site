# games/management/commands/compare_games.py
from django.core.management.base import BaseCommand
from games.models import Game, Genre, Keyword, Theme, PlayerPerspective, Company, GameMode, GameEngine
from games.similarity import GameSimilarity
from django.db.models import Prefetch
import time


class Command(BaseCommand):
    help = 'Сравнение двух игр через алгоритм похожести'

    def add_arguments(self, parser):
        parser.add_argument('game1', type=str, help='ID первой игры')
        parser.add_argument('game2', type=str, help='ID второй игры')
        parser.add_argument(
            '--detailed',
            action='store_true',
            help='Показать детальную разбивку по компонентам'
        )

    def get_game_with_details(self, game_id):
        """Получает игру со всеми связанными данными"""
        return Game.objects.filter(id=game_id).prefetch_related(
            'genres',
            'keywords',
            'themes',
            'player_perspectives',
            'developers',
            'game_modes',
            'engines'
        ).first()

    def handle(self, *args, **options):
        game1_id = int(options['game1'])
        game2_id = int(options['game2'])
        detailed = options['detailed']

        # Получаем игры со всеми данными
        game1 = self.get_game_with_details(game1_id)
        game2 = self.get_game_with_details(game2_id)

        if not game1 or not game2:
            self.stdout.write(self.style.ERROR("Игры не найдены"))
            return

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80))
        self.stdout.write(self.style.SUCCESS('СРАВНЕНИЕ ИГР ЧЕРЕЗ АЛГОРИТМ ПОХОЖЕСТИ'))
        self.stdout.write(self.style.SUCCESS('=' * 80))

        # Информация об играх
        self.stdout.write(f'\n📌 ИГРА 1: {game1.name} (ID: {game1.id})')
        genre_names = [g.name for g in game1.genres.all()[:7]]
        self.stdout.write(f'   Жанры: {", ".join(genre_names) if genre_names else "нет"}')
        self.stdout.write(f'   Ключевые слова: {game1.keywords.count()} шт.')
        engine_names = [e.name for e in game1.engines.all()[:3]]
        self.stdout.write(f'   Движки: {", ".join(engine_names) if engine_names else "нет"}')

        self.stdout.write(f'\n📌 ИГРА 2: {game2.name} (ID: {game2.id})')
        genre_names = [g.name for g in game2.genres.all()[:7]]
        self.stdout.write(f'   Жанры: {", ".join(genre_names) if genre_names else "нет"}')
        self.stdout.write(f'   Ключевые слова: {game2.keywords.count()} шт.')
        engine_names = [e.name for e in game2.engines.all()[:3]]
        self.stdout.write(f'   Движки: {", ".join(engine_names) if engine_names else "нет"}')

        # Создаем экземпляр алгоритма
        similarity = GameSimilarity()

        # Обычный расчет
        self.stdout.write('\n' + '-' * 80)
        self.stdout.write('🧮 РАСЧЕТ СХОЖЕСТИ')
        self.stdout.write('-' * 80)

        result = similarity.calculate_similarity(game1, game2)

        self.stdout.write(f'\n📊 ИТОГОВАЯ СХОЖЕСТЬ: ', ending='')
        if result >= 80:
            self.stdout.write(self.style.SUCCESS(f'{result:.1f}% (очень похожи)'))
        elif result >= 60:
            self.stdout.write(self.style.WARNING(f'{result:.1f}% (похожи)'))
        elif result >= 40:
            self.stdout.write(self.style.WARNING(f'{result:.1f}% (средне)'))
        else:
            self.stdout.write(self.style.ERROR(f'{result:.1f}% (мало общего)'))

        # Детальная разбивка
        if detailed:
            self.stdout.write('\n' + '-' * 80)
            self.stdout.write('🔍 ДЕТАЛЬНАЯ РАЗБИВКА')
            self.stdout.write('-' * 80)

            breakdown = similarity.get_similarity_breakdown(game1, game2)

            # Динамические веса
            self.stdout.write('\n⚖️  Динамические веса:')
            weights = breakdown['dynamic_weights']
            for key, value in weights.items():
                if key not in ['active_criteria_count', 'is_single_criterion'] and value > 0:
                    self.stdout.write(f'   {key}: {value:.1f}%')

            # Общие элементы
            self.stdout.write('\n🤝 Общие элементы:')

            # Жанры
            common_genre_ids = breakdown['genres']['common_elements']
            if common_genre_ids:
                genre_names = Genre.objects.filter(id__in=common_genre_ids).values_list('name', flat=True)
                self.stdout.write(f'   Жанры ({len(common_genre_ids)}): {", ".join(genre_names)}')
                self.stdout.write(
                    f'   Вклад: {breakdown["genres"]["score"]:.1f}/{breakdown["genres"]["max_score"]:.1f}')
            else:
                self.stdout.write(f'   Жанры: нет общих')

            # Ключевые слова
            common_keyword_ids = breakdown['keywords']['common_elements']
            if common_keyword_ids:
                keyword_names = Keyword.objects.filter(id__in=common_keyword_ids).values_list('name', flat=True)
                self.stdout.write(f'\n   Ключевые слова ({len(common_keyword_ids)}): {", ".join(keyword_names[:10])}')
                if len(common_keyword_ids) > 10:
                    self.stdout.write(f'   ... и ещё {len(common_keyword_ids) - 10}')
                self.stdout.write(
                    f'   Вклад: {breakdown["keywords"]["score"]:.1f}/{breakdown["keywords"]["max_score"]:.1f}')
            else:
                self.stdout.write(f'\n   Ключевые слова: нет общих')

            # Темы
            common_theme_ids = breakdown['themes']['common_elements']
            if common_theme_ids:
                theme_names = Theme.objects.filter(id__in=common_theme_ids).values_list('name', flat=True)
                self.stdout.write(f'\n   Темы ({len(common_theme_ids)}): {", ".join(theme_names)}')
                self.stdout.write(
                    f'   Вклад: {breakdown["themes"]["score"]:.1f}/{breakdown["themes"]["max_score"]:.1f}')
            else:
                self.stdout.write(f'\n   Темы: нет общих')

            # Перспективы
            common_perspective_ids = breakdown['perspectives']['common_elements']
            if common_perspective_ids:
                perspective_names = PlayerPerspective.objects.filter(id__in=common_perspective_ids).values_list('name',
                                                                                                                flat=True)
                self.stdout.write(f'\n   Перспективы ({len(common_perspective_ids)}): {", ".join(perspective_names)}')
                self.stdout.write(
                    f'   Вклад: {breakdown["perspectives"]["score"]:.1f}/{breakdown["perspectives"]["max_score"]:.1f}')
            else:
                self.stdout.write(f'\n   Перспективы: нет общих')

            # Режимы игры
            common_mode_ids = breakdown['game_modes']['common_elements']
            if common_mode_ids:
                mode_names = GameMode.objects.filter(id__in=common_mode_ids).values_list('name', flat=True)
                self.stdout.write(f'\n   Режимы игры ({len(common_mode_ids)}): {", ".join(mode_names)}')
                self.stdout.write(
                    f'   Вклад: {breakdown["game_modes"]["score"]:.1f}/{breakdown["game_modes"]["max_score"]:.1f}')
            else:
                self.stdout.write(f'\n   Режимы игры: нет общих')

            # Разработчики
            common_developer_ids = breakdown['developers']['common_elements']
            if common_developer_ids:
                developer_names = Company.objects.filter(id__in=common_developer_ids).values_list('name', flat=True)
                self.stdout.write(f'\n   Разработчики ({len(common_developer_ids)}): {", ".join(developer_names)}')
                self.stdout.write(
                    f'   Вклад: {breakdown["developers"]["score"]:.1f}/{breakdown["developers"]["max_score"]:.1f}')
            else:
                self.stdout.write(f'\n   Разработчики: нет общих')

            # Движки
            common_engine_ids = breakdown.get('engines', {}).get('common_elements', [])
            if common_engine_ids:
                engine_names = GameEngine.objects.filter(id__in=common_engine_ids).values_list('name', flat=True)
                self.stdout.write(f'\n   Движки ({len(common_engine_ids)}): {", ".join(engine_names)}')
                engine_score = breakdown.get('engines', {}).get('score', 0)
                engine_max = breakdown.get('engines', {}).get('max_score', 0)
                self.stdout.write(f'   Вклад: {engine_score:.1f}/{engine_max:.1f}')
            else:
                self.stdout.write(f'\n   Движки: нет общих')

            self.stdout.write(f'\n📊 ИТОГО: {breakdown["total_similarity"]:.1f}%')

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80))