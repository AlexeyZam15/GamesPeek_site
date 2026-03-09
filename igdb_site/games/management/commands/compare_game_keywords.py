# games/management/commands/compare_game_keywords.py

"""
Django management command для сравнения ключевых слов игр.
Принимает ID или названия игр, выбирает самые популярные варианты.
Логика сравнения:
1. Первая игра - исходная
2. Выводятся общие ключевые слова, которые есть у 1-й игры и у ВСЕХ остальных
3. Для каждой последующей игры выводятся ключевые слова, которые есть у 1-й игры и у этой игры,
   но отсутствуют у всех других сравниваемых игр
"""

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Count, Q, Prefetch
from games.models_parts.game import Game
from games.models_parts.keywords import Keyword, KeywordCategory
from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict
import re


class Command(BaseCommand):
    help = 'Сравнивает ключевые слова игр. Первая игра - исходная, остальные - для сравнения.'

    def add_arguments(self, parser):
        parser.add_argument(
            'game_identifiers',
            nargs='+',
            help='ID или названия игр для сравнения (минимум 2 игры). Первая - исходная.'
        )

        parser.add_argument(
            '--use-most-popular',
            action='store_true',
            default=True,
            help='Использовать самые популярные игры при поиске по названию (по умолчанию True)'
        )

        parser.add_argument(
            '--limit-per-name',
            type=int,
            default=1,
            help='Количество самых популярных игр для каждого названия (по умолчанию 1)'
        )

        parser.add_argument(
            '--show-categories',
            action='store_true',
            default=False,
            help='Группировать ключевые слова по категориям'
        )

        parser.add_argument(
            '--min-popularity',
            type=int,
            default=0,
            help='Минимальная популярность ключевого слова (количество использований)'
        )

        parser.add_argument(
            '--verbose',
            action='store_true',
            default=False,
            help='Показывать дополнительную информацию о ключевых словах'
        )

        parser.add_argument(
            '--separator',
            type=str,
            default=', ',
            help='Разделитель для списка ключевых слов (по умолчанию: ", ")'
        )

    def handle(self, *args, **options):
        """Основной обработчик команды."""
        game_identifiers = options['game_identifiers']
        use_most_popular = options['use_most_popular']
        limit_per_name = options['limit_per_name']
        show_categories = options['show_categories']
        min_popularity = options['min_popularity']
        verbose = options['verbose']
        separator = options['separator']

        # Проверяем, что передано минимум 2 игры
        if len(game_identifiers) < 2:
            raise CommandError('Необходимо указать минимум 2 игры для сравнения')

        self.stdout.write(self.style.NOTICE('🔍 Поиск игр по указанным идентификаторам...'))

        # Получаем игры по идентификаторам
        games = self._get_games_by_identifiers(
            game_identifiers,
            use_most_popular,
            limit_per_name
        )

        if len(games) < 2:
            raise CommandError(f'Найдено менее 2 игр: {len(games)}')

        if len(games) != len(game_identifiers):
            self.stdout.write(self.style.WARNING(
                f'⚠️ Найдено только {len(games)} из {len(game_identifiers)} запрошенных игр'
            ))

        # Разделяем игры: первая - исходная, остальные - для сравнения
        source_game = games[0]
        comparison_games = games[1:]

        self.stdout.write(self.style.SUCCESS(
            f'\n✅ Исходная игра: {source_game.name} (ID: {source_game.id}, IGDB ID: {source_game.igdb_id})'
        ))
        self.stdout.write(self.style.SUCCESS('📊 Игры для сравнения:'))
        for i, game in enumerate(comparison_games, 1):
            self.stdout.write(f'   {i}. {game.name} (ID: {game.id}, IGDB ID: {game.igdb_id})')

        # Получаем ключевые слова для всех игр с предзагрузкой категорий
        games_keywords = self._get_games_keywords([source_game] + comparison_games, min_popularity)

        # Получаем множества ключевых слов для каждой игры
        source_keywords_set = set(games_keywords[source_game.id])
        comparison_sets = {
            game.id: set(games_keywords[game.id])
            for game in comparison_games
        }

        # Находим общие ключевые слова для всех игр (которые есть у исходной и у всех остальных)
        common_with_all = source_keywords_set.copy()
        for game in comparison_games:
            common_with_all &= comparison_sets[game.id]

        # Для каждой сравниваемой игры находим уникальные ключевые слова
        # (которые есть у исходной и у этой игры, но нет ни у одной другой сравниваемой)
        unique_per_game = {}

        for i, current_game in enumerate(comparison_games):
            # Начинаем с ключевых слов, которые есть у исходной и у текущей игры
            unique_keywords = source_keywords_set & comparison_sets[current_game.id]

            # Исключаем те, которые есть у любой другой сравниваемой игры
            for other_game in comparison_games:
                if other_game.id != current_game.id:
                    unique_keywords -= comparison_sets[other_game.id]

            unique_per_game[current_game.id] = unique_keywords

        # Вывод результатов
        self._print_results(
            source_game,
            comparison_games,
            games_keywords,
            common_with_all,
            unique_per_game,
            show_categories,
            verbose,
            separator
        )

    def _get_games_by_identifiers(self, identifiers: List[str], use_most_popular: bool, limit_per_name: int) -> List[
        Game]:
        """
        Получает игры по идентификаторам (ID или названиям).
        Если use_most_popular=True, для названий выбирает самые популярные игры.
        """
        games = []
        not_found = []

        for identifier in identifiers:
            # Пробуем интерпретировать как ID
            if identifier.isdigit():
                try:
                    game = Game.objects.get(id=int(identifier))
                    games.append(game)
                    continue
                except Game.DoesNotExist:
                    pass

                try:
                    game = Game.objects.get(igdb_id=int(identifier))
                    games.append(game)
                    continue
                except Game.DoesNotExist:
                    pass

            # Если не ID, ищем по названию
            name_games = self._search_games_by_name(identifier, use_most_popular, limit_per_name)

            if name_games:
                games.extend(name_games)
            else:
                not_found.append(identifier)

        if not_found:
            self.stdout.write(self.style.WARNING(
                f'⚠️ Не найдены игры для идентификаторов: {", ".join(not_found)}'
            ))

        return games

    def _search_games_by_name(self, name: str, use_most_popular: bool, limit: int) -> List[Game]:
        """
        Ищет игры по названию.
        Если use_most_popular=True, возвращает самые популярные (по rating_count).
        """
        # Очищаем название от лишних пробелов и приводим к нижнему регистру для поиска
        clean_name = name.strip()

        # Базовый запрос: точное совпадение или частичное
        base_query = Game.objects.filter(
            Q(name__iexact=clean_name) | Q(name__icontains=clean_name)
        )

        if not base_query.exists():
            # Пробуем искать без учета регистра и с нечетким совпадением
            # Разбиваем на слова и ищем по всем словам
            words = re.findall(r'\w+', clean_name.lower())
            if words:
                q_objects = Q()
                for word in words:
                    if len(word) > 2:  # Игнорируем очень короткие слова
                        q_objects &= Q(name__icontains=word)

                if q_objects:
                    base_query = Game.objects.filter(q_objects)

        if not base_query.exists():
            return []

        if use_most_popular:
            # Сортируем по популярности (rating_count) и берем limit самых популярных
            return list(base_query.order_by('-rating_count')[:limit])
        else:
            # Возвращаем все найденные, но не больше limit
            return list(base_query[:limit])

    def _get_games_keywords(self, games: List[Game], min_popularity: int) -> Dict[int, List[Keyword]]:
        """
        Получает ключевые слова для списка игр с предзагрузкой категорий.
        Возвращает словарь {game_id: [keywords]}.
        """
        game_ids = [game.id for game in games]

        # Получаем все ключевые слова для указанных игр с предзагрузкой категорий
        keywords_qs = Keyword.objects.filter(
            game__id__in=game_ids,
            cached_usage_count__gte=min_popularity
        ).select_related('category').distinct()

        # Группируем ключевые слова по играм
        result = defaultdict(list)

        # Используем values_list для получения связей game-keyword
        from django.db import connection

        # Прямой запрос к таблице many-to-many для оптимизации
        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT game_id, keyword_id
                           FROM games_game_keywords
                           WHERE game_id IN %s
                           """, [tuple(game_ids)])

            game_keyword_pairs = cursor.fetchall()

        # Создаем словарь keyword_id -> keyword объект
        keyword_ids = [pair[1] for pair in game_keyword_pairs]
        keywords_by_id = {
            k.id: k for k in keywords_qs.filter(id__in=keyword_ids)
        }

        # Группируем по играм
        for game_id, keyword_id in game_keyword_pairs:
            if keyword_id in keywords_by_id:
                result[game_id].append(keywords_by_id[keyword_id])

        return dict(result)

    def _print_results(
            self,
            source_game: Game,
            comparison_games: List[Game],
            games_keywords: Dict[int, List[Keyword]],
            common_keywords: Set[Keyword],
            unique_per_game: Dict[int, Set[Keyword]],
            show_categories: bool,
            verbose: bool,
            separator: str
    ):
        """Выводит результаты сравнения в отформатированном виде."""

        self.stdout.write('\n' + '=' * 80)
        self.stdout.write(self.style.SUCCESS('📊 РЕЗУЛЬТАТЫ СРАВНЕНИЯ КЛЮЧЕВЫХ СЛОВ'))
        self.stdout.write('=' * 80)

        # 1. Общие ключевые слова (есть у исходной и у всех остальных)
        self.stdout.write(
            '\n' + self.style.MIGRATE_HEADING('🔷 ОБЩИЕ КЛЮЧЕВЫЕ СЛОВА (есть у исходной и у всех остальных):'))

        if common_keywords:
            common_list = sorted(common_keywords, key=lambda k: k.name)

            if show_categories:
                # Группируем по категориям
                by_category = defaultdict(list)
                for kw in common_list:
                    category_name = kw.category.name if kw.category else "Без категории"
                    by_category[category_name].append(kw)

                for category, keywords in sorted(by_category.items()):
                    keywords_list = [kw.name for kw in keywords]
                    self.stdout.write(f'\n  📌 {category}:')
                    self.stdout.write(f'    {separator.join(keywords_list)}')
            else:
                keywords_list = [kw.name for kw in common_list]
                self.stdout.write(f'  {separator.join(keywords_list)}')

            self.stdout.write(f'\n  ✅ Всего: {len(common_keywords)} ключевых слов')
        else:
            self.stdout.write('  ❌ Нет общих ключевых слов')

        # 2. Уникальные ключевые слова для каждой сравниваемой игры
        self.stdout.write('\n' + '=' * 80)
        self.stdout.write(
            self.style.MIGRATE_HEADING('🔶 УНИКАЛЬНЫЕ КЛЮЧЕВЫЕ СЛОВА (есть у исходной и у этой игры, но нет у других):'))

        for game in comparison_games:
            unique_keywords = unique_per_game.get(game.id, set())

            self.stdout.write(f'\n  🎮 {game.name}:')

            if unique_keywords:
                unique_list = sorted(unique_keywords, key=lambda k: k.name)

                if show_categories:
                    # Группируем по категориям
                    by_category = defaultdict(list)
                    for kw in unique_list:
                        category_name = kw.category.name if kw.category else "Без категории"
                        by_category[category_name].append(kw)

                    for category, keywords in sorted(by_category.items()):
                        keywords_list = [kw.name for kw in keywords]
                        self.stdout.write(f'\n    📌 {category}:')
                        self.stdout.write(f'      {separator.join(keywords_list)}')
                else:
                    keywords_list = [kw.name for kw in unique_list]
                    self.stdout.write(f'    {separator.join(keywords_list)}')

                self.stdout.write(f'\n    ✅ Уникальных: {len(unique_keywords)} ключевых слов')
            else:
                self.stdout.write('    ❌ Нет уникальных ключевых слов')

        # Дополнительная статистика
        if verbose:
            self.stdout.write('\n' + '=' * 80)
            self.stdout.write(self.style.MIGRATE_HEADING('📈 ДОПОЛНИТЕЛЬНАЯ СТАТИСТИКА:'))

            source_keywords = games_keywords[source_game.id]
            source_list = [kw.name for kw in sorted(source_keywords, key=lambda k: k.name)]
            self.stdout.write(f'\n  📊 Все ключевые слова исходной игры ({len(source_keywords)}):')
            self.stdout.write(f'    {separator.join(source_list)}')

            for game in comparison_games:
                game_keywords = games_keywords[game.id]
                common_with_source = set(source_keywords) & set(game_keywords)
                common_list = [kw.name for kw in sorted(common_with_source, key=lambda k: k.name)]

                self.stdout.write(f'\n  📊 {game.name} ({len(game_keywords)} ключевых слов):')
                self.stdout.write(f'    Общие с исходной ({len(common_with_source)}): {separator.join(common_list)}')
                self.stdout.write(
                    f'    Процент совпадения: {len(common_with_source) / len(source_keywords) * 100:.1f}%' if source_keywords else '    Процент совпадения: 0%')

        self.stdout.write('\n' + '=' * 80)