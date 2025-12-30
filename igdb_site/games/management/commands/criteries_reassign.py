"""
Команда для переназначения тем в жанры и ключевых слов в темы
"""

from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Game, Theme, Genre, Keyword
import sys


class Command(BaseCommand):
    help = 'Перенести темы в жанры и ключевые слова в темы'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать что будет сделано без изменения БД'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=1000,
            help='Размер батча для обработки (по умолчанию: 1000)'
        )
        parser.add_argument(
            '--keep-old',
            action='store_true',
            help='Не удалять старые критерии'
        )

    def create_genre_with_igdb_id(self, name):
        """Создать жанр с временным igdb_id"""
        igdb_id = -abs(hash(name)) % 1000000

        genre, created = Genre.objects.get_or_create(
            name=name,
            defaults={'igdb_id': igdb_id}
        )
        return genre, created

    def create_theme_with_igdb_id(self, name):
        """Создать тему с временным igdb_id"""
        igdb_id = -abs(hash(name)) % 1000000

        theme, created = Theme.objects.get_or_create(
            name=name,
            defaults={'igdb_id': igdb_id}
        )
        return theme, created

    def print_progress_bar(self, iteration, total, prefix='', suffix='', length=50, fill='█'):
        """Вывести прогресс-бар в одной строке"""
        percent = f"{100 * (iteration / float(total)):.1f}"
        filled_length = int(length * iteration // total)
        bar = fill * filled_length + '-' * (length - filled_length)

        sys.stdout.write(f'\r{prefix} |{bar}| {percent}% {suffix}')
        sys.stdout.flush()

        if iteration == total:
            print()

    def get_theme_to_genre_mapping(self):
        """Возвращает маппинг тем в жанры"""
        return {
            'Action': 'Action',
            'Open world': 'Open World',
            'Open World': 'Open World',
            'Sandbox': 'Sandbox',
            'Survival': 'Survival',
        }

    def get_keyword_to_theme_mapping(self):
        """Возвращает маппинг ключевых слов в темы"""
        return {
            'gothic': 'Gothic',
            'Gothic': 'Gothic',
            'medieval': 'Medieval',
            'Medieval': 'Medieval',
            'real-time combat': 'Real-time Combat',
        }

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        keep_old = options['keep_old']

        # Получаем маппинги из методов
        theme_to_genre_mapping = self.get_theme_to_genre_mapping()
        keyword_to_theme_mapping = self.get_keyword_to_theme_mapping()

        total_removed_themes = 0
        total_added_genres = 0
        total_removed_keywords = 0
        total_added_themes = 0

        with transaction.atomic():
            # 1. Перенос тем в жанры (удаляем темы, добавляем жанры)
            self.stdout.write('=== ПЕРЕНОС ТЕМ В ЖАНРЫ ===')

            # Получаем все темы, которые нужно перенести
            themes_to_process = Theme.objects.filter(
                name__in=list(theme_to_genre_mapping.keys())
            )

            for theme in themes_to_process:
                theme_name = theme.name
                genre_name = theme_to_genre_mapping.get(theme_name)

                if not genre_name:
                    continue

                try:
                    # Создаем или получаем жанр
                    genre, genre_created = self.create_genre_with_igdb_id(genre_name)

                    if genre_created:
                        self.stdout.write(f'Создан новый жанр: "{genre_name}"')

                    # Получаем ID всех игр с этой темой
                    game_ids = list(Game.objects.filter(themes=theme).values_list('id', flat=True))

                    if not game_ids:
                        self.stdout.write(f'Для темы "{theme_name}" нет игр')
                        continue

                    total_games = len(game_ids)
                    added_genres = 0
                    removed_themes = 0

                    self.stdout.write(f'Тема "{theme_name}" -> жанр "{genre_name}": {total_games} игр')

                    # Обрабатываем батчами с прогресс-баром
                    for i in range(0, total_games, batch_size):
                        batch_ids = game_ids[i:i + batch_size]
                        games = Game.objects.filter(id__in=batch_ids).prefetch_related('genres', 'themes')

                        for game in games:
                            # Добавляем новый жанр
                            if genre not in game.genres.all():
                                if not dry_run:
                                    game.genres.add(genre)
                                added_genres += 1

                            # Удаляем старую тему
                            if theme in game.themes.all() and not keep_old:
                                if not dry_run:
                                    game.themes.remove(theme)
                                removed_themes += 1

                        # Обновляем прогресс-бар
                        current = min(i + batch_size, total_games)
                        self.print_progress_bar(
                            current, total_games,
                            prefix=f'Обработка "{theme_name[:15]}..."',
                            suffix=f'Добавлено: {added_genres}, Удалено: {removed_themes}'
                        )

                    print()  # Новая строка после прогресс-бара
                    self.stdout.write(self.style.SUCCESS(
                        f'  Добавлено жанров: {added_genres}, Удалено тем: {removed_themes}'
                    ))

                    total_added_genres += added_genres
                    total_removed_themes += removed_themes

                except Exception as e:
                    print()  # Новая строка если была ошибка
                    self.stdout.write(self.style.ERROR(f'Ошибка для темы "{theme_name}": {e}'))

            # 2. Перенос ключевых слов в темы
            self.stdout.write('\n=== ПЕРЕНОС КЛЮЧЕВЫХ СЛОВ В ТЕМЫ ===')

            # Получаем все ключевые слова, которые нужно перенести
            keywords_to_process = Keyword.objects.filter(
                name__iregex=r'^(gothic|medieval|real.time.combat|real.time.combat|realtime.combat)$'
            )

            for keyword in keywords_to_process:
                keyword_name = keyword.name
                # Определяем название темы
                theme_name = keyword_to_theme_mapping.get(
                    keyword_name,
                    keyword_name.capitalize()
                )

                try:
                    # Создаем или получаем тему
                    theme, theme_created = self.create_theme_with_igdb_id(theme_name)

                    if theme_created:
                        self.stdout.write(f'Создана новая тема: "{theme_name}"')

                    # Получаем ID всех игр с этим ключевым словом
                    game_ids = list(Game.objects.filter(keywords=keyword).values_list('id', flat=True))

                    if not game_ids:
                        self.stdout.write(f'Для ключ.слова "{keyword_name}" нет игр')
                        continue

                    total_games = len(game_ids)
                    added_themes = 0
                    removed_keywords = 0

                    self.stdout.write(f'Ключ.слово "{keyword_name}" -> тема "{theme_name}": {total_games} игр')

                    # Обрабатываем батчами с прогресс-баром
                    for i in range(0, total_games, batch_size):
                        batch_ids = game_ids[i:i + batch_size]
                        games = Game.objects.filter(id__in=batch_ids).prefetch_related('themes', 'keywords')

                        for game in games:
                            # Добавляем новую тему
                            if theme not in game.themes.all():
                                if not dry_run:
                                    game.themes.add(theme)
                                added_themes += 1

                            # Удаляем старое ключевое слово
                            if keyword in game.keywords.all() and not keep_old:
                                if not dry_run:
                                    game.keywords.remove(keyword)
                                removed_keywords += 1

                        # Обновляем прогресс-бар
                        current = min(i + batch_size, total_games)
                        self.print_progress_bar(
                            current, total_games,
                            prefix=f'Обработка "{keyword_name}"',
                            suffix=f'Добавлено: {added_themes}, Удалено: {removed_keywords}'
                        )

                    print()  # Новая строка после прогресс-бара
                    self.stdout.write(self.style.SUCCESS(
                        f'  Добавлено тем: {added_themes}, Удалено ключ.слов: {removed_keywords}'
                    ))

                    total_added_themes += added_themes
                    total_removed_keywords += removed_keywords

                except Exception as e:
                    print()  # Новая строка если была ошибка
                    self.stdout.write(self.style.ERROR(f'Ошибка для ключ.слова "{keyword_name}": {e}'))

        # Итоги
        self.stdout.write('\n' + '=' * 50)
        if dry_run:
            self.stdout.write(self.style.WARNING('РЕЖИМ ТЕСТИРОВАНИЯ (без изменений в БД)'))
        else:
            self.stdout.write(self.style.SUCCESS('ИЗМЕНЕНИЯ ВНЕСЕНЫ В БД'))

        self.stdout.write(self.style.SUCCESS(f'ИТОГО обработано:'))
        self.stdout.write(
            self.style.SUCCESS(f'  • Темы -> жанры: добавлено {total_added_genres}, удалено {total_removed_themes}'))
        self.stdout.write(self.style.SUCCESS(
            f'  • Ключ.слова -> темы: добавлено {total_added_themes}, удалено {total_removed_keywords}'))

        if keep_old:
            self.stdout.write(self.style.WARNING('СТАРЫЕ КРИТЕРИИ СОХРАНЕНЫ (опция --keep-old)'))
