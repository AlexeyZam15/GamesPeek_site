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
            'realtime combat': 'Real-time Combat',
            'fire emblem': 'Fire Emblem',
            'Fire Emblem': 'Fire Emblem',
        }

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        keep_old = options['keep_old']

        theme_to_genre_mapping = self.get_theme_to_genre_mapping()
        keyword_to_theme_mapping = self.get_keyword_to_theme_mapping()

        total_removed_themes = 0
        total_added_genres = 0
        total_removed_keywords = 0
        total_added_themes = 0

        with transaction.atomic():
            # Создаем savepoint для dry-run режима
            if dry_run:
                savepoint = transaction.savepoint()

            try:
                # 1. Перенос тем в жанры
                added_genres, removed_themes = self._process_themes_to_genres(
                    theme_to_genre_mapping, dry_run, batch_size, keep_old
                )
                total_added_genres += added_genres
                total_removed_themes += removed_themes

                # 2. Перенос ключевых слов в темы
                added_themes, removed_keywords = self._process_keywords_to_themes(
                    keyword_to_theme_mapping, dry_run, batch_size, keep_old
                )
                total_added_themes += added_themes
                total_removed_keywords += removed_keywords

                # Откатываем изменения в dry-run режиме
                if dry_run:
                    transaction.savepoint_rollback(savepoint)

            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Ошибка при обработке: {e}'))
                if dry_run:
                    transaction.savepoint_rollback(savepoint)
                raise

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

    def _process_themes_to_genres(self, theme_to_genre_mapping, dry_run, batch_size, keep_old):
        """Перенос тем в жанры"""
        self.stdout.write('=== ПЕРЕНОС ТЕМ В ЖАНРЫ ===')

        themes_to_process = Theme.objects.filter(
            name__in=list(theme_to_genre_mapping.keys())
        )

        # Кешируем все жанры заранее
        genres_cache = {}
        for theme_name, genre_name in theme_to_genre_mapping.items():
            genre, created = Genre.objects.get_or_create(
                name=genre_name,
                defaults={'igdb_id': -abs(hash(genre_name)) % 1000000}
            )
            genres_cache[theme_name] = genre
            if created:
                self.stdout.write(f'Создан новый жанр: "{genre_name}"')

        total_added_genres = 0
        total_removed_themes = 0

        for theme in themes_to_process:
            theme_name = theme.name
            genre = genres_cache.get(theme_name)

            if not genre:
                continue

            # Получаем ID игр с этой темой
            game_ids = list(Game.objects.filter(themes=theme).values_list('id', flat=True))

            if not game_ids:
                self.stdout.write(f'Для темы "{theme_name}" нет игр')
                continue

            total_games = len(game_ids)
            added_genres = 0
            removed_themes = 0

            self.stdout.write(f'Тема "{theme_name}" -> жанр "{genre.name}": {total_games} игр')

            # Обрабатываем батчами с bulk операциями
            for i in range(0, total_games, batch_size):
                batch_ids = game_ids[i:i + batch_size]

                if not dry_run:
                    # Bulk добавление жанров (только тех, у кого еще нет)
                    existing_genre_ids = set(
                        Game.genres.through.objects.filter(
                            game_id__in=batch_ids,
                            genre_id=genre.id
                        ).values_list('game_id', flat=True)
                    )

                    new_relations = [
                        Game.genres.through(game_id=game_id, genre_id=genre.id)
                        for game_id in batch_ids
                        if game_id not in existing_genre_ids
                    ]

                    if new_relations:
                        Game.genres.through.objects.bulk_create(
                            new_relations,
                            ignore_conflicts=True
                        )
                        added_genres += len(new_relations)

                    # Bulk удаление тем (если не keep-old)
                    if not keep_old:
                        deleted_count, _ = Game.themes.through.objects.filter(
                            game_id__in=batch_ids,
                            theme_id=theme.id
                        ).delete()
                        removed_themes += deleted_count
                else:
                    # Для dry-run просто считаем
                    added_genres += len(batch_ids)
                    if not keep_old:
                        removed_themes += len(batch_ids)

                # Обновляем прогресс-бар
                current = min(i + batch_size, total_games)
                self.print_progress_bar(
                    current, total_games,
                    prefix=f'Обработка "{theme_name[:15]}..."',
                    suffix=f'Добавлено: {added_genres}, Удалено: {removed_themes}'
                )

            print()
            self.stdout.write(self.style.SUCCESS(
                f'  Добавлено жанров: {added_genres}, Удалено тем: {removed_themes}'
            ))

            total_added_genres += added_genres
            total_removed_themes += removed_themes

        return total_added_genres, total_removed_themes

    def _process_keywords_to_themes(self, keyword_to_theme_mapping, dry_run, batch_size, keep_old):
        """Перенос ключевых слов в темы"""
        self.stdout.write('\n=== ПЕРЕНОС КЛЮЧЕВЫХ СЛОВ В ТЕМЫ ===')

        # Получаем ключевые слова по списку имен
        keywords_to_process = Keyword.objects.filter(
            name__in=list(keyword_to_theme_mapping.keys())
        )

        # Кешируем все темы заранее
        themes_cache = {}
        for keyword_name, theme_name in keyword_to_theme_mapping.items():
            theme, created = Theme.objects.get_or_create(
                name=theme_name,
                defaults={'igdb_id': -abs(hash(theme_name)) % 1000000}
            )
            themes_cache[keyword_name] = theme
            if created:
                self.stdout.write(f'Создана новая тема: "{theme_name}"')

        total_added_themes = 0
        total_removed_keywords = 0

        for keyword in keywords_to_process:
            keyword_name = keyword.name
            theme = themes_cache.get(keyword_name)

            if not theme:
                continue

            # УДАЛЯЕМ ключевые слова, даже если нет связанных игр
            if not keep_old and not dry_run:
                # Удаляем ключевое слово из базы данных
                deleted_count, _ = Keyword.objects.filter(id=keyword.id).delete()
                if deleted_count > 0:
                    total_removed_keywords += 1
                    self.stdout.write(f'Ключ.слово "{keyword_name}" удалено (нет связанных игр)')

            # Получаем ID игр с этим ключевым словом
            game_ids = list(Game.objects.filter(keywords=keyword).values_list('id', flat=True))

            if not game_ids:
                continue

            total_games = len(game_ids)
            added_themes = 0
            removed_keywords = 0

            self.stdout.write(f'Ключ.слово "{keyword_name}" -> тема "{theme.name}": {total_games} игр')

            # Обрабатываем батчами с bulk операциями
            for i in range(0, total_games, batch_size):
                batch_ids = game_ids[i:i + batch_size]

                if not dry_run:
                    # Bulk добавление тем (только тех, у кого еще нет)
                    existing_theme_ids = set(
                        Game.themes.through.objects.filter(
                            game_id__in=batch_ids,
                            theme_id=theme.id
                        ).values_list('game_id', flat=True)
                    )

                    new_relations = [
                        Game.themes.through(game_id=game_id, theme_id=theme.id)
                        for game_id in batch_ids
                        if game_id not in existing_theme_ids
                    ]

                    if new_relations:
                        Game.themes.through.objects.bulk_create(
                            new_relations,
                            ignore_conflicts=True
                        )
                        added_themes += len(new_relations)

                    # Bulk удаление ключевых слов (если не keep-old)
                    if not keep_old:
                        deleted_count, _ = Game.keywords.through.objects.filter(
                            game_id__in=batch_ids,
                            keyword_id=keyword.id
                        ).delete()
                        removed_keywords += deleted_count
                else:
                    # Для dry-run просто считаем
                    added_themes += len(batch_ids)
                    if not keep_old:
                        removed_keywords += len(batch_ids)

                # Обновляем прогресс-бар
                current = min(i + batch_size, total_games)
                self.print_progress_bar(
                    current, total_games,
                    prefix=f'Обработка "{keyword_name}"',
                    suffix=f'Добавлено: {added_themes}, Удалено: {removed_keywords}'
                )

            print()
            self.stdout.write(self.style.SUCCESS(
                f'  Добавлено тем: {added_themes}, Удалено ключ.слов: {removed_keywords}'
            ))

            total_added_themes += added_themes
            total_removed_keywords += removed_keywords

        return total_added_themes, total_removed_keywords
