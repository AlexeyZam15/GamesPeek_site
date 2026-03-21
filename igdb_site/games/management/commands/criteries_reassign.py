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

    def _process_themes_to_genres(self, theme_to_genre_mapping, dry_run, batch_size, keep_old):
        """Перенос тем в жанры"""
        self.stdout.write('=== ПЕРЕНОС ТЕМ В ЖАНРЫ ===')

        # Приводим маппинг к нижнему регистру для регистронезависимого сравнения
        mapping_lower = {k.lower(): v for k, v in theme_to_genre_mapping.items()}

        # Получаем все темы и фильтруем по нижнему регистру
        all_themes = Theme.objects.all()
        themes_to_process = []
        for theme in all_themes:
            if theme.name.lower() in mapping_lower:
                themes_to_process.append(theme)

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
            theme_name_lower = theme.name.lower()
            # Находим оригинальный ключ в маппинге
            original_key = None
            for key in theme_to_genre_mapping.keys():
                if key.lower() == theme_name_lower:
                    original_key = key
                    break

            if not original_key:
                continue

            genre = genres_cache.get(original_key)

            if not genre:
                continue

            # Получаем ID игр с этой темой
            game_ids = list(Game.objects.filter(themes=theme).values_list('id', flat=True))

            if not game_ids:
                self.stdout.write(f'Для темы "{theme.name}" нет игр')
                continue

            total_games = len(game_ids)
            added_genres = 0
            removed_themes = 0

            self.stdout.write(f'Тема "{theme.name}" -> жанр "{genre.name}": {total_games} игр')

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
                    prefix=f'Обработка "{theme.name[:15]}..."',
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

        # Приводим маппинг к нижнему регистру для регистронезависимого сравнения
        mapping_lower = {k.lower(): v for k, v in keyword_to_theme_mapping.items()}

        # Получаем все ключевые слова и фильтруем по нижнему регистру
        all_keywords = Keyword.objects.all()
        keywords_to_process = []
        for keyword in all_keywords:
            if keyword.name.lower() in mapping_lower:
                keywords_to_process.append(keyword)

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
            keyword_name_lower = keyword.name.lower()
            # Находим оригинальный ключ в маппинге
            original_key = None
            for key in keyword_to_theme_mapping.keys():
                if key.lower() == keyword_name_lower:
                    original_key = key
                    break

            if not original_key:
                continue

            theme = themes_cache.get(original_key)

            if not theme:
                continue

            # Получаем ID игр с этим ключевым словом
            game_ids = list(Game.objects.filter(keywords=keyword).values_list('id', flat=True))

            if not game_ids:
                continue

            total_games = len(game_ids)
            added_themes = 0
            removed_keywords = 0

            self.stdout.write(f'Ключ.слово "{keyword.name}" -> тема "{theme.name}": {total_games} игр')

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

                    # Ключевые слова НЕ УДАЛЯЕМ, даже если keep_old = False
                    # Удаление убрано по требованию
                else:
                    # Для dry-run просто считаем
                    added_themes += len(batch_ids)

                # Обновляем прогресс-бар
                current = min(i + batch_size, total_games)
                self.print_progress_bar(
                    current, total_games,
                    prefix=f'Обработка "{keyword.name}"',
                    suffix=f'Добавлено: {added_themes}'
                )

            print()
            self.stdout.write(self.style.SUCCESS(
                f'  Добавлено тем: {added_themes}, Ключ.слова сохранены'
            ))

            total_added_themes += added_themes

        return total_added_themes, total_removed_keywords

    def _process_themes_to_keywords(self, theme_to_keyword_mapping, dry_run, batch_size, keep_old):
        """Перенос тем в ключевые слова"""
        self.stdout.write('\n=== ПЕРЕНОС ТЕМ В КЛЮЧЕВЫЕ СЛОВА ===')

        # Приводим маппинг к нижнему регистру для регистронезависимого сравнения
        mapping_lower = {k.lower(): v for k, v in theme_to_keyword_mapping.items()}

        # Получаем все темы и фильтруем по нижнему регистру
        all_themes = Theme.objects.all()
        themes_to_process = []
        for theme in all_themes:
            if theme.name.lower() in mapping_lower:
                themes_to_process.append(theme)

        keywords_cache = {}
        for theme_name, keyword_name in theme_to_keyword_mapping.items():
            keyword, created = Keyword.objects.get_or_create(
                name=keyword_name,
                defaults={'igdb_id': -abs(hash(keyword_name)) % 1000000}
            )
            keywords_cache[theme_name] = keyword
            if created:
                self.stdout.write(f'Создано новое ключ.слово: "{keyword_name}"')

        total_added_keywords = 0
        total_removed_themes = 0

        for theme in themes_to_process:
            theme_name_lower = theme.name.lower()
            # Находим оригинальный ключ в маппинге
            original_key = None
            for key in theme_to_keyword_mapping.keys():
                if key.lower() == theme_name_lower:
                    original_key = key
                    break

            if not original_key:
                continue

            keyword = keywords_cache.get(original_key)

            if not keyword:
                continue

            game_ids = list(Game.objects.filter(themes=theme).values_list('id', flat=True))

            if not game_ids:
                self.stdout.write(f'Для темы "{theme.name}" нет игр')

                if not keep_old and not dry_run:
                    deleted_count, _ = Theme.objects.filter(id=theme.id).delete()
                    if deleted_count > 0:
                        total_removed_themes += 1
                        self.stdout.write(f'Тема "{theme.name}" удалена (нет связанных игр)')
                continue

            total_games = len(game_ids)
            added_keywords = 0
            removed_themes = 0

            self.stdout.write(f'Тема "{theme.name}" -> ключ.слово "{keyword.name}": {total_games} игр')

            for i in range(0, total_games, batch_size):
                batch_ids = game_ids[i:i + batch_size]

                if not dry_run:
                    existing_keyword_ids = set(
                        Game.keywords.through.objects.filter(
                            game_id__in=batch_ids,
                            keyword_id=keyword.id
                        ).values_list('game_id', flat=True)
                    )

                    new_relations = [
                        Game.keywords.through(game_id=game_id, keyword_id=keyword.id)
                        for game_id in batch_ids
                        if game_id not in existing_keyword_ids
                    ]

                    if new_relations:
                        Game.keywords.through.objects.bulk_create(
                            new_relations,
                            ignore_conflicts=True
                        )
                        added_keywords += len(new_relations)

                    if not keep_old:
                        deleted_count, _ = Game.themes.through.objects.filter(
                            game_id__in=batch_ids,
                            theme_id=theme.id
                        ).delete()
                        removed_themes += deleted_count

                        if not Game.objects.filter(themes=theme).exists():
                            theme.delete()
                            self.stdout.write(f'Тема "{theme.name}" полностью удалена')
                else:
                    added_keywords += len(batch_ids)
                    if not keep_old:
                        removed_themes += len(batch_ids)

                current = min(i + batch_size, total_games)
                self.print_progress_bar(
                    current, total_games,
                    prefix=f'Обработка "{theme.name}"',
                    suffix=f'Добавлено: {added_keywords}, Удалено: {removed_themes}'
                )

            print()
            self.stdout.write(self.style.SUCCESS(
                f'  Добавлено ключ.слов: {added_keywords}, Удалено тем: {removed_themes}'
            ))

            total_added_keywords += added_keywords
            total_removed_themes += removed_themes

        return total_added_keywords, total_removed_themes

    def get_theme_to_genre_mapping(self):
        """Возвращает маппинг тем в жанры (регистронезависимый)"""
        return {
            'action': 'Action',
            'open world': 'Open World',
            'sandbox': 'Sandbox',
            'survival': 'Survival',
            'real-time combat': 'Real-time Combat',
            'base building': 'Base Building',
            'simulator': 'Simulator',
            'squad management': 'Squad Management',
        }

    def get_keyword_to_theme_mapping(self):
        """Возвращает маппинг ключевых слов в темы (регистронезависимый)"""
        return {
            'gothic': 'Gothic',
            'medieval': 'Medieval',
            'fire emblem': 'Fire Emblem',
        }

    def get_theme_to_keyword_mapping(self):
        """Возвращает маппинг тем в ключевые слова (регистронезависимый)"""
        return {
            # 'fire emblem': 'Fire Emblem',  # Удалено - теперь переносится из ключевых слов в темы
        }

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

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        keep_old = options['keep_old']

        theme_to_genre_mapping = self.get_theme_to_genre_mapping()
        keyword_to_theme_mapping = self.get_keyword_to_theme_mapping()
        theme_to_keyword_mapping = self.get_theme_to_keyword_mapping()

        total_removed_themes = 0
        total_added_genres = 0
        total_removed_keywords = 0
        total_added_themes = 0
        total_removed_themes_to_keywords = 0
        total_added_keywords = 0

        with transaction.atomic():
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

                # 3. Перенос тем в ключевые слова (НОВЫЙ ФУНКЦИОНАЛ)
                added_keywords, removed_themes_to_keywords = self._process_themes_to_keywords(
                    theme_to_keyword_mapping, dry_run, batch_size, keep_old
                )
                total_added_keywords += added_keywords
                total_removed_themes_to_keywords += removed_themes_to_keywords

                if dry_run:
                    transaction.savepoint_rollback(savepoint)

            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Ошибка при обработке: {e}'))
                if dry_run:
                    transaction.savepoint_rollback(savepoint)
                raise

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
        self.stdout.write(self.style.SUCCESS(
            f'  • Темы -> ключ.слова: добавлено {total_added_keywords}, удалено {total_removed_themes_to_keywords}'))

        if keep_old:
            self.stdout.write(self.style.WARNING('СТАРЫЕ КРИТЕРИИ СОХРАНЕНЫ (опция --keep-old)'))