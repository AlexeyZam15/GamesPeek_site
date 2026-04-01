"""
Django management command to list all genres and themes from the database.

Usage:
    python manage.py list_genres_themes
    python manage.py list_genres_themes --verbose
    python manage.py list_genres_themes --limit=20
    python manage.py list_genres_themes --format=csv
"""

from django.core.management.base import BaseCommand
from games.models_parts.simple_models import Genre, Theme


class Command(BaseCommand):
    """
    Command to display all genres and themes with their game counts.
    """

    help = 'Lists all genres and themes from the database with game counts'

    def add_arguments(self, parser):
        """
        Add command line arguments for the command.
        """
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Show detailed information including IDs and game counts',
        )

        parser.add_argument(
            '--count',
            action='store_true',
            help='Show game count in parentheses after each genre/theme name',
        )

        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Limit the number of results displayed',
        )

        parser.add_argument(
            '--format',
            type=str,
            choices=['table', 'json', 'simple', 'csv', 'comma'],
            default='table',
            help='Output format (table, json, simple, csv, or comma)',
        )

        parser.add_argument(
            '--sort',
            type=str,
            choices=['asc', 'desc', 'name'],
            default='name',
            help='Sort order for genres and themes (asc: by game count ascending, desc: by game count descending, name: by name alphabetically)',
        )

    def handle(self, *args, **options):
        """
        Main execution method for the command.
        """
        verbose = options['verbose']
        count = options['count']
        limit = options['limit']
        output_format = options['format']
        sort_order = options['sort']

        if output_format == 'json':
            self.output_json(verbose, limit, count, sort_order)
        elif output_format in ['csv', 'comma']:
            self.output_comma(verbose, limit, count, sort_order)
        else:
            self.output_table(verbose, limit, output_format, count, sort_order)

    def output_table(self, verbose, limit, output_format, show_count, sort_order):
        """
        Output genres and themes in table format.
        """
        # Получение и сортировка жанров
        genres = Genre.objects.all()

        if sort_order == 'name':
            genres = genres.order_by('name')
        elif sort_order == 'asc':
            genres = sorted(genres, key=lambda g: g.game_count)
        elif sort_order == 'desc':
            genres = sorted(genres, key=lambda g: g.game_count, reverse=True)

        if limit:
            genres = genres[:limit]

        # Жанры
        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80))
        self.stdout.write(self.style.SUCCESS('СПИСОК ВСЕХ ЖАНРОВ:'))
        self.stdout.write(self.style.SUCCESS('=' * 80))

        if not genres:
            self.stdout.write(self.style.WARNING('Жанры не найдены в базе данных.'))
        else:
            for genre in genres:
                game_count = genre.game_count if verbose else None

                if verbose:
                    self.stdout.write(
                        f"{genre.name:<35} "
                        f"(ID: {genre.igdb_id:<8}) "
                        f"Игр: {game_count:>6}"
                    )
                elif show_count:
                    self.stdout.write(f"{genre.name} ({genre.game_count})")
                else:
                    self.stdout.write(f"{genre.name}")

        # Получение и сортировка тем
        themes = Theme.objects.all()

        if sort_order == 'name':
            themes = themes.order_by('name')
        elif sort_order == 'asc':
            themes = sorted(themes, key=lambda t: t.game_set.count())
        elif sort_order == 'desc':
            themes = sorted(themes, key=lambda t: t.game_set.count(), reverse=True)

        if limit:
            themes = themes[:limit]

        # Темы
        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80))
        self.stdout.write(self.style.SUCCESS('СПИСОК ВСЕХ ТЕМ:'))
        self.stdout.write(self.style.SUCCESS('=' * 80))

        if not themes:
            self.stdout.write(self.style.WARNING('Темы не найдены в базе данных.'))
        else:
            for theme in themes:
                game_count = theme.game_set.count()

                if verbose:
                    self.stdout.write(
                        f"{theme.name:<35} "
                        f"(ID: {theme.igdb_id:<8}) "
                        f"Игр: {game_count:>6}"
                    )
                elif show_count:
                    self.stdout.write(f"{theme.name} ({game_count})")
                else:
                    self.stdout.write(f"{theme.name}")

        # Статистика
        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80))
        self.stdout.write(self.style.SUCCESS('СТАТИСТИКА:'))
        self.stdout.write(f"Всего жанров: {Genre.objects.count()}")
        self.stdout.write(f"Всего тем: {Theme.objects.count()}")
        self.stdout.write(self.style.SUCCESS('=' * 80 + '\n'))

    def output_json(self, verbose, limit, show_count, sort_order):
        """
        Output genres and themes in JSON format with each game on separate line.
        """
        import json

        # Получение и сортировка жанров
        genres_qs = Genre.objects.all()

        if sort_order == 'name':
            genres_qs = genres_qs.order_by('name')
        elif sort_order == 'asc':
            genres_qs = sorted(genres_qs, key=lambda g: g.game_count)
        elif sort_order == 'desc':
            genres_qs = sorted(genres_qs, key=lambda g: g.game_count, reverse=True)

        if limit:
            genres_qs = genres_qs[:limit]

        self.stdout.write('{')
        self.stdout.write('  "genres": [')

        genre_count = len(genres_qs) if hasattr(genres_qs, '__len__') else genres_qs.count()
        for idx, genre in enumerate(genres_qs):
            genre_data = {
                'name': genre.name,
            }

            if verbose or show_count:
                genre_data['game_count'] = genre.game_count

            genre_json = json.dumps(genre_data, ensure_ascii=False)

            if idx < genre_count - 1:
                self.stdout.write(f'    {genre_json},')
            else:
                self.stdout.write(f'    {genre_json}')

        self.stdout.write('  ],')
        self.stdout.write('  "themes": [')

        # Получение и сортировка тем
        themes_qs = Theme.objects.all()

        if sort_order == 'name':
            themes_qs = themes_qs.order_by('name')
        elif sort_order == 'asc':
            themes_qs = sorted(themes_qs, key=lambda t: t.game_set.count())
        elif sort_order == 'desc':
            themes_qs = sorted(themes_qs, key=lambda t: t.game_set.count(), reverse=True)

        if limit:
            themes_qs = themes_qs[:limit]

        theme_count = len(themes_qs) if hasattr(themes_qs, '__len__') else themes_qs.count()
        for idx, theme in enumerate(themes_qs):
            theme_data = {
                'name': theme.name,
            }

            if verbose or show_count:
                theme_data['game_count'] = theme.game_set.count()

            theme_json = json.dumps(theme_data, ensure_ascii=False)

            if idx < theme_count - 1:
                self.stdout.write(f'    {theme_json},')
            else:
                self.stdout.write(f'    {theme_json}')

        self.stdout.write('  ],')
        self.stdout.write(f'  "statistics": {{')
        self.stdout.write(f'    "total_genres": {Genre.objects.count()},')
        self.stdout.write(f'    "total_themes": {Theme.objects.count()}')
        self.stdout.write(f'  }}')
        self.stdout.write('}')

    def output_comma(self, verbose, limit, show_count, sort_order):
        """
        Output genres and themes in comma-separated format.
        """
        # Получение и сортировка жанров
        genres_qs = Genre.objects.all()

        if sort_order == 'name':
            genres_qs = genres_qs.order_by('name')
        elif sort_order == 'asc':
            genres_qs = sorted(genres_qs, key=lambda g: g.game_count)
        elif sort_order == 'desc':
            genres_qs = sorted(genres_qs, key=lambda g: g.game_count, reverse=True)

        if limit:
            genres_qs = genres_qs[:limit]

        if show_count:
            genre_names = [f"{genre.name} ({genre.game_count})" for genre in genres_qs]
        else:
            genre_names = [genre.name for genre in genres_qs]

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80))
        self.stdout.write(self.style.SUCCESS('ЖАНРЫ:'))
        self.stdout.write(', '.join(genre_names))

        if verbose:
            self.stdout.write(f"\nКоличество жанров: {len(genre_names)}")

        # Получение и сортировка тем
        themes_qs = Theme.objects.all()

        if sort_order == 'name':
            themes_qs = themes_qs.order_by('name')
        elif sort_order == 'asc':
            themes_qs = sorted(themes_qs, key=lambda t: t.game_set.count())
        elif sort_order == 'desc':
            themes_qs = sorted(themes_qs, key=lambda t: t.game_set.count(), reverse=True)

        if limit:
            themes_qs = themes_qs[:limit]

        if show_count:
            theme_names = [f"{theme.name} ({theme.game_set.count()})" for theme in themes_qs]
        else:
            theme_names = [theme.name for theme in themes_qs]

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80))
        self.stdout.write(self.style.SUCCESS('ТЕМЫ:'))
        self.stdout.write(', '.join(theme_names))

        if verbose:
            self.stdout.write(f"\nКоличество тем: {len(theme_names)}")

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 80 + '\n'))