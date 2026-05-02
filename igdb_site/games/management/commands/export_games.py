"""
Django management command to export games with their characteristics.
Each file contains 1000 games in JSON format (one game per line).
Exports all games by default, or only games without rawg_description when specified.
Sorted by igdb_id.
Cleans output folder before each run.
"""

from django.core.management.base import BaseCommand
import json
import shutil
from pathlib import Path


class Command(BaseCommand):
    help = 'Export games to JSON files (1000 games per file, one game per line)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--output-dir',
            type=str,
            required=True,
            help='Output directory path for JSON files'
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit number of games to export'
        )
        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Offset for pagination'
        )
        parser.add_argument(
            '--year',
            type=int,
            help='Filter by release year'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=500,
            help='Batch size for database queries (default: 500)'
        )
        parser.add_argument(
            '--games-per-file',
            type=int,
            default=1000,
            help='Number of games per file (default: 1000)'
        )
        parser.add_argument(
            '--no-clean',
            action='store_true',
            help='Do not clean output directory before export'
        )
        parser.add_argument(
            '--only-without-rawg',
            action='store_true',
            help='Export only games without rawg_description (NULL or empty string)'
        )
        parser.add_argument(
            '--with-descriptions',
            action='store_true',
            help='Include all description fields (rawg_description, wiki_description, summary, storyline) in the exported data'
        )
        parser.add_argument(
            '--with-keywords',
            action='store_true',
            help='Include all keywords in the exported data'
        )
        parser.add_argument(
            '--game-name',
            type=str,
            nargs='+',
            help='Export exact games by name (case-sensitive exact match). Can specify multiple names separated by spaces'
        )
        parser.add_argument(
            '--simple-list',
            action='store_true',
            help='Export only game names with years in one line separated by commas'
        )
        parser.add_argument(
            '--minimal-json',
            action='store_true',
            help='Export only id, name, and year in JSON format (one game per line)'
        )
        parser.add_argument(
            '--genre',
            type=str,
            help='Filter by genre name (case-insensitive, matches genre name)'
        )

    def _build_queryset(self, options):
        """Build and return the base queryset with filters applied"""
        from games.models_parts.game import Game
        from games.models_parts.simple_models import Genre
        from django.db.models import Q
        from django.utils import timezone

        # Базовый queryset: по умолчанию все игры, сортируем по igdb_id
        queryset = Game.objects.all().order_by('igdb_id')

        # Для simple_list или экспорта только id/name/year не нужны prefetch_related
        if not options.get('simple_list', False) and not options.get('minimal_json', False):
            queryset = queryset.prefetch_related(
                'genres', 'themes', 'platforms', 'publishers', 'developers'
            )

            # Если нужны ключевые слова, используем keyword_ids (без prefetch)
            if options.get('with_keywords', False):
                # keywords теперь property, не нужно prefetch
                pass

        # Фильтрация по точному названию игры (поддержка нескольких названий)
        if options['game_name']:
            game_names = options['game_name']
            queryset = queryset.filter(name__in=game_names)

            found_count = queryset.count()
            if found_count == 0:
                self.stdout.write(self.style.WARNING(f"No games found with exact names: {', '.join(game_names)}"))
                return None
            else:
                found_names = list(queryset.values_list('name', flat=True).distinct())
                missing_names = set(game_names) - set(found_names)

                if missing_names:
                    self.stdout.write(self.style.WARNING(f"Games not found: {', '.join(missing_names)}"))

                if found_names:
                    self.stdout.write(
                        self.style.SUCCESS(f"Found {found_count} game(s) matching: {', '.join(found_names)}"))

        # Фильтрация по жанру (по имени жанра, регистронезависимо)
        if options.get('genre'):
            genre_name = options['genre'].strip()
            try:
                genre = Genre.objects.get(name__iexact=genre_name)
                queryset = queryset.filter(genres=genre)
                self.stdout.write(self.style.SUCCESS(f"Filtering by genre: '{genre.name}'"))
            except Genre.DoesNotExist:
                available_genres = Genre.objects.values_list('name', flat=True).order_by('name')[:10]
                self.stdout.write(self.style.ERROR(f"Genre '{genre_name}' not found"))
                self.stdout.write(f"Available genres (first 10): {', '.join(available_genres)}")
                return None
            except Genre.MultipleObjectsReturned:
                genre = Genre.objects.filter(name__iexact=genre_name).first()
                queryset = queryset.filter(genres=genre)
                self.stdout.write(self.style.WARNING(f"Multiple genres matched, using first: '{genre.name}'"))

        # Фильтрация по наличию rawg_description
        if options['only_without_rawg'] and not options.get('simple_list', False) and not options.get('minimal_json',
                                                                                                      False):
            queryset = queryset.filter(
                Q(rawg_description__isnull=True) | Q(rawg_description='')
            )

        # Фильтрация по году
        if options['year']:
            from django.utils import timezone
            start_date = timezone.datetime(options['year'], 1, 1)
            end_date = timezone.datetime(options['year'] + 1, 1, 1)
            queryset = queryset.filter(first_release_date__gte=start_date,
                                       first_release_date__lt=end_date)

        # Пагинация
        if options['offset']:
            queryset = queryset[options['offset']:]

        if options['limit']:
            queryset = queryset[:options['limit']]

        return queryset

    def _export_simple_list(self, queryset, options):
        """Export games as comma-separated lists of names with years, split across multiple files"""
        from tqdm import tqdm

        total_count = queryset.count()
        games_per_file = options['games_per_file']

        if total_count == 0:
            self.stdout.write(self.style.WARNING("No games found"))
            return

        self.stdout.write(f"Found {total_count} games, generating simple lists...")
        self.stdout.write(f"Will create files with {games_per_file} games each")

        # Используем values_list для быстрого получения только нужных полей
        # Используем iterator() для экономии памяти при большом количестве записей
        game_data = queryset.values_list('name', 'first_release_date').iterator(chunk_size=2000)

        file_number = 1
        current_batch = []
        total_processed = 0

        # Прогресс-бар для обработки игр
        progress_bar = tqdm(total=total_count, desc="Processing games", unit="games")

        for name, release_date in game_data:
            if release_date:
                year_str = f" ({release_date.year})"
            else:
                year_str = " (Year unknown)"
            current_batch.append(f'"{name}{year_str}"')
            total_processed += 1
            progress_bar.update(1)

            # Если набрали нужное количество игр или это последняя игра
            if len(current_batch) >= games_per_file:
                # Записываем текущую партию в файл
                output_file = Path(options['output_dir']) / f"games_list_part_{file_number:04d}.txt"
                output_file.parent.mkdir(parents=True, exist_ok=True)

                with open(output_file, 'w', encoding='utf-8') as f:
                    f.write(', '.join(current_batch))

                self.stdout.write(f"\nCreated {output_file.name} with {len(current_batch)} games")

                current_batch = []
                file_number += 1

        progress_bar.close()

        # Сохраняем последнюю партию если остались игры
        if current_batch:
            output_file = Path(options['output_dir']) / f"games_list_part_{file_number:04d}.txt"
            output_file.parent.mkdir(parents=True, exist_ok=True)

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(', '.join(current_batch))

            self.stdout.write(f"\nCreated {output_file.name} with {len(current_batch)} games")

        self.stdout.write(self.style.SUCCESS(
            f"Successfully exported {total_processed} games to {options['output_dir']} in {file_number} files"
        ))

    def _export_json_files(self, queryset, options):
        """Export games to JSON files with configurable fields (1000 games per file)"""
        from tqdm import tqdm
        import json

        total_count = queryset.count()
        games_per_file = options['games_per_file']
        output_dir = Path(options['output_dir'])

        include_descriptions = options.get('with_descriptions', False)
        include_keywords = options.get('with_keywords', False)

        self.stdout.write(f"Found {total_count} games")
        self.stdout.write(f"Sorting by igdb_id (ascending)")
        self.stdout.write(f"Will create files with {games_per_file} games each")

        if include_descriptions:
            self.stdout.write("Export mode: including description fields")
        else:
            self.stdout.write("Export mode: only id, name, and year")

        if include_keywords:
            self.stdout.write("Export mode: including keywords")

        if total_count == 0:
            self.stdout.write(self.style.WARNING("No games found"))
            return

        exported_count = 0
        file_number = 1
        current_batch = []
        batch_size = options['batch_size']

        progress_bar = tqdm(total=total_count, desc="Exporting games", unit="games")

        for offset in range(0, total_count, batch_size):
            if include_descriptions or include_keywords:
                batch = queryset[offset:offset + batch_size]
            else:
                batch = queryset[offset:offset + batch_size].values_list('igdb_id', 'name', 'first_release_date')

            for game in batch.iterator(chunk_size=200):
                if include_descriptions or include_keywords:
                    igdb_id = game.igdb_id
                    name = game.name
                    release_date = game.first_release_date

                    game_data = {
                        'id': igdb_id,
                        'name': name,
                        'year': release_date.year if release_date else None
                    }

                    if include_descriptions:
                        game_data['rawg_description'] = game.rawg_description
                        game_data['wiki_description'] = game.wiki_description
                        game_data['summary'] = game.summary
                        game_data['storyline'] = game.storyline

                    if include_keywords:
                        # Используем keywords property
                        keywords_list = list(game.keywords.values_list('name', flat=True))
                        game_data['keywords'] = keywords_list
                else:
                    igdb_id, name, release_date = game
                    game_data = {
                        'id': igdb_id,
                        'name': name,
                        'year': release_date.year if release_date else None
                    }

                current_batch.append(game_data)
                exported_count += 1
                progress_bar.update(1)

                if len(current_batch) >= games_per_file:
                    filename = f"games_part_{file_number:04d}.json"
                    filepath = output_dir / filename

                    with open(filepath, 'w', encoding='utf-8') as f:
                        for game_item in current_batch:
                            f.write(json.dumps(game_item, ensure_ascii=False, separators=(',', ':')))
                            f.write('\n')

                    self.stdout.write(f"\nCreated {filename} with {len(current_batch)} games")
                    current_batch = []
                    file_number += 1

        progress_bar.close()

        if current_batch:
            filename = f"games_part_{file_number:04d}.json"
            filepath = output_dir / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                for game_item in current_batch:
                    f.write(json.dumps(game_item, ensure_ascii=False, separators=(',', ':')))
                    f.write('\n')

            self.stdout.write(f"\nCreated {filename} with {len(current_batch)} games")

        self.stdout.write(self.style.SUCCESS(
            f"Successfully exported {exported_count} games to {output_dir} in {file_number} files"
        ))

    def handle(self, *args, **options):
        # Построение queryset
        queryset = self._build_queryset(options)

        if queryset is None:
            return

        # Проверка на пустой результат
        if queryset.count() == 0:
            self.stdout.write(self.style.WARNING("No games found"))
            return

        # Выбор режима экспорта
        if options.get('simple_list', False):
            self._export_simple_list(queryset, options)
        elif options.get('minimal_json', False):
            # Создаем выходную директорию
            output_dir = Path(options['output_dir'])

            # Очищаем папку перед запуском, если не указан флаг --no-clean
            if not options['no_clean'] and output_dir.exists():
                self.stdout.write(f"Cleaning output directory: {output_dir}")
                shutil.rmtree(output_dir)

            output_dir.mkdir(parents=True, exist_ok=True)

            self._export_json_files(queryset, options)
        else:
            # Создаем выходную директорию
            output_dir = Path(options['output_dir'])

            # Очищаем папку перед запуском, если не указан флаг --no-clean
            if not options['no_clean'] and output_dir.exists():
                self.stdout.write(f"Cleaning output directory: {output_dir}")
                shutil.rmtree(output_dir)

            output_dir.mkdir(parents=True, exist_ok=True)

            # Выводим информацию о режиме экспорта
            if options.get('with_descriptions', False):
                self.stdout.write("Export mode: including all descriptions")
            else:
                self.stdout.write("Export mode: without descriptions (only metadata)")

            if options.get('only_without_rawg', False):
                self.stdout.write("Filtering: only games without rawg_description")
            else:
                self.stdout.write("Exporting all games (including those with rawg_description)")

            if options.get('with_keywords', False):
                self.stdout.write("Export mode: including keywords")

            self._export_json_files(queryset, options)