# management/commands/replace_keyword_names.py
from django.core.management.base import BaseCommand
from django.utils.text import Truncator
from games.models import Keyword
from games.igdb_api import make_igdb_request


class Command(BaseCommand):
    help = 'Replace keyword names containing specific text with names from IGDB by ID'

    def add_arguments(self, parser):
        parser.add_argument(
            '--keyword',
            type=str,
            help='Specific keyword text to search for (e.g., "Keyword")',
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be changed without actually making changes',
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=10,
            help='Batch size for IGDB requests (default: 10)',
        )

    def handle(self, *args, **options):
        keyword_text = options['keyword']
        dry_run = options['dry_run']
        batch_size = options['batch_size']

        if not keyword_text:
            self.stdout.write(
                self.style.ERROR('Please specify --keyword option')
            )
            return

        # Находим ключевые слова, содержащие указанный текст в названии
        keywords_to_fix = Keyword.objects.filter(name__icontains=keyword_text)
        total_count = keywords_to_fix.count()

        self.stdout.write(
            self.style.WARNING(
                f'Found {total_count} keywords containing "{keyword_text}" in name'
            )
        )
        self.stdout.write(f'Using batch size: {batch_size}')

        if dry_run:
            self.stdout.write(
                self.style.WARNING('DRY RUN MODE - No changes will be made')
            )

        if not keywords_to_fix.exists():
            self.stdout.write(
                self.style.WARNING('No keywords found with the specified text')
            )
            return

        # Разбиваем на батчи
        keyword_ids = list(keywords_to_fix.values_list('igdb_id', flat=True))
        id_to_keyword = {kw.igdb_id: kw for kw in keywords_to_fix}

        updated_count = 0
        error_count = 0
        not_found_count = 0

        # Обрабатываем батчами
        for i in range(0, len(keyword_ids), batch_size):
            batch_ids = keyword_ids[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(keyword_ids) + batch_size - 1) // batch_size

            self.stdout.write(
                f'\nProcessing batch {batch_num}/{total_batches} ({len(batch_ids)} keywords)...'
            )

            try:
                # Один запрос для всего батча
                igdb_data = make_igdb_request(
                    'keywords',
                    f'fields name; where id = ({",".join(map(str, batch_ids))});',
                    debug=False
                )

                # Создаем маппинг ID -> название из IGDB
                igdb_mapping = {item['id']: item['name'] for item in igdb_data}

                # Обрабатываем каждый ключ в батче
                for keyword_id in batch_ids:
                    keyword = id_to_keyword[keyword_id]

                    if keyword_id in igdb_mapping:
                        new_name = igdb_mapping[keyword_id]
                        old_name = keyword.name

                        # Показываем прогресс для текущего батча
                        current_in_batch = batch_ids.index(keyword_id) + 1
                        progress = f"[{current_in_batch}/{len(batch_ids)}]"

                        self.stdout.write(
                            f'{progress} ID {keyword_id}: "{Truncator(old_name).chars(50)}" -> "{Truncator(new_name).chars(50)}"'
                        )

                        if not dry_run:
                            keyword.name = new_name
                            keyword.save()
                            updated_count += 1
                    else:
                        self.stdout.write(
                            self.style.ERROR(
                                f'[?] No data from IGDB for keyword ID {keyword_id}'
                            )
                        )
                        not_found_count += 1

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(
                        f'Error processing batch {batch_num}: {str(e)}'
                    )
                )
                error_count += len(batch_ids)

        # Итоговый отчет
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write(
            self.style.SUCCESS('PROCESSING COMPLETED')
        )
        self.stdout.write(f'Total keywords processed: {total_count}')

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f'DRY RUN: Would update {updated_count} keywords'
                )
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f'UPDATED: {updated_count} keywords'
                )
            )

        self.stdout.write(
            self.style.ERROR(f'Not found in IGDB: {not_found_count} keywords')
        )
        self.stdout.write(
            self.style.ERROR(f'Errors: {error_count} keywords')
        )

        if dry_run:
            self.stdout.write(
                self.style.WARNING('\nRun without --dry-run to apply changes')
            )