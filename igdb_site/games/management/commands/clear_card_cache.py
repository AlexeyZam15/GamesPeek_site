from django.core.management.base import BaseCommand
from games.models import GameCardCache


class Command(BaseCommand):
    help = 'Clear all game card caches'

    def add_arguments(self, parser):
        parser.add_argument(
            '--force',
            action='store_true',
            help='Force deletion without confirmation',
        )

    def handle(self, *args, **options):
        if not options['force']:
            self.stdout.write(
                self.style.WARNING(
                    'This will delete all cached game cards. '
                    'Type "yes" to continue: '
                )
            )
            if input().lower() != 'yes':
                self.stdout.write(self.style.SUCCESS('Operation cancelled'))
                return

        count = GameCardCache.objects.count()
        GameCardCache.objects.all().delete()

        self.stdout.write(
            self.style.SUCCESS(f'Successfully deleted {count} game cards')
        )