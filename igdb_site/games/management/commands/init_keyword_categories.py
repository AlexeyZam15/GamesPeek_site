from django.core.management.base import BaseCommand
from games.models import KeywordCategory


class Command(BaseCommand):
    help = 'Initialize default keyword categories'

    def handle(self, *args, **options):
        categories = [
            {
                'name': 'Gameplay',
                'description': 'Keywords related to game mechanics, gameplay features, and player actions'
            },
            {
                'name': 'Setting',
                'description': 'Keywords related to game world, environment, time period, and location'
            },
            {
                'name': 'Narrative',
                'description': 'Keywords related to story, plot, and narrative elements'
            },
            {
                'name': 'Visual Style',
                'description': 'Keywords related to graphics, art direction, and visual presentation'
            },
            {
                'name': 'Audio',
                'description': 'Keywords related to sound, music, and audio design'
            },
            {
                'name': 'Miscellaneous',
                'description': 'Other keywords that don\'t fit main categories'
            }
        ]

        for cat_data in categories:
            category, created = KeywordCategory.objects.get_or_create(
                name=cat_data['name'],
                defaults={'description': cat_data['description']}
            )
            if created:
                self.stdout.write(
                    self.style.SUCCESS(f'Created category: {category.name}')
                )
            else:
                self.stdout.write(
                    self.style.WARNING(f'Category already exists: {category.name}')
                )