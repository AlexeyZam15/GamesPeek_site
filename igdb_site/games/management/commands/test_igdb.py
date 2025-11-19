from django.core.management.base import BaseCommand
from games.igdb_api import make_igdb_request


class Command(BaseCommand):
    help = 'Test IGDB API connection'

    def handle(self, *args, **options):
        self.stdout.write("🧪 Testing IGDB API connection...")

        # Простой запрос для теста
        data = "fields name; limit 1;"

        try:
            result = make_igdb_request('games', data)
            self.stdout.write(
                self.style.SUCCESS(f"✅ SUCCESS! Connected to IGDB API")
            )
            self.stdout.write(f"First game: {result[0]['name']}")
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ FAILED: {e}")
            )