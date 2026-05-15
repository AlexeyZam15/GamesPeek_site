from django.core.management.base import BaseCommand
from django.core.cache import cache
import hashlib
from games.models import Game


class Command(BaseCommand):
    help = 'Clear both sitemap caches (sitemap.xml and sitemap_similar_games.xml)'

    def handle(self, *args, **options):
        game_count = Game.objects.count()

        # Ключ для основного sitemap
        key_data_main = f"sitemap_main_v1_{game_count}"
        cache_key_main = hashlib.md5(key_data_main.encode()).hexdigest()

        # Ключ для sitemap_similar_games
        key_data_similar = f"sitemap_similar_v1_{game_count}"
        cache_key_similar = hashlib.md5(key_data_similar.encode()).hexdigest()

        # Удаляем оба ключа
        cache.delete(cache_key_main)
        cache.delete(cache_key_similar)

        # Также очищаем все ключи с префиксом sitemap (на всякий случай)
        cache.delete_pattern("sitemap_*")

        self.stdout.write(self.style.SUCCESS(
            f"Sitemap caches cleared:\n"
            f"   - Main sitemap key: {cache_key_main}\n"
            f"   - Similar sitemap key: {cache_key_similar}\n"
            f"   - game_count: {game_count}"
        ))