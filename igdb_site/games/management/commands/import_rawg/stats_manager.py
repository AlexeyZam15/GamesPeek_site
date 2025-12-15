# games/management/commands/import_rawg/stats_manager.py
import time
from games.models import Game
from django.db.models import Q


class StatsManager:
    """Класс для управления и отображения статистики"""

    def __init__(self, stdout, style):
        self.stdout = stdout
        self.style = style

    def show_rawg_stats(self):
        """Показывает статистику по играм"""
        try:
            total_games = Game.objects.count()
            games_with_rawg = Game.objects.filter(
                ~Q(rawg_description__isnull=True) &
                ~Q(rawg_description__exact='')
            ).count()

            games_filtered = Game.objects.filter(
                game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
            ).count()
            games_filtered_with_rawg = Game.objects.filter(
                Q(game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]) &
                ~Q(rawg_description__isnull=True) &
                ~Q(rawg_description__exact='')
            ).count()

            total_percentage = (games_with_rawg / total_games * 100) if total_games > 0 else 0
            filtered_percentage = (games_filtered_with_rawg / games_filtered * 100) if games_filtered > 0 else 0

            self.stdout.write('\n' + '📊' * 15)
            self.stdout.write('📈 СТАТИСТИКА БАЗЫ ДАННЫХ:')
            self.stdout.write(f'   Всего игр в БД: {total_games:,}')
            self.stdout.write(f'   ✅ С RAWG описанием: {games_with_rawg:,} ({total_percentage:.1f}%)')

            self.stdout.write(f'\n   🎮 Игр с типами 0,1,2,4,5,8,9,10,11: {games_filtered:,}')
            self.stdout.write(f'   ✅ С RAWG описанием: {games_filtered_with_rawg:,} ({filtered_percentage:.1f}%)')

            games_without_rawg = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            ).count()
            games_filtered_without_rawg = Game.objects.filter(
                Q(game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]) &
                (Q(rawg_description__isnull=True) | Q(rawg_description__exact=''))
            ).count()

            self.stdout.write(f'\n   ⏳ Без RAWG описания: {games_without_rawg:,}')
            self.stdout.write(f'   ⏳ Из них с типами 0,1,2,4,5,8,9,10,11: {games_filtered_without_rawg:,}')

            if games_filtered > 0:
                bar_length = 30
                filled = int(bar_length * filtered_percentage / 100)
                bar = "[" + "█" * filled + "░" * (bar_length - filled) + "]"
                self.stdout.write(f'\n   {bar} {filtered_percentage:.1f}% заполнено (основные типы)')

        except Exception as e:
            self.stdout.write(f'   ⚠️ Ошибка получения статистики БД: {e}')

    def show_api_statistics(self, rawg_client):
        """Показывает детальную статистику использования API"""
        if not rawg_client:
            self.stdout.write('\n⚠️  RAWG клиент не инициализирован')
            return

        client_stats = rawg_client.get_stats()

        self.stdout.write('\n' + '📈' * 15)
        self.stdout.write('📊 ДЕТАЛЬНАЯ СТАТИСТИКА API:')

        total_requests = client_stats['total_requests']
        cache_hits = client_stats['cache_hits']
        cache_misses = client_stats['cache_misses']
        total_cache_checks = cache_hits + cache_misses

        if total_cache_checks > 0:
            cache_efficiency = (cache_hits / total_cache_checks) * 100
        else:
            cache_efficiency = 0

        self.stdout.write(f'   🔍 Поисковых запросов: {client_stats["search_requests"]}')
        self.stdout.write(f'   📄 Запросов деталей: {client_stats["detail_requests"]}')
        self.stdout.write(f'   🎯 Всего запросов к API: {total_requests}')
        self.stdout.write(f'   💾 Попаданий в кэш: {cache_hits}')
        self.stdout.write(f'   ❌ Промахов кэша: {cache_misses}')
        self.stdout.write(f'   ⚡ Эффективность кэша: {cache_efficiency:.1f}%')
        self.stdout.write(f'   🚫 Rate limited: {client_stats["rate_limited"]}')