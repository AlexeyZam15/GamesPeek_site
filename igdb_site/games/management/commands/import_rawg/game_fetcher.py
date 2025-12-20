# FILE: game_fetcher.py
from django.db.models import Q
from games.models import Game


class GameFetcher:
    """Класс для получения игр из базы данных"""

    def __init__(self, options, not_found_ids=None):
        self.options = options
        self.not_found_ids = not_found_ids or set()
        self.batch_size = 1000

    def get_games_to_process(self, game_ids_str=None, auto_offset=False):
        """Получает игры для обработки с пагинацией"""
        if self.options.get('debug'):
            print(f"\n[DEBUG GameFetcher] ===== START get_games_to_process =====")
            print(f"[DEBUG GameFetcher] game_ids_str: {game_ids_str}")
            print(f"[DEBUG GameFetcher] auto_offset: {auto_offset}")
            print(f"[DEBUG GameFetcher] overwrite: {self.options.get('overwrite', False)}")

        if self.options.get('overwrite'):
            games_query = Game.objects.all()
        else:
            games_query = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            )

        if not self.options.get('include_all_gametypes', False):
            main_game_types = [0, 1, 2, 4, 5, 8, 9, 10, 11]
            games_query = games_query.filter(game_type__in=main_game_types)

        if game_ids_str:
            try:
                game_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                games_query = games_query.filter(igdb_id__in=game_ids)
            except ValueError:
                return []

        ignore_auto_offset = self.options.get('ignore_auto_offset', False)
        if auto_offset and self.not_found_ids and not ignore_auto_offset:
            games_query = games_query.exclude(igdb_id__in=self.not_found_ids)

        order_by = self.options.get('order_by', 'id')
        games_query = games_query.order_by(order_by)

        games_query = games_query.only('id', 'igdb_id', 'name', 'rawg_description', 'game_type')

        offset = self.options.get('offset', 0)
        limit = self.options.get('limit', 0)

        if offset > 0:
            games_query = games_query[offset:]

        if limit > 0:
            games_query = games_query[:limit]

        games_list = list(games_query)

        if self.options.get('debug'):
            print(f"[DEBUG GameFetcher] Final result: {len(games_list)} игр")
            if not games_list:
                print(f"[DEBUG GameFetcher] === DIAGNOSIS: Why empty list? ===")
            print(f"[DEBUG GameFetcher] ===== END get_games_to_process =====")

        return games_list

    def get_total_games_to_process(self, auto_offset=False):
        """Получает общее количество игр для обработки"""
        try:
            if self.options.get('overwrite'):
                count_query = Game.objects.all()
            else:
                count_query = Game.objects.filter(
                    Q(rawg_description__isnull=True) |
                    Q(rawg_description__exact='')
                )

            if not self.options.get('include_all_gametypes', False):
                count_query = count_query.filter(
                    game_type__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
                )

            if auto_offset and self.not_found_ids:
                count_query = count_query.exclude(igdb_id__in=self.not_found_ids)

            return count_query.count()

        except Exception as e:
            if self.options.get('debug'):
                print(f'⚠️ Ошибка при подсчете игр: {e}')
            return 0

    def get_games_batch(self, offset=0, batch_size=1000, auto_offset=False, game_ids_str=None):
        """Получает игры батчами для обработки"""
        current_options = self.options.copy()
        current_options['limit'] = batch_size
        current_options['offset'] = offset
        current_options['game_ids'] = game_ids_str

        game_fetcher = GameFetcher(current_options, self.not_found_ids)
        return game_fetcher.get_games_to_process(game_ids_str, auto_offset)