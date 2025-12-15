# games/management/commands/import_rawg/game_fetcher.py
from django.db.models import Q
from games.models import Game


class GameFetcher:
    """Класс для получения игр из базы данных"""

    def __init__(self, options, not_found_ids=None):
        self.options = options
        self.not_found_ids = not_found_ids or set()
        self.batch_size = 1000  # Увеличиваем размер батча для базы данных

    def get_games_to_process(self, game_ids_str=None, auto_offset=False):
        """Получает игры для обработки с пагинацией"""
        # Базовый запрос - используем только нужные поля
        if self.options.get('overwrite'):
            games_query = Game.objects.all()
        else:
            games_query = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            )

        # Фильтр по типам игр - используем exists для оптимизации
        if not self.options.get('include_all_gametypes', False):
            games_query = games_query.filter(
                game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
            )

        # Фильтр по конкретным ID
        if game_ids_str:
            try:
                game_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                games_query = games_query.filter(igdb_id__in=game_ids)
            except ValueError:
                return []  # Возвращаем пустой список при ошибке

        # Auto-offset фильтр - используем EXISTS вместо IN для больших списков
        if auto_offset and self.not_found_ids:
            games_query = games_query.exclude(igdb_id__in=self.not_found_ids)

        # Сортировка
        order_by = self.options.get('order_by', 'id')
        games_query = games_query.order_by(order_by)

        # Выбираем только нужные поля
        games_query = games_query.only('id', 'igdb_id', 'name', 'rawg_description', 'game_type_id')

        # Применяем offset и limit из опций
        offset = self.options.get('offset', 0)
        limit = self.options.get('limit', 0)

        # Отладочный вывод
        if self.options.get('debug'):
            print(f"[DEBUG] get_games_to_process: offset={offset}, limit={limit}, auto_offset={auto_offset}")

        # Применяем offset
        if offset > 0:
            games_query = games_query[offset:]

        # Применяем limit
        if limit > 0:
            games_query = games_query[:limit]

        # Возвращаем список
        games_list = list(games_query)

        if self.options.get('debug'):
            print(f"[DEBUG] Возвращаем {len(games_list)} игр")
            if games_list:
                print(f"[DEBUG] Примеры: {games_list[0].name} ... {games_list[-1].name}")

        return games_list

    def get_games_iterator(self, game_ids_str=None, auto_offset=False):
        """Возвращает итератор для обработки больших объемов данных"""
        games_query = self._build_base_query(game_ids_str, auto_offset)
        games_query = games_query.only('id', 'igdb_id', 'name', 'rawg_description', 'game_type_id')
        return games_query.iterator(chunk_size=self.batch_size)

    def _build_base_query(self, game_ids_str=None, auto_offset=False):
        """Строит базовый запрос"""
        if self.options.get('overwrite'):
            games_query = Game.objects.all()
        else:
            games_query = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            )

        if not self.options.get('include_all_gametypes', False):
            games_query = games_query.filter(
                game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
            )

        if game_ids_str:
            try:
                game_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                games_query = games_query.filter(igdb_id__in=game_ids)
            except ValueError:
                return Game.objects.none()

        if auto_offset and self.not_found_ids:
            games_query = games_query.exclude(igdb_id__in=self.not_found_ids)

        order_by = self.options.get('order_by', 'id')
        return games_query.order_by(order_by)

    def get_total_games_to_process(self, auto_offset=False):
        """Получает общее количество игр для обработки с оптимизацией"""
        try:
            # Используем count() с теми же фильтрами
            if self.options.get('overwrite'):
                count_query = Game.objects.all()
            else:
                count_query = Game.objects.filter(
                    Q(rawg_description__isnull=True) |
                    Q(rawg_description__exact='')
                )

            if not self.options.get('include_all_gametypes', False):
                count_query = count_query.filter(
                    game_type__igdb_id__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
                )

            if auto_offset and self.not_found_ids:
                count_query = count_query.exclude(igdb_id__in=self.not_found_ids)

            # Используем count() с distinct если нужно
            return count_query.count()

        except Exception as e:
            if self.options.get('debug'):
                print(f'⚠️ Ошибка при подсчете игр: {e}')
            return 0

    def get_games_batch(self, offset=0, batch_size=1000, auto_offset=False):
        """Получает игры батчами для обработки с учетом всех фильтров"""
        # Строим базовый запрос
        query = self._build_base_query(None, auto_offset)

        # Применяем сортировку
        order_by = self.options.get('order_by', 'id')
        if order_by.startswith('-'):
            order_field = order_by[1:]
            query = query.order_by(f'-{order_field}')
        else:
            query = query.order_by(order_by)

        # Применяем offset и limit
        query = query[offset:offset + batch_size]

        # Выбираем только нужные поля
        query = query.only('id', 'igdb_id', 'name', 'rawg_description', 'game_type_id')

        return list(query)
