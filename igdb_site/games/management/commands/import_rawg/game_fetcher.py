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
        # Отладочный вывод
        if self.options.get('debug'):
            print(f"\n[DEBUG GameFetcher] ===== START get_games_to_process =====")
            print(f"[DEBUG GameFetcher] game_ids_str: {game_ids_str}")
            print(f"[DEBUG GameFetcher] auto_offset: {auto_offset}")
            print(f"[DEBUG GameFetcher] overwrite: {self.options.get('overwrite', False)}")
            print(f"[DEBUG GameFetcher] include_all_gametypes: {self.options.get('include_all_gametypes', False)}")
            print(f"[DEBUG GameFetcher] not_found_ids count: {len(self.not_found_ids) if self.not_found_ids else 0}")
            if self.not_found_ids:
                print(f"[DEBUG GameFetcher] Sample not_found_ids: {list(self.not_found_ids)[:5]}")

        # Базовый запрос - используем только нужные поля
        if self.options.get('overwrite'):
            games_query = Game.objects.all()
            if self.options.get('debug'):
                print(f"[DEBUG GameFetcher] Overwrite mode: ВСЕ игры")
        else:
            games_query = Game.objects.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            )
            if self.options.get('debug'):
                count_no_desc = games_query.count()
                print(f"[DEBUG GameFetcher] Normal mode: {count_no_desc} игр без описания")

        # Фильтр по типам игр
        if not self.options.get('include_all_gametypes', False):
            main_game_types = [0, 1, 2, 4, 5, 8, 9, 10, 11]
            games_query = games_query.filter(game_type__in=main_game_types)
            if self.options.get('debug'):
                count_filtered = games_query.count()
                print(f"[DEBUG GameFetcher] After game_type filter: {count_filtered} игр")

        # Фильтр по конкретным ID
        if game_ids_str:
            try:
                game_ids = [int(id.strip()) for id in game_ids_str.split(',')]

                if self.options.get('debug'):
                    print(f"[DEBUG GameFetcher] Requested game_ids: {game_ids}")

                    # Проверяем существование игр
                    for gid in game_ids:
                        game_obj = Game.objects.filter(igdb_id=gid).first()
                        if game_obj:
                            print(f"[DEBUG GameFetcher] Game {gid}: '{game_obj.name}' exists")
                            print(f"    - rawg_description: {'SET' if game_obj.rawg_description else 'EMPTY'}")
                            print(f"    - game_type: {game_obj.game_type}")
                            print(f"    - In not_found_ids: {gid in self.not_found_ids}")
                        else:
                            print(f"[DEBUG GameFetcher] Game {gid}: NOT FOUND in database")

                games_query = games_query.filter(igdb_id__in=game_ids)

                if self.options.get('debug'):
                    count_after_id_filter = games_query.count()
                    print(f"[DEBUG GameFetcher] After igdb_id filter: {count_after_id_filter} игр")

            except ValueError as e:
                if self.options.get('debug'):
                    print(f"[DEBUG GameFetcher] ValueError parsing game_ids: {e}")
                return []

        # Auto-offset фильтр
        ignore_auto_offset = self.options.get('ignore_auto_offset', False)
        if auto_offset and self.not_found_ids and not ignore_auto_offset:
            if self.options.get('debug'):
                print(f"[DEBUG GameFetcher] Applying auto_offset (exclude {len(self.not_found_ids)} games)")
                # Проверяем какие запрошенные игры будут исключены
                if game_ids_str:
                    requested_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                    excluded_ids = set(requested_ids).intersection(self.not_found_ids)
                    if excluded_ids:
                        print(f"[DEBUG GameFetcher] Will be excluded: {excluded_ids}")
                        for gid in excluded_ids:
                            game = Game.objects.filter(igdb_id=gid).first()
                            if game:
                                print(f"    - {gid}: '{game.name}'")

            games_query = games_query.exclude(igdb_id__in=self.not_found_ids)
        elif self.options.get('debug'):
            if ignore_auto_offset:
                print(f"[DEBUG GameFetcher] ignore_auto_offset=True - skipping auto-offset")
            elif not auto_offset:
                print(f"[DEBUG GameFetcher] auto_offset=False - skipping auto-offset")
            elif not self.not_found_ids:
                print(f"[DEBUG GameFetcher] not_found_ids is empty - skipping auto-offset")

        # Сортировка
        order_by = self.options.get('order_by', 'id')
        games_query = games_query.order_by(order_by)

        if self.options.get('debug'):
            count_before_limit = games_query.count()
            print(f"[DEBUG GameFetcher] Before limit/offset: {count_before_limit} игр")

        # Выбираем только нужные поля
        games_query = games_query.only('id', 'igdb_id', 'name', 'rawg_description')

        # Применяем offset и limit из опций
        offset = self.options.get('offset', 0)
        limit = self.options.get('limit', 0)

        if self.options.get('debug'):
            print(f"[DEBUG GameFetcher] offset: {offset}, limit: {limit}")

        # Применяем offset
        if offset > 0:
            games_query = games_query[offset:]

        # Применяем limit
        if limit > 0:
            games_query = games_query[:limit]

        # Возвращаем список
        games_list = list(games_query)

        if self.options.get('debug'):
            print(f"[DEBUG GameFetcher] Final result: {len(games_list)} игр")
            if games_list:
                for game in games_list:
                    print(
                        f"[DEBUG GameFetcher] Selected game: id={game.id}, igdb_id={game.igdb_id}, name='{game.name}'")
            else:
                # Детальная диагностика почему список пустой
                print(f"[DEBUG GameFetcher] === DIAGNOSIS: Why empty list? ===")

                if game_ids_str:
                    requested_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                    for gid in requested_ids:
                        game = Game.objects.filter(igdb_id=gid).first()
                        if game:
                            print(f"\nGame {gid} '{game.name}':")

                            # Проверяем условия
                            has_rawg = bool(game.rawg_description)
                            in_not_found = gid in self.not_found_ids
                            overwrite = self.options.get('overwrite', False)
                            auto_offset_enabled = auto_offset and not self.options.get('ignore_auto_offset', False)

                            # Проверка по game_type
                            main_types = [0, 1, 2, 4, 5, 8, 9, 10, 11]
                            correct_type = game.game_type in main_types if not self.options.get('include_all_gametypes',
                                                                                                False) else True

                            print(f"  - rawg_description: {has_rawg}")
                            print(f"  - overwrite mode: {overwrite}")
                            print(f"  - auto_offset enabled: {auto_offset_enabled}")
                            print(f"  - in not_found_ids: {in_not_found}")
                            print(f"  - game_type: {game.game_type} (in main types: {correct_type})")

                            # Определяем причину
                            reasons = []
                            if not overwrite and has_rawg:
                                reasons.append("already has rawg_description (overwrite=False)")
                            if auto_offset_enabled and in_not_found:
                                reasons.append("in not_found_ids (auto_offset=True)")
                            if not correct_type and not self.options.get('include_all_gametypes', False):
                                reasons.append("game_type not in main types")

                            if reasons:
                                print(f"  - REASONS for exclusion: {', '.join(reasons)}")
                            else:
                                print(f"  - SHOULD BE PROCESSED!")
                        else:
                            print(f"\nGame {gid}: NOT FOUND in database")

            print(f"[DEBUG GameFetcher] ===== END get_games_to_process =====")

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
                # ИСПРАВЛЕНО: game_type__in вместо game_type__igdb_id__in
                count_query = count_query.filter(
                    game_type__in=[0, 1, 2, 4, 5, 8, 9, 10, 11]
                )

            if auto_offset and self.not_found_ids:
                count_query = count_query.exclude(igdb_id__in=self.not_found_ids)

            # Используем count() с distinct если нужно
            return count_query.count()

        except Exception as e:
            if self.options.get('debug'):
                print(f'⚠️ Ошибка при подсчете игр: {e}')
            return 0

    def get_games_batch(self, offset=0, batch_size=1000, auto_offset=False, game_ids_str=None):
        """Получает игры батчами для обработки с учетом всех фильтров"""
        # Обновляем опции для этого запроса
        current_options = self.options.copy()
        current_options['limit'] = batch_size
        current_options['offset'] = offset
        current_options['game_ids'] = game_ids_str  # ← Добавляем game_ids

        # Создаем новый GameFetcher с обновленными опциями
        game_fetcher = GameFetcher(current_options, self.not_found_ids)

        # Получаем игры
        return game_fetcher.get_games_to_process(game_ids_str, auto_offset)
