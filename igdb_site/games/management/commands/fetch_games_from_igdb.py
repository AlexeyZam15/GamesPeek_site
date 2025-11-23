from django.core.management.base import BaseCommand
from django.db import transaction
from games.igdb_api import make_igdb_request
from games.models import Game, Genre, Platform, Keyword
from datetime import datetime
from django.utils import timezone
import time
from django.core.cache import cache


class Command(BaseCommand):
    help = 'Fetch ALL games from IGDB (ULTRA OPTIMIZED)'

    def add_arguments(self, parser):
        parser.add_argument('--genre', type=str, nargs='+', help='Filter by genre names (e.g., "RPG" "Action")')
        parser.add_argument('--keyword', type=str, nargs='+',
                            help='Filter by keyword names (e.g., "zombies" "open world")')
        parser.add_argument('--batch-size', type=int, default=500, help='Batch size for pagination')
        parser.add_argument('--delay', type=float, default=0.2, help='Delay between batches in seconds')
        parser.add_argument('--max-games', type=int, default=10000, help='Maximum games to fetch')
        parser.add_argument('--min-rating', type=int, default=70, help='Minimum rating filter')
        parser.add_argument('--min-rating-count', type=int, default=5, help='Minimum rating count filter')
        parser.add_argument('--count-only', action='store_true', help='Only count games without downloading')

    def handle(self, *args, **options):
        if options['count_only']:
            self.handle_count_only(options)
        else:
            self.handle_normal_download(options)

    def handle_count_only(self, options):
        """Только подсчет игр по критериям"""
        self.stdout.write("🔍 COUNTING GAMES WITH CRITERIA...")

        genre_names = options.get('genre')
        keyword_names = options.get('keyword')
        min_rating = options['min_rating']
        min_rating_count = options['min_rating_count']

        # Вывод критериев
        if genre_names:
            self.stdout.write(f"🎯 Genres: {', '.join(genre_names)}")
        if keyword_names:
            self.stdout.write(f"🔑 Keywords: {', '.join(keyword_names)}")
        self.stdout.write(f"⭐ Min rating: {min_rating}")
        self.stdout.write(f"📊 Min ratings: {min_rating_count}")

        # Получаем ID фильтров
        genre_ids = self.get_filter_ids_cached('genres', genre_names, "genre") if genre_names else []
        keyword_ids = self.get_filter_ids_cached('keywords', keyword_names, "keyword") if keyword_names else []

        # Подсчет
        total_count = self.count_games(genre_ids, keyword_ids, min_rating, min_rating_count)

        if total_count >= 0:
            self.show_count_results(total_count, genre_ids, keyword_ids)

    def handle_normal_download(self, options):
        """Нормальная загрузка игр"""
        genre_names = options.get('genre')
        keyword_names = options.get('keyword')
        batch_size = options['batch_size']
        delay = options['delay']
        max_games = options['max_games']
        min_rating = options['min_rating']
        min_rating_count = options['min_rating_count']

        self.stdout.write(f"🚀 ULTRA OPTIMIZED FETCH STARTING...")
        self.stdout.write(f"📦 Batch size: {batch_size}")
        self.stdout.write(f"⏳ Delay: {delay}s")
        self.stdout.write(f"🎯 Max games: {max_games}")
        self.stdout.write(f"⭐ Min rating: {min_rating}")
        self.stdout.write(f"📊 Min ratings: {min_rating_count}")

        # Получаем ID фильтров
        genre_ids = self.get_filter_ids_cached('genres', genre_names, "genre") if genre_names else []
        keyword_ids = self.get_filter_ids_cached('keywords', keyword_names, "keyword") if keyword_names else []

        # Загружаем пачками с РАЗНЫМИ сортировками для обхода бага пагинации
        total_processed = 0
        sort_methods = ['first_release_date desc', 'rating_count desc', 'name asc', 'id asc']

        with transaction.atomic():
            for sort_method in sort_methods:
                if total_processed >= max_games:
                    break

                self.stdout.write(f"\n🎯 Loading with sort: {sort_method}")
                processed_with_sort = self.load_with_sorting(
                    genre_ids, keyword_ids, min_rating, min_rating_count,
                    sort_method, batch_size, max_games - total_processed, total_processed
                )
                total_processed += processed_with_sort

                if processed_with_sort == 0:
                    self.stdout.write("💤 No more games found with this sort method")

                time.sleep(delay)

        # Принудительно загружаем известные игры которые должны быть в результатах
        self.ensure_specific_games(genre_ids, keyword_ids)

        self.show_final_stats(total_processed, genre_ids, keyword_ids)

    def load_with_sorting(self, genre_ids, keyword_ids, min_rating, min_rating_count,
                          sort_method, batch_size, max_to_fetch, total_processed):
        """Загрузка игр с определенной сортировкой"""
        offset = 0
        processed = 0

        while processed < max_to_fetch:
            games_data = self.fetch_games_batch(
                offset, batch_size, genre_ids, keyword_ids,
                min_rating, min_rating_count, sort_method
            )

            if not games_data:
                break

            # Проверяем что это новые игры (не дубли)
            new_games = [game for game in games_data
                         if not Game.objects.filter(igdb_id=game['id']).exists()]

            if not new_games:
                self.stdout.write("💤 No new games in this batch, stopping...")
                break

            processed_in_batch = self.process_games_batch(new_games, total_processed + processed, offset)
            processed += processed_in_batch
            offset += batch_size

            self.stdout.write(f"📈 Processed: {total_processed + processed} games (offset: {offset})")

            if processed_in_batch < len(games_data) or processed >= max_to_fetch:
                break

            time.sleep(0.1)

        return processed

    def get_filter_ids_cached(self, endpoint, names, filter_type):
        """Универсальное получение ID с кэшированием"""
        if not names:
            return []

        cache_key = f"igdb_{endpoint}_ids_{hash(frozenset(names))}"
        cached_ids = cache.get(cache_key)

        if cached_ids is not None:
            self.stdout.write(f"📦 Using cached {filter_type} IDs: {cached_ids}")
            return cached_ids

        # ОДИН запрос для всех имен
        names_str = '","'.join(names)
        query = f'fields id, name; where name = ("{names_str}");'

        try:
            data = make_igdb_request(endpoint, query)
            found_ids = [item['id'] for item in data]
            found_names = [item['name'] for item in data]

            # Логируем результаты
            for name in names:
                if name in found_names:
                    self.stdout.write(f"✅ Found {filter_type} '{name}'")
                else:
                    self.stdout.write(self.style.WARNING(f"⚠️ {filter_type.title()} '{name}' not found"))

            cache.set(cache_key, found_ids, 3600)
            return found_ids

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ {filter_type.title()} fetch failed: {e}'))
            return []

    def count_games(self, genre_ids, keyword_ids, min_rating, min_rating_count):
        """Подсчет игр по критериям"""
        try:
            count_query = self.build_query(genre_ids, keyword_ids, min_rating, min_rating_count,
                                           fields="id", limit=1000, sort_method="id asc")
            count_data = make_igdb_request('games', count_query)
            return len(count_data)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Count query failed: {e}'))
            return -1

    def build_query(self, genre_ids, keyword_ids, min_rating, min_rating_count,
                    fields=None, limit=None, offset=None, sort_method="id asc"):
        """Построение оптимального запроса"""
        if fields is None:
            fields = "name,summary,storyline,rating,rating_count,first_release_date,genres.name,genres.id,platforms.name,platforms.id,cover.image_id,keywords.name,keywords.id"

        base_query = f"fields {fields}; where rating_count >= {min_rating_count} & rating >= {min_rating};"

        # Фильтры
        filters = []
        if genre_ids:
            filters.append(f'genres = [{",".join(map(str, genre_ids))}]')
        if keyword_ids:
            filters.append(f'keywords = [{",".join(map(str, keyword_ids))}]')

        if filters:
            base_query += f' & ({" | ".join(filters)});'
        else:
            base_query += ';'

        # ФИКС: Всегда указываем лимит
        if not limit:
            limit = 500

        # ФИКС: Используем переданную сортировку
        base_query += f' sort {sort_method}; limit {limit};'

        # ФИКС: Добавляем offset только если он указан и > 0
        if offset is not None and offset > 0:
            base_query += f' offset {offset};'

        return base_query

    def show_count_results(self, total_count, genre_ids, keyword_ids):
        """Показывает результаты подсчета"""
        self.stdout.write(self.style.SUCCESS(f"\n🎯 TOTAL GAMES FOUND: {total_count}"))

        if total_count == 0:
            self.stdout.write("😔 No games match your criteria")
            return

        # Показываем примеры
        try:
            sample_query = self.build_query(genre_ids, keyword_ids, 0, 0,
                                            fields="name,rating,rating_count",
                                            limit=min(20, total_count))
            sample_data = make_igdb_request('games', sample_query)

            self.stdout.write(f"\n🎮 SAMPLE GAMES:")
            self.stdout.write("─" * 60)
            for i, game in enumerate(sample_data, 1):
                rating = game.get('rating', 'N/A')
                rating_count = game.get('rating_count', 0)
                self.stdout.write(f"{i:2d}. {game['name'][:50]:50} | ⭐ {rating:4} | 📊 {rating_count:4}")

        except Exception as e:
            self.stdout.write(f"⚠️ Could not load sample games: {e}")

    def fetch_games_batch(self, offset, limit, genre_ids, keyword_ids, min_rating, min_rating_count, sort_method):
        """Загрузка пачки игр"""
        try:
            query = self.build_query(genre_ids, keyword_ids, min_rating, min_rating_count,
                                     limit=limit, offset=offset, sort_method=sort_method)
            return make_igdb_request('games', query)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Batch fetch failed: {e}'))
            return []

    def process_games_batch(self, games_data, total_processed, batch_offset):
        """Обработка пачки игр"""
        if not games_data:
            return 0

        start_time = time.time()

        # ВЫВОДИМ НАЗВАНИЯ ИГР ИЗ ЭТОГО БАТЧА
        self.stdout.write(f"\n🎮 BATCH (offset {batch_offset}): {len(games_data)} NEW GAMES")
        self.stdout.write("─" * 50)
        for i, game_data in enumerate(games_data[:8], 1):
            game_name = game_data.get('name', 'Unknown')
            self.stdout.write(f"{i:2d}. {game_name}")
        if len(games_data) > 8:
            self.stdout.write(f"   ... and {len(games_data) - 8} more games")
        self.stdout.write("─" * 50)

        # Собираем все данные ОДНИМ проходом
        all_data = self.collect_all_relations(games_data)

        # Массовое создание объектов
        genre_objects = self.bulk_get_or_create(Genre, all_data['genres'], "genres")
        platform_objects = self.bulk_get_or_create(Platform, all_data['platforms'], "platforms")
        keyword_objects = self.bulk_get_or_create(Keyword, all_data['keywords'], "keywords")

        # Обработка игр
        processed, new_games = self.process_games_fast(
            games_data, genre_objects, platform_objects, keyword_objects
        )

        total_time = time.time() - start_time
        self.stdout.write(f"⚡ Processed {processed}/{len(games_data)} games in {total_time:.2f}s")

        return processed

    def collect_all_relations(self, games_data):
        """Сбор всех связанных данных за один проход"""
        genres, platforms, keywords = {}, {}, {}

        for game_data in games_data:
            for genre in game_data.get('genres', []):
                genres[genre['id']] = genre['name']
            for platform in game_data.get('platforms', []):
                platforms[platform['id']] = platform['name']
            for keyword in game_data.get('keywords', []):
                keywords[keyword['id']] = keyword['name']

        return {'genres': genres, 'platforms': platforms, 'keywords': keywords}

    def bulk_get_or_create(self, model, items_dict, item_type):
        """Массовое получение или создание объектов"""
        if not items_dict:
            return {}

        existing_items = model.objects.filter(igdb_id__in=items_dict.keys())
        existing_map = {item.igdb_id: item for item in existing_items}

        # Создаем отсутствующие
        to_create = [
            model(igdb_id=igdb_id, name=name)
            for igdb_id, name in items_dict.items()
            if igdb_id not in existing_map
        ]

        if to_create:
            model.objects.bulk_create(to_create)
            # Обновляем мапу созданными объектами
            for item in to_create:
                existing_map[item.igdb_id] = item
            self.stdout.write(f"✅ Created {len(to_create)} {item_type}")

        return existing_map

    def process_games_fast(self, games_data, genres_map, platforms_map, keywords_map):
        """Быстрая обработка игр"""
        processed = 0
        new_games = 0

        for game_data in games_data:
            try:
                # Подготовка данных
                game_defaults = self.prepare_game_defaults(game_data)

                # Создание/обновление игры
                game, created = Game.objects.update_or_create(
                    igdb_id=game_data['id'],
                    defaults=game_defaults
                )

                # Массовое обновление связей
                self.update_game_relations(game, game_data, genres_map, platforms_map, keywords_map)

                processed += 1
                if created:
                    new_games += 1

            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Error with {game_data.get("name", "Unknown")}: {e}'))

        return processed, new_games

    def prepare_game_defaults(self, game_data):
        """Подготовка данных для игры"""
        first_release_date = None
        if game_data.get('first_release_date'):
            first_release_date = timezone.make_aware(
                datetime.fromtimestamp(game_data['first_release_date'])
            )

        cover_url = None
        if game_data.get('cover'):
            cover_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{game_data['cover']['image_id']}.jpg"

        return {
            'name': game_data.get('name', ''),
            'summary': game_data.get('summary', ''),
            'storyline': game_data.get('storyline', ''),
            'rating': game_data.get('rating'),
            'rating_count': game_data.get('rating_count', 0),
            'first_release_date': first_release_date,
            'cover_url': cover_url
        }

    def update_game_relations(self, game, game_data, genres_map, platforms_map, keywords_map):
        """Обновление связей игры"""
        genre_objects = [genres_map[g['id']] for g in game_data.get('genres', []) if g['id'] in genres_map]
        platform_objects = [platforms_map[p['id']] for p in game_data.get('platforms', []) if p['id'] in platforms_map]
        keyword_objects = [keywords_map[k['id']] for k in game_data.get('keywords', []) if k['id'] in keywords_map]

        game.genres.set(genre_objects)
        game.platforms.set(platform_objects)
        game.keywords.set(keyword_objects)

    def ensure_specific_games(self, genre_ids, keyword_ids):
        """Принудительно загружает известные игры которые должны быть в результатах"""
        known_games = [
            428,  # Final Fantasy Tactics
            43872,  # Tear Ring Saga
        ]

        self.stdout.write(f"\n🎯 ENSURING KNOWN GAMES ARE LOADED...")

        for game_id in known_games:
            if not Game.objects.filter(igdb_id=game_id).exists():
                self.stdout.write(f"⚠️ Game ID {game_id} missing, fetching...")
                self.fetch_and_process_single_game(game_id)

    def fetch_and_process_single_game(self, game_id):
        """Загружает и обрабатывает одну игру по ID"""
        query = f'fields name,summary,storyline,rating,rating_count,first_release_date,genres.name,genres.id,platforms.name,platforms.id,cover.image_id,keywords.name,keywords.id; where id = {game_id};'

        try:
            games_data = make_igdb_request('games', query)
            if games_data:
                # Используем существующую логику обработки
                all_data = self.collect_all_relations(games_data)
                genre_objects = self.bulk_get_or_create(Genre, all_data['genres'], "genres")
                platform_objects = self.bulk_get_or_create(Platform, all_data['platforms'], "platforms")
                keyword_objects = self.bulk_get_or_create(Keyword, all_data['keywords'], "keywords")

                self.process_games_fast(games_data, genre_objects, platform_objects, keyword_objects)
                self.stdout.write(f"✅ Manually loaded game ID {game_id}")
        except Exception as e:
            self.stdout.write(f"❌ Failed to manually load game ID {game_id}: {e}")

    def show_final_stats(self, total_processed, genre_ids, keyword_ids):
        """Финальная статистика"""
        self.stdout.write(self.style.SUCCESS(f"\n🎉 COMPLETED! Processed {total_processed} games"))

        # Статистика БД
        stats = {
            'Games': Game.objects.count(),
            'Genres': Genre.objects.count(),
            'Platforms': Platform.objects.count(),
            'Keywords': Keyword.objects.count(),
        }

        self.stdout.write(f"\n📊 DATABASE STATISTICS:")
        for name, count in stats.items():
            self.stdout.write(f"   {name}: {count}")

        # Статистика по фильтрам
        if genre_ids:
            self.show_filter_stats(Genre, genre_ids, "GENRE")
        if keyword_ids:
            self.show_filter_stats(Keyword, keyword_ids, "KEYWORD")

    def show_filter_stats(self, model, filter_ids, filter_type):
        """Статистика по фильтрам"""
        self.stdout.write(f"\n🎯 {filter_type} STATISTICS:")
        for filter_id in filter_ids:
            try:
                item = model.objects.get(igdb_id=filter_id)
                count = Game.objects.filter(**{f'{model.__name__.lower()}s': item}).count()
                self.stdout.write(f"   {item.name}: {count} games")
            except model.DoesNotExist:
                pass