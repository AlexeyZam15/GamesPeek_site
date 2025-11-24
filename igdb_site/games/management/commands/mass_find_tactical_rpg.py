from django.core.management.base import BaseCommand
from django.utils import timezone
from games.igdb_api import make_igdb_request, set_debug_mode
from games.models import Game, Genre, Keyword, Platform, Screenshot


class Command(BaseCommand):
    help = 'Загрузка игр из IGDB с различными опциями'

    def add_arguments(self, parser):
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Включить режим отладки IGDB API'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=500,
            help='Размер пачки для загрузки (макс. 500)'
        )
        parser.add_argument(
            '--no-screenshots',
            action='store_true',
            help='НЕ загружать скриншоты для игр'
        )
        parser.add_argument(
            '--max-screenshots',
            type=int,
            default=0,
            help='Максимальное количество скриншотов на игру (0 = загружать все)'
        )
        parser.add_argument(
            '--skip-existing',
            action='store_true',
            help='Пропускать игры, которые уже есть в базе'
        )
        parser.add_argument(
            '--input-file',
            type=str,
            help='Загрузить игры из файла (точные названия на каждой строке)'
        )
        parser.add_argument(
            '--tactical-rpg',
            action='store_true',
            help='Загрузить тактические RPG по жанру и ключевым словам'
        )
        parser.add_argument(
            '--genre-id',
            type=int,
            help='Загрузить игры по ID жанра'
        )
        parser.add_argument(
            '--keyword-id',
            type=int,
            help='Загрузить игры по ID ключевого слова'
        )

    def search_games_by_exact_name_batch(self, game_names, debug=False):
        """Массовый поиск игр по точным названиям - ОДИН ЗАПРОС"""
        all_games = []
        not_found_games = []

        # Разбиваем на пачки по 50 игр (ограничение IGDB)
        batch_size = 50
        batches = [game_names[i:i + batch_size] for i in range(0, len(game_names), batch_size)]

        for batch_num, batch in enumerate(batches, 1):
            if debug:
                self.stdout.write(f'🔍 Пакет {batch_num}/{len(batches)}: {len(batch)} игр')

            # Создаем условие WHERE для всех игр в пачке
            name_conditions = ' | '.join([f'name = "{name}"' for name in batch])
            query = f'''
                fields name,summary,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
                where {name_conditions};
                limit {len(batch) * 2};  # Немного больше на случай дубликатов
            '''.strip()

            try:
                games_data = make_igdb_request('games', query, debug=debug)

                if games_data:
                    # Создаем словарь найденных игр по названию для быстрого поиска
                    found_games_map = {game['name']: game for game in games_data}

                    # Сопоставляем найденные игры с исходным списком
                    for game_name in batch:
                        if game_name in found_games_map:
                            all_games.append(found_games_map[game_name])
                            if debug:
                                self.stdout.write(f'   ✅ Найдено: {game_name}')
                        else:
                            not_found_games.append(game_name)
                            if debug:
                                self.stdout.write(f'   ❌ Не найдено: {game_name}')
                else:
                    # Если ничего не найдено для всей пачки
                    not_found_games.extend(batch)
                    if debug:
                        for game_name in batch:
                            self.stdout.write(f'   ❌ Не найдено: {game_name}')

            except Exception as e:
                # При ошибке добавляем всю пачку в не найденные
                not_found_games.extend(batch)
                if debug:
                    self.stdout.write(f'   ❌ Ошибка пакета: {e}')

        return all_games, not_found_games

    def load_game_screenshots(self, game_id, max_screenshots=0):
        """Загружает скриншоты для конкретной игры"""
        try:
            # Определяем лимит для запроса
            limit = 500 if max_screenshots == 0 else max_screenshots

            query = f'''
                fields *;
                where game = {game_id};
                limit {limit};
            '''

            screenshots_data = make_igdb_request('screenshots', query)

            if not screenshots_data:
                return 0

            game = Game.objects.get(igdb_id=game_id)
            loaded_screenshots = 0

            for screenshot_data in screenshots_data:
                screenshot_id = screenshot_data.get('id')
                image_url = screenshot_data.get('url')

                if not screenshot_id or not image_url:
                    continue

                # Пропускаем если уже существует
                if Screenshot.objects.filter(igdb_id=screenshot_id).exists():
                    continue

                # Создаем URL для скриншота в высоком качестве
                high_res_url = f"https:{image_url.replace('thumb', 'screenshot_big')}"

                screenshot = Screenshot(
                    igdb_id=screenshot_id,
                    game=game,
                    image_url=high_res_url,
                    width=screenshot_data.get('width', 1920),
                    height=screenshot_data.get('height', 1080),
                    caption=screenshot_data.get('caption', '')
                )

                screenshot.save()
                loaded_screenshots += 1

                # Если достигли максимального количества, прерываем
                if max_screenshots > 0 and loaded_screenshots >= max_screenshots:
                    break

            return loaded_screenshots

        except Exception as e:
            self.stderr.write(f'   ❌ Ошибка загрузки скриншотов для игры {game_id}: {e}')
            return 0

    def process_game(self, game_data):
        """Обработать одну игру"""
        igdb_id = game_data['id']
        game_name = game_data.get('name', 'Unknown')

        try:
            if Game.objects.filter(igdb_id=igdb_id).exists():
                return 'skipped'

            game = Game(igdb_id=igdb_id)
            game.name = game_data.get('name', '')
            game.summary = game_data.get('summary', '')
            game.storyline = game_data.get('storyline', '')
            game.rating = game_data.get('rating')
            game.rating_count = game_data.get('rating_count', 0)

            if game_data.get('first_release_date'):
                from datetime import datetime
                naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                game.first_release_date = timezone.make_aware(naive_datetime)

            if game_data.get('cover'):
                cover_url = self.get_cover_url(game_data['cover'])
                if cover_url:
                    game.cover_url = cover_url

            game.save()

            # Обрабатываем связанные данные
            if game_data.get('genres'):
                self.process_genres(game, game_data['genres'])

            if game_data.get('keywords'):
                self.process_keywords(game, game_data['keywords'])

            if game_data.get('platforms'):
                self.process_platforms(game, game_data['platforms'])

            return 'created'

        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки {game_name}: {e}')
            return 'error'

    def process_genres(self, game, genre_ids):
        """Обработка жанров"""
        if not genre_ids:
            return

        genres = []
        id_list = ','.join(map(str, genre_ids))
        query = f'fields id,name; where id = ({id_list});'

        try:
            genre_data = make_igdb_request('genres', query)
            genre_map = {g['id']: g['name'] for g in genre_data}

            for genre_id in genre_ids:
                genre_name = genre_map.get(genre_id, f'Genre {genre_id}')
                genre, created = Genre.objects.get_or_create(
                    igdb_id=genre_id,
                    defaults={'name': genre_name}
                )
                if not created and genre.name.startswith('Genre '):
                    genre.name = genre_name
                    genre.save()
                genres.append(genre)

            game.genres.set(genres)
        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки жанров: {e}')

    def process_keywords(self, game, keyword_ids):
        """Обработка ключевых слов"""
        if not keyword_ids:
            return

        keywords = []
        id_list = ','.join(map(str, keyword_ids))
        query = f'fields id,name; where id = ({id_list});'

        try:
            keyword_data = make_igdb_request('keywords', query)
            keyword_map = {k['id']: k['name'] for k in keyword_data}

            for keyword_id in keyword_ids:
                keyword_name = keyword_map.get(keyword_id, f'Keyword {keyword_id}')
                keyword, created = Keyword.objects.get_or_create(
                    igdb_id=keyword_id,
                    defaults={'name': keyword_name}
                )
                if not created and keyword.name.startswith('Keyword '):
                    keyword.name = keyword_name
                    keyword.save()
                keywords.append(keyword)

            game.keywords.set(keywords)
        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки ключевых слов: {e}')

    def process_platforms(self, game, platform_ids):
        """Обработка платформ"""
        if not platform_ids:
            return

        platforms = []
        id_list = ','.join(map(str, platform_ids))
        query = f'fields id,name; where id = ({id_list});'

        try:
            platform_data = make_igdb_request('platforms', query)
            platform_map = {p['id']: p['name'] for p in platform_data}

            for platform_id in platform_ids:
                platform_name = platform_map.get(platform_id, f'Platform {platform_id}')
                platform, created = Platform.objects.get_or_create(
                    igdb_id=platform_id,
                    defaults={'name': platform_name}
                )
                if not created and platform.name.startswith('Platform '):
                    platform.name = platform_name
                    platform.save()
                platforms.append(platform)

            game.platforms.set(platforms)
        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки платформ: {e}')

    def get_cover_url(self, cover_id):
        """Получить URL обложки"""
        try:
            query = f'fields url; where id = {cover_id};'
            result = make_igdb_request('covers', query)
            if result and 'url' in result[0]:
                return f"https:{result[0]['url'].replace('thumb', 'cover_big')}"
        except Exception as e:
            pass
        return None

    # ОСТАВШИЕСЯ МЕТОДЫ ДЛЯ ДРУГИХ СПОСОБОВ ЗАГРУЗКИ
    def load_tactical_rpg_games(self, batch_size, debug=False):
        """Загрузка тактических RPG"""
        self.stdout.write('🔍 Поиск тактических RPG...')

        # Ищем ID жанра и ключевого слова
        genre_query = 'fields id,name; where name = "Tactical";'
        tactical_genres = make_igdb_request('genres', genre_query, debug=debug)
        tactical_genre_id = tactical_genres[0]['id'] if tactical_genres else None

        keyword_query = 'fields id,name; where name = "tactical turn-based combat";'
        tactical_keywords = make_igdb_request('keywords', keyword_query, debug=debug)
        tactical_keyword_id = tactical_keywords[0]['id'] if tactical_keywords else None

        if not tactical_genre_id and not tactical_keyword_id:
            self.stdout.write('❌ Не найдены тактический жанр или ключевое слово')
            return []

        # Строим условие WHERE
        where_conditions = []
        if tactical_genre_id:
            where_conditions.append(f'genres = ({tactical_genre_id})')
        if tactical_keyword_id:
            where_conditions.append(f'keywords = ({tactical_keyword_id})')

        where_clause = ' | '.join(where_conditions)
        full_where = f'genres = (12) & ({where_clause})'

        return self.load_games_by_query(full_where, batch_size, debug)

    def load_games_by_genre(self, genre_id, batch_size, debug=False):
        """Загрузка игр по жанру"""
        self.stdout.write(f'🔍 Загрузка игр жанра ID: {genre_id}...')
        where_clause = f'genres = ({genre_id})'
        return self.load_games_by_query(where_clause, batch_size, debug)

    def load_games_by_keyword(self, keyword_id, batch_size, debug=False):
        """Загрузка игр по ключевому слову"""
        self.stdout.write(f'🔍 Загрузка игр ключевого слова ID: {keyword_id}...')
        where_clause = f'keywords = ({keyword_id})'
        return self.load_games_by_query(where_clause, batch_size, debug)

    def load_games_by_query(self, where_clause, batch_size, debug=False):
        """Загрузка игр по запросу с пагинацией"""
        all_games = []
        offset = 0
        has_more_games = True

        while has_more_games:
            if debug:
                self.stdout.write(f'   Загрузка пачки: {offset}-{offset + batch_size}...')

            query = f'''
                fields name,summary,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
                where {where_clause};
                sort rating_count desc;
                limit {batch_size};
                offset {offset};
            '''.strip()

            batch_games = make_igdb_request('games', query, debug=debug)

            if not batch_games:
                has_more_games = False
                break

            all_games.extend(batch_games)
            offset += len(batch_games)

            if debug:
                self.stdout.write(f'   Загружено: {len(batch_games)} игр (всего: {len(all_games)})')

            if len(batch_games) < batch_size:
                has_more_games = False

        return all_games

    def handle(self, *args, **options):
        debug = options['debug']
        batch_size = min(options['batch_size'], 500)
        no_screenshots = options.get('no_screenshots', False)
        max_screenshots = options['max_screenshots']
        skip_existing = options['skip_existing']
        input_file = options['input_file']
        tactical_rpg = options['tactical_rpg']
        genre_id = options['genre_id']
        keyword_id = options['keyword_id']

        set_debug_mode(debug)

        self.stdout.write('🎮 ЗАГРУЗКА ИГР ИЗ IGDB')
        self.stdout.write('=' * 60)
        if no_screenshots:
            self.stdout.write('📸 ЗАГРУЗКА СКРИНШОТОВ: ОТКЛЮЧЕНА')
        else:
            screenshot_limit = "все" if max_screenshots == 0 else max_screenshots
            self.stdout.write(f'📸 ЗАГРУЗКА СКРИНШОТОВ: ВКЛЮЧЕНА (макс: {screenshot_limit})')

        # ПРОВЕРКА ОПЦИЙ
        load_methods = [input_file, tactical_rpg, genre_id, keyword_id]
        if sum(1 for method in load_methods if method) != 1:
            self.stderr.write('❌ Укажите ровно один способ загрузки:')
            self.stderr.write('   --input-file FILE    - загрузка из файла')
            self.stderr.write('   --tactical-rpg       - загрузка тактических RPG')
            self.stderr.write('   --genre-id ID        - загрузка по жанру')
            self.stderr.write('   --keyword-id ID      - загрузка по ключевому слову')
            return

        try:
            all_games = []
            not_found_games = []

            if input_file:
                # ЗАГРУЗКА ИЗ ФАЙЛА - ИСПОЛЬЗУЕМ МАССОВЫЙ ПОИСК
                self.stdout.write(f'📁 Загрузка из файла: {input_file}')
                with open(input_file, 'r', encoding='utf-8') as f:
                    game_names = [line.strip() for line in f if line.strip()]

                self.stdout.write(f'🔍 Массовый поиск {len(game_names)} игр...')
                all_games, not_found_games = self.search_games_by_exact_name_batch(game_names, debug)

            elif tactical_rpg:
                # ЗАГРУЗКА ТАКТИЧЕСКИХ RPG
                all_games = self.load_tactical_rpg_games(batch_size, debug)

            elif genre_id:
                # ЗАГРУЗКА ПО ЖАНРУ
                all_games = self.load_games_by_genre(genre_id, batch_size, debug)

            elif keyword_id:
                # ЗАГРУЗКА ПО КЛЮЧЕВОМУ СЛОВУ
                all_games = self.load_games_by_keyword(keyword_id, batch_size, debug)

            if not all_games and not input_file:
                self.stdout.write('❌ Не найдено игр для загрузки')
                return

            self.stdout.write(f'📥 Найдено игр для обработки: {len(all_games)}')

            # ЗАГРУЗКА В БАЗУ
            loaded_count = 0
            skipped_count = 0
            error_count = 0
            screenshots_loaded = 0

            for i, game_data in enumerate(all_games, 1):
                game_id = game_data.get('id')
                game_name = game_data.get('name', 'Unknown')

                if not game_id:
                    error_count += 1
                    continue

                game_exists = Game.objects.filter(igdb_id=game_id).exists()

                if skip_existing and game_exists:
                    skipped_count += 1
                    if debug and i % 50 == 0:
                        self.stdout.write(f'   ⏭️  Уже в базе: {game_name}')
                    continue

                if not game_exists:
                    if debug or i % 20 == 0:
                        self.stdout.write(f'   [{i}/{len(all_games)}] Загрузка: {game_name}')

                    try:
                        result = self.process_game(game_data)
                        if result == 'created':
                            loaded_count += 1

                            # ЗАГРУЗКА СКРИНШОТОВ ДЛЯ НОВЫХ ИГР (если не отключено)
                            if not no_screenshots:
                                loaded = self.load_game_screenshots(game_id, max_screenshots)
                                screenshots_loaded += loaded
                                if debug and loaded > 0:
                                    self.stdout.write(f'   📸 Загружено скриншотов: {loaded}')

                        elif result == 'error':
                            error_count += 1
                    except Exception as e:
                        error_count += 1
                        self.stderr.write(f'   ❌ Ошибка загрузки {game_name}: {e}')
                else:
                    skipped_count += 1
                    if debug and i % 50 == 0:
                        self.stdout.write(f'   ⏭️  Уже в базе: {game_name}')

            # СОХРАНЕНИЕ НЕ НАЙДЕННЫХ ИГР (только для загрузки из файла)
            if input_file and not_found_games:
                not_found_file = f"not_found_{input_file}"
                with open(not_found_file, 'w', encoding='utf-8') as f:
                    for game_name in not_found_games:
                        f.write(f"{game_name}\n")
                self.stdout.write(f'📄 Не найденные игры сохранены в: {not_found_file}')

            # ИТОГИ
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
            self.stdout.write(f'• Найдено игр: {len(all_games)}')
            self.stdout.write(f'• Новых загружено: {loaded_count}')
            self.stdout.write(f'• Уже в базе: {skipped_count}')
            self.stdout.write(f'• Ошибок: {error_count}')

            if input_file:
                self.stdout.write(f'• Не найдено в IGDB: {len(not_found_games)}')

            if not no_screenshots:
                self.stdout.write(f'• Скриншотов загружено: {screenshots_loaded}')
                total_screenshots = Screenshot.objects.count()
                self.stdout.write(f'• Всего скриншотов в базе: {total_screenshots}')

        except Exception as e:
            self.stderr.write(f'❌ Ошибка: {e}')
            import traceback
            self.stderr.write(traceback.format_exc())

    def process_game(self, game_data):
        """Обработать одну игру"""
        igdb_id = game_data['id']
        game_name = game_data.get('name', 'Unknown')

        try:
            if Game.objects.filter(igdb_id=igdb_id).exists():
                return 'skipped'

            game = Game(igdb_id=igdb_id)
            game.name = game_data.get('name', '')
            game.summary = game_data.get('summary', '')
            game.storyline = game_data.get('storyline', '')
            game.rating = game_data.get('rating')
            game.rating_count = game_data.get('rating_count', 0)

            if game_data.get('first_release_date'):
                from datetime import datetime
                naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                game.first_release_date = timezone.make_aware(naive_datetime)

            if game_data.get('cover'):
                cover_url = self.get_cover_url(game_data['cover'])
                if cover_url:
                    game.cover_url = cover_url

            game.save()

            # Обрабатываем связанные данные
            if game_data.get('genres'):
                self.process_genres(game, game_data['genres'])

            if game_data.get('keywords'):
                self.process_keywords(game, game_data['keywords'])

            if game_data.get('platforms'):
                self.process_platforms(game, game_data['platforms'])

            return 'created'

        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки {game_name}: {e}')
            return 'error'

    def process_genres(self, game, genre_ids):
        """Оптимизированная обработка жанров"""
        if not genre_ids:
            return

        genres = []
        id_list = ','.join(map(str, genre_ids))
        query = f'fields id,name; where id = ({id_list});'

        try:
            genre_data = make_igdb_request('genres', query)
            genre_map = {g['id']: g['name'] for g in genre_data}

            for genre_id in genre_ids:
                genre_name = genre_map.get(genre_id, f'Genre {genre_id}')
                genre, created = Genre.objects.get_or_create(
                    igdb_id=genre_id,
                    defaults={'name': genre_name}
                )
                if not created and genre.name.startswith('Genre '):
                    genre.name = genre_name
                    genre.save()
                genres.append(genre)

            game.genres.set(genres)
        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки жанров: {e}')

    def process_keywords(self, game, keyword_ids):
        """Оптимизированная обработка ключевых слов"""
        if not keyword_ids:
            return

        keywords = []
        id_list = ','.join(map(str, keyword_ids))
        query = f'fields id,name; where id = ({id_list});'

        try:
            keyword_data = make_igdb_request('keywords', query)
            keyword_map = {k['id']: k['name'] for k in keyword_data}

            for keyword_id in keyword_ids:
                keyword_name = keyword_map.get(keyword_id, f'Keyword {keyword_id}')
                keyword, created = Keyword.objects.get_or_create(
                    igdb_id=keyword_id,
                    defaults={'name': keyword_name}
                )
                if not created and keyword.name.startswith('Keyword '):
                    keyword.name = keyword_name
                    keyword.save()
                keywords.append(keyword)

            game.keywords.set(keywords)
        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки ключевых слов: {e}')

    def process_platforms(self, game, platform_ids):
        """Оптимизированная обработка платформ"""
        if not platform_ids:
            return

        platforms = []
        id_list = ','.join(map(str, platform_ids))
        query = f'fields id,name; where id = ({id_list});'

        try:
            platform_data = make_igdb_request('platforms', query)
            platform_map = {p['id']: p['name'] for p in platform_data}

            for platform_id in platform_ids:
                platform_name = platform_map.get(platform_id, f'Platform {platform_id}')
                platform, created = Platform.objects.get_or_create(
                    igdb_id=platform_id,
                    defaults={'name': platform_name}
                )
                if not created and platform.name.startswith('Platform '):
                    platform.name = platform_name
                    platform.save()
                platforms.append(platform)

            game.platforms.set(platforms)
        except Exception as e:
            self.stderr.write(f'❌ Ошибка обработки платформ: {e}')

    def get_cover_url(self, cover_id):
        try:
            query = f'fields url; where id = {cover_id};'
            result = make_igdb_request('covers', query)
            if result and 'url' in result[0]:
                return f"https:{result[0]['url'].replace('thumb', 'cover_big')}"
        except Exception as e:
            pass
        return None
