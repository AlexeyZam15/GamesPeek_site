from django.core.management.base import BaseCommand
from django.utils import timezone
from games.igdb_api import make_igdb_request, set_debug_mode
from games.models import Game, Genre, Keyword, Platform, Screenshot


class Command(BaseCommand):
    help = 'Поиск всех тактических RPG по жанру Tactical или ключевому слову tactical turn-based combat'

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
            '--load-screenshots',
            action='store_true',
            help='Загружать скриншоты для игр'
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
            '--update-screenshots',
            action='store_true',
            help='Обновить скриншоты для существующих игр (дозагрузить недостающие)'
        )

    def handle(self, *args, **options):
        debug = options['debug']
        batch_size = min(options['batch_size'], 500)
        load_screenshots = options['load_screenshots']
        max_screenshots = options['max_screenshots']
        skip_existing = options['skip_existing']
        update_screenshots = options['update_screenshots']

        set_debug_mode(debug)

        self.stdout.write('🎮 ПОИСК ВСЕХ ТАКТИЧЕСКИХ RPG (без лимита)')
        if load_screenshots:
            screenshot_limit = "все" if max_screenshots == 0 else max_screenshots
            self.stdout.write(f'📸 ЗАГРУЗКА СКРИНШОТОВ ВКЛЮЧЕНА (макс: {screenshot_limit})')
        if update_screenshots:
            self.stdout.write('🔄 ОБНОВЛЕНИЕ СКРИНШОТОВ ДЛЯ СУЩЕСТВУЮЩИХ ИГР ВКЛЮЧЕНО')
        self.stdout.write('=' * 60)

        try:
            # 1. Сначала найдем ID для тактического жанра и ключевого слова
            self.stdout.write('🔍 Поиск ID тактического жанра и ключевого слова...')

            # Ищем ID жанра "Tactical"
            genre_query = 'fields id,name; where name = "Tactical";'
            tactical_genres = make_igdb_request('genres', genre_query, debug=debug)
            tactical_genre_id = tactical_genres[0]['id'] if tactical_genres else None

            # Ищем ID ключевого слова "tactical turn-based combat"
            keyword_query = 'fields id,name; where name = "tactical turn-based combat";'
            tactical_keywords = make_igdb_request('keywords', keyword_query, debug=debug)
            tactical_keyword_id = tactical_keywords[0]['id'] if tactical_keywords else None

            if not tactical_genre_id and not tactical_keyword_id:
                self.stdout.write('❌ Не найдены тактический жанр или ключевое слово')
                return

            self.stdout.write(f'✅ ID тактического жанра: {tactical_genre_id}')
            self.stdout.write(f'✅ ID ключевого слова: {tactical_keyword_id}')

            # 2. Определяем общее количество через обычный запрос с limit 1
            self.stdout.write('\n📊 Определение общего количества тактических RPG...')

            # Строим условие WHERE
            where_conditions = []
            if tactical_genre_id:
                where_conditions.append(f'genres = ({tactical_genre_id})')
            if tactical_keyword_id:
                where_conditions.append(f'keywords = ({tactical_keyword_id})')

            if not where_conditions:
                self.stdout.write('❌ Нет условий для поиска')
                return

            where_clause = ' | '.join(where_conditions)
            full_where = f'genres = (12) & ({where_clause})'

            # Для определения общего количества загрузим одну игру
            test_query = f'''
                fields id;
                where {full_where};
                limit 1;
            '''.strip()

            test_games = make_igdb_request('games', test_query, debug=debug)

            if not test_games:
                self.stdout.write('❌ Тактические RPG не найдены')
                return

            # 3. Загружаем игры пачками без предварительного подсчета
            self.stdout.write(f'\n📥 Загрузка тактических RPG пачками по {batch_size}...')
            all_tactical_games = []
            offset = 0
            has_more_games = True

            while has_more_games:
                self.stdout.write(f'   Загрузка пачки: {offset}-{offset + batch_size}...')

                query = f'''
                    fields name,summary,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
                    where {full_where};
                    sort rating_count desc;
                    limit {batch_size};
                    offset {offset};
                '''.strip()

                batch_games = make_igdb_request('games', query, debug=debug)

                if not batch_games:
                    has_more_games = False
                    break

                all_tactical_games.extend(batch_games)
                offset += len(batch_games)

                self.stdout.write(f'   Загружено: {len(batch_games)} игр (всего: {len(all_tactical_games)})')

                # Если получили меньше игр чем ожидали, значит это последняя пачка
                if len(batch_games) < batch_size:
                    has_more_games = False

            self.stdout.write(f'📊 Финальное количество загруженных RPG: {len(all_tactical_games)}')

            if not all_tactical_games:
                self.stdout.write('❌ Тактические RPG не найдены')
                return

            # 4. Загружаем игры в базу
            self.stdout.write('\n📥 Загрузка тактических RPG в базу...')
            loaded_count = 0
            skipped_count = 0
            error_count = 0
            screenshots_loaded = 0
            screenshots_updated = 0

            for i, game_data in enumerate(all_tactical_games, 1):
                game_id = game_data.get('id')
                game_name = game_data.get('name', 'Unknown')

                if not game_id:
                    error_count += 1
                    continue

                game_exists = Game.objects.filter(igdb_id=game_id).exists()

                if skip_existing and game_exists:
                    skipped_count += 1
                    if debug and i % 20 == 0:
                        self.stdout.write(f'   ⏭️  Уже в базе (пропуск): {game_name}')
                    continue

                if not game_exists:
                    if debug or i % 10 == 0:
                        self.stdout.write(f'   [{i}/{len(all_tactical_games)}] Загрузка: {game_name}')

                    try:
                        result = self.process_game(game_data)
                        if result == 'created':
                            loaded_count += 1
                            if debug:
                                self.stdout.write(f'   ✅ Загружена: {game_name}')

                            # Загружаем скриншоты для новой игры
                            if load_screenshots:
                                loaded = self.load_game_screenshots(game_id, max_screenshots)
                                screenshots_loaded += loaded

                        elif result == 'error':
                            error_count += 1
                            self.stderr.write(f'   ❌ Ошибка загрузки: {game_name}')
                    except Exception as e:
                        error_count += 1
                        self.stderr.write(f'   ❌ Ошибка загрузки {game_name}: {e}')
                else:
                    skipped_count += 1
                    if debug and i % 20 == 0:
                        self.stdout.write(f'   ⏭️  Уже в базе: {game_name}')

                    # Обновляем скриншоты для существующих игр
                    if update_screenshots or load_screenshots:
                        game = Game.objects.get(igdb_id=game_id)
                        current_screenshots_count = game.screenshots.count()

                        # Получаем общее количество скриншотов на IGDB
                        total_igdb_screenshots = self.get_igdb_screenshots_count(game_id)

                        if current_screenshots_count < total_igdb_screenshots:
                            self.stdout.write(f'   🔄 Обновление скриншотов для: {game_name}')
                            self.stdout.write(
                                f'      В базе: {current_screenshots_count}, на IGDB: {total_igdb_screenshots}')

                            # Удаляем старые скриншоты и загружаем заново
                            game.screenshots.all().delete()
                            loaded = self.load_game_screenshots(game_id, max_screenshots)
                            screenshots_updated += loaded
                            self.stdout.write(f'      ✅ Загружено новых скриншотов: {loaded}')

            # Итоги
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write(self.style.SUCCESS('✅ ПОИСК ЗАВЕРШЕН!'))
            self.stdout.write(f'• Найдено тактических RPG: {len(all_tactical_games)}')
            self.stdout.write(f'• Новых игр загружено в базу: {loaded_count}')
            self.stdout.write(f'• Уже было в базе: {skipped_count}')
            self.stdout.write(f'• Ошибок при загрузке: {error_count}')

            if load_screenshots or update_screenshots:
                self.stdout.write(f'• Скриншотов загружено для новых игр: {screenshots_loaded}')
                self.stdout.write(f'• Скриншотов обновлено для существующих игр: {screenshots_updated}')
                total_screenshots = Screenshot.objects.count()
                self.stdout.write(f'• Всего скриншотов в базе: {total_screenshots}')

            self.stdout.write(f'• Всего тактических RPG в базе: {Game.objects.filter(genres__igdb_id=12).count()}')

            # Топ-10 самых популярных найденных RPG
            self.stdout.write('\n🏆 ТОП-10 САМЫХ ПОПУЛЯРНЫХ ТАКТИЧЕСКИХ RPG:')
            top_games = sorted(all_tactical_games, key=lambda x: x.get('rating_count', 0), reverse=True)[:10]
            for i, game in enumerate(top_games, 1):
                rating = game.get('rating', 'N/A')
                rating_count = game.get('rating_count', 0)
                self.stdout.write(f'   {i}. {game.get("name", "Unknown")} (рейтинг: {rating}, голосов: {rating_count})')

        except Exception as e:
            self.stderr.write(f'❌ Ошибка: {e}')
            import traceback
            self.stderr.write(traceback.format_exc())

    def get_igdb_screenshots_count(self, game_id):
        """Получает общее количество скриншотов для игры на IGDB"""
        try:
            query = f'''
                fields game;
                where game = {game_id};
                limit 500;
            '''
            screenshots_data = make_igdb_request('screenshots', query)
            return len(screenshots_data) if screenshots_data else 0
        except Exception:
            return 0

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