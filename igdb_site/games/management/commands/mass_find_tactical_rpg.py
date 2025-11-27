# mass_find_tactical_rpg.py
from django.core.management.base import BaseCommand
from django.utils import timezone
from django.db import transaction
from games.igdb_api import make_igdb_request, set_debug_mode
from games.models import (
    Game, Genre, Keyword, Platform, Screenshot,
    Series, Company, Theme, PlayerPerspective, GameMode
)
import time
from collections import defaultdict


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
        parser.add_argument(
            '--single-game',
            type=str,
            help='Загрузить одну игру по точному названию'
        )
        parser.add_argument(
            '--update-existing',
            action='store_true',
            help='Обновить существующие игры дополнительными данными'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=0.2,
            help='Задержка между запросами к IGDB (в секундах)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать какие данные будут обновлены без сохранения в базу'
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Удалить существующие игры и загрузить заново'
        )

    def preload_existing_data(self):
        """Предзагружаем существующие данные для оптимизации"""
        self.stdout.write("⚡ Предзагрузка данных...")

        # Кэшируем существующие серии, компании и т.д.
        self.existing_series = {s.igdb_id: s for s in Series.objects.all()}
        self.existing_companies = {c.igdb_id: c for c in Company.objects.all()}
        self.existing_themes = {t.igdb_id: t for t in Theme.objects.all()}
        self.existing_perspectives = {p.igdb_id: p for p in PlayerPerspective.objects.all()}
        self.existing_modes = {m.igdb_id: m for m in GameMode.objects.all()}

        self.stdout.write(f"📚 Серии: {len(self.existing_series)}")
        self.stdout.write(f"🏢 Компании: {len(self.existing_companies)}")
        self.stdout.write(f"🎨 Темы: {len(self.existing_themes)}")
        self.stdout.write(f"👁️ Перспективы: {len(self.existing_perspectives)}")
        self.stdout.write(f"🎮 Режимы: {len(self.existing_modes)}")

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

    def search_single_game_by_name(self, game_name, debug=False):
        """Поиск одной игры по точному названию"""
        query = f'''
            fields name,summary,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
            where name = "{game_name}";
            limit 1;
        '''.strip()

        try:
            games_data = make_igdb_request('games', query, debug=debug)
            if games_data:
                return games_data[0]
            else:
                return None
        except Exception as e:
            self.stderr.write(f'❌ Ошибка поиска игры {game_name}: {e}')
            return None

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

    def process_game(self, game_data, debug=False):
        """Обработать одну игру"""
        igdb_id = game_data['id']
        game_name = game_data.get('name', 'Unknown')

        try:
            if Game.objects.filter(igdb_id=igdb_id).exists():
                if debug:
                    self.stdout.write(f'   ⏭️  Уже в базе: {game_name}')
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

            if debug:
                self.stdout.write(f'   ✅ Загружена: {game_name}')

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

    def update_games_with_additional_data(self, games, delay=0.2, dry_run=False, debug=False):
        """Обновляет игры дополнительными данными: серии, разработчики, издатели, темы, перспективы, режимы"""
        self.stdout.write(f"🔄 Обновление {len(games)} игр дополнительными данными...")

        if dry_run:
            self.stdout.write("🚧 РЕЖИМ DRY RUN - данные не будут сохранены в базу!")

        # Предзагружаем существующие данные
        self.preload_existing_data()

        successful_updates = 0
        failed_updates = 0

        for i, game in enumerate(games, 1):
            if debug:
                self.stdout.write(f"\n[{i}/{len(games)}] Обработка: {game.name}")

            try:
                if dry_run:
                    self.dry_run_update_single_game(game, debug)
                else:
                    self.update_single_game_with_additional_data(game, debug)
                successful_updates += 1
            except Exception as e:
                failed_updates += 1
                self.stderr.write(f"❌ Ошибка обновления {game.name}: {e}")

            # Задержка между запросами
            if i < len(games) and not dry_run:
                time.sleep(delay)

        self.stdout.write(f"\n✅ Обновлено игр: {successful_updates}")
        self.stdout.write(f"❌ Ошибок: {failed_updates}")

    def update_single_game_with_additional_data(self, game, debug=False):
        """Обновляет одну игру дополнительными данными"""
        # Получаем полные данные об игре из IGDB
        query = f'''
            fields collections,franchises,involved_companies.company,
                   involved_companies.developer,involved_companies.publisher,
                   themes,player_perspectives,game_modes;
            where id = {game.igdb_id};
        '''

        game_data = make_igdb_request('games', query, debug=debug)
        if not game_data:
            if debug:
                self.stdout.write(f"   ⏭️  Нет дополнительных данных для {game.name}")
            return

        game_data = game_data[0]

        with transaction.atomic():
            updated = False

            # Серия
            if game_data.get('collections'):
                collection_id = game_data['collections'][0]
                series = self.get_or_create_series(collection_id)
                if series and game.series != series:
                    game.series = series
                    updated = True
                    if debug:
                        self.stdout.write(f"   📚 Добавлена серия: {series.name}")

            # Разработчики и издатели
            developer_ids = []
            publisher_ids = []

            if game_data.get('involved_companies'):
                for company_data in game_data['involved_companies']:
                    company_id = company_data['company']
                    if company_data.get('developer', False):
                        developer_ids.append(company_id)
                    if company_data.get('publisher', False):
                        publisher_ids.append(company_id)

                # Получаем объекты компаний
                developers = [self.get_or_create_company(cid) for cid in developer_ids]
                publishers = [self.get_or_create_company(cid) for cid in publisher_ids]

                if developers:
                    game.developers.set(developers)
                    updated = True
                    if debug:
                        dev_names = [d.name for d in developers]
                        self.stdout.write(f"   🏢 Добавлены разработчики: {', '.join(dev_names)}")

                if publishers:
                    game.publishers.set(publishers)
                    updated = True
                    if debug:
                        pub_names = [p.name for p in publishers]
                        self.stdout.write(f"   📦 Добавлены издатели: {', '.join(pub_names)}")

            # Темы
            if game_data.get('themes'):
                themes = [self.get_or_create_theme(tid) for tid in game_data['themes']]
                if themes:
                    game.themes.set(themes)
                    updated = True
                    if debug:
                        theme_names = [t.name for t in themes]
                        self.stdout.write(f"   🎨 Добавлены темы: {', '.join(theme_names)}")

            # Перспективы
            if game_data.get('player_perspectives'):
                perspectives = [self.get_or_create_perspective(pid) for pid in game_data['player_perspectives']]
                if perspectives:
                    game.player_perspectives.set(perspectives)
                    updated = True
                    if debug:
                        perspective_names = [p.name for p in perspectives]
                        self.stdout.write(f"   👁️ Добавлены перспективы: {', '.join(perspective_names)}")

            # Режимы игры
            if game_data.get('game_modes'):
                modes = [self.get_or_create_mode(mid) for mid in game_data['game_modes']]
                if modes:
                    game.game_modes.set(modes)
                    updated = True
                    if debug:
                        mode_names = [m.name for m in modes]
                        self.stdout.write(f"   🎮 Добавлены режимы: {', '.join(mode_names)}")

            if updated:
                game.save()
                if debug:
                    self.stdout.write(f"   ✅ Игра обновлена: {game.name}")

    def dry_run_update_single_game(self, game, debug=False):
        """Показывает какие данные будут обновлены без сохранения"""
        self.stdout.write(f"\n🎮 ИГРА: {game.name} (ID: {game.igdb_id})")
        self.stdout.write("-" * 40)

        # Получаем данные из IGDB
        query = f'''
            fields collections,franchises,involved_companies.company,
                   involved_companies.developer,involved_companies.publisher,
                   themes,player_perspectives,game_modes;
            where id = {game.igdb_id};
        '''

        game_data = make_igdb_request('games', query, debug=debug)
        if not game_data:
            self.stdout.write("❌ Нет дополнительных данных в IGDB")
            return

        game_data = game_data[0]

        # Серия
        if game_data.get('collections'):
            collection_id = game_data['collections'][0]
            series_data = self.fetch_collections_data([collection_id])
            if series_data:
                series_name = series_data[0].get('name', 'Unknown')
                self.stdout.write(f"📚 Серия: БУДЕТ ДОБАВЛЕНА - {series_name}")
            else:
                self.stdout.write("📚 Серия: не удалось получить данные")
        else:
            self.stdout.write("📚 Серия: нет данных в IGDB")

        # Разработчики
        developer_names = []
        if game_data.get('involved_companies'):
            developer_ids = []
            for company in game_data['involved_companies']:
                if company.get('developer', False):
                    developer_ids.append(company['company'])

            if developer_ids:
                developers_data = self.fetch_companies_data(developer_ids)
                developer_names = [d.get('name', 'Unknown') for d in developers_data]
                self.stdout.write(f"🏢 Разработчики: БУДУТ ДОБАВЛЕНЫ - {', '.join(developer_names)}")
            else:
                self.stdout.write("🏢 Разработчики: нет developer компаний")
        else:
            self.stdout.write("🏢 Разработчики: нет данных в IGDB")

        # Издатели
        publisher_names = []
        if game_data.get('involved_companies'):
            publisher_ids = []
            for company in game_data['involved_companies']:
                if company.get('publisher', False):
                    publisher_ids.append(company['company'])

            if publisher_ids:
                publishers_data = self.fetch_companies_data(publisher_ids)
                publisher_names = [p.get('name', 'Unknown') for p in publishers_data]
                self.stdout.write(f"📦 Издатели: БУДУТ ДОБАВЛЕНЫ - {', '.join(publisher_names)}")
            else:
                self.stdout.write("📦 Издатели: нет publisher компаний")
        else:
            self.stdout.write("📦 Издатели: нет данных в IGDB")

        # Темы
        theme_names = []
        if game_data.get('themes'):
            themes_data = self.fetch_themes_data(game_data['themes'])
            theme_names = [t.get('name', 'Unknown') for t in themes_data]
            self.stdout.write(f"🎨 Темы: БУДУТ ДОБАВЛЕНЫ - {', '.join(theme_names)}")
        else:
            self.stdout.write("🎨 Темы: нет данных в IGDB")

        # Перспективы
        perspective_names = []
        if game_data.get('player_perspectives'):
            perspectives_data = self.fetch_perspectives_data(game_data['player_perspectives'])
            perspective_names = [p.get('name', 'Unknown') for p in perspectives_data]
            self.stdout.write(f"👁️ Перспективы: БУДУТ ДОБАВЛЕНЫ - {', '.join(perspective_names)}")
        else:
            self.stdout.write("👁️ Перспективы: нет данных в IGDB")

        # Режимы игры
        mode_names = []
        if game_data.get('game_modes'):
            modes_data = self.fetch_game_modes_data(game_data['game_modes'])
            mode_names = [m.get('name', 'Unknown') for m in modes_data]
            self.stdout.write(f"🎮 Режимы: БУДУТ ДОБАВЛЕНЫ - {', '.join(mode_names)}")
        else:
            self.stdout.write("🎮 Режимы: нет данных в IGDB")

        # Текущие данные в базе (для сравнения)
        self.stdout.write("\n📊 ТЕКУЩИЕ ДАННЫЕ В БАЗЕ:")
        self.stdout.write(f"📚 Серия: {game.series.name if game.series else 'нет'}")
        self.stdout.write(f"🏢 Разработчики: {', '.join(game.developer_names) if game.developers.exists() else 'нет'}")
        self.stdout.write(f"📦 Издатели: {', '.join(game.publisher_names) if game.publishers.exists() else 'нет'}")
        self.stdout.write(f"🎨 Темы: {', '.join(game.theme_names) if game.themes.exists() else 'нет'}")
        self.stdout.write(
            f"👁️ Перспективы: {', '.join(game.perspective_names) if game.player_perspectives.exists() else 'нет'}")
        self.stdout.write(f"🎮 Режимы: {', '.join(game.game_mode_names) if game.game_modes.exists() else 'нет'}")

    # Вспомогательные методы для получения и создания объектов
    def get_or_create_series(self, series_id):
        """Получает или создает серию"""
        if series_id in self.existing_series:
            return self.existing_series[series_id]

        series_data = self.fetch_collections_data([series_id])
        if series_data:
            series = Series(
                igdb_id=series_id,
                name=series_data[0].get('name', ''),
                description=''
            )
            series.save()
            self.existing_series[series_id] = series
            return series
        return None

    def get_or_create_company(self, company_id):
        """Получает или создает компанию"""
        if company_id in self.existing_companies:
            return self.existing_companies[company_id]

        company_data = self.fetch_companies_data([company_id])
        if company_data:
            data = company_data[0]
            company = Company(
                igdb_id=company_id,
                name=data.get('name', ''),
                description=data.get('description', ''),
                country=data.get('country'),
                logo_url=f"https://images.igdb.com/igdb/image/upload/t_logo_med/{data['logo']['image_id']}.png" if data.get(
                    'logo') else '',
                website=data.get('url', ''),
            )
            company.save()
            self.existing_companies[company_id] = company
            return company
        return None

    def get_or_create_theme(self, theme_id):
        """Получает или создает тему"""
        if theme_id in self.existing_themes:
            return self.existing_themes[theme_id]

        theme_data = self.fetch_themes_data([theme_id])
        if theme_data:
            theme = Theme(
                igdb_id=theme_id,
                name=theme_data[0].get('name', '')
            )
            theme.save()
            self.existing_themes[theme_id] = theme
            return theme
        return None

    def get_or_create_perspective(self, perspective_id):
        """Получает или создает перспективу"""
        if perspective_id in self.existing_perspectives:
            return self.existing_perspectives[perspective_id]

        perspective_data = self.fetch_perspectives_data([perspective_id])
        if perspective_data:
            perspective = PlayerPerspective(
                igdb_id=perspective_id,
                name=perspective_data[0].get('name', '')
            )
            perspective.save()
            self.existing_perspectives[perspective_id] = perspective
            return perspective
        return None

    def get_or_create_mode(self, mode_id):
        """Получает или создает режим игры"""
        if mode_id in self.existing_modes:
            return self.existing_modes[mode_id]

        mode_data = self.fetch_game_modes_data([mode_id])
        if mode_data:
            mode = GameMode(
                igdb_id=mode_id,
                name=mode_data[0].get('name', '')
            )
            mode.save()
            self.existing_modes[mode_id] = mode
            return mode
        return None

    # Методы для получения данных из IGDB
    def fetch_collections_data(self, collection_ids):
        """Получает данные о коллекциях из IGDB"""
        if not collection_ids:
            return []

        fields = "id,name"
        query = f'fields {fields}; where id = ({",".join(map(str, collection_ids))});'
        try:
            return make_igdb_request('collections', query)
        except Exception as e:
            self.stdout.write(f"⚠️ Ошибка при получении данных коллекций: {e}")
            return []

    def fetch_companies_data(self, company_ids):
        """Получает данные о компаниях из IGDB"""
        if not company_ids:
            return []

        fields = "id,name,description,country,logo.image_id,url"
        query = f'fields {fields}; where id = ({",".join(map(str, company_ids))});'
        try:
            return make_igdb_request('companies', query)
        except Exception as e:
            self.stdout.write(f"⚠️ Ошибка при получении данных компаний: {e}")
            return []

    def fetch_themes_data(self, theme_ids):
        """Получает данные о темах из IGDB"""
        return self._fetch_simple_objects('themes', theme_ids)

    def fetch_perspectives_data(self, perspective_ids):
        """Получает данные о перспективах из IGDB"""
        return self._fetch_simple_objects('player_perspectives', perspective_ids)

    def fetch_game_modes_data(self, mode_ids):
        """Получает данные о режимах игры из IGDB"""
        return self._fetch_simple_objects('game_modes', mode_ids)

    def _fetch_simple_objects(self, endpoint, object_ids):
        """Универсальный метод для получения простых объектов"""
        if not object_ids:
            return []

        fields = "id,name"
        query = f'fields {fields}; where id = ({",".join(map(str, object_ids))});'
        try:
            return make_igdb_request(endpoint, query)
        except:
            return []

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
        single_game = options['single_game']
        update_existing = options['update_existing']
        delay = options['delay']
        dry_run = options['dry_run']
        overwrite = options.get('overwrite', False)  # Новая опция

        set_debug_mode(debug)

        self.stdout.write('🎮 ЗАГРУЗКА ИГР ИЗ IGDB')
        self.stdout.write('=' * 60)

        if dry_run:
            self.stdout.write('🚧 РЕЖИМ DRY RUN - данные не будут сохранены в базу!')

        if overwrite:
            self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ - существующие игры будут удалены и загружены заново!')

        if no_screenshots:
            self.stdout.write('📸 ЗАГРУЗКА СКРИНШОТОВ: ОТКЛЮЧЕНА')
        else:
            screenshot_limit = "все" if max_screenshots == 0 else max_screenshots
            self.stdout.write(f'📸 ЗАГРУЗКА СКРИНШОТОВ: ВКЛЮЧЕНА (макс: {screenshot_limit})')

        # ПРОВЕРКА ОПЦИЙ
        load_methods = [input_file, tactical_rpg, genre_id, keyword_id, single_game, update_existing]
        if sum(1 for method in load_methods if method) != 1:
            self.stderr.write('❌ Укажите ровно один способ загрузки:')
            self.stderr.write('   --single-game NAME     - загрузка одной игры по названию')
            self.stderr.write('   --input-file FILE      - загрузка из файла')
            self.stderr.write('   --tactical-rpg         - загрузка тактических RPG')
            self.stderr.write('   --genre-id ID          - загрузка по жанру')
            self.stderr.write('   --keyword-id ID        - загрузка по ключевому слову')
            self.stderr.write('   --update-existing      - обновить существующие игры')
            return

        try:
            all_games = []
            not_found_games = []

            if single_game:
                # ЗАГРУЗКА ОДНОЙ ИГРЫ ПО НАЗВАНИЮ
                self.stdout.write(f'🎯 Загрузка одной игры: {single_game}')
                game_data = self.search_single_game_by_name(single_game, debug)
                if game_data:
                    all_games = [game_data]

                    # ПРОВЕРКА ПЕРЕЗАПИСИ ДЛЯ ОДНОЙ ИГРЫ
                    if overwrite:
                        game_id = game_data.get('id')
                        existing_game = Game.objects.filter(igdb_id=game_id).first()
                        if existing_game:
                            if dry_run:
                                self.stdout.write(
                                    f'   🗑️  DRY RUN: Будет удалена существующая игра "{existing_game.name}"')
                            else:
                                self.stdout.write(f'   🗑️  Удаление существующей игры: {existing_game.name}')
                                existing_game.delete()
                else:
                    self.stdout.write(f'❌ Игра не найдена: {single_game}')
                    return

            elif input_file:
                # ЗАГРУЗКА ИЗ ФАЙЛА - ИСПОЛЬЗУЕМ МАССОВЫЙ ПОИСК
                self.stdout.write(f'📁 Загрузка из файла: {input_file}')
                with open(input_file, 'r', encoding='utf-8') as f:
                    game_names = [line.strip() for line in f if line.strip()]

                self.stdout.write(f'🔍 Массовый поиск {len(game_names)} игр...')
                all_games, not_found_games = self.search_games_by_exact_name_batch(game_names, debug)

                # ПРОВЕРКА ПЕРЕЗАПИСИ ДЛЯ МАССОВОЙ ЗАГРУЗКИ
                if overwrite and all_games:
                    game_ids = [game['id'] for game in all_games if game.get('id')]
                    if game_ids:
                        existing_games = Game.objects.filter(igdb_id__in=game_ids)
                        if dry_run:
                            self.stdout.write(
                                f'   🗑️  DRY RUN: Будет удалено {existing_games.count()} существующих игр')
                        else:
                            deleted_count, _ = existing_games.delete()
                            self.stdout.write(f'   🗑️  Удалено существующих игр: {deleted_count}')

            elif tactical_rpg:
                # ЗАГРУЗКА ТАКТИЧЕСКИХ RPG
                all_games = self.load_tactical_rpg_games(batch_size, debug)

            elif genre_id:
                # ЗАГРУЗКА ПО ЖАНРУ
                all_games = self.load_games_by_genre(genre_id, batch_size, debug)

            elif keyword_id:
                # ЗАГРУЗКА ПО КЛЮЧЕВОМУ СЛОВУ
                all_games = self.load_games_by_keyword(keyword_id, batch_size, debug)

            elif update_existing:
                # ОБНОВЛЕНИЕ СУЩЕСТВУЮЩИХ ИГР
                games_to_update = Game.objects.all()
                if skip_existing:
                    games_to_update = games_to_update.filter(
                        series__isnull=True,
                        developers__isnull=True,
                        publishers__isnull=True
                    )
                self.stdout.write(f'🔄 Обновление {games_to_update.count()} существующих игр...')
                self.update_games_with_additional_data(games_to_update, delay, dry_run, debug)
                return

            if not all_games and not input_file:
                self.stdout.write('❌ Не найдено игр для загрузки')
                return

            self.stdout.write(f'📥 Найдено игр для обработки: {len(all_games)}')

            # ЗАГРУЗКА В БАЗУ
            loaded_count = 0
            skipped_count = 0
            error_count = 0
            screenshots_loaded = 0
            deleted_count = 0

            for i, game_data in enumerate(all_games, 1):
                game_id = game_data.get('id')
                game_name = game_data.get('name', 'Unknown')

                if not game_id:
                    error_count += 1
                    continue

                game_exists = Game.objects.filter(igdb_id=game_id).exists()

                # ОБРАБОТКА ПЕРЕЗАПИСИ
                if overwrite and game_exists:
                    existing_game = Game.objects.filter(igdb_id=game_id).first()
                    if existing_game:
                        if dry_run:
                            self.stdout.write(f'   🗑️  DRY RUN: Будет удалена игра "{existing_game.name}"')
                            deleted_count += 1
                        else:
                            self.stdout.write(f'   🗑️  Удаление: {existing_game.name}')
                            existing_game.delete()
                            deleted_count += 1
                    # После удаления игра больше не существует
                    game_exists = False

                if skip_existing and game_exists:
                    skipped_count += 1
                    if debug and i % 50 == 0:
                        self.stdout.write(f'   ⏭️  Уже в базе: {game_name}')
                    continue

                if not game_exists or overwrite:
                    if debug or i % 20 == 0 or dry_run:
                        action = "Будет загружена" if dry_run else "Загрузка"
                        self.stdout.write(f'   [{i}/{len(all_games)}] {action}: {game_name}')

                    if dry_run:
                        # DRY RUN - только показываем что будет сделано
                        loaded_count += 1
                        if not no_screenshots and max_screenshots != 0:
                            screenshots_text = f" (скриншоты: до {max_screenshots})" if max_screenshots > 0 else " (скриншоты: все)"
                            self.stdout.write(f'   📸 DRY RUN: Будет загружено скриншотов{screenshots_text}')
                    else:
                        # РЕАЛЬНАЯ ЗАГРУЗКА
                        try:
                            result = self.process_game(game_data, debug)
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

            if dry_run:
                self.stdout.write('🚧 РЕЗУЛЬТАТЫ DRY RUN:')
                self.stdout.write(f'• Найдено игр: {len(all_games)}')
                self.stdout.write(f'• Будет загружено: {loaded_count}')
                if overwrite:
                    self.stdout.write(f'• Будет удалено: {deleted_count}')
                self.stdout.write(f'• Будет пропущено: {skipped_count}')
            else:
                self.stdout.write(f'• Найдено игр: {len(all_games)}')
                self.stdout.write(f'• Новых загружено: {loaded_count}')
                if overwrite:
                    self.stdout.write(f'• Удалено существующих: {deleted_count}')
                self.stdout.write(f'• Пропущено: {skipped_count}')
                self.stdout.write(f'• Ошибок: {error_count}')

            if input_file:
                self.stdout.write(f'• Не найдено в IGDB: {len(not_found_games)}')

            if not no_screenshots and not dry_run:
                self.stdout.write(f'• Скриншотов загружено: {screenshots_loaded}')
                total_screenshots = Screenshot.objects.count()
                self.stdout.write(f'• Всего скриншотов в базе: {total_screenshots}')

        except Exception as e:
            self.stderr.write(f'❌ Ошибка: {e}')
            import traceback
            self.stderr.write(traceback.format_exc())
