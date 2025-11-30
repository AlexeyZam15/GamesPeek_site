# management/commands/load_games.py
from django.core.management.base import BaseCommand
from django.db import transaction
from django.utils import timezone
from django.db import models
from games.igdb_api import make_igdb_request, set_debug_mode
from games.models import (
    Game, Genre, Keyword, Platform, Screenshot,
    Series, Company, Theme, PlayerPerspective, GameMode
)
import time
from collections import defaultdict
import os
import concurrent.futures
from threading import Lock

class Command(BaseCommand):
    help = 'Универсальная команда для загрузки и обновления игр из IGDB'

    def add_arguments(self, parser):
        # Основные опции
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
            '--load-screenshots',
            action='store_true',
            help='Загрузить скриншоты для игр (отдельная медленная операция)'
        )
        parser.add_argument(
            '--screenshots-only',
            action='store_true',
            help='Загрузить ТОЛЬКО скриншоты для существующих игр'
        )

        # Режимы загрузки
        load_group = parser.add_mutually_exclusive_group()
        load_group.add_argument(
            '--single-game',
            type=str,
            help='Загрузить одну игру по точному названию'
        )
        load_group.add_argument(
            '--input-file',
            type=str,
            help='Загрузить игры из файла (точные названия на каждой строке)'
        )
        load_group.add_argument(
            '--tactical-rpg',
            action='store_true',
            help='Загрузить тактические RPG по жанру и ключевым словам'
        )
        load_group.add_argument(
            '--genre-id',
            type=int,
            help='Загрузить игры по ID жанра'
        )
        load_group.add_argument(
            '--keyword-id',
            type=int,
            help='Загрузить игры по ID ключевого слова'
        )
        load_group.add_argument(
            '--update-existing',
            action='store_true',
            help='Обновить существующие игры дополнительными данными'
        )
        load_group.add_argument(
            '--update-storylines',
            action='store_true',
            help='Обновить сюжеты для игр'
        )

        # Опции фильтрации
        parser.add_argument(
            '--skip-existing',
            action='store_true',
            help='Пропускать игры, которые уже есть в базе'
        )
        parser.add_argument(
            '--missing-only',
            action='store_true',
            help='Обновлять только игры с отсутствующими данными'
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Удалить существующие игры и загрузить заново'
        )
        parser.add_argument(
            '--start-from',
            type=int,
            default=0,
            help='Начать с определенного индекса игры'
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Ограничить количество обрабатываемых игр'
        )
        parser.add_argument(
            '--game-ids',
            type=str,
            help='Обновить только конкретные игры (через запятую)'
        )

        # Опции данных
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
            '--skip-no-data',
            action='store_true',
            help='Пропускать игры для которых в IGDB нет данных'
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
                fields name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
                where {name_conditions};
                limit {len(batch) * 2};
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
            fields name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
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

    def load_game_screenshots(self, game_id, max_screenshots=0, debug=False):
        """Загружает скриншоты для конкретной игры с защитой от ошибок"""
        try:
            # Сначала проверяем, что игра существует в базе
            if not Game.objects.filter(igdb_id=game_id).exists():
                if debug:
                    self.stdout.write(f'   ⏭️  Игра {game_id} не найдена в базе, пропускаем скриншоты')
                return 0

            # ИСПРАВЛЕНИЕ: Ограничиваем лимит до 500
            limit = 500 if max_screenshots == 0 else min(max_screenshots, 500)

            # ИСПРАВЛЕННЫЙ ЗАПРОС: убираем поле caption
            query = f'''
                fields game,id,url,width,height;
                where game = {game_id};
                limit {limit};
            '''

            screenshots_data = make_igdb_request('screenshots', query, debug=False)

            if not screenshots_data:
                if debug:
                    self.stdout.write(f'   ℹ️  Нет скриншотов для игры {game_id}')
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
                    caption=''
                )

                screenshot.save()
                loaded_screenshots += 1

                # Если достигли максимального количества, прерываем
                if max_screenshots > 0 and loaded_screenshots >= max_screenshots:
                    break

            if debug and loaded_screenshots > 0:
                self.stdout.write(f'   📸 Загружено скриншотов: {loaded_screenshots}')

            return loaded_screenshots

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка загрузки скриншотов для игры {game_id}: {e}')
            return 0

    def process_game(self, game_data, debug=False):
        """ОПТИМИЗИРОВАННАЯ обработка одной игры"""
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
                self.process_genres(game, game_data['genres'], debug)

            if game_data.get('keywords'):
                self.process_keywords(game, game_data['keywords'], debug)

            if game_data.get('platforms'):
                self.process_platforms(game, game_data['platforms'], debug)

            if debug:
                self.stdout.write(f'   ✅ ЗАГРУЖЕНА: {game_name}')

            return 'created'

        except Exception as e:
            if debug:
                self.stderr.write(f'❌ Ошибка обработки {game_name}: {e}')
            return 'error'

    def process_genres_batch(self, game, genre_ids, debug=False):
        """Массовая обработка жанров"""
        if not genre_ids:
            return []

        try:
            # Массовый запрос для всех жанров
            id_list = ','.join(map(str, genre_ids))
            query = f'fields id,name; where id = ({id_list});'

            genre_data = make_igdb_request('genres', query, debug=False)
            if not genre_data:
                return []

            # Массовое создание/получение жанров
            genres_to_add = []
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
                genres_to_add.append(genre)

            # Один запрос для установки всех жанров
            game.genres.set(genres_to_add)
            return genres_to_add

        except Exception as e:
            if debug:
                self.stderr.write(f'❌ Ошибка обработки жанров для игры {game.name}: {e}')
            return []

    def process_keywords_batch(self, game, keyword_ids, debug=False):
        """Массовая обработка ключевых слов"""
        if not keyword_ids:
            return []

        try:
            # Массовый запрос для всех ключевых слов
            id_list = ','.join(map(str, keyword_ids))
            query = f'fields id,name; where id = ({id_list});'

            keyword_data = make_igdb_request('keywords', query, debug=False)
            if not keyword_data:
                return []

            # Массовое создание/получение ключевых слов
            keywords_to_add = []
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
                keywords_to_add.append(keyword)

            # Один запрос для установки всех ключевых слов
            game.keywords.set(keywords_to_add)
            return keywords_to_add

        except Exception as e:
            if debug:
                self.stderr.write(f'❌ Ошибка обработки ключевых слов для игры {game.name}: {e}')
            return []

    def process_platforms_batch(self, game, platform_ids, debug=False):
        """Массовая обработка платформ"""
        if not platform_ids:
            return []

        try:
            # Массовый запрос для всех платформ
            id_list = ','.join(map(str, platform_ids))
            query = f'fields id,name; where id = ({id_list});'

            platform_data = make_igdb_request('platforms', query, debug=False)
            if not platform_data:
                return []

            # Массовое создание/получение платформ
            platforms_to_add = []
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
                platforms_to_add.append(platform)

            # Один запрос для установки всех платформ
            game.platforms.set(platforms_to_add)
            return platforms_to_add

        except Exception as e:
            if debug:
                self.stderr.write(f'❌ Ошибка обработки платформ для игры {game.name}: {e}')
            return []

    def get_cover_url(self, cover_id, debug=False):
        """ЗАМЕНА: Теперь используем массовую загрузку"""
        # Этот метод теперь будет использоваться только для одиночных случаев
        if not cover_id:
            return None

        try:
            query = f'fields url; where id = {cover_id};'
            result = make_igdb_request('covers', query, debug=False)
            if result and 'url' in result[0]:
                url = result[0]['url']
                if url:
                    return f"https:{url.replace('thumb', 'cover_big')}"
        except Exception as e:
            if debug:
                self.stdout.write(f'   ⚠️  Ошибка получения обложки ID {cover_id}: {e}')
        return None

    def get_covers_batch(self, cover_ids, debug=False):
        """Массовая загрузка обложек - ПАЧКАМИ ПО 10 С ПРОГРЕССОМ"""
        if not cover_ids:
            return {}

        try:
            # Убираем дубликаты и разбиваем на пачки по 10
            unique_cover_ids = list(set(cover_ids))
            batch_size = 10
            cover_batches = [unique_cover_ids[i:i + batch_size] for i in range(0, len(unique_cover_ids), batch_size)]

            cover_map = {}
            lock = Lock()
            processed_batches = 0

            def process_batch(batch_data):
                batch_num, batch_cover_ids = batch_data
                try:
                    if debug:
                        self.stdout.write(
                            f'      🖼️  Пачка {batch_num}/{len(cover_batches)}: загрузка {len(batch_cover_ids)} обложек...')

                    id_list = ','.join(map(str, batch_cover_ids))
                    query = f'fields id,url,image_id; where id = ({id_list});'

                    batch_covers = make_igdb_request('covers', query, debug=False)

                    with lock:
                        for cover in batch_covers:
                            cover_id = cover.get('id')
                            if not cover_id:
                                continue

                            # ПРИОРИТЕТ: используем image_id для построения URL
                            if cover.get('image_id'):
                                high_res_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{cover['image_id']}.jpg"
                                cover_map[cover_id] = high_res_url
                            elif cover.get('url'):
                                url = cover['url']
                                high_res_url = f"https:{url.replace('thumb', 'cover_big')}"
                                cover_map[cover_id] = high_res_url

                        nonlocal processed_batches
                        processed_batches += 1
                        if debug:
                            self.stdout.write(f'      ✅ Пачка {batch_num} завершена: {len(batch_covers)} обложек')

                except Exception as e:
                    if debug:
                        self.stderr.write(f'      ❌ Ошибка пачки {batch_num}: {e}')

            if debug:
                self.stdout.write(f'   🖼️  Начало загрузки обложек: {len(cover_batches)} пачек по {batch_size} ID')

            # ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА с номерами пачек
            batch_data = [(i + 1, batch) for i, batch in enumerate(cover_batches)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                executor.map(process_batch, batch_data)

            if debug:
                self.stdout.write(f'   ✅ Загрузка обложек завершена: {len(cover_map)}/{len(unique_cover_ids)} обложек')

            return cover_map

        except Exception as e:
            if debug:
                self.stdout.write(f'   ⚠️ Ошибка массовой загрузки обложек: {e}')
            return {}

    def load_tactical_rpg_games(self, batch_size, debug=False):
        """Загрузка тактических RPG по жанру и ключевым словам"""
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

        # Строим условие WHERE: RPG И (Tactical ИЛИ tactical turn-based combat)
        where_conditions = []
        if tactical_genre_id:
            where_conditions.append(f'genres = ({tactical_genre_id})')
        if tactical_keyword_id:
            where_conditions.append(f'keywords = ({tactical_keyword_id})')

        where_clause = ' | '.join(where_conditions)
        full_where = f'genres = (12) & ({where_clause})'

        if debug:
            self.stdout.write(f'   🎯 Запрос: RPG И (Tactical ИЛИ tactical turn-based combat)')
            if tactical_genre_id:
                self.stdout.write(f'   ✅ Жанр Tactical: ID {tactical_genre_id}')
            if tactical_keyword_id:
                self.stdout.write(f'   ✅ Ключевое слово: ID {tactical_keyword_id}')
            self.stdout.write(f'   📋 SQL: {full_where}')

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
        """Оптимизированная загрузка игр с ПАГИНАЦИЕЙ"""
        all_games = []
        offset = 0
        has_more_games = True
        max_limit = 500  # Максимальный лимит IGDB
        total_loaded = 0

        if debug:
            self.stdout.write('⚡ ОПТИМИЗИРОВАННАЯ ЗАГРУЗКА С ПАГИНАЦИЕЙ...')

        while has_more_games:
            if debug:
                self.stdout.write(f'   📦 Пачка {offset // 500 + 1}: {offset}-{offset + max_limit}...')

            query = f'''
                fields name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
                where {where_clause};
                sort rating_count desc;
                limit {max_limit};
                offset {offset};
            '''.strip()

            batch_games = make_igdb_request('games', query, debug=False)

            if not batch_games:
                if debug:
                    self.stdout.write('   💤 Больше игр нет')
                break

            all_games.extend(batch_games)
            total_loaded += len(batch_games)

            if debug:
                self.stdout.write(f'   ✅ Загружено: {len(batch_games)} игр')
                # ПОКАЗЫВАЕМ ВСЕ ИГРЫ ИЗ ПАЧКИ
                for i, game in enumerate(batch_games, 1):
                    game_name = game.get('name', 'Unknown')
                    rating = game.get('rating', 'N/A')
                    genres_count = len(game.get('genres', []))
                    platforms_count = len(game.get('platforms', []))

                    self.stdout.write(f'      {offset + i}. {game_name} | '
                                      f'Рейтинг: {rating} | '
                                      f'Жанры: {genres_count} | '
                                      f'Платформы: {platforms_count}')

            offset += len(batch_games)

            # Если получили меньше игр чем лимит, значит это последняя пачка
            if len(batch_games) < max_limit:
                has_more_games = False
                if debug:
                    self.stdout.write(f'   🏁 Завершено. Всего пачек: {offset // 500 + 1}')

        if debug:
            self.stdout.write(f'   📊 ВСЕГО НАЙДЕНО: {len(all_games)} игр')

        return all_games

    def update_games_with_additional_data(self, games, delay=0.2, dry_run=False, debug=False, skip_no_data=False):
        """Обновляет игры дополнительными данными: серии, разработчики, издатели, темы, перспективы, режимы"""
        self.stdout.write(f"🔄 Обновление {len(games)} игр дополнительными данными...")

        if dry_run:
            self.stdout.write("🚧 РЕЖИМ DRY RUN - данные не будут сохранены в базу!")

        # Предзагружаем существующие данные
        self.preload_existing_data()

        successful_updates = 0
        failed_updates = 0
        skipped_no_data = 0

        for i, game in enumerate(games, 1):
            if debug:
                self.stdout.write(f"\n[{i}/{len(games)}] Обработка: {game.name}")

            try:
                if dry_run:
                    self.dry_run_update_single_game(game, debug)
                    successful_updates += 1
                else:
                    updated = self.update_single_game_with_additional_data(game, debug)
                    if updated:
                        successful_updates += 1
                    elif skip_no_data:
                        skipped_no_data += 1
            except Exception as e:
                failed_updates += 1
                self.stderr.write(f"❌ Ошибка обновления {game.name}: {e}")

            # Задержка между запросами
            if i < len(games) and not dry_run:
                time.sleep(delay)

        self.stdout.write(f"\n✅ Обновлено игр: {successful_updates}")
        if skipped_no_data > 0:
            self.stdout.write(f"⏭️  Пропущено (нет данных): {skipped_no_data}")
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
            return False

        game_data = game_data[0]

        # Проверяем, есть ли данные для обновления
        if not self.has_additional_data(game_data):
            if debug:
                self.stdout.write(f"   ⏭️  Нет данных для обновления {game.name}")
            return False

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
                return True

        return False

    def has_additional_data(self, game_data):
        """Проверяет, есть ли дополнительные данные в IGDB"""
        has_collections = bool(game_data.get('collections'))
        has_companies = bool(game_data.get('involved_companies'))
        has_themes = bool(game_data.get('themes'))
        has_perspectives = bool(game_data.get('player_perspectives'))
        has_modes = bool(game_data.get('game_modes'))

        return any([has_collections, has_companies, has_themes, has_perspectives, has_modes])

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
            self.stdout.write(f"👁️ Перспективы: БУДЕТ ДОБАВЛЕНЫ - {', '.join(perspective_names)}")
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

    def update_storylines(self, batch_size=10, delay=0.2, missing_only=False, debug=False):
        """Обновляет сюжеты для игр"""
        self.stdout.write('🔄 Starting storyline update...')

        # Какие игры обновляем
        if missing_only:
            games = Game.objects.filter(
                models.Q(storyline__isnull=True) | models.Q(storyline='')
            )
            self.stdout.write(f'📝 Found {games.count()} games with missing storylines')
        else:
            games = Game.objects.all()
            self.stdout.write(f'📝 Processing all {games.count()} games')

        # Мапа id → name
        game_map = dict(games.values_list('igdb_id', 'name'))
        game_ids = list(game_map.keys())
        total_games = len(game_ids)

        updated_count = 0
        not_found_count = 0
        error_batches = 0

        # Обработка батчами
        for i in range(0, total_games, batch_size):
            batch_ids = game_ids[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (total_games + batch_size - 1) // batch_size

            self.stdout.write(f'\n🔄 Batch {batch_num}/{total_batches} — {len(batch_ids)} games')

            # IGDB запрос
            fields = "id,name,storyline"
            query = f'fields {fields}; where id = ({",".join(map(str, batch_ids))});'

            try:
                response = make_igdb_request('games', query, debug=debug)
            except Exception as e:
                error_batches += 1
                self.stdout.write(self.style.ERROR(f'❌ IGDB request failed: {e}'))
                continue

            returned_ids = set()

            # Обновление игр
            for g in response:
                gid = g["id"]
                returned_ids.add(gid)

                storyline = g.get("storyline") or ""
                igdb_name = g.get("name", f"ID {gid}")

                try:
                    game = Game.objects.get(igdb_id=gid)
                except Game.DoesNotExist:
                    not_found_count += 1
                    self.stdout.write(self.style.WARNING(
                        f'⚠️ Game not in DB: {igdb_name}'
                    ))
                    continue

                # Обновление
                if storyline and game.storyline != storyline:
                    game.storyline = storyline
                    game.save(update_fields=['storyline'])
                    updated_count += 1

                    if debug:
                        self.stdout.write(self.style.SUCCESS(f'✅ Updated: {game.name}'))
                else:
                    if debug:
                        self.stdout.write(f'ℹ️ No changes: {game.name}')

            # Уведомление о пропавших ID
            missing_igdb = set(batch_ids) - returned_ids
            for mid in missing_igdb:
                name = game_map.get(mid, f"ID {mid}")
                if debug:
                    self.stdout.write(f'ℹ️ No IGDB data for {name}')

            if delay > 0 and i + batch_size < total_games:
                time.sleep(delay)

        # Итоги
        self.stdout.write('\n' + '=' * 50)
        self.stdout.write(self.style.SUCCESS(f'✅ Updated: {updated_count}'))
        self.stdout.write(self.style.WARNING(f'⚠️ Missing in DB: {not_found_count}'))
        self.stdout.write(self.style.ERROR(f'❌ Failed batches: {error_batches}'))
        self.stdout.write(self.style.SUCCESS(f'🎉 Done! {total_games} games processed.'))

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

    def mass_overwrite_games(self, batch_size=100, delay=0.05, dry_run=False, debug=False):
        """ОПТИМИЗИРОВАННАЯ перезапись всех игр"""
        self.stdout.write("🔥 БЫСТРАЯ ПЕРЕЗАПИСЬ ВСЕХ ИГР")
        self.stdout.write("=" * 50)

        # Получаем только ID и названия игр
        all_games = Game.objects.all().only('id', 'igdb_id', 'name')
        total_games = all_games.count()
        game_names = list(all_games.values_list('name', flat=True))

        self.stdout.write(f"📥 Найдено игр в базе: {total_games}")

        if dry_run:
            self.stdout.write("🚧 РЕЖИМ DRY RUN - игры не будут перезаписаны!")
            return

        # БЫСТРОЕ УДАЛЕНИЕ - одним запросом
        self.stdout.write("🗑️  Мгновенное удаление всех игр...")
        deleted_count, _ = Game.objects.all().delete()
        self.stdout.write(f"✅ Удалено игр: {deleted_count}")

        # Массовая загрузка игр
        self.stdout.write("🔄 Массовая загрузка игр из IGDB...")

        # Загружаем ВСЕ игры одним батчем (если меньше 500)
        if len(game_names) <= 500:
            all_games_data, not_found_games = self.search_games_by_exact_name_batch(game_names, debug)
        else:
            # Если больше 500, разбиваем на батчи по 500
            all_games_data = []
            not_found_games = []
            igdb_batch_size = 500

            for i in range(0, len(game_names), igdb_batch_size):
                batch_names = game_names[i:i + igdb_batch_size]
                batch_games, batch_not_found = self.search_games_by_exact_name_batch(batch_names, False)
                all_games_data.extend(batch_games)
                not_found_games.extend(batch_not_found)

                if debug:
                    self.stdout.write(f"🔍 Батч {(i // igdb_batch_size) + 1}: загружено {len(batch_games)} игр")

        self.stdout.write(f"📥 Найдено в IGDB: {len(all_games_data)}")
        self.stdout.write(f"❌ Не найдено: {len(not_found_games)}")

        # БЫСТРАЯ загрузка с массовыми операциями
        loaded_count = 0
        error_count = 0

        # Обрабатываем игры большими батчами
        fast_batch_size = min(200, len(all_games_data))

        for i in range(0, len(all_games_data), fast_batch_size):
            batch = all_games_data[i:i + fast_batch_size]
            batch_num = (i // fast_batch_size) + 1
            total_batches = (len(all_games_data) + fast_batch_size - 1) // fast_batch_size

            if debug:
                self.stdout.write(f"\n⚡ Батч {batch_num}/{total_batches}: {len(batch)} игр")

            batch_loaded = 0
            batch_errors = 0

            # Массово создаем игры
            games_to_create = []
            for game_data in batch:
                try:
                    game = Game(
                        igdb_id=game_data['id'],
                        name=game_data.get('name', ''),
                        summary=game_data.get('summary', ''),
                        storyline=game_data.get('storyline', ''),
                        rating=game_data.get('rating'),
                        rating_count=game_data.get('rating_count', 0)
                    )

                    if game_data.get('first_release_date'):
                        from datetime import datetime
                        naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                        game.first_release_date = timezone.make_aware(naive_datetime)

                    if game_data.get('cover'):
                        cover_url = self.get_cover_url(game_data['cover'])
                        if cover_url:
                            game.cover_url = cover_url

                    games_to_create.append(game)
                    batch_loaded += 1

                except Exception as e:
                    batch_errors += 1
                    if debug:
                        self.stderr.write(f'   ❌ Ошибка создания: {game_data.get("name", "Unknown")} - {e}')

            # МАССОВОЕ СОХРАНЕНИЕ
            if games_to_create:
                Game.objects.bulk_create(games_to_create, batch_size=100)
                loaded_count += batch_loaded

            error_count += batch_errors

            if debug:
                self.stdout.write(f"   ✅ Создано: {batch_loaded}, Ошибок: {batch_errors}")

        self.stdout.write(f"\n🎉 ПЕРЕЗАПИСЬ ЗАВЕРШЕНА!")
        self.stdout.write(f"✅ Игр загружено: {loaded_count}")
        self.stdout.write(f"❌ Ошибок: {error_count}")

    def process_game_optimized(self, game_data, debug=False):
        """Оптимизированная обработка игры"""
        igdb_id = game_data['id']
        game_name = game_data.get('name', 'Unknown')

        try:
            # Проверяем не была ли игра уже загружена (на случай дубликатов)
            if Game.objects.filter(igdb_id=igdb_id).exists():
                if debug:
                    self.stdout.write(f'   ⏭️  Уже загружена: {game_name}')
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

            # Базовые связанные данные (можно пропустить для скорости)
            if game_data.get('genres'):
                self.process_genres(game, game_data['genres'])

            if game_data.get('keywords'):
                self.process_keywords(game, game_data['keywords'])

            if game_data.get('platforms'):
                self.process_platforms(game, game_data['platforms'])

            if debug and game.id % 100 == 0:  # Логируем каждую 100-ю игру
                self.stdout.write(f'   ✅ Загружена: {game_name}')

            return 'created'

        except Exception as e:
            if debug:
                self.stderr.write(f'❌ Ошибка обработки {game_name}: {e}')
            return 'error'

    def mass_process_games(self, all_games_data, debug=False):
        """Массовая обработка всех игр с отображением прогресса"""
        if not all_games_data:
            return 0, 0

        loaded_count = 0
        error_count = 0

        if debug:
            self.stdout.write(f'🔄 Обработка {len(all_games_data)} игр...')

        # Обрабатываем игры пачками по 50 для баланса скорости и памяти
        batch_size = 50

        for i in range(0, len(all_games_data), batch_size):
            batch = all_games_data[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(all_games_data) + batch_size - 1) // batch_size

            if debug:
                self.stdout.write(f'\n📦 Батч {batch_num}/{total_batches}: {len(batch)} игр')

            batch_loaded = 0
            batch_errors = 0

            for game_data in batch:
                game_name = game_data.get('name', 'Unknown')

                if debug:
                    self.stdout.write(f'   🎮 Обработка: {game_name}')

                try:
                    result = self.process_game(game_data, debug)
                    if result == 'created':
                        batch_loaded += 1
                    elif result == 'error':
                        batch_errors += 1
                except Exception as e:
                    batch_errors += 1
                    if debug:
                        self.stderr.write(f'   ❌ Ошибка: {game_name} - {e}')

            loaded_count += batch_loaded
            error_count += batch_errors

            if debug:
                self.stdout.write(f'   📊 Итоги батча: ✅ {batch_loaded} | ❌ {batch_errors}')

        return loaded_count, error_count

    def process_games_universal(self, all_games_data, overwrite=False, skip_existing=False,
                                no_screenshots=False, max_screenshots=0, dry_run=False,
                                debug=False, load_screenshots=False):
        """БЫСТРАЯ загрузка с МАССОВОЙ загрузкой всех данных"""
        if not all_games_data:
            return 0, 0, 0, 0, 0

        # Таймеры для анализа производительности
        timers = {
            'total_start': time.time(),
            'overwrite_time': 0,
            'preparation_time': 0,
            'basic_data_time': 0,
            'cover_time': 0,
            'date_time': 0,
            'screenshots_time': 0,
            'screenshots_saving_time': 0,
            'saving_time': 0,
            'genres_time': 0,
            'platforms_time': 0,
            'keywords_time': 0,
            'batch_cover_time': 0,
            'batch_screenshots_time': 0,
            'batch_genres_time': 0,
            'batch_platforms_time': 0,
            'batch_keywords_time': 0,
            'relations_time': 0
        }

        # Счетчики для прогресса
        counters = {
            'games_processed': 0,
            'games_with_covers': 0,
            'games_with_dates': 0,
            'games_with_screenshots': 0,
            'screenshots_loaded': 0,
            'genres_loaded': 0,
            'platforms_loaded': 0,
            'keywords_loaded': 0,
            # ДОБАВЛЯЕМ НОВЫЕ СЧЕТЧИКИ ДЛЯ СТАТИСТИКИ
            'total_genres_found': 0,
            'total_platforms_found': 0,
            'total_keywords_found': 0,
            'total_covers_found': 0,
            'total_screenshots_found': 0,
            'total_games': len(all_games_data)
        }

        # БЫСТРАЯ ПЕРЕЗАПИСЬ
        deleted_count = 0
        if overwrite and all_games_data:
            overwrite_start = time.time()
            game_ids = [game['id'] for game in all_games_data if game.get('id')]
            if game_ids:
                existing_games = Game.objects.filter(igdb_id__in=game_ids)
                if dry_run:
                    self.stdout.write(f'   🗑️  DRY RUN: Будет удалено {existing_games.count()} игр')
                else:
                    deleted_count, _ = existing_games.delete()
                    if debug:
                        self.stdout.write(f'   🗑️  Удалено: {deleted_count} игр')
            timers['overwrite_time'] = time.time() - overwrite_start

        if dry_run:
            return len(all_games_data), 0, 0, deleted_count, 0

        # МАССОВАЯ ЗАГРУЗКА ВСЕХ ДАННЫХ ДЛЯ ВСЕХ ИГР
        cover_map = {}
        screenshots_map = {}

        # Собираем все ID для массовой загрузки
        all_cover_ids = []
        all_game_ids = []
        all_genre_ids = []
        all_platform_ids = []
        all_keyword_ids = []
        game_relations_map = {}

        for game_data in all_games_data:
            game_id = game_data.get('id')
            if not game_id:
                continue

            all_game_ids.append(game_id)

            if game_data.get('cover'):
                all_cover_ids.append(game_data['cover'])

            # Сохраняем связи для последующей обработки
            game_relations_map[game_id] = {
                'genres': game_data.get('genres', []),
                'platforms': game_data.get('platforms', []),
                'keywords': game_data.get('keywords', [])
            }

            all_genre_ids.extend(game_data.get('genres', []))
            all_platform_ids.extend(game_data.get('platforms', []))
            all_keyword_ids.extend(game_data.get('keywords', []))

        # Убираем дубликаты и сохраняем общее количество
        all_genre_ids = list(set(all_genre_ids))
        all_platform_ids = list(set(all_platform_ids))
        all_keyword_ids = list(set(all_keyword_ids))
        all_cover_ids = list(set(all_cover_ids))

        # ЗАПИСЫВАЕМ ОБЩЕЕ КОЛИЧЕСТВО НАЙДЕННЫХ ДАННЫХ
        counters['total_genres_found'] = len(all_genre_ids)
        counters['total_platforms_found'] = len(all_platform_ids)
        counters['total_keywords_found'] = len(all_keyword_ids)
        counters['total_covers_found'] = len(all_cover_ids)

        if debug:
            self.stdout.write(f'\n📊 ОБЩАЯ СТАТИСТИКА ДАННЫХ:')
            self.stdout.write(f'   🎮 Игр для обработки: {counters["total_games"]}')
            self.stdout.write(f'   🖼️  Обложек найдено: {counters["total_covers_found"]}')
            self.stdout.write(f'   🎭 Жанров найдено: {counters["total_genres_found"]}')
            self.stdout.write(f'   🖥️  Платформ найдено: {counters["total_platforms_found"]}')
            self.stdout.write(f'   🔑 Ключевых слов найдено: {counters["total_keywords_found"]}')

        # МАССОВАЯ ЗАГРУЗКА ОБЛОЖЕК
        if all_cover_ids:
            if debug:
                self.stdout.write(f'\n   🖼️  Массовая загрузка {len(all_cover_ids)} обложек...')

            batch_cover_start = time.time()
            cover_map = self.get_covers_batch(all_cover_ids, debug)
            timers['batch_cover_time'] = time.time() - batch_cover_start

        # МАССОВАЯ ЗАГРУЗКА СКРИНШОТОВ
        if load_screenshots and not no_screenshots and all_game_ids:
            if debug:
                self.stdout.write(f'   📸 Массовая загрузка скриншотов для {len(all_game_ids)} игр...')

            batch_screenshots_start = time.time()
            screenshots_map = self.get_screenshots_batch(all_game_ids, max_screenshots, debug)
            timers['batch_screenshots_time'] = time.time() - batch_screenshots_start

            # СЧИТАЕМ ОБЩЕЕ КОЛИЧЕСТВО СКРИНШОТОВ
            if screenshots_map:
                counters['total_screenshots_found'] = sum(len(screens) for screens in screenshots_map.values())
                if debug:
                    self.stdout.write(f'   📸 Найдено скриншотов: {counters["total_screenshots_found"]}')

        # МАССОВАЯ ЗАГРУЗКА СВЯЗАННЫХ ДАННЫХ
        genre_map = {}
        platform_map = {}
        keyword_map = {}

        if all_genre_ids:
            if debug:
                self.stdout.write(f'   🎭 Массовая загрузка {len(all_genre_ids)} жанров...')

            batch_genres_start = time.time()
            genre_map = self.fetch_and_create_genres_batch(all_genre_ids, debug)
            timers['batch_genres_time'] = time.time() - batch_genres_start

        if all_platform_ids:
            if debug:
                self.stdout.write(f'   🖥️  Массовая загрузка {len(all_platform_ids)} платформ...')

            batch_platforms_start = time.time()
            platform_map = self.fetch_and_create_platforms_batch(all_platform_ids, debug)
            timers['batch_platforms_time'] = time.time() - batch_platforms_start

        if all_keyword_ids:
            if debug:
                self.stdout.write(f'   🔑 Массовая загрузка {len(all_keyword_ids)} ключевых слов...')

            batch_keywords_start = time.time()
            keyword_map = self.fetch_and_create_keywords_batch(all_keyword_ids, debug)
            timers['batch_keywords_time'] = time.time() - batch_keywords_start

        # ПОДГОТОВКА ИГР С ПРОГРЕССОМ В РЕАЛЬНОМ ВРЕМЕНИ
        games_to_create = []
        total_games = len(all_games_data)

        if debug:
            self.stdout.write(f'\n⚡ Подготовка {total_games} игр...')
            preparation_start = time.time()

        for i, game_data in enumerate(all_games_data, 1):
            game_id = game_data.get('id')
            game_name = game_data.get('name', 'Unknown')

            if not game_id:
                continue

            # Быстрая проверка существования
            if skip_existing and not overwrite and Game.objects.filter(igdb_id=game_id).exists():
                continue

            try:
                # ТАЙМЕР ОСНОВНЫХ ДАННЫХ
                basic_start = time.time()
                game = Game(
                    igdb_id=game_id,
                    name=game_data.get('name', ''),
                    summary=game_data.get('summary', ''),
                    storyline=game_data.get('storyline', ''),
                    rating=game_data.get('rating'),
                    rating_count=game_data.get('rating_count', 0)
                )
                basic_time = time.time() - basic_start
                timers['basic_data_time'] += basic_time

                # ТАЙМЕР ДАТЫ РЕЛИЗА
                date_start = time.time()
                if game_data.get('first_release_date'):
                    try:
                        from datetime import datetime
                        naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                        game.first_release_date = timezone.make_aware(naive_datetime)
                        counters['games_with_dates'] += 1
                    except:
                        pass
                date_time = time.time() - date_start
                timers['date_time'] += date_time

                # ТАЙМЕР ОБЛОЖКИ - ТЕПЕРЬ БЕЗ ЗАПРОСОВ К API!
                cover_start = time.time()
                cover_id = game_data.get('cover')
                if cover_id and cover_id in cover_map:
                    game.cover_url = cover_map[cover_id]
                    counters['games_with_covers'] += 1
                cover_time = time.time() - cover_start
                timers['cover_time'] += cover_time

                # ТАЙМЕР СКРИНШОТОВ - ТЕПЕРЬ БЕЗ ЗАПРОСОВ К API!
                screenshots_time = 0
                screenshots_loaded_for_game = 0
                if load_screenshots and not no_screenshots:
                    screenshots_start = time.time()
                    # Просто считаем количество доступных скриншотов
                    if game_id in screenshots_map:
                        screenshots_loaded_for_game = len(screenshots_map[game_id])
                        if max_screenshots > 0:
                            screenshots_loaded_for_game = min(screenshots_loaded_for_game, max_screenshots)
                    screenshots_time = time.time() - screenshots_start
                    timers['screenshots_time'] += screenshots_time

                    if screenshots_loaded_for_game > 0:
                        counters['screenshots_loaded'] += screenshots_loaded_for_game
                        counters['games_with_screenshots'] += 1

                games_to_create.append(game)
                counters['games_processed'] += 1

            except Exception as e:
                if debug:
                    self.stderr.write(f'   ❌ Ошибка подготовки {game_name}: {e}')

        timers['preparation_time'] = time.time() - preparation_start

        # СОХРАНЕНИЕ ИГР
        loaded_count = 0
        error_count = 0

        if games_to_create:
            if debug:
                self.stdout.write(f'\n   💾 Сохранение {len(games_to_create)} игр в БД...')

            saving_start = time.time()
            try:
                Game.objects.bulk_create(games_to_create, batch_size=200)
                loaded_count = len(games_to_create)
                timers['saving_time'] = time.time() - saving_start

                if debug:
                    self.stdout.write(f'      ✅ Успешно сохранено: {loaded_count} игр за {timers["saving_time"]:.2f}с')

            except Exception as e:
                error_count = len(games_to_create)
                if debug:
                    self.stderr.write(f'   ❌ Ошибка сохранения: {e}')

        # МАССОВОЕ СОХРАНЕНИЕ СКРИНШОТОВ ПОСЛЕ СОЗДАНИЯ ИГР
        screenshots_saved_count = 0
        if load_screenshots and not no_screenshots and screenshots_map and not dry_run:
            if debug:
                self.stdout.write(f'\n   💾 Массовое сохранение скриншотов...')

            screenshots_to_create = []
            saved_games = Game.objects.filter(igdb_id__in=[g.igdb_id for g in games_to_create])
            saved_games_map = {game.igdb_id: game for game in saved_games}

            screenshots_saving_start = time.time()

            for game_id, screenshots_data in screenshots_map.items():
                if game_id not in saved_games_map:
                    continue

                game = saved_games_map[game_id]
                screenshots_count = 0

                for screenshot_data in screenshots_data:
                    if max_screenshots > 0 and screenshots_count >= max_screenshots:
                        break

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
                        caption=''
                    )

                    screenshots_to_create.append(screenshot)
                    screenshots_count += 1

            # Массовое сохранение всех скриншотов
            if screenshots_to_create:
                Screenshot.objects.bulk_create(screenshots_to_create, batch_size=100)
                screenshots_saved_count = len(screenshots_to_create)
                timers['screenshots_saving_time'] = time.time() - screenshots_saving_start
                if debug:
                    self.stdout.write(
                        f'      ✅ Сохранено скриншотов: {screenshots_saved_count} за {timers["screenshots_saving_time"]:.2f}с')

        # МАССОВОЕ СОЗДАНИЕ СВЯЗЕЙ ДЛЯ ВСЕХ ИГР
        if loaded_count > 0 and game_relations_map:
            if debug:
                self.stdout.write(f'\n   🔗 МАССОВОЕ создание связей для {loaded_count} игр...')

            saved_games = Game.objects.filter(igdb_id__in=[g.igdb_id for g in games_to_create])
            saved_games_map = {game.igdb_id: game for game in saved_games}

            relations_start = time.time()

            # Подготавливаем данные для массового создания связей
            game_genre_relations = []
            game_platform_relations = []
            game_keyword_relations = []

            for game_id, relations in game_relations_map.items():
                if game_id not in saved_games_map:
                    continue

                game = saved_games_map[game_id]

                # Жанры
                for genre_id in relations['genres']:
                    if genre_id in genre_map:
                        game_genre_relations.append(Game.genres.through(
                            game_id=game.id,
                            genre_id=genre_map[genre_id].id
                        ))

                # Платформы
                for platform_id in relations['platforms']:
                    if platform_id in platform_map:
                        game_platform_relations.append(Game.platforms.through(
                            game_id=game.id,
                            platform_id=platform_map[platform_id].id
                        ))

                # Ключевые слова
                for keyword_id in relations['keywords']:
                    if keyword_id in keyword_map:
                        game_keyword_relations.append(Game.keywords.through(
                            game_id=game.id,
                            keyword_id=keyword_map[keyword_id].id
                        ))

            # МАССОВОЕ СОХРАНЕНИЕ СВЯЗЕЙ
            if game_genre_relations:
                Game.genres.through.objects.bulk_create(game_genre_relations, batch_size=500, ignore_conflicts=True)
                counters['genres_loaded'] = len(game_genre_relations)
                if debug:
                    self.stdout.write(f'      ✅ Жанры: {len(game_genre_relations)} связей')

            if game_platform_relations:
                Game.platforms.through.objects.bulk_create(game_platform_relations, batch_size=500,
                                                           ignore_conflicts=True)
                counters['platforms_loaded'] = len(game_platform_relations)
                if debug:
                    self.stdout.write(f'      ✅ Платформы: {len(game_platform_relations)} связей')

            if game_keyword_relations:
                Game.keywords.through.objects.bulk_create(game_keyword_relations, batch_size=500, ignore_conflicts=True)
                counters['keywords_loaded'] = len(game_keyword_relations)
                if debug:
                    self.stdout.write(f'      ✅ Ключ. слова: {len(game_keyword_relations)} связей')

            timers['relations_time'] = time.time() - relations_start

            if debug:
                self.stdout.write(f'      🎯 Все связи созданы за {timers["relations_time"]:.2f}с')

        # ВЫВОД ФИНАЛЬНОЙ СТАТИСТИКИ
        if debug and loaded_count > 0:
            total_time = time.time() - timers['total_start']

            self.stdout.write(f'\n🎯 ФИНАЛЬНАЯ СТАТИСТИКА:')
            self.stdout.write(f'   ⏱️  ОБЩЕЕ ВРЕМЯ: {total_time:.2f}с')
            self.stdout.write(f'   🚀 СКОРОСТЬ: {loaded_count / total_time:.1f} игр/сек')

            self.stdout.write(f'\n   📊 ВЫПОЛНЕННЫЕ ОПЕРАЦИИ:')
            self.stdout.write(f'      🎮 Игр обработано: {counters["games_processed"]}/{counters["total_games"]} '
                              f'({(counters["games_processed"] / counters["total_games"] * 100):.1f}%)')
            self.stdout.write(f'      📅 Игр с датами: {counters["games_with_dates"]}')

            # ОБЛОЖКИ - СРАВНЕНИЕ НАЙДЕНО/ЗАГРУЖЕНО
            if counters['total_covers_found'] > 0:
                self.stdout.write(
                    f'      🖼️  Обложки: {counters["games_with_covers"]}/{counters["total_covers_found"]} '
                    f'({(counters["games_with_covers"] / counters["total_covers_found"] * 100):.1f}%)')
            else:
                self.stdout.write(f'      🖼️  Обложки: {counters["games_with_covers"]}/0 (0%)')

            if load_screenshots and not no_screenshots:
                # СКРИНШОТЫ - СРАВНЕНИЕ НАЙДЕНО/ЗАГРУЖЕНО
                if counters['total_screenshots_found'] > 0:
                    self.stdout.write(
                        f'      📸 Скриншоты: {screenshots_saved_count}/{counters["total_screenshots_found"]} '
                        f'({(screenshots_saved_count / counters["total_screenshots_found"] * 100):.1f}%)')
                else:
                    self.stdout.write(f'      📸 Скриншоты: {screenshots_saved_count}/0 (0%)')
                self.stdout.write(f'      📸 Игр со скриншотами: {counters["games_with_screenshots"]}')

            # ИСПРАВЛЕННАЯ СТАТИСТИКА ДЛЯ СВЯЗЕЙ
            # Для жанров, платформ и ключевых слов показываем количество связей, а не процент
            self.stdout.write(
                f'      🎭 Связи жанров: {counters["genres_loaded"]} (уникальных жанров: {counters["total_genres_found"]})')
            self.stdout.write(
                f'      🖥️  Связи платформ: {counters["platforms_loaded"]} (уникальных платформ: {counters["total_platforms_found"]})')
            self.stdout.write(
                f'      🔑 Связи ключ. слов: {counters["keywords_loaded"]} (уникальных слов: {counters["total_keywords_found"]})')

            self.stdout.write(f'\n   ⏰ РАСПРЕДЕЛЕНИЕ ВРЕМЕНИ:')
            time_operations = [
                ('Массовая загрузка ключ. слов', timers['batch_keywords_time']),
                ('Массовая загрузка скриншотов', timers['batch_screenshots_time']),
                ('Массовая загрузка обложек', timers['batch_cover_time']),
                ('Сохранение скриншотов', timers.get('screenshots_saving_time', 0)),
                ('Массовая загрузка платформ', timers['batch_platforms_time']),
                ('Массовая загрузка жанров', timers['batch_genres_time']),
                ('Создание связей', timers['relations_time']),
                ('Удаление игр', timers['overwrite_time']),
                ('Сохранение в БД', timers['saving_time']),
                ('Подготовка данных', timers['preparation_time']),
                ('Обработка дат', timers['date_time']),
                ('Обработка обложек', timers['cover_time'])
            ]

            # Сортируем по времени (самые медленные сначала)
            time_operations.sort(key=lambda x: x[1], reverse=True)

            for op_name, op_time in time_operations:
                if op_time > 0:
                    percent = (op_time / total_time) * 100
                    self.stdout.write(f'      {op_name}: {op_time:.2f}с ({percent:.1f}%)')

        screenshots_loaded = screenshots_saved_count
        return loaded_count, 0, error_count, deleted_count, screenshots_loaded

    def load_game_screenshots_immediately(self, game_id, max_screenshots=0, debug=False):
        """ЗАМЕНА: Теперь используем массовую загрузку (исправленная версия)"""
        # Этот метод теперь будет использоваться только для одиночных случаев
        try:
            # ИСПРАВЛЕНИЕ: Ограничиваем лимит до 500
            limit = 500 if max_screenshots == 0 else min(max_screenshots, 500)

            # ИСПРАВЛЕННЫЙ ЗАПРОС: убираем поле caption
            query = f'''
                fields game,id,url,width,height;
                where game = {game_id};
                limit {limit};
            '''

            screenshots_data = make_igdb_request('screenshots', query, debug=False)

            if not screenshots_data:
                return 0

            loaded_screenshots = 0
            for screenshot_data in screenshots_data:
                screenshot_id = screenshot_data.get('id')
                image_url = screenshot_data.get('url')

                if not screenshot_id or not image_url:
                    continue

                # Пропускаем если уже существует
                if Screenshot.objects.filter(igdb_id=screenshot_id).exists():
                    continue

                loaded_screenshots += 1

                # Если достигли максимального количества, прерываем
                if max_screenshots > 0 and loaded_screenshots >= max_screenshots:
                    break

            return loaded_screenshots

        except Exception as e:
            if debug:
                self.stderr.write(f'      ❌ Ошибка загрузки скриншотов для игры {game_id}: {e}')
            return 0

    def load_screenshots_for_all_games(self, max_screenshots=0, debug=False):
        """Загружает скриншоты для всех игр в базе"""
        games = Game.objects.all()
        total_games = games.count()

        self.stdout.write(f'📸 Загрузка скриншотов для {total_games} игр...')

        total_screenshots = 0
        for i, game in enumerate(games, 1):
            if debug:
                self.stdout.write(f'   [{i}/{total_games}] {game.name}')

            loaded = self.load_game_screenshots(game.igdb_id, max_screenshots, debug)
            total_screenshots += loaded

            if debug and loaded > 0:
                self.stdout.write(f'      📸 Загружено: {loaded} скриншотов')

        self.stdout.write(f'✅ Всего загружено скриншотов: {total_screenshots}')

    def load_screenshots_for_new_games(self, game_ids, max_screenshots=0, debug=False):
        """ЗАМЕНА: Массовая загрузка скриншотов для конкретных игр (исправленная версия)"""
        if not game_ids:
            return 0

        if debug:
            self.stdout.write(f'   📸 Массовая загрузка скриншотов для {len(game_ids)} игр...')

        # Используем новый массовый метод
        screenshots_map = self.get_screenshots_batch(game_ids, max_screenshots, debug)

        total_screenshots = 0
        screenshots_to_create = []

        # Получаем игры из базы
        games = Game.objects.filter(igdb_id__in=game_ids)
        games_map = {game.igdb_id: game for game in games}

        for game_id, screenshots_data in screenshots_map.items():
            if game_id not in games_map:
                continue

            game = games_map[game_id]
            screenshots_count = 0

            for screenshot_data in screenshots_data:
                if max_screenshots > 0 and screenshots_count >= max_screenshots:
                    break

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
                    caption=''  # ИСПРАВЛЕНО: caption больше не получаем из API
                )

                screenshots_to_create.append(screenshot)
                screenshots_count += 1
                total_screenshots += 1

        # Массовое сохранение
        if screenshots_to_create:
            Screenshot.objects.bulk_create(screenshots_to_create, batch_size=100)
            if debug:
                self.stdout.write(f'      ✅ Сохранено скриншотов: {len(screenshots_to_create)}')

        return total_screenshots

    def get_screenshots_batch(self, game_ids, max_screenshots=0, debug=False):
        """Массовая загрузка скриншотов - ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ПАЧКАМИ С ПРОГРЕССОМ"""
        if not game_ids:
            return {}

        try:
            # РАЗБИВАЕМ НА ПАЧКИ по 10 игр
            batch_size = 10
            game_batches = [game_ids[i:i + batch_size] for i in range(0, len(game_ids), batch_size)]

            screenshots_map = defaultdict(list)
            lock = Lock()
            processed_batches = 0

            def process_batch(batch_data):
                batch_num, batch_game_ids = batch_data
                try:
                    if debug:
                        self.stdout.write(
                            f'      📸 Пачка {batch_num}/{len(game_batches)}: загрузка скриншотов для {len(batch_game_ids)} игр...')

                    id_list = ','.join(map(str, batch_game_ids))

                    query = f'''
                        fields game,id,url,width,height;
                        where game = ({id_list});
                        limit 500;
                    '''.strip()

                    batch_screenshots = make_igdb_request('screenshots', query, debug=False)

                    # Блокируем для потокобезопасности
                    with lock:
                        for screenshot in batch_screenshots:
                            game_id = screenshot.get('game')
                            if game_id and screenshot.get('url'):
                                screenshots_map[game_id].append(screenshot)

                        nonlocal processed_batches
                        processed_batches += 1
                        if debug:
                            self.stdout.write(
                                f'      ✅ Пачка {batch_num} завершена: {len(batch_screenshots)} скриншотов')

                except Exception as e:
                    if debug:
                        self.stderr.write(f'      ❌ Ошибка пачки скриншотов {batch_num}: {e}')

            if debug:
                self.stdout.write(f'   📸 Начало загрузки скриншотов: {len(game_batches)} пачек по {batch_size} игр')

            # ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ПАЧЕК с номерами
            batch_data = [(i + 1, batch) for i, batch in enumerate(game_batches)]
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                executor.map(process_batch, batch_data)

            if debug:
                total_screenshots = sum(len(screens) for screens in screenshots_map.values())
                self.stdout.write(
                    f'   ✅ Загрузка скриншотов завершена: {total_screenshots} скриншотов для {len(screenshots_map)} игр')

            return screenshots_map

        except Exception as e:
            if debug:
                self.stdout.write(f'   ⚠️ Ошибка массовой загрузки скриншотов: {e}')
            return {}

    def fetch_and_create_genres_batch(self, genre_ids, debug=False):
        """Массовое создание/получение жанров с параллельной загрузкой"""
        if not genre_ids:
            return {}

        try:
            # Параллельная загрузка данных
            all_genre_data = self.fetch_igdb_data_parallel('genres', genre_ids, debug)

            genre_map = {}
            genres_to_create = []

            # Создаем объекты Genre
            for genre_info in all_genre_data:
                genre_id = genre_info['id']
                genre_name = genre_info.get('name', f'Genre {genre_id}')

                try:
                    genre = Genre.objects.get(igdb_id=genre_id)
                    if genre.name.startswith('Genre '):
                        genre.name = genre_name
                        genre.save()
                except Genre.DoesNotExist:
                    genre = Genre(igdb_id=genre_id, name=genre_name)
                    genres_to_create.append(genre)

                genre_map[genre_id] = genre

            # Массовое создание
            if genres_to_create:
                Genre.objects.bulk_create(genres_to_create)
                for genre in genres_to_create:
                    genre_map[genre.igdb_id] = genre

            if debug:
                self.stdout.write(f'         🎭 Жанры: загружено {len(genre_map)}/{len(genre_ids)}')

            return genre_map

        except Exception as e:
            if debug:
                self.stderr.write(f'         ❌ Ошибка загрузки жанров: {e}')
            return {}

    def fetch_and_create_platforms_batch(self, platform_ids, debug=False):
        """Массовое создание/получение платформ с параллельной загрузкой"""
        if not platform_ids:
            return {}

        try:
            # Параллельная загрузка данных
            all_platform_data = self.fetch_igdb_data_parallel('platforms', platform_ids, debug)

            platform_map = {}
            platforms_to_create = []

            for platform_info in all_platform_data:
                platform_id = platform_info['id']
                platform_name = platform_info.get('name', f'Platform {platform_id}')

                try:
                    platform = Platform.objects.get(igdb_id=platform_id)
                    if platform.name.startswith('Platform '):
                        platform.name = platform_name
                        platform.save()
                except Platform.DoesNotExist:
                    platform = Platform(igdb_id=platform_id, name=platform_name)
                    platforms_to_create.append(platform)

                platform_map[platform_id] = platform

            if platforms_to_create:
                Platform.objects.bulk_create(platforms_to_create)
                for platform in platforms_to_create:
                    platform_map[platform.igdb_id] = platform

            if debug:
                self.stdout.write(f'         🖥️  Платформы: загружено {len(platform_map)}/{len(platform_ids)}')

            return platform_map

        except Exception as e:
            if debug:
                self.stderr.write(f'         ❌ Ошибка загрузки платформ: {e}')
            return {}

    def fetch_and_create_keywords_batch(self, keyword_ids, debug=False):
        """Массовое создание/получение ключевых слов - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
        if not keyword_ids:
            return {}

        try:
            # Параллельная загрузка данных (теперь с оптимизированными настройками)
            all_keyword_data = self.fetch_igdb_data_parallel('keywords', keyword_ids, debug)

            keyword_map = {}
            keywords_to_create = []

            # ОДИН ЗАПРОС для получения существующих ключевых слов
            existing_keywords = Keyword.objects.filter(igdb_id__in=keyword_ids)
            existing_map = {kw.igdb_id: kw for kw in existing_keywords}

            for keyword_info in all_keyword_data:
                keyword_id = keyword_info['id']
                keyword_name = keyword_info.get('name', f'Keyword {keyword_id}')

                if keyword_id in existing_map:
                    keyword = existing_map[keyword_id]
                    # Обновляем имя если оно было заполнено заглушкой
                    if keyword.name.startswith('Keyword '):
                        keyword.name = keyword_name
                        keyword.save()
                else:
                    keyword = Keyword(igdb_id=keyword_id, name=keyword_name)
                    keywords_to_create.append(keyword)

                keyword_map[keyword_id] = keyword

            # Массовое создание
            if keywords_to_create:
                Keyword.objects.bulk_create(keywords_to_create, batch_size=500)
                # Обновляем мапу созданными объектами
                for keyword in keywords_to_create:
                    keyword_map[keyword.igdb_id] = keyword

            if debug:
                self.stdout.write(f'         🔑 Ключ. слова: загружено {len(keyword_map)}/{len(keyword_ids)}')

            return keyword_map

        except Exception as e:
            if debug:
                self.stderr.write(f'         ❌ Ошибка загрузки ключевых слов: {e}')
            return {}

    def process_genres(self, game, genre_ids, debug=False):
        """Простая обертка для обратной совместимости"""
        return self.process_genres_batch(game, genre_ids, debug)

    def process_keywords(self, game, keyword_ids, debug=False):
        """Простая обертка для обратной совместимости"""
        return self.process_keywords_batch(game, keyword_ids, debug)

    def process_platforms(self, game, platform_ids, debug=False):
        """Простая обертка для обратной совместимости"""
        return self.process_platforms_batch(game, platform_ids, debug)

    def update_games_with_additional_data_fast(self, games, delay=0.2, dry_run=False, debug=False, skip_no_data=False,
                                               batch_size=50):
        """Быстрая версия обновления существующих игр"""
        # Используем обычный метод как временное решение
        return self.update_games_with_additional_data(games, delay, dry_run, debug, skip_no_data)

    def fetch_igdb_data_parallel(self, endpoint, all_ids, debug=False):
        """Параллельная загрузка данных из IGDB - ВСЕ ПАЧКАМИ ПО 10"""
        if not all_ids:
            return []

        # ВСЕ ЭНДПОИНТЫ ТЕПЕРЬ ПАЧКАМИ ПО 10
        batch_size = 10
        max_workers = 5

        all_data = []
        lock = Lock()
        processed_batches = 0

        def process_batch(batch_data):
            batch_num, batch_ids = batch_data
            try:
                if debug:
                    self.stdout.write(f'         🔄 {endpoint}: Пачка {batch_num} - {len(batch_ids)} ID')

                id_list = ','.join(map(str, batch_ids))
                query = f'fields id,name; where id = ({id_list});'
                batch_data = make_igdb_request(endpoint, query, debug=False)

                with lock:
                    all_data.extend(batch_data)
                    nonlocal processed_batches
                    processed_batches += 1

                    if debug:
                        self.stdout.write(
                            f'         ✅ {endpoint}: Пачка {batch_num} завершена - {len(batch_data)} объектов')

            except Exception as e:
                if debug:
                    self.stderr.write(f'         ❌ Ошибка пачки {endpoint} {batch_num}: {e}')

        # Создаем пачки
        batches = [all_ids[i:i + batch_size] for i in range(0, len(all_ids), batch_size)]

        if debug:
            self.stdout.write(f'         📦 {endpoint}: {len(batches)} пачек по {batch_size} ID')

        # Параллельная обработка с номерами пачек
        batch_data = [(i + 1, batch) for i, batch in enumerate(batches)]
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            executor.map(process_batch, batch_data)

        if debug:
            self.stdout.write(f'         ✅ {endpoint}: Загружено {len(all_data)}/{len(all_ids)} объектов')

        return all_data

    def handle(self, *args, **options):
        # Основные опции
        debug = options['debug']
        batch_size = min(options['batch_size'], 500)
        delay = options['delay']
        dry_run = options['dry_run']
        skip_existing = options['skip_existing']
        missing_only = options['missing_only']
        overwrite = options['overwrite']
        no_screenshots = options['no_screenshots']
        max_screenshots = options['max_screenshots']
        skip_no_data = options['skip_no_data']
        load_screenshots = options.get('load_screenshots', False)
        screenshots_only = options.get('screenshots_only', False)

        # УБИРАЕМ вызов set_debug_mode для IGDB API
        # set_debug_mode(debug)

        self.stdout.write('🎮 УНИВЕРСАЛЬНАЯ ЗАГРУЗКА ИГР ИЗ IGDB')
        self.stdout.write('=' * 60)

        if dry_run:
            self.stdout.write('🚧 РЕЖИМ DRY RUN - данные не будут сохранены в базу!')

        if overwrite:
            self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ - существующие игры будут удалены и загружены заново!')

        # РЕЖИМ ТОЛЬКО СКРИНШОТЫ
        if screenshots_only:
            self.stdout.write('📸 РЕЖИМ ЗАГРУЗКИ СКРИНШОТОВ')
            self.load_screenshots_for_all_games(
                max_screenshots=max_screenshots,
                debug=debug
            )
            return

        # Определяем режим работы
        if options['update_storylines']:
            self.update_storylines(batch_size, delay, missing_only, debug)
            return

        elif options['update_existing']:
            # ОБНОВЛЕНИЕ СУЩЕСТВУЮЩИХ ИГР
            if options['game_ids']:
                game_ids_list = [int(id.strip()) for id in options['game_ids'].split(',')]
                games = Game.objects.filter(igdb_id__in=game_ids_list)
                self.stdout.write(f"🔄 Обновление {len(games)} конкретных игр...")
            else:
                games = Game.objects.all().order_by('id')
                if skip_existing:
                    games = games.filter(
                        series__isnull=True,
                        developers__isnull=True,
                        publishers__isnull=True
                    )
                if options['limit']:
                    games = games[options['start_from']:options['start_from'] + options['limit']]
                else:
                    games = games[options['start_from']:]

            if overwrite:
                # БЫСТРАЯ ПЕРЕЗАПИСЬ
                self.stdout.write("🔥 ЗАПУСК БЫСТРОЙ ПЕРЕЗАПИСИ...")
                self.mass_overwrite_games(
                    batch_size=min(batch_size, 100),
                    delay=max(delay, 0.05),  # Минимальная задержка 0.05
                    dry_run=dry_run,
                    debug=debug
                )
            else:
                # ОБЫЧНОЕ ОБНОВЛЕНИЕ ДАННЫХ
                self.stdout.write(f'🔄 Обновление {games.count()} существующих игр...')
                self.update_games_with_additional_data_fast(
                    games,
                    delay,
                    dry_run,
                    debug,
                    skip_no_data,
                    batch_size=min(batch_size, 50)
                )
            return

        # РЕЖИМЫ ЗАГРУЗКИ НОВЫХ ИГР
        all_games = []
        not_found_games = []

        if options['single_game']:
            # ЗАГРУЗКА ОДНОЙ ИГРЫ ПО НАЗВАНИЮ
            single_game = options['single_game']
            self.stdout.write(f'🎯 Загрузка одной игры: {single_game}')
            game_data = self.search_single_game_by_name(single_game, debug)
            if game_data:
                all_games = [game_data]
            else:
                self.stdout.write(f'❌ Игра не найдена: {single_game}')
                return

        elif options['input_file']:
            # ЗАГРУЗКА ИЗ ФАЙЛА
            input_file = options['input_file']
            self.stdout.write(f'📁 Загрузка из файла: {input_file}')
            with open(input_file, 'r', encoding='utf-8') as f:
                game_names = [line.strip() for line in f if line.strip()]

            self.stdout.write(f'🔍 Массовый поиск {len(game_names)} игр...')
            all_games, not_found_games = self.search_games_by_exact_name_batch(game_names, debug)

        elif options['tactical_rpg']:
            # ЗАГРУЗКА ТАКТИЧЕСКИХ RPG
            all_games = self.load_tactical_rpg_games(500, debug)

        elif options['genre_id']:
            # ЗАГРУЗКА ПО ЖАНРУ
            all_games = self.load_games_by_genre(options['genre_id'], 500, debug)

        elif options['keyword_id']:
            # ЗАГРУЗКА ПО КЛЮЧЕВОМУ СЛОВУ
            all_games = self.load_games_by_keyword(options['keyword_id'], 500, debug)

        else:
            self.stderr.write('❌ Не указан режим работы. Используйте один из:')
            self.stderr.write('   --single-game NAME')
            self.stderr.write('   --input-file FILE')
            self.stderr.write('   --tactical-rpg')
            self.stderr.write('   --genre-id ID')
            self.stderr.write('   --keyword-id ID')
            self.stderr.write('   --update-existing')
            self.stderr.write('   --update-storylines')
            self.stderr.write('   --screenshots-only')
            return

        if not all_games:
            self.stdout.write('❌ Не найдено игр для загрузки')
            return

        self.stdout.write(f'📥 Найдено игр для обработки: {len(all_games)}')

        # УНИВЕРСАЛЬНАЯ ОБРАБОТКА ВСЕХ ИГР
        loaded_count, skipped_count, error_count, deleted_count, screenshots_loaded = self.process_games_universal(
            all_games_data=all_games,
            overwrite=overwrite,
            skip_existing=skip_existing,
            no_screenshots=no_screenshots,
            max_screenshots=max_screenshots,
            dry_run=dry_run,
            debug=debug,
            load_screenshots=load_screenshots  # ← передаем опцию загрузки скриншотов
        )

        # ОТДЕЛЬНАЯ ЗАГРУЗКА СКРИНШОТОВ если запрошено
        # if load_screenshots and loaded_count > 0 and not dry_run:
        #     self.stdout.write(f'\n📸 ОТДЕЛЬНАЯ ЗАГРУЗКА СКРИНШОТОВ...')
        #     screenshots_loaded = self.load_screenshots_for_new_games(
        #         game_ids=[game['id'] for game in all_games if game.get('id')],
        #         max_screenshots=max_screenshots,
        #         debug=debug
        #     )

        # СОХРАНЕНИЕ НЕ НАЙДЕННЫХ ИГР (только для загрузки из файла)
        if options['input_file'] and not_found_games:
            import os
            not_found_file = f"not_found_{os.path.basename(options['input_file'])}"
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

        if options['input_file']:
            self.stdout.write(f'• Не найдено в IGDB: {len(not_found_games)}')

        if not no_screenshots and not dry_run:
            self.stdout.write(f'• Скриншотов загружено: {screenshots_loaded}')
            total_screenshots = Screenshot.objects.count()
            self.stdout.write(f'• Всего скриншотов в базе: {total_screenshots}')

        # ПОДСКАЗКА ДЛЯ СКРИНШОТОВ
        if loaded_count > 0 and not load_screenshots and not dry_run:
            self.stdout.write(f'\n💡 Для загрузки скриншотов выполните:')
            self.stdout.write(f'   python manage.py load_games --screenshots-only --max-screenshots {max_screenshots}')
