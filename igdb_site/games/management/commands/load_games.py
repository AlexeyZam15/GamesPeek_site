# management/commands/load_games.py
from django.core.management.base import BaseCommand
from django.utils import timezone
from games.igdb_api import make_igdb_request
from games.models import (
    Game, Genre, Keyword, Platform, Series,
    Company, Theme, PlayerPerspective, GameMode, Screenshot
)
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading


class Command(BaseCommand):
    help = 'Загрузка тактических RPG с перезаписью всех данных'

    def add_arguments(self, parser):
        parser.add_argument('--overwrite', action='store_true', help='Удалить существующие игры и загрузить заново')
        parser.add_argument('--debug', action='store_true', help='Включить режим отладки')
        parser.add_argument('--limit', type=int, default=0,
                            help='Ограничить количество загружаемых игр (0 - без ограничения)')

    # ==================== ОСНОВНЫЕ МЕТОДЫ ====================

    def collect_all_data_ids(self, all_games_data, debug=False):
        """Собирает все ID для последующей загрузки"""
        all_game_ids = []
        all_cover_ids = []
        all_genre_ids = set()
        all_platform_ids = set()
        all_keyword_ids = set()
        game_data_map = {}

        if debug:
            self.stdout.write('   📊 Сбор всех ID данных...')

        for game_data in all_games_data:
            game_id = game_data.get('id')
            if not game_id:
                continue

            all_game_ids.append(game_id)
            game_data_map[game_id] = game_data

            if game_data.get('cover'):
                all_cover_ids.append(game_data['cover'])

            if game_data.get('genres'):
                all_genre_ids.update(game_data['genres'])

            if game_data.get('platforms'):
                all_platform_ids.update(game_data['platforms'])

            if game_data.get('keywords'):
                all_keyword_ids.update(game_data['keywords'])

        if debug:
            self.stdout.write(f'   ✅ Собрано ID:')
            self.stdout.write(f'      • Игр: {len(all_game_ids)}')
            self.stdout.write(f'      • Обложек: {len(set(all_cover_ids))}')
            self.stdout.write(f'      • Жанров: {len(all_genre_ids)}')
            self.stdout.write(f'      • Платформ: {len(all_platform_ids)}')
            self.stdout.write(f'      • Ключевых слов: {len(all_keyword_ids)}')

        return {
            'game_data_map': game_data_map,
            'all_game_ids': all_game_ids,
            'all_cover_ids': list(set(all_cover_ids)),  # Удаляем дубликаты
            'all_genre_ids': list(all_genre_ids),
            'all_platform_ids': list(all_platform_ids),
            'all_keyword_ids': list(all_keyword_ids),
            'all_screenshot_games': all_game_ids,  # Все игры могут иметь скриншоты
        }

    def _batch_processor(self, ids_list, process_batch_func, emoji, name, debug=False):
        """Универсальный метод для обработки данных пачками"""
        if not ids_list:
            return {}

        result_map = {}
        lock = threading.Lock()

        # Разбиваем на пачки по 10
        batches = [ids_list[i:i + 10] for i in range(0, len(ids_list), 10)]
        total_batches = len(batches)

        if debug:
            self.stdout.write(f'      {emoji} Загрузка {name}: {len(ids_list)} объектов, {total_batches} пачек')

        # Запускаем параллельную обработку
        max_workers = min(total_batches, 5)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for batch_num, batch_ids in enumerate(batches, 1):
                future = executor.submit(
                    process_batch_func, batch_num, batch_ids, result_map, lock, total_batches, name, debug
                )
                futures.append(future)

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    if debug:
                        with lock:
                            self.stderr.write(f'      ❌ Ошибка в потоке: {e}')

        if debug:
            loaded = len(result_map)
            total = len(ids_list)
            self.stdout.write(f'      {emoji} Всего загружено {name}: {loaded}/{total} (из {total_batches} пачек)')

        return result_map

    def _process_generic_batch(self, batch_num, batch_ids, result_map, lock, total_batches, name, debug,
                               endpoint, create_func):
        """Обрабатывает пачку данных для универсальной загрузки"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} объектов')

            id_list = ','.join(map(str, batch_ids))
            query = f'fields id,name; where id = ({id_list});'

            batch_data = make_igdb_request(endpoint, query, debug=False)

            batch_map = {}
            for item_data in batch_data:
                item_id = item_data.get('id')
                if not item_id:
                    continue

                item_name = item_data.get('name', f'{name} {item_id}')
                item = create_func(item_id, item_name)
                batch_map[item_id] = item

            with lock:
                result_map.update(batch_map)

            if debug:
                with lock:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} объектов')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_data_parallel_generic(self, ids_list, endpoint, model_class, create_func, emoji, name, debug=False):
        """Универсальный метод для параллельной загрузки данных"""

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_generic_batch(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug, endpoint, create_func
            )

        return self._batch_processor(ids_list, process_batch, emoji, name, debug)

    def _process_covers_batch(self, batch_num, batch_ids, cover_map, lock, total_batches, name, debug):
        """Обрабатывает пачку обложек"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_ids)} объектов')

            id_list = ','.join(map(str, batch_ids))
            query = f'fields id,url,image_id; where id = ({id_list});'

            batch_data = make_igdb_request('covers', query, debug=False)

            batch_map = {}
            for cover_data in batch_data:
                cover_id = cover_data.get('id')
                if not cover_id:
                    continue

                if cover_data.get('image_id'):
                    high_res_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{cover_data['image_id']}.jpg"
                    batch_map[cover_id] = high_res_url
                elif cover_data.get('url'):
                    url = cover_data['url']
                    high_res_url = f"https:{url.replace('thumb', 'cover_big')}"
                    batch_map[cover_id] = high_res_url

            with lock:
                cover_map.update(batch_map)

            if debug:
                with lock:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} объектов')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_covers_parallel(self, cover_ids, debug=False):
        """Параллельная загрузка обложек"""
        return self._batch_processor(cover_ids, self._process_covers_batch, '🖼️', 'обложек', debug)

    def _load_game_screenshots(self, game_id, screenshots_count, debug=False):
        """Загружает ВСЕ скриншоты для одной игры, но пачками"""
        try:
            # Если скриншотов нет, пропускаем
            if not screenshots_count or screenshots_count == 0:
                return 0

            # Загружаем ВСЕ скриншоты для игры
            query = f'fields id,url,image_id,width,height; where game = {game_id}; limit {screenshots_count};'
            screenshots_data = make_igdb_request('screenshots', query, debug=False)

            if not screenshots_data:
                return 0

            screenshots_to_create = []
            game_obj = Game.objects.filter(igdb_id=game_id).first()
            if not game_obj:
                return 0

            for screenshot_data in screenshots_data:
                image_id = screenshot_data.get('image_id')
                if image_id:
                    width = screenshot_data.get('width') or 0
                    height = screenshot_data.get('height') or 0

                    screenshot_obj = Screenshot(
                        game=game_obj,
                        igdb_id=screenshot_data.get('id'),
                        image_url=f"https://images.igdb.com/igdb/image/upload/t_original/{image_id}.jpg",
                        width=width,
                        height=height
                    )
                    screenshots_to_create.append(screenshot_obj)

            if screenshots_to_create:
                # Сохраняем пачками по 10 (как вам нужно)
                for i in range(0, len(screenshots_to_create), 10):
                    batch = screenshots_to_create[i:i + 10]
                    Screenshot.objects.bulk_create(batch, ignore_conflicts=True)

            return len(screenshots_to_create)

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка загрузки скриншотов для игры {game_id}: {e}')
            return 0

    def _process_screenshots_batch(self, batch_num, batch_game_ids, result_map, lock,
                                   total_batches, name, debug, screenshots_info):
        """Обрабатывает пачку скриншотов, зная сколько их у каждой игры"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_game_ids)} игр')

            batch_screenshots = 0

            for game_id in batch_game_ids:
                try:
                    # Получаем количество скриншотов для этой игры
                    game_screenshots_count = screenshots_info.get(game_id, 0)

                    if game_screenshots_count > 0:
                        screenshots = self._load_game_screenshots(
                            game_id, game_screenshots_count, debug=debug
                        )
                        with lock:
                            result_map[game_id] = screenshots
                            batch_screenshots += screenshots

                        if debug and screenshots != game_screenshots_count:
                            with lock:
                                self.stdout.write(
                                    f'         ⚠️  Игра {game_id}: загружено {screenshots}/{game_screenshots_count} скриншотов'
                                )
                    else:
                        if debug:
                            with lock:
                                self.stdout.write(f'         ℹ️  Игра {game_id}: нет скриншотов')

                except Exception as e:
                    if debug:
                        with lock:
                            self.stderr.write(f'         ❌ Ошибка скриншотов для игры {game_id}: {e}')

            if debug:
                with lock:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {batch_screenshots} скриншотов'
                    )

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_screenshots_parallel(self, game_ids, screenshots_info, debug=False):
        """Параллельная загрузка скриншотов с учетом информации о количестве"""
        if not game_ids:
            return 0

        def process_batch(batch_num, batch_ids, result_map, lock, total_batches, name, debug):
            return self._process_screenshots_batch(
                batch_num, batch_ids, result_map, lock, total_batches, name, debug, screenshots_info
            )

        result_map = self._batch_processor(game_ids, process_batch, '📸', 'скриншотов', debug)

        total_screenshots = sum(result_map.values()) if result_map else 0
        return total_screenshots

    def _process_additional_data_batch(self, batch_num, batch_game_ids, result_map, lock, total_batches, name, debug):
        """Обрабатывает пачку дополнительных данных"""
        try:
            if debug:
                with lock:
                    self.stdout.write(f'         🔄 Пачка {name} {batch_num}/{total_batches}: {len(batch_game_ids)} игр')

            id_list = ','.join(map(str, batch_game_ids))
            query = f'''
                fields name,collections,franchises,involved_companies.company,
                       involved_companies.developer,involved_companies.publisher,
                       themes,player_perspectives,game_modes;
                where id = ({id_list});
            '''

            batch_data = make_igdb_request('games', query, debug=False)

            with lock:
                for game_data in batch_data:
                    game_id = game_data.get('id')
                    if game_id:
                        result_map[game_id] = game_data

                if debug:
                    self.stdout.write(
                        f'         ✅ Пачка {name} {batch_num}/{total_batches}: {len(batch_data)} игр')

        except Exception as e:
            if debug:
                with lock:
                    self.stderr.write(f'         ❌ Ошибка пачки {name} {batch_num}/{total_batches}: {e}')

    def load_additional_data_parallel(self, game_ids, debug=False):
        """Параллельная загрузка дополнительных данных пачками по 10"""
        return self._batch_processor(game_ids, self._process_additional_data_batch, '📚', 'доп. данных', debug)

    def create_model_func(self, model_class):
        """Универсальная функция создания моделей"""
        model_name = model_class.__name__

        def create_func(item_id, item_name):
            obj, _ = model_class.objects.get_or_create(
                igdb_id=item_id,
                defaults={'name': item_name}
            )
            if obj.name != item_name and obj.name.startswith(f'{model_name} '):
                obj.name = item_name
                obj.save()
            return obj

        return create_func

    def create_game_object(self, game_data, cover_map):
        """Создает объект игры"""
        game = Game(
            igdb_id=game_data.get('id'),
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

        cover_id = game_data.get('cover')
        if cover_id and cover_id in cover_map:
            game.cover_url = cover_map[cover_id]

        return game

    def _create_relations(self, all_game_relations, game_map, relation_type, through_model,
                          relation_field, field_name, batch_size=100, debug=False):
        """Универсальный метод для создания связей"""
        relations_to_create = []
        count = 0

        for rel in all_game_relations:
            game = game_map.get(rel['game_id'])
            if not game:
                continue

            for relation_obj in rel.get(relation_field, []):
                relations_to_create.append(through_model(
                    game_id=game.id,
                    **{f'{field_name}_id': relation_obj.id}
                ))
                count += 1

        if relations_to_create:
            through_model.objects.bulk_create(relations_to_create, batch_size=batch_size, ignore_conflicts=True)
            if debug:
                self.stdout.write(f'   ✅ Создано связей с {relation_field}: {count}')

        return count

    def create_relations_batch(self, all_game_relations, genre_map, platform_map, keyword_map, debug=False):
        """Создает связи для игр пачками"""
        if not all_game_relations:
            if debug:
                self.stdout.write('   ⚠️  Нет связей для создания')
            return 0, 0, 0

        # Получаем ID всех игр из relations
        game_ids = [rel['game_id'] for rel in all_game_relations]

        if debug:
            self.stdout.write(f'   🔍 Поиск {len(game_ids)} игр в базе...')

        games = Game.objects.filter(igdb_id__in=game_ids)
        game_map = {game.igdb_id: game for game in games}

        if debug:
            self.stdout.write(f'   ✅ Найдено {len(game_map)} игр в базе')

            # Проверяем, какие игры не найдены
            missing_games = set(game_ids) - set(game_map.keys())
            if missing_games:
                self.stdout.write(f'   ⚠️  Не найдено {len(missing_games)} игр в базе: {list(missing_games)[:5]}...')

        # Подготавливаем связи для каждого типа
        game_relations_prepared = []
        for rel in all_game_relations:
            prepared_rel = {
                'game_id': rel['game_id'],
                'genres': [],
                'platforms': [],
                'keywords': [],
            }

            # Проверяем, есть ли игра в базе
            if rel['game_id'] not in game_map:
                continue

            # Жанры
            for genre_obj in rel.get('genres', []):
                if genre_obj and hasattr(genre_obj, 'id'):
                    prepared_rel['genres'].append(genre_obj)

            # Платформы
            for platform_obj in rel.get('platforms', []):
                if platform_obj and hasattr(platform_obj, 'id'):
                    prepared_rel['platforms'].append(platform_obj)

            # Ключевые слова
            for keyword_obj in rel.get('keywords', []):
                if keyword_obj and hasattr(keyword_obj, 'id'):
                    prepared_rel['keywords'].append(keyword_obj)

            game_relations_prepared.append(prepared_rel)

        if not game_relations_prepared:
            if debug:
                self.stdout.write('   ⚠️  Нет подготовленных связей')
            return 0, 0, 0

        # Создаем связи
        if debug:
            total_genres = sum(len(rel['genres']) for rel in game_relations_prepared)
            total_platforms = sum(len(rel['platforms']) for rel in game_relations_prepared)
            total_keywords = sum(len(rel['keywords']) for rel in game_relations_prepared)
            self.stdout.write(f'   📊 Всего связей для создания:')
            self.stdout.write(f'      • Жанры: {total_genres}')
            self.stdout.write(f'      • Платформы: {total_platforms}')
            self.stdout.write(f'      • Ключевые слова: {total_keywords}')

        game_genre_count = self._create_relations(
            game_relations_prepared, game_map, 'genres', Game.genres.through, 'genres', 'genre', debug=debug
        )

        game_platform_count = self._create_relations(
            game_relations_prepared, game_map, 'platforms', Game.platforms.through, 'platforms', 'platform', debug=debug
        )

        game_keyword_count = self._create_relations(
            game_relations_prepared, game_map, 'keywords', Game.keywords.through, 'keywords', 'keyword', debug=debug
        )

        if debug:
            self.stdout.write(f'   ✅ Создано связей:')
            self.stdout.write(f'      • С жанрами: {game_genre_count}')
            self.stdout.write(f'      • С платформами: {game_platform_count}')
            self.stdout.write(f'      • С ключевыми словами: {game_keyword_count}')

        return game_genre_count, game_platform_count, game_keyword_count

    def create_additional_relations_batch(self, all_game_relations, series_map, company_map,
                                          theme_map, perspective_map, mode_map, debug=False):
        """Создает дополнительные связи для игр пачками"""
        if not all_game_relations:
            if debug:
                self.stdout.write('   ⚠️  Нет дополнительных связей для создания')
            return 0, 0, 0, 0, 0, 0

        # Получаем ID всех игр из relations
        game_ids = [rel['game_id'] for rel in all_game_relations]

        if debug:
            self.stdout.write(f'   🔍 Поиск {len(game_ids)} игр в базе для доп. связей...')

        games = Game.objects.filter(igdb_id__in=game_ids)
        game_map = {game.igdb_id: game for game in games}

        if debug:
            self.stdout.write(f'   ✅ Найдено {len(game_map)} игр в базе')

        # Подготавливаем связи для каждого типа
        game_relations_prepared = []
        for rel in all_game_relations:
            prepared_rel = {
                'game_id': rel['game_id'],
                'series': [],
                'developers': [],
                'publishers': [],
                'themes': [],
                'perspectives': [],
                'modes': [],
            }

            # Проверяем, есть ли игра в базе
            if rel['game_id'] not in game_map:
                continue

            # Серии
            for series_obj in rel.get('series', []):
                if series_obj and hasattr(series_obj, 'id'):
                    prepared_rel['series'].append(series_obj)

            # Разработчики
            for dev_obj in rel.get('developers', []):
                if dev_obj and hasattr(dev_obj, 'id'):
                    prepared_rel['developers'].append(dev_obj)

            # Издатели
            for pub_obj in rel.get('publishers', []):
                if pub_obj and hasattr(pub_obj, 'id'):
                    prepared_rel['publishers'].append(pub_obj)

            # Темы
            for theme_obj in rel.get('themes', []):
                if theme_obj and hasattr(theme_obj, 'id'):
                    prepared_rel['themes'].append(theme_obj)

            # Перспективы
            for perspective_obj in rel.get('perspectives', []):
                if perspective_obj and hasattr(perspective_obj, 'id'):
                    prepared_rel['perspectives'].append(perspective_obj)

            # Режимы
            for mode_obj in rel.get('modes', []):
                if mode_obj and hasattr(mode_obj, 'id'):
                    prepared_rel['modes'].append(mode_obj)

            game_relations_prepared.append(prepared_rel)

        if not game_relations_prepared:
            if debug:
                self.stdout.write('   ⚠️  Нет подготовленных дополнительных связей')
            return 0, 0, 0, 0, 0, 0

        # Статистика перед созданием
        if debug:
            total_series = sum(len(rel['series']) for rel in game_relations_prepared)
            total_developers = sum(len(rel['developers']) for rel in game_relations_prepared)
            total_publishers = sum(len(rel['publishers']) for rel in game_relations_prepared)
            total_themes = sum(len(rel['themes']) for rel in game_relations_prepared)
            total_perspectives = sum(len(rel['perspectives']) for rel in game_relations_prepared)
            total_modes = sum(len(rel['modes']) for rel in game_relations_prepared)

            self.stdout.write(f'   📊 Всего доп. связей для создания:')
            self.stdout.write(f'      • Серии: {total_series}')
            self.stdout.write(f'      • Разработчики: {total_developers}')
            self.stdout.write(f'      • Издатели: {total_publishers}')
            self.stdout.write(f'      • Темы: {total_themes}')
            self.stdout.write(f'      • Перспективы: {total_perspectives}')
            self.stdout.write(f'      • Режимы: {total_modes}')

        # Обновляем серии для игр
        series_count = 0
        games_to_update = []
        for rel in game_relations_prepared:
            game = game_map.get(rel['game_id'])
            if game and rel.get('series'):
                # Берем первую серию
                game.series = rel['series'][0]
                games_to_update.append(game)
                series_count += 1

        if games_to_update:
            Game.objects.bulk_update(games_to_update, ['series'])
            if debug:
                self.stdout.write(f'   ✅ Создано связей с сериями: {series_count}')

        # Создаем остальные связи
        developer_count = self._create_relations(
            game_relations_prepared, game_map, 'developers', Game.developers.through,
            'developers', 'company', debug=debug
        )

        publisher_count = self._create_relations(
            game_relations_prepared, game_map, 'publishers', Game.publishers.through,
            'publishers', 'company', debug=debug
        )

        theme_count = self._create_relations(
            game_relations_prepared, game_map, 'themes', Game.themes.through,
            'themes', 'theme', debug=debug
        )

        perspective_count = self._create_relations(
            game_relations_prepared, game_map, 'perspectives', Game.player_perspectives.through,
            'perspectives', 'playerperspective', debug=debug
        )

        mode_count = self._create_relations(
            game_relations_prepared, game_map, 'modes', Game.game_modes.through,
            'modes', 'gamemode', debug=debug
        )

        if debug:
            self.stdout.write(f'   ✅ Создано доп. связей:')
            self.stdout.write(f'      • С разработчиками: {developer_count}')
            self.stdout.write(f'      • С издателями: {publisher_count}')
            self.stdout.write(f'      • С темами: {theme_count}')
            self.stdout.write(f'      • С перспективами: {perspective_count}')
            self.stdout.write(f'      • С режимами: {mode_count}')

        return series_count, developer_count, publisher_count, theme_count, perspective_count, mode_count

    def load_tactical_rpg_games(self, debug=False, limit=0):
        """Загрузка тактических RPG по жанру и ключевым словам"""
        self.stdout.write('🔍 Поиск тактических RPG...')

        if limit > 0:
            self.stdout.write(f'   🔒 Установлен лимит: {limit} игр')

        if debug:
            self.stdout.write('   🔎 Поиск жанра "Tactical"...')

        # Ищем жанр Tactical
        genre_query = 'fields id,name; where name = "Tactical";'
        tactical_genres = make_igdb_request('genres', genre_query, debug=False)
        tactical_genre_id = tactical_genres[0]['id'] if tactical_genres else None

        if debug:
            if tactical_genre_id:
                self.stdout.write(f'   ✅ Жанр Tactical найден: ID {tactical_genre_id}')
            else:
                self.stdout.write('   ❌ Жанр Tactical не найден')

        if debug:
            self.stdout.write('   🔎 Поиск ключевого слова "tactical turn-based combat"...')

        # Ищем ключевое слово
        keyword_query = 'fields id,name; where name = "tactical turn-based combat";'
        tactical_keywords = make_igdb_request('keywords', keyword_query, debug=False)
        tactical_keyword_id = tactical_keywords[0]['id'] if tactical_keywords else None

        if debug:
            if tactical_keyword_id:
                self.stdout.write(f'   ✅ Ключевое слово найдено: ID {tactical_keyword_id}')
            else:
                self.stdout.write('   ❌ Ключевое слово не найдено')

        if not tactical_genre_id and not tactical_keyword_id:
            self.stdout.write('❌ Не найдены тактический жанр или ключевое слово')
            return []

        where_conditions = []
        if tactical_genre_id:
            where_conditions.append(f'genres = ({tactical_genre_id})')
        if tactical_keyword_id:
            where_conditions.append(f'keywords = ({tactical_keyword_id})')

        where_clause = ' | '.join(where_conditions)
        full_where = f'genres = (12) & ({where_clause})'  # 12 = RPG жанр

        if debug:
            self.stdout.write('   🎯 Построение запроса...')
            self.stdout.write(f'   📋 Условие: {full_where}')

        return self.load_games_by_query(full_where, debug, limit)

    def load_games_by_query(self, where_clause, debug=False, limit=0):
        """Загрузка игр по запросу с пагинацией"""
        all_games = []
        offset = 0
        max_limit = 500
        batch_number = 1

        if debug:
            if limit > 0:
                self.stdout.write(f'   📥 Начало загрузки игр пачками по {max_limit} (всего до {limit})...')
            else:
                self.stdout.write(f'   📥 Начало загрузки игр пачками по {max_limit}...')

        while True:
            # Если установлен лимит и мы уже набрали достаточно игр - выходим
            if limit > 0 and len(all_games) >= limit:
                if debug:
                    self.stdout.write(f'   🎯 Достигнут лимит {limit} игр')
                break

            # Рассчитываем сколько игр еще нужно загрузить
            current_limit = max_limit
            if limit > 0:
                remaining = limit - len(all_games)
                current_limit = min(remaining, max_limit)

            if debug:
                self.stdout.write(f'   📦 Пачка игр {batch_number}: позиция {offset}-{offset + current_limit}...')

            query = f'''
                fields name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
                where {where_clause};
                sort rating_count desc;
                limit {current_limit};
                offset {offset};
            '''.strip()

            batch_games = make_igdb_request('games', query, debug=False)
            if not batch_games:
                if debug:
                    self.stdout.write(f'   💤 Пачка игр {batch_number}: больше игр нет')
                break

            batch_loaded = len(batch_games)
            all_games.extend(batch_games)

            if debug:
                self.stdout.write(f'   ✅ Пачка игр {batch_number}: загружено {batch_loaded} игр')

            offset += batch_loaded
            batch_number += 1

            # Если загрузили меньше, чем запрашивали, значит это последняя пачка
            # ИЛИ если достигли лимита
            if batch_loaded < current_limit or (limit > 0 and len(all_games) >= limit):
                if debug:
                    if limit > 0 and len(all_games) >= limit:
                        self.stdout.write(f'   🏁 Достигнут лимит {limit} игр. Всего пачек: {batch_number - 1}')
                    else:
                        self.stdout.write(f'   🏁 Завершено. Всего пачек игр: {batch_number - 1}')
                break

        if debug:
            if limit > 0:
                self.stdout.write(f'   📊 Загружено игр: {len(all_games)} из {limit} за {batch_number - 1} пачек')
            else:
                self.stdout.write(f'   📊 Всего загружено игр: {len(all_games)} за {batch_number - 1} пачек')

        # Если установлен лимит, обрезаем список до нужного количества
        if limit > 0:
            all_games = all_games[:limit]
            if debug:
                self.stdout.write(f'   ✂️  Обрезано до лимита {limit}: {len(all_games)} игр')

        return all_games

    def process_all_data_sequentially(self, all_games_data, debug=False):
        """Обрабатывает все данные последовательно по типам, но с параллельными пачками внутри каждого типа"""
        total_games = len(all_games_data)

        if debug:
            self.stdout.write(f'📊 Всего игр: {total_games}')

        start_total_time = time.time()
        all_step_times = {}
        loaded_data_stats = {}  # Статистика загруженных данных

        # 1️⃣ Сбор всех данных
        collected_data, collection_stats = self.collect_all_data_with_stats(all_games_data, debug)
        all_step_times['collect'] = collection_stats['collect_time']
        all_step_times['screenshots_info'] = collection_stats.get('screenshots_info_time', 0)
        all_step_times['additional'] = collection_stats['additional_time']

        # Сохраняем статистику собранных данных
        loaded_data_stats['collected'] = {
            'games': len(collected_data['all_game_ids']),
            'covers': len(collected_data['all_cover_ids']),
            'genres': len(collected_data['all_genre_ids']),
            'platforms': len(collected_data['all_platform_ids']),
            'keywords': len(collected_data['all_keyword_ids']),
            'series': len(collected_data['all_series_ids']),
            'companies': len(collected_data['all_company_ids']),
            'themes': len(collected_data['all_theme_ids']),
            'perspectives': len(collected_data['all_perspective_ids']),
            'modes': len(collected_data['all_mode_ids']),
            'screenshots_discovered': collected_data.get('total_possible_screenshots', 0),
        }

        # 2️⃣ Создание основных данных игр
        if debug:
            self.stdout.write('\n1️⃣  🎮 СОЗДАНИЕ ОСНОВНЫХ ДАННЫХ ИГР...')
        start_step = time.time()
        games_data_list = list(collected_data['game_data_map'].values())
        created_count, game_basic_map = self.create_basic_games(games_data_list, debug)
        all_step_times['basic_games'] = time.time() - start_step

        if debug:
            self.stdout.write(f'   ✅ Создано игр: {created_count}/{total_games}')
            self.stdout.write(f'   ⏱️  Время: {all_step_times["basic_games"]:.2f}с')

        # Если не создано ни одной игры, выходим
        if created_count == 0:
            if debug:
                self.stdout.write('   ⚠️  Нет новых игр для загрузки')

            total_time = time.time() - start_total_time
            skipped_count = total_games  # Все игры пропущены

            # Собираем статистику даже если игр нет
            stats = self._collect_final_statistics(
                total_games, 0, skipped_count, 0, total_time,
                loaded_data_stats, all_step_times, debug
            )

            if debug:
                self._print_complete_statistics(stats)

            return stats

        # 3️⃣ Загрузка всех типов данных последовательно
        data_maps, data_step_times = self.load_all_data_types_sequentially(collected_data, debug)
        all_step_times.update(data_step_times)

        # Сохраняем статистику загруженных данных
        loaded_data_stats['loaded'] = {
            'covers': len(data_maps.get('cover_map', {})),
            'genres': len(data_maps.get('genre_map', {})),
            'platforms': len(data_maps.get('platform_map', {})),
            'keywords': len(data_maps.get('keyword_map', {})),
            'series': len(data_maps.get('series_map', {})),
            'companies': len(data_maps.get('company_map', {})),
            'themes': len(data_maps.get('theme_map', {})),
            'perspectives': len(data_maps.get('perspective_map', {})),
            'modes': len(data_maps.get('mode_map', {})),
        }

        # 4️⃣ Обновление игр обложками
        if debug:
            self.stdout.write('\n📝 ОБНОВЛЕНИЕ ИГР ОБЛОЖКАМИ...')
        start_step = time.time()
        updated_covers = self.update_games_with_covers(
            game_basic_map, data_maps['cover_map'], collected_data['game_data_map'], debug
        )
        all_step_times['update_covers'] = time.time() - start_step

        if debug:
            self.stdout.write(f'   ✅ Обновлено обложек: {updated_covers}')

        # 5️⃣ Загрузка скриншотов
        if debug:
            self.stdout.write('\n📸 ПАРАЛЛЕЛЬНАЯ ЗАГРУЗКА СКРИНШОТОВ...')
        start_step = time.time()

        # Получаем информацию о скриншотах
        screenshots_info = collected_data.get('screenshots_info', {})
        screenshots_loaded = self.load_screenshots_parallel(
            list(game_basic_map.keys()),
            screenshots_info,
            debug=debug
        )

        all_step_times['screenshots'] = time.time() - start_step

        if debug:
            self.stdout.write(f'   ✅ Загружено скриншотов: {screenshots_loaded}')
            self.stdout.write(f'   ⏱️  Время: {all_step_times["screenshots"]:.2f}с')

        # 6️⃣ Подготовка связей
        all_game_relations, prepare_time = self.prepare_game_relations(
            game_basic_map, collected_data['game_data_map'],
            collected_data['additional_data_map'], data_maps, debug
        )
        all_step_times['prepare_relations'] = prepare_time

        # 7️⃣ Создание всех связей
        relations_results, possible_stats, relations_time = self.create_all_relations(all_game_relations, data_maps,
                                                                                      debug)
        all_step_times['relations'] = relations_time

        total_time = time.time() - start_total_time
        skipped_count = total_games - created_count  # Определяем здесь!

        # 8️⃣ Собираем полную финальную статистику
        stats = self._collect_final_statistics(
            total_games, created_count, skipped_count, screenshots_loaded,
            total_time, loaded_data_stats, all_step_times,
            relations_results, possible_stats, debug
        )

        # 9️⃣ Выводим полную статистику
        if debug:
            self._print_complete_statistics(stats)

        return stats

    def handle(self, *args, **options):
        overwrite = options['overwrite']
        debug = options['debug']
        limit = options['limit']

        self.stdout.write('🎮 ЗАГРУЗКА ТАКТИЧЕСКИХ RPG ИЗ IGDB')
        self.stdout.write('=' * 60)

        if limit > 0:
            self.stdout.write(f'📊 ЛИМИТ: загружается не более {limit} игр')

        if debug:
            self.stdout.write('🐛 РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН')
            self.stdout.write('-' * 40)

        # Загружаем тактические RPG
        all_games = self.load_tactical_rpg_games(debug, limit)

        if not all_games:
            self.stdout.write('❌ Не найдено игр для загрузки')
            return

        self.stdout.write(f'📥 Найдено игр для обработки: {len(all_games)}')

        if overwrite:
            self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ - найденные тактические RPG будут удалены и загружены заново!')

            # Получаем ID найденных игр
            game_ids_to_delete = [game_data.get('id') for game_data in all_games if game_data.get('id')]

            if game_ids_to_delete:
                if debug:
                    self.stdout.write(f'   🔍 Поиск игр для удаления: {len(game_ids_to_delete)} ID')

                # Находим игры в базе по igdb_id
                games_to_delete = Game.objects.filter(igdb_id__in=game_ids_to_delete)
                count_before = games_to_delete.count()

                if debug:
                    self.stdout.write(f'   📊 Найдено игр для удаления в базе: {count_before}')

                if count_before > 0:
                    # Удаляем найденные игры (связанные объекты удалятся каскадно)
                    deleted_info = games_to_delete.delete()

                    # Разбираем результат delete()
                    if isinstance(deleted_info, tuple) and len(deleted_info) == 2:
                        total_deleted, deleted_details = deleted_info

                        # Выводим детализированную статистику
                        self.stdout.write(f'🗑️  УДАЛЕНИЕ ЗАВЕРШЕНО:')
                        self.stdout.write(f'   • Всего удалено объектов: {total_deleted}')

                        # Выводим детали по моделям
                        for model_name, count in deleted_details.items():
                            model_display = model_name.split('.')[-1]  # Извлекаем имя модели
                            if count > 0:
                                self.stdout.write(f'   • {model_display}: {count}')
                    else:
                        # Для старых версий Django
                        self.stdout.write(f'🗑️  Удалено игр и связанных данных: {deleted_info}')
                else:
                    self.stdout.write('   ℹ️  Не найдено игр для удаления в базе данных')
            else:
                self.stdout.write('   ⚠️  Не найдено ID игр для удаления')
        else:
            if debug:
                existing_games = Game.objects.count()
                self.stdout.write(f'📊 Текущее количество игр в базе: {existing_games}')

        if debug:
            self.stdout.write('\n⚡ Начало обработки...')
            self.stdout.write('-' * 40)

        # Обработка данных
        result_stats = self.process_all_data_sequentially(all_games, debug)

        # КРАТКАЯ статистика в конце (если не в режиме отладки)
        if not debug:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
            self.stdout.write(f'⏱️  Время: {result_stats["total_time"]:.2f}с')

            if limit > 0:
                self.stdout.write(f'📊 Лимит: {limit}')

            self.stdout.write(f'🎮 Найдено: {result_stats["total_games_found"]}')
            self.stdout.write(f'✅ Загружено: {result_stats["created_count"]}')
            self.stdout.write(f'⏭️  Пропущено: {result_stats["skipped_count"]}')

    def collect_all_data_with_stats(self, all_games_data, debug=False):
        """Собирает все данные со статистикой"""
        total_games = len(all_games_data)

        if debug:
            self.stdout.write(f'📊 Всего игр: {total_games}')

        start_total_time = time.time()
        collection_stats = {}

        # 1️⃣ Сбор основных ID из игр
        if debug:
            self.stdout.write('\n1️⃣  🔍 СБОР ОСНОВНЫХ ID ИЗ ИГР...')

        start_collect_time = time.time()
        collected_data = self.collect_all_data_ids(all_games_data, debug)
        collect_time = time.time() - start_collect_time
        collection_stats['collect_time'] = collect_time

        if debug:
            self.stdout.write(f'   ✅ Основные ID собраны за {collect_time:.2f}с')

        # 2️⃣ Сбор информации о скриншотах (исправленный)
        if debug:
            self.stdout.write('\n2️⃣  📸 СБОР ИНФОРМАЦИИ О СКРИНШОТАХ...')

        start_screenshots_info = time.time()

        # Получаем ID всех игр для сбора информации о скриншотах
        game_ids_for_screenshots = collected_data['all_game_ids']

        if debug:
            self.stdout.write(f'   🔍 Проверка скриншотов для {len(game_ids_for_screenshots)} игр...')

        screenshots_info_result = self.collect_screenshots_info(game_ids_for_screenshots, debug)

        # Сохраняем информацию о скриншотах
        collected_data['screenshots_info'] = screenshots_info_result.get('screenshots_info', {})
        collected_data['total_possible_screenshots'] = screenshots_info_result.get('total_possible_screenshots', 0)

        screenshots_info_time = time.time() - start_screenshots_info
        collection_stats['screenshots_info_time'] = screenshots_info_time

        if debug:
            discovered = collected_data['total_possible_screenshots']
            games_with_screenshots = len(
                [v for v in screenshots_info_result.get('screenshots_info', {}).values() if v > 0])
            self.stdout.write(
                f'   ✅ Найдено скриншотов: {discovered} для {games_with_screenshots} игр за {screenshots_info_time:.2f}с')

        # 3️⃣ Загрузка дополнительных данных (серии, компании, темы и т.д.)
        if debug:
            self.stdout.write('\n3️⃣  📚 ЗАГРУЗКА ДОПОЛНИТЕЛЬНЫХ ДАННЫХ...')

        start_additional = time.time()
        additional_data_map, additional_stats = self.load_and_process_additional_data(
            collected_data['all_game_ids'], debug
        )
        collected_data['additional_data_map'] = additional_data_map

        # Объединяем ID из дополнительных данных
        collected_data['all_series_ids'] = additional_stats.get('all_series_ids', [])
        collected_data['all_company_ids'] = additional_stats.get('all_company_ids', [])
        collected_data['all_theme_ids'] = additional_stats.get('all_theme_ids', [])
        collected_data['all_perspective_ids'] = additional_stats.get('all_perspective_ids', [])
        collected_data['all_mode_ids'] = additional_stats.get('all_mode_ids', [])

        additional_time = time.time() - start_additional
        collection_stats['additional_time'] = additional_time

        if debug:
            self.stdout.write(f'   ✅ Дополнительные данные загружены за {additional_time:.2f}с')

        # 4️⃣ Общая статистика собранных данных
        if debug:
            self.stdout.write('\n📊 ОБЩАЯ СТАТИСТИКА СОБРАННЫХ ДАННЫХ:')
            self.stdout.write('   ────────────────────────────────')

            # Основные данные
            self.stdout.write(f'   🎮 Игр: {len(collected_data["all_game_ids"])}')
            self.stdout.write(f'   🖼️  Обложек: {len(collected_data["all_cover_ids"])}')
            self.stdout.write(f'   🎭 Жанров: {len(collected_data["all_genre_ids"])}')
            self.stdout.write(f'   🖥️  Платформ: {len(collected_data["all_platform_ids"])}')
            self.stdout.write(f'   🔑 Ключевых слов: {len(collected_data["all_keyword_ids"])}')

            # Дополнительные данные
            self.stdout.write(f'   📚 Серий: {len(collected_data.get("all_series_ids", []))}')
            self.stdout.write(f'   🏢 Компаний: {len(collected_data.get("all_company_ids", []))}')
            self.stdout.write(f'   🎨 Тем: {len(collected_data.get("all_theme_ids", []))}')
            self.stdout.write(f'   👁️  Перспектив: {len(collected_data.get("all_perspective_ids", []))}')
            self.stdout.write(f'   🎮 Режимов: {len(collected_data.get("all_mode_ids", []))}')

            # Скриншоты
            discovered = collected_data.get('total_possible_screenshots', 0)
            if discovered > 0:
                games_with = len([v for v in collected_data.get('screenshots_info', {}).values() if v > 0])
                self.stdout.write(f'   📸 Скриншотов: {discovered} (в {games_with} играх)')
            else:
                self.stdout.write(f'   📸 Скриншотов: {discovered}')

            # Время
            total_collection_time = collect_time + screenshots_info_time + additional_time
            self.stdout.write(f'   ⏱️  Общее время сбора: {total_collection_time:.2f}с')

            # Детальное время
            self.stdout.write(f'   ⏱️  Детальное время:')
            self.stdout.write(f'      • Сбор ID: {collect_time:.2f}с')
            self.stdout.write(f'      • Инфо о скриншотах: {screenshots_info_time:.2f}с')
            self.stdout.write(f'      • Доп. данные: {additional_time:.2f}с')

            # Пропорции времени
            if total_collection_time > 0:
                self.stdout.write(f'   📈 Пропорции времени:')
                self.stdout.write(f'      • Сбор ID: {collect_time / total_collection_time * 100:.1f}%')
                self.stdout.write(f'      • Скриншоты: {screenshots_info_time / total_collection_time * 100:.1f}%')
                self.stdout.write(f'      • Доп. данные: {additional_time / total_collection_time * 100:.1f}%')

        # 5️⃣ Готовим финальные данные для возврата
        # Добавляем ключ screenshots_discovered для совместимости
        collected_data['screenshots_discovered'] = collected_data.get('total_possible_screenshots', 0)

        # Собираем всю статистику для возврата
        stats = {
            'collect_time': collect_time,
            'screenshots_info_time': screenshots_info_time,
            'additional_time': additional_time,
            'total_games': total_games,
            'total_collection_time': collect_time + screenshots_info_time + additional_time,
            'collected_counts': {
                'games': len(collected_data.get('all_game_ids', [])),
                'covers': len(collected_data.get('all_cover_ids', [])),
                'genres': len(collected_data.get('all_genre_ids', [])),
                'platforms': len(collected_data.get('all_platform_ids', [])),
                'keywords': len(collected_data.get('all_keyword_ids', [])),
                'series': len(collected_data.get('all_series_ids', [])),
                'companies': len(collected_data.get('all_company_ids', [])),
                'themes': len(collected_data.get('all_theme_ids', [])),
                'perspectives': len(collected_data.get('all_perspective_ids', [])),
                'modes': len(collected_data.get('all_mode_ids', [])),
                'screenshots': collected_data.get('total_possible_screenshots', 0),
                'games_with_screenshots': len(
                    [v for v in collected_data.get('screenshots_info', {}).values() if v > 0]),
            }
        }

        # 6️⃣ Проверяем данные на целостность (только в режиме отладки)
        if debug:
            self._validate_collected_data(collected_data, stats['collected_counts'])

        return collected_data, stats

    def _validate_collected_data(self, collected_data, collected_counts, debug=True):
        """Проверяет целостность собранных данных"""
        if not debug:
            return

        self.stdout.write('\n🔍 ПРОВЕРКА ЦЕЛОСТНОСТИ ДАННЫХ:')

        issues = []

        # Проверяем основные поля
        required_fields = [
            'game_data_map',
            'all_game_ids',
            'all_cover_ids',
            'all_genre_ids',
            'all_platform_ids',
            'all_keyword_ids',
            'additional_data_map',
            'screenshots_info'
        ]

        for field in required_fields:
            if field not in collected_data:
                issues.append(f'Отсутствует поле: {field}')
            elif not collected_data[field]:
                issues.append(f'Пустое поле: {field}')

        # Проверяем соответствие счетчиков
        if 'game_data_map' in collected_data:
            map_count = len(collected_data['game_data_map'])
            ids_count = len(collected_data.get('all_game_ids', []))
            if map_count != ids_count:
                issues.append(f'Несоответствие game_data_map ({map_count}) и all_game_ids ({ids_count})')

        # Проверяем скриншоты
        if 'total_possible_screenshots' in collected_data:
            screenshots_count = collected_data['total_possible_screenshots']
            if screenshots_count == 0:
                issues.append('Не обнаружено скриншотов (total_possible_screenshots = 0)')

        if 'screenshots_info' in collected_data:
            screenshots_info = collected_data['screenshots_info']
            if not isinstance(screenshots_info, dict):
                issues.append('screenshots_info не является словарем')
            elif len(screenshots_info) == 0:
                issues.append('screenshots_info пустой словарь')

        # Выводим результаты проверки
        if issues:
            self.stdout.write('   ⚠️  Найдены проблемы:')
            for issue in issues:
                self.stdout.write(f'      • {issue}')
        else:
            self.stdout.write('   ✅ Данные целостны')

        # Проверяем соответствие collected_counts
        if collected_counts:
            self.stdout.write('   📊 Проверка счетчиков:')
            for key, count in collected_counts.items():
                if count == 0 and key not in ['games_with_screenshots']:
                    self.stdout.write(f'      ⚠️  {key}: {count} (возможно, отсутствуют данные)')
                else:
                    self.stdout.write(f'      ✓ {key}: {count}')

    def load_and_process_additional_data(self, game_ids, debug=False):
        """Загружает и обрабатывает дополнительные данные"""
        additional_data_map = self.load_additional_data_parallel(game_ids, debug)

        # Собираем ID дополнительных данных
        all_series_ids = set()
        all_company_ids = set()
        all_theme_ids = set()
        all_perspective_ids = set()
        all_mode_ids = set()

        for additional_data in additional_data_map.values():
            if additional_data.get('collections'):
                all_series_ids.update(additional_data['collections'])

            if additional_data.get('themes'):
                all_theme_ids.update(additional_data['themes'])

            if additional_data.get('player_perspectives'):
                all_perspective_ids.update(additional_data['player_perspectives'])

            if additional_data.get('game_modes'):
                all_mode_ids.update(additional_data['game_modes'])

            if additional_data.get('involved_companies'):
                for company_data in additional_data['involved_companies']:
                    if company_data.get('company'):
                        all_company_ids.add(company_data['company'])

        return additional_data_map, {
            'all_series_ids': list(all_series_ids),
            'all_company_ids': list(all_company_ids),
            'all_theme_ids': list(all_theme_ids),
            'all_perspective_ids': list(all_perspective_ids),
            'all_mode_ids': list(all_mode_ids)
        }

    def create_basic_games(self, games_data_list, debug=False):
        """Создает игры с основными данными"""
        games_basic_to_create = []
        game_basic_map = {}

        for game_data in games_data_list:
            game_id = game_data.get('id')
            if not game_id:
                continue

            if Game.objects.filter(igdb_id=game_id).exists():
                continue

            try:
                game = self.create_game_object(game_data, {})
                games_basic_to_create.append(game)
                game_basic_map[game_id] = game

            except Exception as e:
                if debug:
                    self.stderr.write(f'   ❌ Ошибка создания игры {game_id}: {e}')

        # Сохраняем игры в базу
        if games_basic_to_create:
            Game.objects.bulk_create(games_basic_to_create)

        return len(games_basic_to_create), game_basic_map

    def update_games_with_covers(self, game_basic_map, cover_map, game_data_map, debug=False):
        """Обновляет игры обложками"""
        games_to_update = []

        for game in Game.objects.filter(igdb_id__in=game_basic_map.keys()):
            game_data = game_data_map.get(game.igdb_id)
            if game_data and game_data.get('cover'):
                cover_id = game_data['cover']
                if cover_id in cover_map:
                    game.cover_url = cover_map[cover_id]
                    games_to_update.append(game)

        if games_to_update:
            Game.objects.bulk_update(games_to_update, ['cover_url'])

        return len(games_to_update)

    def load_all_data_types_sequentially(self, collected_data, debug=False):
        """Последовательно загружает все типы данных"""
        step_times = {}
        data_maps = {}

        # 1️⃣ ПАРАЛЛЕЛЬНАЯ загрузка обложек
        if debug:
            self.stdout.write('\n1️⃣  🖼️  ЗАГРУЗКА ОБЛОЖЕК...')
        start_step = time.time()
        data_maps['cover_map'] = self.load_covers_parallel(collected_data['all_cover_ids'], debug)
        step_times['covers'] = time.time() - start_step

        # 2️⃣ ПАРАЛЛЕЛЬНАЯ загрузка жанров
        if debug:
            self.stdout.write('\n2️⃣  🎭 ЗАГРУЗКА ЖАНРОВ...')
        start_step = time.time()
        data_maps['genre_map'] = self.load_data_parallel_generic(
            collected_data['all_genre_ids'], 'genres', Genre,
            self.create_model_func(Genre), '🎭', 'жанров', debug
        )
        step_times['genres'] = time.time() - start_step

        # 3️⃣ ПАРАЛЛЕЛЬНАЯ загрузка платформ
        if debug:
            self.stdout.write('\n3️⃣  🖥️  ЗАГРУЗКА ПЛАТФОРМ...')
        start_step = time.time()
        data_maps['platform_map'] = self.load_data_parallel_generic(
            collected_data['all_platform_ids'], 'platforms', Platform,
            self.create_model_func(Platform), '🖥️', 'платформ', debug
        )
        step_times['platforms'] = time.time() - start_step

        # 4️⃣ ПАРАЛЛЕЛЬНАЯ загрузка ключевых слов
        if debug:
            self.stdout.write('\n4️⃣  🔑 ЗАГРУЗКА КЛЮЧЕВЫХ СЛОВ...')
        start_step = time.time()
        data_maps['keyword_map'] = self.load_data_parallel_generic(
            collected_data['all_keyword_ids'], 'keywords', Keyword,
            self.create_model_func(Keyword), '🔑', 'ключевых слов', debug
        )
        step_times['keywords'] = time.time() - start_step

        # 5️⃣ ПАРАЛЛЕЛЬНАЯ загрузка серий
        if debug:
            self.stdout.write('\n5️⃣  📚 ЗАГРУЗКА СЕРИЙ...')
        start_step = time.time()
        data_maps['series_map'] = self.load_data_parallel_generic(
            collected_data['all_series_ids'], 'collections', Series,
            self.create_model_func(Series), '📚', 'серий', debug
        )
        step_times['series'] = time.time() - start_step

        # 6️⃣ ПАРАЛЛЕЛЬНАЯ загрузка компаний
        if debug:
            self.stdout.write('\n6️⃣  🏢 ЗАГРУЗКА КОМПАНИЙ...')
        start_step = time.time()
        data_maps['company_map'] = self.load_data_parallel_generic(
            collected_data['all_company_ids'], 'companies', Company,
            self.create_model_func(Company), '🏢', 'компаний', debug
        )
        step_times['companies'] = time.time() - start_step

        # 7️⃣ ПАРАЛЛЕЛЬНАЯ загрузка тем
        if debug:
            self.stdout.write('\n7️⃣  🎨 ЗАГРУЗКА ТЕМ...')
        start_step = time.time()
        data_maps['theme_map'] = self.load_data_parallel_generic(
            collected_data['all_theme_ids'], 'themes', Theme,
            self.create_model_func(Theme), '🎨', 'тем', debug
        )
        step_times['themes'] = time.time() - start_step

        # 8️⃣ ПАРАЛЛЕЛЬНАЯ загрузка перспектив
        if debug:
            self.stdout.write('\n8️⃣  👁️  ЗАГРУЗКА ПЕРСПЕКТИВ...')
        start_step = time.time()
        data_maps['perspective_map'] = self.load_data_parallel_generic(
            collected_data['all_perspective_ids'], 'player_perspectives', PlayerPerspective,
            self.create_model_func(PlayerPerspective), '👁️', 'перспектив', debug
        )
        step_times['perspectives'] = time.time() - start_step

        # 9️⃣ ПАРАЛЛЕЛЬНАЯ загрузка режимов
        if debug:
            self.stdout.write('\n9️⃣  🎮 ЗАГРУЗКА РЕЖИМОВ...')
        start_step = time.time()
        data_maps['mode_map'] = self.load_data_parallel_generic(
            collected_data['all_mode_ids'], 'game_modes', GameMode,
            self.create_model_func(GameMode), '🎮', 'режимов', debug
        )
        step_times['modes'] = time.time() - start_step

        return data_maps, step_times

    def prepare_game_relations(self, game_basic_map, game_data_map, additional_data_map, data_maps, debug=False):
        """Подготавливает связи для игр"""
        if debug:
            self.stdout.write('\n📋 ПОДГОТОВКА СВЯЗЕЙ ДЛЯ ИГР...')

        start_step = time.time()
        all_game_relations = []
        games_without_data = 0
        games_without_additional = 0

        for game_id in game_basic_map.keys():
            game_data = game_data_map.get(game_id)
            if not game_data:
                games_without_data += 1
                continue

            additional_data = additional_data_map.get(game_id, {})
            if not additional_data:
                games_without_additional += 1

            developer_ids = set()
            publisher_ids = set()

            if additional_data.get('involved_companies'):
                for company_data in additional_data['involved_companies']:
                    company_id = company_data.get('company')
                    if not company_id:
                        continue

                    if company_data.get('developer', False):
                        developer_ids.add(company_id)
                    if company_data.get('publisher', False):
                        publisher_ids.add(company_id)

            relations = {
                'game_id': game_id,
                'genres': [],
                'platforms': [],
                'keywords': [],
                'series': [],
                'developers': [],
                'publishers': [],
                'themes': [],
                'perspectives': [],
                'modes': [],
            }

            # Жанры
            for gid in game_data.get('genres', []):
                if gid in data_maps['genre_map']:
                    relations['genres'].append(data_maps['genre_map'][gid])

            # Платформы
            for pid in game_data.get('platforms', []):
                if pid in data_maps['platform_map']:
                    relations['platforms'].append(data_maps['platform_map'][pid])

            # Ключевые слова
            for kid in game_data.get('keywords', []):
                if kid in data_maps['keyword_map']:
                    relations['keywords'].append(data_maps['keyword_map'][kid])

            # Серии
            for sid in additional_data.get('collections', []):
                if sid in data_maps['series_map']:
                    relations['series'].append(data_maps['series_map'][sid])

            # Разработчики
            for cid in developer_ids:
                if cid in data_maps['company_map']:
                    relations['developers'].append(data_maps['company_map'][cid])

            # Издатели
            for cid in publisher_ids:
                if cid in data_maps['company_map']:
                    relations['publishers'].append(data_maps['company_map'][cid])

            # Темы
            for tid in additional_data.get('themes', []):
                if tid in data_maps['theme_map']:
                    relations['themes'].append(data_maps['theme_map'][tid])

            # Перспективы
            for pid in additional_data.get('player_perspectives', []):
                if pid in data_maps['perspective_map']:
                    relations['perspectives'].append(data_maps['perspective_map'][pid])

            # Режимы
            for mid in additional_data.get('game_modes', []):
                if mid in data_maps['mode_map']:
                    relations['modes'].append(data_maps['mode_map'][mid])

            # Проверяем, есть ли хотя бы какие-то связи
            has_relations = any([
                relations['genres'],
                relations['platforms'],
                relations['keywords'],
                relations['series'],
                relations['developers'],
                relations['publishers'],
                relations['themes'],
                relations['perspectives'],
                relations['modes'],
            ])

            if has_relations:
                all_game_relations.append(relations)

            if debug and not has_relations:
                self.stdout.write(f'   ⚠️  Игра {game_id} не имеет связей')

        step_time = time.time() - start_step

        if debug:
            self.stdout.write(f'   📊 Подготовлено связей для {len(all_game_relations)} игр')
            self.stdout.write(f'   ⚠️  Игр без основных данных: {games_without_data}')
            self.stdout.write(f'   ⚠️  Игр без дополнительных данных: {games_without_additional}')

            # Статистика по типам связей
            if all_game_relations:
                stats = {
                    'genres': 0,
                    'platforms': 0,
                    'keywords': 0,
                    'series': 0,
                    'developers': 0,
                    'publishers': 0,
                    'themes': 0,
                    'perspectives': 0,
                    'modes': 0,
                }

                for rel in all_game_relations:
                    stats['genres'] += len(rel['genres'])
                    stats['platforms'] += len(rel['platforms'])
                    stats['keywords'] += len(rel['keywords'])
                    stats['series'] += len(rel['series'])
                    stats['developers'] += len(rel['developers'])
                    stats['publishers'] += len(rel['publishers'])
                    stats['themes'] += len(rel['themes'])
                    stats['perspectives'] += len(rel['perspectives'])
                    stats['modes'] += len(rel['modes'])

                self.stdout.write(f'   📈 Статистика связей:')
                for key, count in stats.items():
                    if count > 0:
                        self.stdout.write(f'      • {key}: {count}')

        return all_game_relations, step_time

    def create_all_relations(self, all_game_relations, data_maps, debug=False):
        """Создает все связи для игр и возвращает статистику возможных связей"""
        if debug:
            self.stdout.write('\n🔗 СОЗДАНИЕ СВЯЗЕЙ...')

        start_step = time.time()

        # Собираем статистику возможных связей
        possible_stats = {
            'possible_genre_relations': 0,
            'possible_platform_relations': 0,
            'possible_keyword_relations': 0,
            'possible_series_relations': 0,
            'possible_developer_relations': 0,
            'possible_publisher_relations': 0,
            'possible_theme_relations': 0,
            'possible_perspective_relations': 0,
            'possible_mode_relations': 0,
        }

        # Подсчитываем возможные связи
        for rel in all_game_relations:
            possible_stats['possible_genre_relations'] += len(rel.get('genres', []))
            possible_stats['possible_platform_relations'] += len(rel.get('platforms', []))
            possible_stats['possible_keyword_relations'] += len(rel.get('keywords', []))
            possible_stats['possible_series_relations'] += len(rel.get('series', []))
            possible_stats['possible_developer_relations'] += len(rel.get('developers', []))
            possible_stats['possible_publisher_relations'] += len(rel.get('publishers', []))
            possible_stats['possible_theme_relations'] += len(rel.get('themes', []))
            possible_stats['possible_perspective_relations'] += len(rel.get('perspectives', []))
            possible_stats['possible_mode_relations'] += len(rel.get('modes', []))

        # Основные связи
        genre_relations, platform_relations, keyword_relations = self.create_relations_batch(
            all_game_relations, data_maps['genre_map'], data_maps['platform_map'],
            data_maps['keyword_map'], debug
        )

        # Дополнительные связи
        series_relations, developer_relations, publisher_relations, theme_relations, perspective_relations, mode_relations = self.create_additional_relations_batch(
            all_game_relations, data_maps['series_map'], data_maps['company_map'],
            data_maps['theme_map'], data_maps['perspective_map'], data_maps['mode_map'], debug
        )

        step_time = time.time() - start_step

        results = {
            'genre_relations': genre_relations,
            'platform_relations': platform_relations,
            'keyword_relations': keyword_relations,
            'series_relations': series_relations,
            'developer_relations': developer_relations,
            'publisher_relations': publisher_relations,
            'theme_relations': theme_relations,
            'perspective_relations': perspective_relations,
            'mode_relations': mode_relations
        }

        return results, possible_stats, step_time

    def print_final_statistics(self, total_games, created_count, screenshots_loaded,
                               total_time, all_step_times, relations_results, collection_stats):
        """Выводит финальную статистику ПОШАГОВОЙ ОБРАБОТКИ"""
        self.stdout.write(f'\n📊 ДЕТАЛЬНАЯ СТАТИСТИКА ОБРАБОТКИ:')
        self.stdout.write(f'   ⏱️  Общее время: {total_time:.2f}с')

        self.stdout.write(f'   📈 Время по шагам:')
        self.stdout.write(f'      🔍 Сбор ID: {all_step_times.get("collect", 0):.2f}с')
        self.stdout.write(f'      📚 Доп. данные: {all_step_times.get("additional", 0):.2f}с')
        self.stdout.write(f'      🎮 Основные игры: {all_step_times.get("basic_games", 0):.2f}с')
        self.stdout.write(f'      🖼️  Обложки: {all_step_times.get("covers", 0):.2f}с')
        self.stdout.write(f'      📝 Обновление обложек: {all_step_times.get("update_covers", 0):.2f}с')

        if 'screenshots' in all_step_times:
            self.stdout.write(f'      📸 Скриншоты: {all_step_times.get("screenshots", 0):.2f}с')

        self.stdout.write(f'      🎭 Жанры: {all_step_times.get("genres", 0):.2f}с')
        self.stdout.write(f'      🖥️  Платформы: {all_step_times.get("platforms", 0):.2f}с')
        self.stdout.write(f'      🔑 Ключевые слова: {all_step_times.get("keywords", 0):.2f}с')
        self.stdout.write(f'      📚 Серии: {all_step_times.get("series", 0):.2f}с')
        self.stdout.write(f'      🏢 Компании: {all_step_times.get("companies", 0):.2f}с')
        self.stdout.write(f'      🎨 Темы: {all_step_times.get("themes", 0):.2f}с')
        self.stdout.write(f'      👁️  Перспективы: {all_step_times.get("perspectives", 0):.2f}с')
        self.stdout.write(f'      🎮 Режимы: {all_step_times.get("modes", 0):.2f}с')
        self.stdout.write(f'      📋 Подготовка связей: {all_step_times.get("prepare_relations", 0):.2f}с')
        self.stdout.write(f'      🔗 Создание связей: {all_step_times.get("relations", 0):.2f}с')

        if total_time > 0:
            self.stdout.write(f'   🚀 Скорость: {total_games / total_time:.1f} игр/сек')

        self.stdout.write(f'   🎮 Игр создано: {created_count}/{total_games}')

        if screenshots_loaded > 0:
            self.stdout.write(f'   📸 Скриншотов загружено: {screenshots_loaded}')

        self.stdout.write(f'   🔗 Связей создано:')
        self.stdout.write(f'      🎭 С жанрами: {relations_results.get("genre_relations", 0)}')
        self.stdout.write(f'      🖥️  С платформами: {relations_results.get("platform_relations", 0)}')
        self.stdout.write(f'      🔑 С ключевыми словами: {relations_results.get("keyword_relations", 0)}')
        self.stdout.write(f'      📚 С сериями: {relations_results.get("series_relations", 0)}')
        self.stdout.write(f'      🏢 С разработчиками: {relations_results.get("developer_relations", 0)}')
        self.stdout.write(f'      📦 С издателями: {relations_results.get("publisher_relations", 0)}')
        self.stdout.write(f'      🎨 С темами: {relations_results.get("theme_relations", 0)}')
        self.stdout.write(f'      👁️  С перспективами: {relations_results.get("perspective_relations", 0)}')
        self.stdout.write(f'      🎮 С режимами: {relations_results.get("mode_relations", 0)}')

    def _collect_final_statistics(self, total_games, created_count, skipped_count, screenshots_loaded,
                                  total_time, loaded_data_stats, all_step_times,
                                  relations_results=None, relations_possible=None, debug=False):
        """Собирает полную финальную статистику"""

        # Статистика базы данных
        total_games_in_db = Game.objects.count()
        total_screenshots = Screenshot.objects.count()
        total_genres = Genre.objects.count()
        total_platforms = Platform.objects.count()
        total_keywords = Keyword.objects.count()
        total_series = Series.objects.count()
        total_companies = Company.objects.count()
        total_themes = Theme.objects.count()
        total_perspectives = PlayerPerspective.objects.count()
        total_modes = GameMode.objects.count()

        # Отладочный вывод для диагностики скриншотов
        if debug:
            self.stdout.write(f'\n🔍 ОТЛАДОЧНАЯ ИНФОРМАЦИЯ О СКРИНШОТАХ:')
            self.stdout.write(f'   • Загружено скриншотов: {screenshots_loaded}')
            self.stdout.write(
                f'   • Собрано информации о скриншотах: {loaded_data_stats.get("collected", {}).get("total_possible_screenshots", 0)}')
            self.stdout.write(f'   • Данные сбора: {loaded_data_stats.get("collected", {})}')
            self.stdout.write(f'   • Данные загрузки: {loaded_data_stats.get("loaded", {})}')

            # Проверяем информацию о скриншотах в collected_data
            if 'collected' in loaded_data_stats:
                collected_data = loaded_data_stats['collected']
                if 'screenshots_discovered' in collected_data:
                    discovered = collected_data['screenshots_discovered']
                    self.stdout.write(f'   • Обнаружено скриншотов (discovered): {discovered}')
                if 'screenshots_info' in collected_data:
                    screenshots_info = collected_data.get('screenshots_info', {})
                    self.stdout.write(f'   • Информация о скриншотах (screenshots_info): {len(screenshots_info)} игр')

        # Добавляем статистику по скриншотам в collected_data
        collected_data_with_screenshots = loaded_data_stats.get('collected', {}).copy()

        # Получаем правильное количество обнаруженных скриншотов
        discovered_screenshots = 0
        if 'collected' in loaded_data_stats and 'screenshots_discovered' in loaded_data_stats['collected']:
            discovered_screenshots = loaded_data_stats['collected']['screenshots_discovered']
        elif 'total_possible_screenshots' in collected_data_with_screenshots:
            discovered_screenshots = collected_data_with_screenshots['total_possible_screenshots']

        collected_data_with_screenshots['screenshots_discovered'] = discovered_screenshots

        # Добавляем информацию о том, сколько игр имеют скриншоты
        if 'screenshots_info' in collected_data_with_screenshots:
            screenshots_info = collected_data_with_screenshots['screenshots_info']
            games_with_screenshots = sum(1 for count in screenshots_info.values() if count > 0)
            collected_data_with_screenshots['games_with_screenshots'] = games_with_screenshots

        # Обновляем loaded_data с информацией о скриншотах
        loaded_data_with_screenshots = loaded_data_stats.get('loaded', {}).copy()
        loaded_data_with_screenshots['screenshots_loaded'] = screenshots_loaded

        # Формируем словарь со всей статистикой
        stats = {
            # Основная статистика
            'total_games_found': total_games,
            'created_count': created_count,
            'skipped_count': skipped_count,
            'error_count': 0,
            'total_time': total_time,

            # Статистика базы данных
            'total_games_in_db': total_games_in_db,
            'total_screenshots': total_screenshots,
            'total_genres': total_genres,
            'total_platforms': total_platforms,
            'total_keywords': total_keywords,
            'total_series': total_series,
            'total_companies': total_companies,
            'total_themes': total_themes,
            'total_perspectives': total_perspectives,
            'total_modes': total_modes,

            # Статистика загруженных данных
            'collected_data': collected_data_with_screenshots,
            'loaded_data': loaded_data_with_screenshots,

            # Время выполнения
            'step_times': all_step_times,

            # Статистика связей
            'relations': relations_results or {},

            # Возможное количество связей
            'relations_possible': relations_possible or {},

            # Дополнительная статистика
            'screenshots_loaded': screenshots_loaded,
            'screenshots_discovered': discovered_screenshots,

            # Процент успешной загрузки
            'screenshots_success_rate': (
                    screenshots_loaded / discovered_screenshots * 100) if discovered_screenshots > 0 else 0,
        }

        # Вычисляем ошибки
        if discovered_screenshots > 0 and screenshots_loaded < discovered_screenshots:
            stats['screenshots_error_count'] = discovered_screenshots - screenshots_loaded

        return stats

    def _print_complete_statistics(self, stats):
        """Выводит полную финальную статистику"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 ПОЛНАЯ СТАТИСТИКА ЗАГРУЗКИ')
        self.stdout.write('=' * 60)

        # Время выполнения
        self.stdout.write(f'⏱️  ОБЩЕЕ ВРЕМЯ: {stats["total_time"]:.2f}с')

        if stats['total_time'] > 0:
            speed = stats['total_games_found'] / stats['total_time']
            self.stdout.write(f'🚀 СКОРОСТЬ: {speed:.1f} игр/сек')

        self.stdout.write('\n🎮 ОСНОВНАЯ СТАТИСТИКА:')
        self.stdout.write(f'   • Найдено в IGDB: {stats["total_games_found"]}')
        self.stdout.write(f'   • Успешно загружено: {stats["created_count"]}')
        self.stdout.write(f'   • Пропущено (уже существуют): {stats["skipped_count"]}')
        self.stdout.write(f'   • Ошибок: {stats["error_count"]}')
        # Убрали пункт про скриншоты отсюда

        # Статистика собранных vs загруженных данных
        self.stdout.write('\n📈 СТАТИСТИКА ДАННЫХ (СОБРАНО / ЗАГРУЖЕНО):')

        data_types = [
            ('🎭 Жанры', 'genres'),
            ('🖥️  Платформы', 'platforms'),
            ('🔑 Ключевые слова', 'keywords'),
            ('📚 Серии', 'series'),
            ('🏢 Компании', 'companies'),
            ('🎨 Темы', 'themes'),
            ('👁️  Перспективы', 'perspectives'),
            ('🎮 Режимы', 'modes'),
            ('🖼️  Обложки', 'covers'),
            ('📸 Скриншоты', 'screenshots'),  # Оставляем здесь
        ]

        for display_name, key in data_types:
            if key == 'screenshots':
                # Используем правильные ключи для скриншотов
                discovered = stats.get('screenshots_discovered', 0)
                loaded = stats.get('screenshots_loaded', 0)

                if discovered > 0 or loaded > 0:
                    percentage = (loaded / discovered * 100) if discovered > 0 else 0

                    # Время загрузки скриншотов
                    time_val = stats['step_times'].get('screenshots', 0)
                    time_str = f" [{time_val:.2f}с]" if time_val > 0 else ""

                    self.stdout.write(f'   • {display_name}: {loaded}/{discovered} ({percentage:.1f}%){time_str}')

                    # Дополнительная информация о скриншотах
                    if 'collected_data' in stats and 'games_with_screenshots' in stats['collected_data']:
                        games_with = stats['collected_data']['games_with_screenshots']
                        games_total = stats['total_games_found']
                        if games_with > 0 and games_total > 0:
                            self.stdout.write(
                                f'     Игры со скриншотами: {games_with}/{games_total} ({games_with / games_total * 100:.1f}%)')
            else:
                collected = stats['collected_data'].get(key, 0)
                loaded = stats['loaded_data'].get(key, 0)

                if collected > 0 or loaded > 0:
                    percentage = (loaded / collected * 100) if collected > 0 else 0

                    # Получаем время для этого типа данных
                    time_key = {
                        'genres': 'genres',
                        'platforms': 'platforms',
                        'keywords': 'keywords',
                        'series': 'series',
                        'companies': 'companies',
                        'themes': 'themes',
                        'perspectives': 'perspectives',
                        'modes': 'modes',
                        'covers': 'covers',
                    }.get(key)

                    time_val = stats['step_times'].get(time_key, 0)
                    time_str = f" [{time_val:.2f}с]" if time_val > 0 else ""

                    self.stdout.write(f'   • {display_name}: {loaded}/{collected} ({percentage:.1f}%){time_str}')

        # Статистика связей
        if stats['relations']:
            self.stdout.write('\n🔗 СТАТИСТИКА СВЯЗЕЙ (СОЗДАНО / ВОЗМОЖНО):')
            relations_info = [
                ('🎭 Жанры', 'genre_relations', 'possible_genre_relations'),
                ('🖥️  Платформы', 'platform_relations', 'possible_platform_relations'),
                ('🔑 Ключевые слова', 'keyword_relations', 'possible_keyword_relations'),
                ('📚 Серии', 'series_relations', 'possible_series_relations'),
                ('🏢 Разработчики', 'developer_relations', 'possible_developer_relations'),
                ('📦 Издатели', 'publisher_relations', 'possible_publisher_relations'),
                ('🎨 Темы', 'theme_relations', 'possible_theme_relations'),
                ('👁️  Перспективы', 'perspective_relations', 'possible_perspective_relations'),
                ('🎮 Режимы', 'mode_relations', 'possible_mode_relations'),
            ]

            relations_time = stats['step_times'].get('relations', 0)

            for display_name, created_key, possible_key in relations_info:
                created = stats['relations'].get(created_key, 0)
                possible = stats['relations_possible'].get(possible_key, 0)

                if possible > 0:
                    percentage = (created / possible * 100) if possible > 0 else 0
                    self.stdout.write(f'   • {display_name}: {created}/{possible} ({percentage:.1f}%)')
                elif created > 0:
                    self.stdout.write(f'   • {display_name}: {created}')

            if relations_time > 0:
                total_created = sum(stats['relations'].values())
                total_possible = sum(stats['relations_possible'].values())

                if total_created > 0 and relations_time > 0:
                    speed = total_created / relations_time
                    self.stdout.write(f'   ⏱️  Время создания связей: {relations_time:.2f}с ({speed:.1f} связей/сек)')

                    if total_possible > 0:
                        total_percentage = (total_created / total_possible * 100)
                        self.stdout.write(
                            f'   📊 Всего связей: {total_created}/{total_possible} ({total_percentage:.1f}%)')
                else:
                    self.stdout.write(f'   ⏱️  Время создания связей: {relations_time:.2f}с')

        # Состояние базы данных
        self.stdout.write('\n🗄️  ТЕКУЩЕЕ СОСТОЯНИЕ БАЗЫ ДАННЫХ:')
        self.stdout.write(f'   🎮 Всего игр: {stats["total_games_in_db"]}')
        self.stdout.write(f'   🎭 Жанров: {stats["total_genres"]}')
        self.stdout.write(f'   🖥️  Платформ: {stats["total_platforms"]}')
        self.stdout.write(f'   🔑 Ключевых слов: {stats["total_keywords"]}')
        self.stdout.write(f'   📚 Серий: {stats["total_series"]}')
        self.stdout.write(f'   🏢 Компаний: {stats["total_companies"]}')
        self.stdout.write(f'   🎨 Тем: {stats["total_themes"]}')
        self.stdout.write(f'   👁️  Перспектив: {stats["total_perspectives"]}')
        self.stdout.write(f'   🎮 Режимов: {stats["total_modes"]}')
        self.stdout.write(f'   📸 Скриншотов: {stats["total_screenshots"]}')  # Оставляем здесь

        # Время ключевых этапов
        self.stdout.write('\n⏱️  ВРЕМЯ КЛЮЧЕВЫХ ЭТАПОВ:')
        key_steps = {
            '🎮 Создание игр': 'basic_games',
            '🖼️  Загрузка обложек': 'covers',
            '📸 Загрузка скриншотов': 'screenshots',
            '🔗 Создание связей': 'relations',
            '📋 Подготовка связей': 'prepare_relations',
        }

        total_key_time = 0
        for display_name, key in key_steps.items():
            if key in stats['step_times'] and stats['step_times'][key] > 0:
                time_val = stats['step_times'][key]
                total_key_time += time_val
                percentage = (time_val / stats['total_time'] * 100) if stats['total_time'] > 0 else 0
                self.stdout.write(f'   • {display_name}: {time_val:.2f}с ({percentage:.1f}%)')

        # Оставшееся время
        other_time = stats['total_time'] - total_key_time
        if other_time > 0:
            other_percentage = (other_time / stats['total_time'] * 100) if stats['total_time'] > 0 else 0
            self.stdout.write(f'   • 📊 Сбор данных: {other_time:.2f}с ({other_percentage:.1f}%)')

    def collect_screenshots_info(self, game_ids, debug=False):
        """Собирает ПРАВИЛЬНУЮ информацию о скриншотах для списка игр"""
        if not game_ids:
            if debug:
                self.stdout.write('   ⚠️  Нет ID игр для проверки скриншотов')
            return {
                'screenshots_info': {},
                'total_possible_screenshots': 0
            }

        screenshots_info = {}
        total_screenshots = 0

        if debug:
            self.stdout.write(f'   🔍 Сбор информации о скриншотах для {len(game_ids)} игр...')

        # Разбиваем на пачки по 50 игр
        batches = [game_ids[i:i + 50] for i in range(0, len(game_ids), 50)]
        total_batches = len(batches)

        if debug:
            self.stdout.write(f'      Разбито на {total_batches} пачек по 50 игр')

        for batch_num, batch_ids in enumerate(batches, 1):
            try:
                id_list = ','.join(map(str, batch_ids))
                # Запрашиваем ВСЕ скриншоты (без лимита per game, но с общим лимитом 500)
                query = f'fields game; where game = ({id_list}); limit 500;'

                screenshots_data = make_igdb_request('screenshots', query, debug=False)

                if debug:
                    self.stdout.write(f'      Пачка {batch_num}: получено {len(screenshots_data)} записей скриншотов')

                # Считаем скриншоты по играм
                for screenshot_data in screenshots_data:
                    game_id = screenshot_data.get('game')
                    if game_id:
                        # Увеличиваем счетчик скриншотов для этой игры
                        screenshots_info[game_id] = screenshots_info.get(game_id, 0) + 1
                        total_screenshots += 1

                if debug and (batch_num % 10 == 0 or batch_num == total_batches):
                    self.stdout.write(
                        f'      📊 Обработано {batch_num}/{total_batches} пачек, найдено {total_screenshots} скриншотов')

            except Exception as e:
                if debug:
                    self.stderr.write(f'      ❌ Ошибка при сборе информации о скриншотах для пачки {batch_num}: {e}')

        if debug:
            games_with_screenshots = len([v for v in screenshots_info.values() if v > 0])
            games_total = len(game_ids)

            self.stdout.write(f'   ✅ Сбор информации о скриншотах завершен:')
            self.stdout.write(f'      • Всего игр: {games_total}')
            self.stdout.write(f'      • Игр со скриншотами: {games_with_screenshots}')
            self.stdout.write(f'      • Обнаружено скриншотов: {total_screenshots}')

            # Детальная статистика
            if screenshots_info:
                avg_screenshots = total_screenshots / games_with_screenshots if games_with_screenshots > 0 else 0
                self.stdout.write(f'      • Среднее скриншотов на игру: {avg_screenshots:.1f}')

                # Распределение по количеству
                distribution = {}
                for count in screenshots_info.values():
                    distribution[count] = distribution.get(count, 0) + 1

                self.stdout.write(f'      • Распределение по количеству скриншотов:')
                for count in sorted(distribution.keys()):
                    self.stdout.write(f'        - {count} скриншотов: {distribution[count]} игр')

        return {
            'screenshots_info': screenshots_info,
            'total_possible_screenshots': total_screenshots,
            'games_with_screenshots': len([v for v in screenshots_info.values() if v > 0]),
            'games_total': len(game_ids)
        }
