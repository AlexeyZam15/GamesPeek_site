import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from collections import Counter
from games.igdb_api import make_igdb_request
from games.models import Game


class DataCollector:
    """Класс для сбора и обработки данных"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr

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

        # 2️⃣ Сбор информации о скриншотах
        if debug:
            self.stdout.write('\n2️⃣  📸 СБОР ИНФОРМАЦИИ О СКРИНШОТАХ...')

        start_screenshots_info = time.time()
        game_ids_for_screenshots = collected_data['all_game_ids']

        if debug:
            self.stdout.write(f'   🔍 Проверка скриншотов для {len(game_ids_for_screenshots)} игр...')

        screenshots_info_result = self.collect_screenshots_info(game_ids_for_screenshots, debug)
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

        # 3️⃣ Загрузка дополнительных данных
        if debug:
            self.stdout.write('\n3️⃣  📚 ЗАГРУЗКА ДОПОЛНИТЕЛЬНЫХ ДАННЫХ...')

        start_additional = time.time()
        from .data_loader import DataLoader
        loader = DataLoader(self.stdout, self.stderr)

        # Используем обновленный метод с 3 параметрами
        additional_data_map, additional_stats = loader.load_and_process_additional_data(
            collected_data['all_game_ids'],
            collected_data['game_data_map'],
            collected_data['screenshots_info'],
            debug
        )
        collected_data['additional_data_map'] = additional_data_map

        collected_data['all_series_ids'] = additional_stats.get('all_series_ids', [])
        collected_data['all_company_ids'] = additional_stats.get('all_company_ids', [])
        collected_data['all_theme_ids'] = additional_stats.get('all_theme_ids', [])
        collected_data['all_perspective_ids'] = additional_stats.get('all_perspective_ids', [])
        collected_data['all_mode_ids'] = additional_stats.get('all_mode_ids', [])

        additional_time = time.time() - start_additional
        collection_stats['additional_time'] = additional_time

        if debug:
            self.stdout.write(f'   ✅ Дополнительные данные загружены за {additional_time:.2f}с')

        # 4️⃣ Общая статистика
        if debug:
            self.stdout.write('\n📊 ОБЩАЯ СТАТИСТИКА СОБРАННЫХ ДАННЫХ:')
            self.stdout.write('   ────────────────────────────────')

            self.stdout.write(f'   🎮 Игр: {len(collected_data["all_game_ids"])}')
            self.stdout.write(f'   🖼️  Обложек: {len(collected_data["all_cover_ids"])}')
            self.stdout.write(f'   🎭 Жанров: {len(collected_data["all_genre_ids"])}')
            self.stdout.write(f'   🖥️  Платформ: {len(collected_data["all_platform_ids"])}')
            self.stdout.write(f'   🔑 Ключевых слов: {len(collected_data["all_keyword_ids"])}')
            self.stdout.write(f'   📚 Серий: {len(collected_data.get("all_series_ids", []))}')
            self.stdout.write(f'   🏢 Компаний: {len(collected_data.get("all_company_ids", []))}')
            self.stdout.write(f'   🎨 Тем: {len(collected_data.get("all_theme_ids", []))}')
            self.stdout.write(f'   👁️  Перспектив: {len(collected_data.get("all_perspective_ids", []))}')
            self.stdout.write(f'   🎮 Режимов: {len(collected_data.get("all_mode_ids", []))}')

            discovered = collected_data.get('total_possible_screenshots', 0)
            if discovered > 0:
                games_with = len([v for v in collected_data.get('screenshots_info', {}).values() if v > 0])
                self.stdout.write(f'   📸 Скриншотов: {discovered} (в {games_with} играх)')
            else:
                self.stdout.write(f'   📸 Скриншотов: {discovered}')

            total_collection_time = collect_time + screenshots_info_time + additional_time
            self.stdout.write(f'   ⏱️  Общее время сбора: {total_collection_time:.2f}с')

        collected_data['screenshots_discovered'] = collected_data.get('total_possible_screenshots', 0)

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

        return collected_data, stats

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

        return {
            'screenshots_info': screenshots_info,
            'total_possible_screenshots': total_screenshots,
            'games_with_screenshots': len([v for v in screenshots_info.values() if v > 0]),
            'games_total': len(game_ids)
        }

    def process_all_data_sequentially(self, all_games_data, debug=False):
        """Обрабатывает все данные последовательно по типам, но с параллельными пачками внутри каждого типа"""
        from .data_loader import DataLoader
        from .relations_handler import RelationsHandler
        from .statistics import Statistics

        loader = DataLoader(self.stdout, self.stderr)
        relations_handler = RelationsHandler(self.stdout, self.stderr)
        stats_handler = Statistics(self.stdout, self.stderr)

        total_games = len(all_games_data)

        if debug:
            self.stdout.write(f'📊 Всего игр: {total_games}')
            self.stdout.write('🚀 Используется оптимизированная загрузка с учетом веса данных')

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
        created_count, game_basic_map = loader.create_basic_games(games_data_list, debug)
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
            stats = stats_handler._collect_final_statistics(
                total_games, 0, skipped_count, 0, total_time,
                loaded_data_stats, all_step_times, debug
            )

            if debug:
                stats_handler._print_complete_statistics(stats)
            else:
                # Выводим минимальную статистику даже без debug
                self.stdout.write('\n' + '=' * 60)
                self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
                self.stdout.write(f'⏱️  Время: {total_time:.2f}с')
                if total_time > 0:
                    speed = total_games / total_time
                    self.stdout.write(f'🚀 СКОРОСТЬ: {speed:.1f} игр/сек')
                self.stdout.write(f'🎮 Найдено: {total_games}')
                self.stdout.write(f'✅ Загружено: 0')
                self.stdout.write(f'⏭️  Пропущено: {skipped_count}')

            return stats

        # 3️⃣ Загрузка всех типов данных последовательно
        data_maps, data_step_times = loader.load_all_data_types_sequentially(collected_data, debug)
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
        updated_covers = loader.update_games_with_covers(
            game_basic_map, data_maps['cover_map'], collected_data['game_data_map'], debug
        )
        all_step_times['update_covers'] = time.time() - start_step

        if debug:
            self.stdout.write(f'   ✅ Обновлено обложек: {updated_covers}')

        # 5️⃣ Загрузка скриншотов
        if debug:
            self.stdout.write('\n📸 ПАРАЛЛЕЛЬНАЯ ЗАГРУЗКА СКРИНШОТОВ...')
        start_step = time.time()

        screenshots_info = collected_data.get('screenshots_info', {})
        game_data_map = collected_data.get('game_data_map', {})
        game_ids = list(game_basic_map.keys())

        screenshots_loaded = loader.load_screenshots_parallel(
            game_ids, game_data_map, screenshots_info, debug=debug
        )

        all_step_times['screenshots'] = time.time() - start_step

        if debug:
            self.stdout.write(f'   ✅ Загружено скриншотов: {screenshots_loaded}')
            self.stdout.write(f'   ⏱️  Время: {all_step_times["screenshots"]:.2f}с')

        # 6️⃣ Подготовка связей
        all_game_relations, prepare_time = relations_handler.prepare_game_relations(
            game_basic_map, collected_data['game_data_map'],
            collected_data['additional_data_map'], data_maps, debug
        )
        all_step_times['prepare_relations'] = prepare_time

        # 7️⃣ Создание всех связей
        relations_results, possible_stats, relations_time = relations_handler.create_all_relations(
            all_game_relations, data_maps, debug
        )
        all_step_times['relations'] = relations_time

        total_time = time.time() - start_total_time
        skipped_count = total_games - created_count

        # 8️⃣ Собираем полную финальную статистику
        stats = stats_handler._collect_final_statistics(
            total_games, created_count, skipped_count, screenshots_loaded,
            total_time, loaded_data_stats, all_step_times,
            relations_results, possible_stats, debug
        )

        # 9️⃣ Выводим полную статистику
        if debug:
            stats_handler._print_complete_statistics(stats)
        else:
            # Без debug - только итоговая статистика
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
            self.stdout.write(f'⏱️  Время: {total_time:.2f}с')
            if total_time > 0:
                speed = total_games / total_time
                self.stdout.write(f'🚀 СКОРОСТЬ: {speed:.1f} игр/сек')
            self.stdout.write(f'🎮 Найдено: {total_games}')
            self.stdout.write(f'✅ Загружено: {created_count}')
            self.stdout.write(f'⏭️  Пропущено: {skipped_count}')

        return stats

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0, skip_existing=False):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        self.stdout.write('🔍 Загрузка популярных игр...')

        if limit > 0:
            self.stdout.write(f'   🔒 Установлен лимит: {limit} игр')
        if offset > 0:
            self.stdout.write(f'   ⏭️  Пропуск первых: {offset} игр')
        if min_rating_count > 0:
            self.stdout.write(f'   ⭐ Минимальное количество оценок: {min_rating_count}')
        if skip_existing:
            self.stdout.write(f'   ⏭️  Режим skip-existing: пропуск игр, которые уже есть в базе')

        # Базовое условие - исключаем игры без названия и с нулевым rating_count
        where_conditions = ['name != null']

        if min_rating_count > 0:
            where_conditions.append(f'rating_count >= {min_rating_count}')
        else:
            # Если не указан min_rating_count, все равно фильтруем игры с хотя бы одной оценкой
            where_conditions.append('rating_count > 0')

        where_clause = ' & '.join(where_conditions)

        if debug:
            self.stdout.write('   🎯 Построение запроса...')
            self.stdout.write(f'   📋 Условие: {where_clause}')
            self.stdout.write('   📊 Сортировка: по количеству оценок (rating_count)')

        return self.load_games_by_query(where_clause, debug, limit, offset, skip_existing)

    def load_games_by_query(self, where_clause, debug=False, limit=0, offset=0, skip_existing=True):
        """Загрузка игр по запросу с пагинацией и offset"""
        all_games = []
        current_offset = offset
        max_limit = 500
        batch_number = 1

        if debug:
            self.stdout.write(f'   📥 Начало загрузки игр...')
            if limit > 0:
                self.stdout.write(f'   🎯 Цель: загрузить до {limit} игр')
            if offset > 0:
                self.stdout.write(f'   ⏭️  Начинаем с позиции: {offset}')
            if skip_existing:
                self.stdout.write(f'   ⏭️  Пропуск существующих игр')

        # Получаем ID игр, которые уже есть в базе (skip-existing всегда включен по умолчанию)
        existing_game_ids = set()
        if skip_existing:
            existing_game_ids = set(Game.objects.values_list('igdb_id', flat=True))
            if debug:
                self.stdout.write(f'   📊 Уже есть в базе: {len(existing_game_ids)} игр')

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
                self.stdout.write(
                    f'   📦 Пачка игр {batch_number}: позиция {current_offset}-{current_offset + current_limit}...')

            query = f'''
                fields name,summary,storyline,genres,keywords,rating,rating_count,first_release_date,platforms,cover;
                where {where_clause};
                sort rating_count desc;
                limit {current_limit};
                offset {current_offset};
            '''.strip()

            batch_games = make_igdb_request('games', query, debug=False)

            if not batch_games:
                if debug:
                    self.stdout.write(f'   💤 Пачка игр {batch_number}: больше игр нет')
                break

            if debug:
                self.stdout.write(f'   ⏱️  Получено {len(batch_games)} игр за пачку')

            # Фильтруем игры, которые уже есть в базе (skip-existing всегда включен по умолчанию)
            if skip_existing:
                filtered_batch_games = []
                skipped_count = 0
                for game in batch_games:
                    game_id = game.get('id')
                    if game_id in existing_game_ids:
                        skipped_count += 1
                        # УБРАН ДЕТАЛЬНЫЙ ВЫВОД ПРОПУЩЕННЫХ ИГР
                    else:
                        filtered_batch_games.append(game)

                if debug and skipped_count > 0:
                    # Оставляем только общее количество
                    self.stdout.write(f'      ⏭️  Пропущено {skipped_count} существующих игр из пачки')

                batch_games = filtered_batch_games

            batch_loaded = len(batch_games)
            all_games.extend(batch_games)

            if debug:
                self.stdout.write(f'   ✅ Пачка игр {batch_number}: загружено {batch_loaded} игр')

            current_offset += current_limit
            batch_number += 1

            # Если IGDB вернул пустой ответ или достигнут лимит
            if batch_loaded == 0 or (limit > 0 and len(all_games) >= limit):
                if debug:
                    if limit > 0 and len(all_games) >= limit:
                        self.stdout.write(f'   🏁 Достигнут лимит {limit} игр. Всего пачек: {batch_number - 1}')
                    else:
                        self.stdout.write(f'   🏁 Завершено. Всего пачек игр: {batch_number - 1}')
                break

        if debug:
            if limit > 0:
                self.stdout.write(
                    f'   📊 Загружено игр: {len(all_games)} из {limit} (с offset {offset}) за {batch_number - 1} пачек')
            else:
                self.stdout.write(
                    f'   📊 Всего загружено игр: {len(all_games)} (с offset {offset}) за {batch_number - 1} пачек')

        # Если установлен лимит, обрезаем список до нужного количества
        if limit > 0:
            all_games = all_games[:limit]
            if debug:
                self.stdout.write(f'   ✂️  Обрезано до лимита {limit}: {len(all_games)} игр')

        return all_games
