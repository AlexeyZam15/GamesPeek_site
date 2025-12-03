from games.models import (
    Game, Genre, Keyword, Platform, Series,
    Company, Theme, PlayerPerspective, GameMode
)
from django.db.models import Q, Count


class RelationsHandler:
    """Класс для обработки связей между моделями"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr

    def _create_relations(self, all_game_relations, game_map, relation_type, through_model,
                          relation_field, field_name, batch_size=100, debug=False):
        """Универсальный метод для создания связей M2M"""
        relations_to_create = []
        count = 0

        # Если game_map пустой, создаем его
        if not game_map:
            # Получаем ID всех игр из relations
            game_ids = [rel['game_id'] for rel in all_game_relations]
            games = Game.objects.filter(igdb_id__in=game_ids)
            game_map = {game.igdb_id: game for game in games}

        for rel in all_game_relations:
            game = game_map.get(rel['game_id'])
            if not game:
                continue

            for relation_obj in rel.get(relation_field, []):
                if relation_obj:
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
        """Создает основные M2M связи для игр пачками"""
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

    def create_all_additional_relations(self, all_game_relations, series_map, company_map,
                                        theme_map, perspective_map, mode_map, debug=False):
        """Создает все дополнительные M2M связи для игр"""
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

            # Серии (теперь M2M - добавляем все)
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
            self.stdout.write(f'      • Серии (M2M): {total_series}')
            self.stdout.write(f'      • Разработчики: {total_developers}')
            self.stdout.write(f'      • Издатели: {total_publishers}')
            self.stdout.write(f'      • Темы: {total_themes}')
            self.stdout.write(f'      • Перспективы: {total_perspectives}')
            self.stdout.write(f'      • Режимы: {total_modes}')

        # Создаем M2M связи с сериями
        series_count = self._create_relations(
            game_relations_prepared, game_map, 'series', Game.series.through,
            'series', 'series', debug=debug
        )

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
            self.stdout.write(f'      • С сериями (M2M): {series_count}')
            self.stdout.write(f'      • С разработчиками: {developer_count}')
            self.stdout.write(f'      • С издателями: {publisher_count}')
            self.stdout.write(f'      • С темами: {theme_count}')
            self.stdout.write(f'      • С перспективами: {perspective_count}')
            self.stdout.write(f'      • С режимами: {mode_count}')

        return series_count, developer_count, publisher_count, theme_count, perspective_count, mode_count

    def prepare_game_relations(self, game_basic_map, game_data_map, additional_data_map, data_maps, debug=False):
        """Подготавливает связи для игр с учетом M2M для series"""
        if debug:
            self.stdout.write('\n📋 ПОДГОТОВКА СВЯЗЕЙ ДЛЯ ИГР...')

        import time
        start_step = time.time()
        all_game_relations = []
        games_without_data = 0
        games_without_additional = 0

        # ★★★ СТАТИСТИКА ПО СЕРИЯМ ★★★
        total_series_found = 0
        total_series_mapped = 0

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
                'series': [],  # Теперь M2M - список
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

            # Серии - теперь M2M, добавляем все серии
            series_ids_in_data = additional_data.get('collections', [])
            total_series_found += len(series_ids_in_data)

            for sid in series_ids_in_data:
                if sid in data_maps['series_map']:
                    relations['series'].append(data_maps['series_map'][sid])
                    total_series_mapped += 1
                elif debug:
                    self.stdout.write(f'   ⚠️  Серия ID {sid} не найдена в series_map для игры {game_id}')

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

            # ★★★ СТАТИСТИКА ПО СЕРИЯМ ★★★
            self.stdout.write(f'\n   🔍 СТАТИСТИКА СЕРИЙ В ПОДГОТОВКЕ:')
            self.stdout.write(f'      • Найдено ID серий в данных: {total_series_found}')
            self.stdout.write(f'      • Сопоставлено с series_map: {total_series_mapped}')

            if total_series_found > 0:
                mapping_rate = (total_series_mapped / total_series_found) * 100
                self.stdout.write(f'      • Процент сопоставления: {mapping_rate:.1f}%')

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

        import time
        start_step = time.time()

        # Собираем статистику возможных связей
        possible_stats = {
            'possible_genre_relations': 0,
            'possible_platform_relations': 0,
            'possible_keyword_relations': 0,
            'possible_series_relations': 0,  # Теперь M2M - считаем все связи
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
            possible_stats['possible_series_relations'] += len(rel.get('series', []))  # Все M2M связи
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

        # Получаем ID игр для создания game_map
        game_ids = [rel['game_id'] for rel in all_game_relations]
        games = Game.objects.filter(igdb_id__in=game_ids)
        game_map = {game.igdb_id: game for game in games}

        # Дополнительные M2M связи
        series_relations = self._create_relations(
            all_game_relations, game_map, 'series', Game.series.through,
            'series', 'series', debug=debug
        )

        developer_relations = self._create_relations(
            all_game_relations, game_map, 'developers', Game.developers.through,
            'developers', 'company', debug=debug
        )

        publisher_relations = self._create_relations(
            all_game_relations, game_map, 'publishers', Game.publishers.through,
            'publishers', 'company', debug=debug
        )

        theme_relations = self._create_relations(
            all_game_relations, game_map, 'themes', Game.themes.through,
            'themes', 'theme', debug=debug
        )

        perspective_relations = self._create_relations(
            all_game_relations, game_map, 'perspectives', Game.player_perspectives.through,
            'perspectives', 'playerperspective', debug=debug
        )

        mode_relations = self._create_relations(
            all_game_relations, game_map, 'modes', Game.game_modes.through,
            'modes', 'gamemode', debug=debug
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