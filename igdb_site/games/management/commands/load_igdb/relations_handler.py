# games/management/commands/load_igdb/relations_handler.py
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
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

    def _create_relations(self, all_game_relations, game_map, relation_field, through_model,
                          field_name, debug=False):
        """Универсальный метод для создания связей M2M"""
        relations_to_create = []
        existing_relations = set()
        count = 0

        all_relation_pairs = []
        for rel in all_game_relations:
            game = game_map.get(rel['game_id'])
            if not game:
                continue

            for relation_obj in rel.get(relation_field, []):
                if relation_obj:
                    all_relation_pairs.append((game.id, relation_obj.id))

        if not all_relation_pairs:
            return 0

        try:
            filter_conditions = []
            for game_id, relation_id in all_relation_pairs:
                filter_conditions.append(Q(game_id=game_id) & Q(**{f'{field_name}_id': relation_id}))

            if filter_conditions:
                combined_condition = filter_conditions[0]
                for condition in filter_conditions[1:]:
                    combined_condition |= condition

                existing_pairs = through_model.objects.filter(combined_condition).values_list(
                    'game_id', f'{field_name}_id'
                )
                existing_relations = set(existing_pairs)
        except Exception as e:
            if debug:
                self.stderr.write(f'   ⚠️  Ошибка при проверке существующих связей: {e}')

        for rel in all_game_relations:
            game = game_map.get(rel['game_id'])
            if not game:
                continue

            for relation_obj in rel.get(relation_field, []):
                if not relation_obj:
                    continue

                if (game.id, relation_obj.id) not in existing_relations:
                    relations_to_create.append(through_model(
                        game_id=game.id,
                        **{f'{field_name}_id': relation_obj.id}
                    ))
                    count += 1

        if relations_to_create:
            try:
                through_model.objects.bulk_create(relations_to_create, batch_size=100, ignore_conflicts=True)
                if debug:
                    self.stdout.write(f'   ✅ Создано связей с {relation_field}: {count}')
            except Exception as e:
                if debug:
                    self.stderr.write(f'   ❌ Ошибка создания связей {relation_field}: {e}')
        else:
            if debug and existing_relations:
                self.stdout.write(f'   ℹ️  Все связи {relation_field} уже существуют: {len(existing_relations)}')

        return count

    def create_relations_batch(self, all_game_relations, debug=False):
        """Создает основные M2M связи для игр пачками"""
        if not all_game_relations:
            if debug:
                self.stdout.write('   ⚠️  Нет связей для создания')
            return 0, 0, 0, 0

        game_ids = [rel['game_id'] for rel in all_game_relations]

        if debug:
            self.stdout.write(f'   🔍 Поиск {len(game_ids)} игр в базе...')

        games = Game.objects.filter(igdb_id__in=game_ids)
        game_map = {game.igdb_id: game for game in games}

        if debug:
            self.stdout.write(f'   ✅ Найдено {len(game_map)} игр в базе')

        genre_relations = self._create_relations(
            all_game_relations, game_map, 'genres', Game.genres.through, 'genre', debug
        )

        platform_relations = self._create_relations(
            all_game_relations, game_map, 'platforms', Game.platforms.through, 'platform', debug
        )

        keyword_relations = self._create_relations(
            all_game_relations, game_map, 'keywords', Game.keywords.through, 'keyword', debug
        )

        engine_relations = self._create_relations(
            all_game_relations, game_map, 'engines', Game.engines.through, 'gameengine', debug
        )

        if debug:
            self.stdout.write(f'   ✅ Создано связей:')
            self.stdout.write(f'      • С жанрами: {genre_relations}')
            self.stdout.write(f'      • С платформами: {platform_relations}')
            self.stdout.write(f'      • С ключевыми словами: {keyword_relations}')
            self.stdout.write(f'      • С движками: {engine_relations}')

        return genre_relations, platform_relations, keyword_relations, engine_relations

    def create_all_additional_relations(self, all_game_relations, debug=False):
        """Создает все дополнительные M2M связи для игр"""
        if not all_game_relations:
            if debug:
                self.stdout.write('   ⚠️  Нет дополнительных связей для создания')
            return {}

        game_ids = [rel['game_id'] for rel in all_game_relations]

        if debug:
            self.stdout.write(f'   🔍 Поиск {len(game_ids)} игр в базе для доп. связей...')

        games = Game.objects.filter(igdb_id__in=game_ids)
        game_map = {game.igdb_id: game for game in games}

        if debug:
            self.stdout.write(f'   ✅ Найдено {len(game_map)} игр в базе')

        results = {}

        results['series_relations'] = self._create_relations(
            all_game_relations, game_map, 'series', Game.series.through,
            'series', debug=debug
        )

        results['developer_relations'] = self._create_relations(
            all_game_relations, game_map, 'developers', Game.developers.through,
            'company', debug=debug
        )

        results['publisher_relations'] = self._create_relations(
            all_game_relations, game_map, 'publishers', Game.publishers.through,
            'company', debug=debug
        )

        results['theme_relations'] = self._create_relations(
            all_game_relations, game_map, 'themes', Game.themes.through,
            'theme', debug=debug
        )

        results['perspective_relations'] = self._create_relations(
            all_game_relations, game_map, 'perspectives', Game.player_perspectives.through,
            'playerperspective', debug=debug
        )

        results['mode_relations'] = self._create_relations(
            all_game_relations, game_map, 'modes', Game.game_modes.through,
            'gamemode', debug=debug
        )

        if debug:
            self.stdout.write(f'   ✅ Создано доп. связей:')
            for key, value in results.items():
                self.stdout.write(f'      • {key}: {value}')

        return results

    def prepare_game_relations(self, game_basic_map, game_data_map, additional_data_map, data_maps, debug=False):
        """Подготавливает связи для игр с учетом M2M для series"""
        if debug:
            self.stdout.write('\n📋 ПОДГОТОВКА СВЯЗЕЙ ДЛЯ ИГР...')

        import time
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
                'engines': [],
                'series': [],
                'developers': [],
                'publishers': [],
                'themes': [],
                'perspectives': [],
                'modes': [],
            }

            for gid in game_data.get('genres', []):
                if gid in data_maps.get('genre_map', {}):
                    relations['genres'].append(data_maps['genre_map'][gid])

            for pid in game_data.get('platforms', []):
                if pid in data_maps.get('platform_map', {}):
                    relations['platforms'].append(data_maps['platform_map'][pid])

            for kid in game_data.get('keywords', []):
                if kid in data_maps.get('keyword_map', {}):
                    relations['keywords'].append(data_maps['keyword_map'][kid])

            for engine_data in game_data.get('game_engines', []):
                if isinstance(engine_data, dict):
                    eid = engine_data.get('id')
                else:
                    eid = engine_data

                if eid and eid in data_maps.get('engine_map', {}):
                    relations['engines'].append(data_maps['engine_map'][eid])
                    if debug:
                        self.stdout.write(f'      • Игра {game_id}: добавлен движок ID {eid}')

            series_ids_in_data = additional_data.get('collections', [])
            for sid in series_ids_in_data:
                if sid in data_maps.get('series_map', {}):
                    relations['series'].append(data_maps['series_map'][sid])

            for cid in developer_ids:
                if cid in data_maps.get('company_map', {}):
                    relations['developers'].append(data_maps['company_map'][cid])

            for cid in publisher_ids:
                if cid in data_maps.get('company_map', {}):
                    relations['publishers'].append(data_maps['company_map'][cid])

            for tid in additional_data.get('themes', []):
                if tid in data_maps.get('theme_map', {}):
                    relations['themes'].append(data_maps['theme_map'][tid])

            for pid in additional_data.get('player_perspectives', []):
                if pid in data_maps.get('perspective_map', {}):
                    relations['perspectives'].append(data_maps['perspective_map'][pid])

            for mid in additional_data.get('game_modes', []):
                if mid in data_maps.get('mode_map', {}):
                    relations['modes'].append(data_maps['mode_map'][mid])

            has_relations = any([
                relations['genres'],
                relations['platforms'],
                relations['keywords'],
                relations['engines'],
                relations['series'],
                relations['developers'],
                relations['publishers'],
                relations['themes'],
                relations['perspectives'],
                relations['modes'],
            ])

            if has_relations:
                all_game_relations.append(relations)

        step_time = time.time() - start_step

        if debug:
            self.stdout.write(f'   📊 Подготовлено связей для {len(all_game_relations)} игр')
            self.stdout.write(f'   ⚠️  Игр без основных данных: {games_without_data}')
            self.stdout.write(f'   ⚠️  Игр без дополнительных данных: {games_without_additional}')

            if all_game_relations:
                stats = {
                    'genres': 0,
                    'platforms': 0,
                    'keywords': 0,
                    'engines': 0,
                    'series': 0,
                    'developers': 0,
                    'publishers': 0,
                    'themes': 0,
                    'perspectives': 0,
                    'modes': 0,
                }

                for rel in all_game_relations:
                    for key in stats.keys():
                        stats[key] += len(rel[key])

                self.stdout.write(f'   📈 Статистика связей:')
                for key, count in stats.items():
                    if count > 0:
                        self.stdout.write(f'      • {key}: {count}')

        return all_game_relations, step_time

    def create_all_relations(self, all_game_relations, debug=False):
        """Создает все связи для игр и возвращает статистику возможных связей"""
        if debug:
            self.stdout.write('\n🔗 СОЗДАНИЕ СВЯЗЕЙ...')

        import time
        start_step = time.time()

        possible_stats = {
            'possible_genre_relations': 0,
            'possible_platform_relations': 0,
            'possible_keyword_relations': 0,
            'possible_engine_relations': 0,
            'possible_series_relations': 0,
            'possible_developer_relations': 0,
            'possible_publisher_relations': 0,
            'possible_theme_relations': 0,
            'possible_perspective_relations': 0,
            'possible_mode_relations': 0,
        }

        for rel in all_game_relations:
            for key in possible_stats.keys():
                field_name = key.replace('possible_', '').replace('_relations', '')
                possible_stats[key] += len(rel.get(field_name, []))

        genre_relations, platform_relations, keyword_relations, engine_relations = self.create_relations_batch(
            all_game_relations, debug
        )

        additional_results = self.create_all_additional_relations(all_game_relations, debug)

        step_time = time.time() - start_step

        results = {
            'genre_relations': genre_relations,
            'platform_relations': platform_relations,
            'keyword_relations': keyword_relations,
            'engine_relations': engine_relations,
            **additional_results
        }

        return results, possible_stats, step_time