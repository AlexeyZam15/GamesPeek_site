# games/management/commands/load_igdb/statistics.py
import time
from games.models import (
    Game, Genre, Keyword, Platform, Series,
    Company, Theme, PlayerPerspective, GameMode, Screenshot
)
from django.db.models import Count, Q


class Statistics:
    """Класс для статистики и диагностики"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr

    def _collect_objects_statistics(self, game_basic_map, data_maps, loaded_data_stats, debug=False):
        """Собирает статистику по созданным объектам"""
        objects_stats = {
            'games': {
                'created': len(game_basic_map),
                'total_in_db': Game.objects.count(),
                'skipped': 0  # Будет заполнено позже
            },
            'genres': {
                'created': 0,
                'total_in_db': Genre.objects.count(),
                'possible': 0
            },
            'platforms': {
                'created': 0,
                'total_in_db': Platform.objects.count(),
                'possible': 0
            },
            'keywords': {
                'created': 0,
                'total_in_db': Keyword.objects.count(),
                'possible': 0
            },
            'series': {
                'created': 0,
                'total_in_db': Series.objects.count(),
                'possible': 0
            },
            'companies': {
                'created': 0,
                'total_in_db': Company.objects.count(),
                'possible': 0
            },
            'themes': {
                'created': 0,
                'total_in_db': Theme.objects.count(),
                'possible': 0
            },
            'perspectives': {
                'created': 0,
                'total_in_db': PlayerPerspective.objects.count(),
                'possible': 0
            },
            'modes': {
                'created': 0,
                'total_in_db': GameMode.objects.count(),
                'possible': 0
            },
            'screenshots': {
                'created': 0,
                'total_in_db': Screenshot.objects.count(),
                'possible': 0
            }
        }

        # Для каждого типа объектов считаем сколько было создано новых
        object_types = [
            ('genre_map', 'genres', Genre),
            ('platform_map', 'platforms', Platform),
            ('keyword_map', 'keywords', Keyword),
            ('series_map', 'series', Series),
            ('company_map', 'companies', Company),
            ('theme_map', 'themes', Theme),
            ('perspective_map', 'perspectives', PlayerPerspective),
            ('mode_map', 'modes', GameMode),
        ]

        for map_key, stat_key, model_class in object_types:
            if map_key in data_maps:
                total_ids = loaded_data_stats['collected'].get(f'all_{stat_key}_ids', [])
                loaded_count = len(data_maps[map_key])

                objects_stats[stat_key]['created'] = loaded_count
                objects_stats[stat_key]['possible'] = len(total_ids) if total_ids else 0

        # Статистика обложек
        if 'cover_map' in data_maps:
            objects_stats['covers'] = {
                'loaded': len(data_maps['cover_map']),
                'total_in_db': 0,
                'possible': len(loaded_data_stats['collected'].get('all_cover_ids', []))
            }

        if debug:
            self.stdout.write(f'\n📊 ПРЕДВАРИТЕЛЬНАЯ СТАТИСТИКА ОБЪЕКТОВ:')
            for key, stats in objects_stats.items():
                if 'created' in stats:
                    self.stdout.write(f'   • {key}: создано {stats["created"]}/{stats.get("possible", "?")}')

        return objects_stats

    def _collect_relations_statistics(self, all_game_relations, relations_results, debug=False):
        """Собирает статистику по созданным связям"""
        if not all_game_relations:
            return {}

        # Подсчитываем возможные связи
        possible_stats = {
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

        # Считаем возможные связи
        for rel in all_game_relations:
            for key in possible_stats.keys():
                possible_stats[key] += len(rel.get(key, []))

        # Считаем созданные связи
        created_stats = {
            'genres': relations_results.get('genre_relations', 0),
            'platforms': relations_results.get('platform_relations', 0),
            'keywords': relations_results.get('keyword_relations', 0),
            'series': relations_results.get('series_relations', 0),
            'developers': relations_results.get('developer_relations', 0),
            'publishers': relations_results.get('publisher_relations', 0),
            'themes': relations_results.get('theme_relations', 0),
            'perspectives': relations_results.get('perspective_relations', 0),
            'modes': relations_results.get('mode_relations', 0),
        }

        # Формируем полную статистику
        relations_stats = {}
        for key in possible_stats.keys():
            relations_stats[key] = {
                'created': created_stats[key],
                'possible': possible_stats[key],
                'percentage': (created_stats[key] / possible_stats[key] * 100) if possible_stats[key] > 0 else 0
            }

        if debug:
            self.stdout.write(f'\n📊 СТАТИСТИКА СВЯЗЕЙ:')
            for key, stats in relations_stats.items():
                if stats['possible'] > 0:
                    self.stdout.write(
                        f'   • {key}: {stats["created"]}/{stats["possible"]} ({stats["percentage"]:.1f}%)')

        return relations_stats

    def _print_detailed_statistics(self, objects_stats, relations_stats, total_time, debug=False):
        """Выводит детальную статистику по всем созданным объектам и связям"""
        self.stdout.write('\n' + '=' * 80)
        self.stdout.write('📊 ПОДРОБНАЯ СТАТИСТИКА СОЗДАННЫХ ОБЪЕКТОВ И СВЯЗЕЙ')
        self.stdout.write('=' * 80)

        # Разделы статистики
        sections = [
            ('🎮 ОСНОВНЫЕ ОБЪЕКТЫ', [
                ('games', 'Игры'),
            ]),
            ('🎭 ДАННЫЕ ИГР', [
                ('genres', 'Жанры'),
                ('platforms', 'Платформы'),
                ('keywords', 'Ключевые слова'),
                ('series', 'Серии'),
                ('companies', 'Компании'),
                ('themes', 'Темы'),
                ('perspectives', 'Перспективы'),
                ('modes', 'Режимы игры'),
            ]),
            ('📸 МЕДИА', [
                ('screenshots', 'Скриншоты'),
            ])
        ]

        # Выводим статистику по объектам
        for section_title, items in sections:
            self.stdout.write(f'\n{section_title}:')
            self.stdout.write('─' * 40)

            for key, display_name in items:
                if key in objects_stats:
                    stats = objects_stats[key]
                    if 'created' in stats:
                        # Объекты
                        created = stats['created']
                        total_in_db = stats.get('total_in_db', 0)
                        existing = stats.get('existing', 0)
                        possible = stats.get('possible', 0)

                        if key == 'games':
                            self.stdout.write(f'   {display_name}:')
                            self.stdout.write(f'      • Создано в этой загрузке: {created}')
                            self.stdout.write(f'      • Всего в базе данных: {total_in_db}')
                            if 'skipped' in stats and stats['skipped'] > 0:
                                self.stdout.write(f'      • Пропущено (уже существовали): {stats["skipped"]}')
                        elif possible > 0:
                            self.stdout.write(f'   {display_name}: {created}/{possible} (создано/найдено)')
                            self.stdout.write(f'      • Всего в базе: {total_in_db}')
                            if existing > 0:
                                self.stdout.write(f'      • Уже существовали: {existing}')
                        else:
                            self.stdout.write(f'   {display_name}: {created} создано')
                            if total_in_db > 0:
                                self.stdout.write(f'      • Всего в базе: {total_in_db}')

        # Выводим статистику по связям
        if relations_stats:
            self.stdout.write('\n🔗 СВЯЗИ МЕЖДУ ОБЪЕКТАМИ:')
            self.stdout.write('─' * 40)

            relation_display_names = {
                'genres': 'Связи с жанрами',
                'platforms': 'Связи с платформами',
                'keywords': 'Связи с ключевыми словами',
                'series': 'Связи с сериями',
                'developers': 'Связи с разработчиками',
                'publishers': 'Связи с издателями',
                'themes': 'Связи с темами',
                'perspectives': 'Связи с перспективами',
                'modes': 'Связи с режимами',
            }

            total_created = 0
            total_possible = 0

            for key, display_name in relation_display_names.items():
                if key in relations_stats:
                    stats = relations_stats[key]
                    if stats['possible'] > 0:
                        total_created += stats['created']
                        total_possible += stats['possible']

                        self.stdout.write(f'   {display_name}:')
                        self.stdout.write(
                            f'      • Создано: {stats["created"]}/{stats["possible"]} ({stats["percentage"]:.1f}%)')

            if total_possible > 0:
                total_percentage = (total_created / total_possible * 100) if total_possible > 0 else 0
                self.stdout.write(f'\n   📈 ИТОГО СВЯЗЕЙ: {total_created}/{total_possible} ({total_percentage:.1f}%)')

        # Общая статистика
        self.stdout.write('\n📈 ОБЩАЯ СТАТИСТИКА:')
        self.stdout.write('─' * 40)
        self.stdout.write(f'⏱️  Общее время выполнения: {total_time:.2f} секунд')

        # Подсчитываем общее количество созданных объектов
        total_objects_created = 0
        for key, stats in objects_stats.items():
            if 'created' in stats:
                total_objects_created += stats['created']

        self.stdout.write(f'🏗️  Всего создано объектов: {total_objects_created}')

        if total_time > 0:
            objects_per_second = total_objects_created / total_time
            self.stdout.write(f'🚀 Скорость создания: {objects_per_second:.1f} объектов/сек')

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
            'collected_data': loaded_data_stats.get('collected', {}).copy(),
            'loaded_data': loaded_data_stats.get('loaded', {}).copy(),

            # Время выполнения
            'step_times': all_step_times,

            # Статистика связей
            'relations': relations_results or {},

            # Возможное количество связей
            'relations_possible': relations_possible or {},

            # Дополнительная статистика
            'screenshots_loaded': screenshots_loaded,
        }

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

        # Статистика связей
        if stats['relations'] and stats['relations_possible']:
            self.stdout.write('\n🔗 СТАТИСТИКА СВЯЗЕЙ (СОЗДАНО / ВОЗМОЖНО):')

            relation_types = [
                ('🎭 Жанры', 'genre_relations', 'possible_genre_relations'),
                ('🖥️  Платформы', 'platform_relations', 'possible_platform_relations'),
                ('🔑 Ключевые слова', 'keyword_relations', 'possible_keyword_relations'),
                ('📚 Серии (M2M)', 'series_relations', 'possible_series_relations'),
                ('🏢 Разработчики', 'developer_relations', 'possible_developer_relations'),
                ('📦 Издатели', 'publisher_relations', 'possible_publisher_relations'),
                ('🎨 Темы', 'theme_relations', 'possible_theme_relations'),
                ('👁️  Перспективы', 'perspective_relations', 'possible_perspective_relations'),
                ('🎮 Режимы', 'mode_relations', 'possible_mode_relations'),
            ]

            for display_name, created_key, possible_key in relation_types:
                created = stats['relations'].get(created_key, 0)
                possible = stats['relations_possible'].get(possible_key, 0)

                if possible > 0:
                    percentage = (created / possible * 100) if possible > 0 else 0
                    self.stdout.write(f'   • {display_name}: {created}/{possible} ({percentage:.1f}%)')

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
        self.stdout.write(f'   📸 Скриншотов: {stats["total_screenshots"]}')

        # Итоговые показатели
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
        self.stdout.write(f'⏱️  Время: {stats["total_time"]:.2f}с')

        if stats['total_time'] > 0:
            speed = stats['total_games_found'] / stats['total_time']
            self.stdout.write(f'🚀 СКОРОСТЬ: {speed:.1f} игр/сек')

        self.stdout.write(f'🎮 Найдено: {stats["total_games_found"]}')
        self.stdout.write(f'✅ Загружено: {stats["created_count"]}')
        self.stdout.write(f'⏭️  Пропущено: {stats["skipped_count"]}')

    def check_series_in_database(self):
        """Проверяет состояние серий в базе данных"""
        self.stdout.write('\n🔍 ПРОВЕРКА СЕРИЙ В БАЗЕ ДАННЫХ')
        self.stdout.write('=' * 60)

        # 1. Общее количество
        total_series = Series.objects.count()
        self.stdout.write(f'📊 Всего серий в базе: {total_series}')

        # 2. Серии с пустыми именами
        empty_name_series = Series.objects.filter(name='')
        empty_count = empty_name_series.count()
        if empty_count > 0:
            self.stdout.write(f'⚠️  Серии с пустыми именами: {empty_count}')

        # 3. Серии с именами по умолчанию
        default_name_series = Series.objects.filter(name__startswith='Series ')
        default_count = default_name_series.count()
        if default_count > 0:
            self.stdout.write(f'⚠️  Серии с именами по умолчанию: {default_count}')

        # 4. Дубликаты по igdb_id
        duplicates = Series.objects.values('igdb_id').annotate(
            count=Count('igdb_id')).filter(count__gt=1)

        duplicate_count = duplicates.count()
        if duplicate_count > 0:
            self.stdout.write(f'🚨 Дубликаты по igdb_id: {duplicate_count}')

        return {
            'total_series': total_series,
            'empty_names': empty_count,
            'default_names': default_count,
            'duplicates': duplicate_count,
        }