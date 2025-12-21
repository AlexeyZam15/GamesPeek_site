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