import time
from collections import Counter
from games.models import (
    Game, Genre, Keyword, Platform, Series,
    Company, Theme, PlayerPerspective, GameMode, Screenshot
)
from django.db.models import Q, Count


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

        # ★★★ ДИАГНОСТИКА РАСХОЖДЕНИЙ В СТАТИСТИКЕ СЕРИЙ ★★★
        if debug and relations_possible and relations_results:
            possible_series = relations_possible.get('possible_series_relations', 0)
            created_series = relations_results.get('series_relations', 0)

            if possible_series > 0:
                self.stdout.write(f'\n🔍 ДЕТАЛЬНАЯ ДИАГНОСТИКА СТАТИСТИКИ СЕРИЙ (M2M):')
                self.stdout.write(f'   • Возможных M2M связей (из данных): {possible_series}')
                self.stdout.write(f'   • Созданных M2M связей: {created_series}')

                if created_series < possible_series:
                    discrepancy = possible_series - created_series
                    self.stdout.write(f'   ❌ Расхождение: {discrepancy}')

                    # Проверяем возможные причины
                    self.stdout.write(f'   🔍 Возможные причины расхождения:')

                    # 1. Проверяем M2M связи в базе
                    from django.db import connection
                    try:
                        with connection.cursor() as cursor:
                            # Получаем количество M2M связей игр с сериями
                            cursor.execute("SELECT COUNT(*) FROM games_game_series")
                            m2m_count = cursor.fetchone()[0]
                            self.stdout.write(f'      • M2M связей в базе: {m2m_count}')

                            # Получаем количество уникальных игр с сериями
                            cursor.execute("SELECT COUNT(DISTINCT game_id) FROM games_game_series")
                            unique_games_with_series = cursor.fetchone()[0]
                            self.stdout.write(f'      • Уникальных игр с сериями: {unique_games_with_series}')

                            # Среднее количество серий на игру
                            if unique_games_with_series > 0:
                                avg_series_per_game = m2m_count / unique_games_with_series
                                self.stdout.write(f'      • Среднее серий на игру (M2M): {avg_series_per_game:.1f}')
                    except Exception as e:
                        self.stdout.write(f'      ⚠️  Не удалось проверить M2M связи: {e}')

                    # 2. Проверяем, сколько игр создано в этой сессии
                    self.stdout.write(f'      • Игр создано в этой сессии: {created_count}')

                else:
                    self.stdout.write(f'   ✅ Все M2M связи созданы успешно!')

                # Процент успешности
                success_rate = (created_series / possible_series) * 100 if possible_series > 0 else 0
                self.stdout.write(f'   📈 Успешность создания M2M связей: {success_rate:.1f}%')

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
            ('📸 Скриншоты', 'screenshots'),
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

        # Статистика связей - УДАЛЕН БЛОК "РЕЗУЛЬТАТЫ ЗАГРУЗКИ"
        if stats['relations'] and stats['relations_possible']:
            self.stdout.write('\n🔗 СТАТИСТИКА СВЯЗЕЙ (СОЗДАНО / ВОЗМОЖНО):')

            # Серии (M2M)
            created_series = stats['relations'].get('series_relations', 0)
            possible_series = stats['relations_possible'].get('possible_series_relations', 0)

            if possible_series > 0:
                series_percentage = (created_series / possible_series * 100) if possible_series > 0 else 0
                self.stdout.write(f'   • 📚 Серии (M2M): {created_series}/{possible_series} ({series_percentage:.1f}%)')

            # Другие типы связей
            relations_info = [
                ('🎭 Жанры', 'genre_relations', 'possible_genre_relations'),
                ('🖥️  Платформы', 'platform_relations', 'possible_platform_relations'),
                ('🔑 Ключевые слова', 'keyword_relations', 'possible_keyword_relations'),
                ('🏢 Разработчики', 'developer_relations', 'possible_developer_relations'),
                ('📦 Издатели', 'publisher_relations', 'possible_publisher_relations'),
                ('🎨 Темы', 'theme_relations', 'possible_theme_relations'),
                ('👁️  Перспективы', 'perspective_relations', 'possible_perspective_relations'),
                ('🎮 Режимы', 'mode_relations', 'possible_mode_relations'),
            ]

            for display_name, created_key, possible_key in relations_info:
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

    def debug_series_relations(self, collected_data, data_maps, all_game_relations, relations_results, debug=False):
        """Детальная диагностика M2M связей с сериями"""
        if not debug:
            return

        self.stdout.write('\n🔍 ДЕТАЛЬНАЯ ДИАГНОСТИКА M2M СВЯЗЕЙ С СЕРИЯМИ')
        self.stdout.write('=' * 60)

        # 1. Проверяем M2M связи в базе данных
        from django.db import connection
        try:
            with connection.cursor() as cursor:
                # Количество M2M связей
                cursor.execute("SELECT COUNT(*) FROM games_game_series")
                m2m_total = cursor.fetchone()[0]
                self.stdout.write(f'📊 M2M СВЯЗЕЙ ИГР-СЕРИЙ В БАЗЕ: {m2m_total}')

                # Количество уникальных игр с сериями
                cursor.execute("SELECT COUNT(DISTINCT game_id) FROM games_game_series")
                unique_games = cursor.fetchone()[0]
                self.stdout.write(f'   • Уникальных игр с сериями: {unique_games}')

                # Среднее количество серий на игру
                if unique_games > 0:
                    avg_series = m2m_total / unique_games
                    self.stdout.write(f'   • Среднее серий на игру: {avg_series:.1f}')

                # Проверяем серии без связей
                cursor.execute("""
                               SELECT s.id, s.name, s.igdb_id
                               FROM games_series s
                                        LEFT JOIN games_game_series gs ON s.id = gs.series_id
                               WHERE gs.series_id IS NULL LIMIT 10
                               """)
                series_without_games = cursor.fetchall()

                if series_without_games:
                    self.stdout.write(f'\n⚠️  СЕРИИ БЕЗ ИГР: {len(series_without_games)}')
                    for s_id, s_name, igdb_id in series_without_games:
                        self.stdout.write(f'   • "{s_name}" (ID: {igdb_id})')

        except Exception as e:
            self.stdout.write(f'   ⚠️  Ошибка проверки M2M связей: {e}')

    def debug_series_loading(self):
        """Метод для диагностики загрузки серий"""
        self.stdout.write('🔍 ДИАГНОСТИКА ПРОБЛЕМЫ С СЕРИЯМИ')
        self.stdout.write('=' * 60)

        # 1. Проверяем текущее состояние в базе
        total_series_in_db = Series.objects.count()
        self.stdout.write(f'📊 СЕРИИ В БАЗЕ ДАННЫХ: {total_series_in_db}')

        # 2. Проверяем дубликаты по igdb_id
        duplicates = Series.objects.values('igdb_id').annotate(
            count=Count('igdb_id')).filter(count__gt=1)

        if duplicates.exists():
            self.stdout.write(f'⚠️  Найдено дубликатов по igdb_id: {duplicates.count()}')
            for dup in duplicates[:5]:
                self.stdout.write(f'   • ID {dup["igdb_id"]}: {dup["count"]} записей')

        # 3. Проверяем серии с пустыми именами
        empty_name_series = Series.objects.filter(name='')
        if empty_name_series.exists():
            self.stdout.write(f'⚠️  Серии с пустыми именами: {empty_name_series.count()}')

        # 4. Проверяем серии с именами по умолчанию
        default_name_series = Series.objects.filter(name__startswith='Series ')
        if default_name_series.exists():
            self.stdout.write(f'⚠️  Серии с именами по умолчанию: {default_name_series.count()}')
            for s in default_name_series[:5]:
                self.stdout.write(f'   • ID {s.igdb_id}: "{s.name}"')

        return {
            'total_series': total_series_in_db,
            'duplicates_count': duplicates.count() if duplicates.exists() else 0,
            'empty_names': empty_name_series.count(),
            'default_names': default_name_series.count()
        }

    def check_series_in_database(self):
        """Проверяет состояние серий в базе данных"""
        from games.models import Series

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
            for series in empty_name_series[:5]:
                self.stdout.write(f'   • ID {series.igdb_id}')

        # 3. Серии с именами по умолчанию
        default_name_series = Series.objects.filter(name__startswith='Series ')
        default_count = default_name_series.count()
        if default_count > 0:
            self.stdout.write(f'⚠️  Серии с именами по умолчанию: {default_count}')
            for series in default_name_series[:5]:
                self.stdout.write(f'   • ID {series.igdb_id}: "{series.name}"')

        # 4. Дубликаты по igdb_id
        duplicates = Series.objects.values('igdb_id').annotate(
            count=Count('igdb_id')).filter(count__gt=1)

        duplicate_count = duplicates.count()
        if duplicate_count > 0:
            self.stdout.write(f'🚨 Дубликаты по igdb_id: {duplicate_count}')
            for dup in duplicates[:5]:
                series_list = Series.objects.filter(igdb_id=dup['igdb_id'])
                self.stdout.write(f'   • ID {dup["igdb_id"]}: {dup["count"]} записей')
                for s in series_list:
                    self.stdout.write(f'     - "{s.name}" (ID базы: {s.id})')

        # 5. Статистика по именам серий (упрощенная)
        self.stdout.write(f'\n📈 Статистика по именам серий:')

        # Имена нормальной длины
        normal_names = Series.objects.filter(~Q(name=''), ~Q(name__startswith='Series '))
        normal_count = normal_names.count()
        self.stdout.write(f'   • Нормальные имена: {normal_count}')

        # Процент нормальных имен
        if total_series > 0:
            normal_percentage = (normal_count / total_series) * 100
            self.stdout.write(f'   • Процент нормальных имен: {normal_percentage:.1f}%')

        # 6. Примеры последних добавленных серий
        recent_series = Series.objects.order_by('-id')[:5]
        self.stdout.write(f'\n📋 Последние 5 добавленных серий:')
        for series in recent_series:
            self.stdout.write(f'   • ID {series.igdb_id}: "{series.name}"')

        return {
            'total_series': total_series,
            'empty_names': empty_count,
            'default_names': default_count,
            'duplicates': duplicate_count,
            'normal_names': normal_count
        }