# games/management/commands/load_games.py

from django.core.management.base import BaseCommand
from django.utils import timezone
from games.igdb_api import make_igdb_request
from games.models import Game
import sys
import os
import time
from .load_igdb.data_collector import DataCollector


class BaseGamesCommand(BaseCommand):
    """Базовый класс для команд загрузки IGDB"""

    def add_arguments(self, parser):
        """Общие аргументы для всех команд"""
        parser.add_argument('--genres', type=str, default='',
                            help='Загружать игры с указанными жанрами (логика И между жанрами). Формат: "Жанр1,Жанр2,Жанр3"')
        parser.add_argument('--description-contains', type=str, default='',
                            help='Загружать игры с указанным текстом в описании или названии')
        parser.add_argument('--overwrite', action='store_true',
                            help='Удалить существующие игры и загрузить заново')
        parser.add_argument('--debug', action='store_true',
                            help='Включить режим отладки')
        parser.add_argument('--limit', type=int, default=0,
                            help='Ограничить количество загружаемых игр (0 - без ограничения)')
        parser.add_argument('--offset', type=int, default=0,
                            help='Пропустить указанное количество игр из результатов поиска')
        parser.add_argument('--min-rating-count', type=int, default=0,
                            help='Минимальное количество оценок для фильтрации (0 - без фильтра)')
        parser.add_argument('--keywords', type=str, default='',
                            help='Загружать игры с указанными ключевыми словами (логика И). Формат: "word1,word2,word3"')
        parser.add_argument('--count-only', action='store_true',
                            help='Только подсчитать количество НОВЫХ игр (которых нет в базе) без сохранения')
        parser.add_argument('--repeat', type=int, default=1,
                            help='Повторить команду указанное количество раз')

    def handle(self, *args, **options):
        """Основной метод выполнения команды"""
        repeat_count = options['repeat']
        original_offset = options['offset']
        limit = options['limit']
        debug = options['debug']
        overwrite = options['overwrite']

        if repeat_count < 1:
            self.stdout.write('❌ Количество повторений должно быть положительным числом')
            return

        if repeat_count > 1:
            self.stdout.write(f'🔄 КОМАНДА БУДЕТ ПОВТОРЕНА {repeat_count} РАЗ')
            self.stdout.write('=' * 60)

        # Для режима с лимитом - будем использовать cumulative offset
        # Каждая итерация будет продолжать с того места, где закончила предыдущая
        current_offset = original_offset

        total_stats = {
            'iterations': 0,
            'total_games_found': 0,
            'total_games_checked': 0,
            'total_games_created': 0,
            'total_games_skipped': 0,
            'total_time': 0,
            'last_checked_offset': original_offset,
            'errors': 0,
            'iterations_with_errors': 0,
            'iterations_with_limit_reached': 0,  # Итерации, где достигли лимита
        }

        for iteration in range(1, repeat_count + 1):
            if repeat_count > 1:
                self.stdout.write(f'\n🌀 ИТЕРАЦИЯ {iteration}/{repeat_count}')
                self.stdout.write('=' * 40)

                if current_offset > original_offset:
                    self.stdout.write(f'📊 Начинаем с offset: {current_offset}')

            # Определяем лимит для этой итерации
            if limit > 0:
                iteration_limit = limit
                self.stdout.write(f'🎯 Цель итерации: найти {iteration_limit} новых игр')
            else:
                iteration_limit = 0

            # Добавляем номер итерации в параметры
            iteration_options = options.copy()
            iteration_options['iteration_offset'] = current_offset
            iteration_options['iteration_limit'] = iteration_limit
            iteration_options['_iteration_number'] = iteration

            try:
                iteration_result = self.handle_single_iteration(*args, **iteration_options)

                if iteration_result:
                    total_stats['iterations'] += 1
                    total_stats['total_games_found'] += iteration_result.get('total_games_found', 0)
                    total_stats['total_games_checked'] += iteration_result.get('total_games_checked',
                                                                               iteration_result.get('total_games_found',
                                                                                                    0))
                    total_stats['total_games_created'] += iteration_result.get('created_count', 0)
                    total_stats['total_games_skipped'] += iteration_result.get('skipped_count', 0)
                    total_stats['total_time'] += iteration_result.get('total_time', 0)

                    # Добавляем ошибки из итерации
                    iteration_errors = iteration_result.get('errors', 0)
                    if iteration_errors > 0:
                        total_stats['errors'] += iteration_errors
                        total_stats['iterations_with_errors'] += 1

                    # Проверяем, достиг ли лимит в этой итерации
                    iteration_limit_reached = iteration_result.get('limit_reached', False)
                    if iteration_limit_reached:
                        total_stats['iterations_with_limit_reached'] += 1

                    # Получаем последний проверенный offset
                    # Приоритет: limit_reached_at_offset > last_checked_offset > расчетный
                    limit_reached_offset = iteration_result.get('limit_reached_at_offset')
                    if limit_reached_offset is not None:
                        last_checked_this_iteration = limit_reached_offset
                    else:
                        last_checked_this_iteration = iteration_result.get('last_checked_offset',
                                                                           current_offset + iteration_result.get(
                                                                               'total_games_checked',
                                                                               iteration_result.get('total_games_found',
                                                                                                    0)) - 1)

                    # Обновляем offset для следующей итерации
                    previous_offset = current_offset
                    current_offset = last_checked_this_iteration + 1
                    total_stats['last_checked_offset'] = last_checked_this_iteration

                    if debug:
                        self.stdout.write(f'   📊 Итерация {iteration}:')
                        self.stdout.write(f'      • Начальный offset: {previous_offset}')
                        self.stdout.write(f'      • Просмотрено игр: {iteration_result.get("total_games_checked", 0)}')
                        self.stdout.write(f'      • Найдено новых: {iteration_result.get("total_games_found", 0)}')
                        self.stdout.write(f'      • Загружено: {iteration_result.get("created_count", 0)}')
                        self.stdout.write(f'      • Ошибок: {iteration_errors}')
                        if iteration_limit_reached:
                            self.stdout.write(f'      • Лимит достигнут: ДА')
                            if limit_reached_offset is not None:
                                self.stdout.write(f'      • Лимит достигнут на offset: {limit_reached_offset}')
                        self.stdout.write(f'      • Последний проверенный offset: {last_checked_this_iteration}')
                        self.stdout.write(f'      • Следующий offset: {current_offset}')

                    # Проверяем, стоит ли продолжать
                    if limit > 0 and iteration < repeat_count:
                        remaining_iterations = repeat_count - iteration
                        if debug:
                            self.stdout.write(f'   📈 Осталось итераций: {remaining_iterations}')

                else:
                    # Если итерация не вернула результат
                    if debug:
                        self.stdout.write(f'   ⚠️  Итерация {iteration} не вернула результат')
                    total_stats['iterations'] += 1

            except Exception as e:
                total_stats['errors'] += 1
                total_stats['iterations_with_errors'] += 1
                self.stderr.write(f'❌ ОШИБКА в итерации {iteration}: {str(e)}')
                if debug:
                    import traceback
                    self.stderr.write(f'📋 Трассировка ошибки:')
                    self.stderr.write(traceback.format_exc())

                # Продолжаем выполнение
                if iteration < repeat_count:
                    self.stdout.write(f'   ⏩ Пропускаем итерацию {iteration} из-за ошибки')
                total_stats['iterations'] += 1

            # Пауза между итерациями
            if iteration < repeat_count:
                pause_time = 2
                if debug:
                    self.stdout.write(f'   ⏸️  Пауза {pause_time} секунд...')
                time.sleep(pause_time)

        # Вывод общей статистики
        if repeat_count > 1 or total_stats['iterations'] > 1:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('📊 ОБЩАЯ СТАТИСТИКА ВСЕХ ИТЕРАЦИЙ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🔄 Итераций выполнено: {total_stats["iterations"]}/{repeat_count}')
            self.stdout.write(f'📍 Начальный offset: {original_offset}')
            self.stdout.write(f'📍 Последний проверенный offset: {total_stats["last_checked_offset"]}')
            self.stdout.write(f'📍 Следующий offset (для продолжения): {current_offset}')
            self.stdout.write(f'👀 Всего просмотрено игр: {total_stats["total_games_checked"]}')
            self.stdout.write(f'🎮 Всего найдено новых: {total_stats["total_games_found"]}')
            self.stdout.write(f'✅ Всего загружено игр: {total_stats["total_games_created"]}')
            self.stdout.write(f'⏭️  Всего пропущено игр: {total_stats["total_games_skipped"]}')
            self.stdout.write(f'❌ Ошибок: {total_stats["errors"]}')
            self.stdout.write(f'⚠️  Итераций с ошибками: {total_stats["iterations_with_errors"]}')

            if limit > 0:
                self.stdout.write(
                    f'🎯 Лимит достигнут в итерациях: {total_stats["iterations_with_limit_reached"]}/{total_stats["iterations"]}')

            self.stdout.write(f'⏱️  Общее время: {total_stats["total_time"]:.2f}с')

            # Статус выполнения
            success_rate = ((total_stats['iterations'] - total_stats['iterations_with_errors']) / total_stats[
                'iterations'] * 100) if total_stats['iterations'] > 0 else 0
            self.stdout.write(f'📈 Успешных итераций: {success_rate:.1f}%')

            if limit > 0:
                total_possible_games = limit * repeat_count
                self.stdout.write(
                    f'🎯 Общий целевой лимит: {total_possible_games} игр (по {limit} на итерацию × {repeat_count} итераций)')
                percentage = (total_stats[
                                  "total_games_created"] / total_possible_games * 100) if total_possible_games > 0 else 0
                self.stdout.write(f'📈 Достигнуто: {percentage:.1f}% от общего лимита')

                # Показываем эффективность по итерациям
                if total_stats['iterations'] > 0:
                    avg_games_per_iteration = total_stats['total_games_created'] / total_stats['iterations']
                    self.stdout.write(f'📊 Среднее игр за итерацию: {avg_games_per_iteration:.1f} (цель: {limit})')

            # Эффективность поиска
            if total_stats['total_games_created'] > 0:
                efficiency = total_stats['total_games_checked'] / total_stats['total_games_created']
                self.stdout.write(f'📊 Эффективность поиска: {efficiency:.1f} просмотренных на 1 новую игру')

                # Скорость загрузки
                if total_stats['total_time'] > 0:
                    games_per_second = total_stats['total_games_created'] / total_stats['total_time']
                    self.stdout.write(f'🚀 Скорость загрузки: {games_per_second:.2f} игр/сек')
                    checked_per_second = total_stats['total_games_checked'] / total_stats['total_time']
                    self.stdout.write(f'🔍 Скорость проверки: {checked_per_second:.2f} игр/сек')

            if total_stats['iterations'] > 0:
                avg_time = total_stats['total_time'] / total_stats['iterations']
                self.stdout.write(f'📈 Среднее время на итерацию: {avg_time:.2f}с')

        # Дополнительная информация для режима overwrite
        if overwrite:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'✅ Перезаписано игр: {total_stats["total_games_created"]}')

            # Правильная информация о offset
            self.stdout.write(f'📍 Начальный offset: {original_offset}')
            self.stdout.write(f'📍 Последний проверенный offset: {total_stats["last_checked_offset"]}')
            self.stdout.write(f'📍 Следующий offset (для продолжения): {current_offset}')

            # Рассчитываем проверенные позиции (игры с offset от original_offset до last_checked_offset включительно)
            if total_stats['last_checked_offset'] >= original_offset:
                checked_positions = total_stats['last_checked_offset'] - original_offset + 1
                self.stdout.write(f'📊 Проверено позиций в IGDB: {checked_positions}')

                # Показываем соотношение проверенных позиций и загруженных игр
                if total_stats['total_games_created'] > 0:
                    self.stdout.write(
                        f'📊 Соотношение позиций/игр: {checked_positions}:{total_stats["total_games_created"]}')

                    # Эффективность (сколько позиций нужно проверить для одной игры)
                    efficiency = checked_positions / total_stats['total_games_created']
                    self.stdout.write(f'📊 Эффективность: {efficiency:.1f} позиций на 1 игру')

            self.stdout.write(f'👀 Просмотрено игр из IGDB: {total_stats["total_games_checked"]}')

            # Эффективность поиска
            if total_stats['total_games_created'] > 0 and total_stats['total_games_checked'] > 0:
                search_efficiency = total_stats['total_games_checked'] / total_stats['total_games_created']
                self.stdout.write(
                    f'🔍 Эффективность поиска: {search_efficiency:.1f} просмотренных на 1 перезаписанную игру')

            # Статистика по лимиту
            if limit > 0:
                self.stdout.write(f'🎯 Лимит достигнут в {total_stats["iterations_with_limit_reached"]} итерациях')

            if total_stats['errors'] > 0:
                self.stdout.write(f'⚠️  Ошибок при перезаписи: {total_stats["errors"]}')

        self.stdout.write('=' * 60)

        # Итоговый статус
        if total_stats['errors'] > 0:
            self.stdout.write('⚠️  ЗАГРУЗКА ЗАВЕРШЕНА С ОШИБКАМИ!')
        else:
            self.stdout.write('✅ ВСЕ ИТЕРАЦИИ ЗАВЕРШЕНЫ УСПЕШНО!')

        return total_stats

    def handle_single_iteration(self, *args, **options):
        """Обработка одной итерации команды"""
        genres_str = options['genres']
        description_contains = options['description_contains']
        overwrite = options['overwrite']
        debug = options['debug']
        limit = options['limit']
        offset = options['offset']
        min_rating_count = options['min_rating_count']
        keywords_str = options['keywords']
        count_only = options['count_only']

        # Получаем параметры для конкретной итерации
        iteration_offset = options.get('iteration_offset', offset)
        iteration_limit = options.get('iteration_limit', limit)

        # Получаем номер итерации для информационных сообщений
        iteration_number = options.get('_iteration_number', 1)
        repeat_count = options.get('repeat', 1)

        # Инициализируем счетчик ошибок для этой итерации
        errors = 0
        iteration_start_time = time.time()

        self.stdout.write('🎮 ЗАГРУЗКА ИГР ИЗ IGDB')
        self.stdout.write('=' * 60)

        if count_only:
            self.stdout.write('🔢 РЕЖИМ: ПОДСЧЕТ НОВЫХ ИГР (которых нет в базе)')
            self.stdout.write('⚠️  Игры не будут сохранены в базу данных!')

        # Определяем тип загрузки
        if genres_str and description_contains:
            genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]
            self.stdout.write(
                f'🎭📝 РЕЖИМ: Игры со всеми жанрами ({len(genre_list)}) И текстом "{description_contains}" в описании/названии')
            self.stdout.write(f'   🎭 Жанры: {", ".join(genre_list)}')
        elif genres_str:
            genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]
            self.stdout.write(
                f'🎭 РЕЖИМ: Игры со всеми жанрами ({len(genre_list)}): {", ".join(genre_list)}')
        elif description_contains:
            self.stdout.write(f'📝 РЕЖИМ: Игры с текстом "{description_contains}" в описании/названии')
        elif keywords_str:
            keyword_list = [k.strip() for k in keywords_str.split(',') if k.strip()]
            self.stdout.write(
                f'🔑 РЕЖИМ: Игры с ключевыми словами ({len(keyword_list)} слов): {", ".join(keyword_list)}')
        else:
            self.stdout.write('📊 РЕЖИМ: Все популярные игры')

        if repeat_count > 1:
            self.stdout.write(f'🔄 Итерация {iteration_number}/{repeat_count}')

        # Используем offset и limit для этой конкретной итерации
        actual_offset = iteration_offset
        actual_limit = iteration_limit

        if actual_limit > 0:
            self.stdout.write(f'📊 ЛИМИТ: {actual_limit} НОВЫХ игр (для этой итерации)')
        if actual_offset > 0:
            self.stdout.write(f'⏭️  OFFSET: начинаем с позиции {actual_offset} в результатах поиска')
        if min_rating_count > 0:
            self.stdout.write(f'⭐ ФИЛЬТР: игры с не менее {min_rating_count} оценками')

        if overwrite and not count_only:
            self.stdout.write('🔄 OVERWRITE: найденные игры будут удалены и загружены заново')

        if count_only and overwrite:
            self.stdout.write('⚠️  Overwrite игнорируется в режиме count-only')

        if debug:
            self.stdout.write('🐛 РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН')
            self.stdout.write('-' * 40)

        # Определяем режим фильтрации
        if overwrite:
            skip_existing = False  # Не пропускаем существующие, перезаписываем их
        else:
            skip_existing = True  # Обычный режим: пропускаем существующие игры

        result = None
        # Загружаем игры в зависимости от режима с обработкой ошибок
        try:
            if genres_str and description_contains:
                result = self.load_games_by_genres_and_description(
                    genres_str, description_contains, debug, actual_limit, actual_offset,
                    min_rating_count, skip_existing, count_only
                )
            elif genres_str:
                result = self.load_games_by_genres(
                    genres_str, debug, actual_limit, actual_offset,
                    min_rating_count, skip_existing, count_only
                )
            elif description_contains:
                result = self.load_games_by_description(
                    description_contains, debug, actual_limit, actual_offset,
                    min_rating_count, skip_existing, count_only
                )
            elif keywords_str:
                result = self.load_games_by_keywords(
                    keywords_str, debug, actual_limit, actual_offset,
                    min_rating_count, skip_existing, count_only
                )
            else:
                result = self.load_all_popular_games(
                    debug, actual_limit, actual_offset,
                    min_rating_count, skip_existing, count_only
                )
        except Exception as e:
            errors += 1
            self.stderr.write(f'❌ ОШИБКА при загрузке игр: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())

        # Проверяем результат
        if not result or not result.get('new_games'):
            iteration_time = time.time() - iteration_start_time

            if result and result.get('total_games_checked', 0) > 0:
                if overwrite:
                    self.stdout.write(f'ℹ️  Найдено {result.get("total_games_checked", 0)} игр для перезаписи')
                else:
                    self.stdout.write(
                        f'❌ Найдено {result.get("total_games_checked", 0)} игр, но все они уже есть в базе')
            else:
                if errors == 0:
                    self.stdout.write('❌ Не найдено игр для загрузки')

            # Получаем последний проверенный offset
            last_checked = result.get('last_checked_offset', actual_offset) if result else actual_offset

            return {
                'total_games_found': 0,
                'total_games_checked': result.get('total_games_checked', 0) if result else 0,
                'created_count': 0,
                'skipped_count': result.get('existing_games_skipped', 0) if result else 0,
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': last_checked,
                'limit_reached': result.get('limit_reached', False) if result else False,
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        all_games = result['new_games']
        total_games_checked = result['total_games_checked']
        new_games_count = result['new_games_count']
        existing_games_skipped = result['existing_games_skipped']
        limit_reached = result.get('limit_reached', False)
        limit_reached_at_offset = result.get('limit_reached_at_offset')

        # Получаем последний проверенный offset
        # Приоритет: limit_reached_at_offset > last_checked_offset > расчетный
        if limit_reached_at_offset is not None:
            last_checked_offset = limit_reached_at_offset
        else:
            last_checked_offset = result.get('last_checked_offset',
                                             actual_offset + total_games_checked - 1)

        if overwrite:
            self.stdout.write(f'📥 Найдено игр для перезаписи: {new_games_count}')
        else:
            self.stdout.write(f'📥 Найдено игр для обработки: {new_games_count}')

        self.stdout.write(f'👀 Всего просмотрено игр из IGDB: {total_games_checked}')
        self.stdout.write(f'📍 Последний проверенный offset: {last_checked_offset}')
        self.stdout.write(f'📍 Следующий offset для продолжения: {last_checked_offset + 1}')

        if limit_reached:
            self.stdout.write(f'🎯 Лимит {actual_limit} достигнут на offset {last_checked_offset}')

        if existing_games_skipped > 0 and not overwrite:
            self.stdout.write(f'⏭️  Пропущено существующих игр: {existing_games_skipped}')

        # Режим только подсчета
        if count_only:
            iteration_time = time.time() - iteration_start_time
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ПОДСЧЕТ ЗАВЕРШЕН!')
            self.stdout.write(f'🎮 Игр можно загрузить (которых нет в базе): {new_games_count}')

            if errors > 0:
                self.stdout.write(f'❌ Ошибок при подсчете: {errors}')

            return {
                'total_games_found': new_games_count,
                'total_games_checked': total_games_checked,
                'created_count': 0,
                'skipped_count': existing_games_skipped,
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': last_checked_offset,
                'limit_reached': limit_reached,
                'limit_reached_at_offset': limit_reached_at_offset,
            }

        # Обработка режима перезаписи
        if overwrite:
            try:
                self._handle_overwrite_mode(all_games, debug)
            except Exception as e:
                errors += 1
                self.stderr.write(f'❌ ОШИБКА при удалении игр: {str(e)}')
                if debug:
                    import traceback
                    self.stderr.write(f'📋 Трассировка ошибки:')
                    self.stderr.write(traceback.format_exc())

        # Обработка данных
        result_stats = None
        try:
            collector = DataCollector(self.stdout, self.stderr)
            result_stats = collector.process_all_data_sequentially(all_games, debug)
        except Exception as e:
            errors += 1
            self.stderr.write(f'❌ ОШИБКА при обработке данных: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            # Создаем пустую статистику при ошибке
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }

        # Добавляем информацию о просмотренных играх
        if result_stats:
            result_stats['total_games_checked'] = total_games_checked
            result_stats['total_games_found'] = new_games_count
            result_stats['errors'] = errors
            result_stats['last_checked_offset'] = last_checked_offset
            result_stats['limit_reached'] = limit_reached
            result_stats['limit_reached_at_offset'] = limit_reached_at_offset
        else:
            # Если result_stats не создан
            iteration_time = time.time() - iteration_start_time
            result_stats = {
                'total_games_checked': total_games_checked,
                'total_games_found': new_games_count,
                'created_count': 0,
                'skipped_count': 0,
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': last_checked_offset,
                'limit_reached': limit_reached,
                'limit_reached_at_offset': limit_reached_at_offset,
            }

        # КРАТКАЯ статистика в конце
        if not debug:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
            self.stdout.write(f'⏱️  Время: {result_stats["total_time"]:.2f}с')

            if repeat_count > 1:
                self.stdout.write(f'🔄 Итерация {iteration_number}/{repeat_count}')

            if errors > 0:
                self.stdout.write(f'❌ Ошибок в итерации: {errors}')

            if limit_reached:
                self.stdout.write(f'🎯 Лимит достигнут на offset {last_checked_offset}')

            if overwrite:
                self.stdout.write(f'🔄 Перезаписано игр: {result_stats["created_count"]}')
            else:
                self.stdout.write(f'✅ Загружено игр: {result_stats["created_count"]}')

        # Подробная статистика в режиме отладки
        elif debug:
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('📊 СТАТИСТИКА ИТЕРАЦИИ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🔄 Итерация: {iteration_number}/{repeat_count}')
            self.stdout.write(f'📍 Начальный offset: {actual_offset}')
            self.stdout.write(f'📍 Последний проверенный offset: {last_checked_offset}')
            self.stdout.write(f'📍 Следующий offset: {last_checked_offset + 1}')
            self.stdout.write(f'👀 Просмотрено игр: {total_games_checked}')
            self.stdout.write(f'🎮 Найдено новых: {new_games_count}')
            self.stdout.write(f'✅ Загружено игр: {result_stats.get("created_count", 0)}')
            self.stdout.write(f'❌ Ошибок: {errors}')

            if limit_reached:
                self.stdout.write(f'🎯 Лимит достигнут: ДА (на offset {last_checked_offset})')

            self.stdout.write(f'⏱️  Время: {result_stats.get("total_time", 0):.2f}с')

            if errors > 0:
                self.stdout.write('⚠️  ИТЕРАЦИЯ ЗАВЕРШЕНА С ОШИБКАМИ')
            else:
                self.stdout.write('✅ ИТЕРАЦИЯ ЗАВЕРШЕНА УСПЕШНО')

        return result_stats

    def load_games_by_genres(self, genres_str, debug=False, limit=0, offset=0, min_rating_count=0,
                             skip_existing=True, count_only=False):
        """Загрузка игр по жанрам с логикой И (должны быть ВСЕ указанные жанры)"""
        collector = DataCollector(self.stdout, self.stderr)

        genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]

        if not genre_list:
            self.stdout.write('⚠️  Не указаны жанры')
            return []

        if debug:
            self.stdout.write(f'🔍 Поиск жанров: {", ".join(genre_list)}')

        # Получаем ID для всех жанров
        genre_ids = []
        for genre in genre_list:
            query = f'fields id,name; where name = "{genre}";'
            result = make_igdb_request('genres', query, debug=False)
            if result:
                genre_ids.append(str(result[0]['id']))
                if debug:
                    self.stdout.write(f'   ✅ Жанр "{genre}" найден: ID {result[0]["id"]}')
            else:
                if debug:
                    self.stdout.write(f'   ❌ Жанр "{genre}" не найден')

        if not genre_ids:
            self.stdout.write('❌ Не найдены указанные жанры')
            return []

        if debug:
            self.stdout.write(f'📋 Найдено ID жанров: {", ".join(genre_ids)}')

        # Формируем условие для поиска игр (логика И - должны быть ВСЕ жанры)
        genre_conditions = [f'genres = ({genre_id})' for genre_id in genre_ids]
        where_clause = ' & '.join(genre_conditions)

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

        if debug:
            self.stdout.write(f'🎯 Условие поиска (И): {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_genres_and_description(self, genres_str, description_text, debug=False, limit=0, offset=0,
                                             min_rating_count=0, skip_existing=True, count_only=False):
        """Загрузка игр по жанрам И тексту в описании"""
        collector = DataCollector(self.stdout, self.stderr)

        genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]

        if not genre_list:
            self.stdout.write('⚠️  Не указаны жанры')
            return []

        if debug:
            self.stdout.write(f'🔍 Поиск жанров: {", ".join(genre_list)}')
            self.stdout.write(f'🔍 Текст для поиска: "{description_text}"')

        # Получаем ID для всех жанров
        genre_ids = []
        for genre in genre_list:
            query = f'fields id,name; where name = "{genre}";'
            result = make_igdb_request('genres', query, debug=False)
            if result:
                genre_ids.append(str(result[0]['id']))
                if debug:
                    self.stdout.write(f'   ✅ Жанр "{genre}" найден: ID {result[0]["id"]}')
            else:
                if debug:
                    self.stdout.write(f'   ❌ Жанр "{genre}" не найден')

        if not genre_ids:
            self.stdout.write('❌ Не найдены указанные жанры')
            return []

        if debug:
            self.stdout.write(f'📋 Найдено ID жанров: {", ".join(genre_ids)}')

        # Формируем условие для поиска игр (логика И между жанрами)
        genre_conditions = [f'genres = ({genre_id})' for genre_id in genre_ids]
        genres_condition = ' & '.join(genre_conditions)

        # Формируем общее условие: жанры И (текст в названии ИЛИ описании)
        text_condition = f'(name ~ *"{description_text}"* | summary ~ *"{description_text}"* | storyline ~ *"{description_text}"*)'
        where_clause = f'{genres_condition} & {text_condition}'

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

        if debug:
            self.stdout.write(f'🎯 Итоговое условие поиска: {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_description(self, description_text, debug=False, limit=0, offset=0, min_rating_count=0,
                                  skip_existing=True, count_only=False):
        """Загрузка игр по тексту в описании или названии"""
        collector = DataCollector(self.stdout, self.stderr)

        if debug:
            self.stdout.write(f'🔍 Ищу игры с текстом: "{description_text}"')

        # Используем поиск по тексту
        return collector.load_games_by_search(description_text, debug, limit, offset, skip_existing, min_rating_count,
                                              count_only)

    def load_games_by_keywords(self, keywords_str, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False):
        """Загрузка игр по ключевым словам с логикой И"""
        collector = DataCollector(self.stdout, self.stderr)

        keyword_list = [k.strip() for k in keywords_str.split(',') if k.strip()]

        if not keyword_list:
            self.stdout.write('⚠️  Не указаны ключевые слова')
            return []

        if debug:
            self.stdout.write(f'🔍 Поиск ключевых слов: {", ".join(keyword_list)}')

        # Получаем ID для всех ключевых слов
        keyword_ids = []
        for keyword in keyword_list:
            query = f'fields id,name; where name = "{keyword}";'
            result = make_igdb_request('keywords', query, debug=False)
            if result:
                keyword_ids.append(str(result[0]['id']))
                if debug:
                    self.stdout.write(f'   ✅ Ключевое слово "{keyword}" найдено: ID {result[0]["id"]}')
            else:
                if debug:
                    self.stderr.write(f'   ❌ Ключевое слово "{keyword}" не найдено')

        if not keyword_ids:
            self.stdout.write('❌ Не найдены указанные ключевые слова')
            return []

        if debug:
            self.stdout.write(f'📋 Найдено ID ключевых слов: {", ".join(keyword_ids)}')

        # Формируем условие для поиска игр (логика И)
        keyword_conditions = [f'keywords = ({keyword_id})' for keyword_id in keyword_ids]
        where_clause = ' & '.join(keyword_conditions)

        if min_rating_count > 0:
            where_clause = f'{where_clause} & rating_count >= {min_rating_count}'

        if debug:
            self.stdout.write(f'🎯 Условие поиска: {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def _handle_overwrite_mode(self, all_games, debug):
        """Обрабатывает режим перезаписи"""
        self.stdout.write('🔄 РЕЖИМ ПЕРЕЗАПИСИ - найденные игры будут удалены и загружены заново!')

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

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        collector = DataCollector(self.stdout, self.stderr)
        return collector.load_all_popular_games(debug, limit, offset, min_rating_count, skip_existing, count_only)

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


class Command(BaseGamesCommand):
    """Команда для загрузки игр из IGDB"""

    help = 'Загрузка игр из IGDB с разными фильтрами'

    def handle(self, *args, **options):
        """Основной метод"""
        super().handle(*args, **options)