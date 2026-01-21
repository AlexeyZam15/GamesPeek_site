# games/management/commands/load_igdb/game_loader.py
import time
import signal
import threading
from django.utils import timezone
from games.models import Game
from games.igdb_api import make_igdb_request
from .data_collector import DataCollector
from .data_loader import DataLoader
from .relations_handler import RelationsHandler
from .statistics import Statistics
from .offset_manager import OffsetManager


class GameLoader:
    """Основной класс для выполнения команды загрузки игр"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr
        self.max_consecutive_no_new_games = 3
        self.debug_mode = False

    def _ensure_logs_directory(self, debug=False):
        """Создает папку для логов при старте команды"""
        import os
        from django.conf import settings

        try:
            project_root = settings.BASE_DIR
        except (ImportError, AttributeError):
            current_file_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file_dir)))

        log_dir = os.path.join(project_root, 'load_games_logs')

        if not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
            if debug:
                self.stdout.write(f'📁 Создана папка для логов: {log_dir}')
        else:
            if debug:
                self.stdout.write(f'📁 Папка для логов уже существует: {log_dir}')

        return log_dir

    def update_multiple_games_data(self, game_ids, debug=False):
        """Батчевое обновление данных для нескольких игр"""
        if not game_ids:
            return 0, []

        updated_count = 0
        all_update_details = []

        # ОПТИМИЗАЦИЯ 1: Получаем все игры из базы одним запросом
        games_map = {}
        existing_games = Game.objects.filter(igdb_id__in=game_ids)
        for game in existing_games:
            games_map[game.igdb_id] = game

        # ОПТИМИЗАЦИЯ 2: Батчевый запрос к IGDB API (до 500 игр за раз)
        for i in range(0, len(game_ids), 500):
            batch_ids = game_ids[i:i + 500]
            id_list = ','.join(map(str, batch_ids))

            query = f'''
                fields id,name,summary,storyline,genres,keywords,rating,rating_count,
                       first_release_date,platforms,cover,game_type,screenshots,
                       collections,franchises,involved_companies.company,
                       involved_companies.developer,involved_companies.publisher,
                       themes,player_perspectives,game_modes;
                where id = ({id_list});
            '''

            # ОДИН запрос для пачки игр вместо N запросов
            games_data = make_igdb_request('games', query, debug=False)
            if not games_data:
                continue

            # Создаем словарь данных по ID
            games_data_map = {gd['id']: gd for gd in games_data if 'id' in gd}

            # ОПТИМИЗАЦИЯ 3: Собираем все ID данных для пачки
            all_cover_ids = []
            all_genre_ids = set()
            all_platform_ids = set()
            all_keyword_ids = set()
            all_series_ids = set()
            all_company_ids = set()
            all_theme_ids = set()
            all_perspective_ids = set()
            all_mode_ids = set()
            screenshots_info = {}

            for game_data in games_data:
                game_id = game_data.get('id')
                if not game_id:
                    continue

                if game_data.get('cover'):
                    all_cover_ids.append(game_data['cover'])

                if game_data.get('genres'):
                    all_genre_ids.update(game_data['genres'])

                if game_data.get('platforms'):
                    all_platform_ids.update(game_data['platforms'])

                if game_data.get('keywords'):
                    all_keyword_ids.update(game_data['keywords'])

                if game_data.get('screenshots'):
                    screenshots_info[game_id] = len(game_data['screenshots'])

                # Дополнительные данные
                if game_data.get('collections'):
                    all_series_ids.update(game_data['collections'])
                if game_data.get('involved_companies'):
                    all_company_ids.update(
                        c.get('company') for c in game_data['involved_companies'] if c.get('company'))
                if game_data.get('themes'):
                    all_theme_ids.update(game_data['themes'])
                if game_data.get('player_perspectives'):
                    all_perspective_ids.update(game_data['player_perspectives'])
                if game_data.get('game_modes'):
                    all_mode_ids.update(game_data['game_modes'])

            # ОПТИМИЗАЦИЯ 4: Загружаем все данные пачками
            collected_data = {
                'all_cover_ids': list(set(all_cover_ids)),
                'all_genre_ids': list(all_genre_ids),
                'all_platform_ids': list(all_platform_ids),
                'all_keyword_ids': list(all_keyword_ids),
                'all_series_ids': list(all_series_ids),
                'all_company_ids': list(all_company_ids),
                'all_theme_ids': list(all_theme_ids),
                'all_perspective_ids': list(all_perspective_ids),
                'all_mode_ids': list(all_mode_ids),
                'screenshots_info': screenshots_info,
                'game_data_map': games_data_map,
            }

            # Загружаем все типы данных одним вызовом
            data_loader = DataLoader(self.stdout, self.stderr)
            data_maps, step_times = data_loader.load_all_data_types_sequentially(
                collected_data, debug
            )

            # ОПТИМИЗАЦИЯ 5: Обрабатываем все игры пачки
            for game_id in batch_ids:
                if game_id not in games_map or game_id not in games_data_map:
                    continue

                game = games_map[game_id]
                game_data = games_data_map[game_id]

                # Проверяем что нужно обновить
                success, details = self._update_single_game_with_existing_data(
                    game, game_data, data_maps, collected_data, debug
                )

                if success:
                    updated_count += 1
                    all_update_details.append({
                        'game_name': game.name,
                        'game_id': game_id,
                        'details': details
                    })

        return updated_count, all_update_details

    def _update_single_game_with_existing_data(self, game, game_data, data_maps, collected_data, debug):
        """Обновляет одну игру используя уже загруженные данные"""
        details = {
            'updated_fields': [],
            'updated_relations': [],
            'screenshots_added': 0,
            'still_missing': [],
        }

        try:
            # Проверяем недостающие данные
            missing_data, missing_count = self.check_missing_game_data(game)

            if missing_count == 0:
                return True, details

            # 1. Обновляем обложку если отсутствует
            if not missing_data['has_cover'] and game_data.get('cover'):
                cover_id = game_data['cover']
                if cover_id in data_maps.get('cover_map', {}):
                    new_cover_url = data_maps['cover_map'][cover_id]
                    if game.cover_url != new_cover_url:
                        game.cover_url = new_cover_url
                        details['updated_fields'].append('cover_url')

            # 2. Обновляем описание если отсутствует
            if not missing_data['has_description'] and game_data.get('summary'):
                if not game.summary or not game.summary.strip():
                    game.summary = game_data.get('summary', '')
                    details['updated_fields'].append('summary')

            # 3. Обновляем рейтинг если отсутствует
            if not missing_data['has_rating'] and 'rating' in game_data:
                if game.rating != game_data.get('rating'):
                    game.rating = game_data.get('rating')
                    details['updated_fields'].append('rating')

            # 4. Обновляем дату релиза если отсутствует
            if not missing_data['has_release_date'] and game_data.get('first_release_date'):
                from datetime import datetime
                from django.utils import timezone
                naive_datetime = datetime.fromtimestamp(game_data['first_release_date'])
                new_date = timezone.make_aware(naive_datetime)
                if game.first_release_date != new_date:
                    game.first_release_date = new_date
                    details['updated_fields'].append('first_release_date')

            # Сохраняем обновленные поля
            if details['updated_fields']:
                game.save(update_fields=details['updated_fields'])

            # 5. Загружаем скриншоты если отсутствуют
            if not missing_data['has_screenshots'] and collected_data['screenshots_info'].get(game.igdb_id, 0) > 0:
                screenshots_info = collected_data['screenshots_info']
                data_loader = DataLoader(self.stdout, self.stderr)
                screenshots_loaded = data_loader.load_screenshots_parallel(
                    [game.igdb_id], collected_data['game_data_map'],
                    screenshots_info, debug
                )
                if screenshots_loaded > 0:
                    details['screenshots_added'] = screenshots_loaded

            return True, details

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка обновления игры {game.igdb_id}: {e}')
            return False, details

    def update_all_games_missing_data(self, options, debug=False):
        """Обновляет недостающие данные для всех игр в базе"""
        from games.models import Game
        import time

        # Создаем папку для логов
        log_dir = self._ensure_logs_directory(debug)
        if debug:
            self.stdout.write(f'📁 Логи будут сохраняться в: {log_dir}')

        # Получаем все игры из базы
        all_games = Game.objects.all().order_by('-rating_count')
        total_games = all_games.count()

        if total_games == 0:
            self.stdout.write('❌ В базе нет игр для обновления')
            return 0, []

        # Создаем прогресс-бар
        progress_bar = self._create_progress_bar()
        progress_bar.total_games = total_games

        # Инициализируем статистику
        updated_count = 0
        failed_count = 0
        skipped_count = 0
        update_details = []
        start_time = time.time()

        for i, game in enumerate(all_games, 1):
            if debug:
                self.stdout.write(f'\n   🔄 [{i}/{total_games}] Проверка: {game.name} (ID: {game.igdb_id})')

            # Проверяем, какие данные отсутствуют
            missing_data, missing_count = self.check_missing_game_data(game)

            if missing_count == 0:
                skipped_count += 1
                if debug:
                    self.stdout.write(f'   ✅ Все данные уже есть')
                # Обновляем прогресс-бар
                progress_bar.update(
                    total_loaded=i,
                    updated_count=updated_count,
                    failed_count=failed_count,
                    skipped_count=skipped_count
                )
                continue

            if debug:
                self.stdout.write(f'   ⚠️  Отсутствует данных: {missing_count}')

            # Обновляем недостающие данные
            success, details = self.update_missing_game_data(game.igdb_id, debug)

            if success:
                # Проверяем, были ли реальные обновления
                has_updates = (
                        len(details['updated_fields']) > 0 or
                        len(details['updated_relations']) > 0 or
                        details['screenshots_added'] > 0
                )

                if has_updates:
                    updated_count += 1
                    update_details.append({
                        'game_name': game.name,
                        'game_id': game.igdb_id,
                        'details': details
                    })

                    # Сохраняем лог об обновлении только если есть реальные обновления
                    self._save_update_log_immediately(game, details, missing_data, missing_count, debug)

                    if debug:
                        self.stdout.write(
                            f'   ✅ Успешно обновлено с {len(details["updated_fields"])} полей, {len(details["updated_relations"])} связей')
                else:
                    # Игра проверена, но обновлений не требовалось
                    skipped_count += 1
                    if debug:
                        self.stdout.write(f'   ℹ️  Проверена, обновлений не требуется')
            else:
                failed_count += 1
                if debug:
                    self.stdout.write(f'   ❌ Не удалось обновить')

            # Обновляем прогресс-бар каждые 10 игр или после каждой игры в debug
            if debug or i % 10 == 0:
                progress_bar.update(
                    total_loaded=i,
                    updated_count=updated_count,
                    failed_count=failed_count,
                    skipped_count=skipped_count
                )

            # Пауза между играми чтобы не перегружать API
            if i < total_games:
                time.sleep(0.5)

        total_time = time.time() - start_time

        # Финальное обновление прогресс-бара
        progress_bar.update(
            total_loaded=total_games,
            updated_count=updated_count,
            failed_count=failed_count,
            skipped_count=skipped_count
        )

        # Финальное сообщение прогресс-бара
        progress_bar.final_message(
            f"✅ Обновлено: {updated_count} | ❌ Ошибок: {failed_count} | ⏭️  Пропущено: {skipped_count}"
        )

        # Очищаем прогресс-бар
        progress_bar.clear()

        # Выводим итоговую статистику
        self.stdout.write(f'\n' + '=' * 60)
        self.stdout.write(f'📊 ИТОГОВАЯ СТАТИСТИКА ОБНОВЛЕНИЯ ВСЕХ ИГР')
        self.stdout.write('=' * 60)
        self.stdout.write(f'🎮 Всего проверено игр: {total_games}')
        self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
        self.stdout.write(f'❌ Не удалось обновить: {failed_count}')
        self.stdout.write(f'⏭️  Пропущено (все данные есть): {skipped_count}')
        self.stdout.write(f'⏱️  Общее время: {total_time:.2f}с')

        if total_time > 0:
            games_per_second = total_games / total_time
            self.stdout.write(f'🚀 Скорость: {games_per_second:.1f} игр/сек')

        if update_details:
            self.stdout.write(f'\n🎮 ОБНОВЛЕННЫЕ ИГРЫ:')
            self.stdout.write('─' * 40)

            # Показываем только первые 10 игр в деталях
            for detail in update_details[:10]:
                game_name = detail['game_name']
                details = detail['details']

                updates = []
                if details.get('updated_fields'):
                    updates.append(f'поля: {", ".join(details["updated_fields"])}')
                if details.get('updated_relations'):
                    updates.append(f'связи: {", ".join(details["updated_relations"])}')
                if details.get('screenshots_added', 0) > 0:
                    updates.append(f'скриншотов: {details["screenshots_added"]}')

                if updates:
                    self.stdout.write(f'🎯 {game_name}: {", ".join(updates)}')

            if len(update_details) > 10:
                self.stdout.write(f'📊 ... и ещё {len(update_details) - 10} игр')

        return updated_count, update_details

    def load_games_by_names(self, game_names_str, debug=False, limit=0, offset=0, min_rating_count=0,
                            skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка САМОЙ ПОПУЛЯРНОЙ игры по точному названию"""
        collector = DataCollector(self.stdout, self.stderr)

        # Для режима game-names используем лимит 1 - только самую популярную игру
        effective_limit = 1

        # ПЕРЕДАЕМ skip_existing=false если режим update-missing-data
        # чтобы получить ВСЕ найденные игры
        skip_for_update = skip_existing

        # Если это режим обновления, не пропускаем существующие игры
        if hasattr(self, 'current_options') and self.current_options.get('update_missing_data'):
            skip_for_update = False
            if debug:
                self.stdout.write(f'   🔄 РЕЖИМ ОБНОВЛЕНИЯ: не пропускаем существующие игры')

        return collector.load_games_by_names(
            game_names_str, debug, effective_limit, offset, min_rating_count,
            skip_for_update, count_only, game_types_str
        )

    def _process_standard_game_data(self, result, params, iteration_start_time, errors):
        """Обрабатывает стандартную загрузку игр (не update-missing-data)"""
        # Обработка режима перезаписи
        if params['overwrite'] and result['new_games']:
            try:
                self._handle_overwrite_mode(result['new_games'], params['debug'])
            except Exception as e:
                errors += 1
                self.stderr.write(f'❌ ОШИБКА при удалении игр: {str(e)}')
                if params['debug']:
                    import traceback
                    self.stderr.write(f'📋 Трассировка ошибки:')
                    self.stderr.write(traceback.format_exc())

        # Обработка режима только подсчета
        if params['count_only']:
            iteration_time = time.time() - iteration_start_time

            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('✅ ПОДСЧЕТ ЗАВЕРШЕН!')
            self.stdout.write(f'🎮 Игр можно загрузить (которых нет в базе): {result["new_games_count"]}')

            if errors > 0:
                self.stdout.write(f'❌ Ошибок при подсчете: {errors}')

            return {
                'total_games_found': result['new_games_count'],
                'total_games_checked': result['total_games_checked'],
                'created_count': 0,
                'skipped_count': result['existing_games_skipped'],
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', 0),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        # Обработка данных (стандартная загрузка)
        result_stats = None
        try:
            # Создаем экземпляры классов для обработки данных
            collector = DataCollector(self.stdout, self.stderr)
            loader = DataLoader(self.stdout, self.stderr)
            handler = RelationsHandler(self.stdout, self.stderr)
            stats = Statistics(self.stdout, self.stderr)

            # Устанавливаем обработчик прерывания
            interrupted = threading.Event()

            def signal_handler(sig, frame):
                interrupted.set()
                loader.set_interrupted()
                if params['debug']:
                    self.stdout.write('\n   ⏹️  Получен сигнал прерывания в обработке данных')

            original_sigint = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, signal_handler)

            try:
                # Шаг 1: Собираем все ID данных
                if params['debug']:
                    self.stdout.write('\n📊 СБОР ВСЕХ ID ДАННЫХ...')

                collected_data = collector.collect_all_data_ids(result['new_games'], params['debug'])

                # Проверка прерывания
                if interrupted.is_set():
                    self.stdout.write('   ⏹️  Прерывание: пропускаем создание игр')
                    raise KeyboardInterrupt()

                # Шаг 2: Создаем основные объекты игр
                if params['debug']:
                    self.stdout.write('\n🎮 СОЗДАНИЕ ОСНОВНЫХ ОБЪЕКТОВ ИГР...')

                created_count, game_basic_map, skipped_games = loader.create_basic_games(
                    result['new_games'], params['debug']
                )

                # Если не создано игр, возвращаем нулевую статистику
                if created_count == 0:
                    signal.signal(signal.SIGINT, original_sigint)
                    return {
                        'created_count': 0,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                # Проверка прерывания
                if interrupted.is_set():
                    self.stdout.write('   ⏹️  Прерывание: пропускаем загрузку данных')
                    signal.signal(signal.SIGINT, original_sigint)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                # Шаг 3: Загружаем все остальные данные
                if params['debug']:
                    self.stdout.write('\n📥 ЗАГРУЗКА ВСЕХ ДАННЫХ...')

                # Получаем информацию о скриншотах из collected_data
                screenshots_info = collected_data.get('screenshots_info', {})

                # Загружаем скриншоты ПЕРЕД дополнительными данными
                screenshots_loaded = 0
                if 'all_screenshot_games' in collected_data and screenshots_info:
                    if params['debug']:
                        self.stdout.write(f'\n📸 ЗАГРУЗКА СКРИНШОТОВ...')
                        self.stdout.write(f'   📊 Информация о скриншотах: {screenshots_info}')

                    screenshots_loaded = loader.load_screenshots_parallel(
                        collected_data['all_screenshot_games'],
                        collected_data['game_data_map'],
                        screenshots_info,
                        params['debug']
                    )

                    if params['debug']:
                        self.stdout.write(f'   ✅ Загружено скриншотов: {screenshots_loaded}')

                # Загружаем дополнительные данные
                additional_data_map, additional_ids = loader.load_and_process_additional_data(
                    list(game_basic_map.keys()),
                    collected_data['game_data_map'],
                    screenshots_info,  # Передаем информацию о скриншотах
                    params['debug']
                )

                # Обновляем collected_data с дополнительными ID
                collected_data.update(additional_ids)

                # Шаг 4: Загружаем все типы данных
                data_maps, step_times = loader.load_all_data_types_sequentially(
                    collected_data, params['debug']
                )

                # Проверка прерывания
                if interrupted.is_set():
                    self.stdout.write('   ⏹️  Прерывание: пропускаем создание связей')
                    signal.signal(signal.SIGINT, original_sigint)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                        'screenshots_loaded': screenshots_loaded,
                    }, errors

                # Шаг 5: Обновляем игры с обложками
                cover_updates = loader.update_games_with_covers(
                    game_basic_map, data_maps.get('cover_map', {}),
                    collected_data['game_data_map'], params['debug']
                )

                # Проверка прерывания
                if interrupted.is_set():
                    self.stdout.write('   ⏹️  Прерывание: пропускаем создание связей')
                    signal.signal(signal.SIGINT, original_sigint)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                        'screenshots_loaded': screenshots_loaded,
                    }, errors

                # Шаг 6: Подготавливаем и создаем связи
                all_game_relations, relations_prep_time = handler.prepare_game_relations(
                    game_basic_map, collected_data['game_data_map'],
                    additional_data_map, data_maps, params['debug']
                )

                # Создаем все связи
                relations_results, relations_possible, relations_time = handler.create_all_relations(
                    all_game_relations, params['debug']
                )

                # Шаг 7: Собираем полную статистику
                loaded_data_stats = {
                    'collected': collected_data,
                    'loaded': {k: len(v) for k, v in data_maps.items()}
                }

                step_times['relations_preparation'] = relations_prep_time
                step_times['relations_creation'] = relations_time

                # Собираем статистику объектов
                objects_stats = stats._collect_objects_statistics(
                    game_basic_map, data_maps, loaded_data_stats, params['debug']
                )

                # Добавляем статистику пропущенных игр
                objects_stats['games']['skipped'] = skipped_games

                # Добавляем статистику скриншотов
                objects_stats['screenshots']['created'] = screenshots_loaded

                # Собираем статистику связей
                relations_stats = stats._collect_relations_statistics(
                    all_game_relations, relations_results, params['debug']
                )

                # Выводим детальную статистику
                stats._print_detailed_statistics(
                    objects_stats, relations_stats,
                    time.time() - iteration_start_time,
                    params['debug']
                )

                # Собираем финальную статистику
                final_stats = stats._collect_final_statistics(
                    result['new_games_count'], created_count, skipped_games, screenshots_loaded,
                    time.time() - iteration_start_time, loaded_data_stats, step_times,
                    relations_results, relations_possible, params['debug']
                )

                # Добавляем детальную статистику в final_stats
                final_stats['objects_detailed'] = objects_stats
                final_stats['relations_detailed'] = relations_stats

                # Выводим статистику
                stats._print_complete_statistics(final_stats)

                result_stats = {
                    'created_count': created_count,
                    'skipped_count': skipped_games,
                    'total_time': time.time() - iteration_start_time,
                    'screenshots_loaded': screenshots_loaded,
                    'relations_created': sum(relations_results.values()) if relations_results else 0,
                    'objects_stats': objects_stats,
                    'relations_stats': relations_stats,
                }

            finally:
                # Восстанавливаем оригинальный обработчик сигнала
                signal.signal(signal.SIGINT, original_sigint)

        except KeyboardInterrupt:
            # Обработка прерывания в обработке данных
            self.stdout.write('\n   ⏹️  Прерывание в обработке данных')
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }
            errors += 1
        except Exception as e:
            errors += 1
            self.stderr.write(f'❌ ОШИБКА при обработке данных: {str(e)}')
            if params['debug']:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }

        return result_stats, errors

    from django.db import transaction

    def _update_existing_game_data(self, result, params, iteration_start_time, errors):
        """Обновляет данные существующих игр"""
        try:
            updated_count = 0
            failed_count = 0
            update_details = []

            games_to_process = []
            if result.get('all_found_games'):
                games_to_process = result['all_found_games']
            elif result.get('new_games'):
                games_to_process = result['new_games']

            if params['debug']:
                self.stdout.write(f'\n🔄 ОБНОВЛЕНИЕ ДАННЫХ ДЛЯ СУЩЕСТВУЮЩИХ ИГР')
                self.stdout.write(f'   • Всего найдено игр для обработки: {len(games_to_process)}')

            for i, game_data in enumerate(games_to_process, 1):
                game_id = game_data.get('id')
                game_name = game_data.get('name', f'ID {game_id}')

                if params['debug']:
                    self.stdout.write(f'\n   🔄 [{i}/{len(games_to_process)}] Обновление: {game_name}')

                if game_id:
                    success, details = self.update_missing_game_data(game_id, params['debug'])
                    if success:
                        updated_count += 1
                        update_details.append({
                            'game_name': game_name,
                            'game_id': game_id,
                            'details': details
                        })
                        if params['debug']:
                            self.stdout.write(f'   ✅ Успешно обновлена: {game_name}')
                    else:
                        failed_count += 1
                        if params['debug']:
                            self.stdout.write(f'   ❌ Не удалось обновить: {game_name}')
                else:
                    failed_count += 1
                    if params['debug']:
                        self.stdout.write(f'   ❌ Нет ID у игры: {game_name}')

            iteration_time = time.time() - iteration_start_time

            # Сохраняем пакетный лог
            self._log_batch_update(update_details, len(games_to_process), updated_count,
                                   failed_count, iteration_start_time, time.time(), params['debug'])

            # ВЫВОД ФИНАЛЬНОЙ СТАТИСТИКИ
            self.stdout.write(f'\n' + '=' * 60)
            self.stdout.write(f'📊 ФИНАЛЬНАЯ СТАТИСТИКА ОБНОВЛЕНИЯ ДАННЫХ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🔄 ОБРАБОТАНО ИГР: {len(games_to_process)}')
            self.stdout.write(f'✅ УСПЕШНО ОБНОВЛЕНО: {updated_count}')
            self.stdout.write(f'❌ НЕ УДАЛОСЬ ОБНОВИТЬ: {failed_count}')
            self.stdout.write(f'⏱️  ВРЕМЯ: {iteration_time:.2f}с')

            if iteration_time > 0:
                speed = len(games_to_process) / iteration_time
                self.stdout.write(f'🚀 Скорость: {speed:.1f} игр/сек')

            return {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': len(games_to_process),
                'created_count': 0,
                'skipped_count': result.get('existing_games_skipped', 0),
                'updated_count': updated_count,
                'update_details': update_details,
                'total_time': iteration_time,
                'errors': errors + failed_count,
                'last_checked_offset': result.get('last_checked_offset', 0),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        except Exception as e:
            errors += 1
            self.stderr.write(f'❌ ОШИБКА при обновлении данных: {str(e)}')
            if params['debug']:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())

            iteration_time = time.time() - iteration_start_time
            return {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': result.get('new_games_count', 0),
                'created_count': 0,
                'skipped_count': result.get('existing_games_skipped', 0),
                'updated_count': 0,
                'update_details': [],
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', 0),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

    def update_missing_game_data(self, game_id, debug=False):
        """Обновляет недостающие данные для конкретной игры"""
        from games.models import Game
        from games.igdb_api import make_igdb_request
        import os
        import json
        from datetime import datetime

        details = {
            'updated_fields': [],
            'updated_relations': [],
            'screenshots_added': 0,
            'still_missing': [],
            'cover_url': None,
            'summary': None,
            'rating': None,
            'release_date': None,
            'game_name': None,
            'timestamp': None
        }

        try:
            # Получаем игру из базы
            game = Game.objects.filter(igdb_id=game_id).first()
            if not game:
                if debug:
                    self.stdout.write(f'   ❌ Игра с ID {game_id} не найдена в базе')
                # СОЗДАЕМ ФАЙЛ ОБ ОШИБКЕ ДАЖЕ ЕСЛИ ИГРЫ НЕТ
                self._create_debug_file(f"game_not_found_{game_id}", {"error": "Game not found in database"})
                return False, details

            details['game_name'] = game.name
            details['timestamp'] = datetime.now().isoformat()

            # ДЕБАГ: создаем файл сразу при начале обработки
            if debug:
                self._create_debug_file(f"start_processing_{game_id}", {
                    "game_id": game_id,
                    "game_name": game.name,
                    "start_time": datetime.now().isoformat()
                })

            if debug:
                self.stdout.write(f'\n   🔍 ПРОВЕРКА НЕДОСТАЮЩИХ ДАННЫХ ДЛЯ: {game.name} (ID: {game_id})')

            # Проверяем, каких данных не хватает
            missing_data, missing_count = self.check_missing_game_data(game)

            if debug:
                self.stdout.write(f'   📊 СТАТУС ДАННЫХ:')
                for key, value in missing_data.items():
                    status = "✅ ЕСТЬ" if value else "❌ ОТСУТСТВУЕТ"
                    self.stdout.write(f'      • {key}: {status}')

            # ДЕБАГ: сохраняем статус данных
            self._create_debug_file(f"missing_data_{game_id}", {
                "game_id": game_id,
                "missing_data": missing_data,
                "missing_count": missing_count
            })

            if missing_count == 0:
                if debug:
                    self.stdout.write(f'   ✅ У игры "{game.name}" все данные уже есть')
                return True, details

            if debug:
                self.stdout.write(f'   📊 Недостающих данных: {missing_count} из {len(missing_data)}')

            # Загружаем данные игры из IGDB
            query = f'''
                fields id,name,summary,storyline,genres,keywords,rating,rating_count,
                       first_release_date,platforms,cover,game_type,screenshots,
                       collections,franchises,involved_companies.company,
                       involved_companies.developer,involved_companies.publisher,
                       themes,player_perspectives,game_modes;
                where id = {game_id};
            '''

            games_data = make_igdb_request('games', query, debug=False)
            if not games_data:
                if debug:
                    self.stdout.write(f'   ❌ Данные игры {game_id} не найдены в IGDB')
                self._create_debug_file(f"igdb_not_found_{game_id}", {"error": "Game not found in IGDB"})
                return False, details

            game_data = games_data[0]

            if debug:
                self.stdout.write(f'   📥 ДАННЫЕ ИЗ IGDB:')
                self.stdout.write(f'      • ID обложки в IGDB: {game_data.get("cover")}')
                self.stdout.write(f'      • Скриншотов в IGDB: {len(game_data.get("screenshots", []))}')
                self.stdout.write(f'      • Жанров в IGDB: {len(game_data.get("genres", []))}')
                self.stdout.write(f'      • Платформ в IGDB: {len(game_data.get("platforms", []))}')

            # ДЕБАГ: сохраняем данные из IGDB
            self._create_debug_file(f"igdb_data_{game_id}", {
                "game_id": game_id,
                "igdb_cover": game_data.get("cover"),
                "igdb_screenshots": len(game_data.get("screenshots", [])),
                "igdb_genres": len(game_data.get("genres", [])),
                "igdb_platforms": len(game_data.get("platforms", []))
            })

            # Создаем экземпляры для обработки данных
            collector = DataCollector(self.stdout, self.stderr)
            loader = DataLoader(self.stdout, self.stderr)
            handler = RelationsHandler(self.stdout, self.stderr)

            # Собираем все ID данных
            collected_data = collector.collect_all_data_ids([game_data], debug)

            # Загружаем все типы данных
            data_maps, step_times = loader.load_all_data_types_sequentially(
                collected_data, debug
            )

            # 1. Обновляем обложку если отсутствует
            if not missing_data['has_cover'] and game_data.get('cover'):
                cover_id = game_data['cover']
                if cover_id in data_maps.get('cover_map', {}):
                    new_cover_url = data_maps['cover_map'][cover_id]
                    if game.cover_url != new_cover_url:
                        game.cover_url = new_cover_url
                        details['updated_fields'].append('cover_url')
                        details['cover_url'] = new_cover_url
                        if debug:
                            self.stdout.write(f'   🖼️  Обновлена обложка: {new_cover_url}')

            # 2. Обновляем описание если отсутствует
            if not missing_data['has_description'] and game_data.get('summary'):
                if not game.summary or not game.summary.strip():
                    game.summary = game_data.get('summary', '')
                    details['updated_fields'].append('summary')
                    details['summary'] = game.summary
                    if debug:
                        self.stdout.write(f'   📝 Обновлено описание ({len(game.summary)} симв.)')

            # 3. Обновляем рейтинг если отсутствует
            if not missing_data['has_rating'] and 'rating' in game_data:
                if game.rating != game_data.get('rating'):
                    game.rating = game_data.get('rating')
                    details['updated_fields'].append('rating')
                    details['rating'] = game.rating
                    if debug:
                        self.stdout.write(f'   ⭐ Обновлен рейтинг: {game.rating}')

            # 4. Обновляем дату релиза если отсутствует
            if not missing_data['has_release_date'] and game_data.get('first_release_date'):
                from datetime import datetime as dt
                from django.utils import timezone
                naive_datetime = dt.fromtimestamp(game_data['first_release_date'])
                new_date = timezone.make_aware(naive_datetime)
                if game.first_release_date != new_date:
                    game.first_release_date = new_date
                    details['updated_fields'].append('first_release_date')
                    details['release_date'] = new_date
                    if debug:
                        self.stdout.write(f'   📅 Обновлена дата релиза: {new_date}')

            # ДЕБАГ: сохраняем что планируем обновить
            self._create_debug_file(f"planned_updates_{game_id}", {
                "game_id": game_id,
                "planned_fields": details['updated_fields'],
                "has_updates": len(details['updated_fields']) > 0
            })

            # 5. Сохраняем обновленные поля игры
            if details['updated_fields']:
                # ШАГ 1: Сохраняем в базу
                game.save(update_fields=details['updated_fields'])

                # ДЕБАГ: создаем файл подтверждения сохранения
                self._create_debug_file(f"saved_to_db_{game_id}", {
                    "game_id": game_id,
                    "saved_fields": details['updated_fields'],
                    "timestamp": datetime.now().isoformat()
                })

                # ШАГ 2: Создаем файл лога (ПОСЛЕ сохранения, ДО прогресс-бара)
                self._save_update_log_immediately(game, details, missing_data, missing_count, debug)

                if debug:
                    self.stdout.write(f'   💾 Сохранены обновленные поля: {", ".join(details["updated_fields"])}')

            # 6. Загружаем скриншоты если отсутствуют
            if not missing_data['has_screenshots'] and game_data.get('screenshots'):
                screenshots_info = collected_data.get('screenshots_info', {})
                screenshots_loaded = loader.load_screenshots_parallel(
                    [game_id], collected_data['game_data_map'],
                    screenshots_info, debug
                )
                if screenshots_loaded > 0:
                    details['screenshots_added'] = screenshots_loaded
                    if debug:
                        self.stdout.write(f'   📸 Загружено скриншотов: {screenshots_loaded}')

            # 7. Подготавливаем связи для обновления
            game_basic_map = {game_id: game}
            additional_data_map = {game_id: game_data}

            all_game_relations, relations_prep_time = handler.prepare_game_relations(
                game_basic_map, collected_data['game_data_map'],
                additional_data_map, data_maps, debug
            )

            # 8. Создаем только недостающие связи
            if all_game_relations:
                # Жанры
                if not missing_data['has_genres'] and game_data.get('genres'):
                    genre_count, platform_count, keyword_count = handler.create_relations_batch(
                        all_game_relations, debug
                    )
                    if genre_count > 0:
                        details['updated_relations'].append(f'жанры ({genre_count})')
                    if platform_count > 0:
                        details['updated_relations'].append(f'платформы ({platform_count})')
                    if keyword_count > 0:
                        details['updated_relations'].append(f'ключевые слова ({keyword_count})')

                # Дополнительные связи
                additional_results = handler.create_all_additional_relations(
                    all_game_relations, debug
                )

                # Добавляем информацию о дополнительных связях
                for rel_type, count in additional_results.items():
                    if count > 0:
                        rel_name = rel_type.replace('_relations', '').replace('_', ' ')
                        details['updated_relations'].append(f'{rel_name} ({count})')

            # 9. Проверяем что осталось отсутствующим
            new_missing_data, new_missing_count = self.check_missing_game_data(game)
            for data_type, has_data in new_missing_data.items():
                if not has_data:
                    details['still_missing'].append(data_type.replace('has_', ''))

            if debug:
                self.stdout.write(f'   ✅ Обновление завершено для игры "{game.name}"')

                if details['updated_fields'] or details['updated_relations'] or details['screenshots_added'] > 0:
                    self.stdout.write(f'   📈 ОБНОВЛЕНО:')
                    if details['updated_fields']:
                        self.stdout.write(f'      • Поля: {", ".join(details["updated_fields"])}')
                    if details['updated_relations']:
                        self.stdout.write(f'      • Связи: {", ".join(details["updated_relations"])}')
                    if details['screenshots_added'] > 0:
                        self.stdout.write(f'      • Скриншотов: {details["screenshots_added"]}')

                if new_missing_count < missing_count:
                    self.stdout.write(f'   📊 УЛУЧШЕНИЕ: было {missing_count} недостающих → стало {new_missing_count}')
                else:
                    self.stdout.write(f'   ⚠️  Недостающих данных осталось: {new_missing_count}')

            # ДЕБАГ: финальный файл
            self._create_debug_file(f"final_{game_id}", {
                "game_id": game_id,
                "success": True,
                "updated_fields": details['updated_fields'],
                "updated_relations": details['updated_relations'],
                "screenshots_added": details['screenshots_added']
            })

            return True, details

        except Exception as e:
            # ДЕБАГ: файл об ошибке
            self._create_debug_file(f"error_{game_id}", {
                "game_id": game_id,
                "error": str(e),
                "error_type": type(e).__name__,
                "timestamp": datetime.now().isoformat()
            })

            if debug:
                self.stderr.write(f'   ❌ Ошибка при обновлении игры {game_id}: {str(e)}')
                import traceback
                self.stderr.write(f'   📋 Трассировка: {traceback.format_exc()}')
            return False, details

    def _create_debug_file(self, filename, data):
        """Создает простой отладочный файл"""
        import os
        import json
        from datetime import datetime

        try:
            # Просто в текущей директории
            debug_dir = 'debug_logs'
            os.makedirs(debug_dir, exist_ok=True)

            filepath = os.path.join(debug_dir, f"{filename}.json")
            data['debug_timestamp'] = datetime.now().isoformat()

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
        except:
            pass  # Игнорируем ошибки создания дебаг файлов

    def _save_update_log_immediately(self, game, details, missing_data, missing_count_before, debug=False):
        """СОХРАНЯЕТ ФАЙЛ ЛОГА сразу после сохранения игры"""
        import os
        import json
        from datetime import datetime

        try:
            # Способ 1: Используем BASE_DIR из настроек Django
            try:
                from django.conf import settings
                project_root = settings.BASE_DIR
            except (ImportError, AttributeError):
                # Способ 2: Определяем путь от текущего файла
                current_file_dir = os.path.dirname(os.path.abspath(__file__))
                project_root = os.path.dirname(os.path.dirname(os.path.dirname(current_file_dir)))

            log_dir = os.path.join(project_root, 'load_games_logs')

            # Выводим через stderr, чтобы не мешать прогресс-бару
            if debug:
                self.stderr.write(f'\n   📁 Создаем папку: {log_dir}')

            os.makedirs(log_dir, exist_ok=True)

            if debug:
                self.stderr.write(f'   ✅ Папка создана/существует')

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            detail_file = os.path.join(log_dir, f'game_update_{game.igdb_id}_{timestamp}.json')

            if debug:
                self.stderr.write(f'   📄 Создаем файл: {detail_file}')

            log_data = {
                'game_id': game.igdb_id,
                'game_name': game.name,
                'update_time': datetime.now().isoformat(),
                'updated_fields': details['updated_fields'],
                'updated_relations': details['updated_relations'],
                'screenshots_added': details['screenshots_added'],
                'missing_data_before': missing_data,
                'missing_count_before': missing_count_before,
                'field_details': {
                    'cover_url': details.get('cover_url'),
                    'summary_length': len(details.get('summary', '')) if details.get('summary') else 0,
                    'rating': details.get('rating'),
                    'release_date': details.get('release_date'),
                },
                'still_missing': details['still_missing']
            }

            with open(detail_file, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)

            if debug:
                self.stderr.write(f'   ✅ Файл создан, размер: {os.path.getsize(detail_file)} байт')

            today = datetime.now().strftime("%Y%m%d")
            log_file = os.path.join(log_dir, f'updates_{today}.log')

            with open(log_file, 'a', encoding='utf-8') as f:
                timestamp_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                updates = []
                if details['updated_fields']:
                    updates.append(f"поля: {', '.join(details['updated_fields'])}")
                if details['updated_relations']:
                    updates.append(f"связи: {', '.join(details['updated_relations'])}")
                if details['screenshots_added'] > 0:
                    updates.append(f"скриншотов: {details['screenshots_added']}")

                log_entry = f"[{timestamp_str}] {game.name} (ID: {game.igdb_id}): {', '.join(updates) if updates else 'нет обновлений'}\n"
                f.write(log_entry)

            if debug:
                self.stderr.write(f'   📝 Добавлено в общий лог')

        except Exception as e:
            if debug:
                self.stderr.write(f'\n   ⚠️  Не удалось сохранить лог: {type(e).__name__}: {e}')
                import traceback
                self.stderr.write(f'   📋 Трассировка: {traceback.format_exc()[:500]}...')

    def _log_batch_update(self, update_details, total_games, updated_count, failed_count,
                          start_time, end_time, debug=False):
        """Сохраняет лог пакетного обновления"""
        import os
        import json
        from datetime import datetime

        # Создаем папку load_games_logs в корне проекта
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        log_dir = os.path.join(project_root, 'load_games_logs')
        os.makedirs(log_dir, exist_ok=True)

        # Файл для пакетного лога
        today = datetime.now().strftime("%Y%m%d_%H%M%S")
        batch_file = os.path.join(log_dir, f'batch_update_{today}.json')

        batch_data = {
            'batch_info': {
                'total_games': total_games,
                'updated_count': updated_count,
                'failed_count': failed_count,
                'success_rate': (updated_count / total_games * 100) if total_games > 0 else 0,
                'start_time': datetime.fromtimestamp(start_time).isoformat(),
                'end_time': datetime.fromtimestamp(end_time).isoformat(),
                'duration_seconds': end_time - start_time,
            },
            'updated_games': [
                {
                    'game_id': detail['game_id'],
                    'game_name': detail['game_name'],
                    'details': detail['details']
                }
                for detail in update_details
            ],
            'summary': {
                'by_field': self._summarize_updates_by_field(update_details),
                'by_game': len(update_details)
            }
        }

        try:
            with open(batch_file, 'w', encoding='utf-8') as f:
                json.dump(batch_data, f, indent=2, ensure_ascii=False, default=str)

            if debug:
                self.stdout.write(f'\n📁 Пакетный лог сохранен в: {batch_file}')

        except Exception as e:
            if debug:
                self.stderr.write(f'   ⚠️  Не удалось сохранить пакетный лог: {e}')

    def _summarize_updates_by_field(self, update_details):
        """Суммирует обновления по типам полей"""
        summary = {
            'cover_url': 0,
            'summary': 0,
            'rating': 0,
            'first_release_date': 0,
            'screenshots': 0,
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

        for detail in update_details:
            details = detail['details']

            for field in details.get('updated_fields', []):
                if field in summary:
                    summary[field] += 1

            for relation in details.get('updated_relations', []):
                # Извлекаем тип связи из строки "жанры (3)"
                rel_type = relation.split(' ')[0]
                if rel_type in ['жанры', 'genres']:
                    summary['genres'] += 1
                elif rel_type in ['платформы', 'platforms']:
                    summary['platforms'] += 1
                elif rel_type in ['ключевые', 'keywords']:
                    summary['keywords'] += 1
                elif rel_type in ['серии', 'series']:
                    summary['series'] += 1
                elif rel_type in ['разработчики', 'developers']:
                    summary['developers'] += 1
                elif rel_type in ['издатели', 'publishers']:
                    summary['publishers'] += 1
                elif rel_type in ['темы', 'themes']:
                    summary['themes'] += 1
                elif rel_type in ['перспективы', 'perspectives']:
                    summary['perspectives'] += 1
                elif rel_type in ['режимы', 'modes']:
                    summary['modes'] += 1

            if details.get('screenshots_added', 0) > 0:
                summary['screenshots'] += 1

        return summary

    def check_missing_game_data(self, game_obj):
        """Проверяет, каких данных не хватает у игры"""
        missing_data = {
            'has_cover': bool(game_obj.cover_url and game_obj.cover_url.strip()),
            'has_screenshots': game_obj.screenshots.exists(),
            'has_genres': game_obj.genres.exists(),
            'has_platforms': game_obj.platforms.exists(),
            'has_keywords': game_obj.keywords.exists(),
            'has_description': bool(game_obj.summary and game_obj.summary.strip()),
            'has_rating': game_obj.rating is not None,
            'has_release_date': game_obj.first_release_date is not None,
            'has_series': game_obj.series.exists(),
            'has_developers': game_obj.developers.exists(),
            'has_publishers': game_obj.publishers.exists(),
            'has_themes': game_obj.themes.exists(),
            'has_perspectives': game_obj.player_perspectives.exists(),
            'has_modes': game_obj.game_modes.exists(),
        }

        # Считаем, сколько данных отсутствует
        missing_count = sum(1 for has_data in missing_data.values() if not has_data)

        return missing_data, missing_count

    def _get_saved_offset(self, options):
        """Получает сохраненный offset для текущих параметров"""
        params = self._get_offset_params(options)
        return OffsetManager.load_offset(params)

    def _save_offset_for_continuation(self, options, current_offset):
        """Сохраняет offset для продолжения"""
        params = self._get_offset_params(options)
        saved = OffsetManager.save_offset(params, current_offset)

        if saved and options.get('debug', False):
            self.stdout.write(f'   💾 Сохранен offset для параметров: {current_offset}')

        return saved

    def _handle_reset_offset(self, options, debug):
        """Обрабатывает сброс сохраненного offset"""
        params = self._get_offset_params(options)
        cleared = OffsetManager.clear_offset(params)

        if cleared:
            self.stdout.write('🔄 Сброшен сохраненный offset для текущих параметров')
        else:
            self.stdout.write('⚠️  Не удалось сбросить offset или offset не существует')

    def _get_offset_params(self, options):
        """Получает параметры для создания ключа offset"""
        # ВСЕГДА в одном порядке для одинаковых параметров
        return {
            'game_modes': options.get('game_modes', ''),
            'game_names': options.get('game_names', ''),
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': self._get_loading_mode(options),
        }

    def load_games_by_game_mode(self, game_mode_name, debug=False, limit=0, offset=0, min_rating_count=0,
                                skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по режиму игры (например, Battle Royale)"""
        collector = DataCollector(self.stdout, self.stderr)

        if debug:
            self.stdout.write(f'🔍 Поиск режима игры: "{game_mode_name}"')

        # Получаем ID режима игры по имени
        query = f'fields id,name; where name = "{game_mode_name}";'

        try:
            result = make_igdb_request('game_modes', query, debug=False)
        except Exception as e:
            if debug:
                self.stderr.write(f'❌ Ошибка при поиске режима игры: {e}')
            return collector._empty_result()

        if not result:
            if debug:
                self.stdout.write(f'❌ Режим игры "{game_mode_name}" не найден')
            return collector._empty_result()

        game_mode_id = result[0]['id']
        found_mode_name = result[0].get('name', game_mode_name)

        if debug:
            self.stdout.write(f'✅ Режим игры "{found_mode_name}" найден: ID {game_mode_id}')

        # Формируем условие для поиска игр - сразу по ID режима
        where_conditions = [f'game_modes = ({game_mode_id})']

        if min_rating_count > 0:
            where_conditions.append(f'rating_count >= {min_rating_count}')
        else:
            where_conditions.append('rating_count > 0')

        where_conditions.append('name != null')

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_conditions.append(f'game_type = ({game_types_str_query})')
            except ValueError:
                if debug:
                    self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        where_clause = ' & '.join(where_conditions)

        if debug:
            self.stdout.write(f'🎯 Условие поиска: {where_clause}')

        # Используем существующий метод загрузки по запросу
        # Передаем контекст что это специфический поиск
        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            query_context={'is_specific_search': True, 'mode_id': game_mode_id}
        )

    def _setup_execution_environment(self, options):
        """Настройка окружения выполнения команды"""
        # Сохраняем options для доступа в других методах
        self.current_options = options

        # Инициализация параметров
        repeat_count = options['repeat']
        original_offset = options['offset']
        limit = options['limit']
        iteration_limit = options['iteration_limit']
        debug = options['debug']
        overwrite = options['overwrite']
        count_only = options['count_only']
        clear_cache = options.get('clear_cache', False)
        reset_offset = options.get('reset_offset', False)
        update_missing_data = options.get('update_missing_data', False)

        # Если режим обновления данных с game-names, корректируем параметры
        if update_missing_data and options['game_names']:
            # Для режима обновления одной игры по имени:
            # - только одна итерация
            # - лимит 1 игра
            # - не используем общий лимита
            repeat_count = -1  # одна итерация
            limit = 0  # без общего лимита
            iteration_limit = 1  # только одна игра за итерацию
            if debug:
                self.stdout.write(f'   🎯 РЕЖИМ ОБНОВЛЕНИЯ ДАННЫХ: 1 игра за итерацию')

        # Проверяем, нужно ли обновлять все игры в базе
        update_all_games = False
        if update_missing_data and not any([
            options['game_names'],
            options['game_modes'],
            options['genres'],
            options['description_contains'],
            options['keywords']
        ]):
            update_all_games = True
            if debug:
                self.stdout.write(f'   🎯 РЕЖИМ ОБНОВЛЕНИЯ: ВСЕ игры в базе')

        # Если режим обновления всех игр (без конкретных фильтров)
        if update_all_games:
            if debug:
                self.stdout.write(f'\n🎯 ЗАПУСК ОБНОВЛЕНИЯ ДАННЫХ ДЛЯ ВСЕХ ИГР В БАЗЕ')

            # Вызываем специальный метод для обновления всех игр
            updated_count, update_details = self.update_all_games_missing_data(options, debug)

            self.stdout.write(f'\n' + '=' * 60)
            self.stdout.write(f'✅ ОБНОВЛЕНИЕ ВСЕХ ИГР ЗАВЕРШЕНО!')
            self.stdout.write(f'🎯 Обновлено игр: {updated_count}')

            return None, None, None, None, None, None, True

        # Сохраняем режим отладки
        self.debug_mode = debug

        # Очищаем кэш если нужно
        if clear_cache:
            self.clear_game_cache()

        # Сбрасываем offset если нужно
        if reset_offset:
            self._handle_reset_offset(options, debug)

        # Определяем режим выполнения
        execution_mode = self._determine_execution_mode(repeat_count)

        # Инициализируем прогресс-бар
        progress_bar = self._create_progress_bar()

        # Если есть общий лимит, показываем его как цель
        if limit > 0:
            progress_bar.total_games = limit
            progress_bar.update()
        # Для режима обновления одной игры показываем 1 как цель
        elif update_missing_data and options['game_names']:
            progress_bar.total_games = 1
            progress_bar.update()
        # Для бесконечного режима не показываем общий прогресс
        elif execution_mode['infinite_mode']:
            progress_bar.total_games = 0
            progress_bar.update()

        # Выводим информацию о запуске
        self._display_startup_info(execution_mode, iteration_limit, limit)

        # Инициализация статистики
        current_offset = original_offset

        # Загружаем сохраненный offset если не указан явно
        if original_offset == 0 and not reset_offset:
            saved_offset = self._get_saved_offset(options)
            if saved_offset is not None:
                current_offset = saved_offset
                self.stdout.write(f'📍 Начинаем с сохраненного offset: {current_offset}')

        total_stats = self._initialize_total_stats(original_offset)

        # Если это TTY терминал, оставляем место для прогресс-бара
        if (hasattr(progress_bar, 'is_tty') and progress_bar.is_tty and not count_only and not debug):
            self.stdout.write('\n' * 2)

        return execution_mode, progress_bar, current_offset, total_stats, options, limit

    def _run_execution_loop(self, execution_mode, progress_bar, current_offset, total_stats, options, limit):
        """Выполнение основного цикла команды"""
        debug = options.get('debug', False)
        reset_offset = options.get('reset_offset', False)
        iteration = 1

        try:
            while True:
                # Выполняем одну итерацию
                should_continue, current_offset, total_stats = self._execute_single_iteration(
                    iteration, current_offset, total_stats, execution_mode,
                    limit, options['iteration_limit'], options, progress_bar
                )

                if not should_continue:
                    break

                # Пауза между итерациями
                if iteration < execution_mode['repeat_count'] or execution_mode['infinite_mode']:
                    pause_time = 2
                    if self.debug_mode:
                        self.stdout.write(f'   ⏸️  Пауза {pause_time} секунд...')
                    time.sleep(pause_time)

                iteration += 1

        except KeyboardInterrupt:
            # Глобальное прерывание команды
            self._handle_global_interrupt(total_stats, execution_mode,
                                          options['offset'], current_offset,
                                          limit, progress_bar)
            if not reset_offset:
                self._save_offset_for_continuation(options, current_offset)

            return

        except Exception as e:
            # Обработка других исключений
            self.stderr.write(f'\n❌ Неожиданная ошибка: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())

            # Все равно сохраняем offset если нужно
            if not reset_offset:
                self._save_offset_for_continuation(options, current_offset)

            return

        # ФИНАЛЬНЫЙ ЭТАП
        self._finalize_execution(total_stats, limit, progress_bar,
                                 execution_mode, options['offset'],
                                 current_offset, limit, options['overwrite'])

        # Итоговый статус
        self._display_final_status(total_stats, limit)

    def execute_command(self, options):
        """Основной метод выполнения команды"""
        # Создаем папку для логов сразу при старте
        log_dir = self._ensure_logs_directory(options.get('debug', False))
        if options.get('debug', False):
            self.stdout.write(f'📁 Логи будут сохраняться в: {log_dir}')

        # Настройка окружения
        setup_result = self._setup_execution_environment(options)

        # Если режим обновления всех игр, завершаем выполнение
        if setup_result[6] if len(setup_result) > 6 else False:
            return

        # Распаковываем результаты настройки
        execution_mode, progress_bar, current_offset, total_stats, options, limit = setup_result

        # Запускаем основной цикл выполнения
        self._run_execution_loop(execution_mode, progress_bar, current_offset, total_stats, options, limit)

    def _execute_single_iteration(self, iteration, current_offset, total_stats, execution_mode,
                                  limit, iteration_limit, options, progress_bar):
        """Выполняет одну итерацию загрузки"""
        # Проверяем, следует ли продолжать
        should_continue = self._should_continue_iteration(
            iteration, execution_mode, total_stats, limit, self.max_consecutive_no_new_games
        )

        if not should_continue:
            return False, current_offset, total_stats

        # Выводим информацию о текущей итерации только в debug режиме
        if self.debug_mode:
            if execution_mode['repeat_count'] > 1 or execution_mode['infinite_mode']:
                self.stdout.write(f'\n🌀 ИТЕРАЦИЯ {iteration}')
                if execution_mode['infinite_mode']:
                    self.stdout.write(
                        f'🌀 (бесконечный режим, итераций без новых игр: {total_stats["iterations_with_no_new_games"]}/{self.max_consecutive_no_new_games})')
                self.stdout.write('=' * 40)

                if current_offset > options['offset']:
                    self.stdout.write(f'📊 Начинаем с offset: {current_offset}')

            # Рассчитываем лимит для этой итерации
            iteration_limit_actual, can_continue = self._calculate_iteration_limit(
                limit, iteration_limit, total_stats
            )

            if not can_continue or iteration_limit_actual <= 0:
                self.stdout.write(f'\n✅ ДОСТИГНУТ ОБЩИЙ ЛИМИТ: {limit} игр загружено')
                return False, current_offset, total_stats

            self.stdout.write(f'🎯 Цель итерации: найти {iteration_limit_actual} новых игр')
            if limit > 0:
                remaining_limit = limit - total_stats['total_games_created']
                self.stdout.write(f'   (осталось до лимита: {remaining_limit})')
        else:
            # В не-debug режиме просто обновляем прогресс-бар
            iteration_limit_actual, can_continue = self._calculate_iteration_limit(
                limit, iteration_limit, total_stats
            )

            if not can_continue or iteration_limit_actual <= 0:
                if limit > 0 and total_stats['total_games_created'] >= limit:
                    progress_bar.final_message(f"✅ ДОСТИГНУТ ОБЩИЙ ЛИМИТ: {limit} игр загружено")
                return False, current_offset, total_stats

        try:
            # Выполняем итерацию
            iteration_result = self.handle_single_iteration(
                iteration=iteration,
                current_offset=current_offset,
                iteration_limit_actual=iteration_limit_actual,
                options=options
            )

            if iteration_result.get('success', True):
                # Обновляем общую статистику
                current_offset = self._update_total_stats(
                    total_stats, iteration_result, iteration,
                    current_offset, execution_mode, progress_bar
                )
            else:
                # Если итерация не вернула результат
                if self.debug_mode:
                    self.stdout.write(f'   ⚠️  Итерация {iteration} не вернула результат')
                total_stats['iterations'] += 1
                total_stats['iterations_with_no_new_games'] += 1

                # Обновляем прогресс-бар
                if progress_bar:
                    progress_bar.update(
                        total_loaded=total_stats['total_games_created'],
                        current_iteration=iteration,
                        iterations_without_new=total_stats['iterations_with_no_new_games']
                    )

        except KeyboardInterrupt:
            raise
        except Exception as e:
            self._handle_iteration_error(e, iteration, execution_mode, total_stats, progress_bar)

        return True, current_offset, total_stats

    def handle_single_iteration(self, iteration, current_offset, iteration_limit_actual, options):
        """Обработка одной итерации команды"""
        debug = options.get('debug', False)

        # Подготовка параметров
        params = self._get_execution_parameters(options)

        # Передаем options в params для доступа к update_missing_data
        params['update_missing_data'] = options.get('update_missing_data', False)

        # Только в debug режиме показываем заголовок
        if debug:
            self.stdout.write('🎮 ЗАГРУЗКА ИГР ИЗ IGDB')
            self.stdout.write('=' * 60)

            # Определяем тип загрузки
            self._display_loading_type(params)

            # Информация об итерации
            iteration_info = {
                'iteration_number': iteration,
                'repeat_count': options.get('repeat', 1),
                'iteration_offset': current_offset,
                'iteration_limit_actual': iteration_limit_actual,
            }
            self._display_iteration_info(params, iteration_info)

        # Используем offset и limit для этой конкретной итерации
        actual_offset = current_offset
        actual_limit = iteration_limit_actual

        # Определение режимов
        skip_existing = self._determine_skip_mode(params)

        errors = 0
        iteration_start_time = time.time()

        # Если режим обновления, показываем специальное сообщение (только в debug)
        if params.get('update_missing_data') and debug:
            self.stdout.write(f'🔄 РЕЖИМ ОБНОВЛЕНИЯ ДАННЫХ: проверка и дополнение недостающих данных')
            # В режиме обновления не пропускаем существующие игры
            skip_existing = False

        # Загрузка игр
        result = self._load_games_for_iteration(params, actual_limit, actual_offset, skip_existing, debug)

        # Обработка результатов загрузки
        if result is None:
            return self._handle_failed_loading(iteration_start_time, errors, actual_offset)

        # Проверка наличия игр
        if not result.get('all_found_games') and not result.get('new_games'):
            return self._handle_empty_results(result, errors, params, actual_offset, iteration_start_time)

        # Обработка режима обновления недостающих данных
        if params.get('update_missing_data'):
            return self._update_existing_game_data(result, params, iteration_start_time, errors)

        # Обработка режима count-only
        if params['count_only']:
            return self._handle_count_only_mode(result, errors, iteration_start_time, actual_offset)

        # Обработка данных игр (стандартная загрузка)
        result_stats, errors = self._process_standard_game_data(
            result, params, iteration_start_time, errors
        )

        # Подготовка финальной статистики (только в debug)
        if debug:
            iteration_info = {
                'iteration_number': iteration,
                'repeat_count': options.get('repeat', 1),
            }

            final_stats = self._prepare_final_iteration_stats(
                result, result_stats, actual_offset, actual_limit,
                errors, iteration_info, params, iteration_start_time
            )

            # Отображение статистики итерации
            return self._display_iteration_statistics_complete(
                final_stats, result, actual_offset, actual_limit,
                params, iteration_info, errors, result_stats
            )
        else:
            # В не-debug режиме возвращаем только базовую статистику
            return {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': result['new_games_count'],
                'created_count': result_stats.get('created_count', 0),
                'skipped_count': result['existing_games_skipped'],
                'total_time': result_stats.get('total_time', time.time() - iteration_start_time),
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', actual_offset),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

    def _display_loading_type(self, params):
        """Отображает тип загрузки"""
        game_modes_str = params.get('game_modes_str', '')  # НОВОЕ
        game_names_str = params.get('game_names_str', '')
        genres_str = params['genres_str']
        description_contains = params['description_contains']
        keywords_str = params['keywords_str']
        game_types_str = params['game_types_str']

        if params['count_only']:
            self.stdout.write('🔢 РЕЖИМ: ПОДСЧЕТ НОВЫХ ИГР (которых нет в базе)')
            self.stdout.write('⚠️  Игры не будут сохранены в базу данных!')

        # НОВАЯ ВЕТКА: поиск по режимам игры (ВЫСШИЙ ПРИОРИТЕТ)
        if game_modes_str:
            mode_list = [m.strip() for m in game_modes_str.split(',') if m.strip()]
            if mode_list:
                self.stdout.write(f'🎮 РЕЖИМ: Игры с режимом: "{mode_list[0]}"')
                self.stdout.write(f'   🔍 Поиск самых популярных игр с указанным режимом')

                if len(mode_list) > 1:
                    self.stdout.write(
                        f'   ⚠️  Указано {len(mode_list)} режимов, используется только первый: "{mode_list[0]}"')

    def _display_iteration_info(self, params, iteration_info):
        """Отображает информацию об итерации"""
        game_names_str = params.get('game_names_str', '')
        game_types_str = params['game_types_str']
        iteration_number = iteration_info['iteration_number']
        repeat_count = iteration_info['repeat_count']
        actual_limit = iteration_info['iteration_limit_actual']
        actual_offset = iteration_info['iteration_offset']

        # Показываем информацию о типах игр
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                self.stdout.write(f'🎮 ФИЛЬТР ПО ТИПАМ ИГР: {game_types}')
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        # Специальное сообщение для режима поиска по имени
        if game_names_str:
            name_list = [n.strip() for n in game_names_str.split(',') if n.strip()]
            if name_list:
                self.stdout.write(f'🔎 Будет загружена САМАЯ ПОПУЛЯРНАЯ игра с названием: "{name_list[0]}"')
        elif repeat_count > 1:
            self.stdout.write(f'🔄 Итерация {iteration_number}/{repeat_count}')

        # Для режима поиска по имени показываем специальный лимит
        if game_names_str:
            self.stdout.write(f'📊 ЛИМИТ: 1 игра (самая популярная с указанным названием)')
        elif actual_limit > 0:
            self.stdout.write(f'📊 ЛИМИТ ИТЕРАЦИИ: {actual_limit} НОВЫХ игр')
        else:
            self.stdout.write(f'📊 ИТЕРАЦИЯ: загрузка без лимита')

        if actual_offset > 0:
            self.stdout.write(f'⏭️  OFFSET: начинаем с позиции {actual_offset} в результатах поиска')

        if params['min_rating_count'] > 0:
            self.stdout.write(f'⭐ ФИЛЬТР: игры с не менее {params["min_rating_count"]} оценками')

        if params['overwrite'] and not params['count_only']:
            self.stdout.write('🔄 OVERWRITE: найденные игры будут удалены и загружены заново')

        if params['count_only'] and params['overwrite']:
            self.stdout.write('⚠️  Overwrite игнорируется в режиме count-only')

        if params['debug']:
            self.stdout.write('🐛 РЕЖИМ ОТЛАДКИ ВКЛЮЧЕН')
            self.stdout.write('-' * 40)

    def _determine_skip_mode(self, params):
        """Определяет режим пропуска существующих игр"""
        if params['overwrite']:
            return False
        else:
            return True

    def _load_games_for_iteration(self, params, actual_limit, actual_offset, skip_existing, debug):
        """Загружает игры для итерации"""
        try:
            # НОВАЯ ВЕТКА: поиск по режимам игры (ВЫСШИЙ ПРИОРИТЕТ)
            if params.get('game_modes_str'):
                return self.load_games_by_game_mode(
                    params['game_modes_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            # Поиск по именам игр
            elif params.get('game_names_str'):
                return self.load_games_by_names(
                    params['game_names_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str'] and params['description_contains']:
                return self.load_games_by_genres_and_description(
                    params['genres_str'], params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str']:
                return self.load_games_by_genres(
                    params['genres_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['description_contains']:
                return self.load_games_by_description(
                    params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['keywords_str']:
                return self.load_games_by_keywords(
                    params['keywords_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            else:
                return self.load_all_popular_games(
                    debug, actual_limit, actual_offset, params['min_rating_count'],
                    skip_existing, params['count_only'], params['game_types_str']
                )
        except Exception as e:
            self.stderr.write(f'❌ ОШИБКА при загрузке игр: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            return None

    def _handle_failed_loading(self, iteration_start_time, errors, actual_offset):
        """Обрабатывает неудачную загрузку"""
        iteration_time = time.time() - iteration_start_time
        self.stdout.write('❌ Ошибка при загрузке игр, результат None')

        return {
            'total_games_found': 0,
            'total_games_checked': 0,
            'created_count': 0,
            'skipped_count': 0,
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': actual_offset,
            'limit_reached': False,
            'limit_reached_at_offset': None,
        }

    def _handle_empty_results(self, result, errors, params, actual_offset, iteration_start_time):
        """Обрабатывает пустые результаты"""
        iteration_time = time.time() - iteration_start_time

        if result and result.get('total_games_checked', 0) > 0:
            if params['overwrite']:
                self.stdout.write(f'ℹ️  Найдено {result.get("total_games_checked", 0)} игр для перезаписи')
            else:
                self.stdout.write(
                    f'❌ Найдено {result.get("total_games_checked", 0)} игр, но все они уже есть в базе')
        else:
            if errors == 0:
                self.stdout.write('❌ Не найдено игр для загрузки')

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

    def _handle_count_only_mode(self, result, errors, iteration_start_time, actual_offset):
        """Обрабатывает режим только подсчета"""
        iteration_time = time.time() - iteration_start_time

        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('✅ ПОДСЧЕТ ЗАВЕРШЕН!')
        self.stdout.write(f'🎮 Игр можно загрузить (которых нет в базе): {result["new_games_count"]}')

        if errors > 0:
            self.stdout.write(f'❌ Ошибок при подсчете: {errors}')

        return {
            'total_games_found': result['new_games_count'],
            'total_games_checked': result['total_games_checked'],
            'created_count': 0,
            'skipped_count': result['existing_games_skipped'],
            'total_time': iteration_time,
            'errors': errors,
            'last_checked_offset': result.get('last_checked_offset', actual_offset),
            'limit_reached': result.get('limit_reached', False),
            'limit_reached_at_offset': result.get('limit_reached_at_offset'),
        }

    def _process_game_data_for_iteration(self, result, params, iteration_start_time, errors):
        """Обрабатывает данные игр для итерации"""
        # Если режим обновления недостающих данных - вызываем специальную функцию
        if params.get('update_missing_data'):
            return self._update_existing_game_data(result, params, iteration_start_time, errors)

        # Стандартная обработка (существующая логика)
        return self._process_standard_game_data(result, params, iteration_start_time, errors)

    def _prepare_final_iteration_stats(self, result, result_stats, actual_offset, actual_limit,
                                       errors, iteration_info, params, iteration_start_time):
        """Подготавливает финальную статистику итерации"""
        if result_stats:
            result_stats['total_games_checked'] = result['total_games_checked']
            result_stats['total_games_found'] = result['new_games_count']
            result_stats['errors'] = errors
            result_stats['last_checked_offset'] = result.get('last_checked_offset', actual_offset)
            result_stats['limit_reached'] = result.get('limit_reached', False)
            result_stats['limit_reached_at_offset'] = result.get('limit_reached_at_offset')
        else:
            iteration_time = time.time() - iteration_start_time
            result_stats = {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': result['new_games_count'],
                'created_count': 0,
                'skipped_count': 0,
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', actual_offset),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        return result_stats

    def _display_iteration_statistics_complete(self, final_stats, result, actual_offset, actual_limit,
                                               params, iteration_info, errors, result_stats):
        """Отображает полную статистику итерации"""
        all_games = result.get('all_found_games', result.get('new_games', []))
        total_games_checked = result['total_games_checked']
        new_games_count = result['new_games_count']
        existing_games_skipped = result['existing_games_skipped']
        limit_reached = result.get('limit_reached', False)
        limit_reached_at_offset = result.get('limit_reached_at_offset')

        # Получаем последний проверенный offset
        if limit_reached_at_offset is not None:
            last_checked_offset = limit_reached_at_offset
        else:
            last_checked_offset = result.get('last_checked_offset',
                                             actual_offset + total_games_checked - 1)

        # Вывод основной информации
        self._display_main_iteration_info(params, new_games_count, total_games_checked,
                                          existing_games_skipped, last_checked_offset,
                                          actual_limit, limit_reached)

        # Если режим обновления данных, показываем специальную статистику
        if params.get('update_missing_data'):
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write('🔄 СТАТИСТИКА ОБНОВЛЕНИЯ ДАННЫХ')
            self.stdout.write('=' * 60)

            updated_count = final_stats.get('updated_count', 0)
            update_details = final_stats.get('update_details', [])

            self.stdout.write(f'👀 Всего найдено игр в IGDB: {len(all_games)}')
            self.stdout.write(f'✅ Успешно обновлено: {updated_count}')

            if update_details:
                for detail in update_details:
                    game_name = detail['game_name']
                    details = detail['details']

                    self.stdout.write(f'\n🎯 {game_name}:')
                    if details.get('updated_fields'):
                        self.stdout.write(f'   📝 Обновленные поля: {", ".join(details["updated_fields"])}')
                    if details.get('updated_relations'):
                        self.stdout.write(f'   🔗 Добавленные связи: {", ".join(details["updated_relations"])}')
                    if details.get('screenshots_added', 0) > 0:
                        self.stdout.write(f'   📸 Добавлено скриншотов: {details["screenshots_added"]}')

            self.stdout.write(f'⏱️  Время: {final_stats["total_time"]:.2f}с')

        else:
            # Вывод краткой или подробной статистики для стандартного режима
            if not params['debug']:
                self._display_short_iteration_stats(result_stats, iteration_info, errors,
                                                    limit_reached, last_checked_offset, params)
            else:
                self._display_detailed_iteration_stats(result_stats, iteration_info, actual_offset,
                                                       last_checked_offset, total_games_checked,
                                                       new_games_count, errors, limit_reached)

        # Возвращаем статистику
        return final_stats

    def _display_main_iteration_info(self, params, new_games_count, total_games_checked,
                                     existing_games_skipped, last_checked_offset,
                                     actual_limit, limit_reached):
        """Отображает основную информацию об итерации"""
        game_names_str = params.get('game_names_str', '')

        if game_names_str:
            name_list = [n.strip() for n in game_names_str.split(',') if n.strip()]
            if name_list:
                self.stdout.write(f'🔍 Поиск самой популярной игры с названием: "{name_list[0]}"')
        elif params['overwrite']:
            self.stdout.write(f'📥 Найдено игр для перезаписи: {new_games_count}')
        else:
            self.stdout.write(f'📥 Найдено игр для обработки: {new_games_count}')

        # Для режима поиска по имени показываем специфичную информацию
        if not game_names_str:
            self.stdout.write(f'👀 Всего просмотрено игр из IGDB: {total_games_checked}')
            self.stdout.write(f'📍 Последний проверенный offset: {last_checked_offset}')
            self.stdout.write(f'📍 Следующий offset для продолжения: {last_checked_offset + 1}')

            if limit_reached:
                self.stdout.write(f'🎯 Лимит {actual_limit} достигнут на offset {last_checked_offset}')

            if existing_games_skipped > 0 and not params['overwrite']:
                self.stdout.write(f'⏭️  Пропущено существующих игр: {existing_games_skipped}')
        else:
            # Для режима поиска по имени - упрощенная информация
            if new_games_count > 0:
                self.stdout.write(f'✅ Найдена игра для загрузки')
            else:
                if existing_games_skipped > 0:
                    self.stdout.write(f'ℹ️  Игра уже есть в базе данных')
                else:
                    self.stdout.write(f'❌ Игра с таким названием не найдена')

    def _display_short_iteration_stats(self, result_stats, iteration_info, errors,
                                       limit_reached, last_checked_offset, params):
        """Отображает краткую статистику итерации"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА!')
        self.stdout.write(f'⏱️  Время: {result_stats["total_time"]:.2f}с')

        if iteration_info['repeat_count'] > 1:
            self.stdout.write(f'🔄 Итерация {iteration_info["iteration_number"]}/{iteration_info["repeat_count"]}')

        if errors > 0:
            self.stdout.write(f'❌ Ошибок в итерации: {errors}')

        if limit_reached:
            self.stdout.write(f'🎯 Лимит достигнут на offset {last_checked_offset}')

        if params['overwrite']:
            self.stdout.write(f'🔄 Перезаписано игр: {result_stats.get("created_count", 0)}')
        else:
            self.stdout.write(f'✅ Загружено игр: {result_stats.get("created_count", 0)}')

    def _display_detailed_iteration_stats(self, result_stats, iteration_info, actual_offset,
                                          last_checked_offset, total_games_checked,
                                          new_games_count, errors, limit_reached):
        """Отображает подробную статистику итерации"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 ПОДРОБНАЯ СТАТИСТИКА ИТЕРАЦИИ')
        self.stdout.write('=' * 60)
        self.stdout.write(f'🔄 Итерация: {iteration_info["iteration_number"]}/{iteration_info["repeat_count"]}')
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

    # Методы загрузки игр из IGDB
    def load_games_by_genres(self, genres_str, debug=False, limit=0, offset=0, min_rating_count=0,
                             skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
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

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_clause = f'{where_clause} & game_type = ({game_types_str_query})'
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        if debug:
            self.stdout.write(f'🎯 Условие поиска (И): {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_genres_and_description(self, genres_str, description_text, debug=False, limit=0, offset=0,
                                             min_rating_count=0, skip_existing=True, count_only=False,
                                             game_types_str='0,1,2,4,5,8,9,10,11'):
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

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_clause = f'{where_clause} & game_type = ({game_types_str_query})'
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        if debug:
            self.stdout.write(f'🎯 Итоговое условие поиска: {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_description(self, description_text, debug=False, limit=0, offset=0, min_rating_count=0,
                                  skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по тексту в описании или названии"""
        collector = DataCollector(self.stdout, self.stderr)

        if debug:
            self.stdout.write(f'🔍 Ищу игры с текстом: "{description_text}"')

        # Формируем базовое условие для поиска
        where_conditions = [
            f'name ~ *"{description_text}"* | summary ~ *"{description_text}"* | storyline ~ *"{description_text}"*']

        if min_rating_count > 0:
            where_conditions.append(f'rating_count >= {min_rating_count}')
        else:
            where_conditions.append('rating_count > 0')

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_conditions.append(f'game_type = ({game_types_str_query})')
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        where_clause = ' & '.join(where_conditions)

        if debug:
            self.stdout.write(f'   🎯 Условие поиска: {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_games_by_keywords(self, keywords_str, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
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

        # Добавляем фильтр по game_type если указаны типы
        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_clause = f'{where_clause} & game_type = ({game_types_str_query})'
            except ValueError:
                self.stderr.write(f'   ⚠️  Ошибка парсинга game-types: "{game_types_str}"')

        if debug:
            self.stdout.write(f'🎯 Условие поиска: {where_clause}')

        return collector.load_games_by_query(where_clause, debug, limit, offset, skip_existing, count_only)

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        collector = DataCollector(self.stdout, self.stderr)
        return collector.load_all_popular_games(debug, limit, offset, min_rating_count, skip_existing, count_only,
                                                game_types_str)

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
                # Удаляем найденные игры
                deleted_info = games_to_delete.delete()

                # Разбираем результат delete()
                if isinstance(deleted_info, tuple) and len(deleted_info) == 2:
                    total_deleted, deleted_details = deleted_info
                    self.stdout.write(f'🗑️  УДАЛЕНИЕ ЗАВЕРШЕНО:')
                    self.stdout.write(f'   • Всего удалено объектов: {total_deleted}')

                    for model_name, count in deleted_details.items():
                        model_display = model_name.split('.')[-1]
                        if count > 0:
                            self.stdout.write(f'   • {model_display}: {count}')
                else:
                    self.stdout.write(f'🗑️  Удалено игр и связанных данных: {deleted_info}')
            else:
                self.stdout.write('   ℹ️  Не найдено игр для удаления в базе данных')
        else:
            self.stdout.write('   ⚠️  Не найдено ID игр для удаления')

    # Вспомогательные методы из base_command
    def _create_progress_bar(self):
        """Создает подходящий прогресс-бар для текущего терминала"""
        import os
        import sys

        # Проверяем поддержку ANSI
        supports_ansi = False
        if hasattr(sys.stdout, 'isatty') and sys.stdout.isatty():
            if os.name == 'nt':
                supports_ansi = os.environ.get('TERM') == 'xterm' or \
                                os.environ.get('WT_SESSION') is not None or \
                                os.environ.get('ANSICON') is not None
            else:
                supports_ansi = True

        if supports_ansi:
            from .base_command import TopProgressBar
            return TopProgressBar(self.stdout)
        else:
            from .base_command import SimpleProgressBar
            return SimpleProgressBar(self.stdout)

    def _determine_execution_mode(self, repeat_count):
        """Определяет режим выполнения команды"""
        infinite_mode = repeat_count == 0
        single_run_mode = repeat_count == -1
        finite_mode = repeat_count > 0

        if single_run_mode:
            repeat_count = 1
            self.stdout.write('🔄 РЕЖИМ: ОДНА ИТЕРАЦИЯ (--repeat -1)')
        elif infinite_mode:
            self.stdout.write('🔄 РЕЖИМ: БЕСКОНЕЧНО (--repeat 0) - пока не закончатся игры')
            repeat_count = 999999
        elif finite_mode:
            self.stdout.write(f'🔄 РЕЖИМ: {repeat_count} ПОВТОРЕНИЙ')
        else:
            raise ValueError(
                'Неверное значение --repeat. Используйте: -1 (один раз), 0 (бесконечно), >0 (фиксированно)')

        return {
            'infinite_mode': infinite_mode,
            'single_run_mode': single_run_mode,
            'finite_mode': finite_mode,
            'repeat_count': repeat_count
        }

    def _initialize_total_stats(self, original_offset):
        """Инициализирует общую статистику"""
        return {
            'iterations': 0,
            'total_games_found': 0,
            'total_games_checked': 0,
            'total_games_created': 0,
            'total_games_skipped': 0,
            'total_time': 0,
            'last_checked_offset': original_offset,
            'errors': 0,
            'iterations_with_errors': 0,
            'iterations_with_limit_reached': 0,
            'iterations_with_no_new_games': 0,
            'max_iterations_reached': False,
            'interrupted': False,
        }

    def _display_startup_info(self, execution_mode, iteration_limit, limit):
        """Отображает информацию о запуске команды"""
        if execution_mode['repeat_count'] > 1 or execution_mode['infinite_mode']:
            repeat_display = execution_mode["repeat_count"] if not execution_mode[
                'infinite_mode'] else "до исчерпания игр"
            self.stdout.write(f'🔄 КОМАНДА БУДЕТ ПОВТОРЕНА {repeat_display} РАЗ')
            self.stdout.write(f'📊 Игр за итерацию: {iteration_limit}')
            if limit > 0:
                self.stdout.write(f'🎯 Общий лимит игр: {limit}')
            self.stdout.write('=' * 60)

    def _get_execution_parameters(self, options):
        """Получает параметры выполнения из options"""
        return {
            'game_modes_str': options['game_modes'],
            'game_names_str': options['game_names'],
            'genres_str': options['genres'],
            'description_contains': options['description_contains'],
            'overwrite': options['overwrite'],
            'debug': options['debug'],
            'limit': options['limit'],
            'offset': options['offset'],
            'min_rating_count': options['min_rating_count'],
            'keywords_str': options['keywords'],
            'count_only': options['count_only'],
            'game_types_str': options['game_types'],
            'iteration_limit': options['iteration_limit'],
            # ДОБАВЛЯЕМ:
            'update_missing_data': options.get('update_missing_data', False),
        }

    def _calculate_iteration_limit(self, limit, iteration_limit, total_stats):
        """Рассчитывает лимит для текущей итерации"""
        if limit > 0:
            remaining_limit = limit - total_stats['total_games_created']
            if remaining_limit <= 0:
                return 0, False
            iteration_limit_actual = min(iteration_limit, remaining_limit)
            return iteration_limit_actual, True
        else:
            return iteration_limit, True

    def _should_continue_iteration(self, iteration, execution_mode, total_stats, limit, max_consecutive_no_new_games):
        """Проверяет, следует ли продолжать выполнение"""
        infinite_mode = execution_mode['infinite_mode']
        single_run_mode = execution_mode['single_run_mode']
        finite_mode = execution_mode['finite_mode']
        repeat_count = execution_mode['repeat_count']

        # Проверяем условия остановки для бесконечного режима
        if infinite_mode and iteration > 1:
            if total_stats['iterations_with_no_new_games'] >= max_consecutive_no_new_games:
                self.stdout.write(f'\n⚠️  ОСТАНОВКА: {max_consecutive_no_new_games} итераций подряд без новых игр')
                return False

            if limit > 0 and total_stats['total_games_created'] >= limit:
                self.stdout.write(f'\n✅ ДОСТИГНУТ ЛИМИТ: {limit} игр загружено')
                return False

        # Для конечного режима проверяем лимит итераций
        if finite_mode and iteration > repeat_count:
            total_stats['max_iterations_reached'] = True
            return False

        # Для режима одного раза
        if single_run_mode and iteration > 1:
            return False

        return True

    def _update_total_stats(self, total_stats, iteration_stats, iteration,
                            current_offset, execution_mode, progress_bar):
        """Обновляет общую статистику"""
        # Обновляем статистику
        total_stats['iterations'] += 1
        total_stats['total_games_found'] += iteration_stats.get('total_games_found', 0)
        total_stats['total_games_checked'] += iteration_stats.get('total_games_checked',
                                                                  iteration_stats.get('total_games_found', 0))
        total_stats['total_games_created'] += iteration_stats.get('created_count', 0)
        total_stats['total_games_skipped'] += iteration_stats.get('skipped_count', 0)
        total_stats['total_time'] += iteration_stats.get('total_time', 0)

        # Проверяем, были ли найдены новые игры в этой итерации
        new_games_this_iteration = iteration_stats.get('created_count', 0)
        if new_games_this_iteration == 0 and iteration_stats.get('total_games_found', 0) == 0:
            total_stats['iterations_with_no_new_games'] += 1
        else:
            total_stats['iterations_with_no_new_games'] = 0

        # ОБНОВЛЯЕМ ПРОГРЕСС-БАР
        if progress_bar:
            progress_bar.update(
                total_loaded=total_stats['total_games_created'],
                current_iteration=iteration,
                iterations_without_new=total_stats['iterations_with_no_new_games']
            )

        # Добавляем ошибки из итерации
        iteration_errors = iteration_stats.get('errors', 0)
        if iteration_errors > 0:
            total_stats['errors'] += iteration_errors
            total_stats['iterations_with_errors'] += 1

        # Получаем последний проверенный offset
        limit_reached_offset = iteration_stats.get('limit_reached_at_offset')
        if limit_reached_offset is not None:
            last_checked_this_iteration = limit_reached_offset
        else:
            last_checked_this_iteration = iteration_stats.get('last_checked_offset',
                                                              current_offset + iteration_stats.get(
                                                                  'total_games_checked',
                                                                  iteration_stats.get('total_games_found', 0)) - 1)

        total_stats['last_checked_offset'] = last_checked_this_iteration
        new_offset = last_checked_this_iteration + 1

        if self.debug_mode:
            self.stdout.write(f'   📊 Итерация {iteration}:')
            self.stdout.write(f'      • Начальный offset: {current_offset}')
            self.stdout.write(f'      • Просмотрено игр: {iteration_stats.get("total_games_checked", 0)}')
            self.stdout.write(f'      • Найдено новых: {iteration_stats.get("total_games_found", 0)}')
            self.stdout.write(f'      • Загружено: {iteration_stats.get("created_count", 0)}')
            self.stdout.write(f'      • Ошибок: {iteration_errors}')
            self.stdout.write(f'      • Последний проверенный offset: {last_checked_this_iteration}')
            self.stdout.write(f'      • Следующий offset: {new_offset}')

        return new_offset

    def _handle_global_interrupt(self, total_stats, execution_mode,
                                 original_offset, current_offset,
                                 limit, progress_bar):
        """Обрабатывает глобальное прерывание команды (Ctrl+C)"""
        self.stdout.write('\n\n🛑 КОМАНДА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')

        if progress_bar:
            progress_bar.final_message("🛑 ВЫПОЛНЕНИЕ КОМАНДЫ ПРЕРВАНО")
            progress_bar.clear()

        self._display_interrupted_statistics(total_stats, execution_mode,
                                             original_offset, current_offset, limit)

        total_stats['interrupted'] = True

    def _display_interrupted_statistics(self, total_stats, execution_mode,
                                        original_offset, current_offset, limit):
        """Выводит статистику при прерывании команды"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('🛑 СТАТИСТИКА ПРЕРВАННОЙ КОМАНДЫ')
        self.stdout.write('=' * 60)

        if execution_mode['infinite_mode']:
            self.stdout.write(f'🔄 РЕЖИМ: БЕСКОНЕЧНЫЙ (прервано пользователем)')
        elif execution_mode['single_run_mode']:
            self.stdout.write(f'🔄 РЕЖИМ: ОДНА ИТЕРАЦИЯ (прервано)')
        else:
            self.stdout.write(f'🔄 Итераций выполнено: {total_stats["iterations"]} (прервано)')

        self.stdout.write(f'📍 Начальный offset: {original_offset}')
        self.stdout.write(f'📍 Текущий offset: {current_offset}')
        self.stdout.write(f'👀 Всего просмотрено игр: {total_stats["total_games_checked"]}')
        self.stdout.write(f'🎮 Всего найдено новых: {total_stats["total_games_found"]}')
        self.stdout.write(f'✅ Всего загружено игр: {total_stats["total_games_created"]}')
        self.stdout.write(f'⏭️  Всего пропущено игр: {total_stats["total_games_skipped"]}')
        self.stdout.write(f'❌ Ошибок: {total_stats["errors"]}')

        if limit > 0:
            self.stdout.write(f'🎯 Общий лимит игр: {limit} (загружено: {total_stats["total_games_created"]})')

        self.stdout.write(f'⏱️  Общее время: {total_stats["total_time"]:.2f}с')

    def _display_final_statistics(self, total_stats, execution_mode, original_offset,
                                  current_offset, limit, overwrite):
        """Выводит финальную статистику"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('📊 ОБЩАЯ СТАТИСТИКА ВСЕХ ИТЕРАЦИЙ')
        self.stdout.write('=' * 60)

        if execution_mode['infinite_mode']:
            self.stdout.write(
                f'🔄 РЕЖИМ: БЕСКОНЕЧНЫЙ (остановка после {self.max_consecutive_no_new_games} итераций без игр)')
        elif execution_mode['single_run_mode']:
            self.stdout.write(f'🔄 РЕЖИМ: ОДНА ИТЕРАЦИЯ')
        else:
            self.stdout.write(f'🔄 Итераций выполнено: {total_stats["iterations"]}/{execution_mode["repeat_count"]}')

        if total_stats['max_iterations_reached']:
            self.stdout.write(f'⚠️  ДОСТИГНУТ МАКСИМАЛЬНЫЙ ЛИМИТ ИТЕРАЦИЙ: {execution_mode["repeat_count"]}')

        self.stdout.write(f'📍 Начальный offset: {original_offset}')
        self.stdout.write(f'📍 Последний проверенный offset: {total_stats["last_checked_offset"]}')
        self.stdout.write(f'📍 Следующий offset (для продолжения): {current_offset}')
        self.stdout.write(f'👀 Всего просмотрено игр: {total_stats["total_games_checked"]}')
        self.stdout.write(f'🎮 Всего найдено новых: {total_stats["total_games_found"]}')
        self.stdout.write(f'✅ Всего загружено игр: {total_stats["total_games_created"]}')
        self.stdout.write(f'⏭️  Всего пропущено игр: {total_stats["total_games_skipped"]}')
        self.stdout.write(f'❌ Ошибок: {total_stats["errors"]}')
        self.stdout.write(f'⚠️  Итераций с ошибками: {total_stats["iterations_with_errors"]}')
        self.stdout.write(f'🚫 Итераций без новых игр: {total_stats["iterations_with_no_new_games"]}')

        if limit > 0:
            self.stdout.write(f'🎯 Общий лимит игр: {limit} (достигнуто: {total_stats["total_games_created"]})')

        self.stdout.write(f'⏱️  Общее время: {total_stats["total_time"]:.2f}с')

    def _display_final_status(self, total_stats, limit):
        """Выводит итоговый статус команды"""
        self.stdout.write('=' * 60)
        if total_stats['errors'] > 0:
            self.stdout.write('⚠️  ЗАГРУЗКА ЗАВЕРШЕНА С ОШИБКАМИ!')
        elif total_stats['iterations_with_no_new_games'] >= self.max_consecutive_no_new_games:
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА: ИГРЫ ЗАКОНЧИЛИСЬ')
        elif total_stats['max_iterations_reached']:
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА: ДОСТИГНУТ ЛИМИТ ИТЕРАЦИЙ')
        elif limit > 0 and total_stats['total_games_created'] >= limit:
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА: ДОСТИГНУТ ЛИМИТ ИГР')
        elif total_stats['interrupted']:
            self.stdout.write('🛑 ЗАГРУЗКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ')
        else:
            self.stdout.write('✅ ЗАГРУЗКА ЗАВЕРШЕНА УСПЕШНО!')

    def _finalize_execution(self, total_stats, limit, progress_bar,
                            execution_mode, original_offset,
                            current_offset, limit_val, overwrite):
        """Завершает выполнение команды"""
        if progress_bar:
            if limit > 0:
                if total_stats['total_games_created'] >= limit:
                    progress_bar.final_message(
                        f"✅ ЗАГРУЗКА ЗАВЕРШЕНА: {total_stats['total_games_created']}/{limit} игр загружено")
                else:
                    progress_bar.final_message(
                        f"⚠️  ЗАГРУЗКА ОСТАНОВЛЕНА: {total_stats['total_games_created']}/{limit} игр загружено")
            else:
                progress_bar.final_message(f"✅ ЗАГРУЗКА ЗАВЕРШЕНА: {total_stats['total_games_created']} игр загружено")

            progress_bar.clear()

        self._display_final_statistics(
            total_stats, execution_mode, original_offset,
            current_offset, limit_val, overwrite
        )

    def clear_game_cache(self):
        """Очищает кэш проверенных игр"""
        try:
            from .game_cache import GameCacheManager
            cleared = GameCacheManager.clear_cache()
            self.stdout.write(f"✅ Кэш проверенных игр очищен")
            return cleared
        except Exception as e:
            self.stderr.write(f"❌ Ошибка при очистке кэша: {e}")
            return False

    def _get_where_clause_for_current_command(self, options):
        """Получает where_clause для текущей команды"""
        game_names_str = options.get('game_names', '')  # НОВОЕ
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')
        game_types_str = options.get('game_types', '')
        min_rating_count = options.get('min_rating_count', 0)

        where_parts = []

        # НОВАЯ ВЕТКА: поиск по именам
        if game_names_str:
            name_list = [n.strip() for n in game_names_str.split(',') if n.strip()]
            name_conditions = [f'name ~ *"{name}"*' for name in name_list]
            where_parts.append(f'({" | ".join(name_conditions)})')
        # Определяем режим загрузки
        elif genres_str and description_contains:
            where_parts.append('genres = (...)')
            where_parts.append(f'(name ~ *"{description_contains}"* | summary ~ *"{description_contains}"*)')
        elif genres_str:
            where_parts.append('genres = (...)')
        elif description_contains:
            where_parts.append(f'(name ~ *"{description_contains}"* | summary ~ *"{description_contains}"*)')
        elif keywords_str:
            where_parts.append('keywords = (...)')

        # Обязательные условия
        if game_names_str:
            # Для поиска по именам rating_count может быть 0
            where_parts.append('name != null')
            if min_rating_count > 0:
                where_parts.append(f'rating_count >= {min_rating_count}')
        else:
            where_parts.append('rating_count > 0')
            where_parts.append('name != null')
            if min_rating_count > 0:
                where_parts.append(f'rating_count >= {min_rating_count}')

        if game_types_str:
            try:
                game_types = [int(gt.strip()) for gt in game_types_str.split(',') if gt.strip()]
                if game_types:
                    game_types_str_query = ','.join(map(str, game_types))
                    where_parts.append(f'game_type = ({game_types_str_query})')
            except ValueError:
                pass

        return ' & '.join(where_parts) if where_parts else 'rating_count > 0 & name != null'

    def _get_loading_mode(self, options):
        """Определяет режим загрузки для ключа offset"""
        game_names_str = options.get('game_names', '')  # НОВОЕ
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')

        if game_names_str:
            return 'game_names'  # НОВЫЙ РЕЖИМ
        elif genres_str and description_contains:
            return 'genres_and_description'
        elif genres_str:
            return 'genres'
        elif description_contains:
            return 'description'
        elif keywords_str:
            return 'keywords'
        else:
            return 'popular'

    def _get_query_key_for_current_command(self, options, where_clause):
        """Создает ключ запроса для текущей команды"""
        params = {
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': self._get_loading_mode(options),
        }

        return OffsetManager.get_query_key(where_clause, **params)

    def _handle_iteration_error(self, error, iteration, execution_mode, total_stats, progress_bar):
        """Обрабатывает ошибки в итерации"""
        # Проверка типа ошибки
        if isinstance(error, KeyboardInterrupt):
            raise error

        # Обработка обычных ошибок
        self._update_error_statistics(total_stats)
        self._display_error_details(error, iteration)

        should_continue = self._determine_continuation_mode(
            execution_mode, iteration, total_stats, progress_bar
        )

        if should_continue:
            self.stdout.write(f'   ⏩ Пропускаем итерацию {iteration} из-за ошибки')
            total_stats['iterations_with_no_new_games'] += 1
            total_stats['iterations'] += 1

            if progress_bar:
                progress_bar.update(
                    total_loaded=total_stats['total_games_created'],
                    current_iteration=iteration,
                    iterations_without_new=total_stats['iterations_with_no_new_games']
                )

        return should_continue

    def _update_error_statistics(self, total_stats):
        """Обновляет статистику ошибок"""
        total_stats['errors'] += 1
        total_stats['iterations_with_errors'] += 1

    def _display_error_details(self, error, iteration):
        """Выводит детали ошибки"""
        self.stderr.write(f'❌ ОШИБКА в итерации {iteration}: {str(error)}')
        if self.debug_mode:
            import traceback
            self.stderr.write(f'📋 Трассировка ошибки:')
            self.stderr.write(traceback.format_exc())

    def _determine_continuation_mode(self, execution_mode, iteration, total_stats, progress_bar):
        """Определяет режим продолжения после ошибки"""
        infinite_mode = execution_mode['infinite_mode']
        finite_mode = execution_mode['finite_mode']
        repeat_count = execution_mode['repeat_count']

        if infinite_mode:
            return True
        elif finite_mode and iteration < repeat_count:
            return True
        else:
            return False
