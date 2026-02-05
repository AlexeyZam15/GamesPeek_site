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


class ProcessingProgressBar:
    """Прогресс-бар для этапа обработки данных - работает в одной строке"""

    def __init__(self, stdout, total_games):
        self.stdout = stdout
        self.total_games = total_games
        self.steps = {
            'collecting_ids': ('📊', 'Сбор ID данных'),
            'creating_games': ('🎮', 'Создание игр'),
            'loading_all_types': ('🖼️', 'Типы данных'),
            'loading_screenshots': ('📸', 'Скриншоты'),
            'loading_additional': ('📚', 'Доп. данные'),
            'updating_covers': ('💾', 'Обложки'),
            'preparing_relations': ('🔗', 'Связи'),
            'creating_relations': ('⚙️', 'Создание связей'),
        }
        self.step_order = [
            'collecting_ids', 'creating_games', 'loading_all_types',
            'loading_screenshots', 'loading_additional', 'updating_covers',
            'preparing_relations', 'creating_relations'
        ]
        self.current_step_index = -1
        self.last_printed_length = 0
        self.is_tty = hasattr(stdout, 'isatty') and stdout.isatty()
        self.step_results = {}
        self.start_time = time.time()

    def start_step(self, step_key, info=''):
        """Начинает новый шаг"""
        if not self.is_tty:
            return

        self.current_step_index += 1
        if step_key not in self.steps:
            return

        emoji, name = self.steps[step_key]
        step_num = self.current_step_index + 1
        total_steps = len(self.step_order)

        # Форматируем сообщение
        message = f'   [{step_num}/{total_steps}] {emoji} {name}'
        if info:
            message += f': {info}'

        # Очищаем предыдущую строку
        if self.last_printed_length > 0:
            self.stdout.write('\r' + ' ' * self.last_printed_length + '\r')

        # Печатаем новое сообщение
        self.stdout.write('\r' + message)
        self.stdout.flush()

        self.last_printed_length = len(message)

    def complete_step(self, step_key, result_info=''):
        """Завершает шаг и показывает результат"""
        if step_key not in self.steps:
            return

        emoji, name = self.steps[step_key]
        self.step_results[step_key] = result_info

        if self.is_tty:
            # Очищаем строку
            self.stdout.write('\r' + ' ' * self.last_printed_length + '\r')
            self.last_printed_length = 0

        # Показываем результат шага
        if result_info:
            self.stdout.write(f'   ✅ {emoji} {name}: {result_info}')
        else:
            self.stdout.write(f'   ✅ {emoji} {name} завершено')

    def clear(self):
        """Очищает строку прогресса"""
        if self.is_tty and self.last_printed_length > 0:
            self.stdout.write('\r' + ' ' * self.last_printed_length + '\r')
            self.stdout.flush()
            self.last_printed_length = 0

    def show_summary(self, total_time, created_count):
        """Показывает итоговую статистику"""
        self.clear()

        # Рассчитываем скорость
        if total_time > 0 and created_count > 0:
            speed = created_count / total_time
            speed_str = f' ({speed:.1f} игр/с)'
        else:
            speed_str = ''

        self.stdout.write(f'   ✅ Обработано {created_count} игр за {total_time:.1f}с{speed_str}')


class ProcessingStatusLine:
    """Строка статуса для отображения прогресса в одной строке"""

    def __init__(self, stdout):
        self.stdout = stdout
        self.last_message = ''
        self.last_length = 0

    def update(self, message):
        """Обновляет строку статуса"""
        # Очищаем предыдущую строку
        if self.last_length > 0:
            self.stdout.write('\r' + ' ' * self.last_length + '\r')

        # Выводим новое сообщение
        self.stdout.write('\r' + message)
        self.stdout.flush()

        self.last_message = message
        self.last_length = len(message)

    def clear(self):
        """Очищает строку статуса"""
        if self.last_length > 0:
            self.stdout.write('\r' + ' ' * self.last_length + '\r')
            self.stdout.flush()
            self.last_message = ''
            self.last_length = 0

class GameLoader:
    """Основной класс для выполнения команды загрузки игр"""

    def __init__(self, stdout, stderr):
        self.stdout = stdout
        self.stderr = stderr
        self.max_consecutive_no_new_games = 3
        self.debug_mode = False
        self._last_processed_count = 0  # Добавить эту строку

    def _get_cover_ids_batch(self, game_ids, debug=False):
        """Получает cover_id для батча игр с rate limiting"""
        from games.igdb_api import make_igdb_request
        import time

        if not game_ids:
            return {}

        # Оптимальные настройки
        MAX_RETRIES = 2
        RETRY_DELAYS = [1.0, 3.0]

        id_list = ','.join(map(str, game_ids[:10]))
        query = f'fields id,cover.image_id; where id = ({id_list});'

        for attempt in range(MAX_RETRIES + 1):
            try:
                games_data = make_igdb_request('games', query, debug=False)
                return {gd['id']: gd.get('cover') for gd in games_data if 'id' in gd}
            except Exception as e:
                if attempt < MAX_RETRIES:
                    delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 5.0
                    if debug:
                        self.stdout.write(f'      ⏸️  Пауза {delay:.1f} сек...')
                    time.sleep(delay)
                else:
                    if debug:
                        self.stderr.write(f'      ❌ Ошибка запроса к IGDB: {e}')

        return {}

    def _get_saved_offset_for_update_covers(self, options):
        """Получает сохраненный offset для режима обновления обложек"""
        params = self._get_offset_params_for_update_covers(options)
        return OffsetManager.load_offset(params)

    def _save_offset_for_update_covers(self, options, current_offset):
        """Сохраняет offset для режима обновления обложек"""
        params = self._get_offset_params_for_update_covers(options)
        saved = OffsetManager.save_offset(params, current_offset)

        if saved:
            if options.get('debug', False):
                self.stdout.write(f'   💾 Offset для обновления обложек сохранен: {current_offset}')
        return saved

    def _get_offset_params_for_update_covers(self, options):
        """Получает параметры для создания ключа offset для режима обновления обложек"""
        # Создаем отдельные параметры для update_covers
        params = {
            'update_covers': True,  # Главный маркер режима
            'game_modes': options.get('game_modes', ''),
            'game_names': options.get('game_names', ''),
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': 'update_covers',  # Явно указываем режим
        }

        # Добавляем информацию о фильтрах для уникальности ключа
        has_filters = any([
            options.get('game_names'),
            options.get('game_modes'),
            options.get('genres'),
            options.get('description_contains'),
            options.get('keywords')
        ])

        if has_filters:
            params['has_filters'] = True
        else:
            params['has_filters'] = False

        return params

    def _update_game_covers(self, result, params, iteration_start_time, errors):
        """Обновляет обложки для найденных игр"""
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
                self.stdout.write(f'\n🖼️  ОБНОВЛЕНИЕ ОБЛОЖЕК ДЛЯ НАЙДЕННЫХ ИГР')
                self.stdout.write(f'   • Всего найдено игр для обработки: {len(games_to_process)}')

            if not games_to_process:
                if params['debug']:
                    self.stdout.write(f'   ⚠️  Нет игр для обновления обложек')

                iteration_time = time.time() - iteration_start_time
                return {
                    'total_games_checked': result['total_games_checked'],
                    'total_games_found': result.get('new_games_count', 0),
                    'created_count': 0,
                    'skipped_count': result.get('existing_games_skipped', 0),
                    'updated_count': 0,
                    'total_time': iteration_time,
                    'errors': errors,
                    'last_checked_offset': result.get('last_checked_offset', 0),
                    'limit_reached': result.get('limit_reached', False),
                    'limit_reached_at_offset': result.get('limit_reached_at_offset'),
                }

            # Создаем экземпляры для обработки данных
            collector = DataCollector(self.stdout, self.stderr)
            loader = DataLoader(self.stdout, self.stderr)

            # Собираем все ID данных
            collected_data = collector.collect_all_data_ids(games_to_process, params['debug'])

            # Загружаем данные об обложках
            cover_ids = collected_data.get('all_cover_ids', [])
            if params['debug']:
                self.stdout.write(f'   📥 Загрузка данных об обложках: {len(cover_ids)} ID')

            # Загружаем данные обложек
            cover_map = loader.load_covers_parallel(cover_ids, params['debug'])

            if params['debug']:
                self.stdout.write(f'   ✅ Загружено обложек: {len(cover_map)}')

            # Для каждой игры обновляем обложку
            for i, game_data in enumerate(games_to_process, 1):
                game_id = game_data.get('id')
                game_name = game_data.get('name', f'ID {game_id}')

                if params['debug']:
                    self.stdout.write(f'\n   🔄 [{i}/{len(games_to_process)}] Обновление обложки: {game_name}')

                if not game_id:
                    failed_count += 1
                    if params['debug']:
                        self.stdout.write(f'      ❌ Нет ID у игры: {game_name}')
                    continue

                # Получаем cover_id из данных игры
                cover_id = game_data.get('cover')
                if not cover_id:
                    failed_count += 1
                    if params['debug']:
                        self.stdout.write(f'      ❌ Нет cover_id у игры: {game_name}')
                    continue

                # Проверяем, есть ли обложка в загруженных данных
                if cover_id not in cover_map:
                    failed_count += 1
                    if params['debug']:
                        self.stdout.write(f'      ❌ Обложка {cover_id} не найдена в загруженных данных')
                    continue

                # Получаем новый URL обложки
                new_cover_url = cover_map[cover_id]

                # Находим игру в базе
                from games.models import Game
                game = Game.objects.filter(igdb_id=game_id).first()
                if not game:
                    failed_count += 1
                    if params['debug']:
                        self.stdout.write(f'      ❌ Игра {game_id} не найдена в базе данных')
                    continue

                # Проверяем текущую обложку
                current_cover_url = game.cover_url or ""

                # Если URL одинаковые, пропускаем
                if current_cover_url == new_cover_url:
                    if params['debug']:
                        self.stdout.write(f'      ⏭️  Обложка уже актуальна')
                    continue

                # Обновляем обложку
                try:
                    game.cover_url = new_cover_url
                    game.save(update_fields=['cover_url'])
                    updated_count += 1

                    update_details.append({
                        'game_name': game.name,
                        'game_id': game_id,
                        'old_cover': current_cover_url,
                        'new_cover': new_cover_url
                    })

                    if params['debug']:
                        self.stdout.write(f'      ✅ Обновлена обложка')
                        self.stdout.write(
                            f'      📍 Старая: {current_cover_url[:50]}...' if current_cover_url else '      📍 Старая: не было')
                        self.stdout.write(f'      📍 Новая: {new_cover_url[:50]}...')

                except Exception as e:
                    failed_count += 1
                    if params['debug']:
                        self.stderr.write(f'      ❌ Ошибка обновления обложки: {e}')

            iteration_time = time.time() - iteration_start_time

            # Вывод финальной статистики
            if params['debug']:
                self.stdout.write(f'\n' + '=' * 60)
                self.stdout.write(f'📊 ФИНАЛЬНАЯ СТАТИСТИКА ОБНОВЛЕНИЯ ОБЛОЖЕК')
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
            self.stderr.write(f'❌ ОШИБКА при обновлении обложек: {str(e)}')
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

    def _setup_update_covers_environment(self, options, debug, original_offset):
        """Настройка окружения для режима обновления обложек - оптимизированная"""
        from games.models import Game
        import time

        # НЕМЕДЛЕННО выводим только заголовок
        self.stdout.write(f'\n🖼️  ЗАПУСК ОБНОВЛЕНИЯ ОБЛОЖЕК ДЛЯ ИГР В БАЗЕ')
        self.stdout.write('=' * 60)
        self.stdout.write(f'📍 Начинаем с offset: {original_offset}')

        # Быстрая проверка общего количества
        total_in_db = Game.objects.count()
        if debug:
            self.stdout.write(f'📊 Всего игр в базе: {total_in_db}')

        # Определяем, нужно ли обновлять обложки всех игр или только по фильтрам
        update_all_covers = not any([
            options.get('game_names'),
            options.get('game_modes'),
            options.get('genres'),
            options.get('description_contains'),
            options.get('keywords')
        ])

        start_time = time.time()

        try:
            if update_all_covers:
                # Обновление обложек для всех игр
                if debug:
                    self.stdout.write(f'🎯 РЕЖИМ: ОБНОВЛЕНИЕ ВСЕХ ОБЛОЖЕК')
                updated_count = self.update_all_game_covers(options, debug)
            else:
                # Обновление обложек по фильтрам
                if debug:
                    self.stdout.write(f'🎯 РЕЖИМ: ОБНОВЛЕНИЕ ПО ФИЛЬТРАМ')
                updated_count = self.update_filtered_game_covers(options, debug)

            total_time = time.time() - start_time

            # Выводим финальное сообщение
            self.stdout.write(f'\n' + '=' * 60)
            self.stdout.write(f'✅ ОБНОВЛЕНИЕ ОБЛОЖЕК ЗАВЕРШЕНО!')
            self.stdout.write(f'🖼️  Обновлено обложек: {updated_count}')
            self.stdout.write(f'⏱️  Общее время: {total_time:.2f}с')

            if total_time > 0 and updated_count > 0:
                speed = updated_count / total_time
                self.stdout.write(f'🚀 Скорость обновления: {speed:.1f} обложек/сек')

            return None, None, None, None, None, None, True

        except KeyboardInterrupt:
            total_time = time.time() - start_time
            self.stdout.write(f'\n🛑 ОБНОВЛЕНИЕ ОБЛОЖЕК ПРЕРВАНО')
            self.stdout.write(f'⏱️  Время выполнения: {total_time:.2f}с')
            raise

        except Exception as e:
            total_time = time.time() - start_time
            self.stderr.write(f'\n❌ Ошибка при обновлении обложек: {str(e)}')
            self.stderr.write(f'⏱️  Время до ошибки: {total_time:.2f}с')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            return None, None, None, None, None, None, True

    def update_all_game_covers(self, options, debug=False):
        """Обновляет обложки для всех игр в базе - с прогресс-баром"""
        from games.models import Game
        import time
        import concurrent.futures

        # ОПТИМАЛЬНЫЕ НАСТРОЙКИ
        MAX_WORKERS = 3
        BATCH_SIZE = 10
        DELAY_BETWEEN_BATCHES = 0.4

        offset = options.get('offset', 0)
        limit = options.get('limit', 0)

        # Загружаем сохраненный offset если он 0
        if offset == 0 and not options.get('reset_offset', False):
            saved_offset = self._get_saved_offset_for_update_covers(options)
            if saved_offset is not None:
                offset = saved_offset

        # Получаем все игры из базы
        all_games_query = Game.objects.all().order_by('id')
        total_in_db = Game.objects.count()

        if offset > 0:
            all_games_query = all_games_query[offset:]

        all_game_ids = list(all_games_query.values_list('igdb_id', flat=True))

        if limit > 0:
            all_game_ids = all_game_ids[:limit]

        total_games = len(all_game_ids)

        if total_games == 0:
            self.stdout.write('❌ В базе нет игр для обновления обложек')
            return 0

        # ====== СОЗДАЕМ ПРОГРЕСС-БАР ТОЛЬКО В НЕ-DEBUG РЕЖИМЕ ======
        progress_bar = None
        if not debug:
            progress_bar = self._create_progress_bar()
            progress_bar.total_games = total_games
            progress_bar.desc = "Обновление обложек"
            progress_bar.update(
                total_loaded=0,
                current_iteration=1,
                iterations_without_new=0,
                created_count=0,
                updated_count=0,
                skipped_count=0,
                processed_count=0,
                errors=0
            )

        # Выводим заголовок ТОЛЬКО В DEBUG
        if debug:
            self.stdout.write(f'\n🎯 ЗАПУСК ОБНОВЛЕНИЯ ОБЛОЖЕК')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🎮 Всего игр в базе: {total_in_db}')
            self.stdout.write(f'📍 Будет обновлено: {total_games} игр')
            self.stdout.write(f'⚡ Параллельных воркеров: {MAX_WORKERS}')
            self.stdout.write(f'📦 Размер пачки: {BATCH_SIZE} игр')
            self.stdout.write(f'⏸️  Задержка между пачками: {DELAY_BETWEEN_BATCHES} сек')
            self.stdout.write('=' * 60)
        else:
            # В НЕ-debug режиме тоже показываем базовую инфо
            self.stdout.write(f'\n🖼️  ОБНОВЛЕНИЕ ОБЛОЖЕК ДЛЯ ИГР')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🎮 Всего игр в базе: {total_in_db}')
            self.stdout.write(f'📍 Будет обновлено: {total_games} игр')
            if offset > 0:
                self.stdout.write(f'📍 Начинаем с offset: {offset}')
            self.stdout.write('=' * 60)

        start_time = time.time()
        updated_count = 0
        processed_count = 0
        error_count = 0

        # Создаем мапу всех игр
        games_by_igdb_id = {}
        try:
            games = Game.objects.filter(igdb_id__in=all_game_ids).only('id', 'igdb_id', 'cover_url', 'name')
            for game in games:
                games_by_igdb_id[game.igdb_id] = game
        except Exception as e:
            self.stderr.write(f'❌ Ошибка загрузки игр из базы: {e}')
            if progress_bar:
                progress_bar.final_message("❌ Ошибка загрузки игр из базы")
                progress_bar.clear()
            return 0

        # Разбиваем на пачки
        all_batches = []
        for i in range(0, len(all_game_ids), BATCH_SIZE):
            batch = all_game_ids[i:i + BATCH_SIZE]
            if batch:
                all_batches.append(batch)

        total_batches = len(all_batches)

        # Главный цикл обработки
        all_updates = {}

        try:
            for group_start in range(0, total_batches, MAX_WORKERS):
                group_end = min(group_start + MAX_WORKERS, total_batches)
                current_group = all_batches[group_start:group_end]

                # В DEBUG: текстовый прогресс
                if debug:
                    progress_percent = (group_end / total_batches) * 100
                    games_processed = group_end * BATCH_SIZE
                    self.stdout.write(f'📊 Прогресс: {progress_percent:.1f}% ({games_processed}/{total_games} игр)')

                # Обрабатываем группу
                with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = []
                    for i, batch in enumerate(current_group):
                        future = executor.submit(
                            self._load_single_update_batch,
                            group_start + i + 1,
                            batch,
                            games_by_igdb_id,
                            debug
                        )
                        futures.append(future)

                    # Собираем результаты
                    for future in concurrent.futures.as_completed(futures):
                        try:
                            batch_updated, batch_updates = future.result(timeout=20)
                            updated_count += batch_updated

                            # Собираем обновления
                            for update in batch_updates:
                                if isinstance(update, dict) and 'id' in update and 'cover_url' in update:
                                    all_updates[update['id']] = update['cover_url']

                        except concurrent.futures.TimeoutError:
                            error_count += 1
                        except Exception as e:
                            error_count += 1
                            if debug:
                                self.stderr.write(f'   ❌ Ошибка future: {e}')

                # Обновляем счетчики
                for batch in current_group:
                    processed_count += len(batch)

                # ====== ОБНОВЛЯЕМ ПРОГРЕСС-БАР ======
                if progress_bar and not debug:
                    progress_bar.update(
                        total_loaded=processed_count,
                        current_iteration=1,
                        iterations_without_new=0,
                        created_count=0,
                        updated_count=updated_count,
                        skipped_count=processed_count - updated_count,
                        processed_count=processed_count,
                        errors=error_count
                    )

                # Пауза для rate limiting
                if group_end < total_batches:
                    time.sleep(DELAY_BETWEEN_BATCHES)

        except KeyboardInterrupt:
            # Сохраняем offset при прерывании
            next_offset = offset + processed_count
            self._save_offset_for_update_covers(options, next_offset)

            if progress_bar:
                progress_bar.final_message(f"🛑 Прервано на {processed_count}/{total_games} игр")
                progress_bar.clear()

            self.stdout.write(f'\n🛑 ОБНОВЛЕНИЕ ПРЕРВАНО')
            self.stdout.write(f'📍 Обработано: {processed_count}/{total_games} игр')
            self.stdout.write(f'📍 Обновлено: {updated_count} обложек')
            self.stdout.write(f'📍 Следующий offset: {next_offset}')
            raise

        # Массовое сохранение
        if all_updates:
            try:
                from django.db.models import Case, When, Value
                from django.db.models import CharField

                when_conditions = []
                for game_id, cover_url in all_updates.items():
                    when_conditions.append(When(id=game_id, then=Value(cover_url)))

                Game.objects.filter(id__in=all_updates.keys()).update(
                    cover_url=Case(*when_conditions, default=Value(''), output_field=CharField())
                )

            except Exception as e:
                if debug:
                    self.stderr.write(f'❌ Ошибка массового сохранения: {e}')

        # Сохраняем offset
        next_offset = offset + processed_count
        self._save_offset_for_update_covers(options, next_offset)

        total_time = time.time() - start_time

        # ====== ФИНАЛЬНОЕ СООБЩЕНИЕ ПРОГРЕСС-БАРА ======
        if progress_bar and not debug:
            if updated_count > 0:
                progress_bar.final_message(f"✅ Обновлено {updated_count}/{total_games} обложек")
            else:
                progress_bar.final_message(f"⚠️  Нет обновлений ({processed_count} игр проверено)")
            progress_bar.clear()

        # Статистика
        self.stdout.write(f'\n📊 ИТОГОВАЯ СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'👀 Обработано игр: {processed_count}/{total_games}')
        self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
        self.stdout.write(f'⏭️  Пропущено (уже актуальны): {processed_count - updated_count}')
        self.stdout.write(f'❌ Ошибок: {error_count}')
        self.stdout.write(f'⏱️  Время: {total_time:.2f}с')

        if total_time > 0:
            games_per_second = processed_count / total_time
            self.stdout.write(f'🚀 Скорость: {games_per_second:.1f} игр/сек')

        return updated_count

    def update_filtered_game_covers(self, options, debug=False):
        """Обновляет обложки для игр по фильтрам - максимально оптимизированная версия"""
        from games.models import Game
        from django.db.models import Q
        import time
        import concurrent.futures
        from games.igdb_api import make_igdb_request

        offset = options.get('offset', 0)
        limit = options.get('limit', 0)
        game_names = options.get('game_names', '')
        game_modes = options.get('game_modes', '')
        genres = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords = options.get('keywords', '')

        # Если указаны конкретные имена игр - игнорируем offset
        if game_names:
            offset = 0

        query = Game.objects.all()

        if game_names:
            name_list = [n.strip() for n in game_names.split(',') if n.strip()]
            name_filters = Q()
            for name in name_list:
                name_filters |= Q(name__icontains=name)
            query = query.filter(name_filters)

        if genres:
            genre_list = [g.strip() for g in genres.split(',') if g.strip()]
            for genre in genre_list:
                query = query.filter(genres__name__icontains=genre)

        if description_contains:
            text = description_contains
            query = query.filter(Q(summary__icontains=text) | Q(storyline__icontains=text))

        if keywords:
            keyword_list = [k.strip() for k in keywords.split(',') if k.strip()]
            for keyword in keyword_list:
                query = query.filter(keywords__name__icontains=keyword)

        query = query.order_by('id')

        # Получаем только ID игр
        games = list(query.values_list('id', 'igdb_id', 'cover_url', 'name'))
        total_games = len(games)

        if total_games == 0:
            self.stdout.write('❌ Не найдено игр для обновления обложек')
            return 0

        if limit > 0:
            games = games[:limit]
            total_games = len(games)

        start_time = time.time()
        updated_count = 0
        error_count = 0

        # 1. Подготавливаем данные для быстрого доступа
        games_by_id = {}
        games_by_igdb_id = {}
        for game_id, igdb_id, cover_url, name in games:
            games_by_id[game_id] = {
                'igdb_id': igdb_id,
                'cover_url': cover_url,
                'name': name
            }
            games_by_igdb_id[igdb_id] = game_id

        # 2. Получаем все IGDB ID
        all_igdb_ids = [igdb_id for _, igdb_id, _, _ in games]

        if debug:
            self.stdout.write(f'\n🎯 ОБНОВЛЕНИЕ ОБЛОЖЕК ПО ФИЛЬТРАМ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'👀 Найдено игр по фильтрам: {total_games}')
            self.stdout.write(f'⚡ Параллельных воркеров: 8')
            self.stdout.write(f'📦 Размер пачки: 10 игр')
            self.stdout.write('=' * 60)

        # 3. Разбиваем на пачки по 10 игр
        BATCH_SIZE = 10
        MAX_WORKERS = 8
        all_batches = []

        for i in range(0, len(all_igdb_ids), BATCH_SIZE):
            batch = all_igdb_ids[i:i + BATCH_SIZE]
            if batch:
                all_batches.append(batch)

        total_batches = len(all_batches)

        if debug:
            self.stdout.write(f'📊 Создано {total_batches} пачек')

        # 4. Функция обработки одной пачки
        def process_batch(batch_num, batch_igdb_ids):
            try:
                # Запрос к IGDB
                id_list = ','.join(map(str, batch_igdb_ids))
                query = f'fields id,cover.image_id; where id = ({id_list});'

                games_data = make_igdb_request('games', query, debug=False)
                if not games_data:
                    return batch_num, [], 0

                # Собираем обновления
                updates = []
                local_updated = 0

                for game_data in games_data:
                    igdb_id = game_data.get('id')
                    if not igdb_id or igdb_id not in games_by_igdb_id:
                        continue

                    game_id = games_by_igdb_id[igdb_id]
                    game_info = games_by_id[game_id]

                    cover_data = game_data.get('cover', {})
                    image_id = cover_data.get('image_id')

                    if not image_id:
                        continue

                    new_cover_url = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"
                    current_cover_url = game_info['cover_url'] or ""

                    if current_cover_url != new_cover_url:
                        updates.append({
                            'id': game_id,
                            'cover_url': new_cover_url
                        })
                        local_updated += 1

                return batch_num, updates, len(batch_igdb_ids), local_updated

            except Exception as e:
                if debug:
                    self.stderr.write(f'      ❌ Ошибка в пачке {batch_num}: {e}')
                return batch_num, [], len(batch_igdb_ids), 0

        # 5. Параллельная обработка
        all_updates = []
        processed_count = 0

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Запускаем задачи
            future_to_batch = {}
            for batch_num, batch_ids in enumerate(all_batches, 1):
                future = executor.submit(process_batch, batch_num, batch_ids)
                future_to_batch[future] = batch_num

            # Собираем результаты
            completed = 0

            for future in concurrent.futures.as_completed(future_to_batch):
                batch_num = future_to_batch[future]
                try:
                    batch_num, updates, processed, local_updated = future.result(timeout=15)

                    all_updates.extend(updates)
                    updated_count += local_updated
                    processed_count += processed
                    completed += 1

                    if debug and completed % 10 == 0:
                        progress = (completed / total_batches) * 100
                        self.stdout.write(f'   📊 Прогресс: {progress:.1f}% ({completed}/{total_batches})')

                except concurrent.futures.TimeoutError:
                    error_count += 1
                    if debug:
                        self.stdout.write(f'   ⏱️  Таймаут пачки {batch_num}')
                except Exception as e:
                    error_count += 1
                    if debug:
                        self.stderr.write(f'   ❌ Ошибка пачки {batch_num}: {e}')

        # 6. Массовое обновление
        if all_updates:
            try:
                # Группируем обновления
                update_dict = {}
                for update in all_updates:
                    update_dict[update['id']] = update['cover_url']

                # Обновляем игры через bulk_update
                games_to_update = []
                for game_id, cover_url in update_dict.items():
                    # Нужно получить объекты Game
                    try:
                        game = Game.objects.get(id=game_id)
                        game.cover_url = cover_url
                        games_to_update.append(game)
                    except Game.DoesNotExist:
                        continue

                if games_to_update:
                    Game.objects.bulk_update(games_to_update, ['cover_url'])

            except Exception as e:
                if debug:
                    self.stderr.write(f'   ❌ Ошибка массового обновления: {e}')
                # Fallback: обновляем по одной
                for update in all_updates:
                    try:
                        Game.objects.filter(id=update['id']).update(cover_url=update['cover_url'])
                    except Exception:
                        continue

        total_time = time.time() - start_time

        # 7. Вывод статистики
        self.stdout.write(f'\n' + '=' * 60)
        self.stdout.write(f'📊 ИТОГОВАЯ СТАТИСТИКА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'👀 Обработано игр: {processed_count}/{total_games}')
        self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
        self.stdout.write(f'⏭️  Пропущено (уже актуальны): {processed_count - updated_count}')
        self.stdout.write(f'❌ Ошибок: {error_count}')
        self.stdout.write(f'⏱️  Общее время: {total_time:.2f}с')

        if total_time > 0:
            games_per_second = processed_count / total_time
            self.stdout.write(f'🚀 Скорость обработки: {games_per_second:.1f} игр/сек')

        return updated_count

    def _display_update_covers_final_stats(self, offset, processed_count, total_games,
                                           updated_count, total_time):
        self.stdout.write(f'\n' + '=' * 60)
        self.stdout.write(f'📊 ИТОГОВАЯ СТАТИСТИКА ОБНОВЛЕНИЯ ОБЛОЖЕК')
        self.stdout.write('=' * 60)

        if offset > 0:
            self.stdout.write(f'📍 Начальный offset: {offset}')
            next_offset = offset + processed_count
            self.stdout.write(f'📍 Следующий offset для продолжения: {next_offset}')
        else:
            self.stdout.write(f'📍 Обработано игр: {processed_count}/{total_games}')

        self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
        self.stdout.write(f'⏭️  Пропущено (обложки актуальны): {processed_count - updated_count}')
        self.stdout.write(f'⏱️  Общее время: {total_time:.2f}с')

        if total_time > 0:
            games_per_second = processed_count / total_time
            self.stdout.write(f'🚀 Скорость обработки: {games_per_second:.1f} игр/сек')

            if updated_count > 0:
                updates_per_second = updated_count / total_time
                self.stdout.write(f'📈 Скорость обновления: {updates_per_second:.1f} обложек/сек')

    def update_game_cover(self, game, game_data, data_maps, details, debug=False):
        """Обновляет обложку игры без проверки доступности"""
        if not game_data.get('cover'):
            return False

        cover_id = game_data['cover']
        if cover_id not in data_maps.get('cover_map', {}):
            return False

        new_cover_url = data_maps['cover_map'][cover_id]

        # Заменяем t_thumb на t_cover_big если есть
        if 't_thumb' in new_cover_url:
            new_cover_url = new_cover_url.replace('t_thumb', 't_cover_big')

        # Убеждаемся, что это JPG формат
        if not new_cover_url.endswith('.jpg'):
            if new_cover_url.endswith('.webp'):
                new_cover_url = new_cover_url.replace('.webp', '.jpg')
            else:
                new_cover_url += '.jpg'

        current_url = game.cover_url or ""

        if debug:
            self.stdout.write(f'   🔍 Обновление обложки для {game.name}:')
            self.stdout.write(f'      Текущая URL: {current_url}')
            self.stdout.write(f'      Новая URL: {new_cover_url}')

        # Если URL совпадают - пропускаем
        if current_url == new_cover_url:
            if debug:
                self.stdout.write(f'   ⏭️  Обложка уже актуальна: {current_url}')
            return False

        # Обновляем обложку без проверки доступности
        game.cover_url = new_cover_url
        if 'cover_url' not in details['updated_fields']:
            details['updated_fields'].append('cover_url')
        details['cover_url'] = new_cover_url

        if debug:
            self.stdout.write(f'   🖼️  Обновляем обложку')
        return True

    def _setup_execution_environment(self, options):
        """Основная настройка окружения выполнения команды"""
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
        update_covers = options.get('update_covers', False)  # НОВОЕ

        # ВЫВОДИМ ИНФОРМАЦИЮ О OFFSET В НАЧАЛЕ
        self.stdout.write(f'📍 Указанный offset: {original_offset}')

        # Получаем фактический offset (с учетом сохраненного)
        actual_offset = self._display_offset_info(options, original_offset)

        # Проверяем режим обновления обложек (ВЫСШИЙ ПРИОРИТЕТ)
        if update_covers:
            return self._setup_update_covers_environment(options, debug, actual_offset)

        # Проверяем режим обновления данных
        elif update_missing_data:
            return self._setup_update_mode_environment(options, debug, actual_offset)

        # Если не режим обновления, продолжаем стандартную настройку
        return self._setup_standard_environment(options, debug, repeat_count, actual_offset,
                                                limit, iteration_limit, clear_cache, reset_offset)

    def _display_offset_info(self, options, original_offset):
        """Показывает информацию об offset"""
        if options.get('reset_offset', False):
            self.stdout.write('🔄 Offset сброшен по запросу')
            return original_offset

        if original_offset == 0:
            saved_offset = self._get_saved_offset(options)
            if saved_offset is not None:
                self.stdout.write(f'📍 Начинаем с сохраненного offset: {saved_offset}')
                return saved_offset

        return original_offset

    def _save_offset_for_continuation(self, options, current_offset):
        """Сохраняет offset для продолжения"""
        params = self._get_offset_params(options)
        saved = OffsetManager.save_offset(params, current_offset)

        if saved:
            self.stdout.write(f'💾 Offset сохранен для продолжения: {current_offset}')

        return saved

    def _handle_update_mode_with_filters(self, options, debug):
        """Обрабатывает режим обновления данных с фильтрами и offset"""
        from games.models import Game
        from django.db.models import Q
        import time

        # Получаем параметры
        offset = options.get('offset', 0)
        limit = options.get('limit', 0)
        game_names = options.get('game_names', '')
        game_modes = options.get('game_modes', '')
        genres = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords = options.get('keywords', '')

        # Строим запрос с фильтрами
        query = Game.objects.all()

        # Применяем фильтры
        if game_names:
            name_list = [n.strip() for n in game_names.split(',') if n.strip()]
            name_filters = Q()
            for name in name_list:
                name_filters |= Q(name__icontains=name)
            query = query.filter(name_filters)
            if debug:
                self.stdout.write(f'   🔍 Фильтр по именам: {name_list}')

        if genres:
            # Для жанров нужно искать через M2M связь
            genre_list = [g.strip() for g in genres.split(',') if g.strip()]
            for genre in genre_list:
                query = query.filter(genres__name__icontains=genre)
            if debug:
                self.stdout.write(f'   🔍 Фильтр по жанрам: {genre_list}')

        if description_contains:
            text = description_contains
            query = query.filter(Q(summary__icontains=text) | Q(storyline__icontains=text))
            if debug:
                self.stdout.write(f'   🔍 Фильтр по тексту: "{text}"')

        if keywords:
            keyword_list = [k.strip() for k in keywords.split(',') if k.strip()]
            for keyword in keyword_list:
                query = query.filter(keywords__name__icontains=keyword)
            if debug:
                self.stdout.write(f'   🔍 Фильтр по ключевым словам: {keyword_list}')

        # Для game_modes пока пропускаем, так как нужен более сложный запрос
        if game_modes and debug:
            self.stdout.write(f'   ⚠️  Фильтр по режимам игры пока не поддерживается в режиме обновления')

        # Сортируем для стабильного offset
        query = query.order_by('id')

        # Применяем offset
        if offset > 0:
            query = query[offset:]

        # Получаем ID игр
        if limit > 0:
            game_ids = list(query.values_list('igdb_id', flat=True)[:limit])
        else:
            game_ids = list(query.values_list('igdb_id', flat=True))

        total_games = len(game_ids)

        if debug:
            self.stdout.write(f'   📊 Найдено игр по фильтрам: {total_games}')
            self.stdout.write(f'   📍 Начальный offset: {offset}')
            if limit > 0:
                self.stdout.write(f'   🎯 Лимит обновления: {limit} игр')

        if total_games == 0:
            self.stdout.write('❌ Не найдено игр для обновления')
            return 0, []

        # Создаем прогресс-бар только если не debug
        progress_bar = None
        if not debug:
            progress_bar = self._create_progress_bar()
            progress_bar.total_games = total_games
            progress_bar.desc = "Обновление данных игр"
            progress_bar.update(
                total_loaded=0,
                current_iteration=1,
                iterations_without_new=0,
                created_count=0,
                updated_count=0,
                skipped_count=0,
                processed_count=0,
                errors=0
            )

        start_time = time.time()
        updated_count = 0
        update_details = []

        try:
            # Используем update_missing_game_data для каждой игры отдельно
            # Это гарантирует загрузку всех недостающих данных, включая скриншоты и M2M связи
            for i, game_id in enumerate(game_ids, 1):
                if progress_bar:
                    # Обновляем прогресс-бар
                    progress_bar.update(
                        total_loaded=i,
                        current_iteration=1,
                        iterations_without_new=0,
                        created_count=0,
                        updated_count=updated_count,
                        skipped_count=i - updated_count - 1,
                        processed_count=i,
                        errors=0
                    )

                if debug:
                    self.stdout.write(f'\n   🔄 [{i}/{total_games}] Обновление игры ID: {game_id}')

                # Получаем имя игры для логов
                game = Game.objects.filter(igdb_id=game_id).first()
                if not game:
                    if debug:
                        self.stdout.write(f'      ❌ Игра с ID {game_id} не найдена в базе')
                    continue

                # Проверяем, каких данных не хватает (теперь получаем 3 значения)
                missing_data, missing_count, cover_status = self.check_missing_game_data(game)

                if debug:
                    self.stdout.write(f'      🔍 "{game.name}": {missing_count} недостающих данных')
                    self.stdout.write(f'      📋 Статус обложки: {cover_status}')
                    if missing_count > 0:
                        missing_list = [key.replace('has_', '') for key, has_data in missing_data.items() if
                                        not has_data]
                        self.stdout.write(f'      📋 Отсутствует: {", ".join(missing_list)}')

                # Если все данные уже есть - пропускаем
                if missing_count == 0:
                    if debug:
                        self.stdout.write(f'      ⏭️  Все данные уже есть, пропускаем')
                    continue

                # Выполняем обновление недостающих данных
                success, details = self.update_missing_game_data(game_id, debug)

                if success:
                    updated_count += 1

                    update_details.append({
                        'game_name': game.name,
                        'game_id': game_id,
                        'details': details
                    })

                    if debug:
                        if details.get('updated_fields'):
                            self.stdout.write(f'      ✅ Обновлены поля: {", ".join(details["updated_fields"])}')
                        if details.get('updated_relations'):
                            self.stdout.write(f'      🔗 Добавлены связи: {", ".join(details["updated_relations"])}')
                        if details.get('screenshots_added', 0) > 0:
                            self.stdout.write(f'      📸 Добавлено скриншотов: {details["screenshots_added"]}')
                else:
                    if debug:
                        self.stdout.write(f'      ❌ Не удалось обновить: {game.name}')

                total_time = time.time() - start_time

                # Финальное сообщение прогресс-бара
                if progress_bar:
                    progress_bar.final_message(
                        f"✅ Обновлено: {updated_count} | ⏭️  Пропущено: {total_games - updated_count}"
                    )
                    progress_bar.clear()

                # Сохраняем offset для продолжения
                next_offset = offset + total_games
                self._save_offset_for_continuation(options, next_offset)

                if debug:
                    self.stdout.write(f'\n' + '=' * 60)
                    self.stdout.write(f'📊 ИТОГОВАЯ СТАТИСТИКА ОБНОВЛЕНИЯ:')
                    self.stdout.write('=' * 60)
                    self.stdout.write(f'📍 Обработано игр: {total_games}')
                    self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
                    self.stdout.write(f'⏭️  Пропущено (все данные уже есть): {total_games - updated_count}')
                    self.stdout.write(f'⏱️  Время: {total_time:.2f}с')
                    self.stdout.write(f'📍 Следующий offset для продолжения: {next_offset}')

                return updated_count, update_details

        except KeyboardInterrupt:
            # Получаем количество обработанных игр
            if progress_bar:
                processed_count = progress_bar.total_loaded
            else:
                processed_count = len(update_details) + (total_games - updated_count)

            # Рассчитываем новый offset
            next_offset = offset + processed_count

            # Сохраняем offset
            self._save_offset_for_continuation(options, next_offset)

            # Финальное сообщение прогресс-бара
            if progress_bar:
                progress_bar.final_message(
                    f"🛑 Прервано на: {processed_count}/{total_games} игр"
                )
                progress_bar.clear()

            self.stdout.write(f'\n🛑 КОМАНДА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')
            self.stdout.write(f'📍 Обработано игр: {processed_count}/{total_games}')
            self.stdout.write(f'📍 Следующий offset: {next_offset}')

            # Пробрасываем исключение дальше
            raise

    def _handle_update_mode_iteration(self, params, actual_offset, actual_limit,
                                      iteration_start_time, debug, iteration):
        """Обрабатывает итерацию в режиме обновления всех игр"""
        from games.models import Game

        # Получаем параметры из options
        options = self.current_options

        if debug:
            self.stdout.write(f'\n🔄 ИТЕРАЦИЯ {iteration}: ОБНОВЛЕНИЕ ВСЕХ ИГР')
            self.stdout.write('=' * 60)
            self.stdout.write(f'📍 Начальный offset: {actual_offset}')
            self.stdout.write(f'📊 Лимит итерации: {actual_limit} игр')

            # Показываем фильтры если есть
            filters = []
            if options.get('game_names'):
                filters.append(f'имена: {options["game_names"]}')
            if options.get('genres'):
                filters.append(f'жанры: {options["genres"]}')
            if options.get('description_contains'):
                filters.append(f'текст: {options["description_contains"]}')
            if options.get('keywords'):
                filters.append(f'ключевые слова: {options["keywords"]}')

            if filters:
                self.stdout.write(f'🔍 Фильтры: {", ".join(filters)}')

        # Получаем ID игр с учетом offset и limit
        query = Game.objects.all().order_by('id')

        if actual_offset > 0:
            query = query[actual_offset:]

        if actual_limit > 0:
            game_ids = list(query.values_list('igdb_id', flat=True)[:actual_limit])
        else:
            game_ids = list(query.values_list('igdb_id', flat=True))

        total_games = len(game_ids)

        if total_games == 0:
            if debug:
                self.stdout.write('⚠️  Не найдено игр для обновления')
            return {
                'total_games_checked': 0,
                'total_games_found': 0,
                'created_count': 0,
                'skipped_count': 0,
                'updated_count': 0,
                'total_time': time.time() - iteration_start_time,
                'errors': 0,
                'last_checked_offset': actual_offset,
                'limit_reached': False,
                'limit_reached_at_offset': None,
            }

        if debug:
            self.stdout.write(f'🎯 Будет обновлено игр: {total_games}')

        # Выполняем обновление
        updated_count, update_details = self.update_multiple_games_data(
            game_ids, debug, None  # Без прогресс-бара в debug режиме
        )

        total_time = time.time() - iteration_start_time

        if debug:
            self.stdout.write(f'\n📊 СТАТИСТИКА ИТЕРАЦИИ:')
            self.stdout.write(f'   • Обработано игр: {total_games}')
            self.stdout.write(f'   • Успешно обновлено: {updated_count}')
            self.stdout.write(f'   • Пропущено: {total_games - updated_count}')
            self.stdout.write(f'   • Время: {total_time:.2f}с')

            if total_time > 0:
                speed = total_games / total_time
                self.stdout.write(f'   • Скорость: {speed:.1f} игр/сек')

        # Рассчитываем следующий offset
        last_checked_offset = actual_offset + total_games

        return {
            'total_games_checked': total_games,
            'total_games_found': total_games,
            'created_count': 0,
            'skipped_count': 0,
            'updated_count': updated_count,
            'total_time': total_time,
            'errors': 0,
            'last_checked_offset': last_checked_offset,
            'limit_reached': total_games == 0 or (actual_limit > 0 and total_games < actual_limit),
            'limit_reached_at_offset': last_checked_offset if total_games == 0 else None,
        }

    def update_all_games_missing_data(self, options, debug=False):
        """Обновляет недостающие данные для всех игр в базе с поддержкой offset"""
        from games.models import Game
        import time

        # Создаем папку для логов
        log_dir = self._ensure_logs_directory(debug)

        # Получаем параметры offset и limit
        offset = options.get('offset', 0)
        limit = options.get('limit', 0)

        # Загружаем сохраненный offset если он 0 и не было сброса
        if offset == 0 and not options.get('reset_offset', False):
            saved_offset = self._get_saved_offset(options)
            if saved_offset is not None:
                offset = saved_offset
                self.stdout.write(f'📍 Используем сохраненный offset: {offset}')

        # Получаем все игры из базы с сортировкой по ID
        all_games_query = Game.objects.all().order_by('id')

        # Показываем сколько всего игр в базе
        total_in_db = Game.objects.count()

        # Применяем offset
        if offset > 0:
            all_games_query = all_games_query[offset:]
            remaining_games = total_in_db - offset
            self.stdout.write(f'📍 Игр осталось для обновления: {remaining_games}')

        # Получаем ID игр
        all_game_ids = list(all_games_query.values_list('igdb_id', flat=True))

        # Применяем limit если указан
        if limit > 0:
            all_game_ids = all_game_ids[:limit]

        total_games = len(all_game_ids)

        if total_games == 0:
            self.stdout.write('❌ В базе нет игр для обновления')
            return 0, []

        # Создаем прогресс-бар только если не debug
        progress_bar = None
        if not debug:
            progress_bar = self._create_progress_bar()
            progress_bar.total_games = total_games
            progress_bar.desc = "Обновление данных игр"
            progress_bar.update(
                total_loaded=0,
                current_iteration=1,
                iterations_without_new=0,
                created_count=0,
                updated_count=0,
                skipped_count=0,
                processed_count=0,
                errors=0
            )

        if debug:
            self.stdout.write(f'\n🎯 ЗАПУСК ОБНОВЛЕНИЯ ДАННЫХ ДЛЯ ВСЕХ ИГР В БАЗЕ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'🎮 Всего игр в базе: {total_in_db}')
            self.stdout.write(f'📍 Начинаем с offset: {offset}')
            self.stdout.write(f'📍 Будет обновлено игр: {total_games}')
            if limit > 0:
                self.stdout.write(f'🎯 Лимит обновления: {limit} игр')
            self.stdout.write(f'🔄 Размер пачки: 10 игр')
        else:
            # В не-debug режиме тоже показываем базовую информацию
            self.stdout.write(f'🎯 Всего игр в базе: {total_in_db}')
            self.stdout.write(f'📍 Начинаем с offset: {offset}')
            self.stdout.write(f'📍 Будет обновлено: {total_games} игр')
            if limit > 0:
                self.stdout.write(f'🎯 Лимит: {limit} игр')
            self.stdout.write('=' * 60)

        start_time = time.time()
        processed_count = 0  # Добавляем счетчик обработанных игр

        try:
            # Используем батчевый метод с прогресс-баром
            updated_count, update_details = self.update_multiple_games_data(
                all_game_ids, debug, progress_bar
            )

            total_time = time.time() - start_time

            # Получаем реальное количество обработанных игр из прогресс-бара
            if progress_bar:
                processed_count = progress_bar.total_loaded
            else:
                processed_count = updated_count + len(
                    [d for d in update_details if not d['details'].get('updated_fields')])

            # Сохраняем offset для продолжения
            next_offset = offset + processed_count
            self._save_offset_for_continuation(options, next_offset)

            # Выводим финальную статистику
            self._display_update_final_stats(offset, processed_count, total_games, updated_count, total_time)

        except KeyboardInterrupt:
            # Получаем количество обработанных игр из прогресс-бара
            if progress_bar:
                processed_count = progress_bar.total_loaded
            else:
                # Если нет прогресс-бара, используем атрибут
                processed_count = getattr(self, '_last_processed_count', 0)

            # Рассчитываем новый offset
            next_offset = offset + processed_count

            # Сохраняем offset
            self._save_offset_for_continuation(options, next_offset)

            self.stdout.write(f'\n🛑 КОМАНДА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')
            self.stdout.write(f'📍 Обработано игр: {processed_count}')
            self.stdout.write(f'📍 Следующий offset: {next_offset}')

            # Пробрасываем исключение дальше
            raise

        return updated_count, update_details

    def _display_update_final_stats(self, offset, processed_count, total_games, updated_count, total_time):
        """Выводит финальную статистику обновления"""
        self.stdout.write(f'\n' + '=' * 60)
        self.stdout.write(f'📊 ИТОГОВАЯ СТАТИСТИКА ОБНОВЛЕНИЯ ВСЕХ ИГР')
        self.stdout.write('=' * 60)
        self.stdout.write(f'📍 Начальный offset: {offset}')
        self.stdout.write(f'📍 Обработано игр: {processed_count}/{total_games}')
        self.stdout.write(f'✅ Успешно обновлено: {updated_count}')
        self.stdout.write(f'⏭️  Пропущено (данные уже есть): {processed_count - updated_count}')
        self.stdout.write(f'⏱️  Общее время: {total_time:.2f}с')

        next_offset = offset + processed_count
        self.stdout.write(f'📍 Следующий offset для продолжения: {next_offset}')

        if total_time > 0:
            games_per_second = processed_count / total_time
            self.stdout.write(f'🚀 Скорость обработки: {games_per_second:.1f} игр/сек')

            if updated_count > 0:
                updates_per_second = updated_count / total_time
                self.stdout.write(f'📈 Скорость обновления: {updates_per_second:.1f} игр/сек')

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

    def update_multiple_games_data(self, game_ids, debug=False, progress_bar=None):
        """Батчевое обновление данных с прогресс-баром"""
        if not game_ids:
            return 0, []

        import concurrent.futures
        from games.igdb_api import make_igdb_request

        updated_count = 0
        all_update_details = []

        # ИГДБ лимит: максимум 10 ID в IN clause
        BATCH_SIZE = 10
        MAX_PARALLEL_BATCHES = 4  # 4 пачки × 10 = 40 игр одновременно
        total_batches = (len(game_ids) + BATCH_SIZE - 1) // BATCH_SIZE

        # Счетчики для статистики
        processed_count = 0
        skipped_count = 0
        error_count = 0

        if debug:
            self.stdout.write(f'   📊 Начинаем обработку {len(game_ids)} игр')
            self.stdout.write(f'   🎯 Размер пачки: {BATCH_SIZE} игр (лимит IGDB)')
            self.stdout.write(f'   ⚡ Параллельных пачек: {MAX_PARALLEL_BATCHES}')
            self.stdout.write(f'   🔄 Всего пачек: {total_batches}')

        # Получаем все игры из базы
        try:
            existing_games = Game.objects.filter(igdb_id__in=game_ids)
            games_map = {game.igdb_id: game for game in existing_games}
            if debug:
                self.stdout.write(f'   ✅ Найдено игр в базе: {len(games_map)}/{len(game_ids)}')
        except Exception as e:
            self.stderr.write(f'   ❌ Ошибка загрузки игр из базы: {e}')
            return 0, []

        # Инициализируем прогресс-бар если есть
        if progress_bar and not debug:
            progress_bar.total_games = len(game_ids)
            progress_bar.desc = "Обновление данных игр"
            progress_bar.update(
                total_loaded=0,
                updated_count=0,
                current_iteration=1,
                iterations_without_new=0,
                created_count=0,
                failed_count=0,
                skipped_count=0,
                processed_count=0,
                errors=0
            )

        # Обрабатываем пачки группами для параллелизма
        for group_start in range(0, total_batches, MAX_PARALLEL_BATCHES):
            try:
                group_end = min(group_start + MAX_PARALLEL_BATCHES, total_batches)
                group_size = group_end - group_start

                # Счетчики для группы
                group_updated = 0
                group_processed = 0
                group_skipped = 0

                if debug:
                    self.stdout.write(
                        f'\n   📦 ГРУППА {group_start // MAX_PARALLEL_BATCHES + 1}: {group_size} пачек × {BATCH_SIZE} игр')
                    self.stdout.write('   ' + '=' * 40)

                # Подготавливаем пачки для параллельной загрузки
                batch_futures = {}

                with concurrent.futures.ThreadPoolExecutor(max_workers=group_size) as executor:
                    # Запускаем параллельную загрузку пачек
                    for batch_in_group in range(group_size):
                        batch_num = group_start + batch_in_group
                        start_idx = batch_num * BATCH_SIZE
                        end_idx = min(start_idx + BATCH_SIZE, len(game_ids))
                        batch_ids = game_ids[start_idx:end_idx]

                        if not batch_ids:
                            continue

                        # Запускаем загрузку пачки в отдельном потоке
                        future = executor.submit(
                            self._load_single_update_batch,
                            batch_num, batch_ids, games_map, debug
                        )
                        batch_futures[future] = (batch_num, batch_ids)

                    # Обрабатываем результаты
                    for future in concurrent.futures.as_completed(batch_futures):
                        batch_num, batch_ids = batch_futures[future]

                        try:
                            batch_updated, batch_details = future.result(timeout=30)

                            # Рассчитываем статистику для пачки
                            batch_processed = len(batch_ids)
                            batch_skipped = batch_processed - batch_updated

                            # Обновляем общие счетчики
                            group_updated += batch_updated
                            group_processed += batch_processed
                            group_skipped += batch_skipped

                            updated_count += batch_updated
                            processed_count += batch_processed
                            skipped_count += batch_skipped
                            all_update_details.extend(batch_details)

                            if debug:
                                if batch_updated > 0:
                                    self.stdout.write(f'   ✅ Пачка {batch_num + 1}: {batch_updated} обновлений')
                                else:
                                    self.stdout.write(
                                        f'   ⏭️  Пачка {batch_num + 1}: нет обновлений (пропущено {batch_skipped})')

                            # ОБНОВЛЯЕМ ПРОГРЕСС-БАР после каждой пачки
                            if progress_bar and not debug:
                                progress_bar.update(
                                    total_loaded=processed_count,  # сколько игр обработано
                                    current_iteration=group_start // MAX_PARALLEL_BATCHES + 1,
                                    iterations_without_new=0 if group_updated > 0 else 1,
                                    created_count=0,  # в режиме обновления созданий нет
                                    updated_count=updated_count,  # сколько игр обновлено
                                    skipped_count=skipped_count,  # сколько игр пропущено
                                    processed_count=processed_count,  # сколько обработано
                                    errors=error_count  # количество ошибок
                                )

                        except concurrent.futures.TimeoutError:
                            error_count += 1
                            if debug:
                                self.stderr.write(f'   ⏱️  Пачка {batch_num + 1}: ТАЙМАУТ (30 сек)')
                        except Exception as e:
                            error_count += 1
                            if debug:
                                self.stderr.write(f'   ❌ Пачка {batch_num + 1}: {str(e)[:100]}')

                # Вывод прогресса только в debug
                if debug:
                    progress_percent = (group_end / total_batches) * 100
                    self.stdout.write(f'\n   📊 Прогресс группы:')
                    self.stdout.write(f'      • Обработано игр: {group_processed}')
                    self.stdout.write(f'      • Обновлено: {group_updated}')
                    self.stdout.write(f'      • Пропущено: {group_skipped}')
                    self.stdout.write(
                        f'   📈 Общий прогресс: {progress_percent:.1f}% ({group_end}/{total_batches} пачек)')
                    self.stdout.write(f'   🎯 Обновлено всего: {updated_count} игр')

                # Сохраняем текущее количество обработанных игр для возможного прерывания
                self._last_processed_count = processed_count

                # Пауза между группами (только если не debug чтобы не замедлять)
                if group_end < total_batches and not debug:
                    time.sleep(0.5)

            except KeyboardInterrupt:
                # Сохраняем последнее количество обработанных игр
                self._last_processed_count = processed_count
                raise

        # Итог в debug
        if debug:
            self.stdout.write(f'\n' + '=' * 60)
            self.stdout.write(f'   🎯 ВСЕ ПАЧКИ ОБРАБОТАНЫ!')
            self.stdout.write(f'   📊 ИТОГОВАЯ СТАТИСТИКА:')
            self.stdout.write(f'      • Всего игр: {len(game_ids)}')
            self.stdout.write(f'      • Обработано: {processed_count}')
            self.stdout.write(f'      • Обновлено: {updated_count}')
            self.stdout.write(f'      • Пропущено: {skipped_count}')
            self.stdout.write(f'      • Ошибок: {error_count}')
            self.stdout.write(f'   📈 Процент обновленных: {(updated_count / len(game_ids) * 100):.1f}%')

        # Финальное обновление прогресс-бара
        if progress_bar and not debug:
            progress_bar.update(
                total_loaded=len(game_ids),  # все игры обработаны
                current_iteration=total_batches // MAX_PARALLEL_BATCHES + 1,
                iterations_without_new=0 if updated_count > 0 else 1,
                created_count=0,
                updated_count=updated_count,  # итоговое количество обновлений
                skipped_count=skipped_count,  # итоговое количество пропущенных
                processed_count=processed_count,  # итоговое количество обработанных
                errors=error_count  # итоговое количество ошибок
            )

        return updated_count, all_update_details

    def _load_single_update_batch(self, batch_num, batch_ids, games_map, debug=False):
        """Загружает и обновляет одну пачку из 10 игр - с rate limiting"""
        from games.igdb_api import make_igdb_request
        from games.models import Game
        import time

        if not batch_ids:
            return 0, []

        try:
            # Оптимальные настройки для IGDB API
            MAX_RETRIES = 2
            RETRY_DELAYS = [1.0, 3.0]  # Экспоненциальная backoff

            # 1. Запрос с правильными rate limits
            id_list = ','.join(map(str, batch_ids))
            query = f'fields id,cover.image_id; where id = ({id_list});'

            games_data = None
            last_error = None

            for attempt in range(MAX_RETRIES + 1):
                try:
                    games_data = make_igdb_request('games', query, debug=False)
                    break
                except Exception as e:
                    last_error = e
                    if attempt < MAX_RETRIES:
                        delay = RETRY_DELAYS[attempt] if attempt < len(RETRY_DELAYS) else 5.0
                        if debug:
                            error_msg = str(e).lower()
                            if "429" in str(e) or "too many" in error_msg or "rate limit" in error_msg:
                                self.stdout.write(f'      ⏸️  Rate limit, пауза {delay:.1f} сек...')
                            else:
                                self.stdout.write(f'      ⏸️  Ошибка API, пауза {delay:.1f} сек...')
                        time.sleep(delay)

            if not games_data:
                if debug and last_error:
                    error_msg = str(last_error)[:100]
                    self.stdout.write(f'      ⚠️  Пачка {batch_num}: ошибка после ретраев: {error_msg}')
                return 0, []

            # 2. Быстрое создание мапы cover_id
            cover_map = {}
            for gd in games_data:
                if 'id' in gd and 'cover' in gd:
                    cover_data = gd['cover']
                    image_id = cover_data.get('image_id')
                    if image_id:
                        cover_map[gd['id']] = f"https://images.igdb.com/igdb/image/upload/t_cover_big/{image_id}.jpg"

            batch_updated = 0
            updates_dict = {}

            for game_id in batch_ids:
                if game_id not in games_map:
                    continue

                game = games_map[game_id]
                new_cover_url = cover_map.get(game_id)

                if not new_cover_url:
                    continue

                current_cover_url = game.cover_url or ""

                if current_cover_url != new_cover_url:
                    updates_dict[game.id] = new_cover_url
                    batch_updated += 1

            # 4. Массовое обновление
            if updates_dict:
                try:
                    from django.db.models import Case, When, Value
                    from django.db.models import CharField

                    when_conditions = []
                    for game_db_id, new_url in updates_dict.items():
                        when_conditions.append(When(id=game_db_id, then=Value(new_url)))

                    Game.objects.filter(id__in=updates_dict.keys()).update(
                        cover_url=Case(*when_conditions, default=Value(''), output_field=CharField())
                    )

                    if debug and batch_updated > 0:
                        self.stdout.write(f'      💾 Пачка {batch_num}: {batch_updated} обновлений')

                except Exception as e:
                    if debug:
                        self.stderr.write(f'      ❌ Ошибка сохранения пачки {batch_num}: {e}')
                    # Fallback
                    for game_db_id, new_url in updates_dict.items():
                        try:
                            Game.objects.filter(id=game_db_id).update(cover_url=new_url)
                        except Exception:
                            continue

            return batch_updated, []

        except Exception as e:
            if debug:
                self.stderr.write(f'      ❌ Критическая ошибка пачки {batch_num}: {e}')
            return 0, []

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

            # 6. Подготавливаем и создаем ВСЕ связи M2M
            game_basic_map = {game.igdb_id: game}
            additional_data_map = {game.igdb_id: game_data}

            handler = RelationsHandler(self.stdout, self.stderr)

            # Подготавливаем связи
            all_game_relations, relations_prep_time = handler.prepare_game_relations(
                game_basic_map, collected_data['game_data_map'],
                additional_data_map, data_maps, debug
            )

            # Создаем все связи, если есть что создавать
            if all_game_relations:
                # Создаем основные связи (жанры, платформы, ключевые слова)
                genre_count, platform_count, keyword_count = handler.create_relations_batch(
                    all_game_relations, debug
                )

                # Создаем дополнительные связи (серии, компании, темы и т.д.)
                additional_results = handler.create_all_additional_relations(
                    all_game_relations, debug
                )

                # Записываем статистику обновлений
                if genre_count > 0:
                    details['updated_relations'].append(f'жанры ({genre_count})')
                if platform_count > 0:
                    details['updated_relations'].append(f'платформы ({platform_count})')
                if keyword_count > 0:
                    details['updated_relations'].append(f'ключевые слова ({keyword_count})')

                # Добавляем информацию о дополнительных связях
                for rel_type, count in additional_results.items():
                    if count > 0:
                        rel_name = rel_type.replace('_relations', '').replace('_', ' ')
                        details['updated_relations'].append(f'{rel_name} ({count})')

            return True, details

        except Exception as e:
            if debug:
                self.stderr.write(f'   ❌ Ошибка обновления игры {game.igdb_id}: {e}')
            return False, details

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

        # Создаем прогресс-бар для обработки
        total_games = result.get('new_games_count', 0)
        progress_bar = ProcessingProgressBar(self.stdout, total_games)

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
                progress_bar.clear()
                if params['debug']:
                    self.stdout.write('\n   ⏹️  Получен сигнал прерывания в обработке данных')

            original_sigint = signal.getsignal(signal.SIGINT)
            signal.signal(signal.SIGINT, signal_handler)

            try:
                # Шаг 1: Собираем все ID данных
                progress_bar.start_step('collecting_ids', f'{total_games} игр')
                collected_data = collector.collect_all_data_ids(result['new_games'], params['debug'])
                game_ids_count = len(collected_data.get('all_game_ids', []))
                progress_bar.complete_step('collecting_ids', f'{game_ids_count} ID собрано')

                # Проверка прерывания
                if interrupted.is_set():
                    progress_bar.clear()
                    raise KeyboardInterrupt()

                # Шаг 2: Создаем основные объекты игр
                progress_bar.start_step('creating_games', 'создание...')
                created_count, game_basic_map, skipped_games = loader.create_basic_games(
                    result['new_games'], params['debug']
                )
                progress_bar.complete_step('creating_games', f'создано: {created_count}, пропущено: {skipped_games}')

                # Если не создано игр, возвращаем нулевую статистику
                if created_count == 0:
                    signal.signal(signal.SIGINT, original_sigint)
                    progress_bar.show_summary(time.time() - iteration_start_time, 0)
                    return {
                        'created_count': 0,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                # Проверка прерывания
                if interrupted.is_set():
                    progress_bar.clear()
                    signal.signal(signal.SIGINT, original_sigint)
                    progress_bar.show_summary(time.time() - iteration_start_time, created_count)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                    }, errors

                # Шаг 3: Загружаем данные обложек
                progress_bar.start_step('loading_all_types', 'обложки, жанры, платформы...')

                # Загружаем все типы данных
                data_maps, step_times = loader.load_all_data_types_sequentially(
                    collected_data, params['debug']
                )

                covers_count = len(data_maps.get('cover_map', {}))
                genres_count = len(data_maps.get('genre_map', {}))
                platforms_count = len(data_maps.get('platform_map', {}))

                progress_bar.complete_step('loading_all_types',
                                           f'обложек: {covers_count}, жанров: {genres_count}, платформ: {platforms_count}'
                                           )

                # Получаем информацию о скриншотах из collected_data
                screenshots_info = collected_data.get('screenshots_info', {})

                # Шаг 4: Загружаем скриншоты ПЕРЕД дополнительными данными
                screenshots_loaded = 0
                if 'all_screenshot_games' in collected_data and screenshots_info:
                    progress_bar.start_step('loading_screenshots', 'загрузка...')

                    screenshots_loaded = loader.load_screenshots_parallel(
                        collected_data['all_screenshot_games'],
                        collected_data['game_data_map'],
                        screenshots_info,
                        params['debug']
                    )

                    progress_bar.complete_step('loading_screenshots', f'загружено: {screenshots_loaded}')

                # Шаг 5: Загружаем дополнительные данные
                progress_bar.start_step('loading_additional', 'компании, серии, темы...')

                additional_data_map, additional_ids = loader.load_and_process_additional_data(
                    list(game_basic_map.keys()),
                    collected_data['game_data_map'],
                    screenshots_info,
                    params['debug']
                )

                # Обновляем collected_data с дополнительными ID
                collected_data.update(additional_ids)

                companies_count = len(additional_ids.get('all_company_ids', []))
                series_count = len(additional_ids.get('all_series_ids', []))

                progress_bar.complete_step('loading_additional',
                                           f'компаний: {companies_count}, серий: {series_count}'
                                           )

                # Проверка прерывания
                if interrupted.is_set():
                    progress_bar.clear()
                    signal.signal(signal.SIGINT, original_sigint)
                    progress_bar.show_summary(time.time() - iteration_start_time, created_count)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                        'screenshots_loaded': screenshots_loaded,
                    }, errors

                # Шаг 6: Обновляем игры с обложками
                progress_bar.start_step('updating_covers', 'проверка и обновление...')

                cover_updates = loader.update_games_with_covers(
                    game_basic_map, data_maps.get('cover_map', {}),
                    collected_data['game_data_map'], params['debug']
                )

                progress_bar.complete_step('updating_covers', f'обновлено: {cover_updates}')

                # Проверка прерывания
                if interrupted.is_set():
                    progress_bar.clear()
                    signal.signal(signal.SIGINT, original_sigint)
                    progress_bar.show_summary(time.time() - iteration_start_time, created_count)
                    return {
                        'created_count': created_count,
                        'skipped_count': skipped_games,
                        'total_time': time.time() - iteration_start_time,
                        'screenshots_loaded': screenshots_loaded,
                    }, errors

                # Шаг 7: Подготавливаем и создаем связи
                progress_bar.start_step('preparing_relations', 'подготовка M2M связей...')

                all_game_relations, relations_prep_time = handler.prepare_game_relations(
                    game_basic_map, collected_data['game_data_map'],
                    additional_data_map, data_maps, params['debug']
                )

                relations_count = len(all_game_relations)
                progress_bar.complete_step('preparing_relations', f'подготовлено: {relations_count} связей')

                # Шаг 8: Создаем все связи
                progress_bar.start_step('creating_relations', 'создание связей...')

                relations_results, relations_possible, relations_time = handler.create_all_relations(
                    all_game_relations, params['debug']
                )

                genres_relations = relations_results.get('genre_relations', 0)
                platforms_relations = relations_results.get('platform_relations', 0)
                keywords_relations = relations_results.get('keyword_relations', 0)

                progress_bar.complete_step('creating_relations',
                                           f'жанров: {genres_relations}, платформ: {platforms_relations}, ключ. слов: {keywords_relations}'
                                           )

                # Показываем итоговую статистику
                total_time = time.time() - iteration_start_time
                progress_bar.show_summary(total_time, created_count)

                # Собираем статистику только в debug режиме
                if params['debug']:
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
                        total_time,
                        params['debug']
                    )

                    # Собираем финальную статистику
                    final_stats = stats._collect_final_statistics(
                        result['new_games_count'], created_count, skipped_games, screenshots_loaded,
                        total_time, loaded_data_stats, step_times,
                        relations_results, relations_possible, params['debug']
                    )

                    # Выводим статистику
                    stats._print_complete_statistics(final_stats)

                result_stats = {
                    'created_count': created_count,
                    'skipped_count': skipped_games,
                    'total_time': total_time,
                    'screenshots_loaded': screenshots_loaded,
                    'relations_created': sum(relations_results.values()) if relations_results else 0,
                }

            finally:
                # Восстанавливаем оригинальный обработчик сигнала
                signal.signal(signal.SIGINT, original_sigint)

        except KeyboardInterrupt:
            # Обработка прерывания в обработке данных
            progress_bar.clear()
            self.stdout.write('\n   ⏹️  Прерывание в обработке данных')
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
            }
            errors += 1
        except Exception as e:
            errors += 1
            progress_bar.clear()
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

        if result_stats is None:
            result_stats = {
                'created_count': 0,
                'skipped_count': 0,
                'total_time': time.time() - iteration_start_time,
                'errors': errors
            }
        else:
            # Убедимся, что в result_stats есть ключ errors
            result_stats['errors'] = errors

        return result_stats

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
                return False, details

            details['game_name'] = game.name
            details['timestamp'] = datetime.now().isoformat()

            if debug:
                self.stdout.write(f'\n   🔍 ПРОВЕРКА НЕДОСТАЮЩИХ ДАННЫХ ДЛЯ: {game.name} (ID: {game_id})')

            # Проверяем, каких данных не хватает
            missing_data, missing_count, cover_status = self.check_missing_game_data(game)

            if debug:
                self.stdout.write(f'   📊 СТАТУС ДАННЫХ:')
                for key, value in missing_data.items():
                    status = "✅ ЕСТЬ" if value else "❌ ОТСУТСТВУЕТ"
                    self.stdout.write(f'      • {key}: {status}')
                self.stdout.write(f'      • Обложка: {cover_status}')

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
                return False, details

            game_data = games_data[0]

            if debug:
                self.stdout.write(f'   📥 ДАННЫЕ ИЗ IGDB:')
                self.stdout.write(f'      • ID обложки в IGDB: {game_data.get("cover")}')

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

            # ОБНОВЛЯЕМ ОБЛОЖКУ с проверкой доступности
            cover_updated = self.update_game_cover(game, game_data, data_maps, details, debug)

            # 2. Обновляем описание если отсутствует
            if not missing_data['has_description'] and game_data.get('summary'):
                if not game.summary or not game.summary.strip():
                    game.summary = game_data.get('summary', '')
                    details['updated_fields'].append('summary')
                    details['summary'] = game.summary
                    if debug:
                        self.stdout.write(f'   📝 Обновляем описание ({len(game.summary)} симв.)')

            # 3. Обновляем рейтинг если отсутствует
            if not missing_data['has_rating'] and 'rating' in game_data:
                if game.rating != game_data.get('rating'):
                    game.rating = game_data.get('rating')
                    details['updated_fields'].append('rating')
                    details['rating'] = game.rating
                    if debug:
                        self.stdout.write(f'   ⭐ Обновляем рейтинг: {game.rating}')

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
                        self.stdout.write(f'   📅 Обновляем дату релиза')

            # 5. Сохраняем обновленные поля игры
            if details['updated_fields']:
                try:
                    game.save(update_fields=details['updated_fields'])
                    if debug:
                        self.stdout.write(f'   💾 Сохранены поля: {", ".join(details["updated_fields"])}')
                except Exception as e:
                    if debug:
                        self.stderr.write(f'   ❌ Ошибка сохранения игры {game_id}: {e}')
                    return False, details

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

            # 7. Подготавливаем и создаем ВСЕ связи M2M
            game_basic_map = {game_id: game}
            additional_data_map = {game_id: game_data}

            all_game_relations, relations_prep_time = handler.prepare_game_relations(
                game_basic_map, collected_data['game_data_map'],
                additional_data_map, data_maps, debug
            )

            # 8. Создаем все связи, если они есть и нужны
            if all_game_relations:
                # Проверяем, какие связи действительно отсутствуют
                needs_genres = not missing_data['has_genres'] and game_data.get('genres')
                needs_platforms = not missing_data['has_platforms'] and game_data.get('platforms')
                needs_keywords = not missing_data['has_keywords'] and game_data.get('keywords')
                needs_series = not missing_data['has_series'] and game_data.get('collections')
                needs_developers = not missing_data['has_developers'] and game_data.get('involved_companies')
                needs_publishers = not missing_data['has_publishers'] and game_data.get('involved_companies')
                needs_themes = not missing_data['has_themes'] and game_data.get('themes')
                needs_perspectives = not missing_data['has_perspectives'] and game_data.get('player_perspectives')
                needs_modes = not missing_data['has_modes'] and game_data.get('game_modes')

                # Создаем только те связи, которые отсутствуют
                if needs_genres or needs_platforms or needs_keywords:
                    genre_count, platform_count, keyword_count = handler.create_relations_batch(
                        all_game_relations, debug
                    )

                    if genre_count > 0:
                        details['updated_relations'].append(f'жанры ({genre_count})')
                    if platform_count > 0:
                        details['updated_relations'].append(f'платформы ({platform_count})')
                    if keyword_count > 0:
                        details['updated_relations'].append(f'ключевые слова ({keyword_count})')

                # Создаем дополнительные связи
                if any([needs_series, needs_developers, needs_publishers, needs_themes, needs_perspectives,
                        needs_modes]):
                    additional_results = handler.create_all_additional_relations(
                        all_game_relations, debug
                    )

                    # Добавляем информацию о созданных связях
                    for rel_type, count in additional_results.items():
                        if count > 0:
                            rel_name = rel_type.replace('_relations', '').replace('_', ' ')

                            # Проверяем, действительно ли эта связь была нужна
                            should_add = False
                            if rel_type == 'series_relations' and needs_series:
                                should_add = True
                            elif rel_type == 'developer_relations' and needs_developers:
                                should_add = True
                            elif rel_type == 'publisher_relations' and needs_publishers:
                                should_add = True
                            elif rel_type == 'theme_relations' and needs_themes:
                                should_add = True
                            elif rel_type == 'perspective_relations' and needs_perspectives:
                                should_add = True
                            elif rel_type == 'mode_relations' and needs_modes:
                                should_add = True

                            if should_add:
                                details['updated_relations'].append(f'{rel_name} ({count})')

            # 9. Проверяем что осталось отсутствующим
            new_missing_data, new_missing_count, new_cover_status = self.check_missing_game_data(game)
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

            return True, details

        except Exception as e:
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
        """Проверяет, каких данных не хватает у игры с проверкой доступности обложки"""
        import requests

        # Для обложки делаем специальную проверку доступности
        has_cover_accessible = False
        current_cover_status = "нет"
        if game_obj.cover_url and game_obj.cover_url.strip():
            try:
                response = requests.head(game_obj.cover_url, timeout=5, allow_redirects=True)
                has_cover_accessible = response.status_code == 200
                current_cover_status = f"статус: {response.status_code}"
            except Exception as e:
                has_cover_accessible = False
                current_cover_status = f"ошибка: {e}"

        missing_data = {
            'has_cover': has_cover_accessible,
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

        return missing_data, missing_count, current_cover_status

    def _get_saved_offset(self, options):
        """Получает сохраненный offset для текущих параметров"""
        params = self._get_offset_params(options)
        return OffsetManager.load_offset(params)

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
        params = {
            'game_modes': options.get('game_modes', ''),
            'game_names': options.get('game_names', ''),
            'genres': options.get('genres', ''),
            'description_contains': options.get('description_contains', ''),
            'keywords': options.get('keywords', ''),
            'game_types': options.get('game_types', ''),
            'min_rating_count': options.get('min_rating_count', 0),
            'mode': self._get_loading_mode(options),
        }

        # НОВОЕ: добавляем параметр update_covers для отдельного offset
        if options.get('update_covers', False):
            params['update_covers'] = True

        return params

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
            query_context={'is_specific_search': True, 'mode_id': game_mode_id},
            show_progress=False  # НЕ показываем прогресс загрузки
        )

    def _setup_update_mode_environment(self, options, debug, original_offset):
        """Настройка окружения для режима обновления данных"""
        # Проверяем, нужно ли обновлять все игры в базе
        update_all_games = not any([
            options.get('game_names'),
            options.get('game_modes'),
            options.get('genres'),
            options.get('description_contains'),
            options.get('keywords')
        ])

        # Если режим обновления всех игр (без конкретных фильтров)
        if update_all_games:
            # НЕМЕДЛЕННО выводим только заголовок
            self.stdout.write(f'\n🎯 ЗАПУСК ОБНОВЛЕНИЯ ДАННЫХ ДЛЯ ВСЕХ ИГР В БАЗЕ')
            self.stdout.write('=' * 60)
            self.stdout.write(f'📍 Начинаем с offset: {original_offset}')

            try:
                # Обновление всех игр без фильтров
                updated_count, update_details = self.update_all_games_missing_data(options, debug)

                # Выводим финальное сообщение
                self.stdout.write(f'\n' + '=' * 60)
                self.stdout.write(f'✅ ОБНОВЛЕНИЕ ВСЕХ ИГР ЗАВЕРШЕНО!')
                self.stdout.write(f'🎯 Обновлено игр: {updated_count}')

            except KeyboardInterrupt:
                # Исключение уже обработано в update_all_games_missing_data
                # Просто возвращаем флаг остановки
                return None, None, None, None, None, None, True

            except Exception as e:
                self.stderr.write(f'\n❌ Ошибка при обновлении: {str(e)}')
                if debug:
                    import traceback
                    self.stderr.write(f'📋 Трассировка ошибки:')
                    self.stderr.write(traceback.format_exc())
                return None, None, None, None, None, None, True

            # Возвращаем флаг остановки
            return None, None, None, None, None, None, True

        # Если режим обновления с фильтрами
        # НЕМЕДЛЕННО выводим только заголовок
        self.stdout.write(f'\n🎯 ЗАПУСК ОБНОВЛЕНИЯ ДАННЫХ ДЛЯ ИГР ПО ФИЛЬТРАМ')
        self.stdout.write('=' * 60)
        self.stdout.write(f'📍 Начинаем с offset: {original_offset}')

        try:
            # Запускаем обновление с фильтрами
            updated_count, update_details = self._handle_update_mode_with_filters(options, debug)

            # Выводим финальное сообщение
            self.stdout.write(f'\n' + '=' * 60)
            self.stdout.write(f'✅ ОБНОВЛЕНИЕ ПО ФИЛЬТРАМ ЗАВЕРШЕНО!')
            self.stdout.write(f'🎯 Обновлено игр: {updated_count}')

        except KeyboardInterrupt:
            # Исключение уже обработано в _handle_update_mode_with_filters
            # Просто возвращаем флаг остановки
            return None, None, None, None, None, None, True

        except Exception as e:
            self.stderr.write(f'\n❌ Ошибка при обновлении: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            return None, None, None, None, None, None, True

        # Возвращаем флаг остановки
        return None, None, None, None, None, None, True

    def _setup_standard_environment(self, options, debug, repeat_count, original_offset,
                                    limit, iteration_limit, clear_cache, reset_offset):
        """Настройка окружения для стандартного режима загрузки"""
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
        # Для бесконечного режима не показываем общий прогресс
        elif execution_mode['infinite_mode']:
            progress_bar.total_games = 0
            progress_bar.update()
        # Для конечного режима без общего лимита показываем 0
        else:
            progress_bar.total_games = 0
            progress_bar.update()

        # Инициализация статистики
        current_offset = original_offset

        # Загружаем сохраненный offset если не указан явно
        if original_offset == 0 and not reset_offset:
            saved_offset = self._get_saved_offset(options)
            if saved_offset is not None:
                current_offset = saved_offset
                self.stdout.write(f'📍 Начинаем с сохраненного offset: {current_offset}')

        total_stats = self._initialize_total_stats(original_offset)

        # Выводим информацию о запуске с текущим offset
        self._display_startup_info(execution_mode, iteration_limit, limit, current_offset)

        # ВЫВОДИМ ДОПОЛНИТЕЛЬНУЮ ИНФОРМАЦИЮ О ПАРАМЕТРАХ
        self.stdout.write('📋 ПАРАМЕТРЫ ЗАГРУЗКИ:')

        # Фильтры
        if options.get('game_modes'):
            self.stdout.write(f'   🎮 Режимы игры: {options["game_modes"]}')
        if options.get('game_names'):
            self.stdout.write(f'   🔍 Имена игр: {options["game_names"]}')
        if options.get('genres'):
            self.stdout.write(f'   🎭 Жанры: {options["genres"]}')
        if options.get('description_contains'):
            self.stdout.write(f'   📝 Текст в описании: "{options["description_contains"]}"')
        if options.get('keywords'):
            self.stdout.write(f'   🔑 Ключевые слова: {options["keywords"]}')

        # Дополнительные параметры
        if options.get('min_rating_count', 0) > 0:
            self.stdout.write(f'   ⭐ Минимум оценок: {options["min_rating_count"]}')

        game_types = options.get('game_types', '0,1,2,4,5,8,9,10,11')
        if game_types != '0,1,2,4,5,8,9,10,11':
            self.stdout.write(f'   🎮 Типы игр: {game_types}')

        if options.get('overwrite'):
            self.stdout.write(f'   🔄 Режим перезаписи: ВКЛЮЧЕН')

        if options.get('count_only'):
            self.stdout.write(f'   🔢 Только подсчет: ДА')

        self.stdout.write('=' * 60)

        # Если это TTY терминал, оставляем место для прогресс-бара
        count_only = options.get('count_only', False)
        if (hasattr(progress_bar, 'is_tty') and progress_bar.is_tty and not count_only and not debug):
            self.stdout.write('\n' * 2)

        return execution_mode, progress_bar, current_offset, total_stats, options, limit, False

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

        # ФИНАЛЬНЫЙ ЭТАП (только если не было прерывания)
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
        execution_mode, progress_bar, current_offset, total_stats, options, limit = setup_result[:6]

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

        # Передаем options в params для доступа к update_missing_data и update_covers
        params['update_missing_data'] = options.get('update_missing_data', False)
        params['update_covers'] = options.get('update_covers', False)

        # Определяем режимы
        skip_existing = self._determine_skip_mode(params)

        errors = 0
        iteration_start_time = time.time()

        # Загрузка игр
        result = self._load_games_for_iteration(params, iteration_limit_actual, current_offset, skip_existing, debug)

        # Обработка результатов загрузки
        if result is None:
            return self._handle_failed_loading(iteration_start_time, errors, current_offset)

        # Проверка наличия игр
        if not result.get('all_found_games') and not result.get('new_games'):
            return self._handle_empty_results(result, errors, params, current_offset, iteration_start_time)

        # Если нет новых игр для обработки
        new_games_count = result.get('new_games_count', 0)
        if new_games_count == 0:
            iteration_time = time.time() - iteration_start_time
            return {
                'total_games_checked': result['total_games_checked'],
                'total_games_found': 0,
                'created_count': 0,
                'skipped_count': result.get('existing_games_skipped', 0),
                'total_time': iteration_time,
                'errors': errors,
                'last_checked_offset': result.get('last_checked_offset', current_offset),
                'limit_reached': result.get('limit_reached', False),
                'limit_reached_at_offset': result.get('limit_reached_at_offset'),
            }

        # ИСПРАВЛЕНИЕ: метод возвращает один объект, а не два значения
        result_stats = self._process_standard_game_data(
            result, params, iteration_start_time, errors
        )

        # ИСПРАВЛЕНИЕ: получаем ошибки из результата
        errors = result_stats.get('errors', 0)
        iteration_time = time.time() - iteration_start_time

        return {
            'total_games_checked': result['total_games_checked'],
            'total_games_found': new_games_count,
            'created_count': result_stats.get('created_count', 0) if result_stats else 0,
            'skipped_count': result['existing_games_skipped'],
            'total_time': result_stats.get('total_time', iteration_time) if result_stats else iteration_time,
            'errors': errors,
            'last_checked_offset': result.get('last_checked_offset', current_offset),
            'limit_reached': result.get('limit_reached', False),
            'limit_reached_at_offset': result.get('limit_reached_at_offset'),
        }

    def _display_loading_type(self, params):
        """Отображает тип загрузки"""
        game_modes_str = params.get('game_modes_str', '')
        game_names_str = params.get('game_names_str', '')
        genres_str = params['genres_str']
        description_contains = params['description_contains']
        keywords_str = params['keywords_str']
        game_types_str = params['game_types_str']

        if params.get('update_covers'):  # НОВОЕ
            self.stdout.write('🖼️  РЕЖИМ: ОБНОВЛЕНИЕ ОБЛОЖЕК')
            self.stdout.write('⚠️  Будут проверены и обновлены обложки у существующих игр')
            return

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
            # НЕ показываем эти сообщения если есть статус-строка
            # Только в debug режиме или если нет статус-строки

            # НОВАЯ ВЕТКА: поиск по режимам игры (ВЫСШИЙ ПРИОРИТЕТ)
            if params.get('game_modes_str'):
                result = self.load_games_by_game_mode(
                    params['game_modes_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            # Поиск по именам игр
            elif params.get('game_names_str'):
                result = self.load_games_by_names(
                    params['game_names_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str'] and params['description_contains']:
                result = self.load_games_by_genres_and_description(
                    params['genres_str'], params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['genres_str']:
                result = self.load_games_by_genres(
                    params['genres_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['description_contains']:
                result = self.load_games_by_description(
                    params['description_contains'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            elif params['keywords_str']:
                result = self.load_games_by_keywords(
                    params['keywords_str'], debug, actual_limit, actual_offset,
                    params['min_rating_count'], skip_existing, params['count_only'], params['game_types_str']
                )
            else:
                result = self.load_all_popular_games(
                    debug, actual_limit, actual_offset, params['min_rating_count'],
                    skip_existing, params['count_only'], params['game_types_str']
                )

            # Показываем результат загрузки только в debug режиме
            if debug and result and isinstance(result, dict):
                total_checked = result.get('total_games_checked', 0)
                new_games = result.get('new_games_count', 0)
                skipped = result.get('existing_games_skipped', 0)

                self.stdout.write(f'   📊 Результат загрузки:')
                self.stdout.write(f'      👀 Просмотрено игр: {total_checked}')
                self.stdout.write(f'      🆕 Найдено новых: {new_games}')
                if skipped > 0:
                    self.stdout.write(f'      ⏭️  Пропущено (уже в базе): {skipped}')

            return result

        except Exception as e:
            self.stderr.write(f'❌ ОШИБКА при загрузке игр: {str(e)}')
            if debug:
                import traceback
                self.stderr.write(f'📋 Трассировка ошибки:')
                self.stderr.write(traceback.format_exc())
            # Возвращаем пустой словарь, а не список
            return self._empty_result()

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
            return self._empty_result()  # Возвращаем пустой результат как словарь

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
            return self._empty_result()  # Возвращаем пустой результат как словарь

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

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False  # НЕ показываем прогресс загрузки
        )

    def load_games_by_genres_and_description(self, genres_str, description_text, debug=False, limit=0, offset=0,
                                             min_rating_count=0, skip_existing=True, count_only=False,
                                             game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по жанрам И тексту в описании"""
        collector = DataCollector(self.stdout, self.stderr)

        genre_list = [g.strip() for g in genres_str.split(',') if g.strip()]

        if not genre_list:
            self.stdout.write('⚠️  Не указаны жанры')
            return self._empty_result()  # Возвращаем пустой результат как словарь

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
            return self._empty_result()  # Возвращаем пустой результат как словарь

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

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False  # НЕ показываем прогресс загрузки
        )

    def _empty_result(self):
        """Возвращает пустой результат как словарь"""
        return {
            'new_games': [],
            'all_found_games': [],
            'total_games_checked': 0,
            'new_games_count': 0,
            'existing_games_skipped': 0,
            'last_checked_offset': 0,
            'limit_reached': False,
            'limit_reached_at_offset': None,
            'interrupted': False,
        }

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

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False  # НЕ показываем прогресс загрузки
        )

    def load_games_by_keywords(self, keywords_str, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка игр по ключевым словам с логикой И"""
        collector = DataCollector(self.stdout, self.stderr)

        keyword_list = [k.strip() for k in keywords_str.split(',') if k.strip()]

        if not keyword_list:
            self.stdout.write('⚠️  Не указаны ключевые слова')
            return self._empty_result()  # Возвращаем пустой результат как словарь

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
            return self._empty_result()  # Возвращаем пустой результат как словарь

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

        return collector.load_games_by_query(
            where_clause, debug, limit, offset,
            skip_existing, count_only,
            show_progress=False  # НЕ показываем прогресс загрузки
        )

    def load_all_popular_games(self, debug=False, limit=0, offset=0, min_rating_count=0,
                               skip_existing=True, count_only=False, game_types_str='0,1,2,4,5,8,9,10,11'):
        """Загрузка всех игр с сортировкой по популярности (rating_count)"""
        collector = DataCollector(self.stdout, self.stderr)
        return collector.load_all_popular_games(
            debug, limit, offset, min_rating_count,
            skip_existing, count_only, game_types_str
        )

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

    def _display_startup_info(self, execution_mode, iteration_limit, limit, current_offset=0):
        """Отображает информацию о запуске команды"""
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('🚀 ЗАПУСК КОМАНДЫ ЗАГРУЗКИ ИГР')
        self.stdout.write('=' * 60)

        # Режим выполнения
        if execution_mode['infinite_mode']:
            self.stdout.write(f'🔄 РЕЖИМ: БЕСКОНЕЧНЫЙ (пока не закончатся игры)')
        elif execution_mode['single_run_mode']:
            self.stdout.write(f'🔄 РЕЖИМ: ОДНА ИТЕРАЦИЯ')
        else:
            self.stdout.write(f'🔄 РЕЖИМ: {execution_mode["repeat_count"]} ПОВТОРЕНИЙ')

        # Параметры загрузки
        self.stdout.write(f'📊 Игр за итерацию: {iteration_limit}')

        if limit > 0:
            self.stdout.write(f'🎯 Общий лимит игр: {limit}')
        else:
            self.stdout.write(f'🎯 Общий лимит: БЕЗ ЛИМИТА')

        if current_offset > 0:
            self.stdout.write(f'📍 Начальный offset: {current_offset}')
        else:
            self.stdout.write(f'📍 Начальный offset: 0 (с начала)')

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
            'update_missing_data': options.get('update_missing_data', False),
            'update_covers': options.get('update_covers', False),  # НОВОЕ
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

        # СОХРАНЯЕМ OFFSET ПЕРЕД ВЫВОДОМ СТАТИСТИКИ
        options = self.current_options
        reset_offset = options.get('reset_offset', False)

        if not reset_offset:
            self._save_offset_for_continuation(options, current_offset)
            self.stdout.write(f'💾 Offset сохранен для продолжения: {current_offset}')

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
        self.stdout.write(f'📍 Текущий offset (сохранен): {current_offset}')
        self.stdout.write(f'👀 Всего просмотрено игр: {total_stats["total_games_checked"]}')
        self.stdout.write(f'🎮 Всего найдено новых: {total_stats["total_games_found"]}')
        self.stdout.write(f'✅ Всего загружено игр: {total_stats["total_games_created"]}')
        self.stdout.write(f'⏭️  Всего пропущено игр: {total_stats["total_games_skipped"]}')
        self.stdout.write(f'❌ Ошибок: {total_stats["errors"]}')

        if limit > 0:
            self.stdout.write(f'🎯 Общий лимит игр: {limit} (загружено: {total_stats["total_games_created"]})')

        self.stdout.write(f'⏱️  Общее время: {total_stats["total_time"]:.2f}с')

        # ПОДСКАЗКА ДЛЯ ПОЛЬЗОВАТЕЛЯ
        self.stdout.write('\n💡 Для продолжения с этого места запустите команду снова')
        self.stdout.write('   Offset будет автоматически загружен из сохранения')

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
        game_names_str = options.get('game_names', '')
        game_modes_str = options.get('game_modes', '')
        genres_str = options.get('genres', '')
        description_contains = options.get('description_contains', '')
        keywords_str = options.get('keywords', '')
        update_covers = options.get('update_covers', False)
        update_missing_data = options.get('update_missing_data', False)

        # ПРИОРИТЕТ 1: режимы обновления данных
        if update_covers:
            return 'update_covers'
        elif update_missing_data:
            return 'update_missing_data'
        # ПРИОРИТЕТ 2: режимы загрузки по фильтрам
        elif game_modes_str:
            return 'game_modes'
        elif game_names_str:
            return 'game_names'
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
