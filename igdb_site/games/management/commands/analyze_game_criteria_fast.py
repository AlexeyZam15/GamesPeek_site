# games/management/commands/analyze_game_criteria_fast.py
"""
МАКСИМАЛЬНО УСКОРЕННАЯ команда анализа ТОЛЬКО критериев
Использует все возможные оптимизации: многопоточность, bulk_create, предзагрузку
"""

import sys
import os
import time
import threading
import signal
from typing import Dict, Any, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q

from games.models import Game, Genre, Theme, PlayerPerspective, GameMode
from games.analyze.range_cache import RangeCacheManager
from games.analyze.pattern_manager import PatternManager


class Command(BaseCommand):
    """
    МАКСИМАЛЬНО УСКОРЕННАЯ команда анализа критериев

    Оптимизации:
    1. Предзагрузка ВСЕХ паттернов в память (один раз)
    2. Предзагрузка ВСЕХ критериев из БД в память (id, name)
    3. Многопоточная обработка с пулом потоков
    4. Массовое обновление через bulk_create (одна транзакция)
    5. Кэширование через RangeCacheManager
    6. Минимизация обращений к БД

    Примеры:
        python manage.py analyze_game_criteria_fast --update-game --threads 16
        python manage.py analyze_game_criteria_fast --limit 100000 --threads 12 --batch-save 10000
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.progress_bar = None
        self.output_file = None
        self.original_stdout = None
        self.limit = None
        self.offset = 0
        self.game_name = None
        self.verbose = False
        self.threads = 16
        self.batch_save = 10000
        self.force_restart = False
        self.output_path = None
        self.only_found = False
        self.combine_all_texts = False
        self.no_progress = False
        self.patterns = None
        self.criteria_cache = None
        self.criteria_by_name = None
        self.compiled_patterns = None
        self._interrupted = False

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=None,
                            help='Максимальное количество игр для анализа')
        parser.add_argument('--offset', type=int, default=0,
                            help='Пропустить первые N игр')
        parser.add_argument('--game-name', type=str, default=None,
                            help='Название игры для анализа (поиск по частичному совпадению)')
        parser.add_argument('--verbose', action='store_true',
                            help='Подробный вывод')
        parser.add_argument('--threads', type=int, default=24,
                            help='Количество потоков для обработки (по умолчанию 24)')
        parser.add_argument('--force-restart', action='store_true',
                            help='Начать обработку заново, игнорируя кэш')
        parser.add_argument('--output', type=str,
                            help='Путь к файлу для экспорта результатов')
        parser.add_argument('--only-found', action='store_true',
                            help='Показывать только игры где были найдены критерии')
        parser.add_argument('--no-progress', action='store_true',
                            help='Отключить отображение прогресс-бара')
        parser.add_argument('--auto-save', action='store_true',
                            help='Автоматически сохранять в БД без подтверждения')
        parser.add_argument('--no-combine-texts', action='store_true',
                            help='НЕ объединять все источники текста (использовать только summary)')

    def _load_existing_relations(self) -> Dict[str, set]:
        """Загружает все существующие связи из БД для быстрой проверки"""
        from django.db import connection
        from games.models import Game, Genre, Theme, PlayerPerspective, GameMode

        games_genres_table = Game.genres.through._meta.db_table
        games_themes_table = Game.themes.through._meta.db_table
        games_perspectives_table = Game.player_perspectives.through._meta.db_table
        games_modes_table = Game.game_modes.through._meta.db_table

        existing_relations = {
            'genres': set(),
            'themes': set(),
            'perspectives': set(),
            'game_modes': set()
        }

        with connection.cursor() as cursor:
            cursor.execute(f"SELECT game_id, genre_id FROM {games_genres_table}")
            for row in cursor.fetchall():
                existing_relations['genres'].add((row[0], row[1]))

            cursor.execute(f"SELECT game_id, theme_id FROM {games_themes_table}")
            for row in cursor.fetchall():
                existing_relations['themes'].add((row[0], row[1]))

            cursor.execute(f"SELECT game_id, playerperspective_id FROM {games_perspectives_table}")
            for row in cursor.fetchall():
                existing_relations['perspectives'].add((row[0], row[1]))

            cursor.execute(f"SELECT game_id, gamemode_id FROM {games_modes_table}")
            for row in cursor.fetchall():
                existing_relations['game_modes'].add((row[0], row[1]))

        return existing_relations

    def _confirm_save(self, count: int, results_count: int, elements_count: int) -> bool:
        """Запрашивает подтверждение сохранения с принудительной синхронизацией"""
        import sys
        import os

        # Показываем информацию
        sys.stderr.write(f"\n💾 Готово к сохранению в БД: {count} игр\n")
        sys.stderr.write(f"   🎯 Игр с новыми критериями: {results_count}\n")
        sys.stderr.write(f"   📈 Всего элементов: {elements_count}\n")
        sys.stderr.flush()

        if self.output_path:
            sys.stderr.write(f"   📄 Результаты сохранены в: {self.output_path}\n")
            sys.stderr.flush()

        # Авто-сохранение
        if self.auto_save:
            sys.stderr.write("\n✅ Авто-сохранение включено\n")
            sys.stderr.flush()
            return True

        # Принудительно отключаем буферизацию для stdin
        sys.stderr.write("\n⚠️ Сохранить результаты в БД? (y/N): ")
        sys.stderr.flush()

        # Очищаем буфер stdin перед чтением
        try:
            # Пробуем прочитать все ожидающие данные из stdin
            import select
            if select.select([sys.stdin], [], [], 0)[0]:
                sys.stdin.read()
        except:
            pass

        try:
            # Читаем ответ с таймаутом
            answer = sys.stdin.readline().strip().lower()
        except (EOFError, KeyboardInterrupt, OSError):
            sys.stderr.write("\n")
            sys.stderr.flush()
            return False

        if answer in ['y', 'yes', 'да', 'д', '1']:
            sys.stderr.write("\n✅ Сохраняем...\n")
            sys.stderr.flush()
            return True

        # Только один вывод при отмене
        sys.stderr.write("\n⏭️ Сохранение отменено\n")
        sys.stderr.flush()
        return False

    def _preload_all_data(self):
        """Предзагрузка ВСЕХ данных в память"""
        self.patterns = PatternManager.get_all_patterns()

        self.criteria_cache = {
            'genres': {},
            'themes': {},
            'perspectives': {},
            'game_modes': {}
        }

        self.criteria_by_name = {
            'genres': {},
            'themes': {},
            'perspectives': {},
            'game_modes': {}
        }

        # Жанры
        for g in Genre.objects.all().only('id', 'name'):
            self.criteria_cache['genres'][g.id] = g.name
            self.criteria_by_name['genres'][g.name.lower()] = g.id

        # Темы
        for t in Theme.objects.all().only('id', 'name'):
            self.criteria_cache['themes'][t.id] = t.name
            self.criteria_by_name['themes'][t.name.lower()] = t.id

        # Перспективы
        for p in PlayerPerspective.objects.all().only('id', 'name'):
            self.criteria_cache['perspectives'][p.id] = p.name
            self.criteria_by_name['perspectives'][p.name.lower()] = p.id

        # Режимы игры
        for m in GameMode.objects.all().only('id', 'name'):
            self.criteria_cache['game_modes'][m.id] = m.name
            self.criteria_by_name['game_modes'][m.name.lower()] = m.id

        # Компилируем паттерны
        self.compiled_patterns = {}
        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
            self.compiled_patterns[criteria_type] = []
            for name, patterns in self.patterns[criteria_type].items():
                name_lower = name.lower()
                if name_lower in self.criteria_by_name[criteria_type]:
                    criteria_id = self.criteria_by_name[criteria_type][name_lower]
                    for pattern in patterns:
                        self.compiled_patterns[criteria_type].append({
                            'name': name,
                            'name_lower': name_lower,
                            'id': criteria_id,
                            'pattern': pattern
                        })

    def _get_games_to_analyze(self) -> List[Dict]:
        """Максимально быстрая загрузка игр через сырой SQL или по имени"""
        import sys
        from django.db import connection

        # ПРОВЕРКА: если указано имя игры, используем поиск по имени
        if self.game_name:
            sys.stderr.write(f"\n   🔍 Поиск игр по имени: '{self.game_name}'\n")
            sys.stderr.flush()

            # Экранируем имя для безопасного поиска
            search_term = f"%{self.game_name}%"

            # Строим SQL запрос с поиском по имени
            fields = ['id', 'name', 'summary', 'storyline', 'rawg_description', 'wiki_description']
            fields_str = ', '.join(fields)

            sql = f"""
                SELECT {fields_str}
                FROM games_game
                WHERE name ILIKE %s
                ORDER BY 
                    CASE WHEN name ILIKE %s THEN 1 ELSE 2 END,
                    rating DESC NULLS LAST,
                    id
                LIMIT %s
                OFFSET %s
            """

            # Определяем лимит и offset для поиска по имени
            actual_limit = self.limit if self.limit else 50  # По умолчанию не более 50 игр
            actual_offset = self.offset if self.offset else 0

            games = []
            with connection.cursor() as cursor:
                # Ищем точное совпадение в первую очередь
                exact_term = self.game_name
                cursor.execute(sql, [search_term, exact_term, actual_limit, actual_offset])

                for row in cursor.fetchall():
                    games.append({
                        'id': row[0],
                        'name': row[1],
                        'summary': row[2],
                        'storyline': row[3],
                        'rawg_description': row[4],
                        'wiki_description': row[5]
                    })

            sys.stderr.write(f"   ✅ Найдено игр по имени: {len(games)}\n")
            sys.stderr.flush()
            return games

        # ОРИГИНАЛЬНЫЙ КОД: массовая загрузка всех игр (если game_name не указан)
        sys.stderr.write(f"\n   📊 Подсчет количества игр... ")
        sys.stderr.flush()

        # Получаем общее количество через сырой SQL (быстрее чем ORM)
        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM games_game")
            total_count = cursor.fetchone()[0]

        sys.stderr.write(f"{total_count} игр\n")
        sys.stderr.flush()

        # Определяем лимит и offset
        if self.limit:
            actual_limit = self.limit
        else:
            actual_limit = total_count - self.offset if self.offset > 0 else total_count

        sys.stderr.write(f"   🎯 Будет загружено: {actual_limit} игр\n")
        sys.stderr.flush()

        # Строим SQL запрос с нужными полями
        fields = ['id', 'name', 'summary', 'storyline', 'rawg_description', 'wiki_description']
        fields_str = ', '.join(fields)

        # Используем сырой SQL для максимальной скорости
        sql = f"""
            SELECT {fields_str}
            FROM games_game
            ORDER BY id
            OFFSET %s
            LIMIT %s
        """

        games = []
        batch_size = 20000  # Увеличенный размер батча

        sys.stderr.write(f"\n   ⬇️ Загрузка батчами по {batch_size} записей (сырой SQL)...\n")
        sys.stderr.flush()

        # Загружаем батчами через сырой SQL
        for offset in range(0, actual_limit, batch_size):
            current_limit = min(batch_size, actual_limit - offset)
            current_offset = self.offset + offset

            with connection.cursor() as cursor:
                cursor.execute(sql, [current_offset, current_limit])

                # Преобразуем в словари максимально быстро
                batch = []
                for row in cursor.fetchall():
                    batch.append({
                        'id': row[0],
                        'name': row[1],
                        'summary': row[2],
                        'storyline': row[3],
                        'rawg_description': row[4],
                        'wiki_description': row[5]
                    })

                games.extend(batch)

            processed = len(games)
            percent = (processed / actual_limit) * 100 if actual_limit > 0 else 0

            sys.stderr.write(f"\r   ⬇️ Загружено: {processed}/{actual_limit} игр ({percent:.1f}%)")
            sys.stderr.flush()

        sys.stderr.write(f"\n   ✅ Загружено {len(games)} игр (сырой SQL, {batch_size} записей/батч)\n")
        sys.stderr.flush()

        return games

    def _get_game_text(self, game: Game) -> str:
        """Максимально быстрое получение текста"""
        if self.combine_all_texts:
            parts = []
            if game.summary:
                parts.append(game.summary)
            if game.storyline:
                parts.append(game.storyline)
            if game.rawg_description:
                parts.append(game.rawg_description)
            if game.wiki_description:
                parts.append(game.wiki_description)
            return ' '.join(parts)

        return game.summary or game.storyline or game.rawg_description or game.wiki_description or ''

    def _analyze_game_fast(self, game_dict: Dict, existing_relations: Dict[str, set] = None) -> Dict[str, Any]:
        """Анализ одной игры с диагностикой времени (работает со словарем)"""
        import time
        import re
        import sys

        # Диагностика
        if hasattr(self, '_analyze_times'):
            self._analyze_times.append(time.time())
            if len(self._analyze_times) > 100:
                self._analyze_times.pop(0)

        # Получаем текст из словаря
        if self.combine_all_texts:
            parts = []
            for key in ['summary', 'storyline', 'rawg_description', 'wiki_description']:
                value = game_dict.get(key)
                if value and isinstance(value, str) and value.strip():
                    parts.append(value)
            text = ' '.join(parts) if parts else ''
        else:
            text = (game_dict.get('summary') or
                    game_dict.get('storyline') or
                    game_dict.get('rawg_description') or
                    game_dict.get('wiki_description') or
                    '')

        # Проверка наличия текста
        if not text or len(text.strip()) < 10:
            return {
                'id': game_dict['id'],
                'name': game_dict['name'],
                'has_results': False,
                'skipped': True,
                'reason': 'no_text' if not text else 'short',
                'count': 0,
                'found': {'genres': [], 'themes': [], 'perspectives': [], 'game_modes': []},
                'pattern_info': {},
                'summary': {'found_count': 0},
                'success': True,
                'results': {'genres': [], 'themes': [], 'perspectives': [], 'game_modes': []},
                'has_new': False
            }

        # Нормализация текста
        text_normalized = re.sub(r'\s+', ' ', text)
        text_lower = text_normalized.lower()

        found = {
            'genres': [],
            'themes': [],
            'perspectives': [],
            'game_modes': []
        }

        pattern_info = {
            'genres': [],
            'themes': [],
            'perspectives': [],
            'game_modes': []
        }

        total_found = 0
        game_id = game_dict['id']

        # Основной поиск
        for criteria_type, patterns in self.compiled_patterns.items():
            found_ids = set()
            for p in patterns:
                match = p['pattern'].search(text_lower)
                if match:
                    found_ids.add(p['id'])

                    match_start = match.start()
                    match_end = match.end()
                    matched_text = text_normalized[match_start:match_end]

                    context_start = max(0, match_start - 40)
                    context_end = min(len(text_normalized), match_end + 40)
                    context = text_normalized[context_start:context_end]
                    if context_start > 0:
                        context = "..." + context
                    if context_end < len(text_normalized):
                        context = context + "..."

                    pattern_info[criteria_type].append({
                        'name': p['name'],
                        'id': p['id'],
                        'matched_text': matched_text,
                        'context': context,
                        'pattern': p['pattern'].pattern,
                        'status': 'found'
                    })

                    if len(pattern_info[criteria_type]) >= 50:
                        break

            if found_ids:
                for fid in found_ids:
                    # Проверяем, является ли этот критерий новым для игры
                    is_new = True
                    if existing_relations:
                        if (game_id, fid) in existing_relations.get(criteria_type, set()):
                            is_new = False

                    found[criteria_type].append({
                        'id': fid,
                        'name': self.criteria_cache[criteria_type][fid],
                        'is_new': is_new
                    })
                total_found += len(found_ids)

        # ВЫВОД НАЙДЕННЫХ КРИТЕРИЕВ С ПОМЕТКОЙ НОВЫХ
        # Показываем для любой игры, если:
        # 1. Запущено с --game-name (анализ конкретной игры)
        # 2. Или включен --verbose
        if (self.game_name or self.verbose) and total_found > 0:
            sys.stderr.write(f"\n=== ИТОГОВЫЕ НАЙДЕННЫЕ КРИТЕРИИ для {game_dict['name']} ===\n")
            for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                if found[criteria_type]:
                    type_name = {
                        'genres': 'Жанры',
                        'themes': 'Темы',
                        'perspectives': 'Перспективы',
                        'game_modes': 'Режимы игры'
                    }.get(criteria_type, criteria_type)
                    sys.stderr.write(f"\n{type_name}:\n")
                    for item in found[criteria_type]:
                        if item.get('is_new', False):
                            sys.stderr.write(f"  - {item['name']} (ID: {item['id']}) [НОВЫЙ]\n")
                        else:
                            sys.stderr.write(f"  - {item['name']} (ID: {item['id']})\n")
            sys.stderr.write(f"\nВсего найдено уникальных критериев: {total_found}\n")
            sys.stderr.write("=" * 70 + "\n")
            sys.stderr.flush()

        return {
            'id': game_dict['id'],
            'name': game_dict['name'],
            'has_results': total_found > 0,
            'skipped': False,
            'count': total_found,
            'found': found,
            'pattern_info': pattern_info,
            'summary': {'found_count': total_found},
            'success': True,
            'results': found,
            'has_new': any(item.get('is_new', False) for items in found.values() for item in items)
        }

    def _parallel_analysis(self, games: List[Dict], existing_relations: Dict[str, set] = None) -> Dict[str, Any]:
        """Максимально быстрый многопоточный анализ с предварительной проверкой существующих связей"""
        import sys
        from queue import Queue, Empty
        import signal

        stats = {
            'total': len(games),
            'processed': 0,
            'with_results': 0,
            'with_new_results': 0,
            'total_found': 0,
            'total_new_found': 0,
            'skipped': 0,
            'skipped_no_text': 0,
            'skipped_short': 0,
            'skipped_error': 0,
            'no_results': 0,
            'errors': 0,
            'to_save': [],
            'all_results': []
        }

        task_queue = Queue()
        for game_dict in games:
            task_queue.put(game_dict)

        result_queue = Queue()

        stats_lock = threading.Lock()
        stop_flag = threading.Event()
        active_workers = 0
        workers_lock = threading.Lock()

        def signal_handler(signum, frame):
            """Обработчик сигнала для корректного завершения"""
            sys.stderr.write("\n⏹️ Получен сигнал прерывания, останавливаем потоки...\n")
            sys.stderr.flush()
            stop_flag.set()

        # Устанавливаем обработчик сигнала
        original_handler = signal.signal(signal.SIGINT, signal_handler)

        def worker():
            nonlocal active_workers
            with workers_lock:
                active_workers += 1

            while not stop_flag.is_set():
                try:
                    game_dict = task_queue.get(timeout=0.5)
                except Empty:
                    break

                try:
                    # Передаем existing_relations в _analyze_game_fast
                    result = self._analyze_game_fast(game_dict, existing_relations)

                    # Гарантируем наличие всех полей
                    result['skipped'] = result.get('skipped', False)
                    result['has_results'] = result.get('has_results', False)
                    result['count'] = result.get('count', 0)
                    result['has_new'] = False

                    # Проверяем, есть ли реально новые связи
                    if result['has_results'] and existing_relations:
                        game_id = result['id']
                        new_criteria = {'genres': [], 'themes': [], 'perspectives': [], 'game_modes': []}
                        has_new = False
                        new_count = 0

                        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                            for crit in result.get('found', {}).get(criteria_type, []):
                                crit_id = crit.get('id')
                                if crit_id and (game_id, crit_id) not in existing_relations[criteria_type]:
                                    crit['is_new'] = True
                                    new_criteria[criteria_type].append(crit)
                                    has_new = True
                                    new_count += 1
                                else:
                                    crit['is_new'] = False
                                    new_criteria[criteria_type].append(crit)

                        if has_new:
                            result['found'] = new_criteria
                            result['count'] = new_count
                            result['has_new'] = True

                    result_queue.put(result)

                except Exception as e:
                    result_queue.put({
                        'id': game_dict['id'],
                        'name': game_dict['name'],
                        'has_results': False,
                        'skipped': True,
                        'reason': 'error',
                        'error': str(e),
                        'count': 0,
                        'found': {'genres': [], 'themes': [], 'perspectives': [], 'game_modes': []},
                        'has_new': False
                    })
                finally:
                    task_queue.task_done()

            with workers_lock:
                active_workers -= 1

        # Запускаем все рабочие потоки
        threads = []
        for _ in range(self.threads):
            t = threading.Thread(target=worker)
            t.daemon = True
            t.start()
            threads.append(t)

        analysis_start = time.time()
        processed_count = 0
        last_progress_time = time.time()
        # Для расчета средней скорости
        speed_window = []
        last_speed_update = time.time()
        last_processed_for_speed = 0

        def format_time(seconds):
            """Форматирует время в читаемый вид"""
            if seconds < 60:
                return f"{seconds:.0f} сек"
            elif seconds < 3600:
                minutes = seconds / 60
                return f"{minutes:.1f} мин"
            else:
                hours = seconds / 3600
                return f"{hours:.1f} ч"

        try:
            # Основной цикл обработки результатов
            while processed_count < len(games) and not stop_flag.is_set():
                try:
                    result = result_queue.get(timeout=0.5)
                    processed_count += 1

                    # Обновляем статистику
                    with stats_lock:
                        stats['processed'] += 1
                        stats['all_results'].append(result)

                        is_skipped = result.get('skipped', False)
                        has_results = result.get('has_results', False)
                        reason = result.get('reason', '')

                        # Категоризация
                        if is_skipped:
                            stats['skipped'] += 1
                            if reason == 'no_text':
                                stats['skipped_no_text'] += 1
                            elif reason == 'short':
                                stats['skipped_short'] += 1
                            elif reason == 'error':
                                stats['skipped_error'] += 1
                                stats['errors'] += 1
                            else:
                                stats['skipped_error'] += 1
                        elif has_results:
                            stats['with_results'] += 1
                            stats['total_found'] += result.get('count', 0)

                            if result.get('has_new', False):
                                stats['with_new_results'] += 1
                                stats['total_new_found'] += result.get('count', 0)
                                if self.update_game:
                                    stats['to_save'].append({
                                        'id': result['id'],
                                        'found': result.get('found', {})
                                    })
                        else:
                            stats['no_results'] += 1

                    # Расчет скорости и времени каждую секунду
                    now = time.time()
                    if now - last_progress_time >= 1.0:
                        elapsed = now - analysis_start
                        remaining_games = len(games) - processed_count

                        # Рассчитываем скорость обработки (игр в секунду)
                        if processed_count > 0 and elapsed > 0:
                            current_speed = processed_count / elapsed

                            # Используем скользящее окно для более стабильной скорости
                            speed_window.append((now, processed_count))
                            # Оставляем только последние 10 секунд
                            speed_window = [(t, c) for t, c in speed_window if now - t <= 10]

                            if len(speed_window) >= 2:
                                oldest_time, oldest_count = speed_window[0]
                                time_diff = now - oldest_time
                                count_diff = processed_count - oldest_count
                                if time_diff > 0:
                                    avg_speed = count_diff / time_diff
                                else:
                                    avg_speed = current_speed
                            else:
                                avg_speed = current_speed

                            # Рассчитываем оставшееся время
                            if avg_speed > 0:
                                remaining_seconds = remaining_games / avg_speed
                                eta_str = format_time(remaining_seconds)
                            else:
                                eta_str = "неизвестно"
                        else:
                            avg_speed = 0
                            eta_str = "расчет..."

                        percent = (processed_count / len(games)) * 100 if len(games) > 0 else 0

                        # Формируем строку прогресса
                        progress_line = f"\r📊 Обработано: {processed_count}/{len(games)} игр ({percent:.1f}%) | Скорость: {avg_speed:.1f} игр/сек | Осталось: {eta_str}    "
                        sys.stderr.write(progress_line)
                        sys.stderr.flush()
                        last_progress_time = now

                except Empty:
                    # Проверяем состояние потоков
                    with workers_lock:
                        workers_alive = active_workers > 0

                    # Если нет активных потоков и очередь задач пуста
                    if not workers_alive and task_queue.empty():
                        # Забираем все оставшиеся результаты
                        try:
                            while True:
                                result = result_queue.get_nowait()
                                processed_count += 1

                                with stats_lock:
                                    stats['processed'] += 1
                                    stats['all_results'].append(result)

                                    is_skipped = result.get('skipped', False)
                                    has_results = result.get('has_results', False)
                                    reason = result.get('reason', '')

                                    if is_skipped:
                                        stats['skipped'] += 1
                                        if reason == 'no_text':
                                            stats['skipped_no_text'] += 1
                                        elif reason == 'short':
                                            stats['skipped_short'] += 1
                                        elif reason == 'error':
                                            stats['skipped_error'] += 1
                                            stats['errors'] += 1
                                        else:
                                            stats['skipped_error'] += 1
                                    elif has_results:
                                        stats['with_results'] += 1
                                        stats['total_found'] += result.get('count', 0)

                                        if result.get('has_new', False):
                                            stats['with_new_results'] += 1
                                            stats['total_new_found'] += result.get('count', 0)
                                            if self.update_game:
                                                stats['to_save'].append({
                                                    'id': result['id'],
                                                    'found': result.get('found', {})
                                                })
                                    else:
                                        stats['no_results'] += 1
                        except Empty:
                            pass
                        break

                    continue

        except KeyboardInterrupt:
            stop_flag.set()
            sys.stderr.write("\n⏹️ Прерывание... Останавливаем потоки...\n")
            sys.stderr.flush()

            # Даем потокам время на завершение
            timeout = 5
            start_wait = time.time()

            # Ждем завершения всех потоков
            for t in threads:
                remaining = timeout - (time.time() - start_wait)
                if remaining > 0:
                    t.join(timeout=remaining)
                else:
                    break

            # Принудительно собираем оставшиеся результаты
            try:
                while True:
                    result = result_queue.get_nowait()
                    processed_count += 1

                    with stats_lock:
                        stats['processed'] += 1
                        stats['all_results'].append(result)

                        is_skipped = result.get('skipped', False)
                        has_results = result.get('has_results', False)
                        reason = result.get('reason', '')

                        if is_skipped:
                            stats['skipped'] += 1
                            if reason == 'no_text':
                                stats['skipped_no_text'] += 1
                            elif reason == 'short':
                                stats['skipped_short'] += 1
                            elif reason == 'error':
                                stats['skipped_error'] += 1
                                stats['errors'] += 1
                            else:
                                stats['skipped_error'] += 1
                        elif has_results:
                            stats['with_results'] += 1
                            stats['total_found'] += result.get('count', 0)

                            if result.get('has_new', False):
                                stats['with_new_results'] += 1
                                stats['total_new_found'] += result.get('count', 0)
                                if self.update_game:
                                    stats['to_save'].append({
                                        'id': result['id'],
                                        'found': result.get('found', {})
                                    })
                        else:
                            stats['no_results'] += 1
            except Empty:
                pass

        # Восстанавливаем оригинальный обработчик сигнала
        signal.signal(signal.SIGINT, original_handler)

        # Финальный сбор всех оставшихся результатов
        try:
            while True:
                result = result_queue.get_nowait()
                processed_count += 1

                with stats_lock:
                    stats['processed'] += 1
                    stats['all_results'].append(result)

                    is_skipped = result.get('skipped', False)
                    has_results = result.get('has_results', False)
                    reason = result.get('reason', '')

                    if is_skipped:
                        stats['skipped'] += 1
                        if reason == 'no_text':
                            stats['skipped_no_text'] += 1
                        elif reason == 'short':
                            stats['skipped_short'] += 1
                        elif reason == 'error':
                            stats['skipped_error'] += 1
                            stats['errors'] += 1
                        else:
                            stats['skipped_error'] += 1
                    elif has_results:
                        stats['with_results'] += 1
                        stats['total_found'] += result.get('count', 0)

                        if result.get('has_new', False):
                            stats['with_new_results'] += 1
                            stats['total_new_found'] += result.get('count', 0)
                            if self.update_game:
                                stats['to_save'].append({
                                    'id': result['id'],
                                    'found': result.get('found', {})
                                })
                    else:
                        stats['no_results'] += 1
        except Empty:
            pass

        # Финальный вывод
        elapsed_total = time.time() - analysis_start
        sys.stderr.write(f"\n✅ Обработано: {stats['processed']}/{len(games)} игр за {format_time(elapsed_total)}\n")
        sys.stderr.flush()

        analysis_time = time.time() - analysis_start
        stats['analysis_time'] = analysis_time
        stats['games_per_second'] = stats['processed'] / analysis_time if analysis_time > 0 else 0
        stats['interrupted'] = stop_flag.is_set()

        return stats

    def _init_progress_bar(self, total_games: int):
        """Инициализирует прогресс-бар без немедленного отображения"""
        if self.no_progress or total_games <= 1:
            return

        try:
            from .analyzer.progress_bar import ProgressBar

            stat_width = max(4, len(str(total_games)))

            self.progress_bar = ProgressBar(
                total=total_games,
                desc="Анализ игр",
                bar_length=30,
                update_interval=0.1,
                stat_width=stat_width,
                emoji_spacing=1
            )

            # Сбрасываем счетчик прогресса
            if hasattr(self.progress_bar, 'current'):
                self.progress_bar.current = 0
            if hasattr(self.progress_bar, '_progress_bar'):
                self.progress_bar._progress_bar.current = 0

            # ОБНУЛЯЕМ СТАТИСТИКУ ПЕРЕД СТАРТОМ
            self.progress_bar.update_stats({
                'found_count': 0,
                'total_criteria_found': 0,
                'skipped_total': 0,
                'not_found_count': 0,
                'errors': 0,
                'updated': 0,
                'in_batch': 0
            })

            # Принудительно отключаем немедленное отображение
            # Устанавливаем флаг, что прогресс-бар еще не должен отображаться
            if hasattr(self.progress_bar, '_progress_bar'):
                self.progress_bar._progress_bar._force_update = lambda: None

            # Отключаем автоматический вывод при инициализации
            if hasattr(self.progress_bar, '_progress_bar') and hasattr(self.progress_bar._progress_bar, 'display'):
                original_display = self.progress_bar._progress_bar.display
                self.progress_bar._progress_bar.display = lambda: None

        except Exception:
            self.progress_bar = None
            self.no_progress = True

    def _bulk_save_results(self, to_save: List[Dict]):
        """Максимально быстрое сохранение для PostgreSQL с индикацией прогресса"""
        if not to_save:
            return

        import sys
        from django.db import connection, transaction

        total_games = len(to_save)
        sys.stderr.write(f"\n💾 Сохранение {total_games} игр в БД...\n")
        sys.stderr.flush()

        save_start = time.time()

        # Получаем названия таблиц через Django ORM
        from games.models import Game, Genre, Theme, PlayerPerspective, GameMode

        # Названия связующих таблиц (many-to-many)
        games_genres_table = Game.genres.through._meta.db_table
        games_themes_table = Game.themes.through._meta.db_table
        games_perspectives_table = Game.player_perspectives.through._meta.db_table
        games_modes_table = Game.game_modes.through._meta.db_table

        # Собираем все связи
        genre_relations = []
        theme_relations = []
        perspective_relations = []
        mode_relations = []

        for item in to_save:
            game_id = item['id']

            for criteria_type, items in item['found'].items():
                if not items:
                    continue
                for crit in items:
                    crit_id = crit['id']
                    if criteria_type == 'genres':
                        genre_relations.append((game_id, crit_id))
                    elif criteria_type == 'themes':
                        theme_relations.append((game_id, crit_id))
                    elif criteria_type == 'perspectives':
                        perspective_relations.append((game_id, crit_id))
                    elif criteria_type == 'game_modes':
                        mode_relations.append((game_id, crit_id))

        total_relations = len(genre_relations) + len(theme_relations) + len(perspective_relations) + len(mode_relations)
        sys.stderr.write(f"   📊 Связей для сохранения: {total_relations}\n")
        sys.stderr.flush()

        # Функция для массовой вставки с динамическими названиями таблиц
        def bulk_insert(relations, table_name, id_field, type_name):
            if not relations:
                return 0

            with connection.cursor() as cursor:
                # Получаем существующие связи
                game_ids = list(set(r[0] for r in relations))
                crit_ids = list(set(r[1] for r in relations))

                cursor.execute(f"""
                    SELECT game_id, {id_field}_id FROM {table_name}
                    WHERE game_id = ANY(%s) AND {id_field}_id = ANY(%s)
                """, [game_ids, crit_ids])
                existing = set(cursor.fetchall())

                # Новые связи
                new_relations = [(g, c) for g, c in relations if (g, c) not in existing]

                if not new_relations:
                    return 0

                # COPY вставка
                import io
                data_buffer = io.StringIO()
                for game_id, crit_id in new_relations:
                    data_buffer.write(f"{game_id}\t{crit_id}\n")
                data_buffer.seek(0)

                cursor.copy_from(data_buffer, table_name,
                                 columns=('game_id', f'{id_field}_id'))

                return len(new_relations)

        # Сохраняем с индикацией прогресса
        with transaction.atomic():
            # Жанры
            sys.stderr.write(f"   📖 Сохранение жанров... ")
            sys.stderr.flush()
            start = time.time()
            genres_added = bulk_insert(genre_relations, games_genres_table, 'genre', 'Жанры')
            sys.stderr.write(f"{genres_added} связей за {time.time() - start:.1f}с\n")
            sys.stderr.flush()

            # Темы
            sys.stderr.write(f"   📖 Сохранение тем... ")
            sys.stderr.flush()
            start = time.time()
            themes_added = bulk_insert(theme_relations, games_themes_table, 'theme', 'Темы')
            sys.stderr.write(f"{themes_added} связей за {time.time() - start:.1f}с\n")
            sys.stderr.flush()

            # Перспективы
            sys.stderr.write(f"   📖 Сохранение перспектив... ")
            sys.stderr.flush()
            start = time.time()
            perspectives_added = bulk_insert(perspective_relations, games_perspectives_table, 'playerperspective',
                                             'Перспективы')
            sys.stderr.write(f"{perspectives_added} связей за {time.time() - start:.1f}с\n")
            sys.stderr.flush()

            # Режимы
            sys.stderr.write(f"   📖 Сохранение режимов... ")
            sys.stderr.flush()
            start = time.time()
            modes_added = bulk_insert(mode_relations, games_modes_table, 'gamemode', 'Режимы')
            sys.stderr.write(f"{modes_added} связей за {time.time() - start:.1f}с\n")
            sys.stderr.flush()

        save_time = time.time() - save_start
        total_added = genres_added + themes_added + perspectives_added + modes_added

        sys.stderr.write(f"\n   ✅ Сохранено за {save_time:.1f}с:\n")
        sys.stderr.write(f"      Жанры: {genres_added} связей\n")
        sys.stderr.write(f"      Темы: {themes_added} связей\n")
        sys.stderr.write(f"      Перспективы: {perspectives_added} связей\n")
        sys.stderr.write(f"      Режимы: {modes_added} связей\n")
        sys.stderr.write(f"      Всего: {total_added} связей\n")
        if save_time > 0 and total_added > 0:
            sys.stderr.write(f"      ⚡ Скорость: {total_added / save_time:.0f} связей/сек\n")
        sys.stderr.flush()

    def _output_results(self, results: List[Dict]):
        """Вывод результатов в файл (только игры с новыми критериями)"""
        if not self.output_file:
            return

        # Фильтруем только игры с новыми критериями
        games_with_new = [r for r in results if r.get('has_new', False)]

        if not games_with_new:
            self.stdout.write("=" * 60)
            self.stdout.write("📊 РЕЗУЛЬТАТЫ АНАЛИЗА")
            self.stdout.write("=" * 60)
            self.stdout.write("\n✅ Нет игр с новыми критериями для сохранения")
            self.stdout.write("=" * 60)
            return

        self.stdout.write("=" * 60)
        self.stdout.write("📊 РЕЗУЛЬТАТЫ АНАЛИЗА (только игры с НОВЫМИ критериями)")
        self.stdout.write("=" * 60)
        self.stdout.write(f"\n📈 Всего игр с новыми критериями: {len(games_with_new)}\n")
        self.stdout.write("=" * 60)

        index = 1
        for r in games_with_new:
            self.stdout.write(f"\n{index}. 🎮 {r['name']} (ID: {r['id']})")

            if r.get('skipped'):
                if r.get('reason') == 'no_text':
                    self.stdout.write("   ⏭️ Пропущено: нет текста для анализа")
                elif r.get('reason') == 'short':
                    self.stdout.write("   ⏭️ Пропущено: текст слишком короткий")
                elif r.get('reason') == 'error':
                    self.stdout.write(f"   ❌ Ошибка: {r.get('error', 'неизвестная ошибка')}")
                index += 1
                continue

            if not r['has_results']:
                self.stdout.write("   ℹ️ Критерии не найдены")
                index += 1
                continue

            pattern_info = r.get('pattern_info', {})

            for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                items = r['found'].get(criteria_type, [])
                if not items:
                    continue

                display_name = self._get_display_name(criteria_type)
                self.stdout.write(f"   📌 {display_name}:")

                for item in items:
                    item_name = item['name']
                    item_id = item['id']

                    context_info = self._find_match_context(pattern_info, criteria_type, item_id, item_name)

                    if context_info:
                        self.stdout.write(f"      • {item_name}")
                        self.stdout.write(f"        {context_info}")
                    else:
                        self.stdout.write(f"      • {item_name}")

                self.stdout.write("")

            if self.verbose and pattern_info:
                self.stdout.write("   🔍 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О СОВПАДЕНИЯХ:")
                for criteria_type, matches in pattern_info.items():
                    if not matches:
                        continue

                    display_name = self._get_display_name(criteria_type)
                    self.stdout.write(f"      📌 {display_name}:")

                    for match in matches:
                        if match.get('status') == 'found':
                            name = match.get('name', 'N/A')
                            matched_text = match.get('matched_text', '')
                            context = match.get('context', '')

                            output = f"         • {name}"
                            if matched_text:
                                output += f" → найдено как \"{matched_text}\""
                            if context:
                                clean_context = ' '.join(context.split())
                                if len(clean_context) > 60:
                                    clean_context = clean_context[:57] + "..."
                                output += f" в контексте: \"{clean_context}\""

                            self.stdout.write(output)

                self.stdout.write("")

            index += 1

        self.stdout.write("=" * 60)
        self.stdout.write(f"✅ Вывод завершен. Показано игр с новыми критериями: {len(games_with_new)}")
        self.stdout.write("=" * 60)

    def _find_match_context(self, pattern_info: Dict, criteria_type: str, item_id: int, item_name: str) -> str:
        """Находит контекст для конкретного элемента"""
        if criteria_type not in pattern_info:
            return ""

        matches_for_category = pattern_info[criteria_type]
        if not matches_for_category:
            return ""

        item_name_lower = item_name.lower()

        for match in matches_for_category:
            if match.get('status') != 'found':
                continue

            match_name = match.get('name', '')
            matched_text = match.get('matched_text', '')
            context = match.get('context', '')
            pattern = match.get('pattern', '')

            if match_name and match_name.lower() == item_name_lower:
                if context:
                    clean_context = ' '.join(context.split())
                    if pattern:
                        return f'("{pattern}" как "{matched_text}" в: "{clean_context}")'
                    else:
                        return f'(найдено как "{matched_text}" в: "{clean_context}")'
                elif matched_text:
                    if pattern:
                        return f'("{pattern}" как "{matched_text}")'
                    else:
                        return f'(найдено как "{matched_text}")'

            if matched_text and matched_text.lower() == item_name_lower:
                if context:
                    clean_context = ' '.join(context.split())
                    if pattern:
                        return f'("{pattern}" как "{matched_text}" в: "{clean_context}")'
                    else:
                        return f'(найдено как "{matched_text}" в: "{clean_context}")'
                else:
                    if pattern:
                        return f'("{pattern}" как "{matched_text}")'
                    else:
                        return f'(найдено как "{matched_text}")'

        return ""

    def _get_display_name(self, key: str) -> str:
        """Возвращает читаемое имя для типа критерия"""
        names = {
            'genres': 'Жанры',
            'themes': 'Темы',
            'perspectives': 'Перспективы',
            'game_modes': 'Режимы игры',
        }
        return names.get(key, key)

    def _print_statistics(self, stats: Dict, start_time: float, interrupted: bool = False):
        """Вывод финальной статистики"""
        total_time = time.time() - start_time

        saved_count = len(stats.get('to_save', [])) if self.update_game else 0

        not_found_count = stats['processed'] - stats['with_results'] - stats['skipped']
        if not_found_count < 0:
            not_found_count = 0

        sys.stderr.write("\n" + "=" * 70 + "\n")
        if interrupted:
            sys.stderr.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО)\n")
        else:
            sys.stderr.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write(f"🔄 Обработано игр: {stats['processed']}\n")
        sys.stderr.write(f"🎯 Игр с найденными критериями: {stats['with_results']}\n")
        sys.stderr.write(f"📈 Всего критериев найдено: {stats['total_found']}\n")
        sys.stderr.write(f"⏭️ Пропущено (нет текста/короткий текст): {stats['skipped']}\n")
        sys.stderr.write(f"⚪ Игр без найденных элементов: {not_found_count}\n")
        sys.stderr.write(f"❌ Ошибок: {stats['errors']}\n")

        if self.update_game and saved_count > 0:
            sys.stderr.write(f"💾 Сохранено в БД: {saved_count} игр\n")

        sys.stderr.write(f"⏱️ Время выполнения: {total_time:.1f} секунд\n")

        if stats.get('games_per_second', 0) > 0:
            sys.stderr.write(f"⚡ Средняя скорость: {stats['games_per_second']:.0f} игр/сек\n")

        sys.stderr.write("=" * 70 + "\n")

        if self.output_path:
            sys.stderr.write(f"✅ Результаты сохранены в: {self.output_path}\n")

        if interrupted:
            sys.stderr.write("\n⚠️ Анализ был прерван. Частичные результаты сохранены.\n")
        else:
            sys.stderr.write("\n✨ Анализ успешно завершен!\n")
        sys.stderr.flush()

    def _setup_output_file(self):
        try:
            directory = os.path.dirname(self.output_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            self.original_stdout = self.stdout._out
            self.output_file = open(self.output_path, 'w', encoding='utf-8')
            self.stdout._out = self.output_file
        except Exception as e:
            self.stderr.write(f"❌ Ошибка открытия файла: {e}")

    def _cleanup(self):
        if self.output_file:
            try:
                self.output_file.close()
                if self.original_stdout:
                    self.stdout._out = self.original_stdout
            except:
                pass

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        import signal

        start_time = time.time()
        results = None
        interrupted = False

        def signal_handler(signum, frame):
            """Обработчик сигнала для корректного завершения"""
            nonlocal interrupted
            interrupted = True
            sys.stderr.write("\n⏹️ Получен сигнал прерывания, завершаем работу...\n")
            sys.stderr.flush()

        # Устанавливаем обработчик сигнала
        original_handler = signal.signal(signal.SIGINT, signal_handler)

        try:
            # Сохраняем параметры
            self.limit = options.get('limit')
            self.offset = options.get('offset', 0)
            self.game_name = options.get('game_name')
            self.verbose = options.get('verbose', False)
            self.threads = min(options.get('threads', 16), 32)
            self.force_restart = options.get('force_restart', False)
            self.output_path = options.get('output')
            self.only_found = options.get('only_found', False)
            self.no_progress = options.get('no_progress', False)
            self.auto_save = options.get('auto_save', False)

            # update_game ВСЕГДА True (по умолчанию обновляем БД)
            self.update_game = True

            # combine_all_texts теперь True по умолчанию, отключается через --no-combine-texts
            self.combine_all_texts = not options.get('no_combine_texts', False)

            # Настройка вывода
            self.output_file = None
            self.original_stdout = None
            if self.output_path:
                self._setup_output_file()

            sys.stderr.write("=" * 70 + "\n")
            sys.stderr.write("🚀 МАКСИМАЛЬНО УСКОРЕННЫЙ АНАЛИЗ КРИТЕРИЕВ\n")
            sys.stderr.write("=" * 70 + "\n")
            sys.stderr.write(f"🔧 Потоков: {self.threads}\n")
            sys.stderr.write(f"🔄 Обновление БД: {'✅' if self.update_game else '❌'}\n")
            if self.update_game:
                sys.stderr.write(
                    f"💾 Режим сохранения: {'Авто' if self.auto_save else 'С подтверждением после анализа'}\n")
            sys.stderr.write(f"📄 Вывод в файл: {'✅' if self.output_path else '❌'}\n")
            sys.stderr.write(f"📚 Объединять все тексты: {'✅' if self.combine_all_texts else '❌'}\n")
            if self.game_name:
                sys.stderr.write(f"🎮 Поиск по имени: '{self.game_name}'\n")
            sys.stderr.write("=" * 70 + "\n")
            sys.stderr.flush()

            if self.force_restart:
                sys.stderr.write("\n🧹 Очищаем кэш...\n")
                sys.stderr.flush()
                RangeCacheManager.clear_all_games()
                sys.stderr.write("   ✅ Готово\n")
                sys.stderr.flush()

            sys.stderr.write("\n📚 Загрузка данных...\n")
            sys.stderr.flush()
            self._preload_all_data()

            sys.stderr.write("\n🎮 Загрузка игр...\n")
            sys.stderr.flush()
            games_to_analyze = self._get_games_to_analyze()
            sys.stderr.write(f"   ✅ Получено {len(games_to_analyze)} игр\n")
            sys.stderr.flush()

            if not games_to_analyze:
                sys.stderr.write("\n✅ Нет игр для анализа\n")
                sys.stderr.flush()
                self._cleanup()
                return

            # Загружаем существующие связи (всегда, так как update_game всегда True)
            sys.stderr.write("\n📚 Загрузка существующих связей для проверки...\n")
            sys.stderr.flush()
            existing_relations = self._load_existing_relations()
            sys.stderr.write(f"   ✅ Загружено связей: жанры={len(existing_relations['genres'])}, "
                             f"темы={len(existing_relations['themes'])}, "
                             f"перспективы={len(existing_relations['perspectives'])}, "
                             f"режимы={len(existing_relations['game_modes'])}\n")
            sys.stderr.flush()

            # Запускаем анализ
            self.progress_bar = None
            results = self._parallel_analysis(games_to_analyze, existing_relations)

            if results:
                # Вывод в файл (только игры с новыми критериями)
                if self.output_path and results.get('all_results'):
                    sys.stderr.write(f"\n📝 Запись в файл (только игры с НОВЫМИ критериями)...\n")
                    sys.stderr.flush()
                    self._output_results(results['all_results'])
                    sys.stderr.write(f"   ✅ {self.output_path}\n")
                    sys.stderr.flush()

                # Статистика после анализа
                total_time = time.time() - start_time
                not_found = results['processed'] - results['with_results'] - results['skipped']
                if not_found < 0:
                    not_found = 0

                sys.stderr.write("\n" + "=" * 70 + "\n")
                if results.get('interrupted'):
                    sys.stderr.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО)\n")
                else:
                    sys.stderr.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
                sys.stderr.write("=" * 70 + "\n")
                sys.stderr.write(f"🔄 Обработано: {results['processed']} игр\n")
                sys.stderr.write(f"🎯 Игр с найденными критериями: {results['with_results']}\n")
                sys.stderr.write(f"✨ Игр с НОВЫМИ критериями: {results['with_new_results']}\n")
                sys.stderr.write(f"📈 Всего критериев: {results['total_found']} (новых: {results['total_new_found']})\n")
                sys.stderr.write(f"⏭️ Пропущено: {results['skipped']}\n")
                sys.stderr.write(f"⚪ Без элементов: {not_found}\n")
                sys.stderr.write(f"❌ Ошибок: {results['errors']}\n")
                sys.stderr.write(f"⏱️ Время: {total_time:.1f}с\n")
                if results.get('games_per_second', 0) > 0:
                    sys.stderr.write(f"⚡ Скорость: {results['games_per_second']:.0f} игр/сек\n")
                sys.stderr.write("=" * 70 + "\n")
                sys.stderr.flush()

                # ЗАПРАШИВАЕМ ПОДТВЕРЖДЕНИЕ ПОСЛЕ АНАЛИЗА (если не auto_save)
                if not self.auto_save and results.get('to_save') and not results.get('interrupted'):
                    sys.stderr.write(
                        f"\n💾 Найдено {len(results['to_save'])} игр с новыми критериями ({results['total_new_found']} новых элементов).\n")
                    sys.stderr.write("Сохранить результаты в БД? (y/N): ")
                    sys.stderr.flush()

                    try:
                        answer = sys.stdin.readline().strip().lower()
                    except (EOFError, KeyboardInterrupt):
                        answer = 'n'

                    if answer in ['y', 'yes', 'да', 'д', '1']:
                        sys.stderr.write("\n✅ Сохраняем...\n")
                        sys.stderr.flush()
                        self._bulk_save_results(results['to_save'])
                        sys.stderr.write("   ✅ Сохранение завершено\n")
                        sys.stderr.flush()
                    else:
                        sys.stderr.write("\n⏭️ Сохранение отменено\n")
                        sys.stderr.flush()
                elif self.auto_save and results.get('to_save') and not results.get('interrupted'):
                    sys.stderr.write(f"\n💾 Авто-сохранение {len(results['to_save'])} игр в БД...\n")
                    sys.stderr.flush()
                    self._bulk_save_results(results['to_save'])
                    sys.stderr.write("   ✅ Сохранение завершено\n")
                    sys.stderr.flush()
                elif results.get('interrupted') and results.get('to_save'):
                    if self.auto_save:
                        sys.stderr.write(f"\n💾 Авто-сохранение {len(results['to_save'])} игр в БД (прервано)...\n")
                        sys.stderr.flush()
                        self._bulk_save_results(results['to_save'])
                        sys.stderr.write("   ✅ Сохранение завершено\n")
                        sys.stderr.flush()
                    else:
                        sys.stderr.write(
                            f"\n💾 Найдено {len(results['to_save'])} игр с новыми критериями ({results['total_new_found']} новых элементов).\n")
                        sys.stderr.write("Сохранить результаты в БД? (y/N): ")
                        sys.stderr.flush()

                        try:
                            answer = sys.stdin.readline().strip().lower()
                        except (EOFError, KeyboardInterrupt):
                            answer = 'n'

                        if answer in ['y', 'yes', 'да', 'д', '1']:
                            sys.stderr.write("\n✅ Сохраняем...\n")
                            sys.stderr.flush()
                            self._bulk_save_results(results['to_save'])
                            sys.stderr.write("   ✅ Сохранение завершено\n")
                            sys.stderr.flush()
                        else:
                            sys.stderr.write("\n⏭️ Сохранение отменено\n")
                            sys.stderr.flush()

        except KeyboardInterrupt:
            sys.stderr.write("\n⏹️ Прервано\n")
            sys.stderr.flush()
            if results and results.get('to_save'):
                sys.stderr.write(f"\n💾 Сохранение {len(results['to_save'])} игр в БД...\n")
                sys.stderr.flush()
                try:
                    self._bulk_save_results(results['to_save'])
                    sys.stderr.write("   ✅ Сохранено\n")
                    sys.stderr.flush()
                except Exception as e:
                    sys.stderr.write(f"   ⚠️ Ошибка: {e}\n")
                    sys.stderr.flush()
        except Exception as e:
            sys.stderr.write(f"\n❌ Ошибка: {e}\n")
            sys.stderr.flush()
            import traceback
            traceback.print_exc(file=sys.stderr)
        finally:
            # Восстанавливаем оригинальный обработчик сигнала
            signal.signal(signal.SIGINT, original_handler)
            self._cleanup()
            sys.stderr.write("\n✨ Завершено\n")
            sys.stderr.flush()
            sys.exit(0)
