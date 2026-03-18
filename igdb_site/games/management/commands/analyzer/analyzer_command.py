# games/management/commands/analyzer/analyzer_command.py
"""
Основной класс команды анализа с использованием нового API - ИСПРАВЛЕННАЯ ВЕРСИЯ
Использует только существующие методы API
"""

import sys
import os
import time
import threading  # <-- ДОБАВЬТЕ ЭТУ СТРОКУ
from typing import Dict, Any, List, Set
from django.core.management.base import BaseCommand
from django.db.models import QuerySet

from games.models import Game
from games.analyze import GameAnalyzerAPI
from .progress_bar import ProgressBar
from .state_manager import StateManager
from .batch_updater import BatchUpdater
from .output_formatter import OutputFormatter
from .text_preparer import TextPreparer
from games.analyze.range_cache import RangeCacheManager
from .analysis_cache import AnalysisCache

class AnalyzerCommand(BaseCommand):
    """Команда анализа игр с использованием API"""

    def __init__(self, *args, **kwargs):
        # Вызываем родительский конструктор
        super().__init__(*args, **kwargs)

        # Инициализируем атрибуты
        self.output_path = None
        self.output_file = None
        self.original_stdout = None
        self.original_stderr = None
        self.stats = {}
        self.api = None
        self.state_manager = None
        self.batch_updater = None
        self.progress_bar = None
        self.output_formatter = None
        self.text_preparer = None
        self.stats_lock = threading.Lock()
        self.program_start_time = None  # <-- ДОБАВИТЬ

        # Простые таймеры
        self.timers = {}
        self.timer_stack = []
        self.timer_results = []

        # Опции
        self.game_id = None
        self.game_name = None
        self.description = None
        self.limit = None
        self.offset = 0
        self.update_game = False
        self.min_text_length = 10
        self.verbose = False
        self.debug = False
        self.only_found = False
        self.batch_size = 1000
        self.ignore_existing = False
        self.hide_skipped = False
        self.no_progress = False
        self.force_restart = False
        self.keywords = False
        self.clear_cache = False
        self.output_path = None
        self.exclude_existing = False

        # Источники текста
        self.use_wiki = False
        self.use_rawg = False
        self.use_storyline = False
        self.prefer_wiki = False
        self.prefer_storyline = False
        self.combine_texts = False
        self.combine_all_texts = False

        # Новые параметры
        self.comprehensive_mode = False
        self.combined_mode = False

        # Дополнительные атрибуты
        self._new_criteria_detected = False
        self._in_batch_update = False
        self.total_games_estimate = 0
        self.debug_mode = False
        self._last_debug_stats = None

        # КЭШ для результатов анализа (огромная оптимизация)
        self.analysis_cache = {}
        self.cache_hits = 0
        self.cache_misses = 0
        self.analysis_cache_manager = AnalysisCache()

    def _handle_error(self, e):
        """Обрабатывает ошибку (для верхнего уровня)"""
        import traceback
        if self.output_file:
            self.stderr.write(f"❌ Неожиданная ошибка: {e}")
            traceback.print_exc()
        else:
            if self.original_stderr:
                self.original_stderr.write(f"❌ Неожиданная ошибка: {e}\n")
                traceback.print_exc(file=self.original_stderr)

    def _handle_analysis_error(self, game, result, stats):
        """Обрабатывает ошибку анализа"""
        with self.stats_lock:
            self.stats['errors'] += 1
            self.stats['error_games'] += 1
            self.stats['processed'] += 1
        stats['errors'] += 1
        stats['processed'] += 1
        self.state_manager.add_processed_game(game.id)

    def _process_games_batch(self, games, should_process_all, new_criteria, checked_criteria, start_time):
        """Обрабатывает батч игр - УПРОЩЕННАЯ ВЕРСИЯ"""
        self.timer_start("Основной цикл обработки")

        if self.batch_updater:
            self.batch_updater.total_games_added = 0

        self._init_stats()
        self._initialize_progress_bar_for_batch()

        # Простая последовательная обработка
        processed = 0
        for game in games.iterator(chunk_size=1000):
            if self.limit and processed >= self.limit:
                break

            # Обрабатываем одну игру
            self._process_single_game(game)
            processed += 1

            # Обновляем прогресс после КАЖДОЙ игры
            if self.progress_bar:
                self.progress_bar.update(1)
                self.progress_bar.update_stats(self._get_progress_stats())

        # Финальное обновление
        if self.progress_bar:
            self.progress_bar.update_stats(self._get_progress_stats())

        self.timer_stop()
        return {'processed_in_this_run': self.stats['processed']}

    def _process_single_game(self, game):
        """Обрабатывает одну игру"""
        try:
            # Получаем текст
            text = self.text_preparer.prepare_text(game)

            if not text:
                with self.stats_lock:
                    self.stats['skipped_no_text'] = self.stats.get('skipped_no_text', 0) + 1
                    self.stats['processed'] = self.stats.get('processed', 0) + 1
                self.state_manager.add_processed_game(game.id)
                return

            # Анализируем
            exclude_existing = self.exclude_existing or (not self.ignore_existing)

            # Проверяем кэш
            cached = self.analysis_cache_manager.get(text, self.keywords, exclude_existing)
            if cached:
                result = cached
                self.cache_hits += 1
            else:
                # Анализируем через API
                if self.keywords:
                    result = self.api.force_analyze_game_text(
                        text=text, game_id=game.id, analyze_keywords=True,
                        existing_game=game, detailed_patterns=self.verbose,
                        exclude_existing=exclude_existing
                    )
                else:
                    result = self.api.force_analyze_game_text(
                        text=text, game_id=game.id, analyze_keywords=False,
                        existing_game=game, detailed_patterns=self.verbose,
                        exclude_existing=exclude_existing
                    )

                # Сохраняем в кэш
                self.analysis_cache_manager.set(text, self.keywords, exclude_existing, result)
                self.cache_misses += 1

            # Обновляем статистику
            has_new_elements = False
            new_elements_count = 0

            if self.keywords:
                items = result.get('results', {}).get('keywords', {}).get('items', [])
                if items:
                    # Проверяем, есть ли новые ключевые слова
                    from games.models import Keyword
                    keyword_ids = [k['id'] for k in items]
                    existing_ids = set(game.keywords.values_list('id', flat=True))
                    new_ids = [kid for kid in keyword_ids if kid not in existing_ids]

                    if new_ids:
                        has_new_elements = True
                        new_elements_count = len(new_ids)
                        self.stats['found_count'] = self.stats.get('found_count', 0) + 1
                        self.stats['total_criteria_found'] = self.stats.get('total_criteria_found', 0) + len(new_ids)
                        result['has_results'] = True
                    else:
                        self.stats['not_found_count'] = self.stats.get('not_found_count', 0) + 1
                else:
                    self.stats['not_found_count'] = self.stats.get('not_found_count', 0) + 1
            else:
                if result['success'] and result['has_results']:
                    found_count = result['summary'].get('found_count', 0)
                    # Здесь нужно проверить, есть ли новые элементы
                    # Для простоты считаем, что если есть результаты и не ignore_existing, то есть новые
                    if not self.ignore_existing:
                        has_new_elements = True
                        new_elements_count = found_count

                    self.stats['found_count'] = self.stats.get('found_count', 0) + 1
                    self.stats['total_criteria_found'] = self.stats.get('total_criteria_found', 0) + found_count
                else:
                    self.stats['not_found_count'] = self.stats.get('not_found_count', 0) + 1

            self.stats['processed'] = self.stats.get('processed', 0) + 1
            self.state_manager.add_processed_game(game.id)

            # ВЫВОД В ФАЙЛ - ДЛЯ ВСЕХ ИГР, даже без результатов
            if self.output_formatter and (result.get('has_results', False) or not self.only_found):
                self.output_formatter.print_game_in_batch(
                    game=game,
                    index=self.stats['processed'],
                    result=result,
                    stats=self.stats,
                    only_found=self.only_found,
                    verbose=self.verbose,
                    keywords=self.keywords,
                    ignore_existing=self.ignore_existing,
                    update_game=self.update_game,
                    comprehensive_mode=False,
                    combined_mode=False,
                    exclude_existing=exclude_existing
                )

            # ===== ВАЖНО: ДОБАВЛЯЕМ В БАТЧ ЕСЛИ НУЖНО =====
            if self.update_game and has_new_elements:
                added = self._add_to_batch_if_needed(game, result, has_new_elements)
                # После добавления проверяем, не пора ли обновить батч
                self._check_batch_update()

        except Exception as e:
            import traceback
            traceback.print_exc(file=sys.stderr)
            self.stats['errors'] = self.stats.get('errors', 0) + 1
            self.stats['processed'] = self.stats.get('processed', 0) + 1

    def _update_game_stats(self, game, result):
        """Обновляет статистику игры"""
        if self.keywords:
            items = result.get('results', {}).get('keywords', {}).get('items', [])
            if items:
                from games.models import Keyword
                keyword_ids = [k['id'] for k in items]
                existing_ids = set(game.keywords.values_list('id', flat=True))
                new_ids = [kid for kid in keyword_ids if kid not in existing_ids]

                if new_ids:
                    with self.stats_lock:
                        self.stats['found_games'] += 1
                        self.stats['found_elements'] += len(new_ids)
                        self.stats['keywords_found'] += 1
                        self.stats['keywords_count'] += len(new_ids)
                        self.stats['found_count'] += 1
                        self.stats['total_criteria_found'] += len(new_ids)

                    if self.update_game:
                        self.batch_updater.add_game_for_update(
                            game_id=game.id, results=result['results'], is_keywords=True
                        )
                        with self.stats_lock:
                            self.stats['updated_games'] += 1
                else:
                    with self.stats_lock:
                        self.stats['keywords_not_found'] += 1
                        self.stats['not_found_count'] += 1
            else:
                with self.stats_lock:
                    self.stats['keywords_not_found'] += 1
                    self.stats['not_found_count'] += 1
        else:
            if result['has_results']:
                found_count = result['summary'].get('found_count', 0)
                with self.stats_lock:
                    self.stats['found_games'] += 1
                    self.stats['found_elements'] += found_count
                    self.stats['found_count'] += 1
                    self.stats['total_criteria_found'] += found_count

                if self.update_game:
                    self.batch_updater.add_game_for_update(
                        game_id=game.id, results=result['results'], is_keywords=False
                    )
                    with self.stats_lock:
                        self.stats['updated_games'] += 1
            else:
                with self.stats_lock:
                    self.stats['not_found_count'] += 1

    def _get_progress_stats(self):
        """Возвращает статистику для прогресс-бара"""
        return {
            'found_count': self.stats.get('found_count', 0),
            'total_criteria_found': self.stats.get('total_criteria_found', 0),
            'skipped_total': self.stats.get('skipped_no_text', 0) + self.stats.get('skipped_short_text', 0),
            # <-- ИСПРАВЛЕНО
            'not_found_count': self.stats.get('not_found_count', 0),
            'errors': self.stats.get('errors', 0),
            'updated': self.stats.get('updated', 0),
            'in_batch': len(self.batch_updater.games_to_update) if self.batch_updater and hasattr(self.batch_updater,
                                                                                                  'games_to_update') else 0,
        }

    def _analyze_game(self, game, text, exclude):
        """Анализирует игру"""
        # Проверяем кэш
        cached = self.analysis_cache_manager.get(text, self.keywords, exclude)
        if cached:
            with self.stats_lock:
                self.cache_hits += 1
                # Помечаем, что результат из кэша
                cached['_cached'] = True
                # Обновляем статистику для закэшированного результата
                self._update_game_stats_from_cache(game, cached)
            return cached

        # Анализируем через API
        if hasattr(self, 'comprehensive_mode') and self.comprehensive_mode:
            result = self.api.analyze_game_text_comprehensive(
                text=text, game_id=game.id, existing_game=game,
                detailed_patterns=self.verbose, exclude_existing=exclude
            )
        elif hasattr(self, 'combined_mode') and self.combined_mode:
            result = self.api.analyze_game_text_combined(
                text=text, game_id=game.id, existing_game=game,
                detailed_patterns=self.verbose, exclude_existing=exclude
            )
        else:
            result = self.api.force_analyze_game_text(
                text=text, game_id=game.id, analyze_keywords=self.keywords,
                existing_game=game, detailed_patterns=self.verbose,
                exclude_existing=exclude
            )

        # Помечаем, что результат НЕ из кэша
        result['_cached'] = False

        # Сохраняем в кэш
        with self.stats_lock:
            self.cache_misses += 1
            self.analysis_cache_manager.set(text, self.keywords, exclude, result)

        return result

    def _update_game_stats_from_cache(self, game, result):
        """Обновляет статистику для закэшированного результата"""
        with self.stats_lock:  # ОБЯЗАТЕЛЬНО блокируем
            if self.keywords:
                items = result.get('results', {}).get('keywords', {}).get('items', [])
                if items:
                    from games.models import Keyword
                    keyword_ids = [k['id'] for k in items]
                    existing_ids = set(game.keywords.values_list('id', flat=True))
                    new_ids = [kid for kid in keyword_ids if kid not in existing_ids]

                    if new_ids:
                        self.stats['found_games'] = self.stats.get('found_games', 0) + 1
                        self.stats['found_elements'] = self.stats.get('found_elements', 0) + len(new_ids)
                        self.stats['keywords_found'] = self.stats.get('keywords_found', 0) + 1
                        self.stats['keywords_count'] = self.stats.get('keywords_count', 0) + len(new_ids)
                        self.stats['found_count'] = self.stats.get('found_count', 0) + 1
                        self.stats['total_criteria_found'] = self.stats.get('total_criteria_found', 0) + len(new_ids)
                    else:
                        self.stats['keywords_not_found'] = self.stats.get('keywords_not_found', 0) + 1
                        self.stats['not_found_count'] = self.stats.get('not_found_count', 0) + 1
                else:
                    self.stats['keywords_not_found'] = self.stats.get('keywords_not_found', 0) + 1
                    self.stats['not_found_count'] = self.stats.get('not_found_count', 0) + 1
            else:
                if result['has_results']:
                    found_count = result['summary'].get('found_count', 0)
                    self.stats['found_games'] = self.stats.get('found_games', 0) + 1
                    self.stats['found_elements'] = self.stats.get('found_elements', 0) + found_count
                    self.stats['found_count'] = self.stats.get('found_count', 0) + 1
                    self.stats['total_criteria_found'] = self.stats.get('total_criteria_found', 0) + found_count
                else:
                    self.stats['not_found_count'] = self.stats.get('not_found_count', 0) + 1

    def _flush_batch(self):
        """Финальное обновление батча"""
        self.timer_start("Финальное обновление батча")
        if self.update_game and self.batch_updater:
            if hasattr(self.batch_updater, 'games_to_update') and self.batch_updater.games_to_update:
                updated = self.batch_updater.flush()
                self.stats['updated'] += updated
                self.stats['updated_games'] += updated
        self.timer_stop()

    def _prefetch_games(self, games):
        """Предзагружает игры"""
        self.timer_start("Предзагрузка игр")
        if self.keywords:
            games_with_data = games.select_related().prefetch_related('keywords')
        else:
            games_with_data = games.select_related().prefetch_related(
                'genres', 'themes', 'player_perspectives', 'game_modes'
            )
        self.timer_stop()
        return games_with_data

    def _get_worker_count(self):
        """Определяет количество потоков"""
        import multiprocessing
        cpu_count = multiprocessing.cpu_count()
        max_workers = min(8, cpu_count * 2)
        if self.verbose:
            self.stdout.write(f"🔄 Запускаем {max_workers} потоков")
        return max_workers

    def _start_progress_updater(self):
        """Запускает поток обновления прогресс-бара"""
        self.stop_updater = threading.Event()

        def updater():
            while not self.stop_updater.is_set():
                time.sleep(0.5)
                with self.stats_lock:
                    if self.progress_bar:
                        self.progress_bar.update_stats(self._get_stats())
                        if hasattr(self.progress_bar, 'current'):
                            self.progress_bar.current = self.stats.get('processed', 0)

        thread = threading.Thread(target=updater)
        thread.daemon = True
        thread.start()
        return thread

    def _stop_progress_updater(self, thread):
        """Останавливает поток прогресс-бара"""
        self.stop_updater.set()
        thread.join(timeout=1)
        if self.progress_bar:
            self.progress_bar.update_stats(self._get_stats())
            if hasattr(self.progress_bar, 'current'):
                self.progress_bar.current = self.stats.get('processed', 0)

    def _get_stats(self):
        """Возвращает статистику"""
        return {
            'found_count': self.stats.get('found_count', 0),
            'total_criteria_found': self.stats.get('total_criteria_found', 0),
            'skipped_total': self.stats.get('skipped_total', 0),
            'not_found_count': self.stats.get('not_found_count', 0),
            'errors': self.stats.get('errors', 0),
            'updated': self.stats.get('updated', 0),
            'in_batch': len(self.batch_updater.games_to_update) if self.batch_updater else 0,
        }

    def _run_parallel_processing(self, games_with_data, max_workers):
        """Запускает параллельную обработку"""
        self.timer_start("Параллельная обработка")

        import concurrent.futures
        from itertools import islice

        def chunks(iterable, size):
            iterator = iter(iterable)
            while True:
                chunk = list(islice(iterator, size))
                if not chunk:
                    break
                yield chunk

        self.stats_lock = threading.Lock()

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for chunk in chunks(games_with_data.iterator(chunk_size=1000), 50):
                if self.stop_event.is_set():
                    break
                futures.append(executor.submit(self._process_chunk, chunk))

            for future in concurrent.futures.as_completed(futures):
                if self.stop_event.is_set():
                    break
                self._process_future(future)

        self.timer_stop()

    def _process_chunk(self, chunk):
        """Обрабатывает чанк игр"""
        local_stats = {k: 0 for k in ['processed', 'found_games', 'found_elements',
                                      'skipped_games', 'errors', 'updated_games',
                                      'keywords_found', 'keywords_count',
                                      'keywords_not_found', 'not_found_count']}

        for game in chunk:
            if self.stop_event.is_set():
                break
            self._process_game(game, local_stats)

        return local_stats

    def _process_game(self, game, stats):
        """Обрабатывает одну игру"""
        try:
            text = self.text_preparer.prepare_text(game)
            if not text:
                self._handle_no_text(game, stats)
                return

            exclude = self.exclude_existing or (not self.ignore_existing)
            result = self._analyze_game(game, text, exclude)

            if not result['success']:
                self._handle_analysis_error(game, result, stats)
                return

            self._update_stats(game, result, stats)
            self._output_result(game, result, exclude)

            stats['processed'] += 1
            self.state_manager.add_processed_game(game.id)

        except Exception as e:
            self._handle_exception(game, e, stats)

    def _handle_no_text(self, game, stats):
        """Обрабатывает игру без текста"""
        with self.stats_lock:
            self.stats['skipped_no_text'] += 1
            self.stats['skipped_games'] += 1
            self.stats['processed'] += 1
        stats['skipped_games'] += 1
        stats['processed'] += 1
        self.state_manager.add_processed_game(game.id)

    def _handle_exception(self, game, error, stats):
        """Обрабатывает исключение"""
        with self.stats_lock:
            self.stats['errors'] += 1
            self.stats['processed'] += 1
        stats['errors'] += 1
        stats['processed'] += 1

    def _update_stats(self, game, result, stats):
        """Обновляет статистику"""
        if self.keywords:
            items = result.get('results', {}).get('keywords', {}).get('items', [])
            if items:
                from games.models import Keyword
                ids = [k['id'] for k in items]
                existing = set(game.keywords.values_list('id', flat=True))
                new = [i for i in ids if i not in existing]

                if new:
                    stats['found_games'] += 1
                    stats['found_elements'] += len(new)
                    stats['keywords_found'] += 1
                    stats['keywords_count'] += len(new)

                    if self.update_game:
                        self.batch_updater.add_game_for_update(
                            game_id=game.id, results=result['results'], is_keywords=True
                        )
                        stats['updated_games'] += 1
                else:
                    stats['keywords_not_found'] += 1
            else:
                stats['keywords_not_found'] += 1
        else:
            if result['has_results']:
                count = result['summary'].get('found_count', 0)
                stats['found_games'] += 1
                stats['found_elements'] += count

                if self.update_game:
                    self.batch_updater.add_game_for_update(
                        game_id=game.id, results=result['results'], is_keywords=False
                    )
                    stats['updated_games'] += 1
            else:
                stats['not_found_count'] += 1

    def _output_result(self, game, result, exclude):
        """Выводит результат"""
        if result['has_results'] or not self.only_found:
            self.output_formatter.print_game_in_batch(
                game=game, index=0, result=result, stats=self.stats,
                only_found=self.only_found, verbose=self.verbose,
                keywords=self.keywords, ignore_existing=self.ignore_existing,
                update_game=self.update_game, comprehensive_mode=False,
                combined_mode=False, exclude_existing=exclude
            )

    def _process_future(self, future):
        """Обрабатывает результат задачи"""
        try:
            stats = future.result()
            with self.stats_lock:
                for k, v in stats.items():
                    if k in self.stats:
                        self.stats[k] += v
                    elif k == 'keywords_found':
                        self.stats['keywords_found'] += v
                    elif k == 'keywords_count':
                        self.stats['keywords_count'] += v
                    elif k == 'keywords_not_found':
                        self.stats['keywords_not_found'] += v
                    elif k == 'not_found_count':
                        self.stats['not_found_count'] += v
        except Exception as e:
            if self.verbose:
                self.stderr.write(f"❌ Ошибка: {e}")

    def _display_final_statistics(self, stats: Dict[str, Any], already_processed: int, total_games: int):
        """Выводит финальную статистику в терминал и файл"""
        checked_criteria_count = len(self.state_manager.get_checked_criteria()) if self.state_manager else 0

        if self.output_file and not self.output_file.closed:
            try:
                self.output_file.write("\n" + "=" * 60 + "\n")
                if self.keywords:
                    self.output_file.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КЛЮЧЕВЫЕ СЛОВА)\n")
                else:
                    self.output_file.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КРИТЕРИИ)\n")
                self.output_file.write("=" * 60 + "\n")

                if already_processed > 0:
                    self.output_file.write(f"⏭️ Пропущено ранее обработанных игр: {already_processed}\n")

                if self.keywords:
                    processed_count = stats.get('keywords_processed', stats.get('processed', 0))
                    self.output_file.write(f"🔄 Обработано новых игр: {processed_count}\n")
                    self.output_file.write(f"🎯 Игр с найденными ключ. словами: {stats.get('keywords_found', 0)}\n")
                    self.output_file.write(f"📈 Всего ключевых слов найдено: {stats.get('keywords_count', 0)}\n")
                    self.output_file.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")
                    self.output_file.write(f"💾 Обновлено игр в БД: {stats.get('updated', 0)}\n")

                    if stats.get('keywords_not_found', 0) > 0:
                        self.output_file.write(f"⚡ Игр без ключевых слов: {stats['keywords_not_found']}\n")
                else:
                    self.output_file.write(f"🔄 Обработано новых игр: {stats.get('processed', 0)}\n")
                    self.output_file.write(f"🎯 Игр с найденными критериями: {stats.get('found_count', 0)}\n")
                    self.output_file.write(f"📈 Всего критериев найдено: {stats.get('total_criteria_found', 0)}\n")
                    self.output_file.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")
                    self.output_file.write(f"💾 Обновлено игр в БД: {stats.get('updated', 0)}\n")

                    if stats.get('not_found_count', 0) > 0:
                        self.output_file.write(f"⚡ Игр без критериев: {stats['not_found_count']}\n")

                total_skipped = stats['skipped_no_text'] + stats.get('skipped_short_text', 0) + (
                    stats['keywords_not_found'] if self.keywords else stats['not_found_count']
                )

                self.output_file.write(f"⏭️ Всего пропущено игр: {total_skipped}\n")
                self.output_file.write(f"⏭️ Игр без текста: {stats['skipped_no_text']}\n")

                if 'skipped_short_text' in stats and stats['skipped_short_text'] > 0:
                    self.output_file.write(f"⏭️ Игр с коротким текстом: {stats['skipped_short_text']}\n")

                if self.keywords and stats.get('keywords_not_found', 0) > 0:
                    self.output_file.write(f"⏭️ Игр без ключевых слов: {stats['keywords_not_found']}\n")
                elif not self.keywords and stats.get('not_found_count', 0) > 0:
                    self.output_file.write(f"⏭️ Игр без критериев: {stats['not_found_count']}\n")

                if stats['execution_time'] > 0:
                    self.output_file.write(f"⏱️ Время выполнения: {stats['execution_time']:.1f} секунд\n")

                self.output_file.write("=" * 60 + "\n")
                self.output_file.write("✅ Анализ успешно завершен\n")
                self.output_file.write("=" * 60 + "\n")
                self.output_file.flush()

            except Exception:
                pass

        if self.original_stdout:
            try:
                self.original_stdout.write("\n")
                self.original_stdout.write("=" * 60 + "\n")
                self.original_stdout.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
                self.original_stdout.write("=" * 60 + "\n")

                if stats.get('processed', 0) > 0:
                    if self.keywords:
                        processed_count = stats.get('keywords_processed', stats.get('processed', 0))
                        self.original_stdout.write(f"🔄 Обработано игр: {processed_count}\n")
                        self.original_stdout.write(f"🎯 Игр с ключевыми словами: {stats.get('keywords_found', 0)}\n")
                        self.original_stdout.write(f"📈 Всего ключевых слов: {stats.get('keywords_count', 0)}\n")
                    else:
                        self.original_stdout.write(f"🔄 Обработано игр: {stats.get('processed', 0)}\n")
                        self.original_stdout.write(f"🎯 Игр с критериями: {stats.get('found_count', 0)}\n")
                        self.original_stdout.write(f"📈 Всего критериев: {stats.get('total_criteria_found', 0)}\n")

                    self.original_stdout.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")
                    self.original_stdout.write(f"💾 Обновлено игр в БД: {stats.get('updated', 0)}\n")

                    # Статистика кэша
                    cache_stats = self.analysis_cache_manager.get_stats()
                    if cache_stats['total_requests'] > 0:
                        self.original_stdout.write(
                            f"📊 Кэш анализа: {cache_stats['hit_rate']} попаданий ({cache_stats['hits']}/{cache_stats['total_requests']})\n")
                        self.original_stdout.write(
                            f"   Память: {cache_stats['memory_entries']} записей, Диск: {cache_stats['disk_entries']} файлов ({cache_stats['disk_size_mb']:.1f} МБ)\n")

                    if stats.get('execution_time', 0) > 0:
                        games_per_second = stats.get('processed', 0) / stats['execution_time'] if stats[
                                                                                                      'execution_time'] > 0 else 0
                        self.original_stdout.write(f"⏱️ Время: {stats['execution_time']:.1f} секунд\n")
                        self.original_stdout.write(f"⚡ Скорость: {games_per_second:.1f} игр/сек\n")

                if self.output_path:
                    self.original_stdout.write(f"✅ Результаты сохранены в: {self.output_path}\n")
                else:
                    self.original_stdout.write("✅ Анализ завершен\n")

                self.original_stdout.write("=" * 60 + "\n")
                self.original_stdout.flush()

            except Exception:
                pass

    def _analyze_all_games_execute(self, prepared_data):
        """Выполнение массового анализа с подготовленными данными"""
        if not prepared_data:
            return

        try:
            # Распаковываем подготовленные данные
            games = prepared_data['games']
            total_games = prepared_data['total_games']
            games_to_process = prepared_data['games_to_process']
            estimated_new_games = prepared_data['estimated_new_games']
            already_processed = prepared_data['already_processed']
            checked_criteria = prepared_data['checked_criteria']
            new_criteria = prepared_data['new_criteria']
            should_process_all = prepared_data['should_process_all']

            # Вызываем вторую часть - выполнение анализа
            self._execute_game_analysis(
                games, total_games, games_to_process, estimated_new_games,
                already_processed, checked_criteria, new_criteria,
                should_process_all, start_time=time.time()
            )

        except Exception as e:
            self.stderr.write(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА В ВЫПОЛНЕНИИ МАССОВОГО АНАЛИЗА: {e}")
            import traceback
            traceback.print_exc(file=self.stderr._out)
            raise

    def timer_start(self, name):
        """Начинает замер времени - ИСПРАВЛЕНО (без дублирования)"""
        # Проверяем, не запущен ли уже такой же таймер на вершине стека
        if self.timer_stack and self.timer_stack[-1] == name:
            return

        self.timer_stack.append(name)
        self.timers[name] = time.monotonic()

    def timer_stop(self):
        """Останавливает последний замер времени - ИСПРАВЛЕНО (правильный level)"""
        if not self.timer_stack:
            return

        name = self.timer_stack.pop()
        if name in self.timers:
            elapsed = time.monotonic() - self.timers[name]
            # level = глубина стека ПОСЛЕ извлечения (это правильный уровень для текущего замера)
            level = len(self.timer_stack)

            # Сохраняем результат с полным путем
            full_path = ' -> '.join(self.timer_stack + [name]) if self.timer_stack else name
            self.timer_results.append({
                'name': full_path,
                'elapsed': elapsed,
                'level': level  # Используем правильный level
            })
            del self.timers[name]

    def print_timers(self):
        """Выводит результаты замеров времени"""
        if not self.timer_results:
            return

        total_program_time = time.monotonic() - self.program_start_time

        print("\n" + "=" * 70, file=self.original_stdout)
        print("⏱️ СТАТИСТИКА ВРЕМЕНИ ВЫПОЛНЕНИЯ (секунды)", file=self.original_stdout)
        print("=" * 70, file=self.original_stdout)

        # Группируем по имени операции (без пути)
        aggregated = {}
        for result in self.timer_results:
            # Берем только последнюю часть имени
            name = result['name'].split(' -> ')[-1]
            if name not in aggregated:
                aggregated[name] = {
                    'total': 0,
                    'count': 0,
                    'level': result['level']
                }
            aggregated[name]['total'] += result['elapsed']
            aggregated[name]['count'] += 1

        # Сортируем по убыванию времени
        for name, data in sorted(aggregated.items(), key=lambda x: x[1]['total'], reverse=True):
            indent = "  " * data['level']
            if data['count'] > 1:
                avg = data['total'] / data['count']
                print(f"{indent}{name:<40} {data['total']:>8.2f}s [x{data['count']}, avg: {avg:.2f}s]",
                      file=self.original_stdout)
            else:
                print(f"{indent}{name:<40} {data['total']:>8.2f}s", file=self.original_stdout)

        print("-" * 70, file=self.original_stdout)
        print(f"{'ВСЕГО ВРЕМЯ РАБОТЫ':<40} {total_program_time:>8.2f}s", file=self.original_stdout)
        print("=" * 70, file=self.original_stdout)
        print("", file=self.original_stdout)
        self.original_stdout.flush()

    def _format_time(self, seconds):
        """Форматирует время в секундах"""
        if seconds < 0.001:
            return f"{seconds * 1000000:.0f}µs"
        elif seconds < 1.0:
            return f"{seconds * 1000:.2f}ms"
        else:
            return f"{seconds:.2f}s"

    def _get_text_hash(self, text: str) -> str:
        """Быстрый хеш для текста (используем первые 100 символов + длина)"""
        if len(text) > 100:
            return f"{text[:100]}_{len(text)}"
        return text

    def _process_game_with_text(self, game, text, checked_criteria, force_process):
        """Обрабатывает игру с текстом - анализируем любой текст, даже короткий"""
        self.stats['processed_with_text'] += 1
        self.stats['processed'] += 1

        try:
            exclude_existing = self.exclude_existing or (not self.ignore_existing and not force_process)

            use_cache = not force_process and not self.ignore_existing
            cache_key = None

            if use_cache:
                text_hash = self._get_text_hash(text)
                cache_key = f"{text_hash}_{self.keywords}_{exclude_existing}"

                if cache_key in self.analysis_cache:
                    result = self.analysis_cache[cache_key]
                    self.cache_hits += 1

                    if self.debug and self.cache_hits % 100 == 0:
                        self.original_stdout.write(
                            f"🔍 Кэш попаданий: {self.cache_hits}, промахов: {self.cache_misses}\n")

                    self.timer_start(f"Обработка результатов (кэш)")
                    self._handle_analysis_results(game, result, force_process, exclude_existing)
                    self.timer_stop()

                    self._check_batch_update()
                    self._update_progress_bar_with_stats()
                    return

            self.cache_misses += 1

            if self.verbose and self.original_stdout:
                self.original_stdout.write(f"\n🔍 Начинаем обработку игры {game.id}: {game.name}\n")
                self.original_stdout.write(f"📄 Текст получен, длина: {len(text)} символов\n")
                self.original_stdout.write(f"⚙️ Настройки: exclude_existing={exclude_existing}\n")
                self.original_stdout.flush()

            # API-анализ уже замеряется в _process_games_batch
            result = self._analyze_game_text(game, text, exclude_existing)

            if not result['success']:
                self._handle_analysis_error(game, result)
                return

            if use_cache and cache_key:
                self.analysis_cache[cache_key] = result

                if len(self.analysis_cache) > 10000:
                    items_to_remove = len(self.analysis_cache) // 5
                    for _ in range(items_to_remove):
                        self.analysis_cache.pop(next(iter(self.analysis_cache)))

            # Обработка результатов
            self.timer_start(f"Обработка результатов")
            self._handle_analysis_results(game, result, force_process, exclude_existing)
            self.timer_stop()

            self._check_batch_update()
            self._update_progress_bar_with_stats()

        except Exception as e:
            self._handle_processing_error(game, e)

    def update_batch_count(self):
        """Обновляет счетчик игр в батче"""
        if self.batch_updater:
            if hasattr(self.batch_updater, 'games_to_update'):
                self.stats['in_batch'] = len(self.batch_updater.games_to_update)
            else:
                self.stats['in_batch'] = 0

            self._update_progress_bar_with_stats()

    def _reset_batch_count(self):
        """Сбрасывает счетчик игр в батче"""
        self.stats['in_batch'] = 0
        if self.progress_bar:
            self._update_progress_bar_with_stats()

    def _add_game_to_batch_progress(self, game_id: int, added_to_batch: bool):
        """Обновляет статистику батча для прогресс-бара"""
        if self.batch_updater:
            games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                'games_to_update') else 0

            if added_to_batch:
                self.stats['in_batch'] = games_in_batch
            else:
                self.stats['in_batch'] = games_in_batch

            self._update_progress_bar_with_stats()

    def _debug_print(self, message):
        """Выводит отладочное сообщение"""
        if self.debug_mode and self.original_stderr:
            self.original_stderr.write(f"DEBUG: {message}\n")
            self.original_stderr.flush()

    def _reset_progress_bar(self, total_games: int):
        """Полностью сбрасывает прогресс-бар перед началом обработки"""
        if self.progress_bar:
            self.progress_bar.finish()
            self.progress_bar = None

        if not self.no_progress and total_games > 1:
            self._clean_output_before_progress_bar()
            self.progress_bar = self._init_progress_bar(total_games)

    def _reset_statistics_for_new_run(self):
        """Полностью сбрасывает статистику для нового запуска"""
        self._init_stats()

        if self.original_stdout:
            self.original_stdout.write(f"\n🔄 Статистика пропусков сброшена для нового запуска\n")
            self.original_stdout.write(f"⏭️ Пропуски будут считаться заново, начиная с оффсета {self.offset}\n")
            self.original_stdout.flush()

        if self.output_file:
            self.stdout.write(f"\n🔄 Статистика пропусков сброшена для нового запуска")
            self.stdout.write(f"⏭️ Пропуски будут считаться заново, начиная с оффсета {self.offset}")

    def _load_offset_from_file(self):
        """Загружает оффсет из файла для истории"""
        if not self.output_path:
            return None

        try:
            offset_file = os.path.join(os.path.dirname(self.output_path), "last_offset.txt")
            if os.path.exists(offset_file):
                with open(offset_file, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line and first_line.isdigit():
                        return int(first_line)
        except Exception:
            pass

        return None

    def _update_progress_bar_with_stats(self):
        """Обновляет статистику в прогресс-баре"""
        if not self.progress_bar:
            return

        if self.keywords:
            stats_to_update = {
                'found_count': self.stats.get('keywords_found', 0),
                'total_criteria_found': self.stats.get('keywords_count', 0),
                'skipped_total': self.stats.get('skipped_games', 0),
                'not_found_count': self.stats.get('keywords_not_found', 0),
                'errors': self.stats.get('error_games', 0),
                'updated': self.stats.get('updated_games', 0),
                'in_batch': self.stats.get('in_batch', 0),
            }
        else:
            stats_to_update = {
                'found_count': self.stats.get('found_games', 0),
                'total_criteria_found': self.stats.get('found_elements', 0),
                'skipped_total': self.stats.get('skipped_games', 0),
                'not_found_count': self.stats.get('not_found_count', 0),
                'errors': self.stats.get('error_games', 0),
                'updated': self.stats.get('updated_games', 0),
                'in_batch': self.stats.get('in_batch', 0),
            }
        self.progress_bar.update_stats(stats_to_update)

    def _save_offset_to_file(self, offset):
        """Сохраняет оффсет в файл для истории"""
        if not self.output_path:
            return

        try:
            offset_file = os.path.join(os.path.dirname(self.output_path), "last_offset.txt")
            with open(offset_file, 'w', encoding='utf-8') as f:
                f.write(str(offset))
                f.write(f"\n# Сохранено: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                f.write(f"\n# Команда: analyze_game_criteria --offset {offset}")
                if self.keywords:
                    f.write(" --keywords")
                if self.update_game:
                    f.write(" --update-game")
        except Exception:
            pass

    def _get_current_command_string(self):
        """Возвращает строку с текущими параметрами команды"""
        params = []

        if self.keywords:
            params.append("--keywords")
        if self.update_game:
            params.append("--update-game")
        if self.output_path:
            params.append(f"--output {self.output_path}")
        if self.only_found:
            params.append("--only-found")
        if self.hide_skipped:
            params.append("--hide-skipped")
        if self.batch_size != 1000:
            params.append(f"--batch-size {self.batch_size}")
        if self.combine_all_texts:
            params.append("--combine-all-texts")

        return " ".join(params)

    def _display_interruption_statistics_with_offset(self, stats: Dict[str, Any], already_processed: int,
                                                     last_offset: int):
        """Выводит статистику при прерывании с показом последнего оффсета"""
        if self.output_file and not self.output_file.closed:
            self.stdout.write("\n" + "=" * 60)
            self.stdout.write("⏹️ ОБРАБОТКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ")
            self.stdout.write("=" * 60)

            self.stdout.write(f"📍 ОФФСЕТ ДЛЯ ПРОДОЛЖЕНИЯ: {last_offset}")
            self.stdout.write(f"💾 Используйте --offset {last_offset} для продолжения с этого места")

            if self.keywords:
                key_stats = [
                    ('🔄 Обработано игр', stats['processed']),
                    ('🎯 Игр с найденными ключ. словами', stats['keywords_found']),
                    ('📈 Всего ключевых слов найдено', stats['keywords_count']),
                    ('❌ Ошибок', stats['errors']),
                    ('💾 Обновлено игр', stats['updated']),
                ]
            else:
                key_stats = [
                    ('🔄 Обработано игр', stats['processed']),
                    ('🎯 Игр с найденными критериями', stats['found_count']),
                    ('📈 Всего критериев найдено', stats['total_criteria_found']),
                    ('❌ Ошибок', stats['errors']),
                    ('💾 Обновлено игр', stats['updated']),
                ]

            for display_name, value in key_stats:
                self.stdout.write(f"{display_name}: {value}")

            total_skipped = stats['skipped_no_text'] + stats.get('skipped_short_text', 0) + already_processed

            self.stdout.write(f"⏭️ Всего пропущено игр: {total_skipped}")
            self.stdout.write(f"   ↳ без текста: {stats['skipped_no_text']}")

            if 'skipped_short_text' in stats and stats['skipped_short_text'] > 0:
                self.stdout.write(f"   ↳ с коротким текстом: {stats['skipped_short_text']}")

            if already_processed > 0:
                self.stdout.write(f"   ↳ ранее обработанных: {already_processed}")

            if stats['execution_time'] > 0:
                games_per_second = stats['processed'] / stats['execution_time'] if stats['execution_time'] > 0 else 0
                self.stdout.write(f"⏱️ Время выполнения до прерывания: {stats['execution_time']:.1f} секунд")
                self.stdout.write(f"⚡ Скорость обработки: {games_per_second:.1f} игр/секунду")

                remaining_games = self.total_games_estimate - stats['processed'] if hasattr(self,
                                                                                            'total_games_estimate') else 0
                if remaining_games > 0 and games_per_second > 0:
                    remaining_time = remaining_games / games_per_second
                    self.stdout.write(
                        f"⏳ Осталось обработать примерно: {remaining_time:.1f} секунд ({remaining_games} игр)")

            self.stdout.write("=" * 60)
            self.stdout.flush()

        if self.original_stdout:
            pass

    def _reset_batch_after_update(self):
        """Сбрасывает состояние батча после обновления"""
        if self.batch_updater:
            self.batch_updater.total_games_added = 0
            if hasattr(self.batch_updater, 'games_to_update'):
                self.batch_updater.games_to_update.clear()

    def _process_single_game_in_batch_with_criteria(self, game, checked_criteria, force_process=False):
        """Обрабатывает одну игру с учетом проверенных критериев - С ЛОГИРОВАНИЕМ"""
        if not game.summary and not game.storyline and not game.rawg_description and not game.wiki_description:
            self._handle_game_without_text(game)
            return

        import sys

        has_progress_bar = self.progress_bar is not None
        if has_progress_bar and sys.stderr:
            sys.stderr.write("\033[s")

        if self.debug and self.original_stdout:
            self.original_stdout.write(
                f"\nDEBUG _process_single_game_in_batch_with_criteria: начинаем обработку игры {game.id}: {game.name}\n")
            self.original_stdout.flush()

        # Получение текста
        self.timer_start(f"Получение текста")
        text = self.text_preparer.prepare_text(game)
        self.timer_stop()

        # Логируем длину текста для аномальных игр
        if game.id == 802 or len(text) > 10000:
            print(f"\n🔍 Игра {game.id} ({game.name}) - длина текста: {len(text)} символов", file=sys.stderr)
            if hasattr(game, 'summary') and game.summary:
                print(f"   summary: {len(game.summary)} символов", file=sys.stderr)
            if hasattr(game, 'storyline') and game.storyline:
                print(f"   storyline: {len(game.storyline)} символов", file=sys.stderr)
            if hasattr(game, 'rawg_description') and game.rawg_description:
                print(f"   rawg: {len(game.rawg_description)} символов", file=sys.stderr)
            if hasattr(game, 'wiki_description') and game.wiki_description:
                print(f"   wiki: {len(game.wiki_description)} символов", file=sys.stderr)

        if not text:
            if has_progress_bar and sys.stderr:
                sys.stderr.write("\033[u")
                sys.stderr.flush()

            self._handle_game_without_text(game)
            return

        if has_progress_bar and sys.stderr:
            sys.stderr.write("\033[u")
            sys.stderr.flush()

        self._process_game_with_text(game, text, checked_criteria, force_process)

    def _handle_game_without_text(self, game):
        """Обрабатывает игру без текста"""
        if self.debug and self.original_stdout:
            self.original_stdout.write(f"DEBUG _handle_game_without_text: игра {game.id}\n")
            self.original_stdout.flush()

        self.stats['skipped_no_text'] += 1
        self.stats['skipped_games'] += 1
        self.stats['processed'] += 1

        self.state_manager.add_processed_game(game.id)

        if self.progress_bar:
            self.progress_bar.update(1)
            self._update_progress_bar_with_stats()

    def _handle_short_text_game(self, game):
        """Обрабатывает игру с коротким текстом"""
        if self.debug and self.original_stdout:
            self.original_stdout.write(
                f"DEBUG _handle_short_text_game: игра {game.id}, длина текста < {self.min_text_length}\n")
            self.original_stdout.flush()

        self.stats['skipped_short_text'] += 1
        self.stats['skipped_games'] += 1
        self.stats['processed'] += 1

        self.state_manager.add_processed_game(game.id)

        if self.progress_bar:
            self.progress_bar.update(1)
            self._update_progress_bar_with_stats()

    def _analyze_game_text(self, game, text, exclude_existing):
        """Анализирует текст игры с помощью API - без проверки длины"""
        self.timer_start(f"API-анализ игры {game.id}")

        if hasattr(self, 'comprehensive_mode') and self.comprehensive_mode:
            result = self.api.analyze_game_text_comprehensive(
                text=text,
                game_id=game.id,
                existing_game=game,
                detailed_patterns=self.verbose,
                exclude_existing=exclude_existing
            )
        elif hasattr(self, 'combined_mode') and self.combined_mode:
            result = self.api.analyze_game_text_combined(
                text=text,
                game_id=game.id,
                existing_game=game,
                detailed_patterns=self.verbose,
                exclude_existing=exclude_existing
            )
        else:
            result = self.api.force_analyze_game_text(
                text=text,
                game_id=game.id,
                analyze_keywords=self.keywords,
                existing_game=game,
                detailed_patterns=self.verbose,
                exclude_existing=exclude_existing
            )

        self.timer_stop()
        return result

    def _handle_analysis_results(self, game, result, force_process, exclude_existing):
        """Обрабатывает результаты анализа игры"""
        self.timer_start(f"Обработка результатов игры {game.id}")

        if self.debug:
            import sys
            sys.stderr.write(f"\n=== ОТЛАДКА: _handle_analysis_results для игры {game.id} ===\n")
            sys.stderr.write(f"self.update_game = {self.update_game}\n")
            sys.stderr.flush()

        if self.progress_bar:
            self.progress_bar.update(1)

        if isinstance(result['results'], list):
            if self.keywords:
                result['results'] = {'keywords': {'items': result['results'], 'count': len(result['results'])}}
            else:
                result['results'] = {}
            if self.debug:
                import sys
                sys.stderr.write(f"Преобразовали result['results'] в: {result['results']}\n")
                sys.stderr.flush()

        self.timer_start("Расчет новых элементов")
        has_new_elements, new_elements_count = self._calculate_new_elements(game, result)
        self.timer_stop()

        if self.debug:
            import sys
            sys.stderr.write(f"has_new_elements = {has_new_elements}\n")
            sys.stderr.write(f"new_elements_count = {new_elements_count}\n")
            sys.stderr.flush()

        self.timer_start("Обновление статистики")
        self._update_statistics_after_analysis(game, result, has_new_elements, new_elements_count)
        self.timer_stop()

        # Вывод результатов в файл
        self.timer_start(f"Вывод результатов в файл")
        self.output_formatter.print_game_in_batch(
            game=game,
            index=self.stats['processed'],
            result=result,
            stats=self.stats,
            only_found=self.only_found,
            verbose=self.verbose,
            keywords=self.keywords,
            ignore_existing=self.ignore_existing,
            update_game=self.update_game,
            comprehensive_mode=False,
            combined_mode=False,
            exclude_existing=exclude_existing
        )
        self.timer_stop()

        if self.update_game:
            if self.debug:
                import sys
                sys.stderr.write(f"Вызываем _add_to_batch_if_needed для игры {game.id}\n")
                sys.stderr.flush()
            self.timer_start(f"Добавление в батч")
            added = self._add_to_batch_if_needed(game, result, has_new_elements)
            self.timer_stop()
            if self.debug:
                sys.stderr.write(f"_add_to_batch_if_needed вернул: {added}\n")
                sys.stderr.flush()

        self.timer_start("Добавление в StateManager")
        self.state_manager.add_processed_game(game.id)
        self.timer_stop()

        self.timer_start("Обновление статистики пропусков")
        total_skipped_now = self.stats['skipped_no_text'] + self.stats.get('skipped_short_text', 0)
        self.stats['skipped_total'] = total_skipped_now
        self.timer_stop()

        self._update_progress_bar_with_stats()

        if self.debug:
            import sys
            sys.stderr.write("=== КОНЕЦ ОТЛАДКИ _handle_analysis_results ===\n\n")
            sys.stderr.flush()

        self.timer_stop()  # Обработка результатов игры {game.id}

    def _calculate_new_elements(self, game, result):
        """Определяет, есть ли новые элементы для добавления и их количество"""
        self.timer_start(f"Расчет новых элементов для игры {game.id}")

        has_new_elements = False
        new_elements_count = 0

        if self.keywords:
            self.timer_start("Получение данных keywords")
            keywords_data = result['results'].get('keywords', {})
            items = keywords_data.get('items', [])
            count = keywords_data.get('count', 0)
            self.timer_stop()

            if items and count > 0:
                self.timer_start("Проверка существующих keywords у игры")
                from games.models import Keyword
                keyword_ids = [k['id'] for k in items]
                existing_game_ids = set(game.keywords.values_list('id', flat=True))
                new_ids = [kid for kid in keyword_ids if kid not in existing_game_ids]
                self.timer_stop()

                if new_ids:
                    has_new_elements = True
                    new_elements_count = len(new_ids)
                else:
                    has_new_elements = False
                    new_elements_count = 0
            else:
                has_new_elements = False
                new_elements_count = 0

        else:
            total_found_elements = 0

            for key, data in result['results'].items():
                items = data.get('items', [])
                count = data.get('count', 0)

                if items and count > 0:
                    self.timer_start(f"Проверка существующих {key} у игры")
                    existing_ids = set()
                    if key == 'genres':
                        existing_ids = set(game.genres.values_list('id', flat=True))
                    elif key == 'themes':
                        existing_ids = set(game.themes.values_list('id', flat=True))
                    elif key == 'perspectives':
                        existing_ids = set(game.player_perspectives.values_list('id', flat=True))
                    elif key == 'game_modes':
                        existing_ids = set(game.game_modes.values_list('id', flat=True))

                    item_ids = [i['id'] for i in items]
                    new_ids = [iid for iid in item_ids if iid not in existing_ids]
                    self.timer_stop()

                    if new_ids:
                        has_new_elements = True
                        total_found_elements += count
                        new_elements_count += len(new_ids)

        self.timer_stop()  # Расчет новых элементов для игры {game.id}
        return has_new_elements, new_elements_count

    def _update_statistics_after_analysis(self, game, result, has_new_elements, new_elements_count):
        """Обновляет статистику после анализа игры"""
        if self.keywords:
            keywords_data = result['results'].get('keywords', {})
            items = keywords_data.get('items', [])
            count = keywords_data.get('count', 0)

            if items and count > 0:
                from games.models import Keyword
                keyword_ids = [k['id'] for k in items]
                existing_game_ids = set(game.keywords.values_list('id', flat=True))
                new_ids = [kid for kid in keyword_ids if kid not in existing_game_ids]

                if new_ids and has_new_elements:
                    self.stats['found_games'] += 1
                    self.stats['found_elements'] += new_elements_count
                    self.stats['keywords_found'] += 1
                    self.stats['keywords_count'] += new_elements_count
                elif not new_ids:
                    self.stats['empty_games'] += 1
                    self.stats['keywords_not_found'] += 1
                else:
                    self.stats['empty_games'] += 1
                    self.stats['keywords_not_found'] += 1
            else:
                self.stats['empty_games'] += 1
                self.stats['keywords_not_found'] += 1
        else:
            total_found_elements = 0
            for key, data in result['results'].items():
                count = data.get('count', 0)
                if count > 0:
                    total_found_elements += count

            if has_new_elements:
                self.stats['found_games'] += 1
                self.stats['found_elements'] += new_elements_count
                self.stats['found_count'] += 1
                self.stats['total_criteria_found'] += new_elements_count
            elif total_found_elements > 0:
                self.stats['empty_games'] += 1
                self.stats['not_found_count'] += 1
            elif not result['has_results']:
                self.stats['empty_games'] += 1
                self.stats['not_found_count'] += 1

        if result['has_results']:
            found_count = result['summary'].get('found_count', 0)
            if not self.keywords:
                self.stats['found_count'] += 1
                self.stats['total_criteria_found'] += found_count

            self._update_checked_criteria_after_analysis(result)
        elif not has_new_elements and result.get('has_results', False):
            self.stats['empty_games'] += 1
            if self.keywords:
                self.stats['keywords_not_found'] += 1
            else:
                self.stats['not_found_count'] += 1

    def _add_to_batch_if_needed(self, game, result, has_new_elements):
        """Добавляет игру в батч для обновления если нужно - ОПТИМИЗИРОВАННАЯ ВЕРСИЯ"""
        if not self.update_game:
            return 0

        try:
            if self.keywords:
                keywords_data = result.get('results', {}).get('keywords', {})
                items = keywords_data.get('items', [])

                if not items or not has_new_elements:
                    return 0

                added = self.batch_updater.add_game_for_update(
                    game_id=game.id,
                    results=result['results'],
                    is_keywords=True
                )

                if self.batch_updater:
                    if hasattr(self.batch_updater, 'games_to_update'):
                        self.stats['in_batch'] = len(self.batch_updater.games_to_update)
                    else:
                        self.stats['in_batch'] = 0

                    self._update_progress_bar_with_stats()

                    # ===== ВАЖНО: ПРОВЕРЯЕМ ПОРОГ И ОБНОВЛЯЕМ =====
                    self._check_batch_update()

                return added

            else:
                if not has_new_elements:
                    return 0

                added = self.batch_updater.add_game_for_update(
                    game_id=game.id,
                    results=result['results'],
                    is_keywords=False
                )

                if self.batch_updater:
                    if hasattr(self.batch_updater, 'games_to_update'):
                        self.stats['in_batch'] = len(self.batch_updater.games_to_update)
                    else:
                        self.stats['in_batch'] = 0

                    self._update_progress_bar_with_stats()

                    # ===== ВАЖНО: ПРОВЕРЯЕМ ПОРОГ И ОБНОВЛЯЕМ =====
                    self._check_batch_update()

                return added

        except Exception as e:
            if self.verbose:
                self.stderr.write(f"⚠️ Ошибка при добавлении в батч: {e}")
            return 0

    def _check_batch_update(self):
        """Проверяет и обновляет батч если накопилось много игр (использует batch_size)"""
        if self.update_game and self.batch_updater and not getattr(self, '_in_batch_update', False):
            if hasattr(self.batch_updater, 'games_to_update'):
                games_in_batch = len(self.batch_updater.games_to_update)
            else:
                games_in_batch = 0

            if self.debug and self.original_stdout:
                self.original_stdout.write(f"DEBUG: Проверка батча: {games_in_batch} игр\n")
                self.original_stdout.flush()

            update_threshold = min(max(self.batch_size // 2, 10), 500)

            if games_in_batch >= update_threshold:
                self._in_batch_update = True
                try:
                    self.timer_start(f"Массовое обновление БД")
                    remaining_updates = self.batch_updater.flush()
                    self.timer_stop()

                    if remaining_updates > 0:
                        self.stats['updated'] += remaining_updates
                        self.stats['updated_games'] += remaining_updates

                    self.stats['in_batch'] = 0
                    self._update_progress_bar_with_stats()

                    if self.debug and self.original_stdout:
                        self.original_stdout.write(f"DEBUG: Батч обновлен, updated={remaining_updates}, in_batch=0\n")
                        self.original_stdout.flush()
                finally:
                    self._in_batch_update = False

    def _handle_processing_error(self, game, error):
        """Обрабатывает ошибку при обработке игры"""
        self.stats['errors'] += 1

        if self.verbose and self.original_stderr:
            import traceback
            self.original_stderr.write(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА при обработке игры {game.id}:\n")
            self.original_stderr.write(f"Ошибка: {error}\n")
            traceback.print_exc(file=self.original_stderr)
            self.original_stderr.flush()

        try:
            self.state_manager.add_processed_game(game.id)
        except:
            pass

        if self.progress_bar:
            self._update_progress_bar()

    def _initialize_criteria_tracking(self):
        """Инициализирует отслеживание проверенных критериев - ОПТИМИЗИРОВАНО"""
        self.timer_start("Инициализация отслеживания критериев (общее)")

        if not self.state_manager:
            self.timer_stop()
            return set(), set()

        # ПРИНУДИТЕЛЬНАЯ ОЧИСТКА ПЕРЕД ЗАГРУЗКОЙ
        if self.force_restart:
            print(f"♻️ ПРИНУДИТЕЛЬНЫЙ ПЕРЕЗАПУСК: очищаем всё состояние", file=sys.stderr)
            # Очищаем файлы состояния через state_manager
            self.state_manager.reset_state()
            # Также очищаем кэш анализа если нужно
            if hasattr(self, 'analysis_cache_manager'):
                self.analysis_cache_manager.clear()
            # Сбрасываем checked_criteria
            checked_criteria = set()
            # Сбрасываем processed_games в state_manager
            self.state_manager.processed_games.clear()
            self.timer_stop()
            return checked_criteria, set()

        self.timer_start("Загрузка из StateManager")
        checked_criteria = self.state_manager.get_checked_criteria()
        self.timer_stop()
        print(f"⏱️ Загрузка из StateManager: {self._get_last_timer():.3f}с", file=sys.stderr)

        if self.debug and self.original_stdout:
            self.original_stdout.write(f"\nDEBUG: Загружено {len(checked_criteria)} проверенных критериев\n")
            self.original_stdout.flush()

        self.timer_start("Очистка при force-restart")
        # Этот блок теперь не нужен, т.к. обработали выше
        self.timer_stop()

        self.timer_start("Получение всех критериев из БД")
        if self.keywords:
            from games.models import Keyword
            try:
                # Получаем только ID, не загружаем все объекты
                all_criteria = set(str(id) for id in Keyword.objects.values_list('id', flat=True))
                if self.debug and self.original_stdout:
                    self.original_stdout.write(f"DEBUG: В базе {len(all_criteria)} ключевых слов\n")
                    self.original_stdout.flush()
            except Exception as e:
                print(f"⚠️ Ошибка получения ключевых слов: {e}", file=sys.stderr)
                all_criteria = set()
        else:
            from games.models import Genre, Theme, PlayerPerspective, GameMode
            all_criteria = set()
            try:
                # Получаем все ID из всех таблиц ОДНИМ SQL запросом через UNION
                from django.db import connection
                with connection.cursor() as cursor:
                    cursor.execute("""
                                   SELECT id
                                   FROM games_genre
                                   UNION ALL
                                   SELECT id
                                   FROM games_theme
                                   UNION ALL
                                   SELECT id
                                   FROM games_playerperspective
                                   UNION ALL
                                   SELECT id
                                   FROM games_gamemode
                                   """)
                    all_criteria = set(str(row[0]) for row in cursor.fetchall())

                if self.debug and self.original_stdout:
                    self.original_stdout.write(f"DEBUG: В базе {len(all_criteria)} критериев\n")
                    self.original_stdout.flush()
            except Exception as e:
                print(f"⚠️ Ошибка получения критериев: {e}", file=sys.stderr)
        self.timer_stop()
        print(f"⏱️ Получение всех критериев из БД: {self._get_last_timer():.3f}с", file=sys.stderr)

        self.timer_start("Вычисление новых критериев")
        new_criteria = all_criteria - checked_criteria
        self.timer_stop()
        print(f"⏱️ Вычисление новых критериев: {self._get_last_timer():.3f}с (новых: {len(new_criteria)})",
              file=sys.stderr)

        if self.debug and self.original_stdout:
            self.original_stdout.write(f"DEBUG: Найдено {len(new_criteria)} новых критериев\n")
            self.original_stdout.flush()

        self.timer_start("Проверка игр для обработки")
        from games.models import Game
        games_count = Game.objects.count()
        print(f"⏱️ Всего игр в БД: {games_count}", file=sys.stderr)

        games_after_offset = max(0, games_count - self.offset) if games_count else 0
        limit_remaining = self.limit if self.limit else games_after_offset
        self.timer_stop()
        print(f"⏱️ Проверка игр для обработки: {self._get_last_timer():.3f}с", file=sys.stderr)

        if new_criteria and len(new_criteria) > 0:
            print(f"🎯 Найдено {len(new_criteria)} новых критериев", file=sys.stderr)

            if limit_remaining == 0:
                print(f"ℹ️ Новых критериев: {len(new_criteria)}, но нет игр для обработки", file=sys.stderr)

                self.timer_start("Добавление критериев в проверенные")
                self.state_manager.add_checked_criteria(list(new_criteria))
                self.timer_stop()
                print(f"⏱️ Добавление критериев в проверенные: {self._get_last_timer():.3f}с", file=sys.stderr)

                try:
                    self.timer_start("Сохранение состояния")
                    processed_count = self.state_manager.get_processed_count()
                    self.state_manager.save_state(processed_count)
                    self.timer_stop()
                    print(f"⏱️ Сохранение состояния: {self._get_last_timer():.3f}с", file=sys.stderr)
                except Exception as e:
                    print(f"⚠️ Ошибка сохранения: {e}", file=sys.stderr)

                self._new_criteria_detected = False

                updated_checked_criteria = self.state_manager.get_checked_criteria()
                self.timer_stop()  # Инициализация отслеживания критериев (общее)
                print(f"⏱️ Инициализация отслеживания критериев (общее) ВСЕГО: {self._get_total_timer():.3f}с",
                      file=sys.stderr)
                return updated_checked_criteria, set()
            else:
                print(f"🎯 Будем обрабатывать {limit_remaining} игр с новыми критериями", file=sys.stderr)

                self.timer_start("Добавление новых критериев")
                self.state_manager.add_checked_criteria(list(new_criteria))
                self.timer_stop()
                print(f"⏱️ Добавление новых критериев: {self._get_last_timer():.3f}с", file=sys.stderr)

                self._new_criteria_detected = True

                try:
                    self.timer_start("Сохранение состояния с новыми критериями")
                    processed_count = self.state_manager.get_processed_count()
                    self.state_manager.save_state(processed_count)
                    self.timer_stop()
                    print(f"⏱️ Сохранение состояния с новыми критериями: {self._get_last_timer():.3f}с",
                          file=sys.stderr)
                except Exception:
                    pass

                updated_checked_criteria = self.state_manager.get_checked_criteria()
                self.timer_stop()  # Инициализация отслеживания критериев (общее)
                print(f"⏱️ Инициализация отслеживания критериев (общее) ВСЕГО: {self._get_total_timer():.3f}с",
                      file=sys.stderr)
                return updated_checked_criteria, set()

        self._new_criteria_detected = False
        self.timer_stop()  # Инициализация отслеживания критериев (общее)
        print(f"⏱️ Инициализация отслеживания критериев (общее) ВСЕГО: {self._get_total_timer():.3f}с", file=sys.stderr)
        return checked_criteria, new_criteria

    def _get_last_timer(self):
        """Возвращает время последнего замера"""
        if self.timer_results:
            return self.timer_results[-1]['elapsed']
        return 0

    def _get_total_timer(self):
        """Возвращает общее время текущего замера"""
        if self.timer_stack and self.timer_stack[-1] in self.timers:
            return time.monotonic() - self.timers[self.timer_stack[-1]]
        return 0

    def _update_checked_criteria_after_analysis(self, result: Dict[str, Any]):
        """Обновляет список проверенных критериев после анализа игры - БЕЗ СОХРАНЕНИЯ"""
        if not self.state_manager or not result.get('has_results'):
            return

        try:
            found_criteria_ids = []

            for key, data in result['results'].items():
                if data.get('count', 0) > 0:
                    for item in data.get('items', []):
                        if 'id' in item:
                            found_criteria_ids.append(str(item['id']))

            if found_criteria_ids:
                self.state_manager.add_checked_criteria(found_criteria_ids)
                # УБРАЛИ СОХРАНЕНИЕ - теперь сохраняется только в конце

        except Exception as e:
            if self.verbose:
                self.stderr.write(f"⚠️ Ошибка обновления проверенных критериев: {e}")

    def _should_skip_game_based_on_criteria(self, game_id: int, checked_criteria: Set[str]) -> bool:
        """
        Определяет, нужно ли пропускать игру на основе проверенных критериев.
        Возвращает True если ВСЕ критерии игры уже проверены.
        """
        if not checked_criteria or self.force_restart:
            return False

        try:
            from games.models import Game
            game = Game.objects.get(id=game_id)

            if self.keywords:
                return False
            else:
                should_skip = True

                existing_genres = game.genres.all()
                if existing_genres.exists():
                    genre_ids = set(str(g.id) for g in existing_genres)
                    if not genre_ids.issubset(checked_criteria):
                        should_skip = False

                existing_themes = game.themes.all()
                if existing_themes.exists():
                    theme_ids = set(str(t.id) for t in existing_themes)
                    if not theme_ids.issubset(checked_criteria):
                        should_skip = False

                existing_perspectives = game.player_perspectives.all()
                if existing_perspectives.exists():
                    perspective_ids = set(str(p.id) for p in existing_perspectives)
                    if not perspective_ids.issubset(checked_criteria):
                        should_skip = False

                existing_modes = game.game_modes.all()
                if existing_modes.exists():
                    mode_ids = set(str(m.id) for m in existing_modes)
                    if not mode_ids.issubset(checked_criteria):
                        should_skip = False

                return should_skip

        except Game.DoesNotExist:
            return False
        except Exception as e:
            if self.verbose:
                self.stderr.write(f"⚠️ Ошибка проверки критериев для игры {game_id}: {e}")
            return False

    def _clean_output_before_progress_bar(self):
        """Очищает вывод перед созданием прогресс-бара"""
        import sys

        if hasattr(sys.stderr, 'write'):
            sys.stderr.write("\n")
            sys.stderr.flush()

        if self.original_stderr:
            self.original_stderr.write("\n")
            self.original_stderr.flush()

    def _update_progress_bar(self):
        """Обновляет прогресс-бар ТОЛЬКО статистикой, не изменяя current"""
        if not self.progress_bar:
            return

        total_processed_in_this_run = (
                self.stats['processed'] +
                self.stats['skipped_no_text'] +
                self.stats.get('skipped_short_text', 0) +
                self.stats.get('skipped_cached', 0) +
                self.stats.get('skipped_by_criteria', 0)
        )

        if self.keywords:
            self.progress_bar.update_stats({
                'found_count': self.stats['keywords_found'],
                'total_criteria_found': self.stats['keywords_count'],
                'skipped_total': self.stats.get('skipped_total', 0),
                'not_found_count': self.stats.get('keywords_not_found', 0),
                'errors': self.stats['errors'],
                'updated': self.stats['updated'],
                'in_batch': len(self.batch_updater.games_to_update) if self.batch_updater and hasattr(
                    self.batch_updater, 'games_to_update') else 0,
            })
        else:
            self.progress_bar.update_stats({
                'found_count': self.stats['found_count'],
                'total_criteria_found': self.stats['total_criteria_found'],
                'skipped_total': self.stats.get('skipped_total', 0),
                'not_found_count': self.stats.get('not_found_count', 0),
                'errors': self.stats['errors'],
                'updated': self.stats['updated'],
                'in_batch': len(self.batch_updater.games_to_update) if self.batch_updater and hasattr(
                    self.batch_updater, 'games_to_update') else 0,
            })

    def _init_stats(self):
        """Инициализирует статистику для текущего запуска - полный сброс"""
        self.stats = {
            'processed': 0,
            'processed_with_text': 0,
            'found_count': 0,
            'not_found_count': 0,
            'total_criteria_found': 0,
            'skipped_no_text': 0,
            'skipped_short_text': 0,
            'skipped_by_criteria': 0,
            'games_with_new_criteria': 0,
            'errors': 0,
            'updated': 0,
            'displayed_count': 0,

            'keywords_processed': 0,
            'keywords_found': 0,
            'keywords_count': 0,
            'keywords_not_found': 0,

            'in_batch': 0,
            'found_games': 0,
            'found_elements': 0,
            'skipped_games': 0,
            'empty_games': 0,
            'error_games': 0,
            'updated_games': 0,
        }

        if hasattr(self, 'progress_bar') and self.progress_bar:
            self.progress_bar.update_stats({
                'found_count': 0,
                'total_criteria_found': 0,
                'skipped_total': 0,
                'not_found_count': 0,
                'errors': 0,
                'updated': 0,
                'in_batch': 0,
            })

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        self.program_start_time = time.monotonic()  # <-- ДОБАВИТЬ В САМОЕ НАЧАЛО
        try:
            self.timer_start("Обработка команды")

            self._store_options(options)

            self._init_components()

            if self.clear_cache:
                self.analysis_cache_manager.clear()
                if self.state_manager:
                    self.state_manager.clear_all_state()
                self.analysis_cache = {}
                self.cache_hits = 0
                self.cache_misses = 0
                if self.original_stdout:
                    self.original_stdout.write("🧹 Весь кэш и состояние очищены\n")
                    self.original_stdout.flush()

            if not self.no_progress and not self.game_id and not self.game_name and not self.description:
                try:
                    import sys
                    sys.stderr.write("\n\n")
                    sys.stderr.flush()
                except:
                    pass

            try:
                if self.output_path:
                    self._setup_file_output()

                if self.offset > 0:
                    if self.original_stdout:
                        self.original_stdout.write(f"\n📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}")
                        self.original_stdout.write(f"💾 Начинаем анализ с позиции {self.offset} в списке всех игр")
                        self.original_stdout.write("=" * 60)
                        self.original_stdout.flush()

                    if self.output_file and not self.output_file.closed:
                        self.stdout.write(f"\n📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}")
                        self.stdout.write(f"💾 Начинаем анализ с позиции {self.offset} в списке всех игр")
                        self.stdout.write("=" * 60)

                if not self.only_found:
                    self._print_options_summary()

                self._process_command()

            except KeyboardInterrupt:
                self._handle_interrupt()
            except Exception as e:
                self._handle_error(e)
            finally:
                self.timer_stop()
                self.print_timers()
                self._cleanup()

        except Exception as e:
            import traceback
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА В КОМАНДЕ: {e}"

            if hasattr(self, 'original_stderr') and self.original_stderr:
                self.original_stderr.write(f"\n{error_msg}\n")
                traceback.print_exc(file=self.original_stderr)
                self.original_stderr.flush()
            else:
                self.stderr.write(f"\n{error_msg}\n")
                traceback.print_exc()

            if hasattr(self, 'output_file') and self.output_file and not self.output_file.closed:
                try:
                    self.output_file.write(f"\n{error_msg}\n")
                    traceback.print_exc(file=self.output_file)
                    self.output_file.flush()
                except:
                    pass

            sys.exit(1)

    def _print_pattern_details(self, pattern_info: Dict[str, Any]):
        """Выводит детальную информацию о совпадениях паттернов"""
        if not pattern_info:
            return

        has_found_matches = False
        has_skipped_matches = False

        for criteria_type, matches in pattern_info.items():
            for match in matches:
                if match.get('status') == 'found':
                    has_found_matches = True
                elif match.get('status') == 'skipped' and not self.hide_skipped:
                    has_skipped_matches = True

        if not (has_found_matches or has_skipped_matches):
            return

        if has_found_matches:
            self.stdout.write("  🔍 Совпадения паттернов:")
            seen_matches = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'found':
                        match_key = (match['pattern'], match.get('matched_text', ''), criteria_type)
                        if match_key not in seen_matches:
                            seen_matches.add(match_key)
                            pattern_display = match['pattern']
                            if len(pattern_display) > 80:
                                pattern_display = pattern_display[:77] + "..."
                            self.stdout.write(
                                f"    • '{match.get('matched_text', '')}' ← {self._get_display_name_for_key(criteria_type)}: {pattern_display}")

        if has_skipped_matches and not self.hide_skipped:
            self.stdout.write("  ⏭️ Пропущенные критерии (уже существуют):")
            seen_skipped = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'skipped':
                        if match['name'] not in seen_skipped:
                            seen_skipped.add(match['name'])
                            self.stdout.write(
                                f"    • {match['name']} ({self._get_display_name_for_key(criteria_type)})")

    def _get_display_name_for_key(self, key: str) -> str:
        """Возвращает читаемое имя для типа критерия"""
        names = {
            'genres': 'Жанры',
            'themes': 'Темы',
            'perspectives': 'Перспективы',
            'game_modes': 'Режимы игры',
            'keywords': 'Ключевые слова'
        }
        return names.get(key, key)

    def _handle_batch_interrupt(self, start_time, already_processed):
        """Обрабатывает прерывание в пакетной обработке"""
        if self.update_game and self.batch_updater:
            try:
                games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                    'games_to_update') else 0
                if games_in_batch > 0:
                    remaining_updates = self.batch_updater.flush()
                    self.stats['updated'] += remaining_updates
                    self.stats['updated_games'] += remaining_updates
                    if self.verbose and self.original_stdout:
                        self.original_stdout.write(f"💾 Сохранен батч из {remaining_updates} игр перед прерыванием\n")
                        self.original_stdout.flush()
            except Exception as e:
                if self.verbose and self.original_stderr:
                    self.original_stderr.write(f"⚠️ Не удалось сохранить батч перед прерыванием: {e}\n")
                    self.original_stderr.flush()

        total_processed_in_this_run = self.stats['processed']

        if self.keywords:
            games_without_keywords = self.stats.get('keywords_not_found', 0)
            games_with_keywords = self.stats.get('keywords_found', 0)

            finalized_games = (
                    self.stats.get('skipped_no_text', 0) +
                    self.stats.get('skipped_short_text', 0) +
                    self.stats['updated_games'] +
                    games_without_keywords
            )

            if self.debug and self.original_stdout:
                self.original_stdout.write(f"\nDEBUG _handle_batch_interrupt:\n")
                self.original_stdout.write(f"  skipped_no_text: {self.stats.get('skipped_no_text', 0)}\n")
                self.original_stdout.write(f"  skipped_short_text: {self.stats.get('skipped_short_text', 0)}\n")
                self.original_stdout.write(f"  updated_games: {self.stats['updated_games']}\n")
                self.original_stdout.write(f"  keywords_not_found: {games_without_keywords}\n")
                self.original_stdout.write(f"  ИТОГО finalized_games: {finalized_games}\n")
                self.original_stdout.flush()
        else:
            finalized_games = (
                    self.stats.get('skipped_no_text', 0) +
                    self.stats.get('skipped_short_text', 0) +
                    self.stats['updated_games'] +
                    self.stats.get('not_found_count', 0)
            )

        next_offset = self.offset + finalized_games

        try:
            total_processed = already_processed + self.stats['processed']
            self.state_manager.save_state(total_processed)

            self._save_offset_to_file(next_offset)
        except Exception as e:
            if self.verbose and self.original_stderr:
                self.original_stderr.write(f"⚠️ Не удалось сохранить состояние: {e}\n")
                self.original_stderr.flush()

        if self.original_stdout:
            self.original_stdout.write("\n")
            self.original_stdout.write("⏹️ ОБРАБОТКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ\n")
            self.original_stdout.write("=" * 60 + "\n")

            self.original_stdout.write(f"📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")
            self.original_stdout.write(f"📍 ОБРАБОТАНО В ЭТОМ ЗАПУСКЕ: {finalized_games} игр\n")

            if self.keywords:
                self.original_stdout.write(f"   ↳ Игр без текста: {self.stats.get('skipped_no_text', 0)}\n")
                self.original_stdout.write(f"   ↳ Игр с коротким текстом: {self.stats.get('skipped_short_text', 0)}\n")
                self.original_stdout.write(f"   ↳ Игр сохранено в БД: {self.stats['updated_games']}\n")
                self.original_stdout.write(f"   ↳ Игр без ключевых слов: {self.stats.get('keywords_not_found', 0)}\n")
            else:
                self.original_stdout.write(f"   ↳ Игр без текста: {self.stats.get('skipped_no_text', 0)}\n")
                self.original_stdout.write(f"   ↳ Игр с коротким текстом: {self.stats.get('skipped_short_text', 0)}\n")
                self.original_stdout.write(f"   ↳ Игр сохранено в БД: {self.stats['updated_games']}\n")
                self.original_stdout.write(f"   ↳ Игр без критериев: {self.stats.get('not_found_count', 0)}\n")

            games_processed_but_not_finalized = total_processed_in_this_run - finalized_games

            if games_processed_but_not_finalized > 0:
                self.original_stdout.write(
                    f"⚠️  {games_processed_but_not_finalized} игр обработано, но не зафиксировано\n")
                self.original_stdout.write(f"   (игры с новыми критериями, которые не были сохранены в БД)\n")

            self.original_stdout.write(f"\n📍 ОФФСЕТ ДЛЯ ПРОДОЛЖЕНИЯ: {next_offset}\n")

            if next_offset == self.offset:
                self.original_stdout.write(f"ℹ️  Оффсет не изменился, так как нет зафиксированных результатов\n")

            self.original_stdout.write(f"💾 КОМАНДА ДЛЯ ПРОДОЛЖЕНИЯ:\n")

            base_command = self._get_current_command_string()
            if base_command:
                parts = base_command.split()
                filtered_parts = []
                i = 0
                while i < len(parts):
                    if parts[i] == '--offset' and i + 1 < len(parts):
                        i += 2
                        continue
                    filtered_parts.append(parts[i])
                    i += 1
                base_command = ' '.join(filtered_parts)

            self.original_stdout.write(f"   python manage.py analyze_game_criteria --offset {next_offset}")
            if base_command:
                self.original_stdout.write(f" {base_command}")
            self.original_stdout.write("\n")

            self.original_stdout.write("=" * 60 + "\n")
            self.original_stdout.flush()

        if self.output_file and not self.output_file.closed:
            try:
                self.output_file.write("\n" + "=" * 60 + "\n")
                self.output_file.write("⏹️ ОБРАБОТКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ\n")
                self.output_file.write("=" * 60 + "\n")

                self.output_file.write(f"📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")
                self.output_file.write(f"📍 ОФФСЕТ ДЛЯ ПРОДОЛЖЕНИЯ: {next_offset}\n")
                self.output_file.write(f"💾 ИСПОЛЬЗУЙТЕ: --offset {next_offset}\n")

                if self.keywords:
                    self.output_file.write(f"   ↳ Обработано игр в этом запуске: {finalized_games}\n")
                    self.output_file.write(
                        f"   ↳ Из них без ключевых слов: {self.stats.get('keywords_not_found', 0)}\n")

                self.output_file.write("=" * 60 + "\n")
                self.output_file.flush()
            except Exception as e:
                if self.verbose and self.original_stderr:
                    self.original_stderr.write(f"⚠️ Не удалось записать в файл: {e}\n")
                    self.original_stderr.flush()

        self.stats['execution_time'] = time.time() - start_time

        if self.progress_bar:
            self.progress_bar.set_enabled(False)

        self._display_interruption_statistics_with_offset(self.stats, already_processed, next_offset)

    def _analyze_all_games_prepare(self):
        """Подготовка к массовому анализу - возвращает подготовленные данные"""
        self.timer_start("Подготовка данных")
        try:
            self.timer_start("Загрузка состояния")
            already_processed = self.state_manager.load_state()
            self.timer_stop()

            if self.offset == 0 and not self.force_restart:
                saved_offset = self._load_offset_from_file()
                if saved_offset is not None and saved_offset > 0:
                    self.offset = saved_offset
                    if self.original_stdout:
                        self.original_stdout.write(f"📍 ВОССТАНОВЛЕН ОФФСЕТ ИЗ ПРЕДЫДУЩЕГО ЗАПУСКА: {self.offset}\n")
                        self.original_stdout.flush()

            self.timer_start("Инициализация отслеживания критериев")
            checked_criteria, new_criteria = self._initialize_criteria_tracking()
            self.timer_stop()

            self.timer_start("Получение списка игр")
            games = Game.objects.all().order_by('id')
            total_games = games.count()
            self.timer_stop()

            if self.force_restart and self.offset == 0:
                if self.original_stdout:
                    self.original_stdout.write(f"♻️ Принудительный перезапуск: начинаем с первой игры\n")
                    self.original_stdout.flush()

            if self.offset > 0:
                if self.original_stdout:
                    self.original_stdout.write(f"\n📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")
                    self.original_stdout.write(f"📍 Пропускаем первые {self.offset} игр по порядку ID")
                    self.original_stdout.flush()

                if self.output_file and not self.output_file.closed:
                    self.stdout.write(f"\n📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")
                    self.stdout.write(f"📍 Пропускаем первые {self.offset} игр по порядку ID")

            self.timer_start("Применение оффсета и лимита")
            if self.offset:
                games = games[self.offset:]

            if self.limit:
                games = games[:self.limit]

            games_to_process = games.count()
            self.timer_stop()

            self.timer_start("Расчет параметров обработки")
            estimated_new_games, should_process_all = self._calculate_processing_parameters(
                games_to_process, already_processed, new_criteria
            )
            self.timer_stop()

            if not self._should_continue_processing(estimated_new_games, new_criteria):
                self.timer_stop()  # Подготовка данных
                return None

            self.timer_stop()  # Подготовка данных
            return {
                'games': games,
                'total_games': total_games,
                'games_to_process': games_to_process,
                'estimated_new_games': estimated_new_games,
                'already_processed': already_processed,
                'checked_criteria': checked_criteria,
                'new_criteria': new_criteria,
                'should_process_all': should_process_all,
            }

        except Exception as e:
            self.timer_stop()  # Подготовка данных в случае ошибки
            self.stderr.write(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА В ПОДГОТОВКЕ МАССОВОГО АНАЛИЗА: {e}")
            import traceback
            traceback.print_exc(file=self.stderr._out)
            raise

    def _print_progress_bar_legend(self):
        """Выводит легенду значков прогресс-бара прямо перед ним"""
        if self.original_stdout and not self.no_progress:
            self.original_stdout.write("\n📊 ЗНАЧКИ В ПРОГРЕСС-БАРЕ:")
            self.original_stdout.write("   🎯 = игры с новыми элементами")
            self.original_stdout.write("   📈 = всего найдено элементов")
            self.original_stdout.write("   📦 = игр в батче (ожидают сохранения)\n")
            self.original_stdout.write("⏭️ = пропущено игр (нет текста/короткий текст)")
            self.original_stdout.write("   ⚪ = игр без найденных элементов")
            self.original_stdout.write("   ❌ = ошибки")
            self.original_stdout.write("   💾 = сохранено в БД")
            self.original_stdout.write("   🕒 = время: прошло < осталось\n")
            self.original_stdout.write("")  # Пустая строка перед прогресс-баром
            self.original_stdout.flush()

    def _execute_game_analysis(self, games, total_games, games_to_process, estimated_new_games,
                               already_processed, checked_criteria, new_criteria,
                               should_process_all, start_time):
        """Выполняет анализ игр с инициализацией прогресс-бара и обработкой"""
        self.timer_start("Выполнение анализа")

        # ПРЕДЗАГРУЗКА TRIE ДО НАЧАЛА ОБРАБОТКИ
        if self.keywords:
            self.timer_start("Предзагрузка Trie ключевых слов")
            if self.verbose:
                self.stdout.write("🔄 Предзагружаем Trie ключевых слов...")
            self.api.text_analyzer._ensure_trie_loaded()
            if self.verbose:
                self.stdout.write("✅ Trie загружен")
            self.timer_stop()

        # Вывод информации о начале
        self.timer_start("Вывод информации о начале")
        self._print_analysis_start_info(
            total_games, already_processed, checked_criteria,
            new_criteria, should_process_all, estimated_new_games
        )
        self.timer_stop()

        self._init_stats()

        if self.progress_bar:
            self.progress_bar.finish()
            self.progress_bar = None

        # Вывод легенды
        self.timer_start("Вывод легенды")
        self._print_progress_bar_legend()
        self.timer_stop()

        # Инициализация прогресс-бара
        if not self.no_progress and estimated_new_games > 1:
            if self.original_stderr:
                self.original_stderr.write("\n\n")
                self.original_stderr.flush()

            self.timer_start("Инициализация прогресс-бара")
            self.progress_bar = self._init_progress_bar(estimated_new_games)

            if self.progress_bar:
                if self.progress_bar.current != 0:
                    if self.debug and self.original_stdout:
                        self.original_stdout.write(
                            f"⚠️ Прогресс-бар имеет начальное значение {self.progress_bar.current}, сбрасываем до 0\n")
                        self.original_stdout.flush()
                    self.progress_bar.current = 0

                if hasattr(self.progress_bar, '_progress_bar'):
                    self.progress_bar._progress_bar.current = 0

                self.progress_bar.update_stats({
                    'found_count': 0,
                    'total_criteria_found': 0,
                    'skipped_total': 0,
                    'not_found_count': 0,
                    'errors': 0,
                    'updated': 0,
                    'in_batch': 0,
                })

                if hasattr(self.progress_bar, '_progress_bar'):
                    self.progress_bar._progress_bar._force_update()
            self.timer_stop()
        else:
            self.progress_bar = None

        try:
            # Обработка игр
            self.timer_start("Обработка игр")
            processing_stats = self._process_games_batch(
                games, should_process_all, new_criteria, checked_criteria, start_time
            )
            self.timer_stop()

            # Финальное сохранение состояния - ИСПОЛЬЗУЕМ force_save
            self.timer_start("Финальное сохранение состояния")
            total_processed_now = already_processed + self.stats['processed']
            # Принудительно сохраняем в конце, игнорируя буферизацию
            self.state_manager.force_save(total_processed_now)
            self.timer_stop()

            # Финальное обновление батча
            if self.update_game and self.batch_updater:
                games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                    'games_to_update') else 0
                if games_in_batch > 0:
                    self.timer_start("Финальное обновление батча")
                    remaining_updates = self.batch_updater.flush()
                    self.timer_stop()
                    self.stats['updated'] += remaining_updates
                    self.stats['updated_games'] += remaining_updates

            if self.progress_bar:
                self.progress_bar.finish()

            # Расчет и сохранение оффсета
            if self.keywords:
                finalized_games = (
                        self.stats.get('skipped_no_text', 0) +
                        self.stats.get('skipped_short_text', 0) +
                        self.stats['updated_games'] +
                        self.stats.get('keywords_not_found', 0)
                )
            else:
                finalized_games = (
                        self.stats.get('skipped_no_text', 0) +
                        self.stats.get('skipped_short_text', 0) +
                        self.stats['updated_games'] +
                        self.stats.get('not_found_count', 0)
                )

            next_offset = self.offset + finalized_games

            self.timer_start("Сохранение оффсета")
            self._save_offset_to_file(next_offset)
            self.timer_stop()

            if self.original_stdout:
                self.original_stdout.write(f"\n💾 Сохранен оффсет для продолжения: {next_offset}\n")
                self.original_stdout.write(f"📍 Используйте --offset {next_offset} для продолжения с этого места\n")
                self.original_stdout.flush()

            self.stats['execution_time'] = time.time() - start_time
            self._display_final_statistics(self.stats, already_processed, total_games)

            self.timer_stop()  # Выполнение анализа

        except KeyboardInterrupt:
            self.timer_stop()  # Выполнение анализа
            self._handle_batch_interrupt(start_time, already_processed)

    def _print_analysis_start_info(self, total_games, already_processed, checked_criteria,
                                   new_criteria, should_process_all, estimated_new_games):
        """Выводит информацию о начале анализа"""
        if (self.verbose or self.no_progress) and self.original_stdout:
            mode = "ключевых слов" if self.keywords else "критериев"
            self.original_stdout.write(f"\n🔍 Анализируем {estimated_new_games} игр на наличие {mode}...\n")
            self.original_stdout.write(f"📊 Всего игр в базе: {total_games}\n")

            if should_process_all and new_criteria:
                self.original_stdout.write(f"🎯 Причина: обнаружено {len(new_criteria)} новых критериев\n")
                self.original_stdout.write(f"🎯 Будут проверены ВСЕ игры (включая уже обработанные)\n")

            if already_processed > 0 and not self.force_restart and not should_process_all:
                self.original_stdout.write(f"📊 Уже обработано: {already_processed}\n")
                self.original_stdout.write(f"📊 Игр после оффсета: {estimated_new_games}\n")

            if checked_criteria:
                self.original_stdout.write(f"📊 Проверенных критериев: {len(checked_criteria)}\n")
            if new_criteria:
                self.original_stdout.write(f"🎯 Новых критериев: {len(new_criteria)}\n")

            if not self.no_progress and estimated_new_games > 1:
                self.original_stdout.write("📊 Прогресс:\n")
            self.original_stdout.flush()

        if self.output_file and not self.output_file.closed:
            self.output_file.write("\n" + "=" * 60 + "\n")
            self.output_file.write(f"🔍 АНАЛИЗ ИГР (всего в базе: {total_games})\n")
            self.output_file.write("=" * 60 + "\n")

            if should_process_all and new_criteria:
                self.output_file.write(f"🎯 ОБНАРУЖЕНО {len(new_criteria)} НОВЫХ КРИТЕРИЕВ\n")
                self.output_file.write(f"🎯 ПРОВЕРЯЕМ ВСЕ ИГРЫ (включая уже обработанные)\n")
                self.output_file.write("=" * 60 + "\n")

            self.output_file.write(f"📊 Будут обработаны: {estimated_new_games} игр\n")
            if already_processed > 0 and not self.force_restart and not should_process_all:
                self.output_file.write(f"📊 Уже обработано ранее: {already_processed}\n")
                self.output_file.write(f"📊 Игр после оффсета: {estimated_new_games}\n")
            if checked_criteria:
                self.output_file.write(f"📊 Проверенных критериев: {len(checked_criteria)}\n")
            if new_criteria:
                self.output_file.write(f"🎯 Новых критериев: {len(new_criteria)}\n")
            self.output_file.write("=" * 60 + "\n")
            self.output_file.write("\n")
            self.output_file.flush()

    def _calculate_processing_parameters(self, games_to_process, already_processed, new_criteria):
        """Рассчитывает параметры обработки"""
        if (new_criteria and len(new_criteria) > 0) or self.force_restart:
            estimated_new_games = games_to_process
            should_process_all = True
        else:
            if self.force_restart:
                estimated_new_games = games_to_process
                should_process_all = True
            else:
                estimated_new_games = games_to_process
                should_process_all = False

        return estimated_new_games, should_process_all

    def _should_continue_processing(self, estimated_new_games, new_criteria):
        """Проверяет, нужно ли продолжать обработку"""
        if self.force_restart:
            return True

        if new_criteria and len(new_criteria) > 0:
            return True

        if estimated_new_games == 0:
            if self.original_stdout:
                if self.offset > 0:
                    self.original_stdout.write(f"✅ Все игры после оффсета {self.offset} уже обработаны\n")
                else:
                    self.original_stdout.write("✅ Нет новых игр для обработки\n")
                self.original_stdout.flush()
            if self.output_file:
                if self.offset > 0:
                    self.stdout.write(f"✅ Все игры после оффсета {self.offset} уже обработаны")
                else:
                    self.stdout.write("✅ Нет новых игр для обработки")
            return False

        return True

    def _initialize_progress_bar_for_batch(self):
        """Инициализирует прогресс-бар для обработки батча"""
        if self.progress_bar:
            self.progress_bar.current = 0

            if hasattr(self.progress_bar, '_progress_bar'):
                self.progress_bar._progress_bar.current = 0

            self.progress_bar.update_stats({
                'found_count': 0,
                'total_criteria_found': 0,
                'skipped_total': 0,
                'not_found_count': 0,
                'errors': 0,
                'updated': 0,
                'in_batch': 0,
            })

            if hasattr(self.progress_bar, '_progress_bar'):
                self.progress_bar._progress_bar._force_update()

    def _print_final_debug_statistics(self):
        """Выводит финальную отладочную статистику"""
        self.original_stdout.write(f"\nDEBUG: Финальная статистика после батча:\n")
        important_stats = {
            'processed': self.stats.get('processed', 0),
            'found_games': self.stats.get('found_games', 0),
            'found_elements': self.stats.get('found_elements', 0),
            'skipped_games': self.stats.get('skipped_games', 0),
            'empty_games': self.stats.get('empty_games', 0),
            'error_games': self.stats.get('error_games', 0),
            'updated_games': self.stats.get('updated_games', 0),
            'in_batch': self.stats.get('in_batch', 0),
        }
        for key, value in important_stats.items():
            self.original_stdout.write(f"  {key}: {value}\n")
        self.original_stdout.flush()

    def _print_batch_processing_stats(self, processed_in_this_run, skipped_because_already_processed):
        """Выводит статистику обработки батча"""
        if self.verbose and self.original_stdout:
            if processed_in_this_run > 0:
                self.original_stdout.write(f"\n📊 Обработано игр в этом запуске: {processed_in_this_run}\n")

            if skipped_because_already_processed > 0:
                self.original_stdout.write(f"📊 Пропущено уже обработанных игр: {skipped_because_already_processed}\n")

            self.original_stdout.flush()

    def _process_single_game_with_strategy(self, game, should_process_all, new_criteria, checked_criteria,
                                           processed_in_this_run, skipped_because_already_processed,
                                           skipped_because_criteria_checked, processed_previously_processed_games):
        """Обрабатывает одну игру с учетом стратегии обработки"""
        skip_by_criteria_enabled = not self.keywords

        if self.force_restart:
            skip_by_criteria_enabled = False

        game_was_processed_before = not self.force_restart and self.state_manager.is_game_processed(game.id)

        if should_process_all or self.force_restart:
            if game_was_processed_before and not self.force_restart:
                processed_previously_processed_games += 1

            self.timer_start(f"Обработка игры {game.id}")
            self._process_single_game_in_batch_with_criteria(game, checked_criteria,
                                                             should_process_all or self.force_restart)
            self.timer_stop()
            processed_in_this_run += 1

            if self.progress_bar:
                self._update_progress_bar_with_stats()

        elif not should_process_all:
            if game_was_processed_before:
                skipped_because_already_processed += 1
                self.state_manager.add_processed_game(game.id)

                if self.progress_bar:
                    self.progress_bar.update(1)
                    self._update_progress_bar_with_stats()

                return {
                    'processed_in_this_run': processed_in_this_run,
                    'skipped_because_already_processed': skipped_because_already_processed,
                    'skipped_because_criteria_checked': skipped_because_criteria_checked,
                    'processed_previously_processed_games': processed_previously_processed_games,
                }

            if skip_by_criteria_enabled and checked_criteria and self._should_skip_game_based_on_criteria(game.id,
                                                                                                          checked_criteria):
                skipped_because_criteria_checked += 1
                self.stats['skipped_by_criteria'] += 1
                self.stats['skipped_total'] += 1
                self.state_manager.add_processed_game(game.id)

                if self.progress_bar:
                    self.progress_bar.update(1)
                    self._update_progress_bar_with_stats()

                return {
                    'processed_in_this_run': processed_in_this_run,
                    'skipped_because_already_processed': skipped_because_already_processed,
                    'skipped_because_criteria_checked': skipped_because_criteria_checked,
                    'processed_previously_processed_games': processed_previously_processed_games,
                }

            self.timer_start(f"Обработка игры {game.id}")
            self._process_single_game_in_batch_with_criteria(game, checked_criteria, False)
            self.timer_stop()
            processed_in_this_run += 1

            if self.progress_bar:
                self._update_progress_bar_with_stats()

        return {
            'processed_in_this_run': processed_in_this_run,
            'skipped_because_already_processed': skipped_because_already_processed,
            'skipped_because_criteria_checked': skipped_because_criteria_checked,
            'processed_previously_processed_games': processed_previously_processed_games,
        }

    def _check_and_update_batch(self):
        """Проверяет и обновляет батч если накопилось много игр"""
        if self.update_game and self.batch_updater and not getattr(self, '_in_batch_update', False):
            games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                'games_to_update') else 0

            update_threshold = min(max(self.batch_size // 2, 10), 500)

            if games_in_batch >= update_threshold:
                self._in_batch_update = True
                try:
                    self.timer_start("Массовое обновление БД")
                    real_updated = self.batch_updater.flush()
                    self.timer_stop()

                    if real_updated > 0:
                        self.stats['updated'] += real_updated
                        self.stats['updated_games'] += real_updated

                        if self.verbose and self.original_stdout:
                            self.original_stdout.write(
                                f"💾 Реально обновлено {real_updated} игр (порог: {update_threshold})\n")
                            self.original_stdout.flush()

                    self._update_progress_bar_with_stats()

                except SystemExit:
                    raise
                except Exception as e:
                    if self.original_stderr:
                        self.original_stderr.write(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА при обновлении батча: {e}\n")
                        import traceback
                        traceback.print_exc(file=self.original_stderr)
                    import sys
                    sys.exit(1)
                finally:
                    self._in_batch_update = False

    def _flush_remaining_batch(self):
        """Обновляет оставшийся батч после завершения цикла"""
        if not self.update_game or not self.batch_updater or getattr(self, '_in_batch_update', False):
            return

        try:
            games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                'games_to_update') else 0

            if games_in_batch > 0:
                self._in_batch_update = True
                try:
                    self.timer_start("Финальное массовое обновление БД")
                    real_updated = self.batch_updater.flush()
                    self.timer_stop()

                    if real_updated > 0:
                        self.stats['updated'] += real_updated
                        self.stats['updated_games'] += real_updated

                    self.stats['in_batch'] = 0

                    if self.progress_bar:
                        self.progress_bar.update_stats({
                            'updated': self.stats['updated_games'],
                            'in_batch': 0
                        })

                    if self.verbose and self.original_stdout:
                        self.original_stdout.write(f"💾 Финальное обновление: сохранено {real_updated} игр\n")
                        self.original_stdout.flush()

                finally:
                    self._in_batch_update = False
        except Exception as e:
            self._in_batch_update = False
            if self.verbose and self.original_stderr:
                self.original_stderr.write(f"\n❌ Ошибка финального обновления батча: {e}\n")
                self.original_stderr.flush()

    def _print_startup_info(self, total_games, already_processed, checked_criteria, new_criteria,
                            should_process_all, estimated_new_games):
        """Выводит информацию о начале анализа"""
        if (self.verbose or self.no_progress) and self.original_stdout:
            mode = "ключевых слов" if self.keywords else "критериев"
            self.original_stdout.write(f"\n🔍 Анализируем {estimated_new_games} игр на наличие {mode}...\n")
            self.original_stdout.write(f"📊 Всего игр в базе: {total_games}\n")

            if should_process_all and new_criteria:
                self.original_stdout.write(f"🎯 Причина: обнаружено {len(new_criteria)} новых критериев\n")
                self.original_stdout.write(f"🎯 Будут проверены ВСЕ игры (включая уже обработанные)\n")

            if already_processed > 0 and not self.force_restart and not should_process_all:
                self.original_stdout.write(f"📊 Уже обработано: {already_processed}\n")
                self.original_stdout.write(f"📊 Осталось обработать: {estimated_new_games}\n")

            if checked_criteria:
                self.original_stdout.write(f"📊 Проверенных критериев: {len(checked_criteria)}\n")
            if new_criteria:
                self.original_stdout.write(f"🎯 Новых критериев: {len(new_criteria)}\n")

            if not self.no_progress and estimated_new_games > 1:
                self.original_stdout.write("📊 Прогресс:\n")
            self.original_stdout.flush()

        if self.output_file and not self.output_file.closed:
            self.output_file.write("\n" + "=" * 60 + "\n")
            self.output_file.write(f"🔍 АНАЛИЗ ИГР (всего в базе: {total_games})\n")
            self.output_file.write("=" * 60 + "\n")

            if should_process_all and new_criteria:
                self.output_file.write(f"🎯 ОБНАРУЖЕНО {len(new_criteria)} НОВЫХ КРИТЕРИЕВ\n")
                self.output_file.write(f"🎯 ПРОВЕРЯЕМ ВСЕ ИГРЫ (включая уже обработанные)\n")
                self.output_file.write("=" * 60 + "\n")

            self.output_file.write(f"📊 Будут обработаны: {estimated_new_games} игр\n")
            if already_processed > 0 and not self.force_restart and not should_process_all:
                self.output_file.write(f"📊 Уже обработано ранее: {already_processed}\n")
                self.output_file.write(f"📊 Осталось обработать: {estimated_new_games}\n")
            if checked_criteria:
                self.output_file.write(f"📊 Проверенных критериев: {len(checked_criteria)}\n")
            if new_criteria:
                self.output_file.write(f"🎯 Новых критериев: {len(new_criteria)}\n")
            self.output_file.write("=" * 60 + "\n")
            self.output_file.write("\n")
            self.output_file.flush()

    def _print_to_terminal(self, message: str, end: str = "\n"):
        """Печатает только в терминал (не используется при прогресс-баре)"""
        if not self.original_stdout or (
                not self.no_progress and not self.game_id and not self.game_name and not self.description):
            return

        if self.no_progress or self.game_id or self.game_name or self.description:
            self.original_stdout.write(message + end)
            self.original_stdout.flush()

    def _print_to_file(self, message: str, end: str = "\n"):
        """Печатает только в файл"""
        if self.output_file and not self.output_file.closed:
            self.output_file.write(message + end)
            self.output_file.flush()

    def _print_both(self, message: str, end: str = "\n"):
        """Печатает и в терминал и в файл"""
        if self.no_progress or self.game_id or self.game_name or self.description:
            self._print_to_terminal(message, end)
        self._print_to_file(message, end)

    def _store_options(self, options):
        """Сохраняет опции"""
        self.game_id = options.get('game_id')
        self.game_name = options.get('game_name')
        self.description = options.get('description')
        self.limit = options.get('limit')
        self.offset = options.get('offset', 0)
        self.update_game = options.get('update_game', False)
        self.min_text_length = options.get('min_text_length', 10)
        self.verbose = options.get('verbose', False)
        self.debug = options.get('debug', False)
        self.only_found = options.get('only_found', False)
        self.batch_size = options.get('batch_size', 1000)
        self.ignore_existing = options.get('ignore_existing', False)
        self.hide_skipped = options.get('hide_skipped', False)
        self.no_progress = options.get('no_progress', False)
        self.force_restart = options.get('force_restart', False)
        self.keywords = options.get('keywords', False)
        self.clear_cache = options.get('clear_cache', False)
        self.output_path = options.get('output')
        self.exclude_existing = options.get('exclude_existing', False)

        self.use_wiki = options.get('use_wiki', False)
        self.use_rawg = options.get('use_rawg', False)
        self.use_storyline = options.get('use_storyline', False)
        self.prefer_wiki = options.get('prefer_wiki', False)
        self.prefer_storyline = options.get('prefer_storyline', False)
        self.combine_texts = options.get('combine_texts', False)
        self.combine_all_texts = options.get('combine_all_texts', False)

        if hasattr(self, 'api') and self.api:
            self.api.force_restart = self.force_restart

    def _init_components(self):
        """Инициализирует компоненты"""
        self.stdout.write("🔧 Инициализируем компоненты команды...")

        self.timer_start("Инициализация")

        try:
            self.timer_start("Загрузка GameAnalyzerAPI")
            self.stdout.write("   🔧 Загружаем GameAnalyzerAPI...")
            from games.analyze import GameAnalyzerAPI

            api_verbose = False
            self.api = GameAnalyzerAPI(verbose=api_verbose)

            self.api.debug = self.debug

            self.stdout.write("   ✅ GameAnalyzerAPI инициализирован")
            self.timer_stop()

            if self.clear_cache:
                self.timer_start("Очистка кеша анализатора")
                self.stdout.write("   🔧 Очищаем кеш анализатора...")
                self.api.clear_analysis_cache()
                self.stdout.write("   ✅ Кеш анализатора очищен")
                self.timer_stop()

            self.timer_start("Инициализация StateManager")
            self.stdout.write("   🔧 Инициализируем StateManager...")
            from .state_manager import StateManager
            self.state_manager = StateManager(
                output_path=self.output_path,
                keywords_mode=self.keywords,
                force_restart=self.force_restart,
                verbose=self.verbose
            )
            self.stdout.write(f"   ✅ StateManager инициализирован (файл: {self.state_manager.state_file})")
            self.timer_stop()

            self.timer_start("Инициализация BatchUpdater")
            self.stdout.write("   🔧 Инициализируем BatchUpdater...")
            from .batch_updater import BatchUpdater
            self.batch_updater = BatchUpdater(verbose=self.verbose)
            self.batch_updater.command_instance = self
            self.stdout.write("   ✅ BatchUpdater инициализирован")
            self.timer_stop()

            self.timer_start("Инициализация OutputFormatter")
            self.stdout.write("   🔧 Инициализируем OutputFormatter...")
            from .output_formatter import OutputFormatter
            self.output_formatter = OutputFormatter(self)
            self.stdout.write("   ✅ OutputFormatter инициализирован")
            self.timer_stop()

            self.timer_start("Инициализация TextPreparer")
            self.stdout.write("   🔧 Инициализируем TextPreparer...")
            from .text_preparer import TextPreparer
            self.text_preparer = TextPreparer(self)
            self.stdout.write(f"   ✅ TextPreparer инициализирован (режим: {self.text_preparer.text_source_mode})")
            self.timer_stop()

            if not self.no_progress and self.verbose:
                self.stdout.write("⚠️  ВНИМАНИЕ: С включенным прогресс-баром подробный вывод будет ограничен")
                self.stdout.write("⚠️  Используйте --no-progress для полного verbose вывода")
            elif not self.no_progress and not self.verbose:
                self.stdout.write("ℹ️  Прогресс-бар включен. Для подробного вывода используйте --verbose")

            self.stdout.write("✅ Все компоненты успешно инициализированы")

            self.timer_stop()  # Инициализация

        except ImportError as e:
            self.stderr.write(f"❌ Ошибка импорта: {e}")
            self.stderr.write("   Проверьте наличие файлов в папках:")
            self.stderr.write("   - games/analyze/")
            self.stderr.write("   - games/management/commands/analyzer/")
            import traceback
            traceback.print_exc(file=self.stderr._out)
            raise

        except Exception as e:
            self.stderr.write(f"❌ Ошибка инициализации компонентов: {e}")
            import traceback
            traceback.print_exc(file=self.stderr._out)
            raise

    def _setup_file_output(self):
        """Настраивает вывод в файл"""
        if not self.output_path:
            return

        try:
            directory = os.path.dirname(self.output_path)
            if directory:
                os.makedirs(directory, exist_ok=True)

            self.original_stdout = self.stdout._out
            self.original_stderr = self.stderr._out

            self.output_file = open(self.output_path, 'w', encoding='utf-8')

            self.stdout._out = self.output_file
            self.stderr._out = self.output_file

            if self.original_stdout:
                self.original_stdout.write(f"📁 Вывод сохраняется в файл: {self.output_path}\n")
                self.original_stdout.write("=" * 60 + "\n")
                self.original_stdout.flush()

        except Exception as e:
            if self.original_stderr:
                self.original_stderr.write(f"❌ Ошибка открытия файла: {e}\n")
            else:
                import sys
                sys.stderr.write(f"❌ Ошибка открытия файла: {e}\n")

    def _print_options_summary(self):
        """Выводит сводку опций и легенду значков прогресс-бара и служебных сообщений"""
        if self.output_file:
            self.stdout.write("=" * 60)
            self.stdout.write("🎮 НАСТРОЙКИ АНАЛИЗА ИГР")
            self.stdout.write("=" * 60)

            if self.offset > 0:
                self.stdout.write(f"📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}")
                self.stdout.write(f"📍 Начинаем с позиции {self.offset} в списке всех игр")

            self.stdout.write(f"📊 Режим анализа: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
            self.stdout.write(f"🔄 Режим обновления: {'✅ ВКЛ' if self.update_game else '❌ ВЫКЛ'}")
            self.stdout.write(f"🔄 Принудительный перезапуск: {'✅ ВКЛ' if self.force_restart else '❌ ВЫКЛ'}")
            self.stdout.write(f"🔍 Игнорировать существующие: {'✅ ВКЛ' if self.ignore_existing else '❌ ВЫКЛ'}")
            self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ВКЛ' if self.hide_skipped else '❌ ВЫКЛ'}")
            self.stdout.write(f"📏 Минимальная длина текста: {self.min_text_length}")
            self.stdout.write(f"📚 Источник текста: {self.text_preparer.get_source_description()}")
            self.stdout.write(f"📦 Размер батча: {self.batch_size}")
            self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}")

            self.stdout.write("=" * 60)
            self.stdout.write("📊 ЗНАЧКИ В СООБЩЕНИЯХ ПЕРЕД ПРОГРЕСС-БАРОМ:")
            self.stdout.write("   🎯 = ОБНАРУЖЕНО N НОВЫХ КРИТЕРИЕВ - найдены новые критерии в системе")
            self.stdout.write("   ℹ️ = информационное сообщение (новые критерии добавлены к проверенным)")
            self.stdout.write("   ♻️ = принудительный перезапуск (очищены проверенные критерии и обработанные игры)")
            self.stdout.write("   📍 = информация об оффсете (с какой позиции начинаем)")
            self.stdout.write("   ⚠️ = предупреждение/ошибка")
            self.stdout.write("   ✅ = успешное завершение")
            self.stdout.write("   💾 = сохранение данных/состояния")

            self.stdout.write("=" * 60)
            self.stdout.write("📊 ЗНАЧКИ В ПРОГРЕСС-БАРЕ:")
            self.stdout.write("   🎯 = игры с новыми элементами")
            self.stdout.write("   📈 = всего найдено элементов")
            self.stdout.write("   📦 = игр в батче (ожидают сохранения)")
            self.stdout.write("   ⏭️ = пропущено игр (нет текста/короткий текст)")
            self.stdout.write("   ⚪ = игр без найденных элементов")
            self.stdout.write("   ❌ = ошибки")
            self.stdout.write("   💾 = сохранено в БД")
            self.stdout.write("   🕒 = время выполнения (прошло < осталось)")

            self.stdout.write("=" * 60)
            self.stdout.write("")

    def _process_command(self):
        """Обрабатывает команду в зависимости от аргументов"""
        self.timer_start("Обработка команды")

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("🚀 ЗАПУСК ОБРАБОТКИ КОМАНДЫ")
        self.stdout.write("=" * 60)

        self.stdout.write(f"🔍 Параметры команды:")
        self.stdout.write(f"   game_id: {self.game_id}")
        self.stdout.write(f"   game_name: {self.game_name}")
        self.stdout.write(f"   description: {self.description}")
        self.stdout.write(f"   limit: {self.limit}")
        self.stdout.write(f"   offset: {self.offset}")
        self.stdout.write(f"   update_game: {self.update_game}")
        self.stdout.write(f"   keywords: {self.keywords}")
        self.stdout.write(f"   exclude_existing: {self.exclude_existing}")

        if self.debug and self.original_stdout:
            self.original_stdout.write(f"\n🔍 DEBUG: offset={self.offset}\n")
            self.original_stdout.flush()

        if self.game_id:
            self.stdout.write(f"🔍 Выбран режим: Анализ одной игры по ID")
            self.timer_start("Анализ одной игры")
            self._analyze_single_game_by_id(self.game_id)
            self.timer_stop()
        elif self.game_name:
            self.stdout.write(f"🔍 Выбран режим: Поиск игр по названию")
            self.timer_start("Поиск и анализ по названию")
            self._analyze_games_by_name(self.game_name)
            self.timer_stop()
        elif self.description:
            self.stdout.write("🔍 Выбран режим: Анализ произвольного текста")
            self.timer_start("Анализ текста")
            self._analyze_description(self.description)
            self.timer_stop()
        else:
            self.stdout.write("🔍 Выбран режим: Массовый анализ всех игр")

            if self.debug and self.original_stdout:
                self.original_stdout.write(f"\n🔍 DEBUG: Начинаем массовый анализ...\n")
                self.original_stdout.flush()

            self.timer_start("Массовый анализ")
            prepared_data = self._analyze_all_games_prepare()

            if prepared_data:
                self._analyze_all_games_execute(prepared_data)
            self.timer_stop()

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("🏁 ОБРАБОТКА КОМАНДЫ ЗАВЕРШЕНА")
        self.stdout.write("=" * 60)

        self.timer_stop()  # Обработка команды

    def _analyze_single_game_by_id(self, game_id: int):
        """Анализирует одну игру по ID (с поддержкой батч-обновления)"""
        try:
            game = Game.objects.get(id=game_id)

            if self.verbose and game.rating:
                self.stdout.write(f"⭐ Рейтинг: {game.rating:.1f} (оценок: {game.rating_count})")

            self.output_formatter.print_game_header(game, self.keywords)

            self.timer_start(f"Получение текста")
            text = self.text_preparer.prepare_text(game)
            self.timer_stop()

            if not text:
                self.stdout.write("❌ У игры нет текста для анализа")
                self.stats['skipped_no_text'] += 1
                self.stats['skipped_games'] += 1
                self.state_manager.add_processed_game(game.id)
                return

            if len(text) < self.min_text_length:
                self.stdout.write(f"⏭️ Пропущено (текст слишком короткий: {len(text)} < {self.min_text_length})")
                self.stats['skipped_short_text'] += 1
                self.stats['skipped_games'] += 1
                self.state_manager.add_processed_game(game.id)
                return

            self.timer_start(f"API-анализ")
            result = self.api.force_analyze_game_text(
                text=text,
                game_id=game_id,
                analyze_keywords=self.keywords,
                existing_game=game,
                detailed_patterns=self.verbose,
                exclude_existing=self.exclude_existing
            )
            self.timer_stop()

            self.timer_start(f"Вывод результатов")
            self.output_formatter.print_game_results(game, result, self.keywords)
            self.timer_stop()

            self.stats['processed'] += 1
            self.stats['processed_with_text'] += 1

            if result['success']:
                if self.keywords:
                    keywords_data = result.get('results', {}).get('keywords', {})
                    items = keywords_data.get('items', [])
                    if items:
                        from games.models import Keyword
                        keyword_ids = [k['id'] for k in items]
                        existing_game_ids = set(game.keywords.values_list('id', flat=True))
                        new_ids = [kid for kid in keyword_ids if kid not in existing_game_ids]

                        if new_ids:
                            self.stats['keywords_found'] += 1
                            self.stats['keywords_count'] += len(new_ids)
                            self.stats['found_games'] += 1
                            self.stats['found_elements'] += len(new_ids)
                        else:
                            self.stats['keywords_not_found'] += 1
                            self.stats['empty_games'] += 1
                    else:
                        self.stats['keywords_not_found'] += 1
                        self.stats['empty_games'] += 1
                else:
                    if result['has_results']:
                        found_count = result['summary'].get('found_count', 0)
                        self.stats['found_count'] += 1
                        self.stats['total_criteria_found'] += found_count
                        self.stats['found_games'] += 1
                        self.stats['found_elements'] += found_count
                    else:
                        self.stats['not_found_count'] += 1
                        self.stats['empty_games'] += 1

            if self.update_game and result['has_results']:
                if not hasattr(self, 'batch_updater') or self.batch_updater is None:
                    from .batch_updater import BatchUpdater
                    self.batch_updater = BatchUpdater(verbose=self.verbose)
                    self.batch_updater.command_instance = self

                self.timer_start(f"Добавление в батч")
                added = self.batch_updater.add_game_for_update(
                    game_id=game.id,
                    results=result['results'],
                    is_keywords=self.keywords
                )
                self.timer_stop()

                if added:
                    self.stats['in_batch'] = len(self.batch_updater.games_to_update)
                    if len(self.batch_updater.games_to_update) > 0:
                        self.timer_start(f"Обновление БД")
                        remaining_updates = self.batch_updater.flush()
                        self.timer_stop()
                        if remaining_updates > 0:
                            self.stats['updated'] += remaining_updates
                            self.stats['updated_games'] += remaining_updates
                            self.stats['in_batch'] = 0
                            if self.verbose:
                                self.stdout.write(f"💾 Данные обновлены в базе")
                        else:
                            if self.verbose:
                                self.stdout.write(f"ℹ️ Нет новых элементов для добавления")

            self.state_manager.add_processed_game(game.id)

        except Game.DoesNotExist:
            self.stderr.write(f"❌ Игра с ID {game_id} не найдена")
        except Exception as e:
            self.stderr.write(f"❌ Ошибка при анализе игры {game_id}: {e}")
            import traceback
            traceback.print_exc(file=self.stderr._out)
            self.stats['errors'] += 1
            self.stats['error_games'] += 1

    def _get_base_query(self) -> QuerySet:
        """Возвращает базовый QuerySet"""
        return Game.objects.all().order_by('id')

    def _init_progress_bar(self, total_games: int):
        """Инициализирует прогресс-бар с выводом в терминал"""
        if self.no_progress or total_games <= 1:
            return None

        max_possible = max(total_games, 99999 if total_games > 99999 else total_games)
        stat_width = max(4, len(str(max_possible)))

        progress_bar = ProgressBar(
            total=total_games,
            desc="Анализ игр",
            bar_length=30,
            update_interval=0.1,
            stat_width=stat_width,
            emoji_spacing=1
        )

        progress_bar.current = 0

        if hasattr(progress_bar, '_progress_bar'):
            progress_bar._progress_bar.current = 0

            if hasattr(progress_bar._progress_bar, 'stats'):
                progress_bar._progress_bar.stats = {
                    'found_count': 0,
                    'total_criteria_found': 0,
                    'skipped_total': 0,
                    'errors': 0,
                    'updated': 0,
                    'in_batch': 0,
                    'not_found_count': 0,
                }

        if hasattr(progress_bar, '_progress_bar'):
            progress_bar._progress_bar._force_update()

        return progress_bar

    def _handle_interrupt(self):
        """Обрабатывает прерывание"""
        if self.progress_bar:
            self.progress_bar.set_enabled(False)

        self._restore_output_streams()

        if self.original_stdout:
            self.original_stdout.write("\n")
            self.original_stdout.write("⏹️ Обработка прервана пользователем\n")
            self.original_stdout.flush()

    def _restore_output_streams(self):
        """Восстанавливает потоки вывода"""
        if self.output_file:
            try:
                self.output_file.close()

                if self.original_stdout:
                    self.stdout._out = self.original_stdout
                if self.original_stderr:
                    self.stderr._out = self.original_stderr

                if self.output_path and (self.no_progress or self.game_id or self.game_name or self.description):
                    self.stdout.write(f"\n✅ Результаты экспортированы в: {self.output_path}")

            except Exception as e:
                if self.original_stderr:
                    self.original_stderr.write(f"\n⚠️ Ошибка закрытия файла: {e}\n")

    def _cleanup(self):
        """Очистка ресурсов"""
        if self.output_file:
            try:
                self.output_file.close()
                if self.original_stdout:
                    self.stdout._out = self.original_stdout
                if self.original_stderr:
                    self.stderr._out = self.original_stderr
            except Exception as e:
                if self.original_stderr:
                    self.original_stderr.write(f"⚠️ Ошибка закрытия файла: {e}\n")

        if self.api:
            self.api.clear_analysis_cache()

    def _analyze_games_by_name(self, game_name: str):
        """Анализирует игры по названию (сначала точное совпадение, потом по популярности)"""
        from django.db.models import Q

        self._start_time = time.time()

        exact_matches = Game.objects.filter(name__iexact=game_name).order_by('id')

        if exact_matches.exists():
            games = exact_matches
            match_type = "точному названию"
        else:
            games = Game.objects.filter(
                Q(name__icontains=game_name)
            ).order_by(
                '-rating',
                '-rating_count',
                'id'
            )
            match_type = "частичному названию (по популярности)"

        game_count = games.count()

        if game_count == 0:
            self.stderr.write(f"❌ Игры с названием содержащим '{game_name}' не найдены")
            return

        self.stdout.write(f"🔍 Найдено {game_count} игр по {match_type}:")

        display_limit = min(5, game_count)
        for i, game in enumerate(games[:display_limit], 1):
            rating_info = f" (рейтинг: {game.rating:.1f}, оценок: {game.rating_count})" if game.rating else ""
            self.stdout.write(f"  {i}. {game.name}{rating_info}")

        if game_count > display_limit:
            self.stdout.write(f"  ... и еще {game_count - display_limit} игр")

        self.stdout.write("")

        self._init_stats()

        if self.update_game:
            from .batch_updater import BatchUpdater
            self.batch_updater = BatchUpdater(verbose=self.verbose)
            self.batch_updater.command_instance = self

        if not self.no_progress and game_count > 1:
            self.progress_bar = self._init_progress_bar(game_count)

        for i, game in enumerate(games, 1):
            if self.verbose:
                self.stdout.write(f"\n--- Игра {i}/{game_count}: {game.name} ---")

            self.timer_start(f"Обработка игры {game.id}")
            self._analyze_single_game_by_id(game.id)
            self.timer_stop()

            if self.progress_bar:
                self.progress_bar.update(1)
                self._update_progress_bar_with_stats()

        if self.update_game and self.batch_updater:
            games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                'games_to_update') else 0
            if games_in_batch > 0:
                self.timer_start("Финальное обновление батча")
                remaining_updates = self.batch_updater.flush()
                self.timer_stop()
                self.stats['updated'] += remaining_updates
                self.stats['updated_games'] += remaining_updates

        if self.progress_bar:
            self.progress_bar.finish()

        self.state_manager.save_state(self.stats['processed'])

        self.stats['execution_time'] = time.time() - self._start_time
        self._display_final_statistics(self.stats, 0, Game.objects.count())

    def _analyze_description(self, description: str):
        """Анализирует произвольный текст"""
        self.stdout.write("🔍 Анализируем произвольный текст...")

        result = self.api.analyze_game_text(
            text=description,
            analyze_keywords=self.keywords,
            detailed_patterns=self.verbose
        )

        self.output_formatter.print_text_analysis_result(result, self.keywords)

    def _display_interruption_statistics(self, stats: Dict[str, Any], already_processed: int):
        """Выводит статистику при прерывании"""
        self.stdout.write("📊 Частичная статистика (прервано):")

        total_processed = stats['processed'] + stats['skipped_no_text'] + stats.get('skipped_short_text', 0)

        if self.keywords:
            key_stats = [
                ('🔄 Обработано игр', stats['processed']),
                ('🎯 Игр с найденными ключ. словами', stats['keywords_found']),
                ('📈 Всего ключевых слов найдено', stats['keywords_count']),
                ('❌ Ошибок', stats['errors']),
                ('💾 Обновлено игр', stats['updated']),
            ]
        else:
            key_stats = [
                ('🔄 Обработано игр', stats['processed']),
                ('🎯 Игр с найденными критериями', stats['found_count']),
                ('📈 Всего критериев найдено', stats['total_criteria_found']),
                ('❌ Ошибок', stats['errors']),
                ('💾 Обновлено игр', stats['updated']),
            ]

        for display_name, value in key_stats:
            self.stdout.write(f"{display_name}: {value}")

        total_skipped = stats['skipped_no_text'] + stats.get('skipped_short_text', 0) + already_processed

        self.stdout.write(f"⏭️ Всего пропущено игр: {total_skipped}")
        self.stdout.write(f"   ↳ без текста: {stats['skipped_no_text']}")

        if 'skipped_short_text' in stats and stats['skipped_short_text'] > 0:
            self.stdout.write(f"   ↳ с коротким текстом: {stats['skipped_short_text']}")

        if already_processed > 0:
            self.stdout.write(f"   ↳ ранее обработанных: {already_processed}")

        if stats['execution_time'] > 0:
            games_per_second = stats['processed'] / stats['execution_time'] if stats['execution_time'] > 0 else 0
            self.stdout.write(f"⏱️ Время выполнения до прерывания: {stats['execution_time']:.1f} секунд")
            self.stdout.write(f"⚡ Скорость обработки: {games_per_second:.1f} игр/секунду")

            remaining_games = self.total_games_estimate - total_processed if hasattr(self,
                                                                                     'total_games_estimate') else 0
            if remaining_games > 0 and games_per_second > 0:
                remaining_time = remaining_games / games_per_second
                self.stdout.write(
                    f"⏳ Осталось обработать примерно: {remaining_time:.1f} секунд ({remaining_games} игр)")

    def _format_stat_key(self, key: str) -> str:
        """Форматирует ключ статистики для вывода"""
        formats = {
            'processed': '🔄 Обработано игр',
            'updated': '💾 Обновлено игр',
            'skipped_no_text': '⏭️ Пропущено (нет текста)',
            'errors': '❌ Ошибок',
            'found_count': '🎯 Игр с найденными критериями',
            'total_criteria_found': '📈 Всего критериев найдено',
            'displayed_count': '👁️ Показано игр',
            'keywords_processed': '🔄 Обработано игр (ключ. слова)',
            'keywords_found': '🎯 Игр с найденными ключ. словами',
            'keywords_count': '📈 Всего ключевых слов найдено',
        }
        return formats.get(key, key.capitalize())