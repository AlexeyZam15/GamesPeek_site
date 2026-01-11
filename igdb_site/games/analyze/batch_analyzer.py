# games/analyze/batch_analyzer.py - ОБНОВЛЕННЫЙ КЛАСС
import time
from typing import Dict, Any, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.db import connection

from games.models import Game
from .text_analyzer import TextAnalyzer
from .utils import get_game_text, update_game_criteria
from .range_cache import RangeCacheManager


class BatchAnalyzer:
    """ОПТИМИЗИРОВАННЫЙ пакетный анализатор с параллельной обработкой"""

    def __init__(self, verbose: bool = False, max_workers: int = 4):
        self.verbose = verbose
        self.max_workers = max_workers
        self.text_analyzer = TextAnalyzer(verbose=verbose)

    def analyze_games_fast(
            self,
            game_ids: Optional[List[int]] = None,
            analyze_keywords: bool = False,
            update_database: bool = False,
            text_source: str = 'default',
            limit: Optional[int] = None,
            offset: int = 0,
            use_cache: bool = True,
            batch_size: int = 50,
            verbose: Optional[bool] = None
    ) -> Dict[str, Any]:
        """
        ОПТИМИЗИРОВАННЫЙ: Анализирует несколько игр с параллельной обработкой
        """
        start_time = time.time()
        current_verbose = verbose if verbose is not None else self.verbose

        # Получаем игры
        if game_ids:
            games = Game.objects.filter(id__in=game_ids)
        else:
            games = Game.objects.all().order_by('id')

        total_games_in_db = Game.objects.count()
        total_games = games.count()

        # Фильтруем игры по кэшу если нужно
        games_to_analyze = []
        skipped_cached = 0

        if use_cache and not game_ids:
            for game in games:
                if not RangeCacheManager.is_game_checked(game.id):
                    games_to_analyze.append(game)
                else:
                    skipped_cached += 1

        else:
            games_to_analyze = list(games)

        # Применяем лимит и offset
        if offset:
            games_to_analyze = games_to_analyze[offset:]
        if limit:
            games_to_analyze = games_to_analyze[:limit]

        analyzed_games = len(games_to_analyze)

        if current_verbose:
            print(f"🔍 Анализируем {analyzed_games} игр (пропущено {skipped_cached} по кэшу)")
            if analyze_keywords:
                print("⚡ Используется оптимизированный поиск ключевых слов (Trie)")

        # Разделяем на батчи для параллельной обработки
        batches = self._create_batches(games_to_analyze, batch_size)

        # Статистика
        stats = {
            'total_games_in_database': total_games_in_db,
            'games_already_checked': skipped_cached,
            'games_selected_for_analysis': analyzed_games,
            'games_with_text': 0,
            'games_with_results': 0,
            'total_criteria_found': 0,
            'games_updated': 0,
            'errors': 0,
            'detailed_results': [],
            'cache_used': use_cache,
            'skipped_cached': skipped_cached,
            'batches_processed': 0,
            'batch_size': batch_size,
            'max_workers': self.max_workers
        }

        # Параллельная обработка батчей
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []

            for batch_idx, batch in enumerate(batches):
                future = executor.submit(
                    self._process_batch,
                    batch,
                    batch_idx,
                    analyze_keywords,
                    update_database,
                    text_source,
                    stats
                )
                futures.append(future)

            # Собираем результаты
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    # Обновляем общую статистику
                    self._update_stats_from_batch(stats, batch_results)
                    stats['batches_processed'] += 1

                    if current_verbose and stats['batches_processed'] % 10 == 0:
                        self._print_progress(stats, start_time)

                except Exception as e:
                    stats['errors'] += 1
                    if current_verbose:
                        print(f"❌ Ошибка обработки батча: {e}")

        # Финальная статистика
        stats['execution_time'] = time.time() - start_time

        if current_verbose:
            self._print_final_stats(stats)

        return {
            'success': True,
            'statistics': stats,
            'timestamp': start_time
        }

    def _create_batches(self, games: List[Game], batch_size: int) -> List[List[Game]]:
        """Создает батчи для обработки"""
        return [games[i:i + batch_size] for i in range(0, len(games), batch_size)]

    def _process_batch(
            self,
            batch: List[Game],
            batch_idx: int,
            analyze_keywords: bool,
            update_database: bool,
            text_source: str,
            stats: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Обрабатывает один батч игр"""
        batch_results = {
            'games_with_text': 0,
            'games_with_results': 0,
            'total_criteria_found': 0,
            'games_updated': 0,
            'errors': 0,
            'games': []
        }

        for game in batch:
            try:
                # Получаем текст
                text = get_game_text(game, text_source)

                if not text:
                    batch_results['games'].append({
                        'game_id': game.id,
                        'has_text': False,
                        'has_results': False
                    })
                    continue

                batch_results['games_with_text'] += 1

                # Анализируем
                analysis_result = self.text_analyzer.analyze(
                    text=text,
                    analyze_keywords=analyze_keywords,
                    existing_game=game,
                    detailed_patterns=False,
                    exclude_existing=True
                )

                game_result = {
                    'game_id': game.id,
                    'has_text': True,
                    'has_results': analysis_result['has_results'],
                    'found_count': analysis_result['summary'].get('found_count', 0),
                    'text_length': len(text)
                }

                if analysis_result['has_results']:
                    batch_results['games_with_results'] += 1
                    batch_results['total_criteria_found'] += analysis_result['summary'].get('found_count', 0)

                    # Обновляем базу если нужно
                    if update_database:
                        updated = update_game_criteria(
                            game=game,
                            results=analysis_result['results'],
                            is_keywords=analyze_keywords
                        )
                        if updated:
                            batch_results['games_updated'] += 1
                            game_result['updated'] = True

                batch_results['games'].append(game_result)

            except Exception as e:
                batch_results['errors'] += 1
                batch_results['games'].append({
                    'game_id': game.id,
                    'error': str(e)
                })

        return batch_results

    def _update_stats_from_batch(self, stats: Dict[str, Any], batch_results: Dict[str, Any]):
        """Обновляет общую статистику из результатов батча"""
        stats['games_with_text'] += batch_results['games_with_text']
        stats['games_with_results'] += batch_results['games_with_results']
        stats['total_criteria_found'] += batch_results['total_criteria_found']
        stats['games_updated'] += batch_results['games_updated']
        stats['errors'] += batch_results['errors']
        stats['detailed_results'].extend(batch_results['games'])

    def _print_progress(self, stats: Dict[str, Any], start_time: float):
        """Выводит прогресс обработки"""
        elapsed = time.time() - start_time
        processed = stats['games_with_text'] + stats['errors']
        total = stats['games_selected_for_analysis']

        if elapsed > 0 and processed > 0:
            games_per_second = processed / elapsed
            estimated_total = elapsed * total / processed if processed > 0 else 0
            remaining = max(0, estimated_total - elapsed)

            print(f"📊 Прогресс: {processed}/{total} игр "
                  f"({processed / total * 100:.1f}%) | "
                  f"⚡ {games_per_second:.1f} игр/сек | "
                  f"⏱️ Осталось: {remaining:.0f}сек")

    def _print_final_stats(self, stats: Dict[str, Any]):
        """Выводит финальную статистику"""
        print("\n" + "=" * 60)
        print("📊 ФИНАЛЬНАЯ СТАТИСТИКА")
        print("=" * 60)
        print(f"🔄 Обработано игр: {stats['games_selected_for_analysis']}")
        print(f"📝 Игр с текстом: {stats['games_with_text']}")
        print(f"🎯 Игр с результатами: {stats['games_with_results']}")
        print(f"📈 Всего найдено элементов: {stats['total_criteria_found']}")
        print(f"💾 Обновлено игр: {stats['games_updated']}")
        print(f"❌ Ошибок: {stats['errors']}")
        print(f"⏱️ Время выполнения: {stats['execution_time']:.1f} секунд")

        if stats['execution_time'] > 0:
            games_per_second = stats['games_with_text'] / stats['execution_time']
            print(f"⚡ Скорость обработки: {games_per_second:.1f} игр/секунду")
        print("=" * 60)