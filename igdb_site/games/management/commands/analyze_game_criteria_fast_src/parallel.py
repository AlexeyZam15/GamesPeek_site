# games/management/commands/analyze_game_criteria_fas_src/parallel.py

import os
import sys
import time
import signal
import threading
from queue import Queue, Empty
from typing import Dict, Any, List

try:
    import ctypes
except Exception:
    ctypes = None


def _detect_console_encoding() -> str:
    """
    Определяет кодировку консоли (fallback для os.write).
    Windows: GetConsoleOutputCP() -> 'cp866' / 'utf-8' / 'cp1251' / ...
    Fallback: sys.stderr.encoding -> 'utf-8'.
    """
    if ctypes is not None and hasattr(ctypes, 'windll'):
        try:
            cp = ctypes.windll.kernel32.GetConsoleOutputCP()
            if cp == 65001:
                return 'utf-8'
            if cp:
                return f'cp{cp}'
        except Exception:
            pass
    enc = getattr(sys.stderr, 'encoding', None)
    if enc:
        return enc
    return 'utf-8'


# ASCII-замена эмодзи на случай, если поток вывода не поддерживает Unicode
_EMOJI_ASCII_FALLBACK = {
    '🚀': '[START]',
    '📊': '[PROGRESS]',
    '⚡': '[SPEED]',
    '⏳': '[ETA]',
    '🔑': '[FOUND]',
    '🆕': '[NEW]',
    '📦': '[QUEUE]',
    '🎮': '[GAMES]',
    '🧵': '[THREADS]',
    '📖': '[LEGEND]',
    '✅': '[OK]',
    '⚠️': '[WARN]',
    '⏹️': '[STOP]',
    '🎯': '[TARGET]',
    '⏭️': '[SKIP]',
    '⏱️': '[TIME]',
    '█': '#',
    '░': '-',
}


def _ascii_fallback(text: str) -> str:
    """Заменяет эмодзи/символы шкалы на ASCII-эквиваленты."""
    for emoji, repl in _EMOJI_ASCII_FALLBACK.items():
        text = text.replace(emoji, repl)
    # На всякий случай — любые оставшиеся не-ASCII заменяем на '?'
    return text.encode('ascii', errors='replace').decode('ascii')


class ParallelMixin:
    """Многопоточный анализ игр с прогресс-баром и прерыванием."""

    # ------------------------------------------------------------------ #
    #  Оформление прогресс-бара
    # ------------------------------------------------------------------ #

    def _print_progress_legend(self, total_games: int, threads: int) -> None:
        """
        Печатает легенду значков и параметры запуска перед стартом
        параллельного анализа — в стиле add_new_keywords_from_descriptions
        и fill_descriptions_from_steamdb.
        """
        lines = [
            '',
            '=' * 70,
            '🚀 ПАРАЛЛЕЛЬНЫЙ АНАЛИЗ ИГР',
            '=' * 70,
            f'🎮 Игр к обработке: {total_games:,}',
            f'🧵 Потоков: {threads}',
            '',
            '📖 ЛЕГЕНДА:',
            '  📊 прогресс      — общий прогресс обработки (шкала + %)',
            '  ⚡ скорость      — игр в секунду (сглаженная)',
            '  ⏳ осталось      — оценка времени до завершения (ETA)',
            '  🔑 найдено       — всего найдено критериев/ключевых слов',
            '  🆕 новых         — из них новых (ещё не в БД)',
            '  📦 очередь       — задач в очереди / результатов в очереди',
            '=' * 70,
            '',
        ]
        for line in lines:
            self._emit_line(line)

    def _emit_line(self, line: str, newline: bool = True) -> None:
        """
        Печатает строку в sys.stderr (как остальные команды проекта),
        чтобы эмодзи корректно отображались в консоли.
        При UnicodeEncodeError — fallback на ASCII.
        """
        text = line + ('\n' if newline else '')
        try:
            sys.stderr.write(text)
            sys.stderr.flush()
        except UnicodeEncodeError:
            try:
                sys.stderr.write(_ascii_fallback(text))
                sys.stderr.flush()
            except Exception:
                pass
        except Exception:
            # Крайний fallback: прямой os.write с кодировкой консоли
            try:
                enc = _detect_console_encoding()
                os.write(2, text.encode(enc, errors='replace'))
            except Exception:
                pass

    def _emit_progress_line(self, line: str) -> None:
        """
        Печатает строку прогресса в одну строку (\r), зачищая хвост.
        Использует sys.stderr (как остальные команды) — эмодзи работают.
        """
        padded = line + ' ' * 20
        try:
            sys.stderr.write(padded)
            sys.stderr.flush()
        except UnicodeEncodeError:
            try:
                sys.stderr.write(_ascii_fallback(padded))
                sys.stderr.flush()
            except Exception:
                pass
        except Exception:
            try:
                enc = _detect_console_encoding()
                os.write(2, padded.encode(enc, errors='replace'))
            except Exception:
                pass

    # ------------------------------------------------------------------ #
    #  Основной параллельный анализ
    # ------------------------------------------------------------------ #

    def _parallel_analysis(self, games: List[Dict], existing_relations: Dict[str, set] = None) -> Dict[str, Any]:
        """Максимально быстрый многопоточный анализ - с полной статистикой и прерыванием"""

        total_games = len(games)
        stats = {
            'total': total_games,
            'processed': 0,
            'with_results': 0,
            'with_new_results': 0,
            'total_found': 0,
            'total_new_found': 0,
            'skipped': 0,
            'errors': 0,
            'to_save': [],
            'all_results': []
        }

        task_queue = Queue()
        for game in games:
            task_queue.put(game)

        result_queue = Queue()
        stats_lock = threading.Lock()
        stop_flag = threading.Event()
        active_workers = 0
        workers_lock = threading.Lock()
        interrupted = False

        existing_sets = existing_relations if existing_relations else {
            'genres': set(), 'themes': set(), 'perspectives': set(), 'game_modes': set(),
            'keywords': set(), 'keywords_by_game': {}
        }

        def signal_handler(signum, frame):
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                stop_flag.set()

        original_handler = signal.signal(signal.SIGINT, signal_handler)

        # Легенда и параметры запуска — до старта потоков
        self._print_progress_legend(total_games, self.threads)

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
                    result = self._analyze_game_fast(game_dict, existing_sets)
                    result_queue.put(result)
                except Exception:
                    result_queue.put({
                        'id': game_dict['id'],
                        'name': game_dict['name'],
                        'has_results': False,
                        'skipped': True,
                        'reason': 'error',
                        'count': 0,
                        'found': {'genres': [], 'themes': [], 'perspectives': [], 'game_modes': [], 'keywords': []},
                        'has_new': False
                    })

            with workers_lock:
                active_workers -= 1

        threads = []
        num_threads = min(self.threads, total_games) if total_games > 0 else 0
        for _ in range(num_threads):
            t = threading.Thread(target=worker)
            t.daemon = True
            t.start()
            threads.append(t)

        processed = 0
        start_time = time.time()
        last_report = time.time()
        last_processed = 0
        last_speed_time = start_time
        smooth_speed = 0.0

        def format_time(seconds):
            if seconds < 0:
                return '—'
            if seconds < 60:
                return f"{seconds:.0f}сек"
            elif seconds < 3600:
                return f"{seconds / 60:.1f}мин"
            else:
                return f"{seconds / 3600:.1f}ч"

        def make_bar(percent: float, length: int = 24) -> str:
            """Визуальная шкала прогресса: [████████░░░░░░░░]."""
            if percent < 0:
                percent = 0
            if percent > 100:
                percent = 100
            filled = int(length * percent / 100.0)
            return '█' * filled + '░' * (length - filled)

        def build_progress_line(processed_now: int, speed: float, eta_str: str) -> str:
            """
            Формирует красивую строку прогресса в стиле остальных команд:
            📊 [████░░░░] 42.1% | 37234/88256 | ⚡ 1234 игр/с | ⏳ 1.2мин |
            🔑 269529 | 🆕 252437 | 📦 86351/20
            """
            percent = (processed_now / total_games) * 100 if total_games > 0 else 100.0
            bar = make_bar(percent)
            q_task = task_queue.qsize()
            q_res = result_queue.qsize()
            return (
                f"\r📊 [{bar}] {percent:5.1f}% | "
                f"{processed_now}/{total_games} | "
                f"⚡ {speed:.1f} игр/с | "
                f"⏳ {eta_str} | "
                f"🔑 {stats['total_found']} | "
                f"🆕 {stats['total_new_found']} | "
                f"📦 {q_task}/{q_res}"
            )

        try:
            while processed < total_games and not stop_flag.is_set():
                batch_results = []
                try:
                    result = result_queue.get(timeout=0.05)
                    batch_results.append(result)

                    while True:
                        try:
                            batch_results.append(result_queue.get_nowait())
                        except Empty:
                            break
                except Empty:
                    with workers_lock:
                        workers_alive = active_workers > 0
                    if not workers_alive and task_queue.empty():
                        break
                    # всё равно обновим прогресс, чтобы показать, что живы
                    now = time.time()
                    if now - last_report >= 1.0:
                        time_delta = now - last_speed_time
                        processed_delta = processed - last_processed
                        if time_delta > 0 and processed_delta > 0:
                            instant_speed = processed_delta / time_delta
                            smooth_speed = instant_speed if smooth_speed == 0 else (
                                    smooth_speed * 0.7 + instant_speed * 0.3)
                        eta = format_time((total_games - processed) / smooth_speed) if smooth_speed > 0 else "расчет..."
                        self._emit_progress_line(build_progress_line(processed, smooth_speed, eta))
                        last_report = now
                        last_processed = processed
                        last_speed_time = now
                    time.sleep(0.01)
                    continue

                # Обновляем счётчики под локом, но append в списки — вне лока
                local_with_results = 0
                local_with_new_results = 0
                local_total_found = 0
                local_total_new_found = 0
                local_skipped = 0
                new_to_save = []

                for result in batch_results:
                    processed += 1
                    stats['all_results'].append(result)

                    if result.get('skipped'):
                        local_skipped += 1
                    elif result.get('has_results'):
                        local_with_results += 1
                        local_total_found += result.get('count', 0)

                        if result.get('has_new'):
                            local_with_new_results += 1
                            local_total_new_found += result.get('count', 0)
                            if self.update_game:
                                new_to_save.append({
                                    'id': result['id'],
                                    'found': result.get('found', {})
                                })

                if new_to_save:
                    stats['to_save'].extend(new_to_save)

                with stats_lock:
                    stats['processed'] = processed
                    stats['with_results'] += local_with_results
                    stats['with_new_results'] += local_with_new_results
                    stats['total_found'] += local_total_found
                    stats['total_new_found'] += local_total_new_found
                    stats['skipped'] += local_skipped

                now = time.time()
                if now - last_report >= 1.0 or processed == total_games:
                    time_delta = now - last_speed_time
                    processed_delta = processed - last_processed

                    if time_delta > 0 and processed_delta > 0:
                        instant_speed = processed_delta / time_delta
                        if smooth_speed == 0:
                            smooth_speed = instant_speed
                        else:
                            smooth_speed = smooth_speed * 0.7 + instant_speed * 0.3

                    if smooth_speed > 0 and processed < total_games:
                        remaining_seconds = (total_games - processed) / smooth_speed
                        eta = format_time(remaining_seconds)
                    elif processed >= total_games:
                        eta = "0сек"
                    else:
                        eta = "расчет..."

                    self._emit_progress_line(build_progress_line(processed, smooth_speed, eta))

                    last_report = now
                    last_processed = processed
                    last_speed_time = now
                    time.sleep(0.01)

        except KeyboardInterrupt:
            if not interrupted:
                interrupted = True
                stop_flag.set()
            self._emit_line("\n⏹️ Прерывание... Ожидание завершения потоков\n")

        finally:
            signal.signal(signal.SIGINT, original_handler)

            for t in threads:
                t.join(timeout=2)

            try:
                while True:
                    result = result_queue.get_nowait()
                    stats['processed'] += 1
                    stats['all_results'].append(result)
                    if result.get('skipped'):
                        stats['skipped'] += 1
                    elif result.get('has_results'):
                        stats['with_results'] += 1
                        stats['total_found'] += result.get('count', 0)
                        if result.get('has_new'):
                            stats['with_new_results'] += 1
                            stats['total_new_found'] += result.get('count', 0)
                            if self.update_game:
                                stats['to_save'].append({
                                    'id': result['id'],
                                    'found': result.get('found', {})
                                })
            except Empty:
                pass

            elapsed = time.time() - start_time
            final_speed = stats['processed'] / elapsed if elapsed > 0 else 0

            total = total_games if total_games > 0 else 1
            percent = (stats['processed'] / total) * 100
            bar = make_bar(percent)

            if interrupted or stop_flag.is_set():
                status_suffix = " [ПРЕРВАНО]"
                eta = "—"
            elif stats['processed'] >= total_games:
                status_suffix = ""
                eta = "0сек"
            else:
                status_suffix = ""
                if final_speed > 0:
                    remaining_seconds = (total_games - stats['processed']) / final_speed
                    eta = format_time(remaining_seconds)
                else:
                    eta = "—"

            # Финальная строка прогресса (без \r — с новой строки)
            final_line = (
                f"\n📊 [{bar}] {percent:5.1f}% | "
                f"{stats['processed']}/{total_games} | "
                f"⚡ {final_speed:.1f} игр/с | "
                f"⏳ {eta} | "
                f"🔑 {stats['total_found']} | "
                f"🆕 {stats['total_new_found']}{status_suffix}"
            )
            self._emit_line(final_line)

            # Итоговая сводка — в стиле остальных команд проекта
            summary_lines = [
                '',
                '=' * 70,
                '✅ ПАРАЛЛЕЛЬНЫЙ АНАЛИЗ ЗАВЕРШЁН',
                '=' * 70,
                f"📊 Обработано: {stats['processed']:,}/{total_games:,} игр",
                f"⚡ Средняя скорость: {final_speed:.1f} игр/сек",
                f"⏱️ Время: {format_time(elapsed)}",
                f"🔑 Найдено критериев/ключевых слов: {stats['total_found']:,}",
                f"🆕 Новых: {stats['total_new_found']:,}",
                f"🎯 Игр с результатами: {stats['with_results']:,} (с новыми: {stats['with_new_results']:,})",
                f"⏭️ Пропущено: {stats['skipped']:,}",
                f"📦 К сохранению: {len(stats['to_save']):,} игр",
            ]
            if interrupted or stop_flag.is_set():
                summary_lines.append('⚠️ Анализ был ПРЕРВАН — частичные результаты сохранены')
            summary_lines.extend(['=' * 70, ''])
            for line in summary_lines:
                self._emit_line(line)

            stats['analysis_time'] = elapsed
            stats['games_per_second'] = final_speed
            stats['interrupted'] = interrupted

            return stats