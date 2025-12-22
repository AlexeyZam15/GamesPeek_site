# games/management/commands/analyzer/game_processor.py
from typing import Dict, List
import time
import json
import os
import sys
from django.db.models import QuerySet
from games.models import Game
from .progress_bar import ProgressBar


class GameProcessor:
    """Класс для обработки игр в батчах"""

    def __init__(self, command_instance):
        self.command = command_instance
        self.analyzer = command_instance.analyzer
        self.progress_bar = None
        self.checkpoint_interval = 1000
        self.state_file = None
        self.processed_games = set()
        self.already_processed_count = 0
        self.estimated_games_to_process = 0

    def _process_games_loop(self, games_iterator, stats: Dict,
                            start_time: float, total_games_in_db: int) -> Dict:
        """Основной цикл обработки игр"""
        actually_processed_count = 0
        games_to_update = []

        # Устанавливаем начальное значение пропущенных ранее обработанных игр
        stats['skipped_already_processed'] = self.already_processed_count

        try:
            for index, game in enumerate(games_iterator, 1):
                # Пропускаем уже обработанные игры
                if game.id in self.processed_games:
                    continue

                # Обновляем счетчики НОВЫХ игр
                stats['processed'] += 1
                actually_processed_count += 1

                if self.command.keywords:
                    stats['keywords_processed'] = stats.get('keywords_processed', 0) + 1

                # Обрабатываем игру
                try:
                    text_to_analyze = self.command.get_text_to_analyze(game)

                    # Проверяем только на полное отсутствие текста
                    if not text_to_analyze or text_to_analyze.strip() == "":
                        stats['skipped_no_text'] += 1
                        self._handle_no_text_game(game, index, stats)
                        self.processed_games.add(game.id)
                        continue

                    # Игра имеет текст - увеличиваем счетчик
                    stats['processed_with_text'] += 1

                    # Анализируем текст
                    results, pattern_info = self.analyzer.analyze_all_patterns(
                        text_to_analyze,
                        game=game,
                        ignore_existing=self.command.ignore_existing,
                        collect_patterns=self.command.verbose,
                        keywords_mode=self.command.keywords
                    )

                    # Определяем, найдены ли критерии
                    if self.command.keywords:
                        keywords_results = results.get('keywords', [])
                        has_found_criteria = len(keywords_results) > 0
                        criteria_count = len(keywords_results)
                    else:
                        has_found_criteria = any(
                            len(results[key]) > 0 for key in ['genres', 'themes', 'perspectives', 'game_modes'] if
                            key in results
                        )
                        criteria_count = sum(
                            len(results.get(key, [])) for key in ['genres', 'themes', 'perspectives', 'game_modes']
                        )

                    # Обновляем статистику найденного/ненайденного
                    if has_found_criteria:
                        if self.command.keywords:
                            stats['keywords_found'] += 1
                            stats['keywords_count'] += criteria_count
                        else:
                            stats['found_count'] += 1
                            stats['total_criteria_found'] += criteria_count
                    else:
                        # Игра с текстом, но без критериев
                        if self.command.keywords:
                            stats['keywords_not_found'] += 1
                        else:
                            stats['not_found_count'] += 1

                    # Отображаем результаты
                    self._display_game_results(game, index, stats, results, pattern_info,
                                               has_found_criteria, criteria_count, text_to_analyze)

                    # Если есть критерии и нужно обновлять - добавляем в батч
                    if has_found_criteria and self.command.update_game:
                        games_to_update.append((game, results))

                        # Выполняем пакетное обновление каждые 100 игр (как было изначально)
                        if len(games_to_update) >= 100:
                            self._execute_batch_update(games_to_update, stats)
                            games_to_update.clear()

                    self.processed_games.add(game.id)

                except Exception as e:
                    stats['errors'] += 1
                    self._handle_game_error(game, e, stats)
                    self.processed_games.add(game.id)

                # Обновляем прогресс-бар
                self._update_progress_bar(stats, actually_processed_count)

                # Периодическое сохранение состояния файла
                if actually_processed_count % self.checkpoint_interval == 0:
                    self._save_state()

            # Обрабатываем оставшиеся игры для обновления
            if games_to_update:
                self._execute_batch_update(games_to_update, stats)

        except KeyboardInterrupt:
            # При прерывании корректно завершаем прогресс-бар
            if self.progress_bar:
                self.progress_bar.finish()
                self.progress_bar = None

            # Обрабатываем оставшиеся игры
            if games_to_update:
                self._execute_batch_update(games_to_update, stats)

            stats['processed'] = actually_processed_count
            if self.command.keywords:
                stats['keywords_processed'] = actually_processed_count
            stats['execution_time'] = time.time() - start_time

            self._save_state()
            return stats

        except Exception as e:
            # При других ошибках тоже завершаем прогресс-бар
            if self.progress_bar:
                self.progress_bar.finish()
                self.progress_bar = None

            # Обрабатываем оставшиеся игры
            if games_to_update:
                self._execute_batch_update(games_to_update, stats)

            stats['processed'] = actually_processed_count
            if self.command.keywords:
                stats['keywords_processed'] = actually_processed_count
            stats['execution_time'] = time.time() - start_time

            self._save_state()
            raise

        return stats

    def process_games_batch(self, games_queryset: QuerySet) -> Dict:
        """Обрабатывает игры батчами для экономии памяти"""
        stats = self._init_stats()

        # Настраиваем отслеживание состояния
        self._setup_state_tracking()

        # Получаем информацию о количестве игр
        total_games_in_db = games_queryset.count()
        self.estimated_games_to_process = max(0, total_games_in_db - self.already_processed_count)

        if self.estimated_games_to_process == 0:
            self._handle_all_games_processed(total_games_in_db)
            return stats

        # Инициализируем прогресс-бар
        self._init_progress_bar()

        # Обрабатываем игры
        games_iterator = games_queryset.iterator(chunk_size=self.command.batch_size)
        start_time = time.time()

        # Основная обработка
        stats = self._process_games_loop(
            games_iterator, stats, start_time, total_games_in_db
        )

        # Завершаем работу
        stats = self._finish_processing(stats, start_time)

        return stats

    def _init_stats(self) -> Dict:
        """Инициализирует статистику"""
        return {
            'processed': 0,
            'processed_with_text': 0,
            'found_count': 0,
            'not_found_count': 0,
            'total_criteria_found': 0,
            'skipped_no_text': 0,
            'skipped_already_processed': 0,
            'errors': 0,
            'updated': 0,
            'displayed_count': 0,
            'execution_time': 0,
            'keywords_processed': 0,
            'keywords_found': 0,
            'keywords_count': 0,
            'keywords_not_found': 0,
        }

    def _setup_state_tracking(self):
        """Настраивает отслеживание состояния"""
        if hasattr(self.command, 'output_path') and self.command.output_path:
            state_suffix = "_keywords" if hasattr(self.command, 'keywords') and self.command.keywords else "_criteria"
            state_path = os.path.splitext(self.command.output_path)[0] + f'_state{state_suffix}.json'
            self.state_file = state_path

            # Если force-restart, очищаем состояние
            if getattr(self.command, 'force_restart', False):
                if os.path.exists(state_path):
                    try:
                        os.remove(state_path)
                        original_out = self.command.original_stdout or sys.stdout
                        original_out.write(f"🗑️ Удален файл состояния: {state_path}\n")
                        original_out.flush()
                    except Exception as e:
                        original_out = self.command.original_stdout or sys.stdout
                        original_out.write(f"⚠️ Не удалось удалить файл состояния: {e}\n")
                        original_out.flush()
                self.processed_games = set()
                self.already_processed_count = 0
                return

            # Загружаем ранее обработанные игры
            if os.path.exists(state_path):
                try:
                    with open(state_path, 'r', encoding='utf-8') as f:
                        state_data = json.load(f)

                    saved_mode = state_data.get('mode', '')
                    current_mode = 'keywords' if hasattr(self.command,
                                                         'keywords') and self.command.keywords else 'criteria'

                    if saved_mode != current_mode:
                        original_out = self.command.original_stdout or sys.stdout
                        original_out.write(
                            f"⚠️ Обнаружено состояние от другого режима ({saved_mode}). Начинаем заново.\n")
                        original_out.flush()
                        self.processed_games = set()
                        self.already_processed_count = 0
                    else:
                        self.processed_games = set(state_data.get('processed_games', []))
                        self.already_processed_count = len(self.processed_games)
                        if self.already_processed_count > 0:
                            mode_text = "ключевых слов" if current_mode == 'keywords' else "критериев"
                            original_out = self.command.original_stdout or sys.stdout
                            original_out.write(
                                f"📖 Загружено состояние: {self.already_processed_count} ранее обработанных игр (режим: {mode_text})\n")
                            original_out.flush()

                except Exception as e:
                    original_out = self.command.original_stdout or sys.stdout
                    original_out.write(f"⚠️ Ошибка загрузки состояния: {e}\n")
                    original_out.flush()
                    self.processed_games = set()
                    self.already_processed_count = 0
            else:
                self.processed_games = set()
                self.already_processed_count = 0
        else:
            self.processed_games = set()
            self.already_processed_count = 0

    def _handle_all_games_processed(self, total_games_in_db: int):
        """Обрабатывает случай когда все игры уже обработаны"""
        original_out = self.command.original_stdout or sys.stdout
        mode = "ключевых слов" if hasattr(self.command, 'keywords') and self.command.keywords else "критериев"
        original_out.write(f"✅ Все игры ({total_games_in_db}) уже обработаны ранее (режим: {mode})\n")
        original_out.flush()

    def _init_progress_bar(self):
        """Инициализирует прогресс-бар с правильным total"""
        use_progress_bar = (
                self.estimated_games_to_process > 1 and
                not getattr(self.command, 'no_progress', False)
        )

        if use_progress_bar:
            # Автоматически рассчитываем ширину поля для статистики
            # На основе максимального возможного числа
            max_possible = max(
                self.estimated_games_to_process,  # максимальное количество обработанных
                99999 if self.estimated_games_to_process > 99999 else self.estimated_games_to_process
            )

            # Определяем ширину по количеству цифр
            stat_width = len(str(max_possible))
            # Минимальная ширина - 4 цифры
            stat_width = max(4, stat_width)

            # Добавляем 1 пробел после каждого эмодзи для читаемости
            emoji_spacing = 1

            self.progress_bar = ProgressBar(
                total=self.estimated_games_to_process,
                desc="Анализ игр",
                bar_length=30,
                update_interval=0.1,
                stat_width=stat_width,
                emoji_spacing=emoji_spacing
            )

            self.progress_bar.update_stats({
                'found_count': 0,
                'total_criteria_found': 0,
                'skipped_total': 0,
                'errors': 0,
                'updated': 0,
            })

    def _process_single_game(self, game, index: int, stats: Dict) -> tuple:
        """Обрабатывает одну игру, возвращает (has_criteria, criteria_data)"""
        text_to_analyze = self.command.get_text_to_analyze(game)

        # Проверяем только на полное отсутствие текста
        if not text_to_analyze or text_to_analyze.strip() == "":
            stats['skipped_no_text'] += 1
            self._handle_no_text_game(game, index, stats)
            return False, {}

        # Игра имеет текст - увеличиваем счетчик
        stats['processed_with_text'] += 1

        # Анализируем текст
        results, pattern_info = self.analyzer.analyze_all_patterns(
            text_to_analyze,
            game=game,
            ignore_existing=self.command.ignore_existing,
            collect_patterns=self.command.verbose,
            keywords_mode=self.command.keywords
        )

        # Определяем, найдены ли критерии
        if self.command.keywords:
            keywords_results = results.get('keywords', [])
            has_found_criteria = len(keywords_results) > 0
            criteria_count = len(keywords_results)
            criteria_data = keywords_results
        else:
            has_found_criteria = any(
                len(results[key]) > 0 for key in ['genres', 'themes', 'perspectives', 'game_modes'] if key in results
            )
            criteria_count = sum(
                len(results.get(key, [])) for key in ['genres', 'themes', 'perspectives', 'game_modes']
            )
            criteria_data = results

        # Обновляем статистику найденного/ненайденного
        if has_found_criteria:
            if self.command.keywords:
                stats['keywords_found'] += 1
                stats['keywords_count'] += criteria_count
            else:
                stats['found_count'] += 1
                stats['total_criteria_found'] += criteria_count
        else:
            # Игра с текстом, но без критериев
            if self.command.keywords:
                stats['keywords_not_found'] += 1
            else:
                stats['not_found_count'] += 1

        # Отображаем результаты (только вывод, без сохранения!)
        self._display_game_results(game, index, stats, results, pattern_info,
                                   has_found_criteria, criteria_count, text_to_analyze)

        # ВАЖНО: НЕ сохраняем в базу сразу!
        # Возвращаем только данные для последующего сохранения
        return has_found_criteria, criteria_data

    def _handle_no_text_game(self, game, index: int, stats: Dict):
        """Обрабатывает игру без текста"""
        if self.command.verbose and not self.command.only_found:
            if hasattr(self.command, 'output_file') and self.command.output_file:
                self.command.stdout.write(f"{index}. {game.name} - ⏭️ ПРОПУЩЕНО (текста вообще нет)")
                self.command.stdout.write("")

    def _display_game_results(self, game, index: int, stats: Dict, results, pattern_info,
                              has_found_criteria: bool, criteria_count: int, text_to_analyze: str):
        """Отображает результаты анализа игры"""
        # В режиме only-found пропускаем игры без найденных критериев
        if self.command.only_found and not has_found_criteria:
            return

        # Пропускаем игру если нет реально новых критериев в режиме ignore-existing + update-game
        if self.command.ignore_existing and self.command.update_game and not has_found_criteria:
            return

        # ВЫВОДИМ ДЕТАЛИ
        output_is_file = self.command.stdout._out != sys.stdout and self.command.stdout._out != sys.stderr

        if output_is_file or not self.progress_bar:
            stats['displayed_count'] += 1

            if stats['displayed_count'] > 1:
                self.command.stdout.write("")

            if self.command.verbose and not self.command.only_found:
                self.command.stdout.write(f"{index}. 🔍 Анализируем: {game.name}")
                text_source = self.command._get_text_source_for_game(game, text_to_analyze)
                text_length = len(text_to_analyze)
                self.command.stdout.write(f"   📝 Используется: {text_source} ({text_length} символов)")

            # ВЫВОДИМ РЕЗУЛЬТАТЫ
            if has_found_criteria:
                self._display_found_results(game, results, pattern_info, criteria_count)
            elif not self.command.only_found:
                mode = 'ключевые слова' if self.command.keywords else 'критерии'
                if self.command.ignore_existing:
                    mode = 'новые ключевые слова' if self.command.keywords else 'новые критерии'
                self.command.stdout.write(f"   ⚡ {mode.capitalize()} не найдены")

    def _display_found_results(self, game, results, pattern_info, criteria_count: int):
        """Отображает найденные результаты"""
        if self.command.keywords:
            display_results = {'keywords': results.get('keywords', [])}
            display_pattern_info = {'keywords': pattern_info.get('keywords', [])}
            self.command._print_game_results(game, display_results, criteria_count, display_pattern_info)
        else:
            self.command._print_game_results(game, results, criteria_count, pattern_info)

    def _execute_batch_update(self, games_to_update: list, stats: Dict):
        """Выполняет пакетное обновление игр"""
        if not self.command.update_game or not games_to_update:
            return

        updated_count = self.command.update_game_criteria(games_to_update)
        stats['updated'] += updated_count

        # Обновляем статистику в прогресс-баре
        if self.progress_bar:
            self._update_progress_bar_stats(stats)
            self.progress_bar.update(0)

        # Очищаем список чтобы не накапливать память
        games_to_update.clear()

    def _update_progress_bar(self, stats: Dict, actually_processed_count: int):
        """Обновляет прогресс-бар"""
        if self.progress_bar:
            # Обновляем статистику
            self._update_progress_bar_stats(stats)

            # Обновляем прогресс (только новые игры)
            self.progress_bar.update(1)

    def _update_progress_bar_stats(self, stats: Dict):
        """Обновляет статистику в прогресс-баре"""
        if self.progress_bar:
            # Общее количество пропущенных: без текста + с текстом но без критериев
            if self.command.keywords:
                total_skipped = stats['skipped_no_text'] + stats['keywords_not_found']
                self.progress_bar.update_stats({
                    'found_count': stats['keywords_found'],
                    'total_criteria_found': stats['keywords_count'],
                    'skipped_total': total_skipped,
                    'errors': stats['errors'],
                    'updated': stats['updated'],
                })
            else:
                total_skipped = stats['skipped_no_text'] + stats['not_found_count']
                self.progress_bar.update_stats({
                    'found_count': stats['found_count'],
                    'total_criteria_found': stats['total_criteria_found'],
                    'skipped_total': total_skipped,
                    'errors': stats['errors'],
                    'updated': stats['updated'],
                })

    def _handle_game_error(self, game, error: Exception, stats: Dict):
        """Обрабатывает ошибку при обработке игры"""
        if self.progress_bar:
            self.progress_bar.terminal_stderr.write("\r" + " " * 150 + "\r")
            self.progress_bar.terminal_stderr.flush()

        if not self.command.only_found:
            self.command.stderr.write(f"   ❌ Ошибка при анализе {game.name}: {str(error)}")

        # Обновляем статистику в прогресс-баре
        if self.progress_bar:
            self._update_progress_bar_stats(stats)
            self.progress_bar.update(0)

    def _save_state(self):
        """Сохраняет текущее состояние обработки"""
        if self.state_file:
            try:
                current_mode = 'keywords' if hasattr(self.command, 'keywords') and self.command.keywords else 'criteria'

                state_data = {
                    'processed_games': list(self.processed_games),
                    'timestamp': time.time(),
                    'total_processed': len(self.processed_games),
                    'mode': current_mode,
                    'keywords_mode': hasattr(self.command, 'keywords') and self.command.keywords
                }

                with open(self.state_file, 'w', encoding='utf-8') as f:
                    json.dump(state_data, f, ensure_ascii=False, indent=2)

            except Exception as e:
                original_out = self.command.original_stdout or sys.stdout
                original_out.write(f"⚠️ Ошибка сохранения состояния: {e}\n")
                original_out.flush()

    def _finish_processing(self, stats: Dict, start_time: float) -> Dict:
        """Завершает обработку и выводит статистику"""
        # Завершаем прогресс-бар
        self._finish_progress_bar()

        # Финальное сохранение состояния
        self._save_state()

        # Выводим статистику
        stats['execution_time'] = time.time() - start_time

        # Выводим в терминал
        self._display_terminal_stats(stats, start_time)

        # Выводим в файл
        self._display_file_stats(stats, start_time)

        return stats

    def _finish_progress_bar(self):
        """Завершает прогресс-бар"""
        if self.progress_bar:
            self.progress_bar.finish()
            self.progress_bar = None

    def _display_terminal_stats(self, stats: Dict, start_time: float):
        """Отображает статистику в терминале"""
        original_out = self.command.original_stdout or sys.stdout

        # Показываем сколько игр было пропущено (ранее обработанных)
        if self.already_processed_count > 0:
            original_out.write(f"⏭️ Пропущено {self.already_processed_count} ранее обработанных игр\n")

        # Рассчитываем общее количество пропущенных
        if self.command.keywords:
            total_skipped = stats['skipped_no_text'] + stats['keywords_not_found']
            original_out.write(f"✅ Обработано {stats['keywords_processed']} новых игр\n")
            if stats['keywords_processed'] > 0:
                original_out.write(f"🎯 Игр с найденными ключевыми словами: {stats['keywords_found']}\n")
                original_out.write(f"📈 Всего ключевых слов найдено: {stats['keywords_count']}\n")
                if stats['keywords_not_found'] > 0:
                    original_out.write(f"⚡ Игр без ключевых слов: {stats['keywords_not_found']}\n")
        else:
            total_skipped = stats['skipped_no_text'] + stats['not_found_count']
            original_out.write(f"✅ Обработано {stats['processed']} новых игр\n")
            if stats['processed'] > 0:
                original_out.write(f"🎯 Игр с найденными критериями: {stats['found_count']}\n")
                original_out.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}\n")
                if stats['not_found_count'] > 0:
                    original_out.write(f"⚡ Игр без критериев: {stats['not_found_count']}\n")

        if total_skipped > 0:
            original_out.write(f"⏭️ Всего пропущено игр: {total_skipped}\n")
            if stats['skipped_no_text'] > 0:
                original_out.write(f"   ↳ без текста: {stats['skipped_no_text']}\n")
            if self.command.keywords and stats['keywords_not_found'] > 0:
                original_out.write(f"   ↳ без ключевых слов: {stats['keywords_not_found']}\n")
            elif not self.command.keywords and stats['not_found_count'] > 0:
                original_out.write(f"   ↳ без критериев: {stats['not_found_count']}\n")

        if stats['errors'] > 0:
            original_out.write(f"❌ Ошибок: {stats['errors']}\n")

        if stats['updated'] > 0:
            original_out.write(f"💾 Обновлено игр: {stats['updated']}\n")

        exec_time = stats.get('execution_time', time.time() - start_time)
        original_out.write(f"⏱️ Время выполнения: {exec_time:.1f} секунд\n")
        original_out.flush()

    def _display_file_stats(self, stats: Dict, start_time: float):
        """Отображает статистику в файл вывода"""
        if hasattr(self.command, 'output_file') and self.command.output_file:
            try:
                self.command.stdout.write("\n" + "=" * 60 + "\n")

                mode = 'КЛЮЧЕВЫЕ СЛОВА' if hasattr(self.command, 'keywords') and self.command.keywords else 'КРИТЕРИИ'
                self.command.stdout.write(f"📊 ИТОГОВАЯ СТАТИСТИКА АНАЛИЗА ({mode})\n")
                self.command.stdout.write("=" * 60 + "\n")

                if self.already_processed_count > 0:
                    self.command.stdout.write(f"⏭️ Пропущено ранее обработанных игр: {self.already_processed_count}\n")

                if hasattr(self.command, 'keywords') and self.command.keywords:
                    self.command.stdout.write(f"🔄 Обработано новых игр: {stats['keywords_processed']}\n")
                    self.command.stdout.write(f"🎯 Игр с найденными ключ. словами: {stats['keywords_found']}\n")
                    self.command.stdout.write(f"📈 Всего ключевых слов найдено: {stats['keywords_count']}\n")
                    if stats['keywords_not_found'] > 0:
                        self.command.stdout.write(f"⚡ Игр без ключевых слов: {stats['keywords_not_found']}\n")
                else:
                    self.command.stdout.write(f"🔄 Обработано новых игр: {stats['processed']}\n")
                    self.command.stdout.write(f"🎯 Игр с найденными критериями: {stats['found_count']}\n")
                    self.command.stdout.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}\n")
                    if stats['not_found_count'] > 0:
                        self.command.stdout.write(f"⚡ Игр без критериев: {stats['not_found_count']}\n")

                total_skipped = stats['skipped_no_text'] + (
                    stats['keywords_not_found'] if self.command.keywords else stats['not_found_count'])
                self.command.stdout.write(f"⏭️ Всего пропущено игр: {total_skipped}\n")
                self.command.stdout.write(f"⏭️ Игр без текста: {stats['skipped_no_text']}\n")
                if self.command.keywords and stats['keywords_not_found'] > 0:
                    self.command.stdout.write(f"⏭️ Игр без ключевых слов: {stats['keywords_not_found']}\n")
                elif not self.command.keywords and stats['not_found_count'] > 0:
                    self.command.stdout.write(f"⏭️ Игр без критериев: {stats['not_found_count']}\n")

                self.command.stdout.write(f"❌ Ошибок: {stats['errors']}\n")
                self.command.stdout.write(f"💾 Обновлено игр: {stats['updated']}\n")

                exec_time = stats.get('execution_time', time.time() - start_time)
                self.command.stdout.write(f"⏱️ Время выполнения: {exec_time:.1f} секунд\n")

                self.command.stdout.write("=" * 60 + "\n")
                self.command.stdout.write("✅ Анализ успешно завершен\n")
                self.command.stdout.write("=" * 60 + "\n")

                self.command.output_file.flush()
            except Exception as e:
                original_out = self.command.original_stdout or sys.stdout
                original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")
                original_out.flush()