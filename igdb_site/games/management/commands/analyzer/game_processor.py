# games/analyzer/game_processor.py
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

        # Для периодического сохранения прогресса
        self.checkpoint_interval = 1000  # Каждые 1000 игр
        self.state_file = None
        self.processed_games = set()

    def process_games_batch(self, games_queryset: QuerySet) -> Dict:
        """Обрабатывает игры батчами для экономии памяти"""
        stats = {
            'processed': 0,
            'updated': 0,
            'skipped_no_text': 0,
            'errors': 0,
            'found_count': 0,
            'total_criteria_found': 0,
            'displayed_count': 0,
        }

        # Настраиваем отслеживание состояния
        self._setup_state_tracking()

        # Получаем общее количество игр в запросе
        total_games_in_db = games_queryset.count()

        # Считаем сколько игр нужно будет пропустить (уже обработанные)
        skipped_already_processed = 0

        if self.processed_games and not getattr(self.command, 'force_restart', False):
            processed_count = len(self.processed_games)

            if processed_count > 0:
                need_check_processed = True
                skipped_already_processed = processed_count
            else:
                need_check_processed = False
        else:
            need_check_processed = False

        # Ориентировочное количество игр для обработки (для прогресс-бара)
        estimated_games_to_process = max(0, total_games_in_db - skipped_already_processed)

        if estimated_games_to_process == 0:
            # Все игры уже обработаны
            original_out = self.command.original_stdout or sys.stdout
            original_out.write(f"✅ Все игры ({total_games_in_db}) уже обработаны ранее\n")
            original_out.flush()
            return stats

        use_progress_bar = (
                estimated_games_to_process > 1 and
                not getattr(self.command, 'no_progress', False)
        )

        if use_progress_bar:
            self.progress_bar = ProgressBar(
                total=estimated_games_to_process,
                desc="Анализ игр",
                bar_length=30,
                update_interval=0.1
            )

        games_iterator = games_queryset.iterator(chunk_size=self.command.batch_size)
        start_time = time.time()

        # Счетчик реально обработанных игр (для прогресс-бара)
        actually_processed_count = 0

        for index, game in enumerate(games_iterator, 1):
            # БЕЗ ВЫВОДА СООБЩЕНИЙ проверяем, не была ли игра уже обработана
            if need_check_processed and game.id in self.processed_games:
                skipped_already_processed += 1
                continue

            stats['processed'] += 1
            actually_processed_count += 1

            try:
                self._process_single_game(game, index, stats)

                # Добавляем игру в список обработанных
                self.processed_games.add(game.id)

            except Exception as e:
                stats['errors'] += 1
                if self.progress_bar:
                    # Очищаем прогресс-бар перед выводом ошибки
                    self.progress_bar.terminal_stderr.write("\r" + " " * 150 + "\r")
                    self.progress_bar.terminal_stderr.flush()

                if not self.command.only_found:
                    self.command.stderr.write(f"   ❌ Ошибка при анализе {game.name}: {str(e)}")

                # Если есть прогресс-бар, обновляем его
                if self.progress_bar:
                    self.progress_bar.update_stats({
                        'found_count': stats['found_count'],
                        'total_criteria_found': stats['total_criteria_found'],
                        'skipped_no_text': stats['skipped_no_text'],
                        'errors': stats['errors'],
                        'updated': stats['updated'],
                    })
                    self.progress_bar.update(0)

            # Обновляем прогресс-бар (после обработки игры)
            if self.progress_bar:
                # Обновляем статистику в прогресс-баре
                self.progress_bar.update_stats({
                    'found_count': stats['found_count'],
                    'total_criteria_found': stats['total_criteria_found'],
                    'skipped_no_text': stats['skipped_no_text'],
                    'errors': stats['errors'],
                    'updated': stats['updated'],
                })
                self.progress_bar.update(1)

            # Периодическое сохранение состояния И ОБНОВЛЕНИЕ ФАЙЛА
            if actually_processed_count % self.checkpoint_interval == 0:
                self._save_state()

                # СБРАСЫВАЕМ БУФЕР ВЫВОДА В ФАЙЛ
                if hasattr(self.command, 'output_file') and self.command.output_file:
                    try:
                        self.command.output_file.flush()
                        # Выводим сообщение о сбросе буфера
                        original_out = self.command.original_stdout or sys.stdout
                        original_out.write(f"\n💾 Сброс буфера файла (обработано {actually_processed_count} игр)\n")
                        original_out.flush()
                    except Exception as e:
                        original_out = self.command.original_stdout or sys.stdout
                        original_out.write(f"\n⚠️ Ошибка сброса буфера файла: {e}\n")
                        original_out.flush()

        # Завершаем прогресс-бар
        if self.progress_bar:
            self.progress_bar.finish()
            self.progress_bar = None

        # Финальное сохранение состояния
        self._save_state()

        # ФИНАЛЬНЫЙ СБРОС БУФЕРА ФАЙЛА
        if hasattr(self.command, 'output_file') and self.command.output_file:
            try:
                self.command.output_file.flush()
            except Exception:
                pass

        # ВЫВОДИМ ФИНАЛЬНУЮ СТАТИСТИКУ
        original_out = self.command.original_stdout or sys.stdout

        if skipped_already_processed > 0:
            original_out.write(f"⏭️  Пропущено {skipped_already_processed} ранее обработанных игр\n")

        original_out.write(f"✅ Обработано {stats['processed']} новых игр\n")

        # Если были новые игры, показываем статистику по найденным критериям
        if stats['processed'] > 0:
            original_out.write(f"🎯 Игр с найденными критериями: {stats['found_count']}\n")
            original_out.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}\n")

        if stats['skipped_no_text'] > 0:
            original_out.write(f"⏭️  Игр без текста: {stats['skipped_no_text']}\n")

        if stats['errors'] > 0:
            original_out.write(f"❌ Ошибок: {stats['errors']}\n")

        if stats['updated'] > 0:
            original_out.write(f"💾 Обновлено игр: {stats['updated']}\n")

        original_out.write(f"⏱️  Время выполнения: {time.time() - start_time:.1f} секунд\n")
        original_out.flush()

        # Добавляем время выполнения
        stats['execution_time'] = time.time() - start_time

        return stats

    def _setup_state_tracking(self):
        """Настраивает отслеживание состояния"""
        # Получаем путь к файлу вывода из команды
        if hasattr(self.command, 'output_path') and self.command.output_path:
            # Создаем файл состояния на основе пути к файлу вывода
            state_path = os.path.splitext(self.command.output_path)[0] + '_state.json'
            self.state_file = state_path

            # Загружаем ранее обработанные игры ТОЛЬКО если не force-restart
            if not getattr(self.command, 'force_restart', False) and os.path.exists(state_path):
                try:
                    with open(state_path, 'r', encoding='utf-8') as f:
                        state_data = json.load(f)
                        self.processed_games = set(state_data.get('processed_games', []))
                        # Выводим в терминал, а не в файл
                        original_out = self.command.original_stdout or sys.stdout
                        if len(self.processed_games) > 0:
                            original_out.write(
                                f"📖 Загружено состояние: {len(self.processed_games)} ранее обработанных игр\n")
                        original_out.flush()
                except Exception as e:
                    original_out = self.command.original_stdout or sys.stdout
                    original_out.write(f"⚠️ Ошибка загрузки состояния: {e}\n")
                    original_out.flush()
                    self.processed_games = set()
            elif getattr(self.command, 'force_restart', False):
                # Если force-restart, очищаем состояние
                self.processed_games = set()
                # Выводим сообщение
                original_out = self.command.original_stdout or sys.stdout
                original_out.write(f"♻️  Режим force-restart: начинаем заново\n")
                original_out.flush()
            else:
                self.processed_games = set()
        elif hasattr(self.command, 'output_file') and self.command.output_file:
            # Если есть файловый объект, но нет пути, создаем стандартное имя
            self.state_file = 'game_processor_state.json'
            # Та же логика для загрузки состояния
            if not getattr(self.command, 'force_restart', False) and os.path.exists(self.state_file):
                try:
                    with open(self.state_file, 'r', encoding='utf-8') as f:
                        state_data = json.load(f)
                        self.processed_games = set(state_data.get('processed_games', []))
                        original_out = self.command.original_stdout or sys.stdout
                        if len(self.processed_games) > 0:
                            original_out.write(
                                f"📖 Загружено состояние: {len(self.processed_games)} ранее обработанных игр\n")
                        original_out.flush()
                except Exception as e:
                    original_out = self.command.original_stdout or sys.stdout
                    original_out.write(f"⚠️ Ошибка загрузки состояния: {e}\n")
                    original_out.flush()
                    self.processed_games = set()
            elif getattr(self.command, 'force_restart', False):
                self.processed_games = set()
                original_out = self.command.original_stdout or sys.stdout
                original_out.write(f"♻️  Режим force-restart: начинаем заново\n")
                original_out.flush()
            else:
                self.processed_games = set()
        else:
            self.processed_games = set()

    def _save_state(self):
        """Сохраняет текущее состояние обработки"""
        if self.state_file:
            try:
                state_data = {
                    'processed_games': list(self.processed_games),
                    'timestamp': time.time(),
                    'total_processed': len(self.processed_games)
                }
                with open(self.state_file, 'w', encoding='utf-8') as f:
                    json.dump(state_data, f, ensure_ascii=False, indent=2)

                # Выводим сообщение о сохранении состояния
                if len(self.processed_games) % 10000 == 0:  # Каждые 10k игр
                    original_out = self.command.original_stdout or sys.stdout
                    original_out.write(f"💾 Сохранено состояние: {len(self.processed_games)} игр\n")
                    original_out.flush()
            except Exception as e:
                original_out = self.command.original_stdout or sys.stdout
                original_out.write(f"⚠️ Ошибка сохранения состояния: {e}\n")
                original_out.flush()


    def _print_statistics_block(self, stats: Dict, current_index: int, total_games: int, output_stream):
        """Печатает блок статистики"""
        output_stream.write("\n" + "=" * 60 + "\n")
        output_stream.write(f"📊 ПРОМЕЖУТОЧНАЯ СТАТИСТИКА (обработано {current_index}/{total_games})\n")
        output_stream.write("=" * 60 + "\n")
        output_stream.write(f"🔄 Обработано: {stats['processed']}\n")
        output_stream.write(f"💾 Обновлено: {stats['updated']}\n")
        output_stream.write(f"⏭️  Пропущено (нет текста): {stats['skipped_no_text']}\n")
        output_stream.write(f"❌ Ошибок: {stats['errors']}\n")
        output_stream.write(f"🎯 Игр с найденными критериями: {stats['found_count']}\n")
        output_stream.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}\n")
        output_stream.write(f"⏱️  Прогресс: {(current_index / total_games * 100):.1f}%\n")
        output_stream.write("=" * 60 + "\n")
        output_stream.flush()

    def _print_file_statistics_block(self, stats: Dict, current_index: int, total_games: int):
        """Печатает статистику в файл - перезаписывая один и тот же блок"""
        output_stream = self.command.stdout._out

        try:
            # Если это первая промежуточная статистика, печатаем шапку
            if current_index == self.checkpoint_interval:
                output_stream.write("\n" + "=" * 60 + "\n")
                output_stream.write("📊 ПРОМЕЖУТОЧНАЯ СТАТИСТИКА (обновляется каждые 1000 игр)\n")
                output_stream.write("=" * 60 + "\n")

            # Используем ANSI escape codes для перемещения курсора
            # Сохраняем позицию курсора
            output_stream.write("\0337")  # Save cursor position

            # Перемещаем курсор в начало блока статистики
            if current_index == self.checkpoint_interval:
                # Для первой статистики - двигаемся на 4 строки вверх
                output_stream.write("\033[4A")
            else:
                # Для последующих - двигаемся на 7 строк вверх
                output_stream.write("\033[7A")

            # Очищаем строки
            for _ in range(7):
                output_stream.write("\033[2K")  # Очистить строку
                output_stream.write("\033[1B")  # Переместиться на строку вниз

            # Возвращаемся на 7 строк вверх
            output_stream.write("\033[7A")

            # Выводим обновленную статистику
            progress_percent = (current_index / total_games * 100) if total_games > 0 else 0
            output_stream.write(f"Обработано: {current_index}/{total_games} ({progress_percent:.1f}%)\n")
            output_stream.write(f"🎯 Игр с найденными критериями: {stats['found_count']}\n")
            output_stream.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}\n")
            output_stream.write(f"🔄 Обработано: {stats['processed']}\n")
            output_stream.write(f"💾 Обновлено: {stats['updated']}\n")
            output_stream.write(f"⏭️  Пропущено (нет текста): {stats['skipped_no_text']}\n")
            output_stream.write(f"❌ Ошибок: {stats['errors']}\n")

            # Восстанавливаем позицию курсора
            output_stream.write("\0338")

            output_stream.flush()
        except Exception:
            # Если ANSI codes не поддерживаются, выводим обычным способом
            self._print_simple_statistics_block(stats, current_index, total_games, output_stream)

    def _print_simple_statistics_block(self, stats: Dict, current_index: int, total_games: int, output_stream):
        """Простой вывод статистики (без ANSI кодов)"""
        # Просто печатаем новую статистику
        output_stream.write("\n" + "=" * 60 + "\n")
        output_stream.write(f"📊 ПРОМЕЖУТОЧНАЯ СТАТИСТИКА (обработано {current_index}/{total_games})\n")
        output_stream.write("=" * 60 + "\n")
        output_stream.write(f"🔄 Обработано: {stats['processed']}\n")
        output_stream.write(f"💾 Обновлено: {stats['updated']}\n")
        output_stream.write(f"⏭️  Пропущено (нет текста): {stats['skipped_no_text']}\n")
        output_stream.write(f"❌ Ошибок: {stats['errors']}\n")
        output_stream.write(f"🎯 Игр с найденными критериями: {stats['found_count']}\n")
        output_stream.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}\n")
        output_stream.write(f"⏱️  Прогресс: {(current_index / total_games * 100):.1f}%\n")
        output_stream.write("=" * 60 + "\n")
        output_stream.flush()

    def _print_terminal_statistics_line(self, stats: Dict, current_index: int, total_games: int):
        """Печатает статистику в терминал - одну обновляемую строку"""
        # Используем оригинальный stderr для терминала
        terminal_stream = sys.stderr

        # Формируем сообщение с возвратом каретки для перезаписи
        progress_percent = (current_index / total_games * 100) if total_games > 0 else 0
        message = f"\r📊 Статистика: {current_index}/{total_games} ({progress_percent:.1f}%) | "
        message += f"🎯: {stats['found_count']} | "
        message += f"📈: {stats['total_criteria_found']} | "
        message += f"🔄: {stats['processed']} | "
        message += f"💾: {stats['updated']} | "
        message += f"⏭️: {stats['skipped_no_text']} | "
        message += f"❌: {stats['errors']}"

        # Добавляем пробелы в конец для очистки остатков предыдущей строки
        terminal_width = 120  # предположительная ширина терминала
        if len(message) < terminal_width:
            message += " " * (terminal_width - len(message))

        terminal_stream.write(message)
        terminal_stream.flush()

    def _process_single_game(self, game, index: int, stats: Dict) -> bool:
        """Обрабатывает одну игру используя ВСЕ паттерны сразу"""
        text_to_analyze = self.command.get_text_to_analyze(game)

        # Проверяем только на полное отсутствие текста
        if not text_to_analyze or text_to_analyze.strip() == "":
            stats['skipped_no_text'] += 1

            # Выводим информацию о пропуске только если не используем прогресс-бар ИЛИ verbose режим
            if self.command.verbose and not self.command.only_found and (not self.progress_bar or self.command.verbose):
                self.command.stdout.write(f"{index}. {game.name} - ⏭️ ПРОПУЩЕНО (текста вообще нет)")
                # Добавляем отступ после пропущенной игры
                if not self.progress_bar or self.command.verbose:
                    self.command.stdout.write("")
            return True

        try:
            # Анализируем текст используя ВСЕ паттерны сразу
            results, pattern_info = self.analyzer.analyze_all_patterns(
                text_to_analyze,
                game=game,
                ignore_existing=self.command.ignore_existing,
                collect_patterns=self.command.verbose,
                keywords_mode=self.command.keywords
            )

            # В РЕЖИМЕ КЛЮЧЕВЫХ СЛОВ
            if self.command.keywords:
                keywords_results = results.get('keywords', [])
                has_found_criteria = len(keywords_results) > 0
                criteria_count = len(keywords_results)

                # Если ignore-existing, проверяем только новые
                if self.command.ignore_existing:
                    existing_keywords = self.analyzer._get_existing_objects(game, 'keywords')
                    existing_names = {kw.name for kw in existing_keywords}
                    new_keywords = [kw for kw in keywords_results if kw.name not in existing_names]
                    actual_found_count = len(new_keywords)
                    actual_has_found_criteria = actual_found_count > 0
                else:
                    new_keywords = keywords_results
                    actual_found_count = criteria_count
                    actual_has_found_criteria = has_found_criteria

            # В ОБЫЧНОМ РЕЖИМЕ
            else:
                has_found_criteria = any(
                    len(results[key]) > 0 for key in ['genres', 'themes', 'perspectives', 'game_modes'] if
                    key in results)
                criteria_count = sum(
                    len(results.get(key, [])) for key in ['genres', 'themes', 'perspectives', 'game_modes'])

                if self.command.ignore_existing and has_found_criteria:
                    actual_found_count = 0
                    actual_has_found_criteria = False
                    for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                        if criteria_type in results:
                            existing_items = self.analyzer._get_existing_objects(game, criteria_type)
                            existing_names = {item.name for item in existing_items}
                            new_items = [item for item in results[criteria_type] if item.name not in existing_names]
                            actual_found_count += len(new_items)
                            if new_items:
                                actual_has_found_criteria = True
                else:
                    actual_found_count = criteria_count
                    actual_has_found_criteria = has_found_criteria

            # ОБНОВЛЯЕМ СТАТИСТИКУ
            stats['total_criteria_found'] += actual_found_count

            if actual_has_found_criteria:
                stats['found_count'] += 1

            # В режиме only-found пропускаем игры без найденных критериев
            if self.command.only_found and not actual_has_found_criteria:
                return True

            # Пропускаем игру если нет реально новых критериев в режиме ignore-existing + update-game
            if self.command.ignore_existing and self.command.update_game and not actual_has_found_criteria:
                return True

            # ВЫВОДИМ ДЕТАЛИ ДАЖЕ С ПРОГРЕСС-БАРОМ (но только в файл, не в терминал)
            # Проверяем, идет ли вывод в файл
            output_is_file = self.command.stdout._out != sys.stdout and self.command.stdout._out != sys.stderr

            if output_is_file or not self.progress_bar:
                # Выводим детали в файл (или в терминал если нет прогресс-бара)
                stats['displayed_count'] += 1

                # Добавляем пустую строку перед результатами новой игры (кроме первой)
                if stats['displayed_count'] > 1:
                    self.command.stdout.write("")

                if self.command.verbose and not self.command.only_found:
                    self.command.stdout.write(f"{index}. 🔍 Анализируем: {game.name}")
                    text_source = self.command._get_text_source_for_game(game, text_to_analyze)
                    text_length = len(text_to_analyze)
                    self.command.stdout.write(f"   📝 Используется: {text_source} ({text_length} символов)")

                # ВЫВОДИМ РЕЗУЛЬТАТЫ
                if actual_has_found_criteria:
                    # Для режима ключевых слов фильтруем результаты
                    if self.command.keywords:
                        display_results = {'keywords': new_keywords} if self.command.ignore_existing else {
                            'keywords': keywords_results}
                        display_pattern_info = {'keywords': pattern_info.get('keywords', [])}
                        self.command._print_game_results(game, display_results, actual_found_count,
                                                         display_pattern_info)
                    else:
                        # Для обычного режима
                        if self.command.ignore_existing:
                            # Фильтруем только новые
                            filtered_results = {}
                            for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                                if criteria_type in results:
                                    existing_items = self.analyzer._get_existing_objects(game, criteria_type)
                                    existing_names = {item.name for item in existing_items}
                                    new_items = [item for item in results[criteria_type] if
                                                 item.name not in existing_names]
                                    if new_items:
                                        filtered_results[criteria_type] = new_items
                            self.command._print_game_results(game, filtered_results, actual_found_count, pattern_info)
                        else:
                            self.command._print_game_results(game, results, actual_found_count, pattern_info)

                    if self.command.update_game:
                        if self.command.update_game_criteria(game, results):
                            stats['updated'] += 1
                        elif self.command.verbose:
                            mode = 'ключевых слов' if self.command.keywords else 'критериев'
                            self.command.stdout.write(f"   ℹ️ Нет новых {mode} для обновления")

                elif not self.command.only_found:
                    # ИСПРАВЛЕННЫЙ ВЫВОД КОГДА НЕ НАЙДЕНО
                    mode = 'ключевые слова' if self.command.keywords else 'новые критерии'
                    if self.command.ignore_existing:
                        mode = 'новые ключевые слова' if self.command.keywords else 'новые критерии'
                    self.command.stdout.write(f"   ⚠️ {mode.capitalize()} не найдены")

            return True

        except Exception as e:
            stats['errors'] += 1
            # Если используется прогресс-бар, очищаем его строку перед выводом ошибки
            if self.progress_bar:
                sys.stderr.write(f"\r{' ' * 150}\r")  # Очищаем строку прогресс-бара
                sys.stderr.flush()

            # Выводим ошибки даже при использовании прогресс-бара
            # Проверяем, идет ли вывод в файл
            output_is_file = self.command.stdout._out != sys.stdout and self.command.stdout._out != sys.stderr

            if not self.command.only_found and (output_is_file or not self.progress_bar):
                # Добавляем пустую строку перед ошибкой (кроме первой)
                if stats['errors'] > 1:
                    self.command.stdout.write("")

                if self.progress_bar:
                    self.command.stdout.write(f"❌ ОШИБКА при анализе {game.name} (ID: {game.id}):")
                else:
                    self.command.stdout.write(f"❌ ОШИБКА при анализе {game.name} (ID: {game.id}):")
                self.command.stdout.write(f"   📝 Текст: {text_to_analyze[:100]}...")
                self.command.stdout.write(f"   🔍 Ошибка: {str(e)}")
                import traceback
                self.command.stdout.write(f"   🕵️ Трассировка: {traceback.format_exc()}")
                self.command.stdout.write("")
            return True
