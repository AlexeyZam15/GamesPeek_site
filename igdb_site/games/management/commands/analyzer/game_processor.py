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

    def _handle_force_restart(self, state_path):
        """Обрабатывает force-restart для конкретного режима"""
        # Очищаем состояние
        self.processed_games = set()

        # Выводим сообщение
        original_out = self.command.original_stdout or sys.stdout
        mode = "ключевых слов" if hasattr(self.command, 'keywords') and self.command.keywords else "критериев"
        original_out.write(f"♻️  Режим force-restart: начинаем заново (режим: {mode})\n")

        # УДАЛЯЕМ ФАЙЛ СОСТОЯНИЯ ДЛЯ ЭТОГО РЕЖИМА, если он существует
        if os.path.exists(state_path):
            try:
                os.remove(state_path)
                original_out.write(f"🗑️  Удален файл состояния: {state_path}\n")
            except Exception as e:
                original_out.write(f"⚠️  Не удалось удалить файл состояния: {e}\n")

        original_out.flush()

    def _save_state(self):
        """Сохраняет текущее состояние обработки"""
        if self.state_file:
            try:
                current_mode = 'keywords' if hasattr(self.command, 'keywords') and self.command.keywords else 'criteria'

                state_data = {
                    'processed_games': list(self.processed_games),
                    'timestamp': time.time(),
                    'total_processed': len(self.processed_games),
                    'mode': current_mode,  # Сохраняем режим для проверки совместимости
                    'keywords_mode': hasattr(self.command, 'keywords') and self.command.keywords
                }

                with open(self.state_file, 'w', encoding='utf-8') as f:
                    json.dump(state_data, f, ensure_ascii=False, indent=2)

                # УБРАНО: сообщение о сохранении состояния каждые 10k игр
                # Молча сохраняем состояние без вывода сообщений

            except Exception as e:
                original_out = self.command.original_stdout or sys.stdout
                original_out.write(f"⚠️ Ошибка сохранения состояния: {e}\n")
                original_out.flush()

    def _setup_state_tracking(self):
        """Настраивает отслеживание состояния"""
        # Получаем путь к файлу вывода из команды
        if hasattr(self.command, 'output_path') and self.command.output_path:
            # Создаем файл состояния на основе пути к файлу вывода
            # ДОБАВЛЯЕМ СУФФИКС ДЛЯ РЕЖИМА КЛЮЧЕВЫХ СЛОВ
            state_suffix = "_keywords" if hasattr(self.command, 'keywords') and self.command.keywords else "_criteria"
            state_path = os.path.splitext(self.command.output_path)[0] + f'_state{state_suffix}.json'
            self.state_file = state_path

            # Загружаем ранее обработанные игры ТОЛЬКО если не force-restart
            if not getattr(self.command, 'force_restart', False) and os.path.exists(state_path):
                try:
                    with open(state_path, 'r', encoding='utf-8') as f:
                        state_data = json.load(f)

                        # Проверяем совместимость режима
                        saved_mode = state_data.get('mode', '')
                        current_mode = 'keywords' if hasattr(self.command,
                                                             'keywords') and self.command.keywords else 'criteria'

                        if saved_mode != current_mode:
                            # Если режимы не совпадают, НЕ загружаем состояние и показываем сообщение
                            original_out = self.command.original_stdout or sys.stdout
                            original_out.write(
                                f"⚠️  Обнаружено состояние от другого режима ({saved_mode}). Начинаем заново.\n")
                            original_out.flush()
                            self.processed_games = set()
                        else:
                            # Режимы совпадают, загружаем состояние
                            self.processed_games = set(state_data.get('processed_games', []))
                            # Выводим в терминал, а не в файл
                            original_out = self.command.original_stdout or sys.stdout
                            if len(self.processed_games) > 0:
                                mode_text = "ключевых слов" if current_mode == 'keywords' else "критериев"
                                original_out.write(
                                    f"📖 Загружено состояние: {len(self.processed_games)} ранее обработанных игр (режим: {mode_text})\n")
                            original_out.flush()

                except Exception as e:
                    original_out = self.command.original_stdout or sys.stdout
                    original_out.write(f"⚠️ Ошибка загрузки состояния: {e}\n")
                    original_out.flush()
                    self.processed_games = set()
            elif getattr(self.command, 'force_restart', False):
                # Если force-restart, очищаем состояние ДАННОГО РЕЖИМА
                self.processed_games = set()
                # Выводим сообщение
                original_out = self.command.original_stdout or sys.stdout
                mode = "ключевых слов" if hasattr(self.command, 'keywords') and self.command.keywords else "критериев"
                original_out.write(f"♻️  Режим force-restart: начинаем заново (режим: {mode})\n")
                original_out.flush()

                # УДАЛЯЕМ ФАЙЛ СОСТОЯНИЯ ДЛЯ ЭТОГО РЕЖИМА
                if os.path.exists(state_path):
                    try:
                        os.remove(state_path)
                        original_out.write(f"🗑️  Удален файл состояния: {state_path}\n")
                        original_out.flush()
                    except Exception as e:
                        original_out.write(f"⚠️  Не удалось удалить файл состояния: {e}\n")
                        original_out.flush()
            else:
                self.processed_games = set()
        elif hasattr(self.command, 'output_file') and self.command.output_file:
            # Если есть файловый объект, но нет пути, создаем стандартное имя
            # ДОБАВЛЯЕМ СУФФИКС ДЛЯ РЕЖИМА КЛЮЧЕВЫХ СЛОВ
            state_suffix = "_keywords" if hasattr(self.command, 'keywords') and self.command.keywords else "_criteria"
            self.state_file = f'game_processor_state{state_suffix}.json'

            # Та же логика для загрузки состояния
            if not getattr(self.command, 'force_restart', False) and os.path.exists(self.state_file):
                try:
                    with open(self.state_file, 'r', encoding='utf-8') as f:
                        state_data = json.load(f)

                        # Проверяем совместимость режима
                        saved_mode = state_data.get('mode', '')
                        current_mode = 'keywords' if hasattr(self.command,
                                                             'keywords') and self.command.keywords else 'criteria'

                        if saved_mode != current_mode:
                            # Если режимы не совпадают, НЕ загружаем состояние
                            original_out = self.command.original_stdout or sys.stdout
                            original_out.write(
                                f"⚠️  Обнаружено состояние от другого режима ({saved_mode}). Начинаем заново.\n")
                            original_out.flush()
                            self.processed_games = set()
                        else:
                            self.processed_games = set(state_data.get('processed_games', []))
                            original_out = self.command.original_stdout or sys.stdout
                            if len(self.processed_games) > 0:
                                mode_text = "ключевых слов" if current_mode == 'keywords' else "критериев"
                                original_out.write(
                                    f"📖 Загружено состояние: {len(self.processed_games)} ранее обработанных игр (режим: {mode_text})\n")
                            original_out.flush()
                except Exception as e:
                    original_out = self.command.original_stdout or sys.stdout
                    original_out.write(f"⚠️ Ошибка загрузки состояния: {e}\n")
                    original_out.flush()
                    self.processed_games = set()
            elif getattr(self.command, 'force_restart', False):
                self.processed_games = set()
                original_out = self.command.original_stdout or sys.stdout
                mode = "ключевых слов" if hasattr(self.command, 'keywords') and self.command.keywords else "критериев"
                original_out.write(f"♻️  Режим force-restart: начинаем заново (режим: {mode})\n")
                original_out.flush()

                # УДАЛЯЕМ ФАЙЛ СОСТОЯНИЯ ДЛЯ ЭТОГО РЕЖИМА
                if os.path.exists(self.state_file):
                    try:
                        os.remove(self.state_file)
                        original_out.write(f"🗑️  Удален файл состояния: {self.state_file}\n")
                        original_out.flush()
                    except Exception as e:
                        original_out.write(f"⚠️  Не удалось удалить файл состояния: {e}\n")
                        original_out.flush()
            else:
                self.processed_games = set()
        else:
            self.processed_games = set()

    def _analyze_game(self, game, index: int, stats: Dict, text_to_analyze: str) -> bool:
        """Анализирует игру и обновляет статистику"""
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
            return self._process_keywords_mode(game, index, stats, results, pattern_info)
        else:
            return self._process_normal_mode(game, index, stats, results, pattern_info)

    def _process_single_game(self, game, index: int, stats: Dict) -> bool:
        """Обрабатывает одну игру используя ВСЕ паттерны сразу"""
        text_to_analyze = self.command.get_text_to_analyze(game)

        # Проверяем только на полное отсутствие текста
        if not text_to_analyze or text_to_analyze.strip() == "":
            stats['skipped_no_text'] += 1
            self._handle_no_text_game(game, index, stats)
            return True

        try:
            return self._analyze_game(game, index, stats, text_to_analyze)
        except Exception as e:
            return self._handle_analysis_error(game, index, stats, e, text_to_analyze)

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
            # Инициализируем статистику для ключевых слов
            'keywords_processed': 0,
            'keywords_found': 0,
            'keywords_count': 0,
        }

        # Сохраняем ссылку на статистику для доступа из других методов
        self._current_stats = stats

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
            mode = "ключевых слов" if hasattr(self.command, 'keywords') and self.command.keywords else "критериев"
            original_out.write(f"✅ Все игры ({total_games_in_db}) уже обработаны ранее (режим: {mode})\n")
            original_out.flush()

            # Обновляем статистику для случая, когда все игры уже обработаны
            stats['processed'] = 0
            stats['execution_time'] = 0
            # Для ключевых слов тоже обнуляем
            if hasattr(self.command, 'keywords') and self.command.keywords:
                stats['keywords_processed'] = 0
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

        try:
            for index, game in enumerate(games_iterator, 1):
                # БЕЗ ВЫВОДА СООБЩЕНИЙ проверяем, не была ли игра уже обработана
                if need_check_processed and game.id in self.processed_games:
                    skipped_already_processed += 1
                    continue

                stats['processed'] += 1
                actually_processed_count += 1

                # ОБНОВЛЯЕМ СТАТИСТИКУ ДЛЯ КЛЮЧЕВЫХ СЛОВ (счетчик обработанных игр)
                if self.command.keywords:
                    stats['keywords_processed'] = stats.get('keywords_processed', 0) + 1

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
                        # Обновляем соответствующую статистику в зависимости от режима
                        if self.command.keywords:
                            self.progress_bar.update_stats({
                                'found_count': stats['keywords_found'],
                                'total_criteria_found': stats['keywords_count'],
                                'skipped_no_text': stats['skipped_no_text'],
                                'errors': stats['errors'],
                                'updated': stats['updated'],
                            })
                        else:
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
                    if self.command.keywords:
                        self.progress_bar.update_stats({
                            'found_count': stats['keywords_found'],
                            'total_criteria_found': stats['keywords_count'],
                            'skipped_no_text': stats['skipped_no_text'],
                            'errors': stats['errors'],
                            'updated': stats['updated'],
                        })
                    else:
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

                    # СБРАСЫВАЕМ БУФЕР ВЫВОДА В ФАЙЛ БЕЗ ВЫВОДА СООБЩЕНИЯ
                    if hasattr(self.command, 'output_file') and self.command.output_file:
                        try:
                            self.command.output_file.flush()
                            # Убрано сообщение о сбросе буфера
                        except Exception:
                            # Молча игнорируем ошибки сброса буфера
                            pass

        except KeyboardInterrupt:
            # При прерывании запоминаем сколько мы успели обработать
            stats['processed'] = actually_processed_count
            if self.command.keywords:
                stats['keywords_processed'] = actually_processed_count
            stats['execution_time'] = time.time() - start_time

            # Сохраняем состояние
            self._save_state()

            # ЗАВЕРШАЕМ ПРОГРЕСС-БАР (если есть)
            if self.progress_bar:
                self.progress_bar.finish()
                self.progress_bar = None

            # ВОЗВРАЩАЕМ СТАТИСТИКУ ДАЖЕ ПРИ ПРЕРЫВАНИИ
            return stats

        except Exception as e:
            # При других ошибках тоже сохраняем статистику
            stats['processed'] = actually_processed_count
            if self.command.keywords:
                stats['keywords_processed'] = actually_processed_count
            stats['execution_time'] = time.time() - start_time

            # Завершаем прогресс-бар
            if self.progress_bar:
                self.progress_bar.finish()
                self.progress_bar = None

            self._save_state()
            raise  # Пробрасываем исключение дальше

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

        # ВЫВОДИМ ФИНАЛЬНУЮ СТАТИСТИКУ В ТЕРМИНАЛ (только при нормальном завершении)
        original_out = self.command.original_stdout or sys.stdout

        if skipped_already_processed > 0:
            original_out.write(f"⏭️  Пропущено {skipped_already_processed} ранее обработанных игр\n")

        # Определяем режим и выводим соответствующую статистику
        if self.command.keywords:
            # Режим ключевых слов
            original_out.write(f"✅ Обработано {stats['keywords_processed']} новых игр\n")
            if stats['keywords_processed'] > 0:
                original_out.write(f"🎯 Игр с найденными ключевыми словами: {stats['keywords_found']}\n")
                original_out.write(f"📈 Всего ключевых слов найдено: {stats['keywords_count']}\n")
        else:
            # Обычный режим
            original_out.write(f"✅ Обработано {stats['processed']} новых игр\n")
            if stats['processed'] > 0:
                original_out.write(f"🎯 Игр с найденными критериями: {stats['found_count']}\n")
                original_out.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}\n")

        if stats['skipped_no_text'] > 0:
            original_out.write(f"⏭️  Игр без текста: {stats['skipped_no_text']}\n")

        if stats['errors'] > 0:
            original_out.write(f"❌ Ошибок: {stats['errors']}\n")

        if stats['updated'] > 0:
            original_out.write(f"💾 Обновлено игр: {stats['updated']}\n")

        execution_time = time.time() - start_time
        stats['execution_time'] = execution_time
        original_out.write(f"⏱️  Время выполнения: {execution_time:.1f} секунд\n")
        original_out.flush()

        # ДОБАВЛЯЕМ СТАТИСТИКУ В ФАЙЛ ВЫВОДА (только при нормальном завершении)
        if hasattr(self.command, 'output_file') and self.command.output_file:
            try:
                # Записываем разделитель перед статистикой
                self.command.stdout.write("\n" + "=" * 60)

                # Определяем режим анализа
                if self.command.keywords:
                    mode = 'КЛЮЧЕВЫЕ СЛОВА'
                else:
                    mode = 'КРИТЕРИИ'

                self.command.stdout.write(f"📊 ИТОГОВАЯ СТАТИСТИКА АНАЛИЗА ({mode})")
                self.command.stdout.write("=" * 60)

                if skipped_already_processed > 0:
                    self.command.stdout.write(f"⏭️  Пропущено ранее обработанных игр: {skipped_already_processed}")

                # Выводим соответствующую статистику
                if self.command.keywords:
                    self.command.stdout.write(f"🔄 Обработано новых игр: {stats['keywords_processed']}")
                    self.command.stdout.write(f"🎯 Игр с найденными ключ. словами: {stats['keywords_found']}")
                    self.command.stdout.write(f"📈 Всего ключевых слов найдено: {stats['keywords_count']}")
                else:
                    self.command.stdout.write(f"🔄 Обработано новых игр: {stats['processed']}")
                    self.command.stdout.write(f"🎯 Игр с найденными критериями: {stats['found_count']}")
                    self.command.stdout.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}")

                self.command.stdout.write(f"⏭️  Игр без текста: {stats['skipped_no_text']}")
                self.command.stdout.write(f"❌ Ошибок: {stats['errors']}")
                self.command.stdout.write(f"💾 Обновлено игр: {stats['updated']}")

                self.command.stdout.write(f"⏱️  Время выполнения: {execution_time:.1f} секунд")

                self.command.stdout.write("=" * 60)
                self.command.stdout.write("✅ Анализ успешно завершен")
                self.command.stdout.write("=" * 60)

                # Сбрасываем буфер для файла
                self.command.output_file.flush()
            except Exception as e:
                # Выводим ошибку в терминал
                original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")
                original_out.flush()

        return stats

    def _update_and_display_results(self, game, index: int, stats: Dict, results, pattern_info,
                                    actual_found_count: int, actual_has_found_criteria: bool,
                                    keywords_mode: bool, new_keywords=None) -> bool:
        """Обновляет статистику и отображает результаты"""
        # ОБНОВЛЯЕМ СТАТИСТИКУ
        if keywords_mode:
            # Для ключевых слов
            stats['keywords_count'] = stats.get('keywords_count', 0) + actual_found_count
            if actual_has_found_criteria:
                stats['keywords_found'] = stats.get('keywords_found', 0) + 1
            # Также обновляем общую статистику для совместимости
            stats['total_criteria_found'] += actual_found_count
            if actual_has_found_criteria:
                stats['found_count'] += 1
        else:
            # Для обычных критериев
            stats['total_criteria_found'] += actual_found_count
            if actual_has_found_criteria:
                stats['found_count'] += 1

        # В режиме only-found пропускаем игры без найденных критериев
        if self.command.only_found and not actual_has_found_criteria:
            return True

        # Пропускаем игру если нет реально новых критериев в режиме ignore-existing + update-game
        if self.command.ignore_existing and self.command.update_game and not actual_has_found_criteria:
            return True

        # ВЫВОДИМ ДЕТАЛИ
        output_is_file = self.command.stdout._out != sys.stdout and self.command.stdout._out != sys.stderr

        if output_is_file or not self.progress_bar:
            # Выводим детали в файл (или в терминал если нет прогресс-бара)
            stats['displayed_count'] += 1

            # Добавляем пустую строку перед результатами новой игры (кроме первой)
            if stats['displayed_count'] > 1:
                self.command.stdout.write("")

            if self.command.verbose and not self.command.only_found:
                self.command.stdout.write(f"{index}. 🔍 Анализируем: {game.name}")
                text_source = self.command._get_text_source_for_game(game, self.command.get_text_to_analyze(game))
                text_length = len(self.command.get_text_to_analyze(game))
                self.command.stdout.write(f"   📝 Используется: {text_source} ({text_length} символов)")

            # ВЫВОДИМ РЕЗУЛЬТАТЫ
            if actual_has_found_criteria:
                self._display_found_results(game, results, pattern_info, actual_found_count, keywords_mode,
                                            new_keywords)

                if self.command.update_game:
                    if self.command.update_game_criteria(game, results):
                        stats['updated'] += 1
                    elif self.command.verbose:
                        mode = 'ключевых слов' if keywords_mode else 'критериев'
                        self.command.stdout.write(f"   ℹ️ Нет новых {mode} для обновления")

            elif not self.command.only_found:
                # ИСПРАВЛЕННЫЙ ВЫВОД КОГДА НЕ НАЙДЕНО
                mode = 'ключевые слова' if keywords_mode else 'новые критерии'
                if self.command.ignore_existing:
                    mode = 'новые ключевые слова' if keywords_mode else 'новые критерии'
                self.command.stdout.write(f"   ⚡ {mode.capitalize()} не найдены")

        return True

    def _display_final_stats(self, stats: Dict, skipped_already_processed: int, start_time: float):
        """Отображает финальную статистику"""
        # В терминал
        self._display_terminal_stats(stats, skipped_already_processed, start_time)

        # В файл
        self._display_file_stats(stats, skipped_already_processed, start_time)

    def _display_terminal_stats(self, stats: Dict, skipped_already_processed: int, start_time: float):
        """Отображает статистику в терминале"""
        original_out = self.command.original_stdout or sys.stdout

        if skipped_already_processed > 0:
            original_out.write(f"⏭️  Пропущено {skipped_already_processed} ранее обработанных игр\n")

        # Определяем режим и выводим соответствующую статистику
        if hasattr(self.command, 'keywords') and self.command.keywords:
            # Режим ключевых слов
            original_out.write(f"✅ Обработано {stats['keywords_processed']} новых игр\n")
            if stats['keywords_processed'] > 0:
                original_out.write(f"🎯 Игр с найденными ключевыми словами: {stats['keywords_found']}\n")
                original_out.write(f"📈 Всего ключевых слов найдено: {stats['keywords_count']}\n")
        else:
            # Обычный режим
            original_out.write(f"✅ Обработано {stats['processed']} новых игр\n")
            if stats['processed'] > 0:
                original_out.write(f"🎯 Игр с найденными критериями: {stats['found_count']}\n")
                original_out.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}\n")

        if stats['skipped_no_text'] > 0:
            original_out.write(f"⏭️  Игр без текста: {stats['skipped_no_text']}\n")

        if stats['errors'] > 0:
            original_out.write(f"❌ Ошибок: {stats['errors']}\n")

        if stats['updated'] > 0:
            original_out.write(f"💾 Обновлено игр: {stats['updated']}\n")

        exec_time = time.time() - start_time
        stats['execution_time'] = exec_time
        original_out.write(f"⏱️  Время выполнения: {exec_time:.1f} секунд\n")
        original_out.flush()

    def _display_file_stats(self, stats: Dict, skipped_already_processed: int, start_time: float):
        """Отображает статистику в файл вывода"""
        if hasattr(self.command, 'output_file') and self.command.output_file:
            try:
                # Записываем разделитель перед статистикой
                self.command.stdout.write("\n" + "=" * 60)

                # Определяем режим анализа
                mode = 'КЛЮЧЕВЫЕ СЛОВА' if hasattr(self.command, 'keywords') and self.command.keywords else 'КРИТЕРИИ'

                self.command.stdout.write(f"📊 ИТОГОВАЯ СТАТИСТИКА АНАЛИЗА ({mode})")
                self.command.stdout.write("=" * 60)

                if skipped_already_processed > 0:
                    self.command.stdout.write(f"⏭️  Пропущено ранее обработанных игр: {skipped_already_processed}")

                # Выводим соответствующую статистику
                if hasattr(self.command, 'keywords') and self.command.keywords:
                    self.command.stdout.write(f"🔄 Обработано новых игр: {stats['keywords_processed']}")
                    self.command.stdout.write(f"🎯 Игр с найденными ключ. словами: {stats['keywords_found']}")
                    self.command.stdout.write(f"📈 Всего ключевых слов найдено: {stats['keywords_count']}")
                else:
                    self.command.stdout.write(f"🔄 Обработано новых игр: {stats['processed']}")
                    self.command.stdout.write(f"🎯 Игр с найденными критериями: {stats['found_count']}")
                    self.command.stdout.write(f"📈 Всего критериев найдено: {stats['total_criteria_found']}")

                self.command.stdout.write(f"⏭️  Игр без текста: {stats['skipped_no_text']}")
                self.command.stdout.write(f"❌ Ошибок: {stats['errors']}")
                self.command.stdout.write(f"💾 Обновлено игр: {stats['updated']}")

                exec_time = stats.get('execution_time', time.time() - start_time)
                self.command.stdout.write(f"⏱️  Время выполнения: {exec_time:.1f} секунд")

                self.command.stdout.write("=" * 60)
                self.command.stdout.write("✅ Анализ успешно завершен")
                self.command.stdout.write("=" * 60)

                # Сбрасываем буфер для файла
                self.command.output_file.flush()
            except Exception as e:
                # Выводим ошибку в терминал
                original_out = self.command.original_stdout or sys.stdout
                original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")
                original_out.flush()

    def get_current_stats(self) -> Dict:
        """Возвращает текущую статистику обработки"""
        if hasattr(self, '_current_stats'):
            # Создаем копию, чтобы не менять оригинал
            return self._current_stats.copy()

        # Если статистика не инициализирована, возвращаем пустой словарь
        return {
            'processed': 0,
            'updated': 0,
            'skipped_no_text': 0,
            'errors': 0,
            'found_count': 0,
            'total_criteria_found': 0,
            'displayed_count': 0,
            'execution_time': 0,
        }

    def _init_stats(self):
        """Инициализирует статистику"""
        return {
            'processed': 0,
            'updated': 0,
            'skipped_no_text': 0,
            'errors': 0,
            'found_count': 0,
            'total_criteria_found': 0,
            'displayed_count': 0,
        }

    def _calculate_skipped_games(self):
        """Рассчитывает количество пропущенных игр"""
        if self.processed_games and not getattr(self.command, 'force_restart', False):
            return len(self.processed_games)
        return 0

    def _handle_all_games_processed(self, total_games: int):
        """Обрабатывает случай когда все игры уже обработаны"""
        original_out = self.command.original_stdout or sys.stdout
        original_out.write(f"✅ Все игры ({total_games}) уже обработаны ранее\n")
        original_out.flush()

    def _init_progress_bar(self, estimated_games_to_process: int):
        """Инициализирует прогресс-бар"""
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

    def _process_games_loop(self, games_iterator, stats: Dict, skipped_already_processed: int,
                            start_time: float) -> Dict:
        """Основной цикл обработки игр"""
        actually_processed_count = 0
        need_check_processed = bool(self.processed_games and not getattr(self.command, 'force_restart', False))

        for index, game in enumerate(games_iterator, 1):
            # Пропускаем уже обработанные игры
            if need_check_processed and game.id in self.processed_games:
                skipped_already_processed += 1
                continue

            stats['processed'] += 1
            actually_processed_count += 1

            try:
                self._process_single_game(game, index, stats)
                self.processed_games.add(game.id)

            except Exception as e:
                stats['errors'] += 1
                self._handle_game_error(game, e, stats)

            # Обновляем прогресс-бар
            self._update_progress_bar(stats)

            # Периодическое сохранение состояния
            if actually_processed_count % self.checkpoint_interval == 0:
                self._handle_periodic_checkpoint(actually_processed_count)

        # Добавляем время выполнения
        stats['execution_time'] = time.time() - start_time
        return stats

    def _handle_game_error(self, game, error: Exception, stats: Dict):
        """Обрабатывает ошибку при обработке игры"""
        if self.progress_bar:
            self.progress_bar.terminal_stderr.write("\r" + " " * 150 + "\r")
            self.progress_bar.terminal_stderr.flush()

        if not self.command.only_found:
            self.command.stderr.write(f"   ❌ Ошибка при анализе {game.name}: {str(error)}")

        # Обновляем статистику в прогресс-баре
        if self.progress_bar:
            self.progress_bar.update_stats({
                'found_count': stats['found_count'],
                'total_criteria_found': stats['total_criteria_found'],
                'skipped_no_text': stats['skipped_no_text'],
                'errors': stats['errors'],
                'updated': stats['updated'],
            })
            self.progress_bar.update(0)

    def _update_progress_bar(self, stats: Dict):
        """Обновляет прогресс-бар"""
        if self.progress_bar:
            self.progress_bar.update_stats({
                'found_count': stats['found_count'],
                'total_criteria_found': stats['total_criteria_found'],
                'skipped_no_text': stats['skipped_no_text'],
                'errors': stats['errors'],
                'updated': stats['updated'],
            })
            self.progress_bar.update(1)

    def _handle_periodic_checkpoint(self, actually_processed_count: int):
        """Обрабатывает периодический чекпоинт"""
        self._save_state()

        # Сбрасываем буфер вывода в файл
        if hasattr(self.command, 'output_file') and self.command.output_file:
            try:
                self.command.output_file.flush()
            except Exception:
                pass

    def _finish_progress_bar(self):
        """Завершает прогресс-бар"""
        if self.progress_bar:
            self.progress_bar.finish()
            self.progress_bar = None

    def _flush_output_file(self):
        """Сбрасывает буфер файла вывода"""
        if hasattr(self.command, 'output_file') and self.command.output_file:
            try:
                self.command.output_file.flush()
            except Exception:
                pass

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

    def _handle_no_text_game(self, game, index: int, stats: Dict):
        """Обрабатывает игру без текста"""
        # ВЫВОДИМ ПРОПУЩЕННЫЕ ИГРЫ ТОЛЬКО В ФАЙЛ, ЕСЛИ ЕСТЬ ВЫВОД В ФАЙЛ
        if self.command.verbose and not self.command.only_found:
            # Проверяем, идет ли вывод в файл
            if hasattr(self.command, 'output_file') and self.command.output_file:
                self.command.stdout.write(f"{index}. {game.name} - ⏭️ ПРОПУЩЕНО (текста вообще нет)")
                self.command.stdout.write("")

    def _process_keywords_mode(self, game, index: int, stats: Dict, results, pattern_info) -> bool:
        """Обрабатывает игру в режиме ключевых слов"""
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

        return self._update_and_display_results(
            game, index, stats, results, pattern_info,
            actual_found_count, actual_has_found_criteria,
            keywords_mode=True,
            new_keywords=new_keywords if self.command.ignore_existing else None
        )

    def _process_normal_mode(self, game, index: int, stats: Dict, results, pattern_info) -> bool:
        """Обрабатывает игру в обычном режиме"""
        has_found_criteria = any(
            len(results[key]) > 0 for key in ['genres', 'themes', 'perspectives', 'game_modes'] if key in results)
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

        return self._update_and_display_results(
            game, index, stats, results, pattern_info,
            actual_found_count, actual_has_found_criteria,
            keywords_mode=False
        )

    def _display_found_results(self, game, results, pattern_info, actual_found_count: int,
                               keywords_mode: bool, new_keywords=None):
        """Отображает найденные результаты"""
        if keywords_mode:
            display_results = {'keywords': new_keywords} if self.command.ignore_existing else {
                'keywords': results.get('keywords', [])}
            display_pattern_info = {'keywords': pattern_info.get('keywords', [])}
            self.command._print_game_results(game, display_results, actual_found_count, display_pattern_info)
        else:
            # Для обычного режима
            if self.command.ignore_existing:
                # Фильтруем только новые
                filtered_results = {}
                for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                    if criteria_type in results:
                        existing_items = self.analyzer._get_existing_objects(game, criteria_type)
                        existing_names = {item.name for item in existing_items}
                        new_items = [item for item in results[criteria_type] if item.name not in existing_names]
                        if new_items:
                            filtered_results[criteria_type] = new_items
                self.command._print_game_results(game, filtered_results, actual_found_count, pattern_info)
            else:
                self.command._print_game_results(game, results, actual_found_count, pattern_info)

    def _handle_analysis_error(self, game, index: int, stats: Dict, error: Exception, text_to_analyze: str) -> bool:
        """Обрабатывает ошибку при анализе"""
        stats['errors'] += 1

        # Если используется прогресс-бар, очищаем его строку перед выводом ошибки
        if self.progress_bar:
            sys.stderr.write(f"\r{' ' * 150}\r")
            sys.stderr.flush()

        # ВЫВОДИМ ОШИБКИ ТОЛЬКО В ФАЙЛ, ЕСЛИ ЕСТЬ ВЫВОД В ФАЙЛ
        if not self.command.only_found:
            # Проверяем, идет ли вывод в файл
            if hasattr(self.command, 'output_file') and self.command.output_file:
                # Добавляем пустую строку перед ошибкой (кроме первой)
                if stats['errors'] > 1:
                    self.command.stdout.write("")

                self.command.stdout.write(f"❌ ОШИБКА при анализе {game.name} (ID: {game.id}):")
                self.command.stdout.write(f"   📝 Текст: {text_to_analyze[:100]}...")
                self.command.stdout.write(f"   🔍 Ошибка: {str(error)}")
                import traceback
                self.command.stdout.write(f"   🕵️ Трассировка: {traceback.format_exc()}")
                self.command.stdout.write("")

        return True
