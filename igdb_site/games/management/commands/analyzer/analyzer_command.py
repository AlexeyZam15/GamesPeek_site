# games/management/commands/analyzer/analyzer_command.py
"""
Основной класс команды анализа с использованием нового API - ИСПРАВЛЕННАЯ ВЕРСИЯ
Использует только существующие методы API
"""

import sys
import os
import time
from typing import Dict, Any, List
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


class AnalyzerCommand(BaseCommand):
    """Команда анализа игр с использованием API"""

    def __init__(self, stdout=None, stderr=None, **kwargs):
        super().__init__(stdout, stderr, **kwargs)
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

        # Опции
        self.game_id = None
        self.game_name = None
        self.description = None
        self.limit = None
        self.offset = 0
        self.update_game = False
        self.min_text_length = 10
        self.verbose = False
        self.only_found = False
        self.batch_size = 1000
        self.ignore_existing = False
        self.hide_skipped = False
        self.no_progress = False
        self.force_restart = False
        self.keywords = False
        self.clear_cache = False

        # Источники текста
        self.use_wiki = False
        self.use_rawg = False
        self.use_storyline = False
        self.prefer_wiki = False
        self.prefer_storyline = False
        self.combine_texts = False
        self.combine_all_texts = False

        # Новые параметры (используем только если реализованы в API)
        self.comprehensive_mode = False
        self.combined_mode = False
        self.exclude_existing = False

    def _clean_output_before_progress_bar(self):
        """Очищает вывод перед созданием прогресс-бара"""
        # ИСПРАВЛЕНИЕ: Очищаем все возможные потоки вывода
        import sys

        # Очищаем stderr (куда обычно пишет прогресс-бар)
        if hasattr(sys.stderr, 'write'):
            sys.stderr.write("\r" + " " * 200 + "\r")
            sys.stderr.flush()

        # Очищаем stdout
        if hasattr(sys.stdout, 'write'):
            sys.stdout.write("\r" + " " * 200 + "\r")
            sys.stdout.flush()

        # Очищаем оригинальные потоки если они есть
        if self.original_stderr:
            self.original_stderr.write("\r" + " " * 200 + "\r")
            self.original_stderr.flush()

        if self.original_stdout:
            self.original_stdout.write("\r" + " " * 200 + "\r")
            self.original_stdout.flush()

    def _update_progress_bar(self):
        """Обновляет прогресс-бар с учетом пропущенных из-за кэша игр"""
        if not self.progress_bar:
            return

        # ИСПРАВЛЕНИЕ: Считаем ВСЕ обработанные игры (включая пропущенные)
        total_processed_including_skipped = (
                self.stats['processed'] +  # Обработанные с текстом
                self.stats['skipped_no_text'] +  # Пропущенные без текста
                self.stats.get('skipped_short_text', 0) +  # Пропущенные с коротким текстом
                self.stats.get('skipped_cached', 0)  # Пропущенные по кэшу
        )

        # ИСПРАВЛЕНИЕ: Устанавливаем актуальный прогресс
        if self.progress_bar.current < total_processed_including_skipped:
            increment = total_processed_including_skipped - self.progress_bar.current
            self.progress_bar.update(increment)

        # Обновляем статистику в прогресс-баре
        if self.keywords:
            self.progress_bar.update_stats({
                'found_count': self.stats['keywords_found'],
                'total_criteria_found': self.stats['keywords_count'],
                'skipped_total': (
                        self.stats['skipped_no_text'] +
                        self.stats.get('skipped_short_text', 0) +
                        self.stats.get('skipped_cached', 0) +
                        self.stats['keywords_not_found']
                ),
                'errors': self.stats['errors'],
                'updated': self.stats['updated'],  # ← ВАЖНО: используем актуальное значение
            })
        else:
            self.progress_bar.update_stats({
                'found_count': self.stats['found_count'],
                'total_criteria_found': self.stats['total_criteria_found'],
                'skipped_total': (
                        self.stats['skipped_no_text'] +
                        self.stats.get('skipped_short_text', 0) +
                        self.stats.get('skipped_cached', 0) +
                        self.stats['not_found_count']
                ),
                'errors': self.stats['errors'],
                'updated': self.stats['updated'],  # ← ВАЖНО: используем актуальное значение
            })

    def _process_single_game_in_batch(self, game):
        """Обрабатывает одну игру в пакетной обработке"""
        # Получаем текст
        text = self.text_preparer.prepare_text(game)

        # Обновляем статистику
        self.stats['processed'] += 1

        if not text:
            self.stats['skipped_no_text'] += 1
            self.state_manager.add_processed_game(game.id)

            # Обновляем прогресс-бар
            if self.progress_bar:
                self._update_progress_bar()

            return

        # Проверяем длину текста
        if len(text) < self.min_text_length:
            self.stats['skipped_short_text'] += 1
            self.state_manager.add_processed_game(game.id)

            # Обновляем прогресс-бар
            if self.progress_bar:
                self._update_progress_bar()

            return

        self.stats['processed_with_text'] += 1

        try:
            # По умолчанию exclude_existing=True (исключаем существующие)
            # Только если явно указано --ignore-existing, ставим False
            exclude_existing = not self.ignore_existing

            result = self.api.force_analyze_game_text(
                text=text,
                game_id=game.id,
                analyze_keywords=self.keywords,
                existing_game=game,  # Всегда передаем игру для проверки существующих
                detailed_patterns=self.verbose,
                exclude_existing=exclude_existing  # По умолчанию True
            )

            if not result['success']:
                self.stats['errors'] += 1
                self.state_manager.add_processed_game(game.id)

                # Обновляем прогресс-бар при ошибке
                if self.progress_bar:
                    self._update_progress_bar()

                return

            # Обновляем статистику
            if result['has_results']:
                found_count = result['summary'].get('found_count', 0)

                # Обновляем статистику в зависимости от режима
                if self.keywords:
                    self.stats['keywords_found'] += 1
                    self.stats['keywords_count'] += found_count
                else:
                    self.stats['found_count'] += 1
                    self.stats['total_criteria_found'] += found_count
            else:
                # Обновляем статистику "не найдено"
                if self.keywords:
                    self.stats['keywords_not_found'] += 1
                else:
                    self.stats['not_found_count'] += 1

            # Вызываем formatter для записи в файл
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

            # Добавляем в батч для обновления если нужно
            games_updated_in_this_batch = 0
            if self.update_game and result['has_results']:
                try:
                    # Получаем количество найденных элементов
                    total_found_elements = 0
                    for key, data in result['results'].items():
                        total_found_elements += data.get('count', 0)

                    # Если есть найденные элементы, добавляем в батч
                    if total_found_elements > 0:
                        # Используем исправленный метод batch_updater
                        games_updated = self.batch_updater.add_game_for_update(
                            game_id=game.id,
                            results=result['results'],
                            is_keywords=self.keywords
                        )

                        # games_updated теперь возвращает количество ОБНОВЛЕННЫХ игр
                        # (0 если игра добавлена в батч, но еще не обновлена)
                        # (>0 если батч был обновлен и в нем были обновленные игры)

                        if games_updated > 0:
                            # Если батч был обновлен, увеличиваем счетчик
                            self.stats['updated'] += games_updated
                            games_updated_in_this_batch = games_updated

                            # Сразу обновляем прогресс-бар
                            if self.progress_bar:
                                self._update_progress_bar()

                except Exception:
                    # Тихо обрабатываем исключения при добавлении в батч
                    pass

            # Всегда добавляем в StateManager
            self.state_manager.add_processed_game(game.id)

            # НЕ обновляем RangeCacheManager для force-анализа!
            if not self.force_restart and not self.verbose and not exclude_existing:
                try:
                    # Но если не force-restart, обновляем кэш
                    RangeCacheManager.update_game_range(game.id, game.id)
                except Exception:
                    pass

            # Обновляем прогресс-бар ПОСЛЕ успешной обработки
            # (если еще не обновляли из-за обновления батча)
            if self.progress_bar and games_updated_in_this_batch == 0:
                self._update_progress_bar()

            # Периодически сохраняем состояние (реже)
            if self.stats['processed'] % 500 == 0:
                try:
                    self.state_manager.save_state(self.stats['processed'])

                    # Также периодически принудительно обновляем батч
                    if self.update_game and self.batch_updater:
                        games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                            'games_to_update') else 0
                        if games_in_batch > 100:  # Если в батче накопилось много игр
                            games_updated = self.batch_updater.flush()
                            if games_updated > 0:
                                self.stats['updated'] += games_updated
                                if self.progress_bar:
                                    self._update_progress_bar()

                except Exception:
                    pass

        except Exception as e:
            self.stats['errors'] += 1

            # Всегда добавляем в StateManager даже при ошибке
            try:
                self.state_manager.add_processed_game(game.id)
            except:
                pass

            # Обновляем прогресс-бар даже при ошибке
            if self.progress_bar:
                self._update_progress_bar()

    def _init_stats(self):
        """Инициализирует статистику"""
        self.stats = {
            'processed': 0,
            'processed_with_text': 0,
            'found_count': 0,
            'not_found_count': 0,
            'total_criteria_found': 0,
            'skipped_no_text': 0,
            'skipped_short_text': 0,
            'skipped_cached': 0,  # Игры пропущенные из-за кэша RangeCacheManager
            'errors': 0,
            'updated': 0,
            'displayed_count': 0,
            'keywords_processed': 0,
            'keywords_found': 0,
            'keywords_count': 0,
            'keywords_not_found': 0,
        }

    def handle(self, *args, **options):
        """Основной обработчик команды с поддержкой очистки кэша при force-restart"""
        try:
            # Сохраняем опции
            self._store_options(options)

            # ИСПРАВЛЕНИЕ: Принудительно очищаем кэш если force-restart
            if self.force_restart:
                self.stdout.write("♻️ Принудительный перезапуск - очищаем кэши...")

                # 1. Очищаем кэш Django (RangeCacheManager)
                try:
                    from django.core.cache import cache
                    cache_keys_to_delete = [
                        'range_cache:checked_criteria_ranges',
                        'range_cache:checked_game_ranges',
                    ]

                    deleted_count = 0
                    for key in cache_keys_to_delete:
                        if cache.delete(key):
                            deleted_count += 1
                            if self.verbose:
                                self.stdout.write(f"   ✅ Удален кэш: {key}")

                    if deleted_count > 0:
                        self.stdout.write(f"✅ Очищено {deleted_count} кэшей RangeCacheManager")
                    else:
                        self.stdout.write("ℹ️ Кэши RangeCacheManager уже пусты")

                except Exception as e:
                    self.stderr.write(f"⚠️ Не удалось очистить Django кэш: {e}")

                # 2. Удаляем файл состояния
                try:
                    if hasattr(self, 'state_manager') and self.state_manager:
                        state_file = self.state_manager.state_file
                        if state_file and os.path.exists(state_file):
                            os.remove(state_file)
                            self.stdout.write(f"✅ Удален файл состояния: {state_file}")
                except Exception as e:
                    self.stderr.write(f"⚠️ Не удалось удалить файл состояния: {e}")

                # 3. Принудительно сбрасываем processed_games
                try:
                    if hasattr(self, 'state_manager') and self.state_manager:
                        self.state_manager.processed_games = set()
                        if self.verbose:
                            self.stdout.write("✅ Очищен кэш processed_games в StateManager")
                except Exception as e:
                    self.stderr.write(f"⚠️ Не удалось очистить processed_games: {e}")

            # Инициализируем компоненты
            self._init_components()

            try:
                # Настраиваем вывод в файл
                if self.output_path:
                    self._setup_file_output()

                # Выводим настройки
                if not self.only_found:
                    self._print_options_summary()

                # Обрабатываем команду
                self._process_command()

            except KeyboardInterrupt:
                self._handle_interrupt()
            except Exception as e:
                self._handle_error(e)
            finally:
                self._cleanup()

        except Exception as e:
            # Глобальная обработка ошибок
            import traceback
            error_msg = f"❌ КРИТИЧЕСКАЯ ОШИБКА В КОМАНДЕ: {e}"

            # Пытаемся вывести в оригинальный stderr если есть
            if hasattr(self, 'original_stderr') and self.original_stderr:
                self.original_stderr.write(f"\n{error_msg}\n")
                traceback.print_exc(file=self.original_stderr)
                self.original_stderr.flush()
            # Иначе в текущий stderr
            else:
                self.stderr.write(f"\n{error_msg}\n")
                traceback.print_exc()

            # Также выводим в файл если он открыт
            if hasattr(self, 'output_file') and self.output_file and not self.output_file.closed:
                try:
                    self.output_file.write(f"\n{error_msg}\n")
                    traceback.print_exc(file=self.output_file)
                    self.output_file.flush()
                except:
                    pass

            # Завершаем с ошибкой
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
        """Обрабатывает прерывание в пакетной обработке с настоящей батч-обработкой"""
        # ВАЖНО: Сначала обновляем оставшиеся игры в батче
        if self.update_game and self.batch_updater:
            try:
                # Получаем количество игр в оставшемся батче
                games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                    'games_to_update') else 0

                if games_in_batch > 0:
                    if self.original_stdout:
                        self.original_stdout.write(f"\n⏳ Обновляем оставшийся батч из {games_in_batch} игр...\n")
                        self.original_stdout.flush()

                    # Обновляем батч
                    remaining_updates = self.batch_updater.flush()
                    self.stats['updated'] += remaining_updates  # Обновляем статистику

                    # Выводим сообщение об обновленных играх
                    if self.original_stdout:
                        if remaining_updates > 0:
                            self.original_stdout.write(f"💾 Обновлено {remaining_updates} игр из оставшегося батча\n")
                        else:
                            self.original_stdout.write(
                                f"ℹ️ В оставшемся батче не было новых элементов для обновления\n")
                        self.original_stdout.flush()

                    # ВАЖНО: Обновляем прогресс-бар с новой статистикой
                    if self.progress_bar:
                        self.progress_bar.update_stats({
                            'updated': self.stats['updated']
                        })
                        self.progress_bar.finish()  # Завершаем с обновленной статистикой
                else:
                    if self.original_stdout:
                        self.original_stdout.write(f"\nℹ️ Нет необработанных игр в батче\n")
                        self.original_stdout.flush()

            except Exception as e:
                if self.original_stderr:
                    self.original_stderr.write(f"⚠️ Ошибка при обновлении оставшегося батча: {e}\n")
                    self.original_stderr.flush()

        # Сохраняем состояние
        try:
            self.state_manager.save_state(self.stats['processed'])
            if self.original_stdout:
                self.original_stdout.write(f"💾 Сохранено состояние: обработано {self.stats['processed']} игр\n")
                self.original_stdout.flush()
        except Exception as e:
            if self.original_stderr:
                self.original_stderr.write(f"⚠️ Ошибка сохранения состояния: {e}\n")
                self.original_stderr.flush()

        # Если прогресс-бар еще не завершен, завершаем его
        if self.progress_bar and self.progress_bar.current < self.progress_bar.total:
            self.progress_bar.finish()

        # Выводим сообщение о прерывании
        if self.original_stdout:
            self.original_stdout.write("\n⏹️ Обработка прервана пользователем\n")
            self.original_stdout.flush()

        if self.output_file:
            self.stdout.write("\n⏹️ Обработка прервана пользователем")

        self.stats['execution_time'] = time.time() - start_time

        # Выводим статистику прерывания
        self._display_interruption_statistics(self.stats, already_processed)

    def _analyze_all_games(self):
        """Анализирует все игры в базе данных"""
        try:
            # Загружаем состояние
            already_processed = self.state_manager.load_state()

            if already_processed > 0 and not self.force_restart:
                # Выводим в терминал
                if self.original_stdout:
                    mode = "ключевых слов" if self.keywords else "критериев"
                    self.original_stdout.write(
                        f"📖 Загружено состояние: {already_processed} ранее обработанных игр (режим: {mode})\n")
                    self.original_stdout.flush()
                # Выводим в файл
                if self.output_file:
                    self.stdout.write(f"📖 Загружено состояние: {already_processed} ранее обработанных игр")

            # Получаем игры
            games = self._get_base_query()
            total_games = games.count()

            # Применяем лимит и offset
            if self.offset:
                games = games[self.offset:]
            if self.limit:
                games = games[:self.limit]

            games_to_process = games.count()

            # ИСПРАВЛЕНИЕ: Рассчитываем сколько игр нужно обработать с учетом уже обработанных
            if self.force_restart:
                estimated_new_games = games_to_process
            else:
                # При обычном режиме обрабатываем все игры, кроме уже обработанных
                estimated_new_games = max(0, games_to_process - already_processed)

            if estimated_new_games == 0:
                # Выводим в терминал
                if self.original_stdout:
                    self.original_stdout.write("✅ Нет новых игр для обработки\n")
                    self.original_stdout.flush()
                # Выводим в файл
                if self.output_file:
                    self.stdout.write("✅ Нет новых игр для обработки")
                return

            # Выводим информацию о начале в терминал
            if self.original_stdout:
                mode = "ключевых слов" if self.keywords else "критериев"
                self.original_stdout.write(f"\n🔍 Анализируем {estimated_new_games} игр на наличие {mode}...\n")
                self.original_stdout.write(f"📊 Всего игр в базе: {total_games}\n")
                if already_processed > 0 and not self.force_restart:
                    self.original_stdout.write(f"📊 Уже обработано: {already_processed}\n")
                    self.original_stdout.write(f"📊 Осталось обработать: {estimated_new_games}\n")

                if not self.no_progress and estimated_new_games > 1:
                    self.original_stdout.write("📊 Прогресс:\n")
                self.original_stdout.flush()

            # Выводим информацию в файл
            if self.output_file:
                self.stdout.write("\n" + "=" * 60)
                self.stdout.write(f"🔍 АНАЛИЗ ИГР (всего в базе: {total_games})")
                self.stdout.write("=" * 60)
                self.stdout.write(f"📊 Будут обработаны: {estimated_new_games} игр")
                if already_processed > 0 and not self.force_restart:
                    self.stdout.write(f"📊 Уже обработано ранее: {already_processed}")
                    self.stdout.write(f"📊 Осталось обработать: {estimated_new_games}")
                self.stdout.write("=" * 60)
                self.stdout.write("")

            # Инициализируем статистику
            self._init_stats()

            # ИСПРАВЛЕНИЕ: Инициализируем прогресс-бар с estimated_new_games
            if not self.no_progress and estimated_new_games > 1:
                self._clean_output_before_progress_bar()
                self.progress_bar = self._init_progress_bar(estimated_new_games)
            else:
                self.progress_bar = None

            start_time = time.time()

            # Обрабатываем игры
            try:
                processed_in_this_run = 0
                skipped_because_already_processed = 0

                for game in games.iterator(chunk_size=self.batch_size):
                    # Проверяем limit
                    if self.limit and processed_in_this_run >= self.limit:
                        break

                    # Пропускаем уже обработанные (если не force-restart)
                    if not self.force_restart and self.state_manager.is_game_processed(game.id):
                        skipped_because_already_processed += 1
                        continue

                    self._process_single_game_in_batch(game)
                    processed_in_this_run += 1

                    # Периодическое сохранение состояния
                    if processed_in_this_run % 500 == 0:
                        self.state_manager.save_state(self.stats['processed'])

                        # ИСПРАВЛЕНИЕ: Периодически обновляем батч в БД (тихо)
                        if self.update_game and self.batch_updater and len(self.batch_updater.games_to_update) > 0:
                            try:
                                remaining_updates = self.batch_updater.flush()
                                self.stats['updated'] += remaining_updates
                            except Exception:
                                pass

                # Выводим информацию о пропусках (только если есть что показать)
                if skipped_because_already_processed > 0 and not self.force_restart:
                    if self.original_stdout:
                        self.original_stdout.write(
                            f"\n📊 Пропущено уже обработанных игр: {skipped_because_already_processed}\n")
                        self.original_stdout.flush()
                    if self.output_file:
                        self.output_file.write(
                            f"\n📊 Пропущено уже обработанных игр: {skipped_because_already_processed}\n")
                        self.output_file.flush()

                # ИСПРАВЛЕНИЕ: ПРИНУДИТЕЛЬНО обновляем оставшиеся игры (тихо)
                if self.update_game and self.batch_updater:
                    remaining_updates = self.batch_updater.flush()
                    self.stats['updated'] += remaining_updates

                # Финальное сохранение состояния
                self.state_manager.save_state(self.stats['processed'])

                # Завершаем прогресс-бар
                if self.progress_bar:
                    self.progress_bar.finish()

                # Выводим статистику
                self.stats['execution_time'] = time.time() - start_time
                self._display_final_statistics(self.stats, already_processed, total_games)

            except KeyboardInterrupt:
                self._handle_batch_interrupt(start_time, already_processed)

        except Exception as e:
            self.stderr.write(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА В МАССОВОМ АНАЛИЗЕ: {e}")
            import traceback
            traceback.print_exc(file=self.stderr._out)
            raise

    def _display_final_statistics(self, stats: Dict[str, Any], already_processed: int, total_games: int):
        """Выводит финальную статистику в терминал и файл"""
        # 1. В файл - полная статистика через formatter
        if self.output_file and not self.output_file.closed:
            try:
                self.output_file.write("\n" + "=" * 60 + "\n")

                if self.keywords:
                    self.output_file.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КЛЮЧЕВЫЕ СЛОВА)\n")
                else:
                    self.output_file.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КРИТЕРИИ)\n")

                self.output_file.write("=" * 60 + "\n")

                # Показываем пропущенные ранее обработанные игры
                if already_processed > 0:
                    self.output_file.write(f"⏭️ Пропущено ранее обработанных игр: {already_processed}\n")

                if self.keywords:
                    processed_count = stats.get('keywords_processed', stats.get('processed', 0))
                    self.output_file.write(f"🔄 Обработано новых игр: {processed_count}\n")
                    self.output_file.write(f"🎯 Игр с найденными ключ. словами: {stats.get('keywords_found', 0)}\n")
                    self.output_file.write(f"📈 Всего ключевых слов найдено: {stats.get('keywords_count', 0)}\n")

                    if stats.get('keywords_not_found', 0) > 0:
                        self.output_file.write(f"⚡ Игр без ключевых слов: {stats['keywords_not_found']}\n")
                else:
                    self.output_file.write(f"🔄 Обработано новых игр: {stats.get('processed', 0)}\n")
                    self.output_file.write(f"🎯 Игр с найденными критериями: {stats.get('found_count', 0)}\n")
                    self.output_file.write(f"📈 Всего критериев найдено: {stats.get('total_criteria_found', 0)}\n")

                    if stats.get('not_found_count', 0) > 0:
                        self.output_file.write(f"⚡ Игр без критериев: {stats['not_found_count']}\n")

                total_skipped = stats['skipped_no_text'] + stats.get('skipped_short_text', 0) + (
                    stats['keywords_not_found'] if self.keywords else stats['not_found_count']
                )

                self.output_file.write(f"⏭️ Всего пропущено игр: {total_skipped}\n")
                self.output_file.write(f"⏭️ Игр без текста: {stats['skipped_no_text']}\n")

                if 'skipped_short_text' in stats and stats['skipped_short_text'] > 0:
                    self.output_file.write(f"⏭️ Игр с коротким текстом: {stats['skipped_short_text']}\n")

                if 'skipped_cached' in stats and stats['skipped_cached'] > 0:
                    self.output_file.write(f"⏭️ Игр пропущено по кэшу: {stats['skipped_cached']}\n")

                if self.keywords and stats.get('keywords_not_found', 0) > 0:
                    self.output_file.write(f"⏭️ Игр без ключевых слов: {stats['keywords_not_found']}\n")
                elif not self.keywords and stats.get('not_found_count', 0) > 0:
                    self.output_file.write(f"⏭️ Игр без критериев: {stats['not_found_count']}\n")

                self.output_file.write(f"❌ Ошибок: {stats['errors']}\n")
                self.output_file.write(f"💾 Обновлено игр: {stats['updated']}\n")

                if stats['execution_time'] > 0:
                    # Рассчитываем скорость обработки
                    games_per_second = stats.get('processed', 0) / stats['execution_time'] if stats[
                                                                                                  'execution_time'] > 0 else 0
                    self.output_file.write(f"⏱️ Время выполнения: {stats['execution_time']:.1f} секунд\n")
                    self.output_file.write(f"⚡ Скорость обработки: {games_per_second:.1f} игр/секунду\n")

                    if stats.get('processed_with_text', 0) > 0:
                        text_games_per_second = stats['processed_with_text'] / stats['execution_time']
                        self.output_file.write(
                            f"⚡ Скорость (игры с текстом): {text_games_per_second:.1f} игр/секунду\n")

                # Общая информация о процессе
                self.output_file.write("-" * 40 + "\n")
                self.output_file.write(f"📊 Всего игр в базе: {total_games}\n")
                if already_processed > 0:
                    self.output_file.write(f"📊 Уже обработано ранее: {already_processed}\n")
                    remaining_games = total_games - (stats.get('processed', 0) + already_processed)
                    self.output_file.write(f"📊 Осталось обработать: {max(0, remaining_games)}\n")

                # Информация о режимах
                if self.exclude_existing:
                    self.output_file.write(f"📝 Режим: исключение существующих критериев\n")
                if self.combine_all_texts:
                    self.output_file.write(f"📝 Источник текста: объединение всех текстов\n")
                elif self.combine_texts:
                    self.output_file.write(f"📝 Источник текста: объединение IGDB текстов\n")

                self.output_file.write(f"📏 Минимальная длина текста: {self.min_text_length}\n")

                if self.limit:
                    self.output_file.write(f"🎯 Ограничение: {self.limit} игр\n")
                if self.offset:
                    self.output_file.write(f"📌 Смещение: {self.offset} игр\n")

                self.output_file.write("=" * 60 + "\n")
                self.output_file.write("✅ Анализ успешно завершен\n")
                self.output_file.write("=" * 60 + "\n")
                self.output_file.flush()

            except Exception as e:
                # Если ошибка записи в файл, выводим в терминал
                if self.original_stderr:
                    self.original_stderr.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")
                    self.original_stderr.flush()

        # 2. В терминал - краткая статистика (только если есть оригинальный stdout)
        if self.original_stdout:
            try:
                # Очищаем строку перед выводом статистики
                self.original_stdout.write("\r" + " " * 150 + "\r")

                self.original_stdout.write("\n" + "=" * 60 + "\n")
                self.original_stdout.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
                self.original_stdout.write("=" * 60 + "\n")

                if self.keywords:
                    self.original_stdout.write(f"🔄 Обработано игр: {stats.get('processed', 0)}\n")
                    self.original_stdout.write(f"🎯 Игр с ключевыми словами: {stats.get('keywords_found', 0)}\n")
                    self.original_stdout.write(f"📈 Всего ключевых слов: {stats.get('keywords_count', 0)}\n")
                else:
                    self.original_stdout.write(f"🔄 Обработано игр: {stats.get('processed', 0)}\n")
                    self.original_stdout.write(f"🎯 Игр с критериями: {stats.get('found_count', 0)}\n")
                    self.original_stdout.write(f"📈 Всего критериев: {stats.get('total_criteria_found', 0)}\n")

                # Показываем пропуски
                if stats.get('skipped_no_text', 0) > 0:
                    self.original_stdout.write(f"⏭️ Пропущено (нет текста): {stats.get('skipped_no_text', 0)}\n")
                if stats.get('skipped_short_text', 0) > 0:
                    self.original_stdout.write(f"⏭️ Пропущено (короткий текст): {stats.get('skipped_short_text', 0)}\n")
                if stats.get('skipped_cached', 0) > 0:
                    self.original_stdout.write(f"⏭️ Пропущено (кэш): {stats.get('skipped_cached', 0)}\n")

                # Показываем "не найдено" в зависимости от режима
                if self.keywords and stats.get('keywords_not_found', 0) > 0:
                    self.original_stdout.write(f"⚡ Игр без ключевых слов: {stats.get('keywords_not_found', 0)}\n")
                elif not self.keywords and stats.get('not_found_count', 0) > 0:
                    self.original_stdout.write(f"⚡ Игр без критериев: {stats.get('not_found_count', 0)}\n")

                self.original_stdout.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")
                self.original_stdout.write(f"💾 Обновлено игр: {stats.get('updated', 0)}\n")

                if stats.get('execution_time', 0) > 0:
                    # Рассчитываем и выводим скорость
                    games_per_second = stats.get('processed', 0) / stats['execution_time'] if stats[
                                                                                                  'execution_time'] > 0 else 0
                    self.original_stdout.write(f"⏱️ Время: {stats['execution_time']:.1f} секунд\n")
                    self.original_stdout.write(f"⚡ Скорость: {games_per_second:.1f} игр/сек\n")

                    # Если есть игры с текстом, показываем отдельную скорость
                    if stats.get('processed_with_text', 0) > 0:
                        text_games_per_second = stats['processed_with_text'] / stats['execution_time']
                        self.original_stdout.write(f"⚡ Скорость (с текстом): {text_games_per_second:.1f} игр/сек\n")

                self.original_stdout.write("-" * 40 + "\n")

                # Показываем общую информацию
                self.original_stdout.write(f"📊 Всего в базе: {total_games} игр\n")
                if already_processed > 0:
                    remaining_games = total_games - (stats.get('processed', 0) + already_processed)
                    self.original_stdout.write(f"📊 Осталось: {max(0, remaining_games)} игр\n")

                # Информация о режиме
                if self.exclude_existing:
                    self.original_stdout.write(f"📝 Режим: исключение существующих\n")
                if self.force_restart:
                    self.original_stdout.write(f"📝 Режим: принудительный перезапуск\n")

                self.original_stdout.write("=" * 60 + "\n")

                if self.output_path:
                    self.original_stdout.write(f"✅ Результаты сохранены в: {self.output_path}\n")
                else:
                    self.original_stdout.write("✅ Анализ завершен\n")

                self.original_stdout.write("=" * 60 + "\n")
                self.original_stdout.flush()

            except Exception as e:
                # Если ошибка вывода в терминал, пытаемся вывести в stderr
                if self.original_stderr:
                    self.original_stderr.write(f"⚠️ Ошибка вывода статистики в терминал: {e}\n")
                    self.original_stderr.write(f"📊 Статистика (кратко): обработано {stats.get('processed', 0)} игр\n")
                    self.original_stderr.flush()

        # 3. Дополнительно: если нет оригинального stdout (например, весь вывод в файл)
        # но есть stderr, выводим краткую статистику туда
        elif hasattr(self, 'original_stderr') and self.original_stderr:
            try:
                self.original_stderr.write("\n" + "=" * 60 + "\n")
                self.original_stderr.write("📊 СТАТИСТИКА АНАЛИЗА\n")
                self.original_stderr.write("=" * 60 + "\n")

                self.original_stderr.write(f"🔄 Обработано: {stats.get('processed', 0)} игр\n")
                self.original_stderr.write(f"🎯 Найдено критериев: {stats.get('total_criteria_found', 0)}\n")
                self.original_stderr.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")

                if stats.get('execution_time', 0) > 0:
                    self.original_stderr.write(f"⏱️ Время: {stats['execution_time']:.1f} секунд\n")

                self.original_stderr.write("=" * 60 + "\n")

                if self.output_path:
                    self.original_stderr.write(f"📁 Результаты в файле: {self.output_path}\n")

                self.original_stderr.flush()

            except Exception as e:
                # Последняя попытка - просто в sys.stderr
                import sys
                sys.stderr.write(f"\n📊 Анализ завершен. Обработано {stats.get('processed', 0)} игр\n")

    def _print_to_terminal(self, message: str, end: str = "\n"):
        """Печатает только в терминал"""
        if self.original_stdout:
            self.original_stdout.write(message + end)
            self.original_stdout.flush()

    def _print_to_file(self, message: str, end: str = "\n"):
        """Печатает только в файл"""
        if self.output_file and not self.output_file.closed:
            self.output_file.write(message + end)
            self.output_file.flush()

    def _print_both(self, message: str, end: str = "\n"):
        """Печатает и в терминал и в файл"""
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
        self.only_found = options.get('only_found', False)
        self.batch_size = options.get('batch_size', 1000)
        self.ignore_existing = options.get('ignore_existing', False)
        self.hide_skipped = options.get('hide_skipped', False)
        self.no_progress = options.get('no_progress', False)
        self.force_restart = options.get('force_restart', False)  # ⬅️ Это должно быть
        self.keywords = options.get('keywords', False)
        self.clear_cache = options.get('clear_cache', False)
        self.output_path = options.get('output')
        self.exclude_existing = options.get('exclude_existing', False)

        # Источники текста
        self.use_wiki = options.get('use_wiki', False)
        self.use_rawg = options.get('use_rawg', False)
        self.use_storyline = options.get('use_storyline', False)
        self.prefer_wiki = options.get('prefer_wiki', False)
        self.prefer_storyline = options.get('prefer_storyline', False)
        self.combine_texts = options.get('combine_texts', False)
        self.combine_all_texts = options.get('combine_all_texts', False)

        # ИСПРАВЛЕНИЕ: Устанавливаем force_restart в API если он уже создан
        if hasattr(self, 'api') and self.api:
            self.api.force_restart = self.force_restart

    def _init_components(self):
        """Инициализирует компоненты"""
        self.stdout.write("🔧 Инициализируем компоненты команды...")

        try:
            # 1. API анализатора
            self.stdout.write("   🔧 Загружаем GameAnalyzerAPI...")
            from games.analyze import GameAnalyzerAPI

            # ИСПРАВЛЕНИЕ: Всегда отключаем verbose в API чтобы не было лишнего вывода
            # который ломает прогресс-бар
            api_verbose = False  # ⬅️ ВСЕГДА False, даже если self.verbose=True

            self.api = GameAnalyzerAPI(verbose=api_verbose)
            self.stdout.write("   ✅ GameAnalyzerAPI инициализирован")

            # 2. Очищаем кеш если нужно
            if self.clear_cache:
                self.stdout.write("   🔧 Очищаем кеш анализатора...")
                self.api.clear_analysis_cache()
                self.stdout.write("   ✅ Кеш анализатора очищен")

            # 3. Менеджер состояния
            self.stdout.write("   🔧 Инициализируем StateManager...")
            from .state_manager import StateManager
            self.state_manager = StateManager(
                output_path=self.output_path,
                keywords_mode=self.keywords,
                force_restart=self.force_restart
            )
            self.stdout.write(f"   ✅ StateManager инициализирован (файл: {self.state_manager.state_file})")

            # 4. Батч-апдейтер
            self.stdout.write("   🔧 Инициализируем BatchUpdater...")
            from .batch_updater import BatchUpdater
            self.batch_updater = BatchUpdater()
            self.stdout.write("   ✅ BatchUpdater инициализирован")

            # 5. Форматировщик вывода
            self.stdout.write("   🔧 Инициализируем OutputFormatter...")
            from .output_formatter import OutputFormatter
            self.output_formatter = OutputFormatter(self)
            self.stdout.write("   ✅ OutputFormatter инициализирован")

            # 6. Подготовщик текста
            self.stdout.write("   🔧 Инициализируем TextPreparer...")
            from .text_preparer import TextPreparer
            self.text_preparer = TextPreparer(self)
            self.stdout.write(f"   ✅ TextPreparer инициализирован (режим: {self.text_preparer.text_source_mode})")

            # ИСПРАВЛЕНИЕ: Предупреждение о конфликте
            if not self.no_progress:
                self.stdout.write("⚠️  Прогресс-бар включен. Verbose вывод в API отключен для корректной работы.")
                if self.verbose:
                    self.stdout.write("⚠️  Ваш --verbose работает только на уровне команды, не в API.")

            self.stdout.write("✅ Все компоненты успешно инициализированы")

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
            # Создаем директорию если нужно
            directory = os.path.dirname(self.output_path)
            if directory:
                os.makedirs(directory, exist_ok=True)

            # Сохраняем оригинальные потоки
            self.original_stdout = self.stdout._out
            self.original_stderr = self.stderr._out

            # Открываем файл
            self.output_file = open(self.output_path, 'w', encoding='utf-8')

            # Перенаправляем вывод stdout в файл
            self.stdout._out = self.output_file
            # stderr тоже в файл
            self.stderr._out = self.output_file

            # Выводим в терминал информацию о файле
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
        """Выводит сводку опций"""
        # В файл - полная информация
        if self.output_file:
            self.stdout.write("=" * 60)
            self.stdout.write("🎮 НАСТРОЙКИ АНАЛИЗА ИГР")
            self.stdout.write("=" * 60)
            self.stdout.write(f"📊 Режим анализа: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
            self.stdout.write(f"🔄 Режим обновления: {'✅ ВКЛ' if self.update_game else '❌ ВЫКЛ'}")
            self.stdout.write(f"🔍 Игнорировать существующие: {'✅ ВКЛ' if self.ignore_existing else '❌ ВЫКЛ'}")
            self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ВКЛ' if self.hide_skipped else '❌ ВЫКЛ'}")
            self.stdout.write(f"📏 Минимальная длина текста: {self.min_text_length}")
            self.stdout.write(f"🗣️ Подробный вывод: {'✅ ВКЛ' if self.verbose else '❌ ВЫКЛ'}")
            self.stdout.write(f"🎯 Только с найденными: {'✅ ВКЛ' if self.only_found else '❌ ВЫКЛ'}")
            self.stdout.write(f"📚 Источник текста: {self.text_preparer.get_source_description()}")
            self.stdout.write(f"📦 Размер батча: {self.batch_size}")
            # ИСПРАВЛЕНИЕ: Добавляем информацию о прогресс-баре
            self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}")
            if not self.no_progress:
                self.stdout.write(f"⚠️  Примечание: С прогресс-баром отключен подробный вывод во время обработки")
            self.stdout.write("=" * 60)
            self.stdout.write("")

        # В терминал - только краткая информация
        if self.original_stdout:
            self.original_stdout.write(f"🎮 Анализ игр запущен\n")
            self.original_stdout.write(f"📊 Режим: {'КЛЮЧЕВЫЕ СЛОВА' if self.keywords else 'КРИТЕРИИ'}\n")
            if self.output_path:
                self.original_stdout.write(f"📁 Результаты в файле: {self.output_path}\n")
            # ИСПРАВЛЕНИЕ: Предупреждение о прогрессе
            if not self.no_progress:
                self.original_stdout.write(f"⚠️  Режим: С ПРОГРЕСС-БАРОМ (детали в файле)\n")
            self.original_stdout.write("=" * 60 + "\n")
            self.original_stdout.flush()

    def _process_command(self):
        """Обрабатывает команду в зависимости от аргументов"""
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

        if self.game_id:
            self.stdout.write(f"🔍 Выбран режим: Анализ одной игры по ID")
            self._analyze_single_game_by_id(self.game_id)
        elif self.game_name:
            self.stdout.write(f"🔍 Выбран режим: Поиск игр по названию")
            self._analyze_games_by_name(self.game_name)
        elif self.description:
            self.stdout.write("🔍 Выбран режим: Анализ произвольного текста")
            self._analyze_description(self.description)
        else:
            self.stdout.write("🔍 Выбран режим: Массовый анализ всех игр")
            self._analyze_all_games()

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write("🏁 ОБРАБОТКА КОМАНДЫ ЗАВЕРШЕНА")
        self.stdout.write("=" * 60)

    def _analyze_single_game_by_id(self, game_id: int):
        """Анализирует одну игру по ID"""
        try:
            game = Game.objects.get(id=game_id)
            self.output_formatter.print_game_header(game, self.keywords)

            # Получаем текст
            text = self.text_preparer.prepare_text(game)

            if not text:
                self.stdout.write("❌ У игры нет текста для анализа")
                return

            # Проверяем длину текста
            if len(text) < self.min_text_length:
                self.stdout.write(f"⏭️ Пропущено (текст слишком короткий: {len(text)} < {self.min_text_length})")
                return

            # Анализируем
            result = self.api.analyze_game_text(
                text=text,
                game_id=game_id,
                analyze_keywords=self.keywords,
                existing_game=game if not self.ignore_existing else None,
                detailed_patterns=self.verbose
            )

            # Отображаем результаты
            self.output_formatter.print_game_results(game, result, self.keywords)

            # Обновляем базу если нужно
            if self.update_game and result['has_results']:
                update_result = self.api.update_game_with_results(
                    game_id=game_id,
                    results=result['results'],
                    is_keywords=self.keywords
                )

                if update_result['success'] and update_result['updated']:
                    self.stdout.write("💾 Данные обновлены в базе")

        except Game.DoesNotExist:
            self.stderr.write(f"❌ Игра с ID {game_id} не найдена")

    def _get_base_query(self) -> QuerySet:
        """Возвращает базовый QuerySet"""
        return Game.objects.all().order_by('id')

    def _init_progress_bar(self, total_games: int):
        """Инициализирует прогресс-бар с выводом в терминал"""
        if self.no_progress or total_games <= 1:
            return None

        # Автоматически рассчитываем ширину поля для статистики
        max_possible = max(total_games, 99999 if total_games > 99999 else total_games)
        stat_width = max(4, len(str(max_possible)))

        # Определяем поток для вывода прогресс-бара
        terminal_stream = None
        if self.original_stderr:
            terminal_stream = self.original_stderr
        else:
            import sys
            terminal_stream = sys.stderr

        # ИСПРАВЛЕНИЕ: Очищаем предыдущие сообщения перед созданием прогресс-бара
        if terminal_stream:
            terminal_stream.write("\r" + " " * 150 + "\r")
            terminal_stream.flush()

        # Создаем прогресс-бар
        progress_bar = ProgressBar(
            total=total_games,
            desc="Анализ игр",
            bar_length=30,
            update_interval=0.1,
            stat_width=stat_width,
            emoji_spacing=1,
            terminal_stream=terminal_stream
        )

        # Инициализируем статистику прогресс-бара
        progress_bar.update_stats({
            'found_count': 0,
            'total_criteria_found': 0,
            'skipped_total': 0,
            'errors': 0,
            'updated': 0,
        })

        return progress_bar

    def _handle_interrupt(self):
        """Обрабатывает прерывание"""
        # Восстанавливаем потоки
        self._restore_output_streams()

        # Выводим в терминал
        if self.original_stdout:
            self.original_stdout.write("\n⏹️ Обработка прервана пользователем\n")

    def _handle_error(self, e):
        """Обрабатывает ошибку"""
        import traceback
        if self.output_file:
            self.stderr.write(f"❌ Неожиданная ошибка: {e}")
            traceback.print_exc()
        else:
            if self.original_stderr:
                self.original_stderr.write(f"❌ Неожиданная ошибка: {e}\n")
                traceback.print_exc(file=self.original_stderr)

    def _restore_output_streams(self):
        """Восстанавливает потоки вывода"""
        if self.output_file:
            try:
                self.output_file.close()
                if self.original_stdout:
                    self.stdout._out = self.original_stdout
                if self.original_stderr:
                    self.stderr._out = self.original_stderr
                if self.output_path:
                    self.stdout.write(f"\n✅ Результаты экспортированы в: {self.output_path}")
            except Exception as e:
                if self.original_stderr:
                    self.original_stderr.write(f"⚠️ Ошибка закрытия файла: {e}\n")

    def _cleanup(self):
        """Очистка ресурсов"""
        # Восстанавливаем потоки
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

        # Очищаем кеш API
        if self.api:
            self.api.clear_analysis_cache()

    def _analyze_games_by_name(self, game_name: str):
        """Анализирует игры по названию"""
        games = Game.objects.filter(name__icontains=game_name)

        if not games.exists():
            self.stderr.write(f"❌ Игры с названием содержащим '{game_name}' не найдены")
            return

        self.stdout.write(f"🔍 Найдено {games.count()} игр с названием содержащим '{game_name}'")

        for game in games:
            self._analyze_single_game_by_id(game.id)

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
            # Рассчитываем скорость
            games_per_second = stats['processed'] / stats['execution_time'] if stats['execution_time'] > 0 else 0
            self.stdout.write(f"⏱️ Время выполнения до прерывания: {stats['execution_time']:.1f} секунд")
            self.stdout.write(f"⚡ Скорость обработки: {games_per_second:.1f} игр/секунду")

            # Показываем оставшееся время
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