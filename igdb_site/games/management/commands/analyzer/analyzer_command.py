# games/management/commands/analyzer/analyzer_command.py
"""
Основной класс команды анализа с использованием нового API - ИСПРАВЛЕННАЯ ВЕРСИЯ
Использует только существующие методы API
"""

import sys
import os
import time
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

        # Всегда передаем ВСЮ статистику
        self.progress_bar.update_stats({
            'found_count': self.stats.get('found_games', 0),
            'total_criteria_found': self.stats.get('found_elements', 0),
            'skipped_total': self.stats.get('skipped_games', 0),
            'not_found_count': self.stats.get('empty_games', 0),
            'errors': self.stats.get('error_games', 0),
            'updated': self.stats.get('updated_games', 0),
            'in_batch': self.stats.get('in_batch', 0),
        })

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
        # Выводим в файл
        if self.output_file and not self.output_file.closed:
            self.stdout.write("\n" + "=" * 60)
            self.stdout.write("⏹️ ОБРАБОТКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ")
            self.stdout.write("=" * 60)

            # Сохраняем последний оффсет для продолжения
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

                # Показываем оставшееся время
                remaining_games = self.total_games_estimate - stats['processed'] if hasattr(self,
                                                                                            'total_games_estimate') else 0
                if remaining_games > 0 and games_per_second > 0:
                    remaining_time = remaining_games / games_per_second
                    self.stdout.write(
                        f"⏳ Осталось обработать примерно: {remaining_time:.1f} секунд ({remaining_games} игр)")

            self.stdout.write("=" * 60)
            self.stdout.flush()

        # Выводим в терминал короткое сообщение
        if self.original_stdout:
            # Уже вывели подробное сообщение в _handle_batch_interrupt
            pass

    def _reset_batch_after_update(self):
        """Сбрасывает состояние батча после обновления"""
        if self.batch_updater:
            # Сбрасываем счетчики батч-апдейтера
            self.batch_updater.total_games_added = 0
            # Очищаем games_to_update на всякий случай
            if hasattr(self.batch_updater, 'games_to_update'):
                self.batch_updater.games_to_update.clear()

    def _process_single_game_in_batch_with_criteria(self, game, checked_criteria, force_process=False):
        """Обрабатывает одну игру с учетом проверенных критериев (основная логика)"""
        # Получаем текст для анализа
        text = self.text_preparer.prepare_text(game)

        # Обновляем статистику
        self.stats['processed'] += 1

        # Проверяем наличие текста
        if not text:
            self._handle_game_without_text(game)
            return

        # Проверяем длину текста
        if len(text) < self.min_text_length:
            self._handle_short_text_game(game)
            return

        # Обрабатываем игру с текстом
        self._process_game_with_text(game, text, checked_criteria, force_process)

    def _handle_game_without_text(self, game):
        """Обрабатывает игру без текста"""
        self.stats['skipped_no_text'] += 1
        self.stats['skipped_total'] += 1

        # ОБНОВЛЯЕМ СТАТИСТИКУ ДЛЯ ПРОГРЕСС-БАРА
        self.stats['skipped_games'] += 1  # ⏭️

        self.state_manager.add_processed_game(game.id)

        # ВАЖНО: обновляем прогресс-бар для пропущенных игр
        if self.progress_bar:
            self.progress_bar.update(1)
            # ПЕРЕДАЕМ ОБНОВЛЕННУЮ СТАТИСТИКУ В ПРОГРЕСС-БАР
            self.progress_bar.update_stats({
                'skipped_total': self.stats['skipped_games'],
                'found_count': self.stats['found_games'],
                'total_criteria_found': self.stats['found_elements'],
                'not_found_count': self.stats['empty_games'],
                'errors': self.stats['error_games'],
                'updated': self.stats['updated_games'],
                'in_batch': self.stats['in_batch'],
            })

    def _handle_short_text_game(self, game):
        """Обрабатывает игру с коротким текстом"""
        self.stats['skipped_short_text'] += 1
        self.stats['skipped_total'] += 1

        # ОБНОВЛЯЕМ СТАТИСТИКУ ДЛЯ ПРОГРЕСС-БАРА
        self.stats['skipped_games'] += 1  # ⏭️

        self.state_manager.add_processed_game(game.id)

        # Обновляем прогресс-бар
        if self.progress_bar:
            self.progress_bar.update(1)
            # ПЕРЕДАЕМ ОБНОВЛЕННУЮ СТАТИСТИКУ В ПРОГРЕСС-БАР
            self.progress_bar.update_stats({
                'skipped_total': self.stats['skipped_games'],
                'found_count': self.stats['found_games'],
                'total_criteria_found': self.stats['found_elements'],
                'not_found_count': self.stats['empty_games'],
                'errors': self.stats['error_games'],
                'updated': self.stats['updated_games'],
                'in_batch': self.stats['in_batch'],
            })

    def _process_game_with_text(self, game, text, checked_criteria, force_process):
        """Обрабатывает игру с текстом"""
        self.stats['processed_with_text'] += 1

        try:
            # Определяем настройки для исключения существующих критериев
            # ИСПРАВЛЕНИЕ: используем параметр exclude_existing из командной строки
            exclude_existing = self.exclude_existing or (not self.ignore_existing and not force_process)

            # Отладочный вывод ТОЛЬКО при --verbose
            if self.verbose and self.original_stdout:
                self.original_stdout.write(f"\n🔍 Начинаем обработку игры {game.id}: {game.name}\n")
                self.original_stdout.write(f"📄 Текст получен, длина: {len(text)} символов\n")
                self.original_stdout.write(f"⚙️ Настройки: exclude_existing={exclude_existing}\n")
                self.original_stdout.flush()

            # Анализируем текст с поддержкой exclude_existing
            result = self._analyze_game_text(game, text, exclude_existing)

            if not result['success']:
                self._handle_analysis_error(game, result)
                return

            # Обрабатываем результаты анализа
            self._handle_analysis_results(game, result, force_process, exclude_existing)

            # Проверяем обновление батча
            self._check_batch_update()

        except Exception as e:
            self._handle_processing_error(game, e)

    def _analyze_game_text(self, game, text, exclude_existing):
        """Анализирует текст игры с помощью API с поддержкой exclude_existing"""
        # В зависимости от режима команды выбираем метод API
        if hasattr(self, 'comprehensive_mode') and self.comprehensive_mode:
            return self.api.analyze_game_text_comprehensive(
                text=text,
                game_id=game.id,
                existing_game=game,
                detailed_patterns=self.verbose,
                exclude_existing=exclude_existing
            )
        elif hasattr(self, 'combined_mode') and self.combined_mode:
            return self.api.analyze_game_text_combined(
                text=text,
                game_id=game.id,
                existing_game=game,
                detailed_patterns=self.verbose,
                exclude_existing=exclude_existing
            )
        else:
            # Стандартный анализ (force_analyze_game_text уже поддерживает exclude_existing)
            return self.api.force_analyze_game_text(
                text=text,
                game_id=game.id,
                analyze_keywords=self.keywords,
                existing_game=game,
                detailed_patterns=self.verbose,
                exclude_existing=exclude_existing
            )

    def _handle_analysis_error(self, game, result):
        """Обрабатывает ошибку анализа"""
        self.stats['errors'] += 1

        # ОБНОВЛЯЕМ СТАТИСТИКУ ДЛЯ ПРОГРЕСС-БАРА
        self.stats['error_games'] += 1  # ❌

        self.state_manager.add_processed_game(game.id)

        # Отладочный вывод ТОЛЬКО при --verbose
        if self.verbose and self.original_stdout:
            self.original_stdout.write(f"❌ Ошибка API: {result.get('error_message', 'Неизвестная ошибка')}\n")
            self.original_stdout.flush()

        # Обновляем прогресс-бар при ошибке
        if self.progress_bar:
            self.progress_bar.update(1)
            # ПЕРЕДАЕМ ОБНОВЛЕННУЮ СТАТИСТИКУ В ПРОГРЕСС-БАР
            if self.keywords:
                self.progress_bar.update_stats({
                    'errors': self.stats['error_games'],
                    'in_batch': self.stats.get('in_batch', 0),
                    'skipped_total': self.stats.get('skipped_games', 0),
                    'not_found_count': self.stats.get('empty_games', 0),
                    'updated': self.stats.get('updated_games', 0),
                    'found_count': self.stats.get('found_games', 0),
                    'total_criteria_found': self.stats.get('found_elements', 0),
                })
            else:
                self.progress_bar.update_stats({
                    'errors': self.stats['error_games'],
                    'in_batch': self.stats.get('in_batch', 0),
                    'skipped_total': self.stats.get('skipped_games', 0),
                    'not_found_count': self.stats.get('empty_games', 0),
                    'updated': self.stats.get('updated_games', 0),
                    'found_count': self.stats.get('found_games', 0),
                    'total_criteria_found': self.stats.get('found_elements', 0),
                })

    def _handle_analysis_results(self, game, result, force_process, exclude_existing):
        """Обрабатывает результаты анализа игры"""
        # ОБНОВЛЯЕМ ПРОГРЕСС ДЛЯ ЛЮБОЙ ОБРАБОТАННОЙ ИГРЫ
        if self.progress_bar:
            self.progress_bar.update(1)

        # Проверяем, есть ли РЕАЛЬНЫЕ НОВЫЕ элементы для добавления
        has_new_elements = False
        new_elements_count = 0

        if self.keywords:
            keywords_data = result['results'].get('keywords', {})
            items = keywords_data.get('items', [])

            if items:  # Если есть ключевые слова в результатах
                # Проверяем, сколько из них действительно новых
                from games.models import Keyword
                keyword_ids = [k['id'] for k in items]
                existing_game_ids = set(game.keywords.values_list('id', flat=True))
                new_ids = [kid for kid in keyword_ids if kid not in existing_game_ids]

                if new_ids:  # Есть новые ключевые слова
                    has_new_elements = True
                    new_elements_count = len(new_ids)
                    self.stats['found_games'] += 1  # 🎯 ТОЛЬКО если есть новые
                    self.stats['found_elements'] += new_elements_count  # 📈 Только новые

                    if self.verbose and self.original_stdout:
                        self.original_stdout.write(
                            f"📌 Игра {game.id}: {len(items)} ключевых слов найдено, {len(new_ids)} новых\n")
                else:  # Все ключевые слова уже есть у игры
                    has_new_elements = False
                    # ИГРА БЕЗ НОВЫХ КРИТЕРИЕВ - ЗАФИКСИРОВАНА СРАЗУ
                    self.stats['empty_games'] += 1  # 🔍

                    if self.verbose and self.original_stdout:
                        self.original_stdout.write(
                            f"ℹ️ Игра {game.id}: все {len(items)} ключевых слов уже есть у игры (зафиксирована)\n")
            else:
                # Вообще нет ключевых слов в результатах
                has_new_elements = False
                # ИГРА БЕЗ НОВЫХ КРИТЕРИЕВ - ЗАФИКСИРОВАНА СРАЗУ
                self.stats['empty_games'] += 1  # 🔍
        else:
            # Для обычных критериев
            total_found_elements = 0

            for key, data in result['results'].items():
                items = data.get('items', [])
                count = data.get('count', 0)

                if items and count > 0:
                    # Проверяем, какие из них новые
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

                    if new_ids:
                        has_new_elements = True
                        total_found_elements += count
                        new_elements_count += len(new_ids)

            if has_new_elements:
                self.stats['found_games'] += 1  # 🎯
                self.stats['found_elements'] += new_elements_count  # 📈
            elif total_found_elements > 0:  # Найдены элементы, но все уже есть
                # ИГРА БЕЗ НОВЫХ КРИТЕРИЕВ - ЗАФИКСИРОВАНА СРАЗУ
                self.stats['empty_games'] += 1  # 🔍
            elif not result['has_results']:  # Вообще ничего не найдено
                # ИГРА БЕЗ НОВЫХ КРИТЕРИЕВ - ЗАФИКСИРОВАНА СРАЗУ
                self.stats['empty_games'] += 1  # 🔍

        # Обновляем основную статистику
        if result['has_results']:
            found_count = result['summary'].get('found_count', 0)
            if self.keywords:
                self.stats['keywords_found'] += 1
                self.stats['keywords_count'] += found_count
            else:
                self.stats['found_count'] += 1
                self.stats['total_criteria_found'] += found_count

            # Обновляем список проверенных критериев
            self._update_checked_criteria_after_analysis(result)
        elif not has_new_elements and result.get('has_results', False):
            # Игра была проанализирована, но ничего не найдено
            # ИГРА БЕЗ НОВЫХ КРИТЕРИЕВ - ЗАФИКСИРОВАНА СРАЗУ
            self.stats['empty_games'] += 1  # 🔍

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

        # Добавляем в батч для обновления если нужно (только если есть новые элементы)
        if self.update_game and has_new_elements:
            added_count = self._add_to_batch_if_needed(game, result)

            # ОБНОВЛЯЕМ СТАТИСТИКУ БАТЧА
            if self.batch_updater:
                games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                    'games_to_update') else 0
                self.stats['in_batch'] = games_in_batch  # 📦

        # Всегда добавляем в StateManager
        self.state_manager.add_processed_game(game.id)

        # ОБНОВЛЯЕМ ВСЮ СТАТИСТИКУ В ПРОГРЕСС-БАРЕ СРАЗУ ПОСЛЕ ОБРАБОТКИ ИГРЫ
        if self.progress_bar:
            # ВСЕГДА передаем ВСЮ статистику
            if self.keywords:
                self.progress_bar.update_stats({
                    'found_count': self.stats['found_games'],  # 🎯
                    'total_criteria_found': self.stats['found_elements'],  # 📈
                    'skipped_total': self.stats['skipped_games'],  # ⏭️
                    'not_found_count': self.stats['empty_games'],  # 🔍
                    'errors': self.stats['error_games'],  # ❌
                    'updated': self.stats['updated_games'],  # 💾
                    'in_batch': self.stats['in_batch'],  # 📦
                })
            else:
                self.progress_bar.update_stats({
                    'found_count': self.stats['found_games'],  # 🎯
                    'total_criteria_found': self.stats['found_elements'],  # 📈
                    'skipped_total': self.stats['skipped_games'],  # ⏭️
                    'not_found_count': self.stats['empty_games'],  # 🔍
                    'errors': self.stats['error_games'],  # ❌
                    'updated': self.stats['updated_games'],  # 💾
                    'in_batch': self.stats['in_batch'],  # 📦
                })

    def _add_to_batch_if_needed(self, game, result):
        """Добавляет игру в батч для обновления если нужно"""
        if not self.update_game:
            return 0

        try:
            added_count = 0

            # ДЛЯ КЛЮЧЕВЫХ СЛОВ: проверяем есть ли реальные ключевые слова
            if self.keywords:
                keywords_data = result.get('results', {}).get('keywords', {})
                items = keywords_data.get('items', [])

                if not items:
                    # Нет ключевых слов - не добавляем в батч
                    return 0

                # Добавляем в батч
                added = self.batch_updater.add_game_for_update(
                    game_id=game.id,
                    results=result['results'],
                    is_keywords=True
                )

                if added > 0:
                    added_count = len(items)
            else:
                # Для обычных критериев проверяем все типы
                total_found_elements = 0
                has_any_results = False

                for key, data in result.get('results', {}).items():
                    count = data.get('count', 0)
                    items = data.get('items', [])
                    if count > 0 and items:
                        total_found_elements += count
                        has_any_results = True

                if has_any_results:
                    # Добавляем в батч только если есть результаты
                    added = self.batch_updater.add_game_for_update(
                        game_id=game.id,
                        results=result['results'],
                        is_keywords=False
                    )

                    if added > 0:
                        added_count = total_found_elements

            # ОБНОВЛЯЕМ СТАТИСТИКУ БАТЧА ДЛЯ ПРОГРЕСС-БАРА
            if self.batch_updater:
                games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                    'games_to_update') else 0
                self.stats['in_batch'] = games_in_batch  # 📦

            return added_count

        except Exception as e:
            if self.verbose:
                self.stderr.write(f"⚠️ Ошибка при добавлении в батч: {e}")
            return 0

    def _check_batch_update(self):
        """Проверяет и обновляет батч если накопилось много игр"""
        # Сохраняем состояние каждые 500 игр
        if self.stats['processed'] % 500 == 0:
            try:
                self.state_manager.save_state(self.stats['processed'])
            except Exception:
                pass

        # Проверяем и обновляем батч каждые 100 обработанных игр
        if self.update_game and self.batch_updater and not getattr(self, '_in_batch_update', False):
            games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                'games_to_update') else 0
            if games_in_batch >= 100:  # Обновляем каждые 100 игр в батче
                self._in_batch_update = True
                try:
                    remaining_updates = self.batch_updater.flush()
                    if remaining_updates > 0:
                        self.stats['updated'] += remaining_updates
                        # Обновляем статистику в прогресс-баре
                        if self.progress_bar:
                            self.progress_bar.update_stats({'updated': self.stats['updated']})
                            self._update_progress_bar()

                        # Выводим отладочное сообщение если verbose
                        if self.verbose and self.original_stdout:
                            self.original_stdout.write(f"💾 Обновлен батч: {remaining_updates} игр\n")
                            self.original_stdout.flush()
                finally:
                    self._in_batch_update = False

    def _handle_processing_error(self, game, error):
        """Обрабатывает ошибку при обработке игры"""
        self.stats['errors'] += 1

        # Отладочный вывод при ошибке ТОЛЬКО при --verbose
        if self.verbose and self.original_stderr:
            import traceback
            self.original_stderr.write(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА при обработке игры {game.id}:\n")
            self.original_stderr.write(f"Ошибка: {error}\n")
            traceback.print_exc(file=self.original_stderr)
            self.original_stderr.flush()

        # Всегда добавляем в StateManager даже при ошибке
        try:
            self.state_manager.add_processed_game(game.id)
        except:
            pass

        # Обновляем прогресс-бар даже при ошибке
        if self.progress_bar:
            self._update_progress_bar()

    def _initialize_criteria_tracking(self):
        """Инициализирует отслеживание проверенных критериев"""
        if not self.state_manager:
            return set(), set()

        # Загружаем существующие проверенные критерии
        checked_criteria = self.state_manager.get_checked_criteria()

        if self.force_restart:
            # Принудительный перезапуск - очищаем всё
            self.state_manager.clear_checked_criteria()
            checked_criteria = set()
            self.state_manager.processed_games.clear()
            self.state_manager.reset_state()
            print(f"♻️ Принудительный перезапуск: очищены проверенные критерии и обработанные игры", file=sys.stderr)

        # Получаем текущие критерии из системы
        if self.keywords:
            from games.models import Keyword
            try:
                all_criteria = set(str(k.id) for k in Keyword.objects.all())
            except Exception as e:
                # Если таблица не существует или есть другие ошибки
                print(f"⚠️ Ошибка получения ключевых слов: {e}", file=sys.stderr)
                all_criteria = set()
        else:
            all_criteria = set()
            try:
                from games.models import Genre, Theme, PlayerPerspective, GameMode
                all_criteria.update(str(g.id) for g in Genre.objects.all())
                all_criteria.update(str(t.id) for t in Theme.objects.all())
                all_criteria.update(str(p.id) for p in PlayerPerspective.objects.all())
                all_criteria.update(str(m.id) for m in GameMode.objects.all())
            except Exception as e:
                print(f"⚠️ Ошибка получения критериев: {e}", file=sys.stderr)

        # Определяем новые критерии
        new_criteria = all_criteria - checked_criteria

        if new_criteria and len(new_criteria) > 0:
            # ОБНАРУЖЕНЫ НОВЫЕ КРИТЕРИИ
            if self.original_stdout:
                self.original_stdout.write(f"\n🎯 ОБНАРУЖЕНО {len(new_criteria)} НОВЫХ КРИТЕРИЕВ ДЛЯ ПРОВЕРКИ!\n")
                self.original_stdout.write("ℹ️ Новые критерии будут добавлены к проверенным\n")
                self.original_stdout.flush()

            if self.output_file:
                self.stdout.write(f"\n🎯 ОБНАРУЖЕНО {len(new_criteria)} НОВЫХ КРИТЕРИЕВ ДЛЯ ПРОВЕРКИ!")
                self.stdout.write("ℹ️ Новые критерии будут добавлены к проверенным")

            # Добавляем новые критерии в проверенные
            self.state_manager.add_checked_criteria(list(new_criteria))

            # Устанавливаем флаг, что обнаружены новые критерии
            self._new_criteria_detected = True

            # Сохраняем состояние с обновленными критериями
            try:
                processed_count = self.state_manager.get_processed_count()
                self.state_manager.save_state(processed_count)
            except Exception:
                pass

            # Возвращаем обновленные проверенные критерии (включая новые)
            updated_checked_criteria = self.state_manager.get_checked_criteria()
            return updated_checked_criteria, set()

        # Сбрасываем флаг если новых критериев нет
        self._new_criteria_detected = False

        return checked_criteria, new_criteria

    def _update_checked_criteria_after_analysis(self, result: Dict[str, Any]):
        """Обновляет список проверенных критериев после анализа игры"""
        if not self.state_manager or not result.get('has_results'):
            return

        try:
            # Собираем ID всех найденных критериев
            found_criteria_ids = []

            for key, data in result['results'].items():
                if data.get('count', 0) > 0:
                    for item in data.get('items', []):
                        if 'id' in item:
                            found_criteria_ids.append(str(item['id']))

            # Добавляем в проверенные критерии
            if found_criteria_ids:
                self.state_manager.add_checked_criteria(found_criteria_ids)

                # Периодически сохраняем
                if self.stats.get('processed', 0) % 100 == 0:
                    self.state_manager.save_state(self.stats['processed'])

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
                # ДЛЯ КЛЮЧЕВЫХ СЛОВ: более мягкая логика
                # Не пропускаем игру только если у нее уже есть ключевые слова
                # и ВСЕ они проверены

                existing_keywords = game.keywords.all()
                if not existing_keywords.exists():
                    return False  # Нет ключевых слов - нужно проверить

                # Проверяем, все ли существующие ключевые слова уже проверены
                existing_ids = set(str(k.id) for k in existing_keywords)
                all_checked = existing_ids.issubset(checked_criteria)

                if all_checked and self.verbose:
                    if self.original_stdout:
                        self.original_stdout.write(f"⏭️ Игра {game_id} пропущена - все ключевые слова уже проверены\n")
                        self.original_stdout.flush()

                return all_checked
            else:
                # Для обычных критериев: проверяем по типам
                should_skip = True

                # Жанры
                existing_genres = game.genres.all()
                if existing_genres.exists():
                    genre_ids = set(str(g.id) for g in existing_genres)
                    if not genre_ids.issubset(checked_criteria):
                        should_skip = False

                # Темы
                existing_themes = game.themes.all()
                if existing_themes.exists():
                    theme_ids = set(str(t.id) for t in existing_themes)
                    if not theme_ids.issubset(checked_criteria):
                        should_skip = False

                # Перспективы
                existing_perspectives = game.player_perspectives.all()
                if existing_perspectives.exists():
                    perspective_ids = set(str(p.id) for p in existing_perspectives)
                    if not perspective_ids.issubset(checked_criteria):
                        should_skip = False

                # Режимы игры
                existing_modes = game.game_modes.all()
                if existing_modes.exists():
                    mode_ids = set(str(m.id) for m in existing_modes)
                    if not mode_ids.issubset(checked_criteria):
                        should_skip = False

                if should_skip and self.verbose:
                    if self.original_stdout:
                        self.original_stdout.write(f"⏭️ Игра {game_id} пропущена - все критерии уже проверены\n")
                        self.original_stdout.flush()

                return should_skip

        except Game.DoesNotExist:
            return False
        except Exception as e:
            if self.verbose:
                self.stderr.write(f"⚠️ Ошибка проверки критериев для игры {game_id}: {e}")
            return False

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
        """Обновляет прогресс-бар с полной статистикой по всем обработанным играм"""
        if not self.progress_bar:
            return

        # Считаем ВСЕ обработанные игры в этом запуске (включая пропущенные)
        total_processed_in_this_run = (
                self.stats['processed'] +  # Обработанные с текстом
                self.stats['skipped_no_text'] +  # Пропущенные без текста
                self.stats.get('skipped_short_text', 0) +  # Пропущенные с коротким текстом
                self.stats.get('skipped_cached', 0) +  # Пропущенные по кэшу
                self.stats.get('skipped_by_criteria', 0)  # Пропущенные по проверенным критериям
        )

        # ВАЖНО: Синхронизируем текущее значение прогресс-бара с реальным прогрессом
        if self.progress_bar.current < total_processed_in_this_run:
            increment = total_processed_in_this_run - self.progress_bar.current

            # Обновляем сам прогресс-бар
            self.progress_bar.update(increment)

            # Обновляем статистику
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
        """Инициализирует статистику для текущего запуска"""
        self.stats = {
            'processed': 0,  # Игры обработанные с текстом в этом запуске
            'processed_with_text': 0,
            'found_count': 0,  # Игры с найденными критериями в этом запуске
            'not_found_count': 0,  # Игры без критериев
            'total_criteria_found': 0,  # Всего критериев найдено в этом запуске
            'skipped_no_text': 0,  # Пропущенные без текста в этом запуске
            'skipped_short_text': 0,  # Пропущенные с коротким текстом
            'skipped_cached': 0,  # Пропущенные по кэшу
            'skipped_by_criteria': 0,  # Пропущенные по проверенным критериям
            'skipped_total': 0,  # ОБЩЕЕ количество реальных пропусков
            'games_with_new_criteria': 0,
            'errors': 0,
            'updated': 0,
            'displayed_count': 0,

            # Для режима ключевых слов
            'keywords_processed': 0,
            'keywords_found': 0,
            'keywords_count': 0,
            'keywords_not_found': 0,

            # ДЛЯ ПРОГРЕСС-БАРА (значки статистики) - НАЧИНАЕМ С 0
            'in_batch': 0,  # 📦 - Игры в батче для обновления
            'found_games': 0,  # 🎯 - Игры с найденными элементами
            'found_elements': 0,  # 📈 - Всего найдено элементов
            'skipped_games': 0,  # ⏭️ - Пропущенные игры
            'empty_games': 0,  # 🔍 - Игры без элементов
            'error_games': 0,  # ❌ - Ошибки
            'updated_games': 0,  # 💾 - Обновлено игр
        }

        # ИНИЦИАЛИЗИРУЕМ ПРОГРЕСС-БАР СО СТАТИСТИКОЙ
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
        """Основной обработчик команды с поддержкой очистки кэша при force-restart"""
        try:
            # Сохраняем опции
            self._store_options(options)

            # Инициализируем компоненты
            self._init_components()

            # Перемещаем курсор вниз для прогресс-баров только если это массовый анализ с прогресс-баром
            if not self.no_progress and not self.game_id and not self.game_name and not self.description:
                try:
                    import sys
                    # Перемещаемся на несколько строк вниз для прогресс-баров
                    sys.stderr.write("\n\n")  # Две пустые строки для прогресс-баров
                    sys.stderr.flush()
                except:
                    pass

            try:
                # Настраиваем вывод в файл
                if self.output_path:
                    self._setup_file_output()

                # ВЫВОДИМ ИНФОРМАЦИЮ О ОФФСЕТЕ ПОСЛЕ НАСТРОЙКИ ВЫВОДА
                if self.offset > 0:
                    # Выводим в терминал
                    if self.original_stdout:
                        self.original_stdout.write(f"\n📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}")
                        self.original_stdout.write(f"💾 Начинаем анализ с позиции {self.offset} в списке всех игр")
                        self.original_stdout.write("=" * 60)
                        self.original_stdout.flush()

                    # Выводим в файл
                    if self.output_file and not self.output_file.closed:
                        self.stdout.write(f"\n📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}")
                        self.stdout.write(f"💾 Начинаем анализ с позиции {self.offset} в списке всех игр")
                        self.stdout.write("=" * 60)

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
        """Обрабатывает прерывание в пакетной обработке"""
        # 1. Сначала сохраняем текущий батч
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

        # 2. Рассчитываем игры с ЗАФИКСИРОВАННЫМИ результатами
        # Игры без новых критериев считаются зафиксированными сразу
        games_without_new_criteria = self.stats['empty_games']  # 🔍 - игры без новых элементов

        # Игры с новыми критериями считаются зафиксированными только если сохранены
        games_with_criteria_and_saved = self.stats['updated_games']  # 💾 - сохраненные игры

        # Общее количество зафиксированных игр
        finalized_games = games_without_new_criteria + games_with_criteria_and_saved

        # 3. ВАЖНО: Учитываем текущий оффсет при расчете следующего оффсета
        # Текущий оффсет + зафиксированные игры в этом запуске
        next_offset = self.offset + finalized_games

        # 4. Сохраняем состояние
        try:
            total_processed = already_processed + self.stats['processed']
            self.state_manager.save_state(total_processed)

            # Также сохраняем оффсет для продолжения в отдельный файл
            self._save_offset_to_file(next_offset)
        except Exception as e:
            if self.verbose and self.original_stderr:
                self.original_stderr.write(f"⚠️ Не удалось сохранить состояние: {e}\n")
                self.original_stderr.flush()

        # 5. Выводим информацию в терминал
        if self.original_stdout:
            self.original_stdout.write("\n")
            self.original_stdout.write("⏹️ ОБРАБОТКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ\n")
            self.original_stdout.write("=" * 60 + "\n")

            # ВАЖНО: Показываем ИСХОДНЫЙ оффсет с которого начали этот запуск
            self.original_stdout.write(f"📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")

            if finalized_games > 0:
                self.original_stdout.write(f"📍 ЗАФИКСИРОВАНО РЕЗУЛЬТАТОВ В ЭТОМ ЗАПУСКЕ: {finalized_games}\n")
                self.original_stdout.write(f"   ↳ Игр без новых критериев: {games_without_new_criteria}\n")
                self.original_stdout.write(f"   ↳ Игр сохранено в БД: {games_with_criteria_and_saved}\n")
            else:
                self.original_stdout.write(f"📍 НЕТ ЗАФИКСИРОВАННЫХ РЕЗУЛЬТАТОВ В ЭТОМ ЗАПУСКЕ\n")

            # Игры, которые обработаны, но не зафиксированы
            total_processed_games = self.stats['processed'] + self.stats['skipped_no_text'] + self.stats.get(
                'skipped_short_text', 0)
            games_processed_but_not_finalized = total_processed_games - finalized_games

            if games_processed_but_not_finalized > 0:
                self.original_stdout.write(
                    f"⚠️  {games_processed_but_not_finalized} игр обработано, но не зафиксировано\n")
                self.original_stdout.write(f"   (игры с новыми критериями, которые не были сохранены в БД)\n")

            # ВАЖНО: Показываем следующий оффсет с учетом текущего
            self.original_stdout.write(f"\n📍 ОФФСЕТ ДЛЯ ПРОДОЛЖЕНИЯ: {next_offset}\n")

            # Если оффсет не изменился, объясняем почему
            if next_offset == self.offset:
                self.original_stdout.write(f"ℹ️  Оффсет не изменился, так как нет зафиксированных результатов\n")

            self.original_stdout.write(f"💾 КОМАНДА ДЛЯ ПРОДОЛЖЕНИЯ:\n")

            # ВАЖНО: В команде для продолжения используем next_offset
            base_command = self._get_current_command_string()
            # Убираем --offset из base_command если он там есть
            if base_command:
                # Фильтруем --offset и следующее за ним число
                parts = base_command.split()
                filtered_parts = []
                i = 0
                while i < len(parts):
                    if parts[i] == '--offset' and i + 1 < len(parts):
                        i += 2  # Пропускаем --offset и значение
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

        # 6. Выводим информацию в файл
        if self.output_file and not self.output_file.closed:
            try:
                self.output_file.write("\n" + "=" * 60 + "\n")
                self.output_file.write("⏹️ ОБРАБОТКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ\n")
                self.output_file.write("=" * 60 + "\n")

                # ВАЖНО: Показываем ИСХОДНЫЙ оффсет
                self.output_file.write(f"📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")

                # ВАЖНО: Показываем следующий оффсет с учетом текущего
                self.output_file.write(f"📍 ОФФСЕТ ДЛЯ ПРОДОЛЖЕНИЯ: {next_offset}\n")
                self.output_file.write(f"💾 ИСПОЛЬЗУЙТЕ: --offset {next_offset}\n")

                self.output_file.write("=" * 60 + "\n")
                self.output_file.flush()
            except Exception as e:
                if self.verbose and self.original_stderr:
                    self.original_stderr.write(f"⚠️ Не удалось записать в файл: {e}\n")
                    self.original_stderr.flush()

        # 7. Обновляем время выполнения
        self.stats['execution_time'] = time.time() - start_time

        # 8. Останавливаем прогресс-бар
        if self.progress_bar:
            self.progress_bar.set_enabled(False)

        # 9. Выводим финальную статистику
        self._display_interruption_statistics_with_offset(self.stats, already_processed, next_offset)

    def _analyze_all_games(self):
        """Анализирует все игры в базе данных с учетом проверенных критериев"""
        try:
            # Загружаем состояние
            already_processed = self.state_manager.load_state()

            # ВАЖНО: При force-restart НЕ загружаем сохраненный оффсет
            if self.offset == 0 and not self.force_restart:
                saved_offset = self._load_offset_from_file()
                if saved_offset is not None and saved_offset > 0:
                    self.offset = saved_offset
                    if self.original_stdout:
                        self.original_stdout.write(f"📍 ВОССТАНОВЛЕН ОФФСЕТ ИЗ ПРЕДЫДУЩЕГО ЗАПУСКА: {self.offset}\n")
                        self.original_stdout.flush()

            # Инициализируем отслеживание критериев
            checked_criteria, new_criteria = self._initialize_criteria_tracking()

            # Получаем игры с правильной сортировкой по ID
            games = Game.objects.all().order_by('id')
            total_games = games.count()

            # ВАЖНО: При force-restart игнорируем оффсет из предыдущих запусков
            # Используем только оффсет переданный в командной строке (скорее всего 0)
            if self.force_restart and self.offset == 0:
                # Явно показываем что начинаем с начала
                if self.original_stdout:
                    self.original_stdout.write(f"♻️ Принудительный перезапуск: начинаем с первой игры\n")
                    self.original_stdout.flush()

            # ВЫВОДИМ ИНФОРМАЦИЮ О ОФФСЕТЕ (если есть и это не force-restart)
            if self.offset > 0 and not self.force_restart:
                if self.original_stdout:
                    self.original_stdout.write(f"\n📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")
                    self.original_stdout.write(f"📍 Пропускаем первые {self.offset} игр по порядку ID")
                    self.original_stdout.flush()

                if self.output_file and not self.output_file.closed:
                    self.stdout.write(f"\n📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")
                    self.stdout.write(f"📍 Пропускаем первые {self.offset} игр по порядку ID")

            # Применяем оффсет и лимит
            if self.offset:
                games = games[self.offset:]  # Пропускаем первые N игр по порядку в списке

            if self.limit:
                games = games[:self.limit]

            games_to_process = games.count()

            # Определяем режим обработки
            should_process_all, estimated_new_games = self._determine_processing_mode(
                games_to_process, already_processed, new_criteria
            )

            # Проверяем, есть ли что обрабатывать
            if not self._should_continue_processing(estimated_new_games, new_criteria):
                return

            # Выводим информацию о начале анализа
            if self.original_stdout:
                self.original_stdout.write(f"\n📊 ВСЕГО ИГР В БАЗЕ: {total_games}\n")

                if self.offset > 0 and not self.force_restart:
                    self.original_stdout.write(f"📊 ИГР ПОСЛЕ ОФФСЕТА {self.offset}: {games_to_process}\n")

                mode = "ключевых слов" if self.keywords else "критериев"
                if should_process_all and new_criteria:
                    self.original_stdout.write(f"🎯 ПРИЧИНА: обнаружено {len(new_criteria)} новых критериев\n")
                    self.original_stdout.write(f"🎯 БУДУТ ПРОВЕРЕНЫ ВСЕ ИГРЫ (включая уже обработанные)\n")
                else:
                    self.original_stdout.write(f"🎯 РЕЖИМ: анализ {mode}\n")

                if already_processed > 0 and not self.force_restart and not should_process_all:
                    # ВАЖНО: Учитываем оффсет при расчете уже обработанных игр
                    games_already_processed_after_offset = max(0, already_processed - self.offset)
                    self.original_stdout.write(
                        f"📊 УЖЕ ОБРАБОТАНО РАНЕЕ (после оффсета): {games_already_processed_after_offset}")
                    self.original_stdout.write(f"📊 ОСТАЛОСЬ ОБРАБОТАТЬ: {estimated_new_games}")

                if checked_criteria:
                    self.original_stdout.write(f"📊 ПРОВЕРЕННЫХ КРИТЕРИЕВ: {len(checked_criteria)}")

                if new_criteria:
                    self.original_stdout.write(f"🎯 НОВЫХ КРИТЕРИЕВ: {len(new_criteria)}")

                if not self.no_progress and estimated_new_games > 1:
                    self.original_stdout.write("📊 ПРОГРЕСС:")

                self.original_stdout.flush()

            # Выводим в файл: всегда, независимо от verbose
            if self.output_file and not self.output_file.closed:
                self.output_file.write("\n" + "=" * 60 + "\n")
                self.output_file.write(f"🔍 АНАЛИЗ ИГР (всего в базе: {total_games})\n")
                self.output_file.write("=" * 60 + "\n")

                # ВАЖНО: При force-restart не показываем информацию об оффсете
                if self.offset > 0 and not self.force_restart:
                    self.output_file.write(f"📍 ИСХОДНЫЙ ОФФСЕТ: {self.offset}\n")
                    self.output_file.write(f"📍 ИГР ПОСЛЕ ОФФСЕТА: {games_to_process}\n")

                if should_process_all and new_criteria:
                    self.output_file.write(f"🎯 ОБНАРУЖЕНО {len(new_criteria)} НОВЫХ КРИТЕРИЕВ\n")
                    self.output_file.write(f"🎯 ПРОВЕРЯЕМ ВСЕ ИГРЫ (включая уже обработанные)\n")
                else:
                    mode = "ключевых слов" if self.keywords else "критериев"
                    self.output_file.write(f"🎯 РЕЖИМ: анализ {mode}\n")

                self.output_file.write("=" * 60 + "\n")
                self.output_file.write(f"📊 БУДУТ ОБРАБОТАНЫ: {estimated_new_games} игр\n")

                if already_processed > 0 and not self.force_restart and not should_process_all:
                    games_already_processed_after_offset = max(0, already_processed - self.offset)
                    self.output_file.write(
                        f"📊 Уже обработано ранее (после оффсета): {games_already_processed_after_offset}\n")
                    self.output_file.write(f"📊 Осталось обработать: {estimated_new_games}\n")

                if checked_criteria:
                    self.output_file.write(f"📊 Проверенных критериев: {len(checked_criteria)}\n")

                if new_criteria:
                    self.output_file.write(f"🎯 Новых критериев: {len(new_criteria)}\n")

                self.output_file.write("=" * 60 + "\n")
                self.output_file.write("\n")
                self.output_file.flush()

            # Инициализируем статистику
            self._init_stats()
            self.stats['skipped_by_criteria'] = 0
            self.stats['games_with_new_criteria'] = 0

            # Инициализируем прогресс-бар
            if not self.no_progress and estimated_new_games > 1:
                self._clean_output_before_progress_bar()
                self.progress_bar = self._init_progress_bar(estimated_new_games)
            else:
                self.progress_bar = None

            start_time = time.time()

            # Обрабатываем игры
            try:
                processing_stats = self._process_games_batch(
                    games, should_process_all, new_criteria, checked_criteria, start_time
                )

                # Финальное сохранение состояния
                total_processed_now = already_processed + self.stats['processed']
                self.state_manager.save_state(total_processed_now)

                # Обновляем оставшийся батч если нужно
                if self.update_game and self.batch_updater:
                    games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                        'games_to_update') else 0
                    if games_in_batch > 0:
                        remaining_updates = self.batch_updater.flush()
                        self.stats['updated'] += remaining_updates
                        # Восстанавливаем основной прогресс-бар
                        if self.progress_bar:
                            self._update_progress_bar()

                # Завершаем прогресс-бар
                if self.progress_bar:
                    self.progress_bar.finish()

                # Выводим статистику с учетом оффсета
                self.stats['execution_time'] = time.time() - start_time
                self._display_final_statistics_with_offset(self.stats, already_processed, total_games, self.offset)

            except KeyboardInterrupt:
                # При прерывании передаем начальный оффсет для правильного расчета
                self._handle_batch_interrupt(start_time, already_processed)

        except Exception as e:
            self.stderr.write(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА В МАССОВОМ АНАЛИЗЕ: {e}")
            import traceback
            traceback.print_exc(file=self.stderr._out)
            raise

    def _print_state_loaded_info(self, already_processed, checked_criteria, new_criteria):
        """Выводит информацию о загруженном состоянии"""
        # В терминал
        if self.original_stdout:
            mode = "ключевых слов" if self.keywords else "критериев"
            self.original_stdout.write(
                f"📖 Загружено состояние: {already_processed} ранее обработанных игр (режим: {mode})\n")

            # ТОЛЬКО новые критерии - проверенные уже выведены в _initialize_criteria_tracking
            if new_criteria and len(new_criteria) > 0:
                self.original_stdout.write(
                    f"🎯 Обнаружено {len(new_criteria)} новых критериев для проверки\n")

            self.original_stdout.flush()

        # В файл
        if self.output_file and not self.output_file.closed:
            mode = "ключевых слов" if self.keywords else "критериев"
            self.stdout.write(f"📖 Загружено состояние: {already_processed} ранее обработанных игр (режим: {mode})")

            # ТОЛЬКО новые критерии
            if new_criteria and len(new_criteria) > 0:
                self.stdout.write(f"🎯 Обнаружено {len(new_criteria)} новых критериев для проверки")

    def _determine_processing_mode(self, games_to_process, already_processed_after_offset, new_criteria):
        """Определяет режим обработки и количество игр для обработки"""
        # Если есть новые критерии или force_restart, нужно обработать ВСЕ игры
        if (new_criteria and len(new_criteria) > 0) or self.force_restart:
            # Есть новые критерии или принудительный перезапуск - обрабатываем все игры
            estimated_new_games = games_to_process
            should_process_all = True
        else:
            # Нет новых критерий и не force_restart - обычная логика
            if self.force_restart:
                estimated_new_games = games_to_process
                should_process_all = True
            else:
                # ВАЖНО: При обычном режиме НЕ вычитаем уже обработанные игры
                # Игры из оффсета просто пропускаются, но не считаются "уже обработанными"
                # Мы обрабатываем ВСЕ игры после оффсета
                estimated_new_games = games_to_process
                should_process_all = False

        return should_process_all, estimated_new_games

    def _should_continue_processing(self, estimated_new_games, new_criteria):
        """Проверяет, нужно ли продолжать обработку"""
        # Если force_restart включен, ВСЕГДА продолжаем обработку
        if self.force_restart:
            return True

        # Если есть новые критерии, ВСЕГДА продолжаем обработку
        if new_criteria and len(new_criteria) > 0:
            return True

        # Проверяем есть ли игры для обработки
        if estimated_new_games == 0:
            # Выводим в терминал
            if self.original_stdout:
                if self.offset > 0:
                    self.original_stdout.write(f"✅ Все игры после оффсета {self.offset} уже обработаны\n")
                else:
                    self.original_stdout.write("✅ Нет новых игр для обработки\n")
                self.original_stdout.flush()
            # Выводим в файл
            if self.output_file:
                if self.offset > 0:
                    self.stdout.write(f"✅ Все игры после оффсета {self.offset} уже обработаны")
                else:
                    self.stdout.write("✅ Нет новых игр для обработки")
            return False

        return True

    def _print_analysis_start_info(self, total_games, already_processed, checked_criteria,
                                   new_criteria, should_process_all, estimated_new_games):
        """Выводит информацию о начале анализа"""
        # Выводим в терминал (только при verbose или если нет прогресс-бара)
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

        # Выводим в файл: всегда, независимо от verbose
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

    def _process_games_batch(self, games, should_process_all, new_criteria, checked_criteria, start_time):
        """Обрабатывает батч игр - оптимизированная версия"""
        processed_in_this_run = 0
        skipped_because_already_processed = 0
        skipped_because_criteria_checked = 0
        processed_previously_processed_games = 0

        # Сбрасываем счетчик добавленных игр в батч-апдейтере
        if self.batch_updater:
            self.batch_updater.total_games_added = 0

        # Инициализируем статистику перед началом обработки
        self._init_stats()

        # Устанавливаем начальное состояние для прогресс-бара
        if self.progress_bar:
            self.progress_bar.current = 0

            # Инициализируем статистику прогресс-бара нулями
            self.progress_bar.update_stats({
                'found_count': 0,
                'total_criteria_found': 0,
                'skipped_total': 0,
                'not_found_count': 0,
                'errors': 0,
                'updated': 0,
                'in_batch': 0,
            })

            # Принудительно обновляем для отображения
            self.progress_bar.update(0)  # Запускаем отображение с 0

        # Оптимизация: используем bulk итерацию
        batch_counter = 0

        for game in games.iterator(chunk_size=self.batch_size):
            # Проверяем limit
            if self.limit and processed_in_this_run >= self.limit:
                break

            # ВАЖНО: для ключевых слов отключаем пропуск по проверенным критериям
            skip_by_criteria_enabled = not self.keywords  # Отключаем для ключевых слов

            # ВАЖНО: при force_restart отключаем все пропуски
            if self.force_restart:
                skip_by_criteria_enabled = False

            # ВАЖНО: Игры, которые были в оффсете, НЕ проверяем на is_game_processed
            # Они просто пропущены по оффсету, а не обработаны ранее командой
            # Проверяем только если это не оффсетные игры
            game_was_processed_before = False
            if not self.force_restart:
                # Только проверяем если игра действительно была обработана вашей командой
                # Оффсетные игры НЕ проверяем
                game_was_processed_before = self.state_manager.is_game_processed(game.id)

            if should_process_all or self.force_restart:
                # force_restart или есть новые критерии - ОБЯЗАТЕЛЬНО обрабатываем все игры
                if game_was_processed_before and not self.force_restart:
                    processed_previously_processed_games += 1

                # Обрабатываем игру
                self._process_single_game_in_batch_with_criteria(game, checked_criteria,
                                                                 should_process_all or self.force_restart)
                processed_in_this_run += 1

            elif not should_process_all:
                # Нет новых критерий - обычная логика
                if game_was_processed_before:
                    skipped_because_already_processed += 1
                    # НЕ увеличиваем skipped_total для уже обработанных ранее - это не реальный пропуск в этом запуске
                    self.state_manager.add_processed_game(game.id)

                    # ВАЖНО: обновляем прогресс-бар для пропущенных игр тоже
                    if self.progress_bar:
                        # Пропущенная игра тоже учитывается в прогрессе
                        self.progress_bar.update(1)
                        # Увеличиваем счетчик пропущенных игр
                        self.stats['skipped_games'] += 1  # ⏭️
                        # Обновляем статистику в прогресс-баре
                        self.progress_bar.update_stats({
                            'skipped_total': self.stats['skipped_games'],
                            'found_count': self.stats['found_games'],
                            'total_criteria_found': self.stats['found_elements'],
                            'not_found_count': self.stats['empty_games'],
                            'errors': self.stats['error_games'],
                            'updated': self.stats['updated_games'],
                            'in_batch': self.stats['in_batch'],
                        })

                    continue

                # Проверяем, можно ли пропустить игру на основе проверенных критериев
                if skip_by_criteria_enabled and checked_criteria and self._should_skip_game_based_on_criteria(game.id,
                                                                                                              checked_criteria):
                    skipped_because_criteria_checked += 1
                    self.stats['skipped_by_criteria'] += 1
                    self.stats['skipped_total'] += 1  # Увеличиваем общий счетчик пропущенных (реальный пропуск)
                    self.stats['skipped_games'] += 1  # ⏭️
                    self.state_manager.add_processed_game(game.id)

                    # ВАЖНО: обновляем прогресс-бар для пропущенных игр
                    if self.progress_bar:
                        # Пропущенная игра тоже учитывается в прогрессе
                        self.progress_bar.update(1)
                        # Обновляем статистику в прогресс-баре
                        self.progress_bar.update_stats({
                            'skipped_total': self.stats['skipped_games'],
                            'found_count': self.stats['found_games'],
                            'total_criteria_found': self.stats['found_elements'],
                            'not_found_count': self.stats['empty_games'],
                            'errors': self.stats['error_games'],
                            'updated': self.stats['updated_games'],
                            'in_batch': self.stats['in_batch'],
                        })

                    continue

                # Обрабатываем игру
                self._process_single_game_in_batch_with_criteria(game, checked_criteria, False)
                processed_in_this_run += 1

            batch_counter += 1

            # Оптимизация: проверяем батч каждые 25 игр (чаще для лучшей отзывчивости)
            if batch_counter % 25 == 0:
                self._check_and_update_batch()

            # Оптимизация: сохраняем состояние каждые 1000 игр
            if processed_in_this_run % 1000 == 0 and processed_in_this_run > 0:
                try:
                    self.state_manager.save_state(self.stats['processed'])
                except Exception:
                    pass

        # После завершения цикла обновляем оставшийся батч
        self._flush_remaining_batch()

        # Выводим статистику обработки
        self._print_batch_processing_stats(processed_in_this_run, processed_previously_processed_games,
                                           skipped_because_already_processed, skipped_because_criteria_checked,
                                           new_criteria)

        return {
            'processed_in_this_run': processed_in_this_run,
            'skipped_because_already_processed': skipped_because_already_processed,
            'skipped_because_criteria_checked': skipped_because_criteria_checked,
            'processed_previously_processed_games': processed_previously_processed_games,
        }

    def _process_single_game_with_strategy(self, game, should_process_all, new_criteria, checked_criteria,
                                           processed_in_this_run, skipped_because_already_processed,
                                           skipped_because_criteria_checked, processed_previously_processed_games):
        """Обрабатывает одну игру с учетом стратегии обработки"""
        # ВАЖНО: для ключевых слов отключаем пропуск по проверенным критериям
        skip_by_criteria_enabled = not self.keywords  # Отключаем для ключевых слов

        # ВАЖНО: при force_restart отключаем все пропуски
        if self.force_restart:
            skip_by_criteria_enabled = False

        # Основная логика обработки
        game_was_processed_before = not self.force_restart and self.state_manager.is_game_processed(game.id)

        if should_process_all or self.force_restart:
            # force_restart или есть новые критерии - ОБЯЗАТЕЛЬНО обрабатываем все игры
            if game_was_processed_before and not self.force_restart:
                processed_previously_processed_games += 1

            # Обрабатываем игру
            self._process_single_game_in_batch_with_criteria(game, checked_criteria,
                                                             should_process_all or self.force_restart)
            processed_in_this_run += 1

            # ВАЖНО: обновляем прогресс-бар после обработки игры
            if self.progress_bar:
                self._update_progress_bar()

        elif not should_process_all:
            # Нет новых критерий - обычная логика
            if game_was_processed_before:
                skipped_because_already_processed += 1
                # НЕ увеличиваем skipped_total для уже обработанных ранее - это не реальный пропуск в этом запуске
                self.state_manager.add_processed_game(game.id)

                # ВАЖНО: обновляем прогресс-бар для пропущенных игр тоже
                if self.progress_bar:
                    # Пропущенная игра тоже учитывается в прогрессе
                    self.progress_bar.update(1)
                    self._update_progress_bar()

                return {
                    'processed_in_this_run': processed_in_this_run,
                    'skipped_because_already_processed': skipped_because_already_processed,
                    'skipped_because_criteria_checked': skipped_because_criteria_checked,
                    'processed_previously_processed_games': processed_previously_processed_games,
                }

            # Проверяем, можно ли пропустить игру на основе проверенных критериев
            if skip_by_criteria_enabled and checked_criteria and self._should_skip_game_based_on_criteria(game.id,
                                                                                                          checked_criteria):
                skipped_because_criteria_checked += 1
                self.stats['skipped_by_criteria'] += 1
                self.stats['skipped_total'] += 1  # Увеличиваем общий счетчик пропущенных (реальный пропуск)
                self.state_manager.add_processed_game(game.id)

                # ВАЖНО: обновляем прогресс-бар для пропущенных игр
                if self.progress_bar:
                    # Пропущенная игра тоже учитывается в прогрессе
                    self.progress_bar.update(1)
                    self._update_progress_bar()

                return {
                    'processed_in_this_run': processed_in_this_run,
                    'skipped_because_already_processed': skipped_because_already_processed,
                    'skipped_because_criteria_checked': skipped_because_criteria_checked,
                    'processed_previously_processed_games': processed_previously_processed_games,
                }

            # Обрабатываем игру
            self._process_single_game_in_batch_with_criteria(game, checked_criteria, False)
            processed_in_this_run += 1

            # Обновляем прогресс-бар после обработки
            if self.progress_bar:
                self._update_progress_bar()

        return {
            'processed_in_this_run': processed_in_this_run,
            'skipped_because_already_processed': skipped_because_already_processed,
            'skipped_because_criteria_checked': skipped_because_criteria_checked,
            'processed_previously_processed_games': processed_previously_processed_games,
        }

    def _check_and_update_batch(self):
        """Проверяет и обновляет батч если накопилось много игр"""
        # Проверяем и обновляем батч каждые 100 обработанных игр
        if self.update_game and self.batch_updater and not getattr(self, '_in_batch_update', False):
            try:
                games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                    'games_to_update') else 0

                # ОБНОВЛЯЕМ СТАТИСТИКУ ИГР В БАТЧЕ
                self.stats['in_batch'] = games_in_batch  # 📦

                if games_in_batch >= 50:  # Обновляем каждые 50 игр в батче
                    self._in_batch_update = True
                    try:
                        # ВЫЗЫВАЕМ flush У БАТЧ-АПДЕЙТЕРА
                        remaining_updates = self.batch_updater.flush()
                        if remaining_updates > 0:
                            self.stats['updated'] += remaining_updates
                            self.stats['updated_games'] += remaining_updates  # 💾

                            # НЕМЕДЛЕННО ОБНОВЛЯЕМ СТАТИСТИКУ В ПРОГРЕСС-БАРЕ
                            if self.progress_bar:
                                if self.keywords:
                                    self.progress_bar.update_stats({
                                        'updated': self.stats['updated_games'],  # 💾
                                        'in_batch': self.stats['in_batch'],  # 📦
                                        'found_count': self.stats['found_games'],  # 🎯
                                        'total_criteria_found': self.stats['found_elements'],  # 📈
                                        'skipped_total': self.stats['skipped_games'],  # ⏭️
                                        'not_found_count': self.stats['empty_games'],  # 🔍
                                        'errors': self.stats['error_games'],  # ❌
                                    })
                                else:
                                    self.progress_bar.update_stats({
                                        'updated': self.stats['updated_games'],  # 💾
                                        'in_batch': self.stats['in_batch'],  # 📦
                                        'found_count': self.stats['found_games'],  # 🎯
                                        'total_criteria_found': self.stats['found_elements'],  # 📈
                                        'skipped_total': self.stats['skipped_games'],  # ⏭️
                                        'not_found_count': self.stats['empty_games'],  # 🔍
                                        'errors': self.stats['error_games'],  # ❌
                                    })
                    finally:
                        self._in_batch_update = False

            except Exception as e:
                self._in_batch_update = False
                if self.verbose and self.original_stderr:
                    self.original_stderr.write(f"\n❌ Ошибка обновления батча: {e}\n")
                    self.original_stderr.flush()

    def _flush_remaining_batch(self):
        """Обновляет оставшийся батч после завершения цикла"""
        if not self.update_game or not self.batch_updater or getattr(self, '_in_batch_update', False):
            return

        try:
            games_in_batch = len(self.batch_updater.games_to_update) if hasattr(self.batch_updater,
                                                                                'games_to_update') else 0

            if games_in_batch > 0:
                # Выводим информацию о финальном обновлении
                if self.verbose and self.original_stdout:
                    self.original_stdout.write(f"\n📦 Финальное обновление: {games_in_batch} игр в батче\n")
                    self.original_stdout.flush()

                self._in_batch_update = True
                try:
                    remaining_updates = self.batch_updater.flush()
                    if remaining_updates > 0:
                        self.stats['updated'] += remaining_updates
                        if self.progress_bar:
                            self.progress_bar.update_stats({
                                'updated': self.stats['updated'],
                                'in_batch': 0  # Сбрасываем после финального обновления
                            })

                        # Выводим сообщение о финальном обновлении
                        if self.verbose and self.original_stdout:
                            self.original_stdout.write(
                                f"💾 Финальный батч обновлен: {remaining_updates} игр (всего: {self.stats['updated']})\n")
                            self.original_stdout.flush()
                    elif games_in_batch > 0:
                        # Если игры были в батче, но не обновились
                        if self.verbose and self.original_stdout:
                            self.original_stdout.write(f"⚠️ Финальный батч не обновлен (0 из {games_in_batch} игр)\n")
                            self.original_stdout.flush()
                finally:
                    self._in_batch_update = False
        except Exception as e:
            self._in_batch_update = False
            if self.verbose and self.original_stderr:
                self.original_stderr.write(f"\n❌ Ошибка финального обновления батча: {e}\n")
                self.original_stderr.flush()

    def _print_batch_processing_stats(self, processed_in_this_run, processed_previously_processed_games,
                                      skipped_because_already_processed, skipped_because_criteria_checked,
                                      new_criteria):
        """Выводит статистику обработки батча"""
        stats_messages = []

        if processed_in_this_run > 0:
            stats_messages.append(f"📊 Обработано игр в этом запуске: {processed_in_this_run}")

        if processed_previously_processed_games > 0:
            stats_messages.append(f"📊 Повторно обработано игр (новые критерии): {processed_previously_processed_games}")

        if skipped_because_already_processed > 0:
            stats_messages.append(f"📊 Пропущено уже обработанных игр: {skipped_because_already_processed}")

        if skipped_because_criteria_checked > 0:
            stats_messages.append(f"📊 Пропущено по проверенным критериям: {skipped_because_criteria_checked}")

        if new_criteria and self.stats.get('games_with_new_criteria', 0) > 0:
            stats_messages.append(f"🎯 Игр с новыми критериями: {self.stats['games_with_new_criteria']}")

        if stats_messages:
            stats_summary = "\n".join(stats_messages)
            if self.verbose and self.original_stdout:
                self.original_stdout.write(f"\n{stats_summary}\n")
                self.original_stdout.flush()
            if self.output_file and not self.output_file.closed:
                self.output_file.write(f"\n{stats_summary}\n")
                self.output_file.flush()

    def _print_startup_info(self, total_games, already_processed, checked_criteria, new_criteria,
                            should_process_all, estimated_new_games):
        """Выводит информацию о начале анализа"""
        # ВЫВОДИМ В ТЕРМИНАЛ: только при verbose или если нет прогресс-бара
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

        # ВЫВОДИМ В ФАЙЛ: всегда, независимо от verbose
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

    def _display_final_statistics(self, stats: Dict[str, Any], already_processed: int, total_games: int):
        """Выводит финальную статистику в терминал и файл"""
        # Получаем информацию о критериях
        checked_criteria_count = len(self.state_manager.get_checked_criteria()) if self.state_manager else 0

        # 1. В файл - полная статистика
        if self.output_file and not self.output_file.closed:
            try:
                self.output_file.write("\n" + "=" * 60 + "\n")
                if self.keywords:
                    self.output_file.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КЛЮЧЕВЫЕ СЛОВА)\n")
                else:
                    self.output_file.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КРИТЕРИИ)\n")
                self.output_file.write("=" * 60 + "\n")
                # ... остальная статистика в файл без изменений ...
                self.output_file.flush()
            except Exception:
                pass

        # 2. В терминал - только если нет прогресс-бара или если это завершение (не прерывание)
        if self.original_stdout:
            try:
                # Переходим на новую строку после прогресс-баров
                self.original_stdout.write("\n")
                self.original_stdout.write("=" * 60 + "\n")
                self.original_stdout.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
                self.original_stdout.write("=" * 60 + "\n")

                # Показываем статистику только если что-то обработано
                if stats.get('processed', 0) > 0:
                    if self.keywords:
                        self.original_stdout.write(f"🔄 Обработано игр: {stats.get('processed', 0)}\n")
                        self.original_stdout.write(f"🎯 Игр с ключевыми словами: {stats.get('keywords_found', 0)}\n")
                        self.original_stdout.write(f"📈 Всего ключевых слов: {stats.get('keywords_count', 0)}\n")
                    else:
                        self.original_stdout.write(f"🔄 Обработано игр: {stats.get('processed', 0)}\n")
                        self.original_stdout.write(f"🎯 Игр с критериями: {stats.get('found_count', 0)}\n")
                        self.original_stdout.write(f"📈 Всего критериев: {stats.get('total_criteria_found', 0)}\n")

                    self.original_stdout.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")
                    self.original_stdout.write(f"💾 Обновлено игр: {stats.get('updated', 0)}\n")

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

    def _print_to_terminal(self, message: str, end: str = "\n"):
        """Печатает только в терминал (не используется при прогресс-баре)"""
        if not self.original_stdout or (
                not self.no_progress and not self.game_id and not self.game_name and not self.description):
            return

        # Только если это одиночная игра или отключен прогресс-бар
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

            # 4. Батч-апдейтер - ПЕРЕДАЕМ verbose параметр
            self.stdout.write("   🔧 Инициализируем BatchUpdater...")
            from .batch_updater import BatchUpdater
            self.batch_updater = BatchUpdater(verbose=self.verbose)  # Передаем флаг verbose
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

            # ИСПРАВЛЕНИЕ: Предупреждение о конфликте ТОЛЬКО при включенном progress bar
            if not self.no_progress and self.verbose:
                self.stdout.write("⚠️  ВНИМАНИЕ: С включенным прогресс-баром подробный вывод будет ограничен")
                self.stdout.write("⚠️  Используйте --no-progress для полного verbose вывода")
            elif not self.no_progress and not self.verbose:
                self.stdout.write("ℹ️  Прогресс-бар включен. Для подробного вывода используйте --verbose")

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

            # ДОБАВЛЯЕМ ИНФОРМАЦИЮ О ОФФСЕТЕ
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
            self.stdout.write("")

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

        # Инициализируем прогресс-бар (TerminalController уже очистил экран)
        progress_bar = ProgressBar(
            total=total_games,
            desc="Анализ игр",
            bar_length=30,
            update_interval=0.1,
            stat_width=stat_width,
            emoji_spacing=1,
            terminal_stream=None  # TerminalController сам управляет выводом
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
        # НЕ завершаем и НЕ очищаем прогресс-бар
        if self.progress_bar:
            self.progress_bar.set_enabled(False)  # Только останавливаем

        # Восстанавливаем потоки
        self._restore_output_streams()

        # Выводим в терминал сообщение о прерывании на НОВОЙ строке
        if self.original_stdout:
            # Переходим на новую строку после прогресс-баров
            self.original_stdout.write("\n")
            self.original_stdout.write("⏹️ Обработка прервана пользователем\n")
            self.original_stdout.flush()

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
                # Закрываем файл
                self.output_file.close()

                # Восстанавливаем потоки
                if self.original_stdout:
                    self.stdout._out = self.original_stdout
                if self.original_stderr:
                    self.stderr._out = self.original_stderr

                # Выводим финальное сообщение только если нет прогресс-бара
                if self.output_path and (self.no_progress or self.game_id or self.game_name or self.description):
                    self.stdout.write(f"\n✅ Результаты экспортированы в: {self.output_path}")

            except Exception as e:
                if self.original_stderr:
                    self.original_stderr.write(f"\n⚠️ Ошибка закрытия файла: {e}\n")

    def _cleanup(self):
        """Очистка ресурсов"""
        # НЕ завершаем прогресс-бар автоматически
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
