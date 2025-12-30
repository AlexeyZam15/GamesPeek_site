# games/management/commands/analyzer/analyzer_command.py
"""
Основной класс команды анализа с использованием нового API
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

        # Новые параметры
        self.comprehensive_mode = False
        self.combined_mode = False
        self.exclude_existing = False

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        # Сохраняем опции
        self._store_options(options)

        # Инициализируем компоненты
        self._init_components()

        try:
            # Настраиваем вывод в файл
            if self.output_path:
                self._setup_file_output()

            # Выводим настройки
            if not self.only_found:
                self._print_options_summary()

            # ДОБАВЬТЕ ОТЛАДОЧНЫЙ ВЫВОД
            self.stdout.write("🔍 Начинаем обработку команды...")

            # Обрабатываем команду
            self._process_command()

        except KeyboardInterrupt:
            self._handle_interrupt()
        except Exception as e:
            self._handle_error(e)
        finally:
            self._cleanup()

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
        # Обновляем оставшиеся игры
        if self.update_game:
            remaining_updates = self.batch_updater.flush()
            self.stats['updated'] += remaining_updates

        # Сохраняем состояние
        self.state_manager.save_state(self.stats['processed'])

        # Завершаем прогресс-бар - ИСПРАВЛЕНО
        if self.progress_bar:
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
                if not self.no_progress and estimated_new_games > 1:
                    self.original_stdout.write("📊 Прогресс:\n")
                self.original_stdout.flush()

            # Выводим информацию в файл
            if self.output_file:
                self.stdout.write("\n" + "=" * 60)
                self.stdout.write(f"🔍 АНАЛИЗ ИГР (всего в базе: {total_games})")
                self.stdout.write("=" * 60)
                self.stdout.write(f"📊 Будут обработаны: {games_to_process} игр")
                if already_processed > 0:
                    self.stdout.write(f"📊 Новых для обработки: {estimated_new_games} игр")
                self.stdout.write("=" * 60)
                self.stdout.write("")

            # Инициализируем статистику
            self._init_stats()

            # Инициализируем прогресс-бар (только для терминала) - ИСПРАВЛЕНО
            self.progress_bar = self._init_progress_bar(estimated_new_games)  # СОХРАНЯЕМ прогресс-бар

            # Отладочный вывод для проверки
            import sys
            if self.original_stderr:
                if self.progress_bar:
                    self.original_stderr.write(f"✅ Прогресс-бар создан. Всего игр: {estimated_new_games}\n")
                    self.original_stderr.flush()
                else:
                    self.original_stderr.write(f"⚠️ Прогресс-бар не создан. Причина:\n")
                    self.original_stderr.write(f"   - no_progress: {self.no_progress}\n")
                    self.original_stderr.write(f"   - estimated_new_games: {estimated_new_games}\n")
                    self.original_stderr.flush()

            start_time = time.time()

            # Обрабатываем игры
            try:
                for game in games.iterator(chunk_size=self.batch_size):
                    # Пропускаем уже обработанные
                    if self.state_manager.is_game_processed(game.id):
                        continue

                    self._process_single_game_in_batch(game)

                    # Периодическое сохранение состояния
                    if self.stats['processed'] % 1000 == 0:
                        self.state_manager.save_state(self.stats['processed'])

                # Обновляем оставшиеся игры
                if self.update_game:
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

    def _display_final_statistics(self, stats, already_processed, total_games):
        """Выводит финальную статистику в терминал и файл"""
        # 1. В файл - полная статистика через formatter
        if self.output_file and not self.output_file.closed:
            self.stdout.write("\n" + "=" * 60)

            if self.keywords:
                self.stdout.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КЛЮЧЕВЫЕ СЛОВА)")
            else:
                self.stdout.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КРИТЕРИИ)")

            self.stdout.write("=" * 60)

            # Показываем пропущенные ранее обработанные игры
            if already_processed > 0:
                self.stdout.write(f"⏭️ Пропущено ранее обработанных игр: {already_processed}")

            if self.keywords:
                processed_count = stats.get('keywords_processed', stats.get('processed', 0))
                self.stdout.write(f"🔄 Обработано новых игр: {processed_count}")
                self.stdout.write(f"🎯 Игр с найденными ключ. словами: {stats.get('keywords_found', 0)}")
                self.stdout.write(f"📈 Всего ключевых слов найдено: {stats.get('keywords_count', 0)}")

                if stats.get('keywords_not_found', 0) > 0:
                    self.stdout.write(f"⚡ Игр без ключевых слов: {stats['keywords_not_found']}")
            else:
                self.stdout.write(f"🔄 Обработано новых игр: {stats.get('processed', 0)}")
                self.stdout.write(f"🎯 Игр с найденными критериями: {stats.get('found_count', 0)}")
                self.stdout.write(f"📈 Всего критериев найдено: {stats.get('total_criteria_found', 0)}")

                if stats.get('not_found_count', 0) > 0:
                    self.stdout.write(f"⚡ Игр без критериев: {stats['not_found_count']}")

            total_skipped = stats['skipped_no_text'] + stats.get('skipped_short_text', 0) + (
                stats['keywords_not_found'] if self.keywords else stats['not_found_count']
            )

            self.stdout.write(f"⏭️ Всего пропущено игр: {total_skipped}")
            self.stdout.write(f"⏭️ Игр без текста: {stats['skipped_no_text']}")

            if 'skipped_short_text' in stats and stats['skipped_short_text'] > 0:
                self.stdout.write(f"⏭️ Игр с коротким текстом: {stats['skipped_short_text']}")

            if self.keywords and stats.get('keywords_not_found', 0) > 0:
                self.stdout.write(f"⏭️ Игр без ключевых слов: {stats['keywords_not_found']}")
            elif not self.keywords and stats.get('not_found_count', 0) > 0:
                self.stdout.write(f"⏭️ Игр без критериев: {stats['not_found_count']}")

            self.stdout.write(f"❌ Ошибок: {stats['errors']}")
            self.stdout.write(f"💾 Обновлено игр: {stats['updated']}")

            if stats['execution_time'] > 0:
                self.stdout.write(f"⏱️ Время выполнения: {stats['execution_time']:.1f} секунд")

            self.stdout.write("=" * 60)
            self.stdout.write("✅ Анализ успешно завершен")
            self.stdout.write("=" * 60)

        # 2. В терминал - краткая статистика
        if self.original_stdout:
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

            self.original_stdout.write(f"⏭️ Пропущено (нет текста): {stats.get('skipped_no_text', 0)}\n")
            self.original_stdout.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")

            if stats.get('execution_time', 0) > 0:
                self.original_stdout.write(f"⏱️ Время: {stats['execution_time']:.1f} секунд\n")

            self.original_stdout.write("=" * 60 + "\n")

            if self.output_path:
                self.original_stdout.write(f"✅ Результаты сохранены в: {self.output_path}\n")
            else:
                self.original_stdout.write("✅ Анализ завершен\n")

            self.original_stdout.write("=" * 60 + "\n")
            self.original_stdout.flush()

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
        self.no_progress = options.get('no_progress', False)  # ВАЖНО!
        self.force_restart = options.get('force_restart', False)
        self.keywords = options.get('keywords', False)
        self.clear_cache = options.get('clear_cache', False)
        self.output_path = options.get('output')

        # Источники текста
        self.use_wiki = options.get('use_wiki', False)
        self.use_rawg = options.get('use_rawg', False)
        self.use_storyline = options.get('use_storyline', False)
        self.prefer_wiki = options.get('prefer_wiki', False)
        self.prefer_storyline = options.get('prefer_storyline', False)
        self.combine_texts = options.get('combine_texts', False)
        self.combine_all_texts = options.get('combine_all_texts', False)

    def _init_components(self):
        """Инициализирует компоненты"""
        self.stdout.write("🔧 Инициализируем компоненты команды...")

        try:
            # 1. API анализатора
            self.stdout.write("   🔧 Загружаем GameAnalyzerAPI...")
            from games.analyze import GameAnalyzerAPI
            self.api = GameAnalyzerAPI(verbose=self.verbose)
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
            self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")
            self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}")
            self.stdout.write("=" * 60)
            self.stdout.write("")

        # В терминал - только краткая информация
        if self.original_stdout:
            self.original_stdout.write(f"🎮 Анализ игр запущен\n")
            self.original_stdout.write(f"📊 Режим: {'КЛЮЧЕВЫЕ СЛОВА' if self.keywords else 'КРИТЕРИИ'}\n")
            self.original_stdout.write(f"📁 Результаты в файле: {self.output_path}\n")
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

    def _process_single_game_in_batch(self, game):
        """Обрабатывает одну игру в пакетной обработке с поддержкой всех режимов"""
        # Показываем отладочные сообщения только если не в режиме only-found
        show_debug = not self.only_found

        if show_debug:
            self.stdout.write(f"\n🎮 Обрабатываем игру #{self.stats['processed'] + 1}: {game.name} (ID: {game.id})")

        # Получаем текст
        text = self.text_preparer.prepare_text(game)

        if show_debug and text:
            self.stdout.write(f"   📏 Длина текста: {len(text)} символов")
        elif show_debug and not text:
            self.stdout.write(f"   ⚠️ Нет текста для анализа")

        # Обновляем статистику
        self.stats['processed'] += 1

        if not text:
            if show_debug:
                self.stdout.write(f"   ⏭️ Пропускаем - нет текста")
            self.stats['skipped_no_text'] += 1
            self.state_manager.add_processed_game(game.id)

            # Обновляем прогресс-бар даже если пропустили игру
            if self.progress_bar:
                self._update_progress_bar()

            return

        # Проверяем длину текста
        if len(text) < self.min_text_length:
            if show_debug:
                self.stdout.write(f"   ⏭️ Пропускаем - текст слишком короткий ({len(text)} < {self.min_text_length})")
            self.stats['skipped_short_text'] += 1
            self.state_manager.add_processed_game(game.id)

            # Обновляем прогресс-бар даже если пропустили игру
            if self.progress_bar:
                self._update_progress_bar()

            return

        self.stats['processed_with_text'] += 1

        # Анализируем
        if show_debug:
            self.stdout.write(f"   🔧 Анализируем текст через API...")

        try:
            # ВЫБОР РЕЖИМА АНАЛИЗА на основе флагов
            if self.comprehensive_mode:
                # КОМПЛЕКСНЫЙ АНАЛИЗ
                if show_debug:
                    self.stdout.write(f"   🔎 Режим: КОМПЛЕКСНЫЙ (все вхождения)")
                result = self.api.analyze_game_text_comprehensive(
                    text=text,
                    game_id=game.id,
                    existing_game=game if not self.ignore_existing and self.exclude_existing else None,
                    exclude_existing=self.exclude_existing
                )
            elif self.combined_mode:
                # КОМБИНИРОВАННЫЙ АНАЛИЗ
                if show_debug:
                    self.stdout.write(f"   🔎 Режим: КОМБИНИРОВАННЫЙ (критерии + ключевые слова)")
                result = self.api.analyze_game_text_combined(
                    text=text,
                    game_id=game.id,
                    existing_game=game if not self.ignore_existing and self.exclude_existing else None,
                    detailed_patterns=self.verbose,
                    exclude_existing=self.exclude_existing
                )
            else:
                # ОБЫЧНЫЙ АНАЛИЗ (критерии или ключевые слова)
                mode_text = "ключевых слов" if self.keywords else "критериев"
                if show_debug:
                    self.stdout.write(f"   🔎 Режим: ОБЫЧНЫЙ ({mode_text})")
                result = self.api.analyze_game_text(
                    text=text,
                    game_id=game.id,
                    analyze_keywords=self.keywords,
                    existing_game=game if not self.ignore_existing and self.exclude_existing else None,
                    detailed_patterns=self.verbose,
                    exclude_existing=self.exclude_existing
                )

            if not result['success']:
                if show_debug:
                    self.stdout.write(f"   ❌ Ошибка анализа: {result.get('error_message', 'Неизвестная ошибка')}")
                self.stats['errors'] += 1
                self.state_manager.add_processed_game(game.id)

                # Обновляем прогресс-бар при ошибке
                if self.progress_bar:
                    self._update_progress_bar()

                return

            # Определяем, нужно ли показывать детали этой игры
            should_show_details = show_debug or (self.only_found and result['has_results'])

            if should_show_details:
                self.stdout.write(f"   📊 Результат: успех={result['success']}, есть_результаты={result['has_results']}")

            # Обновляем статистику
            if result['has_results']:
                # Для комплексного режима используем total_matches, для остальных - found_count
                if self.comprehensive_mode:
                    found_count = result.get('total_matches', 0)
                else:
                    found_count = result['summary'].get('found_count', 0)

                if show_debug:  # Всегда показываем для режима отладки
                    if self.comprehensive_mode:
                        self.stdout.write(f"   ✅ Найдено вхождений: {found_count}")
                    else:
                        self.stdout.write(f"   ✅ Найдено элементов: {found_count}")

                # Обновляем статистику в зависимости от режима
                if self.keywords:
                    self.stats['keywords_found'] += 1
                    self.stats['keywords_count'] += found_count
                elif self.combined_mode:
                    self.stats['found_count'] += 1
                    self.stats['total_criteria_found'] += found_count
                    # Дополнительная статистика для комбинированного режима
                    if 'summary' in result:
                        for key in ['genres_found', 'themes_found', 'perspectives_found', 'game_modes_found',
                                    'keywords_found']:
                            if key in result['summary']:
                                if key not in self.stats:
                                    self.stats[key] = 0
                                self.stats[key] += result['summary'][key]
                elif self.comprehensive_mode:
                    self.stats['found_count'] += 1
                    self.stats['total_criteria_found'] += found_count
                else:
                    self.stats['found_count'] += 1
                    self.stats['total_criteria_found'] += found_count

                # Форматируем и показываем найденные элементы
                if show_debug:
                    for key, data in result['results'].items():
                        if data['count'] > 0:
                            display_name = self._get_display_name_for_key(key)
                            item_names = [item['name'] for item in data['items']]
                            self.stdout.write(f"   📌 {display_name} ({data['count']}): {item_names}")
            else:
                # ВСЕГДА показываем если не найден результат и не в режиме only-found
                if show_debug:
                    if self.comprehensive_mode:
                        self.stdout.write(f"   ⚡ Вхождения не найдены")
                    elif self.combined_mode:
                        self.stdout.write(f"   ⚡ Критерии и ключевые слова не найдены")
                    elif self.keywords:
                        self.stdout.write(f"   ⚡ Ключевые слова не найдены")
                    else:
                        self.stdout.write(f"   ⚡ Критерии не найдены")

                # Обновляем статистику "не найдено"
                if self.keywords:
                    self.stats['keywords_not_found'] += 1
                else:
                    self.stats['not_found_count'] += 1

            # Выводим паттерны если verbose и show_debug
            if show_debug and self.verbose and 'pattern_info' in result:
                self._print_pattern_details(result['pattern_info'])

            # Всегда вызываем formatter, он сам решит что показывать
            self.output_formatter.print_game_in_batch(
                game=game,
                index=self.stats['processed'],  # Используем общий счетчик
                result=result,
                stats=self.stats,
                only_found=self.only_found,
                verbose=self.verbose,
                keywords=self.keywords,
                ignore_existing=self.ignore_existing,
                update_game=self.update_game,
                comprehensive_mode=self.comprehensive_mode,
                combined_mode=self.combined_mode
            )

            # Добавляем в батч для обновления если нужно
            if self.update_game and result['has_results']:
                if show_debug:
                    self.stdout.write(f"   💾 Добавляем в очередь для обновления БД")

                # Для обновления используем правильный метод API в зависимости от режима
                if self.combined_mode or self.comprehensive_mode:
                    # Для комбинированного/комплексного режима используем специальный метод
                    self.batch_updater.add_game_for_update_combined(
                        game_id=game.id,
                        results=result['results'],
                        combined_mode=True
                    )
                else:
                    # Для обычного режима
                    self.batch_updater.add_game_for_update(
                        game_id=game.id,
                        results=result['results'],
                        is_keywords=self.keywords
                    )

            # Помечаем как обработанную
            self.state_manager.add_processed_game(game.id)

            # ВАЖНО: Обновляем прогресс-бар ПОСЛЕ успешной обработки
            if self.progress_bar:
                self._update_progress_bar()

        except Exception as e:
            if show_debug:
                self.stdout.write(f"   ❌ Исключение при анализе: {e}")
                import traceback
                traceback.print_exc()

            self.stats['errors'] += 1
            self.state_manager.add_processed_game(game.id)

            # Обновляем прогресс-бар даже при ошибке
            if self.progress_bar:
                self._update_progress_bar()

    def _get_base_query(self) -> QuerySet:
        """Возвращает базовый QuerySet"""
        return Game.objects.all().order_by('id')

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
            'errors': 0,
            'updated': 0,
            'displayed_count': 0,
            'keywords_processed': 0,
            'keywords_found': 0,
            'keywords_count': 0,
            'keywords_not_found': 0,
        }

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

    def _update_progress_bar(self):
        """Обновляет прогресс-бар"""
        if not self.progress_bar:
            return

        if self.keywords:
            total_skipped = self.stats['skipped_no_text'] + self.stats.get('skipped_short_text', 0) + self.stats[
                'keywords_not_found']
            self.progress_bar.update_stats({
                'found_count': self.stats['keywords_found'],
                'total_criteria_found': self.stats['keywords_count'],
                'skipped_total': total_skipped,
                'errors': self.stats['errors'],
                'updated': self.stats['updated'],
            })
        else:
            total_skipped = self.stats['skipped_no_text'] + self.stats.get('skipped_short_text', 0) + self.stats[
                'not_found_count']
            self.progress_bar.update_stats({
                'found_count': self.stats['found_count'],
                'total_criteria_found': self.stats['total_criteria_found'],
                'skipped_total': total_skipped,
                'errors': self.stats['errors'],
                'updated': self.stats['updated'],
            })

        # ВАЖНО: Обновляем прогресс на 1
        self.progress_bar.update(1)

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