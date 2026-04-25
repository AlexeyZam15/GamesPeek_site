# games/management/commands/add_new_keywords_from_descriptions.py
"""
Команда для добавления новых ключевых слов в базу данных на основе описаний игр.
Находит все слова в тексте, нормализует их через WordNet и создаёт новые ключевые слова,
если их ещё нет в базе данных. Результаты записываются в файл.

ВАЖНО: Эта команда НЕ добавляет связи между играми и ключевыми словами.
Она только создаёт новые ключевые слова в таблице games_keyword.
Для добавления связей используйте отдельную команду.

Нормализация выполняется ТОЧНО ТАК ЖЕ, как в normalize_keywords.py,
используя get_best_base_form() без пакетной обработки для консистентности.
"""

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db import transaction, IntegrityError, models, connection
from games.models import Game, Keyword
from games.analyze import GameAnalyzerAPI
from games.analyze.wordnet_api import get_wordnet_api
import time
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Set, Optional, Tuple
from collections import defaultdict


class Command(BaseCommand):
    help = 'Добавляет новые ключевые слова в БД из описаний игр (без создания связей)'

    def add_arguments(self, parser):
        # Основные параметры
        parser.add_argument(
            '--output',
            type=str,
            default='add_new_keywords_results.txt',
            help='Путь к файлу для сохранения результатов (по умолчанию: add_new_keywords_results.txt)'
        )
        parser.add_argument(
            '--errors-output',
            type=str,
            default='add_new_keywords_errors.txt',
            help='Путь к файлу для сохранения ошибок (по умолчанию: add_new_keywords_errors.txt)'
        )
        parser.add_argument(
            '--found-output',
            type=str,
            default='add_new_keywords_found.txt',
            help='Путь к файлу для сохранения найденных ключевых слов (по умолчанию: add_new_keywords_found.txt)'
        )
        parser.add_argument(
            '--skipped-output',
            type=str,
            default='add_new_keywords_skipped.txt',
            help='Путь к файлу для сохранения пропущенных ключевых слов (по умолчанию: add_new_keywords_skipped.txt)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Режим просмотра: найти ключевые слова, но не добавлять в БД (файл сохраняется)'
        )

        # Параметры для поиска конкретной игры
        parser.add_argument(
            '--game-name',
            type=str,
            help='Анализировать конкретную игру по названию'
        )

        # Параметры массовой обработки
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Максимальное количество игр для анализа'
        )
        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Пропустить первые N игр'
        )
        parser.add_argument(
            '--analysis-batch-size',
            type=int,
            default=100,
            help='Размер батча для этапа анализа (по умолчанию 100)'
        )
        parser.add_argument(
            '--create-batch-size',
            type=int,
            default=1000,
            help='Размер батча для создания ключевых слов (по умолчанию 1000)'
        )

        # Дополнительные опции
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод в консоль'
        )
        parser.add_argument(
            '--no-progress',
            action='store_true',
            help='Отключить прогресс-бар'
        )
        parser.add_argument(
            '--clear-cache',
            action='store_true',
            help='Очистить кэш WordNet перед началом'
        )
        parser.add_argument(
            '--skip-analysis',
            action='store_true',
            help='Пропустить этап анализа (использовать ранее сохранённые данные)'
        )
        parser.add_argument(
            '--delete-stopwords',
            action='store_true',
            help='Удалить из БД все ключевые слова, которые входят в список стоп-слов'
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = None
        self.wordnet_api = None
        self.text_preparer = None

        # Статистика для этапа анализа
        self.analysis_stats = {
            'games_processed': 0,
            'games_with_text': 0,
            'games_skipped_no_text': 0,
            'games_skipped_short_text': 0,
            'words_found': set(),
            'words_not_in_wordnet': set(),
            'stop_words_found': set(),
            'errors': [],
            'start_time': None,
            'end_time': None
        }

        # Статистика для этапа создания ключевых слов
        self.create_stats = {
            'words_processed': 0,
            'existing_keywords_found': 0,
            'new_keywords_created': 0,
            'errors': [],
            'start_time': None,
            'end_time': None
        }

        # Общие данные между этапами
        self.game_words_map = {}  # {game_id: set(normalized_words)}
        self.all_unique_words = set()  # Все уникальные слова из всех игр

        # Кэши для оптимизации
        self.wordnet_cache = {}  # {original_word: normalized_word}
        self.wordnet_exists_cache = {}  # {word: exists_in_wordnet}
        self.stop_words = self._get_stop_words()
        self.word_regex = None
        self.existing_keywords_cache = {}  # {normalized_word: keyword_object}

        # Фиксированные настройки
        self.min_word_length = 3
        self.min_text_length = 50

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        # Сохраняем опции
        self.output_path = options['output']
        self.errors_output_path = options['errors_output']
        self.found_output_path = options.get('found_output', 'add_new_keywords_found.txt')
        self.skipped_output_path = options.get('skipped_output', 'add_new_keywords_skipped.txt')
        self.dry_run = options['dry_run']
        self.game_name = options.get('game_name')
        self.limit = options.get('limit')
        self.offset = options.get('offset', 0)
        self.analysis_batch_size = options.get('analysis_batch_size', 100)
        self.create_batch_size = options.get('create_batch_size', 1000)
        self.verbose = options.get('verbose', False)
        self.no_progress = options.get('no_progress', False)
        self.clear_cache = options.get('clear_cache', False)
        self.skip_analysis = options.get('skip_analysis', False)
        self.delete_stopwords = options.get('delete_stopwords', False)

        try:
            # Если нужно удалить стоп-слова, выполняем только эту операцию
            if self.delete_stopwords:
                self._delete_stopwords_from_db()
                return

            self.stdout.write("=" * 70)
            self.stdout.write("🔍 ДОБАВЛЕНИЕ НОВЫХ КЛЮЧЕВЫХ СЛОВ ИЗ ОПИСАНИЙ ИГР")
            self.stdout.write("=" * 70)

            if self.dry_run:
                self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА (--dry-run)"))
                self.stdout.write(self.style.WARNING("   Ключевые слова НЕ будут добавлены в БД"))

            self.stdout.write(f"📁 Результаты будут сохранены в файл: {self.output_path}")
            self.stdout.write(f"📁 Найденные ключевые слова: {self.found_output_path}")
            self.stdout.write(f"📁 Пропущенные ключевые слова: {self.skipped_output_path}")
            self.stdout.write(f"📁 Ошибки будут сохранены в файл: {self.errors_output_path}")
            self.stdout.write(f"📚 Источник текста: Комбинированные описания (IGDB, Wikipedia, RAWG, Storyline)")
            self.stdout.write(f"📏 Мин. длина слова: {self.min_word_length}")
            self.stdout.write(f"📏 Мин. длина текста: {self.min_text_length} символов")
            self.stdout.write("")
            self.stdout.write(
                self.style.WARNING("⚠️ ВНИМАНИЕ: Эта команда НЕ создаёт связи между играми и ключевыми словами"))
            self.stdout.write(self.style.WARNING("   Только добавляет новые ключевые слова в таблицу games_keyword"))
            self.stdout.write("")

            # Инициализация
            self.stdout.write("🔧 Инициализация компонентов...")
            self.api = GameAnalyzerAPI(verbose=False)
            self.wordnet_api = get_wordnet_api(verbose=self.verbose)
            self.text_preparer = self._create_text_preparer()

            if not self.wordnet_api.is_available():
                self.stdout.write(self.style.WARNING("⚠️ WordNetAPI недоступен, нормализация будет ограничена"))

            self.stdout.write("✅ Компоненты готовы")
            self.stdout.write("")

            if self.clear_cache and self.wordnet_api:
                self.stdout.write("🧹 Очистка кэша WordNet...")
                self.wordnet_api.clear_cache()
                self.stdout.write("✅ Кэш очищен")
                self.stdout.write("")

            # Получаем игры для анализа
            games_queryset = self._get_games_queryset()
            total_games = games_queryset.count()

            if total_games == 0:
                self.stdout.write(self.style.WARNING("⚠️ Нет игр для анализа"))
                return

            self.stdout.write(f"📊 Найдено игр для анализа: {total_games}")
            if self.offset > 0:
                self.stdout.write(f"📍 Пропускаем первые {self.offset} игр")
            if self.limit:
                self.stdout.write(f"🎯 Лимит: {self.limit} игр")
            self.stdout.write("")

            # ЭТАП 1: АНАЛИЗ
            if not self.skip_analysis:
                self.stdout.write("=" * 70)
                self.stdout.write("📊 ЭТАП 1/2: АНАЛИЗ ИГР И СБОР КЛЮЧЕВЫХ СЛОВ")
                self.stdout.write("=" * 70)
                self.stdout.write("")

                if not self.no_progress:
                    self.stdout.write("📊 ЛЕГЕНДА СТАТИСТИКИ (ЭТАП АНАЛИЗА):")
                    self.stdout.write("  📝 = игры с текстом")
                    self.stdout.write("  🔑 = уникальные ключевые слова найдено")
                    self.stdout.write("  ⚠️ = ошибки")
                    self.stdout.write("")

                self.analysis_stats['start_time'] = time.time()
                self._analyze_games_phase(games_queryset, total_games)
                self.analysis_stats['end_time'] = time.time()

                self._print_analysis_stats()
                self._save_analysis_results()

                # СОХРАНЯЕМ НАЙДЕННЫЕ И ПРОПУЩЕННЫЕ СЛОВА В ОТДЕЛЬНЫЕ ФАЙЛЫ
                self._save_found_and_skipped_words()

                if self.analysis_stats['errors']:
                    self._save_errors_to_file(self.analysis_stats['errors'], "analysis")

                self.stdout.write("")
            else:
                self.stdout.write(self.style.WARNING("⏭️ Пропускаем этап анализа (--skip-analysis)"))
                self.stdout.write("")

            # ЭТАП 2: СОЗДАНИЕ КЛЮЧЕВЫХ СЛОВ
            if not self.dry_run and not self.skip_analysis:
                self.stdout.write("=" * 70)
                self.stdout.write("💾 ЭТАП 2/2: СОЗДАНИЕ НОВЫХ КЛЮЧЕВЫХ СЛОВ В БД")
                self.stdout.write("=" * 70)
                self.stdout.write("")

                if not self.no_progress:
                    self.stdout.write("📊 ЛЕГЕНДА СТАТИСТИКИ (ЭТАП СОЗДАНИЯ):")
                    self.stdout.write("  📚 = существующие ключевые слова")
                    self.stdout.write("  ✨ = новые ключевые слова создано")
                    self.stdout.write("")

                self.create_stats['start_time'] = time.time()
                self._create_keywords_phase()
                self.create_stats['end_time'] = time.time()

                self._print_create_stats()
                self._save_create_results()

                if self.create_stats['errors']:
                    self._save_errors_to_file(self.create_stats['errors'], "create")

                self.stdout.write("")
            elif self.dry_run:
                self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА - этап создания ключевых слов пропущен"))
                self.stdout.write("")

            # Итоговая статистика
            self._print_final_summary()

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\n⏹️ Обработка прервана пользователем"))
            self._save_interrupted_results()
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"\n❌ Ошибка: {e}"))
            if self.verbose:
                import traceback
                traceback.print_exc()
            raise

    def _save_found_and_skipped_words(self):
        """Сохраняет найденные и пропущенные ключевые слова в отдельные файлы в той же директории, что и output_path"""
        try:
            # Получаем директорию из output_path (если есть путь, иначе текущая директория)
            output_dir = os.path.dirname(os.path.abspath(self.output_path))

            # Формируем полные пути для файлов
            found_path = os.path.join(output_dir, os.path.basename(self.found_output_path))
            skipped_path = os.path.join(output_dir, os.path.basename(self.skipped_output_path))

            # Сохраняем найденные ключевые слова
            if self.analysis_stats['words_found']:
                os.makedirs(output_dir, exist_ok=True)
                with open(found_path, 'w', encoding='utf-8') as f:
                    f.write("=" * 80 + "\n")
                    f.write("НАЙДЕННЫЕ КЛЮЧЕВЫЕ СЛОВА\n")
                    f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Всего найдено: {len(self.analysis_stats['words_found'])}\n")
                    f.write("=" * 80 + "\n\n")
                    f.write(", ".join(sorted(self.analysis_stats['words_found'])) + "\n")

                if self.verbose:
                    self.stdout.write(self.style.SUCCESS(f"✅ Найденные слова сохранены: {found_path}"))

            # Сохраняем пропущенные ключевые слова (стоп-слова и слова не из WordNet)
            skipped_words = set()
            skipped_words.update(self.analysis_stats['stop_words_found'])
            skipped_words.update(self.analysis_stats['words_not_in_wordnet'])

            if skipped_words:
                os.makedirs(output_dir, exist_ok=True)
                with open(skipped_path, 'w', encoding='utf-8') as f:
                    f.write("=" * 80 + "\n")
                    f.write("ПРОПУЩЕННЫЕ КЛЮЧЕВЫЕ СЛОВА\n")
                    f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Всего пропущено: {len(skipped_words)}\n")
                    f.write(f"  - Стоп-слова: {len(self.analysis_stats['stop_words_found'])}\n")
                    f.write(f"  - Слова не из WordNet: {len(self.analysis_stats['words_not_in_wordnet'])}\n")
                    f.write("=" * 80 + "\n\n")

                    if self.analysis_stats['stop_words_found']:
                        f.write("СТОП-СЛОВА:\n")
                        f.write("-" * 40 + "\n")
                        f.write(", ".join(sorted(self.analysis_stats['stop_words_found'])) + "\n\n")

                    if self.analysis_stats['words_not_in_wordnet']:
                        f.write("СЛОВА НЕ ИЗ WORDNET:\n")
                        f.write("-" * 40 + "\n")
                        f.write(", ".join(sorted(self.analysis_stats['words_not_in_wordnet'])) + "\n")

                if self.verbose:
                    self.stdout.write(self.style.SUCCESS(f"✅ Пропущенные слова сохранены: {skipped_path}"))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Ошибка при сохранении файлов слов: {e}"))

    def _delete_stopwords_from_db(self):
        """Удаляет из базы данных все ключевые слова, которые входят в список стоп-слов"""
        self.stdout.write("=" * 70)
        self.stdout.write(self.style.WARNING("🗑️ УДАЛЕНИЕ СТОП-СЛОВ ИЗ БАЗЫ ДАННЫХ"))
        self.stdout.write("=" * 70)

        if self.dry_run:
            self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА (--dry-run)"))

        stop_words = self._get_stop_words()
        self.stdout.write(f"📚 Загружено стоп-слов: {len(stop_words)}")
        self.stdout.write("")

        # Преобразуем стоп-слова в список для SQL запроса
        stop_words_list = list(stop_words)

        if not stop_words_list:
            self.stdout.write(self.style.SUCCESS("✅ Нет стоп-слов для удаления"))
            return

        # Используем сырой SQL для подсчета и удаления, чтобы избежать ошибок кэширования
        with connection.cursor() as cursor:
            # Сначала подсчитываем количество ключевых слов для удаления
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM games_keyword
                           WHERE name = ANY (%s)
                           """, [stop_words_list])
            count = cursor.fetchone()[0]

            if count == 0:
                self.stdout.write(self.style.SUCCESS("✅ Стоп-слова не найдены в базе данных"))
                return

            self.stdout.write(f"📊 Найдено ключевых слов-стоп-слов: {count}")

            if self.verbose and count > 0:
                self.stdout.write("\n📌 СПИСОК НАЙДЕННЫХ СТОП-СЛОВ:")
                self.stdout.write("-" * 50)
                cursor.execute("""
                               SELECT id,
                                      name,
                                      (SELECT COUNT(*)
                                       FROM games_game_keywords
                                       WHERE keyword_id = games_keyword.id) as games_count
                               FROM games_keyword
                               WHERE name = ANY (%s)
                               ORDER BY name LIMIT 20
                               """, [stop_words_list])
                rows = cursor.fetchall()
                for row in rows:
                    self.stdout.write(f"  • '{row[1]}' (ID: {row[0]}, игр: {row[2]})")
                if count > 20:
                    self.stdout.write(f"  ... и еще {count - 20} слов")

            # Подсчитываем общее количество связей с играми
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM games_game_keywords
                           WHERE keyword_id IN (SELECT id FROM games_keyword WHERE name = ANY (%s))
                           """, [stop_words_list])
            total_connections = cursor.fetchone()[0]

            if total_connections > 0:
                self.stdout.write(
                    self.style.WARNING(f"\n⚠️ ВНИМАНИЕ: Будет удалено {total_connections} связей с играми!"))

                if not self.dry_run:
                    response = input("   Продолжить? (yes/no): ")
                    if response.lower() != 'yes':
                        self.stdout.write(self.style.WARNING("   Операция отменена"))
                        return

            if not self.dry_run:
                self.stdout.write("\n🗑️ Удаление стоп-слов...")

                # Отключаем проверки внешних ключей временно для скорости
                cursor.execute("SET CONSTRAINTS ALL DEFERRED")

                # Сначала удаляем связи из промежуточной таблицы
                cursor.execute("""
                               DELETE
                               FROM games_game_keywords
                               WHERE keyword_id IN (SELECT id FROM games_keyword WHERE name = ANY (%s))
                               """, [stop_words_list])
                deleted_connections = cursor.rowcount

                # Затем удаляем сами ключевые слова
                cursor.execute("""
                               DELETE
                               FROM games_keyword
                               WHERE name = ANY (%s)
                               """, [stop_words_list])
                deleted_keywords = cursor.rowcount

                self.stdout.write(self.style.SUCCESS(f"✅ Удалено связей с играми: {deleted_connections}"))
                self.stdout.write(self.style.SUCCESS(f"✅ Удалено ключевых слов: {deleted_keywords}"))

                try:
                    from games.analyze.keyword_trie import KeywordTrieManager
                    KeywordTrieManager().clear_cache()
                    self.stdout.write(self.style.SUCCESS("✅ Кэш Trie очищен"))
                except ImportError:
                    pass
            else:
                self.stdout.write(self.style.WARNING("\n🔧 РЕЖИМ ПРОСМОТРА - ничего не удалено"))

        self.stdout.write("\n" + "=" * 70)

    def _analyze_games_phase(self, games_queryset, total_games):
        """ЭТАП 1: Анализ игр и сбор уникальных ключевых слов"""
        # Применяем offset и limit
        if self.offset:
            games_queryset = games_queryset[self.offset:]
        if self.limit:
            games_queryset = games_queryset[:self.limit]

        games_list = list(games_queryset)
        games_to_process = len(games_list)

        # Компилируем регулярное выражение
        self.word_regex = re.compile(r'\b[a-zA-Z]{%d,}\b' % self.min_word_length)

        # Создаём прогресс-бар
        if not self.no_progress:
            from tqdm import tqdm
            pbar = tqdm(
                total=games_to_process,
                desc="Анализ игр",
                unit="game",
                bar_format='{l_bar}{bar:20}{r_bar}',
                position=0,
                leave=True
            )
        else:
            pbar = None

        # Обрабатываем игры батчами
        for batch_start in range(0, games_to_process, self.analysis_batch_size):
            batch_end = min(batch_start + self.analysis_batch_size, games_to_process)
            batch = games_list[batch_start:batch_end]

            for game in batch:
                try:
                    # Получаем текст для анализа
                    text = self.text_preparer.prepare_text(game)

                    if not text:
                        self.analysis_stats['games_skipped_no_text'] += 1
                        if pbar:
                            pbar.update(1)
                        continue

                    if len(text) < self.min_text_length:
                        self.analysis_stats['games_skipped_short_text'] += 1
                        if pbar:
                            pbar.update(1)
                        continue

                    self.analysis_stats['games_with_text'] += 1

                    # Извлекаем слова
                    raw_words = list(dict.fromkeys(self.word_regex.findall(text.lower())))

                    if not raw_words:
                        if pbar:
                            pbar.update(1)
                        continue

                    # Нормализуем слова (используем тот же метод, что и в normalize_keywords.py)
                    normalized_dict = self._normalize_word_batch(raw_words)

                    # Фильтруем и сохраняем
                    game_words = set()
                    for original, normalized in normalized_dict.items():
                        if normalized and len(normalized) >= self.min_word_length:
                            normalized_lower = normalized.lower()

                            # Проверяем стоп-слова
                            if normalized_lower in self.stop_words:
                                self.analysis_stats['stop_words_found'].add(normalized_lower)
                                continue

                            # Проверяем WordNet
                            if not self._is_word_in_wordnet(normalized_lower):
                                self.analysis_stats['words_not_in_wordnet'].add(normalized_lower)
                                continue

                            # Сохраняем слово (именно нормализованную форму)
                            game_words.add(normalized_lower)
                            self.analysis_stats['words_found'].add(normalized_lower)

                    if game_words:
                        self.game_words_map[game.id] = game_words
                        self.all_unique_words.update(game_words)

                except Exception as e:
                    error_msg = f"Game {game.id} ({game.name}): {str(e)}"
                    self.analysis_stats['errors'].append(error_msg)
                    if self.verbose:
                        self.stderr.write(f"\n❌ {error_msg}")

                self.analysis_stats['games_processed'] += 1

                if pbar:
                    pbar.update(1)
                    pbar.set_postfix_str(
                        f"📝:{self.analysis_stats['games_with_text']} "
                        f"🔑:{len(self.analysis_stats['words_found'])} "
                        f"⚠️:{len(self.analysis_stats['errors'])}"
                    )

        if pbar:
            pbar.close()

    def _create_keywords_phase(self):
        """ЭТАП 2: Создание новых ключевых слов в БД"""
        if not self.all_unique_words:
            self.stdout.write(self.style.WARNING("⚠️ Нет слов для создания ключевых слов"))
            return

        self.stdout.write(f"📚 Всего уникальных слов для обработки: {len(self.all_unique_words)}")

        # Загружаем существующие ключевые слова
        self.stdout.write("📚 Загрузка существующих ключевых слов...")
        all_keywords = Keyword.objects.all().only('id', 'name')
        for kw in all_keywords:
            self.existing_keywords_cache[kw.name.lower()] = kw
        self.stdout.write(f"✅ Загружено {len(self.existing_keywords_cache)} ключевых слов")
        self.stdout.write("")

        # Определяем слова, которых ещё нет в БД
        words_to_create = []
        for word in self.all_unique_words:
            if word in self.existing_keywords_cache:
                self.create_stats['existing_keywords_found'] += 1
            else:
                words_to_create.append(word)

        self.stdout.write(f"✨ Новых слов для создания: {len(words_to_create)}")
        self.stdout.write(f"📚 Существующих слов: {self.create_stats['existing_keywords_found']}")
        self.stdout.write("")

        if not words_to_create:
            self.stdout.write(self.style.SUCCESS("✅ Все слова уже существуют в БД"))
            return

        # Создаём прогресс-бар
        if not self.no_progress:
            from tqdm import tqdm
            pbar = tqdm(
                total=len(words_to_create),
                desc="Создание ключевых слов",
                unit="слов",
                bar_format='{l_bar}{bar:20}{r_bar}',
                position=0,
                leave=True
            )
        else:
            pbar = None

        # Создаём ключевые слова батчами
        for i in range(0, len(words_to_create), self.create_batch_size):
            batch = words_to_create[i:i + self.create_batch_size]

            try:
                if not self.dry_run:
                    # Получаем минимальный отрицательный igdb_id
                    min_igdb = Keyword.objects.filter(igdb_id__lt=0).aggregate(models.Min('igdb_id'))['igdb_id__min']
                    next_igdb_id = (min_igdb or 0) - 1

                    # Создаём объекты
                    created_kws = []
                    for word in batch:
                        kw = Keyword(
                            igdb_id=next_igdb_id,
                            name=word,  # Сохраняем именно нормализованную форму
                            category=None
                        )
                        created_kws.append(kw)
                        next_igdb_id -= 1

                    # Bulk insert
                    Keyword.objects.bulk_create(created_kws)
                    self.create_stats['new_keywords_created'] += len(batch)
                else:
                    # Dry-run режим
                    self.create_stats['new_keywords_created'] += len(batch)

                if pbar:
                    pbar.update(len(batch))

            except Exception as e:
                error_msg = f"Batch create keywords (batch {i // self.create_batch_size + 1}): {str(e)}"
                self.create_stats['errors'].append(error_msg)
                if self.verbose:
                    self.stderr.write(f"\n❌ {error_msg}")

        if pbar:
            pbar.close()

        self.create_stats['words_processed'] = len(self.all_unique_words)

    def _normalize_word_batch(self, words: List[str]) -> Dict[str, str]:
        """
        Пакетная нормализация слов через WordNetAPI.
        Использует ТОЧНО ТАКОЙ ЖЕ метод, как в normalize_keywords.py:
        вызывает get_best_base_form() для каждого слова.

        Возвращает словарь {исходное_слово: нормализованное_слово}
        """
        if not self.wordnet_api or not self.wordnet_api.is_available():
            return {word: word.lower() for word in words}

        result = {}

        # Обрабатываем каждое слово индивидуально, как в normalize_keywords.py
        for word in words:
            # Проверяем кэш
            if word in self.wordnet_cache:
                result[word] = self.wordnet_cache[word]
            else:
                # Используем ТОТ ЖЕ МЕТОД, что и в normalize_keywords.py
                normalized = self.wordnet_api.get_best_base_form(word)
                self.wordnet_cache[word] = normalized
                result[word] = normalized

        return result

    def _is_word_in_wordnet(self, word: str) -> bool:
        """Проверяет наличие слова в WordNet"""
        if not self.wordnet_api or not self.wordnet_api.is_available():
            return True

        # Проверяем кэш
        if word in self.wordnet_exists_cache:
            return self.wordnet_exists_cache[word]

        # Проверяем наличие synsets
        has_verb = len(self.wordnet_api.wordnet.synsets(word, pos='v')) > 0
        has_noun = len(self.wordnet_api.wordnet.synsets(word, pos='n')) > 0
        has_adj = len(self.wordnet_api.wordnet.synsets(word, pos='a')) > 0
        has_adv = len(self.wordnet_api.wordnet.synsets(word, pos='r')) > 0

        exists = has_verb or has_noun or has_adj or has_adv
        self.wordnet_exists_cache[word] = exists
        return exists

    def _print_analysis_stats(self):
        """Выводит статистику этапа анализа"""
        elapsed = 0
        if self.analysis_stats['start_time'] and self.analysis_stats['end_time']:
            elapsed = self.analysis_stats['end_time'] - self.analysis_stats['start_time']

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write("📊 СТАТИСТИКА ЭТАПА АНАЛИЗА")
        self.stdout.write("=" * 70)
        self.stdout.write(f"🔄 Обработано игр: {self.analysis_stats['games_processed']}")
        self.stdout.write(f"📝 Игр с текстом: {self.analysis_stats['games_with_text']}")
        self.stdout.write(f"⏭️ Пропущено (нет текста): {self.analysis_stats['games_skipped_no_text']}")
        self.stdout.write(f"⏭️ Пропущено (короткий текст): {self.analysis_stats['games_skipped_short_text']}")
        self.stdout.write(f"🔑 Уникальных ключевых слов найдено: {len(self.analysis_stats['words_found'])}")
        self.stdout.write(f"⚪ Уникальных слов не из WordNet: {len(self.analysis_stats['words_not_in_wordnet'])}")
        self.stdout.write(f"⏹️ Уникальных стоп-слов: {len(self.analysis_stats['stop_words_found'])}")
        self.stdout.write(f"❌ Ошибок: {len(self.analysis_stats['errors'])}")

        if elapsed > 0:
            games_per_second = self.analysis_stats['games_processed'] / elapsed
            self.stdout.write(f"⏱️ Время выполнения: {elapsed:.1f} секунд")
            self.stdout.write(f"⚡ Скорость: {games_per_second:.1f} игр/сек")

        self.stdout.write("=" * 70)

    def _print_create_stats(self):
        """Выводит статистику этапа создания ключевых слов"""
        elapsed = 0
        if self.create_stats['start_time'] and self.create_stats['end_time']:
            elapsed = self.create_stats['end_time'] - self.create_stats['start_time']

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write("📊 СТАТИСТИКА ЭТАПА СОЗДАНИЯ КЛЮЧЕВЫХ СЛОВ")
        self.stdout.write("=" * 70)
        self.stdout.write(f"📚 Обработано уникальных слов: {self.create_stats['words_processed']}")
        self.stdout.write(f"📚 Существующих ключевых слов найдено: {self.create_stats['existing_keywords_found']}")
        self.stdout.write(f"✨ Новых ключевых слов создано: {self.create_stats['new_keywords_created']}")
        self.stdout.write(f"❌ Ошибок: {len(self.create_stats['errors'])}")

        if elapsed > 0 and self.create_stats['words_processed'] > 0:
            words_per_second = self.create_stats['words_processed'] / elapsed
            self.stdout.write(f"⏱️ Время выполнения: {elapsed:.1f} секунд")
            self.stdout.write(f"⚡ Скорость: {words_per_second:.1f} слов/сек")

        self.stdout.write("=" * 70)

    def _print_final_summary(self):
        """Выводит итоговую сводку"""
        self.stdout.write("\n" + "=" * 70)
        self.stdout.write("📊 ИТОГОВАЯ СВОДКА")
        self.stdout.write("=" * 70)
        self.stdout.write(f"📝 Всего проанализировано игр: {self.analysis_stats['games_processed']}")
        self.stdout.write(f"🔑 Уникальных ключевых слов найдено: {len(self.analysis_stats['words_found'])}")
        self.stdout.write(f"📚 Существующих ключевых слов: {self.create_stats['existing_keywords_found']}")
        self.stdout.write(f"✨ Новых ключевых слов создано: {self.create_stats['new_keywords_created']}")

        if self.dry_run:
            self.stdout.write(self.style.WARNING("\n🔧 РЕЖИМ ПРОСМОТРА - изменения не сохранены в БД"))
        else:
            self.stdout.write(f"\n✅ Результаты сохранены в БД")

        self.stdout.write(f"📁 Файл результатов: {self.output_path}")
        self.stdout.write(f"📁 Файл ошибок: {self.errors_output_path}")
        self.stdout.write(self.style.WARNING("\n⚠️ ВНИМАНИЕ: Связи между играми и ключевыми словами НЕ созданы"))
        self.stdout.write(self.style.WARNING("   Для создания связей используйте отдельную команду"))
        self.stdout.write("=" * 70)

    def _save_analysis_results(self):
        """Сохраняет результаты этапа анализа в файл"""
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.output_path)), exist_ok=True)

            with open(self.output_path, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write("РЕЗУЛЬТАТЫ АНАЛИЗА КЛЮЧЕВЫХ СЛОВ (ЭТАП 1)\n")
                f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Источник текста: Комбинированные описания (IGDB, Wikipedia, RAWG, Storyline)\n")
                f.write(f"Минимальная длина текста: {self.min_text_length} символов\n")
                f.write(f"Минимальная длина слова: {self.min_word_length}\n")
                f.write("=" * 80 + "\n\n")

                # Статистика
                f.write("📊 СТАТИСТИКА АНАЛИЗА\n")
                f.write("-" * 40 + "\n")
                f.write(f"🔄 Обработано игр: {self.analysis_stats['games_processed']}\n")
                f.write(f"📝 Игр с текстом: {self.analysis_stats['games_with_text']}\n")
                f.write(f"⏭️ Пропущено (нет текста): {self.analysis_stats['games_skipped_no_text']}\n")
                f.write(f"⏭️ Пропущено (короткий текст): {self.analysis_stats['games_skipped_short_text']}\n")
                f.write(f"🔑 Уникальных ключевых слов найдено: {len(self.analysis_stats['words_found'])}\n")
                f.write(f"⚪ Уникальных слов не из WordNet: {len(self.analysis_stats['words_not_in_wordnet'])}\n")
                f.write(f"⏹️ Уникальных стоп-слов: {len(self.analysis_stats['stop_words_found'])}\n\n")

                # Детальная статистика уникальности
                f.write("📊 ДЕТАЛЬНАЯ СТАТИСТИКА УНИКАЛЬНОСТИ\n")
                f.write("-" * 40 + "\n")
                f.write(f"✨ Новые уникальные ключевые слова: {len(self.analysis_stats['words_found'])}\n")
                f.write(f"⚪ Уникальные слова не из WordNet: {len(self.analysis_stats['words_not_in_wordnet'])}\n")
                f.write(f"⏹️ Уникальные стоп-слова: {len(self.analysis_stats['stop_words_found'])}\n")
                total_unique = (len(self.analysis_stats['words_found']) +
                                len(self.analysis_stats['words_not_in_wordnet']) +
                                len(self.analysis_stats['stop_words_found']))
                f.write(f"🔑 Всего уникальных слов обработано: {total_unique}\n\n")

                # Все найденные ключевые слова (в одну строку через запятую)
                if self.analysis_stats['words_found']:
                    f.write("🔑 НАЙДЕННЫЕ УНИКАЛЬНЫЕ КЛЮЧЕВЫЕ СЛОВА\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Всего: {len(self.analysis_stats['words_found'])}\n")
                    f.write(", ".join(sorted(self.analysis_stats['words_found'])) + "\n")
                    f.write("\n")

                # Стоп-слова (в одну строку через запятую)
                if self.analysis_stats['stop_words_found']:
                    f.write("⏹️ СТОП-СЛОВА (ПРОПУЩЕНЫ)\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Всего: {len(self.analysis_stats['stop_words_found'])}\n")
                    f.write(", ".join(sorted(self.analysis_stats['stop_words_found'])) + "\n")
                    f.write("\n")

                # Слова не из WordNet (в одну строку через запятую)
                if self.analysis_stats['words_not_in_wordnet']:
                    f.write("⚪ СЛОВА НЕ ИЗ WORDNET (ПРОПУЩЕНЫ)\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Всего: {len(self.analysis_stats['words_not_in_wordnet'])}\n")
                    f.write(", ".join(sorted(self.analysis_stats['words_not_in_wordnet'])) + "\n")
                    f.write("\n")

                f.write("=" * 80 + "\n")

            if self.verbose:
                self.stdout.write(self.style.SUCCESS(f"✅ Результаты анализа сохранены: {self.output_path}"))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Ошибка при сохранении файла: {e}"))

    def _save_create_results(self):
        """Сохраняет результаты этапа создания ключевых слов в файл"""
        try:
            with open(self.output_path, 'a', encoding='utf-8') as f:
                f.write("\n\n")
                f.write("=" * 80 + "\n")
                f.write("РЕЗУЛЬТАТЫ СОЗДАНИЯ КЛЮЧЕВЫХ СЛОВ (ЭТАП 2)\n")
                f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n\n")

                f.write("📊 СТАТИСТИКА СОЗДАНИЯ\n")
                f.write("-" * 40 + "\n")
                f.write(f"📚 Обработано уникальных слов: {self.create_stats['words_processed']}\n")
                f.write(f"📚 Существующих ключевых слов найдено: {self.create_stats['existing_keywords_found']}\n")
                f.write(f"✨ Новых ключевых слов создано: {self.create_stats['new_keywords_created']}\n\n")

                f.write("=" * 80 + "\n")

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Ошибка при сохранении файла: {e}"))

    def _save_errors_to_file(self, errors: List[str], phase: str):
        """Сохраняет ошибки в отдельный файл"""
        try:
            os.makedirs(os.path.dirname(os.path.abspath(self.errors_output_path)), exist_ok=True)

            with open(self.errors_output_path, 'a', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write(f"ОШИБКИ - ЭТАП {phase.upper()}\n")
                f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("=" * 80 + "\n")

                for error in errors:
                    f.write(f"❌ {error}\n")

                f.write(f"\nВсего ошибок: {len(errors)}\n")
                f.write("=" * 80 + "\n\n")

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"❌ Ошибка при сохранении файла ошибок: {e}"))

    def _save_interrupted_results(self):
        """Сохраняет результаты при прерывании"""
        self.stdout.write("\n💾 Сохранение промежуточных результатов...")
        self._save_analysis_results()
        self._save_found_and_skipped_words()
        if self.analysis_stats['errors']:
            self._save_errors_to_file(self.analysis_stats['errors'], "analysis_interrupted")
        self.stdout.write("✅ Промежуточные результаты сохранены")

    def _get_games_queryset(self):
        """Возвращает QuerySet игр для анализа"""
        if self.game_name:
            exact_matches = Game.objects.filter(name__iexact=self.game_name)

            if exact_matches.exists():
                games = exact_matches.order_by('-rating', '-rating_count', 'id')[:1]
            else:
                games = Game.objects.filter(Q(name__icontains=self.game_name)).order_by(
                    '-rating', '-rating_count', 'id')[:1]

            if self.verbose and games.exists():
                game = games.first()
                self.stdout.write(f"🔍 Найдена игра: {game.name}")

            return games

        return Game.objects.all().order_by('id')

    def _get_stop_words(self) -> Set[str]:
        """Возвращает множество стоп-слов"""
        return {
            # Существующие стоп-слова
            'a', 'an', 'the', 'this', 'that', 'these', 'those',
            'about', 'above', 'across', 'after', 'against', 'along', 'among', 'around', 'at',
            'before', 'behind', 'below', 'beneath', 'beside', 'between', 'beyond', 'by',
            'down', 'during', 'except', 'for', 'from', 'in', 'inside', 'into', 'like',
            'near', 'of', 'off', 'on', 'onto', 'out', 'outside', 'over', 'past',
            'through', 'throughout', 'to', 'toward', 'under', 'underneath', 'until', 'up',
            'upon', 'with', 'within', 'without',
            'and', 'but', 'or', 'nor', 'so', 'yet', 'although', 'because', 'since',
            'unless', 'until', 'while', 'as', 'if', 'than', 'that', 'though', 'when',
            'i', 'you', 'he', 'she', 'it', 'we', 'they',
            'me', 'him', 'her', 'us', 'them',
            'my', 'your', 'his', 'its', 'our', 'their',
            'who', 'whom', 'whose', 'which', 'what',
            'be', 'am', 'is', 'are', 'was', 'were', 'been', 'being',
            'have', 'has', 'had', 'having',
            'do', 'does', 'did', 'doing',
            'will', 'would', 'can', 'could', 'may', 'might', 'must', 'shall', 'should',
            'both', 'each', 'every', 'either', 'neither', 'any', 'some', 'all',
            'much', 'many', 'few', 'little', 'most', 'more', 'less',
            'other', 'another', 'such', 'same',
            'no', 'nor', 'not', 'none', 'nothing', 'non',
            'now', 'then', 'always', 'never', 'sometimes', 'often', 'usually',
            'already', 'yet', 'still', 'ever', 'once', 'twice',
            'here', 'there', 'near', 'nearby', 'far',
            'very', 'too', 'quite', 'almost', 'nearly', 'just', 'only', 'even',
            'how', 'why', 'when', 'where', 'however',
            'also', 'too', 'as', 'well', 'either',
            'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
            'first', 'second', 'third', 'fourth', 'fifth',

            # === НОВЫЕ ДОБАВЛЕННЫЕ СТОП-СЛОВА ===

            # Местоимения и указательные слова
            'nobody', 'nothing', 'nowhere', 'anybody', 'anything', 'anywhere',
            'somebody', 'something', 'somewhere', 'everybody', 'everything', 'everywhere',
            'whatsoever', 'whereby', 'wherein', 'whereupon', 'wherever', 'whenever',
            'whichever', 'whoever', 'whomever', 'whosever', 'notwithstanding',
            'whereas', 'thence', 'thenceforth', 'therefor', 'therein', 'thereof',
            'thereon', 'thereto', 'thereunder', 'thereupon', 'therewith', 'herein',
            'hereof', 'hereon', 'hereto', 'hereunder', 'hereupon', 'herewith',

            # Разговорные сокращения
            'ain\'t', 'gonna', 'wanna', 'gotta', 'kinda', 'sorta', 'lemme', 'gimme',

            # Модальные глаголы и связки
            'ought', 'used', 'need', 'dare',

            # Числительные и порядковые
            'zero', 'hundreds', 'thousands', 'millions', 'dozen', 'half', 'quarter',
            'sixth', 'seventh', 'eighth', 'ninth', 'tenth', 'eleventh', 'twelfth',
            'thirteenth', 'fourteenth', 'fifteenth', 'sixteenth', 'seventeenth',
            'eighteenth', 'nineteenth', 'twentieth', 'thirtieth', 'fortieth', 'fiftieth',
            'sixtieth', 'seventieth', 'eightieth', 'ninetieth', 'hundredth', 'thousandth',

            # Вспомогательные глаголы
            'being', 'become', 'becomes', 'became', 'seem', 'seems', 'seemed', 'seeming',
            'appear', 'appears', 'appeared', 'appearing',

            # Сравнительные и превосходные степени
            'lesser', 'greater', 'further', 'furthermore', 'moreover', 'likewise',
            'otherwise', 'hence', 'henceforth', 'hitherto', 'thereafter', 'hereafter',
            'whereafter',

            # Союзы и частицы
            'lest', 'albeit', 'provided', 'providing', 'seeing', 'granted',
            'wherewith', 'wherewithal',
        }

    def _create_text_preparer(self):
        """Создаёт объект TextPreparer с комбинированными описаниями"""

        class CommandWrapper:
            def __init__(self):
                self.use_wiki = False
                self.use_rawg = False
                self.use_storyline = False
                self.prefer_wiki = False
                self.prefer_storyline = False
                self.combine_texts = False
                self.combine_all_texts = True

        from games.management.commands.analyzer.text_preparer import TextPreparer
        return TextPreparer(CommandWrapper())