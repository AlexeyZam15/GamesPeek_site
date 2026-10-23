# games/management/commands/analyze_game_criteria_fast_src/command.py

import os
import sys
import time
import signal
import subprocess
import threading
import warnings
from typing import Any, Dict, List, Optional, Set

from django.core.management.base import BaseCommand
from django.db.models import Max

from games.analyze.range_cache import RangeCacheManager

from .loading import LoadingMixin
from .analyzer import AnalyzerMixin
from .parallel import ParallelMixin
from .saving import SavingMixin
from .output import OutputMixin
from .progress import ProgressMixin


class Command(
    LoadingMixin,
    AnalyzerMixin,
    ParallelMixin,
    SavingMixin,
    OutputMixin,
    ProgressMixin,
    BaseCommand,
):
    """
    МАКСИМАЛЬНО УСКОРЕННАЯ команда анализа критериев.
    Логика разбита по миксинам в analyze_game_criteria_fas_src/.
    """

    help = 'Максимально ускоренный анализ критериев и ключевых слов'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.progress_bar = None
        self.output_file = None
        self.original_stdout = None
        self.limit = None
        self.offset = 0
        self.game_name = None
        self.criteria_name = None
        self.filter_genre = None
        self.theme = None
        self.verbose = False
        self.threads = 16
        self.batch_save = 10000
        self.force_restart = False
        self.output_path = None
        self.only_found = False
        self.combine_all_texts = False
        self.no_progress = False
        self.custom_pattern = None
        self.patterns = None
        self.criteria_cache = None
        self.criteria_by_name = None
        self.compiled_patterns = None
        self._interrupted = False
        self.game_ids_filter = None
        self.keyword_ids_filter = None
        self._keyword_trie = None
        self._keyword_trie_lock = None
        # Создание ключевых слов (create + merge под одним флагом)
        self.create_keywords = False
        self.analyze_new_on_old = False
        self.create_keywords_limit = None
        self.max_keyword_id_before = 0
        self.new_keyword_ids: Set[int] = set()
        # Удаление низкочастотных ключевых слов (после анализа)
        self.delete_low_usage = False
        self.low_usage_threshold = 1

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=None,
                            help='Максимальное количество игр для анализа')
        parser.add_argument('--offset', type=int, default=0,
                            help='Пропустить первые N игр')
        parser.add_argument('--game-name', type=str, default=None,
                            help='Название игры для анализа (поиск по частичному совпадению)')
        parser.add_argument('--criteria-name', type=str, default=None,
                            help='Имя критерия для анализа')
        parser.add_argument('--filter-genre', type=str, default=None,
                            help='Фильтровать игры по жанру')
        parser.add_argument('--theme', type=str, default=None,
                            help='Анализировать ТОЛЬКО эту тему')
        parser.add_argument('--verbose', action='store_true',
                            help='Подробный вывод')
        parser.add_argument('--threads', type=int, default=24,
                            help='Количество потоков (по умолчанию 24)')
        parser.add_argument('--force-restart', action='store_true',
                            help='Начать обработку заново, игнорируя кэш')
        parser.add_argument('--output', type=str,
                            help='Путь к файлу для экспорта результатов')
        parser.add_argument('--only-found', action='store_true',
                            help='Показывать только игры где были найдены критерии')
        parser.add_argument('--no-progress', action='store_true',
                            help='Отключить отображение прогресс-бара')
        parser.add_argument('--auto-save', action='store_true',
                            help='Автоматически сохранять в БД без подтверждения')
        parser.add_argument('--no-combine-texts', action='store_true',
                            help='НЕ объединять все источники текста')
        parser.add_argument('--custom-pattern', type=str, default=None,
                            help='Пользовательский паттерн для поиска')
        parser.add_argument('--collect-all-patterns', action='store_true',
                            help='Собирать ВСЕ срабатывания паттернов')
        parser.add_argument('--group-by-pattern', action='store_true',
                            help='Группировать вывод по паттернам')
        parser.add_argument('--keywords', action='store_true',
                            help='Анализировать ключевые слова')
        parser.add_argument('--game-ids-file', type=str, default=None,
                            help='Файл со списком ID игр (по одному на строку)')
        parser.add_argument('--keyword-ids-file', type=str, default=None,
                            help='Файл со списком ID ключевых слов (по одному на строку)')
        # === НОВЫЕ ОПЦИИ ===
        parser.add_argument('--create-keywords', action='store_true',
                            help='Перед анализом создать новые ключевые слова из описаний '
                                 'и сразу объединить дубликаты '
                                 '(add_new_keywords_from_descriptions '
                                 '+ normalize_keywords --merge-only)')
        parser.add_argument('--analyze-new-on-old', action='store_true',
                            help='После основного анализа прогнать только НОВЫЕ ключевые слова '
                                 'по СТАРЫМ играм (требует --create-keywords; '
                                 'если новых ключевых слов нет — подшаг пропускается)')
        parser.add_argument('--create-keywords-limit', type=int, default=None,
                            help='Лимит игр для add_new_keywords_from_descriptions '
                                 '(по умолчанию берётся --limit)')
        parser.add_argument('--delete-low-usage', action='store_true',
                            help='После анализа удалить ключевые слова с низким использованием '
                                 '(delete_low_usage_keywords). '
                                 'Ошибки этой команды НЕ роняют основной анализ.')
        parser.add_argument('--low-usage-threshold', type=int, default=1,
                            help='Порог использования для удаления ключевых слов '
                                 '(по умолчанию: 1). Используется только с --delete-low-usage')

    def handle(self, *args, **options):
        warnings.filterwarnings("ignore", message="No WordNet synset found")
        warnings.filterwarnings("ignore", category=UserWarning, module="nltk")

        stop_flag = threading.Event()

        start_time = time.time()
        results = None
        interrupted = False
        exit_code = 0

        def signal_handler(signum, frame):
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                stop_flag.set()
                os.write(2, b"\n[INTERRUPTED] Stopping...\n")

        original_handler = signal.signal(signal.SIGINT, signal_handler)

        try:
            self.limit = options.get('limit')
            self.offset = options.get('offset', 0)
            self.game_name = options.get('game_name')
            self.criteria_name = options.get('criteria_name')
            self.filter_genre = options.get('filter_genre')
            self.theme = options.get('theme')
            self.verbose = options.get('verbose', False)
            self.threads = min(options.get('threads', 16), 32)
            self.force_restart = options.get('force_restart', False)
            self.output_path = options.get('output')
            self.only_found = options.get('only_found', False)
            self.no_progress = options.get('no_progress', False)
            self.auto_save = options.get('auto_save', False)
            self.custom_pattern = options.get('custom_pattern')
            self.collect_all_patterns = options.get('collect_all_patterns', False)
            self.group_by_pattern = options.get('group_by_pattern', False)
            self.keywords_mode = options.get('keywords', False)
            # Новые опции
            self.create_keywords = options.get('create_keywords', False)
            self.analyze_new_on_old = options.get('analyze_new_on_old', False)
            self.create_keywords_limit = options.get('create_keywords_limit')
            self.delete_low_usage = options.get('delete_low_usage', False)
            self.low_usage_threshold = options.get('low_usage_threshold', 1)

            game_ids_file = options.get('game_ids_file')
            keyword_ids_file = options.get('keyword_ids_file')

            if game_ids_file:
                self.game_ids_filter = self._load_game_ids_file(game_ids_file)
                if not self.game_ids_filter:
                    sys.stderr.write(f"   ⚠️ Файл {game_ids_file} пуст или не найден\n")
                    sys.stderr.flush()
                else:
                    sys.stderr.write(
                        f"   📂 Загружено {len(self.game_ids_filter)} ID игр из {game_ids_file}\n"
                    )
                    sys.stderr.flush()

            if keyword_ids_file:
                self.keyword_ids_filter = self._load_keyword_ids_file(keyword_ids_file)
                if not self.keyword_ids_filter:
                    sys.stderr.write(f"   ⚠️ Файл {keyword_ids_file} пуст или не найден\n")
                    sys.stderr.flush()
                else:
                    sys.stderr.write(
                        f"   📂 Загружено {len(self.keyword_ids_filter)} ID ключевых слов из {keyword_ids_file}\n"
                    )
                    sys.stderr.flush()

            self.update_game = True
            self.combine_all_texts = not options.get('no_combine_texts', False)

            self.output_file = None
            self.original_stdout = None
            if self.output_path:
                self._setup_output_file()

            self._print_startup_info()

            if self.force_restart:
                sys.stderr.write("\n🧹 Очищаем кэш...\n")
                sys.stderr.flush()
                RangeCacheManager.clear_all_games()
                sys.stderr.write("   ✅ Готово\n")
                sys.stderr.flush()

            # === НОВОЕ: создание ключевых слов (включая merge) ДО _preload_all_data ===
            if self.create_keywords:
                self._run_create_keywords()

            sys.stderr.write("\n📚 Загрузка данных...\n")
            sys.stderr.flush()
            self._preload_all_data()

            sys.stderr.write("\n🎮 Загрузка игр...\n")
            sys.stderr.flush()
            games_to_analyze = self._get_games_to_analyze()

            if self.filter_genre:
                games_to_analyze = self._filter_games_by_genre(games_to_analyze)

            sys.stderr.write(f"   ✅ Получено {len(games_to_analyze)} игр\n")
            sys.stderr.flush()

            if not games_to_analyze:
                sys.stderr.write("\n✅ Нет игр для анализа\n")
                sys.stderr.flush()
                if self.analyze_new_on_old:
                    self._run_analyze_new_keywords_on_old_games()
                if self.delete_low_usage:
                    self._run_delete_low_usage()
                self._cleanup()
                return

            sys.stderr.write("\n📚 Загрузка существующих связей для проверки...\n")
            sys.stderr.flush()
            existing_relations = self._load_existing_relations()
            sys.stderr.write(f"   ✅ Загружено связей: жанры={len(existing_relations['genres'])}, "
                             f"темы={len(existing_relations['themes'])}, "
                             f"перспективы={len(existing_relations['perspectives'])}, "
                             f"режимы={len(existing_relations['game_modes'])}, "
                             f"ключевые слова={len(existing_relations.get('keywords', set()))}\n")
            sys.stderr.flush()

            self.progress_bar = None
            os.write(2, b"\n[START] Parallel analysis...\n")
            results = self._parallel_analysis(games_to_analyze, existing_relations)

            if results:
                self._process_results(results, start_time)

            if self.analyze_new_on_old:
                self._run_analyze_new_keywords_on_old_games()

            if self.delete_low_usage:
                self._run_delete_low_usage()

        except KeyboardInterrupt:
            self._handle_interrupt(results)
            exit_code = 130
        except Exception as e:
            sys.stderr.write(f"\n❌ Ошибка: {e}\n")
            sys.stderr.flush()
            import traceback
            traceback.print_exc(file=sys.stderr)
            exit_code = 1
        finally:
            signal.signal(signal.SIGINT, original_handler)
            self._cleanup()
            sys.stderr.write("\n✨ Завершено\n")
            sys.stderr.flush()

        # Код возврата: 0 при успехе, 1 при ошибке, 130 при Ctrl+C.
        # Не вызываем sys.exit(0) в finally — иначе ошибки маскируются.
        if exit_code:
            sys.exit(exit_code)

    # === НОВЫЕ МЕТОДЫ: создание ключевых слов (create + merge) ===

    def _call_subcommand(self, command_name: str, args: List[str]) -> int:
        """Вызывает дочернюю manage.py-команду через subprocess (как в full_pipeline)."""
        cmd = [sys.executable, 'manage.py', command_name] + args
        sys.stderr.write(f"\n   ▶️ Выполняется: {' '.join(cmd)}\n")
        sys.stderr.flush()
        result = subprocess.run(cmd, check=False)
        if result.returncode != 0:
            raise RuntimeError(
                f'Команда {command_name} завершилась с кодом {result.returncode}'
            )
        return result.returncode

    def _run_create_keywords(self):
        """
        Создаёт новые ключевые слова из описаний и сразу объединяет дубликаты.

        Шаги:
          1. add_new_keywords_from_descriptions
             (БЕЗ --delete-stopwords! Иначе команда завершится сразу после
              удаления стоп-слов и НЕ создаст ключевые слова.)
             max_keyword_id_before фиксируется ДО вызова.
          2. normalize_keywords --merge-only
          3. пересборка new_keyword_ids — только выжившие (id > max_keyword_id_before)

        Для разовой очистки БД от стоп-слов используйте отдельно:
            python manage.py add_new_keywords_from_descriptions --delete-stopwords
        """
        from games.models import Keyword

        sys.stderr.write("\n" + "=" * 70 + "\n")
        sys.stderr.write("🔑 СОЗДАНИЕ НОВЫХ КЛЮЧЕВЫХ СЛОВ ИЗ ОПИСАНИЙ (+ MERGE ДУБЛИКАТОВ)\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.flush()

        # --- Шаг 1: фиксируем max(id) и создаём ключевые слова ---
        self.max_keyword_id_before = Keyword.objects.aggregate(Max('id'))['id__max'] or 0
        sys.stderr.write(f"   📌 max(keyword.id) до add_new_keywords: {self.max_keyword_id_before}\n")
        sys.stderr.flush()

        create_args = []  # БЕЗ --delete-stopwords

        effective_limit = self.create_keywords_limit
        if effective_limit is None:
            effective_limit = self.limit
        if effective_limit is not None:
            create_args.extend(['--limit', str(effective_limit)])
        if self.offset:
            create_args.extend(['--offset', str(self.offset)])
        if self.verbose:
            create_args.append('--verbose')

        self._call_subcommand('add_new_keywords_from_descriptions', create_args)

        created_count = Keyword.objects.filter(id__gt=self.max_keyword_id_before).count()
        sys.stderr.write(
            f"   📊 Создано новых ключевых слов (id > {self.max_keyword_id_before}): {created_count}\n"
        )
        if created_count == 0:
            sys.stderr.write(
                "   ⚠️ Новых ключевых слов НЕ создано.\n"
                "      Возможные причины:\n"
                "      - все слова уже есть в БД (нормализация WordNet дала существующие имена);\n"
                "      - игры без текста / текст короче min_text_length (50);\n"
                "      - все слова — стоп-слова или не найдены в WordNet.\n"
            )
        sys.stderr.flush()

        if self.max_keyword_id_before == 0:
            sys.stderr.write(
                "   ℹ️ max_keyword_id_before = 0: считаем все текущие ключевые слова «новыми»\n"
            )
            sys.stderr.flush()

        # --- Шаг 2: merge дубликатов ---
        sys.stderr.write("\n   🔄 Объединение дубликатов (normalize_keywords --merge-only)...\n")
        sys.stderr.flush()

        merge_args = ['--merge-only']
        if self.verbose:
            merge_args.append('--verbose')

        self._call_subcommand('normalize_keywords', merge_args)

        # --- Пересборка выживших новых ключевых слов ---
        surviving = Keyword.objects.filter(id__gt=self.max_keyword_id_before)
        self.new_keyword_ids = set(surviving.values_list('id', flat=True))
        sys.stderr.write(
            f"   📊 Выживших новых ключевых слов после merge: {len(self.new_keyword_ids)}\n"
        )
        sys.stderr.flush()

    def _run_analyze_new_keywords_on_old_games(self):
        """Прогоняет только новые ключевые слова по старым играм (как в full_pipeline)."""
        from games.models import Game

        sys.stderr.write("\n" + "=" * 70 + "\n")
        sys.stderr.write("🔄 ПОДШАГ: НОВЫЕ КЛЮЧЕВЫЕ СЛОВА ПО СТАРЫМ ИГРАМ\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.flush()

        if not self.new_keyword_ids:
            sys.stderr.write(
                "   ⏭️ Нет новых ключевых слов (после merge) — подшаг пропущен\n"
                "   ℹ️ Убедитесь, что указан --create-keywords в этом же запуске "
                "и что новые ключевые слова реально создались\n"
            )
            sys.stderr.flush()
            return

        sys.stderr.write(f"   🎯 Новых ключевых слов: {len(self.new_keyword_ids)}\n")
        sys.stderr.flush()

        new_kw_file = 'temp_new_keyword_ids.txt'
        try:
            with open(new_kw_file, 'w', encoding='utf-8') as f:
                for kw_id in sorted(self.new_keyword_ids):
                    f.write(f'{kw_id}\n')
            sys.stderr.write(f"   📁 Создан {new_kw_file}: {len(self.new_keyword_ids)} ID\n")
            sys.stderr.flush()
        except Exception as e:
            sys.stderr.write(f"   ⚠️ Ошибка записи {new_kw_file}: {e}\n")
            sys.stderr.flush()
            return

        loaded_set = set(self.game_ids_filter) if self.game_ids_filter else set()
        if loaded_set:
            sys.stderr.write(
                f"   📊 Исключаем игры из --game-ids-file (новые): {len(loaded_set)}\n"
            )
        else:
            sys.stderr.write(
                "   ℹ️ --game-ids-file не задан: все игры БД считаются «старыми»\n"
            )
        sys.stderr.flush()

        old_game_ids = []
        try:
            qs = Game.objects.all().only('id').order_by('id')
            for gid in qs.values_list('id', flat=True).iterator(chunk_size=10000):
                if gid not in loaded_set:
                    old_game_ids.append(gid)
        except Exception as e:
            sys.stderr.write(f"   ⚠️ Ошибка сбора старых игр: {e}\n")
            sys.stderr.flush()
            return

        if not old_game_ids:
            sys.stderr.write("   ⏭️ Нет старых игр — подшаг пропущен\n")
            sys.stderr.flush()
            return

        old_games_file = 'temp_old_game_ids.txt'
        try:
            with open(old_games_file, 'w', encoding='utf-8') as f:
                for gid in old_game_ids:
                    f.write(f'{gid}\n')
            sys.stderr.write(f"   📁 Создан {old_games_file}: {len(old_game_ids)} ID\n")
            sys.stderr.flush()
        except Exception as e:
            sys.stderr.write(f"   ⚠️ Ошибка записи {old_games_file}: {e}\n")
            sys.stderr.flush()
            return

        args = [
            f'--threads={self.threads}',
            '--keywords',
            '--keyword-ids-file', new_kw_file,
            '--game-ids-file', old_games_file,
        ]
        if self.auto_save:
            args.append('--auto-save')
        args.append('--collect-all-patterns')
        if self.verbose:
            args.append('--verbose')

        sys.stderr.write(
            f"   ▶️ Запуск: analyze_game_criteria_fast --keywords "
            f"--keyword-ids-file {new_kw_file} --game-ids-file {old_games_file} "
            f"(новых ключевых слов: {len(self.new_keyword_ids)}, "
            f"старых игр: {len(old_game_ids)})\n"
        )
        sys.stderr.flush()

        try:
            self._call_subcommand('analyze_game_criteria_fast', args)
        except RuntimeError as e:
            sys.stderr.write(
                f"   ⚠️ Подшаг analyze-new-on-old завершился с ошибкой: {e}\n"
                f"      (основной анализ уже выполнен, продолжаем)\n"
            )
            sys.stderr.flush()

    def _run_delete_low_usage(self):
        """
        Удаляет ключевые слова с низким использованием (delete_low_usage_keywords).

        Ошибки дочерней команды НЕ пробрасываются: анализ уже завершён,
        падать из-за delete нельзя. Пишем warning и продолжаем.
        """
        sys.stderr.write("\n" + "=" * 70 + "\n")
        sys.stderr.write("🗑️ УДАЛЕНИЕ НИЗКОЧАСТОТНЫХ КЛЮЧЕВЫХ СЛОВ\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.flush()

        args = [f'--usage={self.low_usage_threshold}']
        if self.verbose:
            args.append('--verbose')

        sys.stderr.write(
            f"   📊 Порог использования: {self.low_usage_threshold}\n"
        )
        sys.stderr.flush()

        try:
            self._call_subcommand('delete_low_usage_keywords', args)
        except RuntimeError as e:
            sys.stderr.write(
                f"\n   ⚠️ delete_low_usage_keywords завершилась с ошибкой: {e}\n"
                f"      Основной анализ уже сохранён, удаление низкочастотных пропущено.\n"
                f"      Проверьте логи выше (возможно, баг в delete_low_usage_keywords /\n"
                f"      games/models_parts/keywords.py — Count('game') не работает\n"
                f"      с ArrayField keyword_ids).\n"
            )
            sys.stderr.flush()
            return

        try:
            from games.analyze.keyword_trie import KeywordTrieManager
            KeywordTrieManager().clear_cache()
            sys.stderr.write("   ✅ Кэш Trie очищен\n")
            sys.stderr.flush()
        except Exception:
            pass

    def _print_startup_info(self):
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write("🚀 МАКСИМАЛЬНО УСКОРЕННЫЙ АНАЛИЗ КРИТЕРИЕВ\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write(f"🔧 Потоков: {self.threads}\n")
        sys.stderr.write(f"🔄 Обновление БД: {'✅' if self.update_game else '❌'}\n")
        if self.update_game:
            sys.stderr.write(f"💾 Режим сохранения: {'Авто' if self.auto_save else 'С подтверждением после анализа'}\n")
        sys.stderr.write(f"📄 Вывод в файл: {'✅' if self.output_path else '❌'}\n")
        sys.stderr.write(f"📚 Объединять все тексты: {'✅' if self.combine_all_texts else '❌'}\n")
        sys.stderr.write(f"📊 Собирать все паттерны: {'✅' if self.collect_all_patterns else '❌'}\n")
        sys.stderr.write(f"📁 Группировка по паттернам: {'✅' if self.group_by_pattern else '❌'}\n")
        # Новые опции
        sys.stderr.write(
            f"🔑 Создавать ключевые слова (включая merge дубликатов): "
            f"{'✅' if self.create_keywords else '❌'}\n"
        )
        sys.stderr.write(f"🆕 Анализ новых ключевых слов по старым играм: {'✅' if self.analyze_new_on_old else '❌'}\n")
        sys.stderr.write(
            f"🗑️ Удалять низкочастотные ключевые слова: "
            f"{'✅' if self.delete_low_usage else '❌'}"
        )
        if self.delete_low_usage:
            sys.stderr.write(f" (порог: {self.low_usage_threshold})")
        sys.stderr.write("\n")
        if self.create_keywords:
            effective = self.create_keywords_limit if self.create_keywords_limit is not None else self.limit
            sys.stderr.write(
                f"   ↳ Лимит для создания ключевых слов: {effective if effective is not None else 'БЕЗ ЛИМИТА'}\n")
        if self.analyze_new_on_old and not self.create_keywords:
            sys.stderr.write(
                "   ⚠️ --analyze-new-on-old без --create-keywords: "
                "новых ключевых слов не будет, подшаг пропустится\n"
            )
        if self.game_name:
            sys.stderr.write(f"🎮 Поиск по имени: '{self.game_name}'\n")
        if self.game_ids_filter:
            sys.stderr.write(f"🎯 Фильтр по ID игр: {len(self.game_ids_filter)} ID\n")
        if self.keyword_ids_filter:
            sys.stderr.write(f"🎯 Фильтр по ID ключевых слов: {len(self.keyword_ids_filter)} ID\n")
        if self.criteria_name:
            sys.stderr.write(f"🎯 Анализировать критерий: '{self.criteria_name}' (поиск во всех типах)\n")
        if self.filter_genre:
            sys.stderr.write(f"🎭 Фильтровать игры по жанру: '{self.filter_genre}'\n")
        if self.theme:
            sys.stderr.write(f"📚 Анализировать ТОЛЬКО тему: '{self.theme}'\n")
        if self.custom_pattern:
            sys.stderr.write(f"🔧 Пользовательский паттерн: '{self.custom_pattern}'\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.flush()

    def _print_statistics(self, stats: Dict, start_time: float, interrupted: bool = False):
        total_time = time.time() - start_time

        saved_count = len(stats.get('to_save', [])) if self.update_game else 0

        not_found_count = stats['processed'] - stats['with_results'] - stats['skipped']
        if not_found_count < 0:
            not_found_count = 0

        sys.stderr.write("\n" + "=" * 70 + "\n")
        if interrupted:
            sys.stderr.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО)\n")
        else:
            sys.stderr.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write(f"🔄 Обработано игр: {stats['processed']}\n")
        sys.stderr.write(f"🎯 Игр с найденными критериями: {stats['with_results']}\n")
        sys.stderr.write(f"📈 Всего критериев найдено: {stats['total_found']}\n")
        sys.stderr.write(f"⏭️ Пропущено (нет текста/короткий текст): {stats['skipped']}\n")
        sys.stderr.write(f"⚪ Игр без найденных элементов: {not_found_count}\n")
        sys.stderr.write(f"❌ Ошибок: {stats['errors']}\n")

        if self.update_game and saved_count > 0:
            sys.stderr.write(f"💾 Сохранено в БД: {saved_count} игр\n")

        sys.stderr.write(f"⏱️ Время выполнения: {total_time:.1f} секунд\n")

        if stats.get('games_per_second', 0) > 0:
            sys.stderr.write(f"⚡ Средняя скорость: {stats['games_per_second']:.0f} игр/сек\n")

        sys.stderr.write("=" * 70 + "\n")

        if self.output_path:
            sys.stderr.write(f"✅ Результаты сохранены в: {self.output_path}\n")

        if interrupted:
            sys.stderr.write("\n⚠️ Анализ был прерван. Частичные результаты сохранены.\n")
        else:
            sys.stderr.write("\n✨ Анализ успешно завершен!\n")
        sys.stderr.flush()

    def _process_results(self, results: Dict[str, Any], start_time: float):
        """Обрабатывает результаты анализа: сохраняет файл, выводит статистику, спрашивает про БД"""

        if self.output_path and results.get('all_results'):
            sys.stderr.write(f"\n📝 Запись результатов в файл...\n")
            sys.stderr.flush()

            if self.group_by_pattern:
                self._output_results_grouped_by_pattern(results['all_results'])
            else:
                self._output_results(results['all_results'])

            sys.stderr.write(f"   ✅ Результаты сохранены в: {self.output_path}\n")
            sys.stderr.flush()

            self._cleanup()
            sys.stderr.write(f"   ✅ Файл закрыт и сохранен на диск\n")
            sys.stderr.flush()

        total_time = time.time() - start_time
        not_found = results['processed'] - results['with_results'] - results['skipped']
        if not_found < 0:
            not_found = 0

        def format_time(seconds):
            if seconds < 60:
                return f"{seconds:.0f} сек"
            elif seconds < 3600:
                minutes = seconds / 60
                return f"{minutes:.1f} мин"
            else:
                hours = seconds / 3600
                return f"{hours:.1f} ч"

        sys.stderr.write("\n" + "=" * 70 + "\n")
        if results.get('interrupted'):
            sys.stderr.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО)\n")
        else:
            sys.stderr.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write(f"🔄 Обработано: {results['processed']} игр\n")
        sys.stderr.write(f"🎯 Игр с найденными критериями: {results['with_results']}\n")
        sys.stderr.write(f"✨ Игр с НОВЫМИ критериями: {results['with_new_results']}\n")
        sys.stderr.write(f"📈 Всего критериев: {results['total_found']} (новых: {results['total_new_found']})\n")
        sys.stderr.write(f"⏭️ Пропущено: {results['skipped']}\n")
        sys.stderr.write(f"⚪ Без элементов: {not_found}\n")
        sys.stderr.write(f"❌ Ошибок: {results['errors']}\n")
        sys.stderr.write(f"⏱️ Время: {format_time(total_time)}\n")
        if results.get('games_per_second', 0) > 0:
            sys.stderr.write(f"⚡ Скорость: {results['games_per_second']:.0f} игр/сек\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.flush()

        if not self.auto_save and results.get('to_save') and not results.get('interrupted'):
            sys.stderr.write(
                f"\n💾 Найдено {len(results['to_save'])} игр с новыми критериями ({results['total_new_found']} новых элементов).\n")
            sys.stderr.write("Сохранить результаты в БД? (y/N): ")
            sys.stderr.flush()

            try:
                answer = sys.stdin.readline().strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = 'n'

            if answer in ['y', 'yes', 'да', 'д', '1']:
                sys.stderr.write("\n✅ Сохраняем в БД...\n")
                sys.stderr.flush()
                self._bulk_save_results(results['to_save'])
                sys.stderr.write("   ✅ Сохранение в БД завершено\n")
                sys.stderr.flush()
            else:
                sys.stderr.write("\n⏭️ Сохранение в БД отменено\n")
                sys.stderr.flush()
        elif self.auto_save and results.get('to_save') and not results.get('interrupted'):
            sys.stderr.write(f"\n💾 Авто-сохранение {len(results['to_save'])} игр в БД...\n")
            sys.stderr.flush()
            self._bulk_save_results(results['to_save'])
            sys.stderr.write("   ✅ Сохранение в БД завершено\n")
            sys.stderr.flush()
        elif results.get('interrupted') and results.get('to_save'):
            if self.auto_save:
                sys.stderr.write(f"\n💾 Авто-сохранение {len(results['to_save'])} игр в БД (прервано)...\n")
                sys.stderr.flush()
                self._bulk_save_results(results['to_save'])
                sys.stderr.write("   ✅ Сохранение в БД завершено\n")
                sys.stderr.flush()
            else:
                sys.stderr.write(
                    f"\n💾 Найдено {len(results['to_save'])} игр с новыми критериями ({results['total_new_found']} новых элементов).\n")
                sys.stderr.write("Сохранить результаты в БД? (y/N): ")
                sys.stderr.flush()

                try:
                    answer = sys.stdin.readline().strip().lower()
                except (EOFError, KeyboardInterrupt):
                    answer = 'n'

                if answer in ['y', 'yes', 'да', 'д', '1']:
                    sys.stderr.write("\n✅ Сохраняем в БД...\n")
                    sys.stderr.flush()
                    self._bulk_save_results(results['to_save'])
                    sys.stderr.write("   ✅ Сохранение в БД завершено\n")
                    sys.stderr.flush()
                else:
                    sys.stderr.write("\n⏭️ Сохранение в БД отменено\n")
                    sys.stderr.flush()

    def _handle_interrupt(self, results: Dict[str, Any]):
        """Обрабатывает прерывание пользователя"""
        sys.stderr.write("\n⏹️ Прервано пользователем\n")
        sys.stderr.flush()

        if results and results.get('all_results') and self.output_path:
            sys.stderr.write(f"\n📝 Сохранение результатов в файл (прервано)...\n")
            sys.stderr.flush()
            try:
                self._output_results(results['all_results'])
                self._cleanup()
                sys.stderr.write(f"   ✅ Результаты сохранены в: {self.output_path}\n")
                sys.stderr.flush()
            except Exception as e:
                sys.stderr.write(f"   ⚠️ Ошибка сохранения файла: {e}\n")
                sys.stderr.flush()

        if results and results.get('to_save'):
            sys.stderr.write(f"\n💾 Найдено {len(results['to_save'])} игр с новыми критериями.\n")
            sys.stderr.write("Сохранить результаты в БД? (y/N): ")
            sys.stderr.flush()

            try:
                answer = sys.stdin.readline().strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = 'n'

            if answer in ['y', 'yes', 'да', 'д', '1']:
                sys.stderr.write("\n✅ Сохраняем в БД...\n")
                sys.stderr.flush()
                try:
                    self._bulk_save_results(results['to_save'])
                    sys.stderr.write("   ✅ Сохранение в БД завершено\n")
                    sys.stderr.flush()
                except Exception as e:
                    sys.stderr.write(f"   ⚠️ Ошибка: {e}\n")
                    sys.stderr.flush()
            else:
                sys.stderr.write("\n⏭️ Сохранение в БД отменено\n")
                sys.stderr.flush()