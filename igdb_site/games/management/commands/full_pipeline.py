# games/management/commands/full_pipeline.py

"""
Единая команда для выполнения полного пайплайна обработки данных.

Просто последовательно вызывает существующие команды в правильном порядке.
Поддерживает фильтрацию через аргументы команд.

Порядок шагов:
  1. load_games
  2. import_wiki_gameplay
  3. fetch_steam_descriptions
  4. fill_descriptions_from_steamdb
  5. add_new_keywords_from_descriptions   (запоминает max_keyword_id до вызова)
     - ВСЕГДА сначала отдельный вызов с --delete-stopwords (чистка стоп-слов)
     - затем основной вызов БЕЗ --delete-stopwords (создание новых ключевых слов)
  6. normalize_keywords                   (пересобирает temp_new_keyword_ids.txt — только выжившие новые)
  7. criteries_reassign
  8. analyze_game_criteria_fast           (основной анализ: критерии + ключевые слова)
     + подшаг: только НОВЫЕ ключевые слова по СТАРЫМ играм
               (--keyword-ids-file temp_new_keyword_ids.txt --game-ids-file temp_old_game_ids.txt)
  9. update_vectors                       (можно пропустить через --skip-update-vectors)
 10. delete_low_usage_keywords

РЕЖИМ ПЕРЕЗАГРУЗКИ КОНКРЕТНЫХ ИГР ПО IGDB ID:
  --reload-game-igdb-id N [N ...]
    Для каждого IGDB ID:
      • найти игру в БД, запомнить name, удалить её;
      • вызвать load_games --game-names "<name1>,<name2>,..." (перезагрузка из IGDB);
      • найти новые Game.id по igdb_id, записать в temp_reload_game_ids.txt;
      • вызвать analyze_game_criteria_fast --game-ids-file temp_reload_game_ids.txt
        (критерии, затем ключевые слова).
    Остальные шаги пайплайна в этом режиме пропускаются.

ВАЖНО: пайплайн выполняет создание/нормализацию ключевых слов и анализ новых
ключевых слов по старым играм ЧЕРЕЗ ВНЕШНИЕ ШАГИ (отдельные вызовы manage.py),
а НЕ через флаги --create-keywords / --analyze-new-on-old команды
analyze_game_criteria_fast.

Флаги --dry-run / --verbose передаются только тем дочерним командам, которые
их реально объявляют в add_arguments. Для команд без этих флагов
(load_games, import_wiki_gameplay, check_steam_games, criteries_reassign,
update_vectors, delete_low_usage_keywords, analyze_game_criteria_fast*)
соответствующие флаги не передаются, а в режиме --dry-run такие шаги
пропускаются целиком или логируются без выполнения.

* analyze_game_criteria_fast не объявляет --dry-run — в dry-run шаг
  логируется (три вызова), но не выполняется.

Чистка стоп-слов выполняется ВСЕГДА на шаге ADD_KEYWORDS:
  отдельный вызов add_new_keywords_from_descriptions --delete-stopwords
  перед основным вызовом создания новых ключевых слов.

Ctrl+C в дочернем процессе (returncode 3221225786 / 130 / -2 / -1073741510)
трактуется как «прервано пользователем» — пайплайн останавливается штатно,
без RuntimeError и трейсбека, шаг помечается как «прервано».

Точные имена жанров в БД (для --genres, в кавычках):
  • "Role-playing (RPG)"            — 48 411 игр
  • "Strategy"                      — 55 316 игр
  • "Real Time Strategy (RTS)"      —  3 968 игр
  • "Turn-based strategy (TBS)"     —  5 909 игр
  • "Simulator"                     — 13 944 игр
  • "Squad Management"              — 13 392 игр
  • "Tactical"                      —  8 722 игр

Примеры:
    python manage.py full_pipeline --limit 10 --genres "Role-playing (RPG)" --dry-run
    python manage.py full_pipeline --limit 10 --genres "Role-playing (RPG)"
    python manage.py full_pipeline --limit 10 --genres "Role-playing (RPG)" --skip-update-vectors
    python manage.py full_pipeline --limit 10 --text-contains "rpg" --dry-run
    python manage.py full_pipeline --limit 100 --genres "Role-playing (RPG)" --output report.txt
    python manage.py full_pipeline --start-from fill_descriptions_from_steamdb --stop-after fill_descriptions_from_steamdb
    python manage.py full_pipeline --reload-game-igdb-id 12345
    python manage.py full_pipeline --reload-game-igdb-id 12345 67890 --skip-update-vectors
"""

from django.core.management.base import BaseCommand
from django.db.models import Max
import subprocess
import sys
import time
import os
from datetime import datetime
from typing import List, Optional, Dict, Any, Set
from enum import Enum
import signal


class PipelineInterrupted(Exception):
    """Пайплайн прерван пользователем (Ctrl+C в дочернем процессе)."""
    pass


# Коды возврата, соответствующие Ctrl+C:
#   Windows: 3221225786 = 0xC000013A (STATUS_CONTROL_C_EXIT), signed -1073741510
#   Unix:    130 (128+SIGINT), -2 (SIGINT)
CTRL_C_RETURNCODES = {3221225786, -1073741510, 130, -2}


class PipelineStep(Enum):
    """Этапы пайплайна."""
    LOAD_GAMES = "load_games"
    IMPORT_WIKI = "import_wiki_gameplay"
    FETCH_STEAM = "fetch_steam_descriptions"
    FILL_FROM_STEAMDB = "fill_descriptions_from_steamdb"
    ADD_KEYWORDS = "add_new_keywords_from_descriptions"
    NORMALIZE_KEYWORDS = "normalize_keywords"
    CRITERIES_REASSIGN = "criteries_reassign"
    ANALYZE_CRITERIA = "analyze_game_criteria_fast"
    UPDATE_VECTORS = "update_vectors"
    DELETE_LOW_USAGE = "delete_low_usage_keywords"


class Command(BaseCommand):
    """
    Единая команда для выполнения полного пайплайна обработки данных.

    Режим перезагрузки конкретных игр по IGDB ID:
      --reload-game-igdb-id N [N ...]
        Удаляет игры по igdb_id, перезагружает через load_games --game-names,
        анализирует через analyze_game_criteria_fast --game-ids-file.
        Остальные шаги пропускаются.

    Чистка стоп-слов выполняется ВСЕГДА на шаге ADD_KEYWORDS.

    --dry-run / --verbose передаются только тем дочерним командам, которые их
    реально объявляют. analyze_game_criteria_fast --dry-run не объявляет —
    в dry-run шаг логируется (три вызова), но не выполняется.

    Ctrl+C в дочернем процессе трактуется как «прервано пользователем» —
    пайплайн останавливается штатно, без RuntimeError и трейсбека.

    Точные имена жанров в БД (для --genres, в кавычках):
      • "Role-playing (RPG)"
      • "Strategy"
      • "Real Time Strategy (RTS)"
      • "Turn-based strategy (TBS)"
      • "Simulator"
      • "Squad Management"
      • "Tactical"

    Примеры:
        python manage.py full_pipeline --limit 10 --genres "Role-playing (RPG)" "Strategy" --dry-run
        python manage.py full_pipeline --limit 10 --genres "Role-playing (RPG)" --dry-run
        python manage.py full_pipeline --limit 10 --genres "Role-playing (RPG)" --skip-update-vectors
        python manage.py full_pipeline --limit 10 --text-contains "rpg" --dry-run
        python manage.py full_pipeline --limit 100 --genres "Role-playing (RPG)" "Strategy" --output report.txt
        python manage.py full_pipeline --reload-game-igdb-id 12345
    """

    help = 'Выполняет полный пайплайн обработки данных от загрузки до анализа'

    def add_arguments(self, parser):
        """Добавляет аргументы командной строки."""
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Общий лимит игр для обработки (по умолчанию: без лимита)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Режим просмотра: показывает команды без выполнения'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод'
        )
        parser.add_argument(
            '--skip-errors',
            action='store_true',
            help='Пропускать ошибки и продолжать выполнение'
        )
        parser.add_argument(
            '--skip-update-vectors',
            action='store_true',
            help='Пропустить шаг UPDATE_VECTORS (обновление векторов по всей БД — долгий шаг)'
        )
        parser.add_argument(
            '--start-from',
            type=str,
            choices=[step.value for step in PipelineStep],
            help='Начать выполнение с указанного шага'
        )
        parser.add_argument(
            '--stop-after',
            type=str,
            choices=[step.value for step in PipelineStep],
            help='Остановиться после указанного шага'
        )
        parser.add_argument(
            '--genres',
            type=str,
            nargs='+',
            default=[],
            help=(
                'Фильтр по жанрам (логика И между жанрами). '
                'Точные имена из БД (в кавычках): '
                '"Role-playing (RPG)", "Strategy", "Real Time Strategy (RTS)", '
                '"Turn-based strategy (TBS)", "Simulator", "Squad Management", "Tactical". '
                'Пример: --genres "Role-playing (RPG)" "Strategy"'
            )
        )
        parser.add_argument(
            '--text-contains',
            type=str,
            nargs='+',
            default=[],
            help='Фильтр по тексту в описании (логика ИЛИ). Пример: --text-contains "open world" "rpg"'
        )
        parser.add_argument(
            '--steam-workers',
            type=int,
            default=16,
            help='Количество воркеров для Steam (по умолчанию: 16)'
        )
        parser.add_argument(
            '--analyze-threads',
            type=int,
            default=24,
            help='Количество потоков для анализа (по умолчанию: 24)'
        )
        parser.add_argument(
            '--steam-delay',
            type=float,
            default=0.1,
            help='Задержка между запросами к Steam (по умолчанию: 0.1)'
        )
        parser.add_argument(
            '--delete-low-usage',
            action='store_true',
            help='Удалить ключевые слова с низким использованием после всех операций'
        )
        parser.add_argument(
            '--low-usage-threshold',
            type=int,
            default=1,
            help='Порог использования для удаления ключевых слов (по умолчанию: 1)'
        )
        parser.add_argument(
            '--delete-stopwords',
            action='store_true',
            help='[УСТАРЕЛ] Чистка стоп-слов выполняется ВСЕГДА на шаге ADD_KEYWORDS. '
                 'Флаг оставлен для совместимости и ни на что не влияет.'
        )
        parser.add_argument(
            '--no-auto-save',
            action='store_true',
            help='Отключить автоматическое сохранение результатов анализа'
        )
        parser.add_argument(
            '--force-update',
            action='store_true',
            help='Принудительно обновить существующие игры'
        )
        parser.add_argument(
            '--update-all',
            action='store_true',
            help='Обновить ВСЕ игры в базе (игнорирует фильтры)'
        )
        parser.add_argument(
            '--load-games-limit',
            type=int,
            help='Лимит для load_games (переопределяет общий лимит)'
        )
        parser.add_argument(
            '--wiki-limit',
            type=int,
            help='Лимит для import_wiki_gameplay (переопределяет общий лимит)'
        )
        parser.add_argument(
            '--steam-limit',
            type=int,
            help='Лимит для fetch_steam_descriptions (переопределяет общий лимит)'
        )
        parser.add_argument(
            '--keywords-limit',
            type=int,
            help='Лимит для add_new_keywords_from_descriptions (переопределяет общий лимит)'
        )
        parser.add_argument(
            '--analyze-limit',
            type=int,
            help='Лимит для analyze_game_criteria_fast (переопределяет общий лимит)'
        )
        parser.add_argument(
            '--output',
            type=str,
            default='pipeline_report.txt',
            help='Путь к файлу для сохранения отчета'
        )
        parser.add_argument(
            '--reset-offset',
            action='store_true',
            help='Сбросить сохраненный offset и начать с начала'
        )
        parser.add_argument(
            '--reload-game-igdb-id',
            type=int,
            nargs='+',
            default=None,
            help='Перезагрузить конкретные игры по IGDB ID: удалить игру, загрузить заново '
                 'через load_games --game-names, проанализировать критерии и ключевые слова '
                 'только для этой игры. Остальные шаги пайплайна пропускаются. '
                 'Пример: --reload-game-igdb-id 12345 67890'
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_time: Optional[datetime] = None
        self.step_results: Dict[str, Any] = {}
        self.errors: List[Dict[str, Any]] = []
        self.all_steps = list(PipelineStep)
        self.output_file = None
        self.output_file_path = None
        self.dry_run = False
        self.verbose = False
        self.skip_errors = False
        self.skip_update_vectors = False
        self.start_from = None
        self.stop_after = None
        self.limit = None
        self.genres = []
        self.text_contains = []
        self.steam_workers = 16
        self.analyze_threads = 24
        self.steam_delay = 0.1
        self.delete_low_usage = False
        self.low_usage_threshold = 1
        self.delete_stopwords = False
        self.auto_save = True
        self.force_update = False
        self.update_all = False
        self.load_games_limit = None
        self.wiki_limit = None
        self.steam_limit = None
        self.keywords_limit = None
        self.analyze_limit = None
        self.loaded_game_ids = []
        self.reset_offset = False
        self.steam_output_dir = 'steam_data'
        self.reload_game_igdb_ids: List[int] = []
        self.max_keyword_id_before: int = 0
        self.new_keyword_ids: Set[int] = set()
        self.interrupted = False

    def signal_handler(self, signum, frame):
        """Обработчик сигнала прерывания (Ctrl+C)."""
        self._log('\n\n⚠️ Получен сигнал прерывания (Ctrl+C)')
        self._log('⏳ Завершение текущего шага...')

    def _load_game_ids_from_file(self, file_path: str) -> set:
        """Загружает список ID игр из файла."""
        if not file_path or not os.path.exists(file_path):
            return set()

        game_ids = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and line.isdigit():
                        game_ids.add(int(line))
            self.stdout.write(
                self.style.SUCCESS(f'📂 Загружено {len(game_ids)} ID игр из файла: {file_path}')
            )
        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f'❌ Ошибка загрузки ID из файла: {e}')
            )
        return game_ids

    def _write_to_file(self, text: str):
        """Записывает текст в файл отчета."""
        if self.output_file:
            try:
                self.output_file.write(text + '\n')
                self.output_file.flush()
            except Exception:
                pass

    def _log(self, message: str, style: str = None):
        """Выводит сообщение в консоль и файл."""
        if style:
            self.stdout.write(getattr(self.style, style)(message))
        else:
            self.stdout.write(message)
        self._write_to_file(message)

    def _log_section(self, title: str, char: str = '='):
        """Выводит секцию."""
        self._log('')
        self._log(char * 80)
        self._log(title)
        self._log(char * 80)

    def handle(self, *args, **options):
        """Основной обработчик команды."""
        self.start_time = datetime.now()

        self.dry_run = options.get('dry_run', False)
        self.verbose = options.get('verbose', False)
        self.skip_errors = options.get('skip_errors', False)
        self.skip_update_vectors = options.get('skip_update_vectors', False)
        self.limit = options.get('limit', None)
        self.genres = options.get('genres', [])
        self.text_contains = options.get('text_contains', [])
        self.steam_workers = options.get('steam_workers', 16)
        self.analyze_threads = options.get('analyze_threads', 24)
        self.steam_delay = options.get('steam_delay', 0.1)
        self.delete_low_usage = options.get('delete_low_usage', False)
        self.low_usage_threshold = options.get('low_usage_threshold', 1)
        self.delete_stopwords = options.get('delete_stopwords', False)  # no-op
        self.auto_save = not options.get('no_auto_save', False)
        self.force_update = options.get('force_update', False)
        self.update_all = options.get('update_all', False)
        self.load_games_limit = options.get('load_games_limit')
        self.wiki_limit = options.get('wiki_limit')
        self.steam_limit = options.get('steam_limit')
        self.keywords_limit = options.get('keywords_limit')
        self.analyze_limit = options.get('analyze_limit')
        self.reset_offset = options.get('reset_offset', False)
        self.reload_game_igdb_ids = options.get('reload_game_igdb_id') or []

        if options.get('start_from'):
            self.start_from = PipelineStep(options['start_from'])
        if options.get('stop_after'):
            self.stop_after = PipelineStep(options['stop_after'])

        self._setup_output_file(options.get('output', 'pipeline_report.txt'))

        signal.signal(signal.SIGINT, self.signal_handler)

        try:
            self._write_report_header()
            self._print_header()

            if self.reload_game_igdb_ids:
                self._run_reload_games_by_igdb_ids()
                self._print_footer()
                return

            steps = self._get_steps_to_run()

            for step in steps:
                self._run_step(step)
                if self.stop_after == step:
                    self._log(f'\n⏹️ Остановлено после шага {step.value}')
                    break

            self._print_footer()

        except PipelineInterrupted:
            self.interrupted = True
            self._log('\n⏹️ Пайплайн прерван пользователем (Ctrl+C в дочернем процессе)')
            self._log('📄 Промежуточный отчет сохранен')
            self._print_footer()

        except KeyboardInterrupt:
            self.interrupted = True
            self._log('\n⏹️ Прервано пользователем (Ctrl+C)')
            self._log('📄 Промежуточный отчет сохранен')
            self._print_footer()

        except Exception as e:
            self._log(f'\n❌ Критическая ошибка: {e}')
            if self.verbose:
                import traceback
                traceback.print_exc()
            raise

        finally:
            if self.output_file:
                self.output_file.close()
                self._log(f'\n📄 Отчет сохранен в: {self.output_file_path}')

    def _setup_output_file(self, file_path: str):
        """Настраивает файл для вывода отчета."""
        try:
            dir_path = os.path.dirname(os.path.abspath(file_path))
            if dir_path:
                os.makedirs(dir_path, exist_ok=True)
            self.output_file = open(file_path, 'w', encoding='utf-8')
            self.output_file_path = file_path
        except Exception as e:
            self.stdout.write(f'❌ Ошибка создания файла: {e}')
            self.output_file = None

    def _write_report_header(self):
        """Записывает заголовок отчета."""
        self._write_to_file('=' * 80)
        self._write_to_file('ОТЧЕТ О ВЫПОЛНЕНИИ ПАЙПЛАЙНА')
        self._write_to_file('=' * 80)
        self._write_to_file(f'Дата запуска: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
        self._write_to_file('=' * 80)
        self._write_to_file('')

    def _print_header(self):
        """Выводит заголовок с информацией о запуске."""
        self._log_section('🚀 ЗАПУСК ПОЛНОГО ПАЙПЛАЙНА ОБРАБОТКИ ДАННЫХ')

        self._log('\n📋 Параметры:')

        if self.reload_game_igdb_ids:
            self._log(f'  • РЕЖИМ ПЕРЕЗАГРУЗКИ ИГР ПО IGDB ID: {self.reload_game_igdb_ids}')
            self._log('  • Остальные шаги пайплайна будут пропущены')
            self._log(f'\n  • Потоков анализа: {self.analyze_threads}')
            self._log(f'\n📄 Отчет будет сохранен в: {self.output_file_path}')
            return

        if self.limit is not None:
            self._log(f'  • Общий лимит: {self.limit:,} игр')
        else:
            self._log(f'  • Общий лимит: БЕЗ ЛИМИТА')

        self._log(f'  • Режим просмотра: {"✅" if self.dry_run else "❌"}')
        self._log(f'  • Подробный вывод: {"✅" if self.verbose else "❌"}')
        self._log(f'  • Пропуск ошибок: {"✅" if self.skip_errors else "❌"}')
        self._log(f'  • Пропуск UPDATE_VECTORS: {"✅" if self.skip_update_vectors else "❌"}')
        self._log(f'  • Принудительное обновление: {"✅" if self.force_update else "❌"}')
        self._log(f'  • Обновить все игры: {"✅" if self.update_all else "❌"}')
        self._log(f'  • Сброс offset: {"✅" if self.reset_offset else "❌"}')
        self._log(f'  • Чистка стоп-слов: ✅ (всегда)')

        if self.start_from:
            self._log(f'  • Начать с: {self.start_from.value}')
        if self.stop_after:
            self._log(f'  • Остановить после: {self.stop_after.value}')

        if self.genres:
            self._log(f'  • Фильтр по жанрам (И): {", ".join(self.genres)}')
        if self.text_contains:
            self._log(f'  • Фильтр по тексту (ИЛИ): {", ".join(self.text_contains)}')

        self._log(f'\n  • Steam воркеров: {self.steam_workers}')
        self._log(f'  • Потоков анализа: {self.analyze_threads}')
        self._log(f'  • Steam задержка: {self.steam_delay}с')

        if self.delete_low_usage:
            self._log(f'  • Удаление низкочастотных слов: ✅ (порог: {self.low_usage_threshold})')

        self._log(f'\n📄 Отчет будет сохранен в: {self.output_file_path}')

        self._log('\nℹ️ Ключевые слова: создание/нормализация и анализ новых ключевых слов')
        self._log('   по старым играм выполняются ВНЕШНИМИ ШАГАМИ пайплайна,')
        self._log('   а не через флаги --create-keywords / --analyze-new-on-old.')
        self._log('ℹ️ Флаги --dry-run/--verbose передаются только тем дочерним командам,')
        self._log('   которые их объявляют; для остальных шаг в dry-run пропускается.')
        self._log('ℹ️ analyze_game_criteria_fast не объявляет --dry-run — в dry-run шаг')
        self._log('   логируется (три вызова), но не выполняется.')
        self._log('ℹ️ Чистка стоп-слов выполняется ВСЕГДА на шаге ADD_KEYWORDS:')
        self._log('   отдельный вызов add_new_keywords_from_descriptions --delete-stopwords')
        self._log('   перед основным вызовом создания новых ключевых слов.')
        self._log('ℹ️ Ctrl+C в дочернем процессе трактуется как «прервано пользователем»:')
        self._log('   пайплайн останавливается штатно, без RuntimeError и трейсбека.')

    # ------------------------------------------------------------------ #
    #  РЕЖИМ ПЕРЕЗАГРУЗКИ КОНКРЕТНЫХ ИГР ПО IGDB ID
    # ------------------------------------------------------------------ #

    def _run_reload_games_by_igdb_ids(self):
        """
        Перезагружает конкретные игры по IGDB ID:
          1. Для каждого igdb_id находит игру в БД, запоминает name, удаляет игру.
          2. Вызывает load_games --game-names "<name1>,<name2>,...".
          3. Находит новые Game.id по igdb_id, пишет temp_reload_game_ids.txt.
          4. Вызывает analyze_game_criteria_fast --game-ids-file temp_reload_game_ids.txt
             (критерии, затем ключевые слова).
        """
        from games.models import Game

        self._log_section('🔄 РЕЖИМ ПЕРЕЗАГРУЗКИ ИГР ПО IGDB ID')

        if self.dry_run:
            self._log('   ⏭️ [DRY-RUN] Режим перезагрузки пропущен (ничего не удаляем и не грузим)')
            self._log(f'   🔧 [DRY-RUN] Были бы обработаны IGDB ID: {self.reload_game_igdb_ids}')
            return

        # 1. Находим игры, запоминаем имена, удаляем
        games_info = []
        not_found_igdb = []

        for igdb_id in self.reload_game_igdb_ids:
            game = Game.objects.filter(igdb_id=igdb_id).first()
            if not game:
                not_found_igdb.append(igdb_id)
                continue
            games_info.append({'igdb_id': igdb_id, 'game_id': game.id, 'name': game.name})

        if not_found_igdb:
            self._log(f'   ⚠️ Не найдены в БД по IGDB ID: {not_found_igdb}')

        if not games_info:
            self._log('   ❌ Нет игр для перезагрузки (ни один IGDB ID не найден в БД)')
            return

        names = [g['name'] for g in games_info]
        self._log(f'   📋 Игр к перезагрузке: {len(games_info)}')
        for g in games_info:
            self._log(f'      • IGDB ID {g["igdb_id"]} → Game.id {g["game_id"]} («{g["name"]}»)')

        # Удаляем игры
        self._log('\n   🗑️ Удаление старых записей игр...')
        deleted_count, _ = Game.objects.filter(
            igdb_id__in=[g['igdb_id'] for g in games_info]
        ).delete()
        self._log(f'   ✅ Удалено записей: {deleted_count}')

        # 2. Перезагрузка через load_games --game-names
        self._log('\n   📥 Перезагрузка игр через load_games --game-names...')
        args = [
            '--clear-cache',
            '--clear-db-cache',
            '--game-names', ','.join(names),
            '--overwrite',  # чтобы гарантированно перезаписать
        ]
        self._call_command('load_games', args)

        # 3. Находим новые Game.id по igdb_id
        self._log('\n   🔍 Поиск новых Game.id по IGDB ID...')
        new_games = list(
            Game.objects.filter(igdb_id__in=[g['igdb_id'] for g in games_info])
            .values_list('igdb_id', 'id')
        )
        new_map = {igdb_id: gid for igdb_id, gid in new_games}

        reloaded = []
        for g in games_info:
            new_gid = new_map.get(g['igdb_id'])
            if new_gid:
                reloaded.append({'igdb_id': g['igdb_id'], 'game_id': new_gid, 'name': g['name']})
                self._log(f'      ✅ IGDB ID {g["igdb_id"]} → новый Game.id {new_gid}')
            else:
                self._log(f'      ⚠️ IGDB ID {g["igdb_id"]} не найден после перезагрузки')

        if not reloaded:
            self._log('   ❌ Не удалось перезагрузить ни одну игру')
            return

        # 4. Пишем temp_reload_game_ids.txt
        reload_ids_file = 'temp_reload_game_ids.txt'
        try:
            with open(reload_ids_file, 'w', encoding='utf-8') as f:
                for g in reloaded:
                    f.write(f'{g["game_id"]}\n')
            self._log(f'   📁 Создан {reload_ids_file}: {len(reloaded)} ID')
        except Exception as e:
            self._log(f'   ⚠️ Ошибка записи {reload_ids_file}: {e}')
            return

        # 5. Анализ критериев
        self._log('\n   🔍 Анализ КРИТЕРИЕВ для перезагруженных игр...')
        args_criteria = [
            f'--threads={self.analyze_threads}',
            '--auto-save',
            '--collect-all-patterns',
            '--game-ids-file', reload_ids_file,
        ]
        if self.verbose:
            args_criteria.append('--verbose')
        self._call_command('analyze_game_criteria_fast', args_criteria)

        # 6. Анализ ключевых слов
        self._log('\n   🔑 Анализ КЛЮЧЕВЫХ СЛОВ для перезагруженных игр...')
        args_keywords = [
            f'--threads={self.analyze_threads}',
            '--keywords',
            '--auto-save',
            '--collect-all-patterns',
            '--game-ids-file', reload_ids_file,
        ]
        if self.verbose:
            args_keywords.append('--verbose')
        self._call_command('analyze_game_criteria_fast', args_keywords)

        self._log('\n   ✅ Перезагрузка завершена')

    def _get_steps_to_run(self) -> List[PipelineStep]:
        """Возвращает список шагов для выполнения."""
        all_steps = self.all_steps

        start_index = 0
        if self.start_from:
            for i, step in enumerate(all_steps):
                if step == self.start_from:
                    start_index = i
                    break

        steps = all_steps[start_index:]

        if self.stop_after:
            for i, step in enumerate(steps):
                if step == self.stop_after:
                    steps = steps[:i + 1]
                    break

        return steps

    def _run_step(self, step: PipelineStep):
        """Выполняет один шаг пайплайна."""
        self._log_section(f'🔄 ШАГ: {step.value.upper()}')

        step_start = time.time()

        try:
            if step == PipelineStep.LOAD_GAMES:
                self._run_load_games()
            elif step == PipelineStep.IMPORT_WIKI:
                self._run_import_wiki()
            elif step == PipelineStep.FETCH_STEAM:
                self._run_fetch_steam()
            elif step == PipelineStep.FILL_FROM_STEAMDB:
                self._run_fill_from_steamdb()
            elif step == PipelineStep.ADD_KEYWORDS:
                self._run_add_keywords()
            elif step == PipelineStep.NORMALIZE_KEYWORDS:
                self._run_normalize_keywords()
            elif step == PipelineStep.CRITERIES_REASSIGN:
                self._run_criteria_reassign()
            elif step == PipelineStep.ANALYZE_CRITERIA:
                self._run_analyze_criteria()
            elif step == PipelineStep.UPDATE_VECTORS:
                self._run_update_vectors()
            elif step == PipelineStep.DELETE_LOW_USAGE:
                self._run_delete_low_usage()

            step_time = time.time() - step_start
            self.step_results[step.value] = {'status': 'success', 'time': step_time}
            self._log(f'\n✅ Шаг {step.value} завершен за {step_time:.1f}с')

        except PipelineInterrupted:
            step_time = time.time() - step_start
            self.step_results[step.value] = {'status': 'interrupted', 'time': step_time}
            self._log(f'\n⏹️ Шаг {step.value} прерван пользователем')
            raise

        except Exception as e:
            step_time = time.time() - step_start
            self.step_results[step.value] = {'status': 'error', 'time': step_time, 'error': str(e)}
            self.errors.append({'step': step.value, 'error': str(e)})
            self._log(f'\n❌ Шаг {step.value} завершился с ошибкой: {e}')
            if not self.skip_errors:
                raise

    def _run_load_games(self):
        """Выполняет команду load_games."""
        from games.models import Game

        if self.dry_run:
            self._log('   ⏭️ [DRY-RUN] load_games пропущен (команда не поддерживает --dry-run)')
            self.loaded_game_ids = []
            return

        max_id_before = Game.objects.aggregate(Max('id'))['id__max'] or 0
        self._log(f'   📌 max(id) до load_games: {max_id_before}')

        args = [
            '--clear-cache',
            '--clear-db-cache',
        ]

        if self.load_games_limit is not None:
            args.extend(['--limit', str(self.load_games_limit)])
        elif self.limit is not None:
            args.extend(['--limit', str(self.limit)])

        if self.genres:
            args.extend(['--genres', ','.join(self.genres)])

        if self.text_contains:
            args.extend(['--description-contains', ' '.join(self.text_contains)])

        if self.reset_offset:
            args.append('--reset-offset')

        self._call_command('load_games', args)

        new_games = Game.objects.filter(id__gt=max_id_before).order_by('id')
        self.loaded_game_ids = list(new_games.values_list('id', flat=True))
        self._log(f'   📊 Загружено новых игр: {len(self.loaded_game_ids)}')

    def _run_import_wiki(self):
        """Выполняет команду import_wiki_gameplay."""
        if self.dry_run:
            self._log('   ⏭️ [DRY-RUN] import_wiki_gameplay пропущен (команда не поддерживает --dry-run)')
            return

        args = [
            '--skip-existing',
            '--force-update',
        ]

        if self.wiki_limit is not None:
            args.extend(['--limit', str(self.wiki_limit)])
        elif self.limit is not None:
            args.extend(['--limit', str(self.limit)])

        self._call_command('import_wiki_gameplay', args)

    def _run_fetch_steam(self):
        """Выполняет check_steam_games и fetch_steam_descriptions для загруженных игр."""
        if self.dry_run:
            self._log('   ⏭️ [DRY-RUN] fetch_steam_descriptions пропущен')
            self._log('      (check_steam_games не поддерживает --dry-run; '
                      'fetch_steam_descriptions не выполняется в dry-run)')
            return

        temp_ids_path = 'temp_game_ids.txt'
        if os.path.exists(temp_ids_path):
            try:
                os.remove(temp_ids_path)
                self._log(f'   🗑️ Удалён старый {temp_ids_path}')
            except Exception as e:
                self._log(f'   ⚠️ Не удалось удалить {temp_ids_path}: {e}')

        if not self.loaded_game_ids:
            self._log('   ⚠️ Нет загруженных игр для проверки в Steam — шаг пропущен')
            self._log('   ℹ️ Возможные причины: load_games ничего не загрузил (все игры уже в БД) '
                      'или был запущен --dry-run')
            return

        self._create_game_ids_file()

        self._log(f'\n   🔍 Проверка наличия {len(self.loaded_game_ids)} игр в Steam...')
        args_check = [
            '--game-ids-file', temp_ids_path,
        ]
        # check_steam_games не объявляет --dry-run/--verbose — не передаём.
        self._call_command('check_steam_games', args_check)

        self._log('\n   📥 Загрузка описаний из Steam...')
        args_fetch = [
            '--force',
            '--skip-not-found',
            '--skip-no-description',
            f'--workers={self.steam_workers}',
            f'--delay={self.steam_delay}',
            '--game-ids-file', temp_ids_path,
        ]

        if self.steam_limit is not None:
            args_fetch.extend(['--limit', str(self.steam_limit)])
        elif self.limit is not None:
            args_fetch.extend(['--limit', str(self.limit)])

        if self.verbose:
            args_fetch.append('--verbose')

        self._call_command('fetch_steam_descriptions', args_fetch)

    def _run_fill_from_steamdb(self):
        """Выполняет команду fill_descriptions_from_steamdb (заполнение описаний из локального дампа)."""
        if self.dry_run and not self.loaded_game_ids:
            self._log('   ⏭️ [DRY-RUN] fill_descriptions_from_steamdb пропущен')
            self._log('      (load_games в dry-run не выполнялся, loaded_game_ids пуст; '
                      'старый temp_game_ids.txt не читаем)')
            return

        args = [
            '--steamdb-file', 'steamdb.min.json.gz',
        ]

        temp_ids_path = 'temp_game_ids.txt'
        if os.path.exists(temp_ids_path):
            try:
                with open(temp_ids_path, 'r', encoding='utf-8') as f:
                    has_ids = any(line.strip().isdigit() for line in f)
            except Exception:
                has_ids = False

            if has_ids:
                args.extend(['--game-ids-file', temp_ids_path])
                self._log(f'   📂 Используется фильтр по ID из {temp_ids_path}')
            else:
                self._log(f'   📂 {temp_ids_path} пуст — обработка по всей БД')
        else:
            self._log('   📂 Файл temp_game_ids.txt не найден — обработка по всей БД')

        found_list = os.path.join(self.steam_output_dir, 'steam_found.txt')
        if os.path.exists(found_list):
            args.extend(['--found-list-file', found_list])

        os.makedirs(self.steam_output_dir, exist_ok=True)
        descriptions_output = os.path.join(self.steam_output_dir, 'pipeline_descriptions.txt')
        args.extend(['--output', descriptions_output])

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('fill_descriptions_from_steamdb', args)

    def _run_add_keywords(self):
        """
        Выполняет add_new_keywords_from_descriptions.

        Чистка стоп-слов выполняется ВСЕГДА: отдельный вызов
        add_new_keywords_from_descriptions --delete-stopwords перед основным
        вызовом создания новых ключевых слов. Ошибка чистки логируется и не
        прерывает пайплайн.

        Основной вызов идёт БЕЗ --delete-stopwords и реально создаёт новые
        ключевые слова. max_keyword_id_before фиксируется ДО основного вызова,
        чтобы затем собрать new_keyword_ids = id > max_keyword_id_before.
        """
        from games.models import Keyword

        # --- Чистка стоп-слов (ВСЕГДА) ---
        self._log('\n   🗑️ Чистка стоп-слов (--delete-stopwords)...')
        stop_args = ['--delete-stopwords']
        if self.dry_run:
            stop_args.append('--dry-run')
        if self.verbose:
            stop_args.append('--verbose')
        try:
            self._call_command('add_new_keywords_from_descriptions', stop_args)
        except PipelineInterrupted:
            raise
        except RuntimeError as e:
            self._log(f'   ⚠️ Чистка стоп-слов завершилась с ошибкой: {e}')
            self._log('      (продолжаем — основной вызов создания ключевых слов ниже)')

        # --- Фиксируем max(id) и создаём новые ключевые слова ---
        self.max_keyword_id_before = Keyword.objects.aggregate(Max('id'))['id__max'] or 0
        self._log(f'   📌 max(keyword.id) до add_new_keywords: {self.max_keyword_id_before}')

        args = []  # БЕЗ --delete-stopwords

        if self.keywords_limit is not None:
            args.extend(['--limit', str(self.keywords_limit)])
        elif self.limit is not None:
            args.extend(['--limit', str(self.limit)])

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('add_new_keywords_from_descriptions', args)

        if self.dry_run:
            self._log('   📊 [DRY-RUN] новые ключевые слова не собираются')
            return

        new_keywords = Keyword.objects.filter(id__gt=self.max_keyword_id_before)
        self.new_keyword_ids = set(new_keywords.values_list('id', flat=True))
        self._log(f'   📊 Новых ключевых слов после add_new_keywords: {len(self.new_keyword_ids)}')

    def _run_normalize_keywords(self):
        """
        Выполняет normalize_keywords --merge-only.

        После нормализации пересобирает new_keyword_ids — оставляет только
        те новые ключевые слова, которые ВЫЖИЛИ (не были слиты/удалены).
        """
        from games.models import Keyword

        args = ['--merge-only']

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('normalize_keywords', args)

        if self.dry_run:
            self._log('   📊 [DRY-RUN] пересборка new_keyword_ids пропущена')
            return

        if self.max_keyword_id_before == 0:
            self._log('   ℹ️ max_keyword_id_before = 0, пересборка новых ключевых слов невозможна')
            return

        surviving = Keyword.objects.filter(id__gt=self.max_keyword_id_before)
        self.new_keyword_ids = set(surviving.values_list('id', flat=True))
        self._log(f'   📊 Выживших новых ключевых слов после нормализации: {len(self.new_keyword_ids)}')

    def _run_criteria_reassign(self):
        """Выполняет команду criteries_reassign."""
        args = []

        if self.dry_run:
            args.append('--dry-run')

        # criteries_reassign не объявляет --verbose — не передаём.

        self._call_command('criteries_reassign', args)

    def _run_analyze_criteria(self):
        """
        Выполняет analyze_game_criteria_fast.

        Основной анализ:
          - критерии (жанры/темы/перспективы/режимы) по всем играм (в пределах --limit);
          - ключевые слова по всем играм (в пределах --limit).

        Дополнительный подшаг (если есть новые ключевые слова):
          - только НОВЫЕ ключевые слова по СТАРЫМ играм (все игры БД, кроме loaded_game_ids).

        ВАЖНО: analyze_game_criteria_fast НЕ объявляет --dry-run. Поэтому в dry-run
        пайплайн логирует три вызова, но НЕ выполняет их.
        """

        if self.dry_run:
            self._log('   ⏭️ [DRY-RUN] analyze_game_criteria_fast пропущен')
            self._log('      (команда не объявляет --dry-run; ниже — какие вызовы были бы выполнены)')

            self._log('\n   🔧 [DRY-RUN] manage.py analyze_game_criteria_fast '
                      f'--threads={self.analyze_threads} --auto-save --collect-all-patterns'
                      + (f' --limit {self.analyze_limit}' if self.analyze_limit is not None
                         else (f' --limit {self.limit}' if self.limit is not None else '')))
            self._log('   🔧 [DRY-RUN] manage.py analyze_game_criteria_fast '
                      f'--threads={self.analyze_threads} --keywords --auto-save --collect-all-patterns'
                      + (f' --limit {self.analyze_limit}' if self.analyze_limit is not None
                         else (f' --limit {self.limit}' if self.limit is not None else '')))
            self._log('   🔧 [DRY-RUN] подшаг: analyze_game_criteria_fast --keywords '
                      '--keyword-ids-file temp_new_keyword_ids.txt --game-ids-file temp_old_game_ids.txt '
                      '--auto-save --collect-all-patterns (если есть new_keyword_ids)')
            return

        # === 1. Основной анализ критериев ===
        args = [
            f'--threads={self.analyze_threads}',
        ]

        if self.auto_save:
            args.append('--auto-save')

        args.append('--collect-all-patterns')

        if self.analyze_limit is not None:
            args.extend(['--limit', str(self.analyze_limit)])
        elif self.limit is not None:
            args.extend(['--limit', str(self.limit)])

        if self.verbose:
            args.append('--verbose')

        self._call_command('analyze_game_criteria_fast', args)

        # === 2. Основной анализ ключевых слов ===
        self._log('\n   🔑 Запускаем анализ КЛЮЧЕВЫХ СЛОВ...')

        args_keywords = [
            f'--threads={self.analyze_threads}',
            '--keywords',
        ]

        if self.auto_save:
            args_keywords.append('--auto-save')

        args_keywords.append('--collect-all-patterns')

        if self.analyze_limit is not None:
            args_keywords.extend(['--limit', str(self.analyze_limit)])
        elif self.limit is not None:
            args_keywords.extend(['--limit', str(self.limit)])

        if self.verbose:
            args_keywords.append('--verbose')

        self._call_command('analyze_game_criteria_fast', args_keywords)

        # === 3. Подшаг: только НОВЫЕ ключевые слова по СТАРЫМ играм ===
        self._run_analyze_new_keywords_on_old_games()

    def _run_analyze_new_keywords_on_old_games(self):
        """
        Подшаг: прогнать только новые ключевые слова по всем старым играм БД.

        Новые ключевые слова — self.new_keyword_ids (выжившие после нормализации).
        Старые игры — все игры БД, КРОМЕ self.loaded_game_ids.
        """
        from games.models import Game

        self._log('\n   🔄 ПОДШАГ: новые ключевые слова по старым играм')

        if self.dry_run:
            self._log('   ⏭️ [DRY-RUN] подшаг пропущен (analyze_game_criteria_fast не поддерживает --dry-run)')
            return

        if not self.new_keyword_ids:
            self._log('   ⏭️ Нет новых ключевых слов (после нормализации) — подшаг пропущен')
            return

        self._log(f'   🎯 Новых ключевых слов: {len(self.new_keyword_ids)}')

        new_kw_file = 'temp_new_keyword_ids.txt'
        try:
            with open(new_kw_file, 'w', encoding='utf-8') as f:
                for kw_id in sorted(self.new_keyword_ids):
                    f.write(f'{kw_id}\n')
            self._log(f'   📁 Создан {new_kw_file}: {len(self.new_keyword_ids)} ID')
        except Exception as e:
            self._log(f'   ⚠️ Ошибка записи {new_kw_file}: {e}')
            return

        loaded_set = set(self.loaded_game_ids) if self.loaded_game_ids else set()
        self._log(f'   📊 Загруженных (новых) игр: {len(loaded_set)}')

        old_game_ids = []
        try:
            qs = Game.objects.all().only('id').order_by('id')
            for gid in qs.values_list('id', flat=True).iterator(chunk_size=10000):
                if gid not in loaded_set:
                    old_game_ids.append(gid)
        except Exception as e:
            self._log(f'   ⚠️ Ошибка сбора старых игр: {e}')
            return

        if not old_game_ids:
            self._log('   ⏭️ Нет старых игр (все игры — новые) — подшаг пропущен')
            return

        old_games_file = 'temp_old_game_ids.txt'
        try:
            with open(old_games_file, 'w', encoding='utf-8') as f:
                for gid in old_game_ids:
                    f.write(f'{gid}\n')
            self._log(f'   📁 Создан {old_games_file}: {len(old_game_ids)} ID')
        except Exception as e:
            self._log(f'   ⚠️ Ошибка записи {old_games_file}: {e}')
            return

        args = [
            f'--threads={self.analyze_threads}',
            '--keywords',
            '--keyword-ids-file', new_kw_file,
            '--game-ids-file', old_games_file,
        ]

        if self.auto_save:
            args.append('--auto-save')

        args.append('--collect-all-patterns')

        if self.verbose:
            args.append('--verbose')

        self._log(f'   ▶️ Запуск: analyze_game_criteria_fast --keywords '
                  f'--keyword-ids-file {new_kw_file} --game-ids-file {old_games_file} '
                  f'(новых ключевых слов: {len(self.new_keyword_ids)}, старых игр: {len(old_game_ids)})')

        self._call_command('analyze_game_criteria_fast', args)

    def _run_update_vectors(self):
        """Выполняет команду update_vectors (или пропускает, если --skip-update-vectors)."""
        if self.skip_update_vectors:
            self._log('   ⏭️ Пропущено (--skip-update-vectors)')
            return

        if self.dry_run:
            self._log('   ⏭️ [DRY-RUN] update_vectors пропущен (команда не поддерживает --dry-run)')
            return

        self._call_command('update_vectors', [])

    def _run_delete_low_usage(self):
        """Выполняет команду delete_low_usage_keywords."""
        if not self.delete_low_usage:
            self._log('   ⏭️ Пропущено (опция не включена)')
            return

        args = [f'--usage={self.low_usage_threshold}']

        if self.dry_run:
            args.append('--dry-run')

        self._call_command('delete_low_usage_keywords', args)

    def _create_game_ids_file(self):
        """Создает временный файл с ID игр для fetch_steam."""
        if not self.loaded_game_ids:
            return

        file_path = 'temp_game_ids.txt'
        try:
            with open(file_path, 'w') as f:
                for game_id in self.loaded_game_ids:
                    f.write(f'{game_id}\n')
            self._log(f'   📁 Создан файл с ID игр: {file_path} ({len(self.loaded_game_ids)} ID)')
        except Exception as e:
            self._log(f'   ⚠️ Ошибка создания файла ID: {e}')

    def _call_command(self, command_name: str, args: List[str]):
        """Вызывает команду Django без перехвата вывода."""
        if self.dry_run and command_name not in (
                'fill_descriptions_from_steamdb',
                'add_new_keywords_from_descriptions',
                'normalize_keywords',
                'criteries_reassign',
                'delete_low_usage_keywords',
        ):
            self._log(f'\n   🔧 [DRY-RUN] manage.py {command_name} {" ".join(args)}')
            return

        self._log(f'\n   ▶️ Выполняется: manage.py {command_name} {" ".join(args)}')
        self.stdout.flush()

        try:
            result = subprocess.run(
                [sys.executable, 'manage.py', command_name] + args,
                check=False
            )

            if result.returncode in CTRL_C_RETURNCODES:
                self._log('   ⏹️ Дочерний процесс прерван пользователем (Ctrl+C)')
                raise PipelineInterrupted()

            if result.returncode != 0:
                raise RuntimeError(f'Команда завершилась с кодом {result.returncode}')

        except PipelineInterrupted:
            raise
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f'Ошибка выполнения команды {command_name}: {e}')
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f'Неизвестная ошибка при выполнении {command_name}: {e}')

    def _call_command_with_output(self, command_name: str, args: List[str]) -> Dict:
        """Вызывает команду Django без перехвата вывода, возвращает ID загруженных игр."""
        if self.dry_run:
            self._log(f'\n   🔧 [DRY-RUN] manage.py {command_name} {" ".join(args)}')
            return {}

        self._log(f'\n   ▶️ Выполняется: manage.py {command_name} {" ".join(args)}')
        self.stdout.flush()

        try:
            result = subprocess.run(
                [sys.executable, 'manage.py', command_name] + args,
                check=False
            )

            if result.returncode in CTRL_C_RETURNCODES:
                self._log('   ⏹️ Дочерний процесс прерван пользователем (Ctrl+C)')
                raise PipelineInterrupted()

            if result.returncode != 0:
                raise RuntimeError(f'Команда завершилась с кодом {result.returncode}')

            return {'loaded_game_ids': []}

        except PipelineInterrupted:
            raise
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f'Ошибка выполнения команды {command_name}: {e}')
        except RuntimeError:
            raise
        except Exception as e:
            raise RuntimeError(f'Неизвестная ошибка при выполнении {command_name}: {e}')

    def _print_footer(self):
        """Выводит итоговую статистику выполнения."""
        total_time = (datetime.now() - self.start_time).total_seconds()

        self._log_section('📊 ИТОГОВАЯ СТАТИСТИКА ВЫПОЛНЕНИЯ')

        success_count = 0
        error_count = 0
        interrupted_count = 0

        for step_name, result in self.step_results.items():
            status = result.get('status', 'unknown')
            time_str = f"{result.get('time', 0):.1f}с"
            if status == 'success':
                success_count += 1
                self._log(f'  ✅ {step_name}: {time_str}')
            elif status == 'interrupted':
                interrupted_count += 1
                self._log(f'  ⏹️ {step_name}: {time_str} (прервано)')
            elif status == 'error':
                error_count += 1
                self._log(f'  ❌ {step_name}: {time_str} - {result.get("error", "неизвестная ошибка")}')
            else:
                self._log(f'  ⏭️ {step_name}: {time_str}')

        if self.reload_game_igdb_ids:
            self._log(f'\n📋 Режим перезагрузки: IGDB ID {self.reload_game_igdb_ids}')

        self._log(f'\n📊 Всего шагов: {len(self.step_results)}')
        self._log(f'  ✅ Успешно: {success_count}')
        self._log(f'  ⏹️ Прервано: {interrupted_count}')
        self._log(f'  ❌ С ошибками: {error_count}')

        if self.errors:
            self._log(f'\n⚠️ Ошибок всего: {len(self.errors)}')
            for error in self.errors[:3]:
                self._log(f'  • {error.get("step", "unknown")}: {error.get("error", "")[:100]}...')

        hours = int(total_time // 3600)
        minutes = int((total_time % 3600) // 60)
        seconds = int(total_time % 60)
        time_str = f"{hours}ч {minutes}м {seconds}с" if hours > 0 else f"{minutes}м {seconds}с"
        self._log(f'\n⏱️ Общее время: {time_str}')

        if self.interrupted:
            self._log('\n⏹️ ПАЙПЛАЙН ПРЕРВАН ПОЛЬЗОВАТЕЛЕМ (Ctrl+C)')
            self._log('📋 Частичные результаты сохранены, БД не повреждена')
        elif self.dry_run:
            self._log('\n🔧 РЕЖИМ ПРОСМОТРА: изменения не были применены')
            self._log('📋 Все команды были показаны, но ни одна не выполнялась')
        else:
            self._log('\n✨ Пайплайн успешно завершен!')

        self._log('=' * 80)