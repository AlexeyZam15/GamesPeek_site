# games/management/commands/full_pipeline.py

"""
Единая команда для выполнения полного пайплайна обработки данных.

Просто последовательно вызывает существующие команды в правильном порядке.
Поддерживает фильтрацию через аргументы команд.
"""

from django.core.management.base import BaseCommand
import subprocess
import sys
import time
import os
from datetime import datetime
from typing import List, Optional, Dict, Any
from enum import Enum
import signal

class PipelineStep(Enum):
    """Этапы пайплайна."""
    LOAD_GAMES = "load_games"
    IMPORT_WIKI = "import_wiki_gameplay"
    FETCH_STEAM = "fetch_steam_descriptions"
    ADD_KEYWORDS = "add_new_keywords_from_descriptions"
    NORMALIZE_KEYWORDS = "normalize_keywords"
    CRITERIES_REASSIGN = "criteries_reassign"
    ANALYZE_CRITERIA = "analyze_game_criteria_fast"
    UPDATE_VECTORS = "update_vectors"
    DELETE_LOW_USAGE = "delete_low_usage_keywords"


class Command(BaseCommand):
    """
    Единая команда для выполнения полного пайплайна обработки данных.

    Примеры:
        # Загрузить новые игры стратегий или RPG
        python manage.py full_pipeline --limit 10 --genres "Strategy" "RPG" --dry-run

        # Загрузить новые игры с RPG в описании
        python manage.py full_pipeline --limit 10 --text-contains "rpg" --dry-run

        # Реальный запуск
        python manage.py full_pipeline --limit 100 --genres "Strategy" "RPG" --output report.txt
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
            help='Фильтр по жанрам (логика ИЛИ). Пример: --genres "Strategy" "RPG"'
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

    def signal_handler(self, signum, frame):
        """
        Обработчик сигнала прерывания (Ctrl+C).
        Корректно завершает выполнение пайплайна.
        """
        self._log('\n\n⚠️ Получен сигнал прерывания (Ctrl+C)')
        self._log('⏳ Завершение текущего шага...')

    def _load_game_ids_from_file(self, file_path: str) -> set:
        """
        Загружает список ID игр из файла.

        Аргументы:
            file_path: путь к файлу с ID (по одному на строку)

        Возвращает:
            set: множество ID игр
        """
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
        self.limit = options.get('limit', None)
        self.genres = options.get('genres', [])
        self.text_contains = options.get('text_contains', [])
        self.steam_workers = options.get('steam_workers', 16)
        self.analyze_threads = options.get('analyze_threads', 24)
        self.steam_delay = options.get('steam_delay', 0.1)
        self.delete_low_usage = options.get('delete_low_usage', False)
        self.low_usage_threshold = options.get('low_usage_threshold', 1)
        self.auto_save = not options.get('no_auto_save', False)
        self.force_update = options.get('force_update', False)
        self.update_all = options.get('update_all', False)
        self.load_games_limit = options.get('load_games_limit')
        self.wiki_limit = options.get('wiki_limit')
        self.steam_limit = options.get('steam_limit')
        self.keywords_limit = options.get('keywords_limit')
        self.analyze_limit = options.get('analyze_limit')
        self.reset_offset = options.get('reset_offset', False)

        if options.get('start_from'):
            self.start_from = PipelineStep(options['start_from'])
        if options.get('stop_after'):
            self.stop_after = PipelineStep(options['stop_after'])

        self._setup_output_file(options.get('output', 'pipeline_report.txt'))

        signal.signal(signal.SIGINT, self.signal_handler)

        try:
            self._write_report_header()
            self._print_header()

            steps = self._get_steps_to_run()

            for step in steps:
                self._run_step(step)
                if self.stop_after == step:
                    self._log(f'\n⏹️ Остановлено после шага {step.value}')
                    break

            self._print_footer()

        except KeyboardInterrupt:
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

        # Исправлено: проверяем на None
        if self.limit is not None:
            self._log(f'  • Общий лимит: {self.limit:,} игр')
        else:
            self._log(f'  • Общий лимит: БЕЗ ЛИМИТА')

        self._log(f'  • Режим просмотра: {"✅" if self.dry_run else "❌"}')
        self._log(f'  • Подробный вывод: {"✅" if self.verbose else "❌"}')
        self._log(f'  • Пропуск ошибок: {"✅" if self.skip_errors else "❌"}')
        self._log(f'  • Принудительное обновление: {"✅" if self.force_update else "❌"}')
        self._log(f'  • Обновить все игры: {"✅" if self.update_all else "❌"}')
        self._log(f'  • Сброс offset: {"✅" if self.reset_offset else "❌"}')

        if self.start_from:
            self._log(f'  • Начать с: {self.start_from.value}')
        if self.stop_after:
            self._log(f'  • Остановить после: {self.stop_after.value}')

        if self.genres:
            self._log(f'  • Фильтр по жанрам (ИЛИ): {", ".join(self.genres)}')
        if self.text_contains:
            self._log(f'  • Фильтр по тексту (ИЛИ): {", ".join(self.text_contains)}')

        self._log(f'\n  • Steam воркеров: {self.steam_workers}')
        self._log(f'  • Потоков анализа: {self.analyze_threads}')
        self._log(f'  • Steam задержка: {self.steam_delay}с')

        if self.delete_low_usage:
            self._log(f'  • Удаление низкочастотных слов: ✅ (порог: {self.low_usage_threshold})')

        self._log(f'\n📄 Отчет будет сохранен в: {self.output_file_path}')

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

        except Exception as e:
            step_time = time.time() - step_start
            self.step_results[step.value] = {'status': 'error', 'time': step_time, 'error': str(e)}
            self.errors.append({'step': step.value, 'error': str(e)})
            self._log(f'\n❌ Шаг {step.value} завершился с ошибкой: {e}')
            if not self.skip_errors:
                raise

    def _run_load_games(self):
        """Выполняет команду load_games."""
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

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        if self.reset_offset:
            args.append('--reset-offset')

        self._call_command('load_games', args)

        from games.models import Game
        loaded_games = Game.objects.order_by('-id')[:100]
        self.loaded_game_ids = list(loaded_games.values_list('id', flat=True))
        self._log(f'   📊 Загружено игр: {len(self.loaded_game_ids)}')

    def _run_import_wiki(self):
        """Выполняет команду import_wiki_gameplay."""
        args = [
            '--skip-existing',
            '--force-update',
        ]

        if self.wiki_limit is not None:
            args.extend(['--limit', str(self.wiki_limit)])
        elif self.limit is not None:
            args.extend(['--limit', str(self.limit)])

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('import_wiki_gameplay', args)

    def _run_fetch_steam(self):
        """Выполняет команды check_steam_games и fetch_steam_descriptions только для загруженных игр."""

        if not self.loaded_game_ids:
            self._log('   ⚠️ Нет загруженных игр для проверки в Steam')
            return

        self._create_game_ids_file()

        self._log(f'\n   🔍 Проверка наличия {len(self.loaded_game_ids)} игр в Steam...')
        args_check = [
            '--game-ids-file', 'temp_game_ids.txt',
        ]

        if self.dry_run:
            args_check.append('--dry-run')

        if self.verbose:
            args_check.append('--verbose')

        self._call_command('check_steam_games', args_check)

        self._log('\n   📥 Загрузка описаний из Steam...')
        args_fetch = [
            '--force',
            '--skip-not-found',
            '--skip-no-description',
            f'--workers={self.steam_workers}',
            f'--delay={self.steam_delay}',
            '--game-ids-file', 'temp_game_ids.txt',
        ]

        if self.steam_limit is not None:
            args_fetch.extend(['--limit', str(self.steam_limit)])
        elif self.limit is not None:
            args_fetch.extend(['--limit', str(self.limit)])

        if self.dry_run:
            args_fetch.append('--dry-run')

        if self.verbose:
            args_fetch.append('--verbose')

        self._call_command('fetch_steam_descriptions', args_fetch)

    def _run_add_keywords(self):
        """Выполняет команду add_new_keywords_from_descriptions."""
        args = ['--delete-stopwords']

        if self.keywords_limit is not None:
            args.extend(['--limit', str(self.keywords_limit)])
        elif self.limit is not None:
            args.extend(['--limit', str(self.limit)])

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('add_new_keywords_from_descriptions', args)

    def _run_normalize_keywords(self):
        """Выполняет команду normalize_keywords."""
        args = ['--merge-only']

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('normalize_keywords', args)

    def _run_criteria_reassign(self):
        """Выполняет команду criteries_reassign."""
        args = []

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('criteries_reassign', args)

    def _run_analyze_criteria(self):
        """Выполняет команду analyze_game_criteria_fast."""

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

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('analyze_game_criteria_fast', args)

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

        if self.dry_run:
            args_keywords.append('--dry-run')

        if self.verbose:
            args_keywords.append('--verbose')

        self._call_command('analyze_game_criteria_fast', args_keywords)

    def _run_update_vectors(self):
        """Выполняет команду update_vectors."""
        args = []

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

        self._call_command('update_vectors', args)

    def _run_delete_low_usage(self):
        """Выполняет команду delete_low_usage_keywords."""
        if not self.delete_low_usage:
            self._log('   ⏭️ Пропущено (опция не включена)')
            return

        args = [f'--usage={self.low_usage_threshold}']

        if self.dry_run:
            args.append('--dry-run')

        if self.verbose:
            args.append('--verbose')

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
        if self.dry_run:
            self._log(f'\n   🔧 [DRY-RUN] manage.py {command_name} {" ".join(args)}')
            return

        self._log(f'\n   ▶️ Выполняется: manage.py {command_name} {" ".join(args)}')
        self.stdout.flush()

        try:
            result = subprocess.run(
                [sys.executable, 'manage.py', command_name] + args,
                check=False
            )

            if result.returncode != 0:
                raise RuntimeError(f'Команда завершилась с кодом {result.returncode}')

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f'Ошибка выполнения команды {command_name}: {e}')
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

            if result.returncode != 0:
                raise RuntimeError(f'Команда завершилась с кодом {result.returncode}')

            return {'loaded_game_ids': []}

        except subprocess.CalledProcessError as e:
            raise RuntimeError(f'Ошибка выполнения команды {command_name}: {e}')
        except Exception as e:
            raise RuntimeError(f'Неизвестная ошибка при выполнении {command_name}: {e}')

    def _print_footer(self):
        """Выводит итоговую статистику выполнения."""
        total_time = (datetime.now() - self.start_time).total_seconds()

        self._log_section('📊 ИТОГОВАЯ СТАТИСТИКА ВЫПОЛНЕНИЯ')

        success_count = 0
        error_count = 0

        for step_name, result in self.step_results.items():
            status = result.get('status', 'unknown')
            time_str = f"{result.get('time', 0):.1f}с"
            if status == 'success':
                success_count += 1
                self._log(f'  ✅ {step_name}: {time_str}')
            elif status == 'error':
                error_count += 1
                self._log(f'  ❌ {step_name}: {time_str} - {result.get("error", "неизвестная ошибка")}')
            else:
                self._log(f'  ⏭️ {step_name}: {time_str}')

        self._log(f'\n📊 Всего шагов: {len(self.step_results)}')
        self._log(f'  ✅ Успешно: {success_count}')
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

        if self.dry_run:
            self._log('\n🔧 РЕЖИМ ПРОСМОТРА: изменения не были применены')
            self._log('📋 Все команды были показаны, но ни одна не выполнялась')
        else:
            self._log('\n✨ Пайплайн успешно завершен!')

        self._log('=' * 80)