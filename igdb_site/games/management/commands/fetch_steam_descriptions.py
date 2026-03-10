# games/management/commands/fetch_steam_descriptions.py

"""
Django management command для получения описаний игр из Steam API.
Автоматически перезапускается с новым оффсетом для каждой итерации.
Поддерживает поиск игры по названию.
"""

import requests
import time
import logging
import re
import traceback
import signal
import sys
import os
import subprocess
from typing import Optional, Dict, Any, List, Tuple
from datetime import datetime
from pathlib import Path
from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
from django.db.models import Q
from django.conf import settings
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from threading import Lock
from games.models_parts.game import Game
from games.models_parts.simple_models import Platform

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """Steam descriptions fetcher с автоматическим перезапуском для каждого оффсета."""

    help = 'Получение описаний из Steam с автоматическим перезапуском для каждого оффсета'

    def add_arguments(self, parser: CommandParser) -> None:
        """Добавление аргументов команды."""
        parser.add_argument(
            '--limit',
            type=int,
            default=100,
            help='Максимальное количество игр для обработки (по умолчанию: 100)'
        )

        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Смещение от начала списка (по умолчанию: 0)'
        )

        parser.add_argument(
            '--game-name',
            type=str,
            help='Название игры для поиска (будет найдена самая популярная)'
        )

        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Тестовый запуск без сохранения в базу данных'
        )

        parser.add_argument(
            '--batch-size',
            type=int,
            default=50,
            help='Размер итерации (по умолчанию: 50)'
        )

        parser.add_argument(
            '--iteration-pause',
            type=int,
            default=1,
            help='Пауза между итерациями в секундах (по умолчанию: 1)'
        )

        parser.add_argument(
            '--workers',
            type=int,
            default=10,
            help='Количество параллельных воркеров (по умолчанию: 10)'
        )

        parser.add_argument(
            '--delay',
            type=float,
            default=0.1,
            help='Задержка между запросами в секундах (по умолчанию: 0.1)'
        )

        parser.add_argument(
            '--timeout',
            type=float,
            default=1,
            help='Таймаут запроса в секундах (по умолчанию: 1)'
        )

        parser.add_argument(
            '--output-file',
            type=str,
            default='steam_descriptions.txt',
            help='Выходной TXT файл для описаний (будет создан в папке fetch_steam_descriptions)'
        )

        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительное обновление даже если описание уже есть'
        )

        parser.add_argument(
            '--skip-search',
            action='store_true',
            help='Пропустить поиск в Steam, использовать название игры напрямую'
        )

        parser.add_argument(
            '--debug',
            action='store_true',
            help='Показывать детальные сообщения об ошибках'
        )

        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Показывать подробный вывод включая API запросы'
        )

        parser.add_argument(
            '--no-restart',
            action='store_true',
            help='Не перезапускать команду для следующих оффсетов'
        )

        parser.add_argument(
            '--output-dir',
            type=str,
            default='fetch_steam_descriptions',
            help='Директория для сохранения файлов (по умолчанию: fetch_steam_descriptions)'
        )

    def __init__(self, *args, **kwargs):
        """Инициализация команды."""
        super().__init__(*args, **kwargs)
        self.stats_lock = Lock()
        self.output_lock = Lock()
        self.descriptions_buffer = []
        self.buffer_size = 50
        self.session = self._create_session()
        self.pc_platform = None
        self.debug = False
        self.verbose = False
        self.interrupted = False
        self.total_stats = {
            'success': 0,
            'not_found': 0,
            'no_description': 0,
            'error': 0,
            'iterations': 0
        }
        self.output_dir = None
        self.full_output_path = None
        self.current_offset = 0

    def _create_session(self) -> requests.Session:
        """Создание HTTP сессии без повторов."""
        session = requests.Session()

        adapter = requests.adapters.HTTPAdapter(
            pool_connections=30,
            pool_maxsize=30,
            max_retries=0
        )

        session.mount('http://', adapter)
        session.mount('https://', adapter)

        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
        })

        return session

    def signal_handler(self, signum, frame):
        """Обработчик сигнала прерывания (Ctrl+C) - максимально быстрое завершение."""
        self.stdout.write(self.style.ERROR('\n\n⚠️  Получен сигнал прерывания (Ctrl+C)'))
        self.stdout.write(self.style.ERROR('🛑 ФОРСИРОВАННОЕ ЗАВЕРШЕНИЕ...'))
        os._exit(130)

    def log_debug(self, message: str, game_name: str = None):
        """Логирование debug сообщений."""
        if self.debug:
            prefix = f"[DEBUG] {game_name}: " if game_name else "[DEBUG] "
            self.stdout.write(self.style.WARNING(f'{prefix}{message}'))

    def log_verbose(self, message: str, game_name: str = None):
        """Логирование verbose сообщений."""
        if self.verbose:
            prefix = f"[VERBOSE] {game_name}: " if game_name else "[VERBOSE] "
            self.stdout.write(self.style.NOTICE(f'{prefix}{message}'))

    def get_pc_platform(self) -> Optional[Platform]:
        """Получение платформы PC."""
        if self.pc_platform is None:
            try:
                self.pc_platform = Platform.objects.filter(
                    Q(name__iexact='PC') |
                    Q(name__iexact='PC (Microsoft Windows)') |
                    Q(name__icontains='windows')
                ).first()

                if self.pc_platform:
                    self.stdout.write(
                        self.style.SUCCESS(f'✅ Найдена платформа PC: {self.pc_platform.name}')
                    )
                else:
                    self.stdout.write(self.style.ERROR('❌ Платформа PC не найдена!'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Ошибка: {e}'))
                return None
        return self.pc_platform

    def get_games_batch(self, offset: int, batch_size: int, force: bool) -> List[Game]:
        """Получение батча игр с учетом смещения."""
        pc = self.get_pc_platform()
        if not pc:
            return []

        try:
            queryset = Game.objects.filter(platforms=pc)

            if not force:
                queryset = queryset.filter(
                    Q(rawg_description__isnull=True) |
                    Q(rawg_description='')
                )

            # ВАЖНО: Добавляем сортировку по ID или рейтингу для стабильности
            games = list(queryset.order_by('-rating_count', 'id')[offset:offset + batch_size])

            if games:
                self.stdout.write(
                    self.style.SUCCESS(f'📊 Батч: игры {offset + 1}-{offset + len(games)} (смещение {offset})')
                )
                # Для отладки покажем первую игру
                self.stdout.write(f'  Первая игра в батче: {games[0].name} (ID: {games[0].id})')

            return games

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка: {e}'))
            return []

    def clean_html(self, text: Optional[str]) -> Optional[str]:
        """Очистка HTML тегов."""
        if not text:
            return text
        text = re.sub(r'<[^>]+>', ' ', text)
        text = re.sub(r'\s+', ' ', text)
        return text.strip()

    def search_steam(self, game_name: str, timeout: float) -> Optional[int]:
        """Поиск игры в Steam."""
        search_name = re.sub(r'[^\w\s-]', '', game_name)
        search_name = re.sub(r'\s+', ' ', search_name).strip()

        if not search_name:
            return None

        try:
            url = "https://store.steampowered.com/api/storesearch"
            params = {
                'term': search_name[:50],
                'l': 'english',
                'cc': 'us'
            }

            response = self.session.get(url, params=params, timeout=timeout)

            if response.status_code == 200:
                data = response.json()
                if data.get('total', 0) > 0:
                    items = data.get('items', [])
                    if items:
                        return items[0].get('id')
        except Exception:
            pass

        return None

    def fetch_description(self, app_id: int, timeout: float, game_name: str = None) -> Optional[str]:
        """Получение описания игры."""
        try:
            url = "https://store.steampowered.com/api/appdetails"
            params = {
                'appids': app_id,
                'l': 'english',
                'cc': 'us'
            }

            response = self.session.get(url, params=params, timeout=timeout)

            if response.status_code != 200:
                return None

            data = response.json()

            if str(app_id) not in data or not data[str(app_id)]['success']:
                return None

            game_data = data[str(app_id)]['data']

            for field in ['detailed_description', 'about_the_game', 'short_description']:
                desc = game_data.get(field, '')
                if desc:
                    return self.clean_html(desc)

        except Exception:
            pass

        return None

    def format_for_file(self, game: Game, description: str, app_id: int) -> str:
        """Форматирование для файла."""
        try:
            genres = [g.name for g in game.genres.all()[:3]] if hasattr(game, 'genres') else ['N/A']
        except:
            genres = ['N/A']

        lines = [
            f"Game: {game.name}",
            f"ID: {game.id}",
            f"Steam App ID: {app_id}",
            f"Rating: {game.rating or 'N/A'}",
            f"Genres: {', '.join(genres)}",
            "",
            "DESCRIPTION:",
            description
        ]
        return "\n".join(lines)

    def save_buffer(self, output_file: str, is_first: bool = False):
        """Сохранение буфера описаний в файл."""
        if not self.descriptions_buffer:
            return

        # Создаем полный путь к файлу
        if self.output_dir:
            file_path = self.output_dir / output_file
        else:
            file_path = Path(output_file)

        # Создаем директорию если её нет
        file_path.parent.mkdir(parents=True, exist_ok=True)

        with self.output_lock:
            # Первая итерация - перезаписываем файл, остальные - добавляем
            mode = 'w' if is_first else 'a'

            with open(file_path, mode, encoding='utf-8') as f:
                # Если это первая итерация, пишем заголовок
                if is_first:
                    f.write("=" * 80 + "\n")
                    f.write(f"STEAM GAME DESCRIPTIONS\n")
                    f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Offset: {self.current_offset}\n")
                    f.write("=" * 80 + "\n\n")

                for desc in self.descriptions_buffer:
                    f.write(desc)
                    f.write("\n" + "-" * 80 + "\n\n")

            self.descriptions_buffer = []
            self.full_output_path = file_path

    def process_batch(self, games: List[Game], skip_search: bool, timeout: float,
                      delay: float, output_file: str, dry_run: bool,
                      batch_stats: Dict, is_first: bool = False) -> List[Game]:
        """Обработка одного батча игр."""
        games_to_update = []

        self.stdout.write(f'  🕒 Таймаут запросов: {timeout}с, воркеров: {self.workers}')

        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = []
            for game in games:
                future = executor.submit(
                    self.process_game,
                    game, skip_search, timeout, delay,
                    output_file, dry_run, batch_stats
                )
                futures.append((future, game))

            completed = 0
            timeout_count = 0

            for future, game in futures:
                try:
                    result = future.result(timeout=timeout + 5)
                    completed += 1

                    if result['success']:
                        self.stdout.write(
                            f'  [{completed}/{len(games)}] ✓ {game.name[:40]:40} (Steam: {result["app_id"]})'
                        )
                        if not dry_run:
                            game.rawg_description = result['description']
                            games_to_update.append(game)

                    elif result['skipped']:
                        icon = {
                            'not_found': '🔍',
                            'no_description': '📄',
                            'exception': '💥',
                        }.get(result.get('error_type'), '⏭️')

                        self.stdout.write(
                            f'  [{completed}/{len(games)}] {icon} {game.name[:40]:40} - {result.get("error_message", "Пропущено")}'
                        )

                except TimeoutError:
                    completed += 1
                    timeout_count += 1
                    self.stdout.write(
                        f'  [{completed}/{len(games)}] ⌛ {game.name[:40]:40} - ТАЙМАУТ (превышено {timeout}с)'
                    )
                    with self.stats_lock:
                        batch_stats['error'] += 1
                        self.total_stats['error'] += 1

                except Exception as e:
                    completed += 1
                    self.stdout.write(
                        f'  [{completed}/{len(games)}] ⚠️ {game.name[:40]:40} - Ошибка: {str(e)[:30]}'
                    )
                    with self.stats_lock:
                        batch_stats['error'] += 1
                        self.total_stats['error'] += 1

            if timeout_count > 0:
                self.stdout.write(f'  ⚠️ В батче произошло {timeout_count} таймаутов')

        # Сохраняем буфер после обработки батча (даже при dry-run)
        if self.descriptions_buffer:
            self.save_buffer(output_file, is_first)

        return games_to_update

    def process_game(self, game: Game, skip_search: bool, timeout: float,
                     delay: float, output_file: str, dry_run: bool, stats: Dict) -> Dict:
        """Обработка одной игры."""
        result = {
            'success': False,
            'skipped': False,
            'description': None,
            'app_id': None,
            'error_type': None,
            'error_message': None
        }

        try:
            if delay > 0:
                time.sleep(delay)

            app_id = None
            if not skip_search:
                app_id = self.search_steam(game.name, timeout)

            if not app_id:
                with self.stats_lock:
                    stats['not_found'] += 1
                    self.total_stats['not_found'] += 1
                result['skipped'] = True
                result['error_type'] = 'not_found'
                result['error_message'] = 'Не найдена'
                return result

            description = self.fetch_description(app_id, timeout, game.name)

            if not description:
                with self.stats_lock:
                    stats['no_description'] += 1
                    self.total_stats['no_description'] += 1
                result['skipped'] = True
                result['error_type'] = 'no_description'
                result['error_message'] = 'Нет описания'
                return result

            result['success'] = True
            result['description'] = description
            result['app_id'] = app_id

            # Сохраняем в файл даже при dry-run
            if output_file:
                formatted = self.format_for_file(game, description, app_id)
                with self.output_lock:
                    self.descriptions_buffer.append(formatted)
                    if len(self.descriptions_buffer) >= self.buffer_size:
                        self.save_buffer(output_file, is_first=(self.current_offset == 0))

            with self.stats_lock:
                stats['success'] += 1
                self.total_stats['success'] += 1

            return result

        except Exception as e:
            with self.stats_lock:
                stats['error'] += 1
                self.total_stats['error'] += 1
            result['skipped'] = True
            result['error_type'] = 'exception'
            result['error_message'] = str(e)[:50]
            return result

    def handle(self, *args: Any, **options: Any) -> None:
        """Основной метод выполнения."""
        # Устанавливаем обработчик сигнала
        signal.signal(signal.SIGINT, self.signal_handler)

        limit = options['limit']
        self.current_offset = options['offset']
        game_name = options['game_name']
        dry_run = options['dry_run']
        batch_size = options['batch_size']
        iteration_pause = options['iteration_pause']
        self.workers = options['workers']
        delay = options['delay']
        timeout = options['timeout']
        output_file = options['output_file']
        force = options['force']
        skip_search = options['skip_search']
        self.debug = options['debug']
        self.verbose = options['verbose']
        no_restart = options['no_restart']
        output_dir = options['output_dir']

        # Создаем директорию для выходных файлов
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('STEAM DESCRIPTIONS FETCHER (АВТОПЕРЕЗАПУСК)'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        # Получаем PC платформу
        pc = self.get_pc_platform()
        if not pc:
            return

        # Переменная для хранения найденной игры
        target_game = None

        # Если указано имя игры, ищем её
        if game_name:
            self.stdout.write(f'🔍 Поиск игры: "{game_name}"')

            # Сначала точный поиск (без учета регистра)
            self.stdout.write(f'  Шаг 1: Поиск точного совпадения...')
            exact_games = Game.objects.filter(
                Q(name__iexact=game_name) &
                Q(platforms=pc)
            ).order_by('-rating_count')

            exact_count = exact_games.count()
            self.stdout.write(f'  Найдено точных совпадений: {exact_count}')

            if exact_count > 0:
                target_game = exact_games.first()
                self.stdout.write(self.style.SUCCESS(
                    f'  ✅ Точное совпадение: {target_game.name} (ID: {target_game.id}, рейтинг: {target_game.rating})'))
            else:
                # Поиск по частичному совпадению
                self.stdout.write(f'  Шаг 2: Поиск частичных совпадений...')

                all_matches = Game.objects.filter(
                    Q(name__icontains=game_name) &
                    Q(platforms=pc)
                ).order_by('-rating_count')

                total_matches = all_matches.count()
                self.stdout.write(f'  Всего найдено частичных совпадений: {total_matches}')

                if total_matches > 0:
                    self.stdout.write(f'  Топ-5 по рейтингу:')
                    for i, g in enumerate(all_matches[:5], 1):
                        self.stdout.write(f'    {i}. {g.name} (ID: {g.id}, рейтинг: {g.rating})')

                    target_game = all_matches.first()
                    self.stdout.write(self.style.SUCCESS(
                        f'  ✅ Выбрана: {target_game.name} (ID: {target_game.id}, рейтинг: {target_game.rating})'))
                else:
                    self.stdout.write(self.style.ERROR(f'❌ Игра "{game_name}" не найдена'))
                    return

            # Устанавливаем параметры для одной игры
            limit = 1
            self.current_offset = 0
            batch_size = 1
            no_restart = True

        self.stdout.write(f'Лимит: {limit} игр')
        self.stdout.write(f'Текущий offset: {self.current_offset}')
        self.stdout.write(f'Размер итерации: {batch_size}')
        self.stdout.write(f'Пауза между итерациями: {iteration_pause}с')
        self.stdout.write(f'Воркеров: {self.workers}')
        self.stdout.write(f'Задержка: {delay}с')
        self.stdout.write(f'Таймаут: {timeout}с')
        self.stdout.write(f'Dry run: {dry_run}')
        self.stdout.write(f'Выходная директория: {self.output_dir}')
        self.stdout.write(f'Выходной файл: {output_file}')
        self.stdout.write('=' * 60)
        self.stdout.write(self.style.WARNING('⚠️  Нажмите Ctrl+C для завершения'))
        self.stdout.write('=' * 60)

        # Получаем игры для обработки
        games_to_process = []

        if target_game:
            # Используем конкретную найденную игру
            games_to_process = [target_game]
            self.stdout.write(self.style.SUCCESS(f'📊 Обработка конкретной игры: {target_game.name}'))
        else:
            # Получаем батч игр по offset
            games_to_process = self.get_games_batch(self.current_offset, batch_size, force)

        if not games_to_process:
            self.stdout.write(self.style.WARNING('Нет игр для обработки'))
            return

        # Статистика для текущего батча
        batch_stats = {
            'success': 0,
            'not_found': 0,
            'no_description': 0,
            'error': 0
        }

        # Обрабатываем текущий батч
        games_to_update = self.process_batch(
            games_to_process, skip_search, timeout, delay,
            output_file, dry_run, batch_stats,
            is_first=(self.current_offset == 0)
        )

        # Сохраняем буфер если остался
        if self.descriptions_buffer:
            self.save_buffer(output_file, is_first=(self.current_offset == 0))

        # Обновляем БД для текущего батча
        if games_to_update and not dry_run:
            with transaction.atomic():
                Game.objects.bulk_update(games_to_update, ['rawg_description'])
            self.stdout.write(self.style.SUCCESS(f'✅ Обновлено {len(games_to_update)} игр в БД'))

        # Выводим статистику текущего батча
        self.stdout.write(self.style.SUCCESS(f'\n📊 ИТОГ ТЕКУЩЕЙ ИТЕРАЦИИ (offset {self.current_offset}):'))
        self.stdout.write(f'  ✓ Успешно: {batch_stats["success"]}')
        self.stdout.write(f'  🔍 Не найдено: {batch_stats["not_found"]}')
        self.stdout.write(f'  📄 Нет описания: {batch_stats["no_description"]}')
        if batch_stats['error'] > 0:
            self.stdout.write(self.style.ERROR(f'  💥 Ошибок: {batch_stats["error"]}'))

        # Проверяем нужно ли перезапускаться для следующего оффсета
        next_offset = self.current_offset + batch_size
        if not no_restart and not target_game and next_offset < limit and not self.interrupted:
            self.stdout.write(self.style.WARNING(f'\n🔄 Подготовка к следующей итерации (offset {next_offset})...'))
            self.stdout.write(self.style.WARNING(f'⏳ Пауза {iteration_pause}с перед перезапуском...'))

            # Обратный отсчет
            for i in range(iteration_pause, 0, -1):
                if i > 1:
                    time.sleep(1)
                    self.stdout.write(self.style.WARNING(f'   Осталось {i}с...'))
                else:
                    time.sleep(1)
                    break

            self.stdout.write(self.style.WARNING(f'\n🔄 Перезапуск команды с offset {next_offset}...'))

            # Формируем команду для следующего оффсета
            cmd = [
                sys.executable, "manage.py", "fetch_steam_descriptions",
                f"--limit={limit}",
                f"--offset={next_offset}",
                f"--batch-size={batch_size}",
                f"--iteration-pause={iteration_pause}",
                f"--workers={self.workers}",
                f"--delay={delay}",
                f"--timeout={timeout}",
                f"--output-file={output_file}",
                f"--output-dir={output_dir}",
            ]

            if dry_run:
                cmd.append("--dry-run")
            if force:
                cmd.append("--force")
            if skip_search:
                cmd.append("--skip-search")
            if self.debug:
                cmd.append("--debug")
            if self.verbose:
                cmd.append("--verbose")

            # Запускаем следующую команду
            self.stdout.write(self.style.SUCCESS(f'🚀 Запуск: {" ".join(cmd)}'))
            self.stdout.write('')

            try:
                subprocess.run(cmd, check=True)
            except subprocess.CalledProcessError as e:
                self.stdout.write(self.style.ERROR(f'❌ Ошибка при перезапуске: {e}'))
            except KeyboardInterrupt:
                self.stdout.write(self.style.WARNING('\n🛑 Перезапуск прерван пользователем'))
                sys.exit(130)
        else:
            if next_offset >= limit:
                self.stdout.write(self.style.SUCCESS('\n✅ Достигнут лимит, все итерации завершены'))
            elif no_restart:
                self.stdout.write(self.style.WARNING('\n⏹️ Автоперезапуск отключен (--no-restart)'))
            elif target_game:
                self.stdout.write(self.style.SUCCESS('\n✅ Обработка конкретной игры завершена'))

            # Финальный вывод о файле
            if self.full_output_path and self.full_output_path.exists():
                size = self.full_output_path.stat().st_size
                self.stdout.write(self.style.SUCCESS(f'\n📁 Результаты сохранены в файл: {self.full_output_path}'))
                self.stdout.write(self.style.SUCCESS(f'📊 Размер файла: {size:,} байт ({size / 1024:.1f} КБ)'))
            else:
                # Проверяем есть ли файл в директории
                expected_path = self.output_dir / output_file
                if expected_path.exists():
                    size = expected_path.stat().st_size
                    self.stdout.write(self.style.SUCCESS(f'\n📁 Результаты сохранены в файл: {expected_path}'))
                    self.stdout.write(self.style.SUCCESS(f'📊 Размер файла: {size:,} байт ({size / 1024:.1f} КБ)'))
                else:
                    self.stdout.write(self.style.WARNING(f'\n📁 Файл не создан (нет данных или dry run)'))
