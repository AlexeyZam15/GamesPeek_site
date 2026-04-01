"""
Django management command для массовой проверки наличия игр в Steam.
Делает один запрос для проверки множества игр.
"""

import requests
import time
import logging
import signal
import sys
import uuid
import shutil
from typing import List, Set, Dict, Tuple
from datetime import datetime
from pathlib import Path
from django.core.management.base import BaseCommand, CommandParser
from django.db.models import Q
from django.conf import settings
from concurrent.futures import ThreadPoolExecutor, as_completed
from games.models_parts.game import Game
from games.models_parts.simple_models import Platform

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    """Команда для массовой проверки наличия игр в Steam."""

    help = 'Массовая проверка игр в Steam API (один запрос на несколько игр)'

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            '--batch-size',
            type=int,
            default=60,
            help='Количество игр в одном запросе (по умолчанию: 60)'
        )

        parser.add_argument(
            '--delay',
            type=float,
            default=0.5,
            help='Задержка между запросами в секундах (по умолчанию: 0.5)'
        )

        parser.add_argument(
            '--output-dir',
            type=str,
            default='steam_data',
            help='Директория для сохранения файлов (по умолчанию: steam_data)'
        )

        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Максимальное количество игр для проверки (по умолчанию: все)'
        )

        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Смещение от начала списка (по умолчанию: 0)'
        )

        parser.add_argument(
            '--only-missing',
            action='store_true',
            help='Проверять только игры, которых нет в found или not_found файлах'
        )

        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительная проверка всех игр (игнорировать кэш)'
        )

        parser.add_argument(
            '--check-not-found',
            action='store_true',
            help='Повторно проверить игры из файла не найденных (steam_not_found.txt)'
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = self._create_session()
        self.pc_platform = None
        self.found_games = set()
        self.not_found_games = set()
        self.output_dir = None
        self.found_file = None
        self.not_found_file = None
        self.games_cache_file = None
        self.cache_data = {}
        self.interrupted = False  # Добавляем атрибут для отслеживания прерывания

    def save_progress_and_exit(self, cache_file: Path, progress_file: Path, processed: int, total_games: int):
        """
        Сохранение прогресса и корректный выход при прерывании.
        """
        self.stdout.write(self.style.WARNING('\n💾 Сохранение прогресса перед выходом...'))

        # Сохраняем кэш
        self.save_cache_data(cache_file, self.cache_data)

        # Сохраняем информацию о прогрессе
        try:
            with open(progress_file, 'w', encoding='utf-8') as f:
                f.write(f"Last processed offset: {processed}\n")
                f.write(f"Total games: {total_games}\n")
                f.write(f"Progress: {processed}/{total_games} ({processed / total_games * 100:.1f}%)\n")
                f.write(f"Last checked: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Interrupted: True\n")
            self.stdout.write(self.style.SUCCESS(f'✅ Прогресс сохранен: {processed}/{total_games} игр'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка сохранения прогресса: {e}'))

        self.stdout.write(self.style.WARNING('\n⚠️ Для продолжения запустите команду без --force'))
        self.stdout.write(self.style.WARNING('   Прогресс будет восстановлен автоматически'))

    def signal_handler(self, signum, frame):
        """Обработчик сигнала прерывания (Ctrl+C)."""
        self.stdout.write(self.style.ERROR('\n\n⚠️  Получен сигнал прерывания (Ctrl+C)'))
        self.interrupted = True

    def get_last_processed_offset(self, cache_file_path: Path) -> int:
        """
        Получение последнего обработанного оффсета из кэша.
        Возвращает количество уже обработанных игр.
        """
        if not cache_file_path.exists():
            return 0

        try:
            import json
            with open(cache_file_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)

            # Возвращаем количество записей в кэше как количество обработанных игр
            return len(cache)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка чтения кэша для определения оффсета: {e}'))
            return 0

    def save_cache_data(self, cache_file_path: Path, cache_data: Dict[int, Dict[str, any]]):
        """
        Сохранение кэша с данными о проверенных играх.
        """
        try:
            import json
            # Создаем резервную копию если файл существует
            if cache_file_path.exists():
                backup_path = cache_file_path.with_suffix('.backup.json')
                import shutil
                shutil.copy2(cache_file_path, backup_path)

            with open(cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
            # Убираем вывод сообщения, чтобы не сбивать прогресс-бар
            # self.stdout.write(self.style.SUCCESS(f'💾 Кэш сохранен: {len(cache_data)} игр'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка сохранения кэша: {e}'))

    def load_cache_data(self, cache_file_path: Path) -> Dict[int, Dict[str, any]]:
        """
        Загрузка кэша с данными о всех проверенных играх.
        Кэш содержит информацию: найден ли игра, app_id, дата проверки.
        """
        cache = {}

        if not cache_file_path.exists():
            return cache

        try:
            import json
            with open(cache_file_path, 'r', encoding='utf-8') as f:
                cache = json.load(f)
                # Преобразуем ключи из строк в числа
                cache = {int(k): v for k, v in cache.items()}
            self.stdout.write(self.style.SUCCESS(f'📂 Загружен кэш: {len(cache)} игр'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка загрузки кэша: {e}'))

        return cache

    def display_progress_bar(self, current: int, total: int, start_time: datetime,
                             found_count: int = 0, not_found_count: int = 0) -> None:
        """
        Отображает обновляемый прогресс-бар с расчётом времени завершения.
        Использует символы ASCII для совместимости со всеми терминалами.
        """
        if total == 0:
            return

        # Вычисляем проценты
        percent = current / total
        bar_width = 50
        filled_length = int(bar_width * percent)
        bar = '█' * filled_length + '░' * (bar_width - filled_length)

        # Расчёт времени
        elapsed = (datetime.now() - start_time).total_seconds()
        if current > 0:
            eta_seconds = (elapsed / current) * (total - current)
            eta_minutes = int(eta_seconds // 60)
            eta_seconds_remain = int(eta_seconds % 60)

            if eta_minutes > 60:
                eta_hours = eta_minutes // 60
                eta_minutes = eta_minutes % 60
                eta_str = f"{eta_hours}h {eta_minutes}m"
            elif eta_minutes > 0:
                eta_str = f"{eta_minutes}m {eta_seconds_remain}s"
            else:
                eta_str = f"{eta_seconds_remain}s"
        else:
            eta_str = "calculating..."

        # Форматируем скорость
        if elapsed > 0:
            speed = current / elapsed
            speed_str = f"{speed:.1f} games/s"
        else:
            speed_str = "0 games/s"

        # Формируем строку прогресса
        progress_line = (
            f"\r[{bar}] {percent:>6.1%} | "
            f"{current:>6}/{total:<6} | "
            f"✅{found_count:>5} ❌{not_found_count:>5} | "
            f"⏱️{elapsed:>5.0f}s | "
            f"🚀{speed_str:>12} | "
            f"⌛ETA: {eta_str:>12}"
        )

        # Очищаем строку и выводим новую
        self.stdout.write(progress_line, ending='')
        self.stdout.flush()

    def load_not_found_games_for_recheck(self, not_found_file_path: Path) -> List[Tuple[int, str, int]]:
        """
        Загрузка игр из файла not_found для повторной проверки.
        Единый формат: Game ID: 12345 - Название игры (Steam App ID: 67890) - не найдено в Steam
        Возвращает список кортежей (game_id, game_name, app_id)
        """
        games_to_recheck = []

        if not not_found_file_path.exists():
            self.stdout.write(self.style.ERROR(f'❌ Файл {not_found_file_path} не найден'))
            return games_to_recheck

        try:
            with open(not_found_file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith('=') or line.startswith('STEAM NOT FOUND GAMES') or line.startswith('Created:'):
                        continue

                    if 'Game ID:' in line:
                        try:
                            # Парсим формат: Game ID: 12345 - Название игры (Steam App ID: 67890) - не найдено в Steam
                            game_part = line.split('Game ID:')[1].strip()

                            # Извлекаем game_id
                            if '-' in game_part:
                                game_id_str = game_part.split('-')[0].strip()
                                game_id = int(game_id_str)

                                # Извлекаем название игры
                                rest_after_id = game_part.split('-', 1)[1].strip()

                                # Извлекаем app_id из скобок
                                app_id = None
                                game_name = rest_after_id

                                if '(' in rest_after_id and ')' in rest_after_id:
                                    app_id_part = rest_after_id.split('(')[1].split(')')[0]
                                    if 'Steam App ID:' in app_id_part:
                                        app_id_str = app_id_part.split('Steam App ID:')[1].strip()
                                        if app_id_str != 'None':
                                            try:
                                                app_id = int(app_id_str)
                                            except ValueError:
                                                app_id = None

                                    # Извлекаем название (часть до скобки)
                                    game_name = rest_after_id.split('(')[0].strip()

                                games_to_recheck.append((game_id, game_name, app_id))
                            else:
                                self.stdout.write(
                                    self.style.WARNING(f'  ⚠️ Пропущена строка в неправильном формате: {line[:50]}'))
                                continue

                        except (ValueError, IndexError) as e:
                            self.stdout.write(self.style.WARNING(f'  ⚠️ Ошибка парсинга строки: {line[:50]}'))
                            continue

            self.stdout.write(self.style.SUCCESS(
                f'📂 Загружено {len(games_to_recheck)} игр из not_found файла для повторной проверки'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'Ошибка загрузки not_found файла: {e}'))

        return games_to_recheck

    def _create_session(self) -> requests.Session:
        """Создание HTTP сессии."""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.9',
        })
        return session

    def get_pc_platform(self) -> Platform:
        """Получение платформы PC."""
        if self.pc_platform is None:
            queries = [
                Q(name__iexact='PC'),
                Q(name__iexact='PC (Microsoft Windows)'),
                Q(name__icontains='windows'),
            ]
            query = Q()
            for q in queries:
                query |= q
            self.pc_platform = Platform.objects.filter(query).first()
        return self.pc_platform

    def load_existing_games(self, found_file_path: Path, not_found_file_path: Path) -> Tuple[Set[int], Set[int]]:
        """Загрузка уже найденных и не найденных игр из файлов."""
        found = set()
        not_found = set()

        if found_file_path.exists():
            try:
                with open(found_file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if 'Game ID:' in line:
                            try:
                                game_id = int(line.split('Game ID:')[1].split('-')[0].strip())
                                found.add(game_id)
                            except (ValueError, IndexError):
                                pass
                self.stdout.write(self.style.SUCCESS(f'📂 Загружено найденных игр: {len(found)}'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Ошибка загрузки found файла: {e}'))

        if not_found_file_path.exists():
            try:
                with open(not_found_file_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if 'Game ID:' in line:
                            try:
                                game_id = int(line.split('Game ID:')[1].split('-')[0].strip())
                                not_found.add(game_id)
                            except (ValueError, IndexError):
                                pass
                self.stdout.write(self.style.SUCCESS(f'📂 Загружено не найденных игр: {len(not_found)}'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'Ошибка загрузки not_found файла: {e}'))

        return found, not_found

    def check_games_batch(self, games: List[Tuple[int, str, int]]) -> Dict[int, Tuple[bool, int, str]]:
        """
        Проверка одной партии игр в Steam с параллельными запросами.
        games: список кортежей (game_id, game_name, app_id)
        Возвращает словарь {game_id: (найдена, app_id, game_name)}.
        """
        results = {}

        if not games:
            return results

        # 12 параллельных запросов
        max_workers = min(12, len(games))

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for game_id, game_name, app_id in games:
                future = executor.submit(self.search_single_game, game_id, game_name, app_id)
                futures[future] = (game_id, game_name)

            for future in as_completed(futures):
                game_id, game_name = futures[future]

                try:
                    found, new_app_id = future.result(timeout=5)
                    results[game_id] = (found, new_app_id, game_name)
                except Exception as e:
                    results[game_id] = (False, None, game_name)

        return results

    def search_single_game(self, game_id: int, game_name: str, app_id: int = None) -> Tuple[bool, int]:
        """Поиск одной игры в Steam с максимальной скоростью."""
        try:
            # Если у нас уже есть app_id из предыдущей проверки, используем его для быстрой проверки
            if app_id and app_id > 0:
                # Используем короткий таймаут для быстрой проверки
                url = "https://store.steampowered.com/api/appdetails"
                params = {
                    'appids': app_id,
                    'l': 'english',
                    'cc': 'us'
                }
                try:
                    response = self.session.get(url, params=params, timeout=2.0)
                    if response.status_code == 200:
                        data = response.json()
                        str_app_id = str(app_id)
                        if str_app_id in data and data[str_app_id].get('success'):
                            return True, app_id
                except:
                    pass

            # Если не сработало или app_id нет, пробуем поиск по названию
            return self.search_by_name(game_name)

        except Exception as e:
            return False, None

    def search_by_name(self, game_name: str) -> Tuple[bool, int]:
        """Быстрый поиск игры по названию с коротким таймаутом."""
        try:
            # Очищаем название для поиска
            search_name = game_name[:100].strip()

            url = "https://store.steampowered.com/api/storesearch"
            params = {
                'term': search_name,
                'l': 'english',
                'cc': 'us'
            }

            # Используем короткий таймаут для скорости
            response = self.session.get(url, params=params, timeout=2.0)

            if response.status_code != 200:
                return False, None

            data = response.json()
            if data.get('total', 0) > 0:
                items = data.get('items', [])
                if items:
                    app_id = items[0].get('id')
                    return True, app_id

            return False, None

        except Exception as e:
            return False, None

    def save_results(self, results: Dict[int, Tuple[bool, int, str]], found_file: Path, not_found_file: Path):
        """Сохранение результатов проверки в файлы с единым форматом."""
        found_count = 0
        not_found_count = 0

        with open(found_file, 'a', encoding='utf-8') as f_found, \
                open(not_found_file, 'a', encoding='utf-8') as f_not_found:

            for game_id, (found, app_id, game_name) in results.items():
                if found:
                    # Формат: Game ID: 12345 - Название игры (Steam App ID: 67890)
                    f_found.write(f"Game ID: {game_id} - {game_name} (Steam App ID: {app_id})\n")
                    found_count += 1
                else:
                    # Формат: Game ID: 12345 - Название игры (Steam App ID: None) - не найдено в Steam
                    f_not_found.write(f"Game ID: {game_id} - {game_name} (Steam App ID: None) - не найдено в Steam\n")
                    not_found_count += 1

        return found_count, not_found_count

    def handle(self, *args, **options):
        """Основной метод выполнения."""
        start_time = datetime.now()

        # Устанавливаем обработчик сигнала
        signal.signal(signal.SIGINT, self.signal_handler)

        # Получаем параметры
        batch_size = options['batch_size']
        delay = options['delay']
        output_dir = options['output_dir']
        limit = options['limit']
        offset = options['offset']
        only_missing = options['only_missing']
        force = options['force']
        check_not_found = options['check_not_found']

        # Создаем директорию
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Пути к файлам
        found_file = output_path / 'steam_found.txt'
        not_found_file = output_path / 'steam_not_found.txt'
        cache_file = output_path / 'steam_cache.json'
        progress_file = output_path / 'steam_progress.txt'

        # Если указан --force, очищаем все файлы результатов
        if force:
            self.stdout.write(self.style.WARNING('⚠️  РЕЖИМ FORCE: ОЧИСТКА ВСЕХ ПРОВЕРЕННЫХ ИГР'))
            self.stdout.write(self.style.WARNING('=' * 60))

            if cache_file.exists():
                cache_file.unlink()
                self.stdout.write(self.style.SUCCESS(f'✅ Удален файл кэша: {cache_file}'))

            if progress_file.exists():
                progress_file.unlink()
                self.stdout.write(self.style.SUCCESS(f'✅ Удален файл прогресса: {progress_file}'))

            with open(found_file, 'w', encoding='utf-8') as f:
                f.write(f"{'=' * 80}\n")
                f.write(f"STEAM FOUND GAMES\n")
                f.write(f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'=' * 80}\n\n")
            self.stdout.write(self.style.SUCCESS(f'✅ Очищен файл найденных игр: {found_file}'))

            with open(not_found_file, 'w', encoding='utf-8') as f:
                f.write(f"{'=' * 80}\n")
                f.write(f"STEAM NOT FOUND GAMES\n")
                f.write(f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'=' * 80}\n\n")
            self.stdout.write(self.style.SUCCESS(f'✅ Очищен файл не найденных игр: {not_found_file}'))

            self.stdout.write(self.style.WARNING('=' * 60))
            self.stdout.write(self.style.WARNING('✅ ОЧИСТКА ЗАВЕРШЕНА, НАЧИНАЕМ ПРОВЕРКУ ЗАНОВО'))
            self.stdout.write('')

        # Создаем файлы с заголовками если их нет
        if not found_file.exists():
            with open(found_file, 'w', encoding='utf-8') as f:
                f.write(f"{'=' * 80}\n")
                f.write(f"STEAM FOUND GAMES\n")
                f.write(f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'=' * 80}\n\n")

        if not not_found_file.exists():
            with open(not_found_file, 'w', encoding='utf-8') as f:
                f.write(f"{'=' * 80}\n")
                f.write(f"STEAM NOT FOUND GAMES\n")
                f.write(f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'=' * 80}\n\n")

        # Загружаем кэш
        self.cache_data = self.load_cache_data(cache_file)

        # Получаем платформу PC
        pc = self.get_pc_platform()
        if not pc:
            self.stdout.write(self.style.ERROR('❌ Платформа PC не найдена'))
            return

        # Определяем список игр для проверки
        games_to_check = []

        if check_not_found:
            # Режим повторной проверки не найденных игр
            self.stdout.write(self.style.WARNING('🔄 Режим повторной проверки не найденных игр'))
            games_to_check = self.load_not_found_games_for_recheck(not_found_file)

            if not games_to_check:
                self.stdout.write(self.style.ERROR('❌ Нет игр для повторной проверки'))
                return

            # Создаем резервную копию текущего not_found файла
            backup_file = not_found_file.with_suffix('.backup.txt')
            if not_found_file.exists():
                import shutil
                shutil.copy2(not_found_file, backup_file)
                self.stdout.write(self.style.WARNING(f'📋 Создана резервная копия: {backup_file}'))

            # Очищаем файл not_found перед записью новых результатов
            with open(not_found_file, 'w', encoding='utf-8') as f:
                f.write(f"{'=' * 80}\n")
                f.write(f"STEAM NOT FOUND GAMES (после повторной проверки)\n")
                f.write(f"Created: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"{'=' * 80}\n\n")

            self.stdout.write(self.style.WARNING(f'📝 Файл not_found очищен, будут записаны только новые результаты'))

        else:
            # Стандартный режим проверки
            queryset = Game.objects.filter(platforms=pc)

            # Определяем начальный оффсет
            processed_offset = 0

            if not force:
                # Получаем количество уже обработанных игр из кэша
                processed_offset = len(self.cache_data)

                # Проверяем наличие файла прогресса от прерванного запуска
                if progress_file.exists():
                    try:
                        with open(progress_file, 'r', encoding='utf-8') as f:
                            progress_data = f.read()
                            for line in progress_data.split('\n'):
                                if 'Last processed offset:' in line:
                                    saved_offset = int(line.split(':')[1].strip())
                                    if saved_offset > processed_offset:
                                        processed_offset = saved_offset
                                        self.stdout.write(
                                            self.style.WARNING(f'📊 Восстановлен прогресс из файла: {saved_offset} игр'))
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f'Ошибка чтения файла прогресса: {e}'))

                if processed_offset > 0:
                    self.stdout.write(self.style.WARNING(f'📊 Обнаружено {processed_offset} уже проверенных игр в кэше'))
                    self.stdout.write(self.style.WARNING(f'📊 Восстановление с оффсета {processed_offset}'))

                    # Применяем оффсет к queryset
                    queryset = queryset[processed_offset:]
            else:
                self.stdout.write(self.style.WARNING(f'📊 Режим FORCE: начинаем с нуля (оффсет 0)'))

            # Если включен only_missing, исключаем ID из found/not_found файлов
            if only_missing and not force:
                existing_found, existing_not_found = self.load_existing_games(found_file, not_found_file)
                checked_ids = existing_found | existing_not_found
                if checked_ids:
                    queryset = queryset.exclude(id__in=checked_ids)
                    self.stdout.write(self.style.WARNING(f'📊 Исключено уже проверенных игр: {len(checked_ids)}'))

            total_games = queryset.count()

            if limit is not None:
                total_to_check = min(limit, total_games)
                self.stdout.write(self.style.WARNING(f'📊 Лимит: {total_to_check} из {total_games} игр'))
            else:
                total_to_check = total_games
                self.stdout.write(self.style.WARNING(f'📊 Проверяем все {total_to_check} игр'))

            # Получаем игры с учетом offset и limit
            if limit is not None:
                games_to_check = list(queryset.values_list('id', 'name')[offset:offset + total_to_check])
            else:
                games_to_check = list(queryset.values_list('id', 'name')[offset:])

        total_games = len(games_to_check)

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('MASS STEAM GAME CHECKER'))
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(f'\n📊 Всего игр для проверки: {total_games}')
        self.stdout.write(f'📊 Размер батча: {batch_size}')
        self.stdout.write(f'📊 Задержка: {delay}с')
        self.stdout.write(f'📊 Параллельных запросов: 12')
        self.stdout.write('=' * 60)

        # Обрабатываем игры батчами
        total_found = 0
        total_not_found = 0
        processed = 0

        # Открываем файлы для прямой записи
        found_file_handle = open(found_file, 'a', encoding='utf-8')
        not_found_file_handle = open(not_found_file, 'a', encoding='utf-8')

        try:
            for i in range(0, total_games, batch_size):
                # Проверяем флаг прерывания
                if self.interrupted:
                    self.stdout.write(self.style.ERROR('\n⚠️ Обнаружено прерывание...'))
                    break

                batch = games_to_check[i:i + batch_size]

                # Преобразуем batch в формат (game_id, game_name, app_id) для единообразия
                batch_with_app_id = []
                for item in batch:
                    if len(item) == 3:
                        batch_with_app_id.append(item)
                    else:
                        game_id, game_name = item
                        # Пытаемся получить app_id из кэша
                        app_id = None
                        if str(game_id) in self.cache_data:
                            app_id = self.cache_data[str(game_id)].get('app_id')
                        batch_with_app_id.append((game_id, game_name, app_id))

                # Проверяем батч с параллельными запросами
                results = self.check_games_batch(batch_with_app_id)

                # Сохраняем результаты
                batch_found = 0
                batch_not_found = 0

                for game_id, (found, app_id, game_name) in results.items():
                    if found:
                        found_file_handle.write(f"Game ID: {game_id} - {game_name} (Steam App ID: {app_id})\n")
                        found_file_handle.flush()
                        batch_found += 1
                        self.cache_data[str(game_id)] = {
                            'found': True,
                            'app_id': app_id,
                            'checked_at': datetime.now().isoformat(),
                            'game_name': game_name
                        }
                    else:
                        not_found_file_handle.write(
                            f"Game ID: {game_id} - {game_name} (Steam App ID: None) - не найдено в Steam\n")
                        not_found_file_handle.flush()
                        batch_not_found += 1
                        self.cache_data[str(game_id)] = {
                            'found': False,
                            'app_id': None,
                            'checked_at': datetime.now().isoformat(),
                            'game_name': game_name
                        }

                total_found += batch_found
                total_not_found += batch_not_found
                processed += len(batch)

                # Сохраняем кэш после каждого батча
                self.save_cache_data(cache_file, self.cache_data)

                # Обновляем прогресс-бар
                percent = processed / total_games
                bar_width = 50
                filled_length = int(bar_width * percent)
                bar = '█' * filled_length + '░' * (bar_width - filled_length)

                elapsed = (datetime.now() - start_time).total_seconds()
                if processed > 0:
                    eta_seconds = (elapsed / processed) * (total_games - processed)
                    if eta_seconds > 3600:
                        eta_hours = int(eta_seconds // 3600)
                        eta_minutes = int((eta_seconds % 3600) // 60)
                        eta_str = f"{eta_hours}h {eta_minutes}m"
                    elif eta_seconds > 60:
                        eta_minutes = int(eta_seconds // 60)
                        eta_seconds_remain = int(eta_seconds % 60)
                        eta_str = f"{eta_minutes}m {eta_seconds_remain}s"
                    else:
                        eta_str = f"{int(eta_seconds)}s"
                else:
                    eta_str = "calculating..."

                speed = processed / elapsed if elapsed > 0 else 0

                progress_line = (
                    f"\r[{bar}] {percent:>6.1%} | "
                    f"{processed:>6}/{total_games:<6} | "
                    f"✅{total_found:>5} ❌{total_not_found:>5} | "
                    f"⏱️{elapsed:>5.0f}s | "
                    f"🚀{speed:>5.1f}g/s | "
                    f"⌛ETA:{eta_str:>10}"
                )
                self.stdout.write(progress_line, ending='')
                self.stdout.flush()

                # Пауза между батчами
                if i + batch_size < total_games and delay > 0 and not self.interrupted:
                    time.sleep(delay)

        except KeyboardInterrupt:
            self.interrupted = True
            self.stdout.write(self.style.ERROR('\n\n⚠️ Получено прерывание (Ctrl+C)'))

        finally:
            # Переходим на новую строку после прогресс-бара
            self.stdout.write('')

            # Закрываем файловые дескрипторы
            found_file_handle.close()
            not_found_file_handle.close()

            # Сохраняем финальную версию кэша
            self.save_cache_data(cache_file, self.cache_data)

            # Если было прерывание, сохраняем прогресс
            if self.interrupted:
                with open(progress_file, 'w', encoding='utf-8') as f:
                    f.write(f"Last processed offset: {processed}\n")
                    f.write(f"Total games: {total_games}\n")
                    f.write(f"Progress: {processed}/{total_games} ({processed / total_games * 100:.1f}%)\n")
                    f.write(f"Last checked: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Interrupted: True\n")
                self.stdout.write(self.style.WARNING(f'\n💾 Прогресс сохранен: {processed}/{total_games} игр'))
                self.stdout.write(self.style.WARNING(f'💡 Для продолжения запустите без --force'))

            # Если не было прерывания, удаляем файл прогресса
            if not self.interrupted and progress_file.exists():
                progress_file.unlink(missing_ok=True)

        # Финальная статистика
        elapsed = (datetime.now() - start_time).total_seconds()

        self.stdout.write(self.style.SUCCESS('\n' + '=' * 60))
        self.stdout.write(self.style.SUCCESS('📊 ИТОГОВАЯ СТАТИСТИКА'))
        self.stdout.write('=' * 60)
        self.stdout.write(f'  ✅ Найдено в Steam: {total_found}')
        self.stdout.write(f'  ❌ Не найдено в Steam: {total_not_found}')
        self.stdout.write(f'  📊 Всего проверено: {processed}')
        self.stdout.write(f'  📊 Всего в кэше: {len(self.cache_data)}')
        self.stdout.write(f'  ⏱️ Время выполнения: {elapsed:.1f}с')
        self.stdout.write(f'  📁 Файл найденных: {found_file}')
        self.stdout.write(f'  📁 Файл не найденных: {not_found_file}')
        self.stdout.write(f'  📁 Файл кэша: {cache_file}')

        if self.interrupted:
            self.stdout.write(self.style.WARNING(f'  ⚠️ Команда прервана пользователем'))
            self.stdout.write(self.style.WARNING(f'  📁 Файл прогресса: {progress_file}'))
            self.stdout.write(self.style.WARNING(f'  💡 Для продолжения запустите без --force'))

        if check_not_found:
            self.stdout.write(
                self.style.WARNING(f'  💡 Резервная копия предыдущего not_found: {not_found_file}.backup.txt'))

        self.stdout.write('=' * 60)

        # Выходим с соответствующим кодом
        if self.interrupted:
            sys.exit(1)
