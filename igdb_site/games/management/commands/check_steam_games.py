"""
Django management command для массовой проверки наличия игр в Steam.
Делает один запрос для проверки множества игр.
"""

import requests
import time
import logging
import re
import sys
import os
import json
from typing import Optional, Dict, Any, List, Tuple, Set
from datetime import datetime
from pathlib import Path
from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
from django.db.models import Q
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
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

        parser.add_argument(
            '--game-ids-file',
            type=str,
            default=None,
            help='Путь к файлу со списком ID игр для проверки (по одному ID на строку)'
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

    def search_steam(self, game_name: str, timeout: float = 10.0) -> Tuple[Optional[int], Optional[str]]:
        """
        Поиск игры в Steam по названию.

        Args:
            game_name: Название игры для поиска
            timeout: Таймаут запроса в секундах

        Returns:
            Tuple[Optional[int], Optional[str]]: (app_id, error_message)
            - app_id: ID игры в Steam или None если не найдено
            - error_message: Сообщение об ошибке или None при успехе
        """
        if not game_name:
            return None, 'empty_game_name'

        # Очищаем название для поиска
        search_name = re.sub(r'[^\w\s-]', '', game_name)
        search_name = re.sub(r'\s+', ' ', search_name).strip()

        if not search_name:
            return None, 'invalid_name'

        url = "https://store.steampowered.com/api/storesearch"
        params = {
            'term': search_name[:50],
            'l': 'english',
            'cc': 'us'
        }

        try:
            response = self.session.get(url, params=params, timeout=timeout)

            if response.status_code == 200:
                data = response.json()
                if data.get('total', 0) > 0:
                    items = data.get('items', [])
                    if items:
                        app_id = items[0].get('id')
                        if app_id:
                            return app_id, None
                return None, 'not_found'
            elif response.status_code == 429:
                return None, 'rate_limit'
            elif response.status_code == 403:
                return None, 'forbidden'
            else:
                return None, f'http_{response.status_code}'

        except requests.exceptions.Timeout:
            return None, 'timeout'
        except requests.exceptions.ConnectionError:
            return None, 'connection_error'
        except Exception as e:
            return None, f'exception_{str(e)[:50]}'

    def _get_time_string(self, seconds: float) -> str:
        """
        Форматирует время в читаемый вид.

        Аргументы:
            seconds: количество секунд

        Возвращает:
            str: отформатированная строка времени
        """
        if seconds < 60:
            return f"{seconds:.0f}с"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes}м {secs:02d}с"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            secs = int(seconds % 60)
            return f"{hours}ч {minutes}м {secs:02d}с"

    def _save_found_games(self, games):
        """Сохраняет список найденных игр в файл."""
        output_file = 'steam_found.txt'
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for game in games:
                    f.write(f"Game ID: {game['id']} - {game['name']} (Steam App ID: {game['app_id']})\n")
            self.stdout.write(self.style.SUCCESS(f'💾 Список найденных игр сохранен в: {output_file}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка сохранения: {e}'))

    def _save_not_found_games(self, games):
        """Сохраняет список ненайденных игр в файл."""
        output_file = 'steam_not_found.txt'
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for game in games:
                    f.write(f"Game ID: {game['id']} - {game['name']} - не найдено в Steam\n")
            self.stdout.write(self.style.SUCCESS(f'💾 Список ненайденных игр сохранен в: {output_file}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка сохранения: {e}'))

    def _save_errors(self, errors):
        """Сохраняет список ошибок в файл."""
        output_file = 'steam_errors.txt'
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for error in errors:
                    if isinstance(error, dict):
                        f.write(f"Game ID: {error['id']} - Error: {error['error']}\n")
                    else:
                        f.write(f"Game ID: {error}\n")
            self.stdout.write(self.style.SUCCESS(f'💾 Список ошибок сохранен в: {output_file}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка сохранения: {e}'))

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

    def display_progress_bar(self, current: int, total: int, start_time: datetime, message: str = ""):
        """
        Отображает прогресс-бар в консоли.

        Аргументы:
            current: текущее количество обработанных элементов
            total: общее количество элементов
            start_time: время начала выполнения
            message: дополнительное сообщение
        """
        if total <= 0:
            return

        progress = (current / total) * 100
        elapsed = (datetime.now() - start_time).total_seconds()

        if current > 0 and elapsed > 0:
            eta_seconds = (elapsed / current) * (total - current)
            if eta_seconds < 60:
                eta_str = f"{eta_seconds:.0f}с"
            elif eta_seconds < 3600:
                eta_str = f"{eta_seconds / 60:.1f}мин"
            else:
                eta_str = f"{eta_seconds / 3600:.1f}ч"
        else:
            eta_str = "расчет..."

        bar_length = 30
        filled = int(bar_length * current / total)
        bar = '█' * filled + '░' * (bar_length - filled)

        progress_line = (
            f"\r📊 Прогресс: [{bar}] {progress:.1f}% | "
            f"{current}/{total} | "
            f"⏱️ {elapsed:.0f}с | "
            f"⏳ {eta_str}"
        )

        if message:
            progress_line += f" | {message}"

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
        """Основной метод выполнения команды."""
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('🔍 STEAM GAMES CHECKER'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        game_ids_file = options.get('game_ids_file')
        self.dry_run = options.get('dry_run', False)
        self.verbose = options.get('verbose', False)

        if game_ids_file:
            game_ids = self._load_game_ids_from_file(game_ids_file)
            if not game_ids:
                self.stdout.write(self.style.WARNING('⚠️ Нет ID игр для проверки'))
                return
            self.stdout.write(
                self.style.SUCCESS(f'📊 Будет проверено {len(game_ids)} игр в Steam')
            )
        else:
            self.stdout.write(self.style.ERROR('❌ Не указан файл с ID игр (--game-ids-file)'))
            return

        self.stdout.write('')
        self.stdout.write(self.style.WARNING('📋 Проверка наличия игр в Steam...'))
        self.stdout.write('')

        found_games = []
        not_found_games = []
        errors = []

        total = len(game_ids)
        processed = 0

        self.stdout.write(f'📊 Всего игр для проверки: {total}')

        for game_id in game_ids:
            processed += 1
            progress = (processed / total) * 100
            bar_length = 30
            filled = int(bar_length * processed / total)
            bar = '█' * filled + '░' * (bar_length - filled)

            self.stdout.write(
                f'\r   📊 Прогресс: [{bar}] {progress:.1f}% | '
                f'{processed}/{total} игр',
                ending=''
            )
            self.stdout.flush()

            try:
                game = Game.objects.filter(id=game_id).first()
                if not game:
                    self.stdout.write(f'\n   ⚠️ Игра с ID {game_id} не найдена в БД')
                    errors.append(game_id)
                    continue

                if self.dry_run:
                    found_games.append(game_id)
                    self.stdout.write(f'\n   🔍 [DRY-RUN] Проверка игры: {game.name}')
                    continue

                app_id, error = self.search_steam(game.name, 10.0)

                if app_id:
                    found_games.append({
                        'id': game.id,
                        'name': game.name,
                        'app_id': app_id
                    })
                    if self.verbose:
                        self.stdout.write(f'\n   ✅ {game.name}: найден (App ID: {app_id})')
                else:
                    not_found_games.append({
                        'id': game.id,
                        'name': game.name,
                        'error': error
                    })
                    if self.verbose:
                        self.stdout.write(f'\n   ❌ {game.name}: не найден в Steam')

            except Exception as e:
                errors.append({'id': game_id, 'error': str(e)})
                if self.verbose:
                    self.stdout.write(f'\n   💥 Ошибка при проверке игры {game_id}: {e}')

        self.stdout.write('\n')
        self.stdout.write('=' * 60)
        self.stdout.write(self.style.SUCCESS('📊 ИТОГОВАЯ СТАТИСТИКА'))
        self.stdout.write('=' * 60)
        self.stdout.write(f'✅ Найдено в Steam: {len(found_games)}')
        self.stdout.write(f'❌ Не найдено в Steam: {len(not_found_games)}')
        self.stdout.write(f'💥 Ошибок: {len(errors)}')

        if found_games:
            self.stdout.write('\n📌 НАЙДЕННЫЕ ИГРЫ:')
            for game in found_games[:10]:
                self.stdout.write(f'   • {game["name"]} (App ID: {game["app_id"]})')
            if len(found_games) > 10:
                self.stdout.write(f'   ... и еще {len(found_games) - 10} игр')

            if not self.dry_run:
                self._save_found_games(found_games)

        if not_found_games:
            self.stdout.write('\n📌 НЕ НАЙДЕННЫЕ ИГРЫ:')
            for game in not_found_games[:10]:
                self.stdout.write(f'   • {game["name"]}')
            if len(not_found_games) > 10:
                self.stdout.write(f'   ... и еще {len(not_found_games) - 10} игр')

            if not self.dry_run:
                self._save_not_found_games(not_found_games)

        if errors:
            self.stdout.write('\n💥 ОШИБКИ:')
            for error in errors[:5]:
                if isinstance(error, dict):
                    self.stdout.write(f'   • ID {error["id"]}: {error["error"]}')
                else:
                    self.stdout.write(f'   • ID {error}')
            if len(errors) > 5:
                self.stdout.write(f'   ... и еще {len(errors) - 5} ошибок')

            if not self.dry_run:
                self._save_errors(errors)

        self.stdout.write('\n' + '=' * 60)
        if self.dry_run:
            self.stdout.write(self.style.WARNING('🔧 РЕЖИМ ПРОСМОТРА: изменения не сохранены'))
        else:
            self.stdout.write(self.style.SUCCESS('✅ Проверка завершена'))
