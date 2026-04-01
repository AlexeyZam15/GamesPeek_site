"""
Команда для добавления жанров и тем к играм из JSON-файлов.
Максимально оптимизированная версия с массовыми операциями и прогресс-барами.
"""

import json
import hashlib
import os
import signal
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict

from django.core.management.base import BaseCommand, CommandError
from django.db import connection, transaction
from django.db.models import Q

from games.models import Game, Genre, Theme


class Command(BaseCommand):
    help = 'Добавляет жанры и темы к играм из JSON-файлов'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Режим просмотра без сохранения')
        parser.add_argument('--file', type=str, help='Конкретный файл для обработки')
        parser.add_argument('--force-restart', action='store_true', help='Сбросить состояние')
        parser.add_argument('--batch-size', type=int, default=5000, help='Размер пакета (по умолчанию 5000)')
        parser.add_argument('--verbose', action='store_true', help='Подробный вывод')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dry_run = False
        self.force_restart = False
        self.batch_size = 5000
        self.verbose = False
        self.base_dir = None
        self.data_dir = None
        self.log_dir = None
        self.state_file_path = None
        self.additions_global_path = None
        self.additions_current_path = None
        self.errors_log_path = None
        self.processed_state = {}
        self.interrupted = False
        self.start_time = None

        # Кэши для быстрого доступа
        self.genre_map = {}  # name -> id
        self.theme_map = {}  # name -> id
        self.game_map = {}  # igdb_id -> game_id
        self.genre_name_to_id = {}  # прямой маппинг без учета регистра
        self.theme_name_to_id = {}

        self.stats = {
            'total_games': 0,
            'updated_games': 0,
            'skipped_games': 0,
            'error_games': 0,
            'total_genres_added': 0,
            'total_themes_added': 0,
            'processed_files': [],
            'skipped_files': []
        }

        # Множества для быстрой проверки принадлежности
        self.genres_set = {
            'Action', 'Adventure', 'Arcade', 'Base Building', 'Card & Board Game',
            'Fighting', 'MOBA', 'Music', 'Open World', 'Pinball', 'Platform',
            'Point-and-click', 'Precision Combat', 'Puzzle', 'Quiz/Trivia', 'Racing',
            'Real Time Strategy (RTS)', 'Role-playing (RPG)', 'Sandbox', 'Shooter',
            'Simulator', 'Sport', 'Squad Management', 'Strategy', 'Survival',
            'Tactical', 'Turn-based', 'Turn-based strategy (TBS)', 'Visual Novel'
        }

        self.themes_set = {
            '4X (explore, expand, exploit, and exterminate)', 'Business', 'Comedy',
            'Crafting & Gathering', 'Drama', 'Educational', 'Erotic', 'Fantasy',
            'Fire Emblem', 'Gothic', 'Historical', 'Horror', 'Indie', 'Kids',
            'Medieval', 'Mystery', 'Non-fiction', 'Party', 'Post-apocalyptic',
            'Romance', 'Science fiction', 'Stealth', 'Thriller', 'Warfare'
        }

    def _print_file_header(self, file_path: Path, file_num: int, total_files: int):
        """Вывод заголовка перед обработкой файла"""
        if self.verbose:
            return

        file_size = file_path.stat().st_size
        size_mb = file_size / (1024 * 1024)

        self.stdout.write("\n" + "─" * 70)
        self.stdout.write(f"📄 Файл {file_num}/{total_files}: {file_path.name}")
        self.stdout.write(f"   📦 Размер: {size_mb:.2f} МБ")
        self.stdout.write("─" * 70)

    def _setup_paths(self):
        """Настройка путей к файлам"""
        self.base_dir = Path(os.getcwd())
        self.data_dir = self.base_dir / 'add_genres_themes'
        self.log_dir = self.data_dir / 'logs'
        self.state_file_path = self.log_dir / 'processed_files_state.json'
        self.additions_global_path = self.log_dir / 'additions_global.log'
        self.additions_current_path = self.log_dir / 'additions_current.log'
        self.errors_log_path = self.log_dir / 'errors.log'
        self.errors_summary_path = self.log_dir / 'errors_summary.log'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def _clear_logs(self):
        """Очистка временных логов перед началом работы"""
        # Очистка текущего лога добавлений
        with open(self.additions_current_path, 'w', encoding='utf-8') as f:
            f.write("")

        # Очистка подробного лога ошибок
        with open(self.errors_log_path, 'w', encoding='utf-8') as f:
            f.write("")

        # Очистка сводного лога ошибок
        with open(self.errors_summary_path, 'w', encoding='utf-8') as f:
            f.write("")

    def _get_file_hash(self, file_path: Path) -> str:
        """Быстрое вычисление хэша файла"""
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _is_file_processed(self, file_path: Path) -> Tuple[bool, str]:
        """Проверка, обработан ли файл"""
        file_name = file_path.name
        if file_name not in self.processed_state.get('files', {}):
            return False, ""

        file_state = self.processed_state['files'][file_name]
        current_hash = self._get_file_hash(file_path)

        if file_state.get('file_hash') != current_hash:
            return False, "файл изменен"

        return True, f"обработан {file_state.get('processed_at')}"

    def _mark_file_processed(self, file_path: Path, games_updated: int):
        """Отметка файла как обработанного"""
        file_name = file_path.name
        self.processed_state['files'][file_name] = {
            'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'file_hash': self._get_file_hash(file_path),
            'games_updated': games_updated
        }
        with open(self.state_file_path, 'w', encoding='utf-8') as f:
            json.dump(self.processed_state, f, ensure_ascii=False, indent=2)

    def _load_caches(self):
        """Максимально быстрая загрузка кэшей"""
        if self.verbose:
            self.stdout.write("🔧 Загрузка кэшей...")

        # Загрузка жанров с прямым маппингом
        for genre in Genre.objects.all():
            self.genre_map[genre.name] = genre.id
            self.genre_name_to_id[genre.name.lower()] = genre.id

        # Загрузка тем с прямым маппингом
        for theme in Theme.objects.all():
            self.theme_map[theme.name] = theme.id
            self.theme_name_to_id[theme.name.lower()] = theme.id

        # Загрузка игр
        for game in Game.objects.all():
            self.game_map[game.igdb_id] = game.id

        if self.verbose:
            self.stdout.write(
                f"  ✅ Жанров: {len(self.genre_name_to_id)}, "
                f"Тем: {len(self.theme_name_to_id)}, "
                f"Игр: {len(self.game_map)}"
            )

    def _get_genre_id(self, name: str) -> Optional[int]:
        """Максимально быстрый поиск ID жанра"""
        if not name:
            return None

        import re
        # Очистка от всех пробельных символов и невидимых символов
        name_clean = re.sub(r'\s+', ' ', name.strip())

        # Прямой поиск по имени
        if name_clean in self.genre_map:
            return self.genre_map[name_clean]

        # Поиск в нижнем регистре
        name_lower = name_clean.lower()
        genre_id = self.genre_name_to_id.get(name_lower)
        if genre_id:
            return genre_id

        # Если не нашли, выводим отладочную информацию в verbose режиме
        if self.verbose:
            self.stdout.write(f"  ⚠️ Жанр не найден: '{name}' (очищенный: '{name_clean}')")

        return None

    def _get_theme_id(self, name: str) -> Optional[int]:
        """Максимально быстрый поиск ID темы"""
        if not name:
            return None

        import re
        # Очистка от всех пробельных символов и невидимых символов
        name_clean = re.sub(r'\s+', ' ', name.strip())

        # Прямой поиск по имени
        if name_clean in self.theme_map:
            return self.theme_map[name_clean]

        # Поиск в нижнем регистре
        name_lower = name_clean.lower()
        theme_id = self.theme_name_to_id.get(name_lower)
        if theme_id:
            return theme_id

        # Если не нашли, выводим отладочную информацию в verbose режиме
        if self.verbose:
            self.stdout.write(f"  ⚠️ Тема не найдена: '{name}' (очищенный: '{name_clean}')")

        return None

    def _print_progress_bar(self, current, total, prefix='', suffix='', length=40):
        """Вывод прогресс-бара"""
        if self.verbose:
            return

        percent = (current / total) * 100
        filled_length = int(length * current // total)
        bar = '█' * filled_length + '░' * (length - filled_length)

        elapsed = time.time() - self.start_time if self.start_time else 0
        if current > 0 and elapsed > 0:
            speed = current / elapsed
            eta = (total - current) / speed if speed > 0 else 0
            eta_str = f" | ETA: {eta:.0f}с" if eta < 3600 else f" | ETA: {eta / 60:.0f}м"
        else:
            eta_str = ""

        self.stdout.write(
            f'\r{prefix} |{bar}| {percent:.1f}% {suffix}{eta_str}',
            ending=''
        )

        if current == total:
            self.stdout.write()

    def _get_conversion_maps(self):
        """Возвращает словари для конвертации жанров и тем"""
        genre_conversion = {
            'Tower Defense': 'Strategy',
            'Hack and Slash': 'Action',
            'Beat \'em up': 'Action',
            'Metroidvania': 'Action',
            'Grand Strategy': 'Strategy',
            'Life Simulation': 'Simulator',
            'Rhythm': 'Music',
            'Board Game': 'Card & Board Game',
            'Survival Horror': 'Horror',
            'City Builder': 'Base Building',
            'RPG': 'Role-playing (RPG)',
            'Roguelike': 'Roguelike / Roguelite',
            'First-person Shooter': 'Shooter',
            'Third-person Shooter': 'Shooter',
            'Top-down Shooter': 'Shooter',
            'Deck-building': 'Card & Board Game'
        }

        theme_conversion = {
            '4X': '4X (explore, expand, exploit, and exterminate)',
            'Crafting': 'Crafting & Gathering',
            'Space': 'Science fiction',
            'Steampunk': 'Science fiction',
            'Post-apocalyptic': 'Post-apocalyptic',
            'Vampire': 'Fantasy',
            'Magic': 'Fantasy',
            'Dragons': 'Fantasy',
            'Demons': 'Fantasy'
        }

        genre_to_theme = {
            '4X': '4X (explore, expand, exploit, and exterminate)',
            'Crafting': 'Crafting & Gathering'
        }

        theme_to_genre = {
            'Stealth': 'Stealth'
        }

        return genre_conversion, theme_conversion, genre_to_theme, theme_to_genre

    def _load_and_filter_games(self, file_path: Path, data_array: List) -> Tuple[List, int, int]:
        """Загружает и фильтрует игры, присутствующие в БД"""
        total = len(data_array)
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        if not self.verbose:
            self.stdout.write(f" ✅ ({total:,} игр, {file_size_mb:.1f} МБ)")
            self.stdout.write(f"  🔍 Фильтрация игр...")

        valid_games = []
        missing_games = 0

        for data in data_array:
            igdb_id = data.get('id')
            if igdb_id and igdb_id in self.game_map:
                valid_games.append(data)
            else:
                missing_games += 1

        if not self.verbose:
            self.stdout.write(f" ✅ ({len(valid_games):,} в БД, {missing_games:,} пропущено)")

        return valid_games, total, missing_games

    def _load_current_relations(self, game_db_ids: List[int]) -> Tuple[defaultdict, defaultdict]:
        """Загружает текущие связи жанров и тем из БД"""
        if not self.verbose:
            self.stdout.write(f"  🔗 Загрузка текущих связей...")

        current_genres = defaultdict(set)
        current_themes = defaultdict(set)

        with connection.cursor() as cursor:
            cursor.execute("""
                           SELECT game_id, genre_id
                           FROM games_game_genres
                           WHERE game_id IN %s
                           """, [tuple(game_db_ids)])
            for gid, genre_id in cursor.fetchall():
                current_genres[gid].add(genre_id)

            cursor.execute("""
                           SELECT game_id, theme_id
                           FROM games_game_themes
                           WHERE game_id IN %s
                           """, [tuple(game_db_ids)])
            for gid, theme_id in cursor.fetchall():
                current_themes[gid].add(theme_id)

        if not self.verbose:
            total_relations = sum(len(g) for g in current_genres.values()) + sum(
                len(t) for t in current_themes.values())
            self.stdout.write(f" ✅ ({total_relations:,} существующих связей)")

        return current_genres, current_themes

    def _convert_name(self, name: str, genre_conversion: Dict, theme_conversion: Dict,
                      genre_to_theme: Dict, theme_to_genre: Dict) -> Tuple[str, str, bool]:
        """
        Конвертирует название жанра/темы согласно правилам
        Возвращает: (конвертированное_имя, тип, флаг_конвертации)
        Тип: 'genre', 'theme', или None если конвертация не требуется
        """
        # Очищаем от лишних пробелов
        name_clean = name.strip()

        # Проверяем точное совпадение
        if name_clean in genre_conversion:
            return genre_conversion[name_clean], 'genre', True
        elif name_clean in theme_conversion:
            return theme_conversion[name_clean], 'theme', True
        elif name_clean in genre_to_theme:
            return genre_to_theme[name_clean], 'theme', True
        elif name_clean in theme_to_genre:
            return theme_to_genre[name_clean], 'genre', True

        # Проверяем без учета регистра
        name_lower = name_clean.lower()
        for key, value in genre_conversion.items():
            if key.lower() == name_lower:
                return value, 'genre', True
        for key, value in theme_conversion.items():
            if key.lower() == name_lower:
                return value, 'theme', True
        for key, value in genre_to_theme.items():
            if key.lower() == name_lower:
                return value, 'theme', True
        for key, value in theme_to_genre.items():
            if key.lower() == name_lower:
                return value, 'genre', True

        return name_clean, None, False

    def _process_game_genres_themes(self, data: Dict, game_db_id: int, game_name: str,
                                    current_genre_ids: Set[int], current_theme_ids: Set[int],
                                    genres_set_lower: Set[str], themes_set_lower: Set[str],
                                    genre_conversion: Dict, theme_conversion: Dict,
                                    genre_to_theme: Dict, theme_to_genre: Dict) -> Tuple[
        Set[int], Set[int], Set[str], Set[str], List[str], List[str]]:
        """
        Обрабатывает жанры и темы для одной игры
        Возвращает: (new_genre_ids, new_theme_ids, new_genre_names, new_theme_names, missing_genres, missing_themes)
        """
        new_genre_ids = set()
        new_theme_ids = set()
        new_genre_names = set()
        new_theme_names = set()
        missing_genres = []
        missing_themes = []

        # Обработка жанров из поля genres
        for name in data.get('genres', []):
            name_lower = name.lower()
            converted_name, conv_type, converted = self._convert_name(
                name, genre_conversion, theme_conversion, genre_to_theme, theme_to_genre
            )

            if converted:
                name = converted_name
                name_lower = name.lower()
                if conv_type == 'genre':
                    gid = self._get_genre_id(name)
                    if gid:
                        new_genre_ids.add(gid)
                        new_genre_names.add(name)
                        continue
                    else:
                        missing_genres.append(name)
                        continue
                elif conv_type == 'theme':
                    tid = self._get_theme_id(name)
                    if tid:
                        new_theme_ids.add(tid)
                        new_theme_names.add(name)
                        continue
                    else:
                        missing_themes.append(name)
                        continue

            # Проверка принадлежности к жанрам или темам
            if name in self.genres_set or name_lower in genres_set_lower:
                gid = self._get_genre_id(name)
                if gid:
                    new_genre_ids.add(gid)
                    new_genre_names.add(name)
                else:
                    missing_genres.append(name)
            elif name in self.themes_set or name_lower in themes_set_lower:
                tid = self._get_theme_id(name)
                if tid:
                    new_theme_ids.add(tid)
                    new_theme_names.add(name)
                else:
                    missing_themes.append(name)
            else:
                gid = self._get_genre_id(name)
                if gid:
                    new_genre_ids.add(gid)
                    new_genre_names.add(name)
                else:
                    tid = self._get_theme_id(name)
                    if tid:
                        new_theme_ids.add(tid)
                        new_theme_names.add(name)
                    else:
                        missing_genres.append(name)

        # Обработка тем из поля themes
        for name in data.get('themes', []):
            name_lower = name.lower()
            converted_name, conv_type, converted = self._convert_name(
                name, genre_conversion, theme_conversion, genre_to_theme, theme_to_genre
            )

            if converted:
                name = converted_name
                name_lower = name.lower()
                if conv_type == 'theme':
                    tid = self._get_theme_id(name)
                    if tid:
                        new_theme_ids.add(tid)
                        new_theme_names.add(name)
                        continue
                    else:
                        missing_themes.append(name)
                        continue
                elif conv_type == 'genre':
                    gid = self._get_genre_id(name)
                    if gid:
                        new_genre_ids.add(gid)
                        new_genre_names.add(name)
                        continue
                    else:
                        missing_genres.append(name)
                        continue

            # Проверка принадлежности к темам или жанрам
            if name in self.themes_set or name_lower in themes_set_lower:
                tid = self._get_theme_id(name)
                if tid:
                    new_theme_ids.add(tid)
                    new_theme_names.add(name)
                else:
                    missing_themes.append(name)
            elif name in self.genres_set or name_lower in genres_set_lower:
                gid = self._get_genre_id(name)
                if gid:
                    new_genre_ids.add(gid)
                    new_genre_names.add(name)
                else:
                    missing_genres.append(name)
            else:
                tid = self._get_theme_id(name)
                if tid:
                    new_theme_ids.add(tid)
                    new_theme_names.add(name)
                else:
                    missing_themes.append(name)

        return new_genre_ids, new_theme_ids, new_genre_names, new_theme_names, missing_genres, missing_themes

    def _log_errors_and_additions(self, errors_log: List, additions_log: List, error_counter: defaultdict,
                                  file_path: Path):
        """Записывает логи ошибок и добавлений"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Запись подробных ошибок
        if errors_log:
            with open(self.errors_log_path, 'a', encoding='utf-8') as f:
                for igdb_id, game_name, error in errors_log:
                    f.write(f"{timestamp} | Game {igdb_id} | {game_name} | {error}\n")

        # Запись краткой статистики ошибок
        if error_counter:
            with open(self.errors_summary_path, 'a', encoding='utf-8') as f:
                f.write(f"\n{'=' * 60}\n")
                f.write(f"Файл: {file_path.name}\n")
                f.write(f"Время: {timestamp}\n")
                f.write(f"{'-' * 60}\n")
                for error_msg, count in sorted(error_counter.items(), key=lambda x: x[1], reverse=True):
                    f.write(f"{count:>6} x  {error_msg}\n")
                f.write(f"{'=' * 60}\n")

        # Запись добавлений
        if additions_log:
            prefix = "[DRY-RUN] " if self.dry_run else ""
            with open(self.additions_current_path, 'a', encoding='utf-8') as f:
                for igdb_id, game_name, genres, themes in additions_log:
                    if genres:
                        f.write(f"{timestamp} | {prefix}Game {igdb_id} | {game_name} | +{', '.join(genres)}\n")
                    if themes:
                        f.write(f"{timestamp} | {prefix}Game {igdb_id} | {game_name} | +{', '.join(themes)}\n")

            if not self.dry_run:
                with open(self.additions_global_path, 'a', encoding='utf-8') as f:
                    for igdb_id, game_name, genres, themes in additions_log:
                        if genres:
                            f.write(f"{timestamp} | Game {igdb_id} | {game_name} | +{', '.join(genres)}\n")
                        if themes:
                            f.write(f"{timestamp} | Game {igdb_id} | {game_name} | +{', '.join(themes)}\n")

    def _save_to_database(self, all_genre_updates: List, all_theme_updates: List):
        """Сохраняет изменения в БД массовыми операциями"""
        if not self.dry_run and (all_genre_updates or all_theme_updates):
            if not self.verbose:
                self.stdout.write(f"  💾 Сохранение в БД...")

            with connection.cursor() as cursor:
                if all_genre_updates:
                    unique_genres = list(set(all_genre_updates))
                    for i in range(0, len(unique_genres), self.batch_size):
                        batch = unique_genres[i:i + self.batch_size]
                        values = ','.join([f"({gid}, {tid})" for gid, tid in batch])
                        cursor.execute(f"""
                            INSERT INTO games_game_genres (game_id, genre_id) 
                            VALUES {values} 
                            ON CONFLICT (game_id, genre_id) DO NOTHING
                        """)

                if all_theme_updates:
                    unique_themes = list(set(all_theme_updates))
                    for i in range(0, len(unique_themes), self.batch_size):
                        batch = unique_themes[i:i + self.batch_size]
                        values = ','.join([f"({gid}, {tid})" for gid, tid in batch])
                        cursor.execute(f"""
                            INSERT INTO games_game_themes (game_id, theme_id) 
                            VALUES {values} 
                            ON CONFLICT (game_id, theme_id) DO NOTHING
                        """)

            if not self.verbose:
                self.stdout.write(f" ✅")

    def _update_file_stats(self, file_updated: int, file_skipped: int, file_errors: int,
                           file_genres: int, file_themes: int, total: int, file_time: float):
        """Обновляет статистику и выводит итог по файлу"""
        self.stats['total_games'] += total
        self.stats['updated_games'] += file_updated
        self.stats['skipped_games'] += file_skipped
        self.stats['error_games'] += file_errors
        self.stats['total_genres_added'] += file_genres
        self.stats['total_themes_added'] += file_themes

        if not self.verbose:
            self.stdout.write(f"  📈 {file_time:.1f}с | {total / file_time:.0f} игр/с | "
                              f"✅ +{file_updated} | ⏭️ {file_skipped} | ❌ {file_errors}")
        else:
            self.stdout.write(f"\n  ✅ Обновлено: {file_updated} | Пропущено: {file_skipped} | Ошибок: {file_errors}")
            self.stdout.write(f"  📈 Время: {file_time:.1f}с | Скорость: {total / file_time:.0f} игр/с")

    def _process_file(self, file_path: Path) -> Dict:
        """Максимально оптимизированная обработка файла с прогресс-барами"""
        file_start = time.time()

        if not self.verbose:
            self.stdout.write(f"\n  📖 Чтение файла...")

        # Загрузка и парсинг файла
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                data_array = json.loads(content)

            if not isinstance(data_array, list):
                self.stderr.write(self.style.ERROR(f"\n  ❌ Файл не содержит JSON-массив"))
                return {'updated_games': 0, 'total_lines': 0}

        except json.JSONDecodeError as e:
            self.stderr.write(self.style.ERROR(f"\n  ❌ Ошибка парсинга JSON: {e}"))
            return {'updated_games': 0, 'total_lines': 0}
        except MemoryError:
            self.stderr.write(self.style.ERROR(f"\n  ❌ Недостаточно памяти для загрузки файла"))
            return {'updated_games': 0, 'total_lines': 0}

        # Фильтрация игр
        valid_games, total, missing_games = self._load_and_filter_games(file_path, data_array)

        if not valid_games:
            self.stdout.write(f"  ⚠️  Нет игр для обработки")
            return {'updated_games': 0, 'total_lines': total}

        # Загрузка текущих связей
        game_db_ids = [self.game_map[game['id']] for game in valid_games]
        current_genres, current_themes = self._load_current_relations(game_db_ids)

        # Подготовка множеств для быстрой проверки
        if not self.verbose:
            self.stdout.write(f"  ⚙️ Обработка игр...")

        genres_set_lower = {g.lower() for g in self.genres_set}
        themes_set_lower = {t.lower() for t in self.themes_set}

        # Получение словарей конвертации
        genre_conversion, theme_conversion, genre_to_theme, theme_to_genre = self._get_conversion_maps()

        # Для массовых вставок
        all_genre_updates = []
        all_theme_updates = []
        additions_log = []
        errors_log = []
        error_counter = defaultdict(int)

        file_updated = 0
        file_skipped = 0
        file_errors = 0
        file_genres = 0
        file_themes = 0

        # Пакетная обработка
        batches = [valid_games[i:i + self.batch_size] for i in range(0, len(valid_games), self.batch_size)]

        for batch_idx, batch in enumerate(batches):
            if self.interrupted:
                break

            batch_updates_genres = []
            batch_updates_themes = []
            batch_additions = []
            batch_errors = []

            for data in batch:
                igdb_id = data['id']
                game_db_id = self.game_map[igdb_id]
                game_name = data.get('name', 'Unknown')

                # Обработка жанров и тем игры
                new_genre_ids, new_theme_ids, new_genre_names, new_theme_names, missing_genres, missing_themes = \
                    self._process_game_genres_themes(
                        data, game_db_id, game_name,
                        current_genres.get(game_db_id, set()),
                        current_themes.get(game_db_id, set()),
                        genres_set_lower, themes_set_lower,
                        genre_conversion, theme_conversion, genre_to_theme, theme_to_genre
                    )

                # Логирование пропущенных жанров/тем
                for missing in missing_genres:
                    # Проверяем, было ли это конвертированное значение
                    if missing in theme_to_genre.values():
                        # Находим исходное название
                        original_name = None
                        for orig, conv in theme_to_genre.items():
                            if conv == missing:
                                original_name = orig
                                break
                        if original_name:
                            error_msg = f"Конвертация: тема '{original_name}' -> жанр '{missing}' не найден в БД"
                        else:
                            error_msg = f"Жанр '{missing}' не найден"
                    elif missing in genre_to_theme.values():
                        original_name = None
                        for orig, conv in genre_to_theme.items():
                            if conv == missing:
                                original_name = orig
                                break
                        if original_name:
                            error_msg = f"Конвертация: жанр '{original_name}' -> тема '{missing}' не найдена в БД"
                        else:
                            error_msg = f"Тема '{missing}' не найдена"
                    else:
                        error_msg = f"Жанр '{missing}' не найден"

                    batch_errors.append((igdb_id, game_name, error_msg))
                    error_counter[error_msg] += 1

                for missing in missing_themes:
                    # Проверяем, было ли это конвертированное значение
                    if missing in genre_to_theme.values():
                        original_name = None
                        for orig, conv in genre_to_theme.items():
                            if conv == missing:
                                original_name = orig
                                break
                        if original_name:
                            error_msg = f"Конвертация: жанр '{original_name}' -> тема '{missing}' не найдена в БД"
                        else:
                            error_msg = f"Тема '{missing}' не найдена"
                    elif missing in theme_to_genre.values():
                        original_name = None
                        for orig, conv in theme_to_genre.items():
                            if conv == missing:
                                original_name = orig
                                break
                        if original_name:
                            error_msg = f"Конвертация: тема '{original_name}' -> жанр '{missing}' не найден в БД"
                        else:
                            error_msg = f"Жанр '{missing}' не найден"
                    else:
                        error_msg = f"Тема '{missing}' не найдена"

                    batch_errors.append((igdb_id, game_name, error_msg))
                    error_counter[error_msg] += 1

                if missing_genres or missing_themes:
                    file_errors += 1

                # Определение новых элементов
                added_genres = new_genre_ids - current_genres.get(game_db_id, set())
                added_themes = new_theme_ids - current_themes.get(game_db_id, set())

                if added_genres or added_themes:
                    added_genre_names_list = [n for n in new_genre_names if self._get_genre_id(n) in added_genres]
                    added_theme_names_list = [n for n in new_theme_names if self._get_theme_id(n) in added_themes]

                    for gid in added_genres:
                        batch_updates_genres.append((game_db_id, gid))
                    for tid in added_themes:
                        batch_updates_themes.append((game_db_id, tid))

                    file_updated += 1
                    file_genres += len(added_genres)
                    file_themes += len(added_themes)

                    if added_genre_names_list or added_theme_names_list:
                        batch_additions.append((igdb_id, game_name, added_genre_names_list, added_theme_names_list))
                else:
                    file_skipped += 1

            # Накопление для массовой вставки
            all_genre_updates.extend(batch_updates_genres)
            all_theme_updates.extend(batch_updates_themes)
            additions_log.extend(batch_additions)
            errors_log.extend(batch_errors)

            # Прогресс-бар
            if not self.verbose:
                self._print_file_progress(batch_idx, batches, file_updated, file_skipped, file_errors, file_start,
                                          len(valid_games))

        if not self.verbose and not self.interrupted:
            self.stdout.write()

        # Сохранение в БД
        self._save_to_database(all_genre_updates, all_theme_updates)

        # Запись логов
        self._log_errors_and_additions(errors_log, additions_log, error_counter, file_path)

        # Обновление статистики
        file_time = time.time() - file_start
        self._update_file_stats(file_updated, file_skipped, file_errors, file_genres, file_themes, total, file_time)

        return {'updated_games': file_updated, 'total_lines': total}

    def _print_file_progress(self, batch_idx: int, batches: List, file_updated: int,
                             file_skipped: int, file_errors: int, file_start: float, total_games: int):
        """Выводит прогресс-бар для текущего файла"""
        processed_games = (batch_idx + 1) * self.batch_size
        if processed_games > total_games:
            processed_games = total_games

        elapsed_file = time.time() - file_start
        if elapsed_file > 0 and processed_games > 0:
            speed = processed_games / elapsed_file
            eta = (total_games - processed_games) / speed if speed > 0 else 0
            eta_str = f" | ETA: {eta:.0f}с" if eta < 3600 else f" | ETA: {eta / 60:.0f}м"
        else:
            eta_str = ""

        percent = (processed_games / total_games) * 100
        filled_length = int(40 * processed_games // total_games)
        bar = '█' * filled_length + '░' * (40 - filled_length)

        self.stdout.write(
            f"\r  📊 Прогресс |{bar}| {percent:.1f}% "
            f"| ✅ {file_updated} | ⏭️ {file_skipped} | ❌ {file_errors}{eta_str}"
        )

    def _print_summary(self):
        """Вывод итоговой статистики"""
        total_time = time.time() - self.start_time if self.start_time else 0
        self.stdout.write(self.style.SUCCESS("\n" + "=" * 60))
        self.stdout.write(self.style.SUCCESS("📊 ИТОГОВАЯ СТАТИСТИКА"))
        self.stdout.write(self.style.SUCCESS("=" * 60))
        self.stdout.write(f"⏱️  Общее время: {total_time:.1f} сек")
        self.stdout.write(f"📁 Обработано файлов: {len(self.stats['processed_files'])}")
        self.stdout.write(f"🎮 Обработано игр: {self.stats['total_games']:,}")
        self.stdout.write(f"✅ Обновлено игр: {self.stats['updated_games']:,}")
        self.stdout.write(f"⏭️  Пропущено: {self.stats['skipped_games']:,}")
        self.stdout.write(f"❌ Ошибок: {self.stats['error_games']:,}")
        self.stdout.write(f"🏷️  Добавлено жанров: {self.stats['total_genres_added']:,}")
        self.stdout.write(f"🏷️  Добавлено тем: {self.stats['total_themes_added']:,}")

        if self.stats['total_games'] > 0 and total_time > 0:
            self.stdout.write(f"⚡ Средняя скорость: {self.stats['total_games'] / total_time:.1f} игр/сек")

        if self.dry_run:
            self.stdout.write(self.style.WARNING("\n⚠️  [DRY-RUN] Изменения не сохранены в БД"))

    def handle(self, *args, **options):
        signal.signal(signal.SIGINT, self._signal_handler)
        self.start_time = time.time()

        self.dry_run = options['dry_run']
        self.force_restart = options['force_restart']
        self.batch_size = options['batch_size']
        self.verbose = options.get('verbose', False)
        specific_file = options.get('file')

        # Начало работы
        self.stdout.write(self.style.SUCCESS("\n" + "=" * 70))
        self.stdout.write(self.style.SUCCESS("🚀 ЗАПУСК КОМАНДЫ add_genres_themes"))
        self.stdout.write(self.style.SUCCESS("=" * 70))

        if self.dry_run:
            self.stdout.write(self.style.WARNING("⚠️  РЕЖИМ DRY-RUN: изменения не будут сохранены в БД"))

        self.stdout.write(f"📦 Размер пакета: {self.batch_size}")
        self.stdout.write(f"🔧 Режим: {'VERBOSE' if self.verbose else 'NORMAL'}")

        # Этап 1: Настройка путей
        self.stdout.write("\n📁 [1/6] Настройка путей...", ending='')
        self._setup_paths()
        self.stdout.write(" ✅")

        # Этап 2: Очистка логов
        self.stdout.write("🧹 [2/6] Очистка логов...", ending='')
        self._clear_logs()
        self.stdout.write(" ✅")

        # Этап 3: Загрузка состояния
        self.stdout.write("📊 [3/6] Загрузка состояния...", ending='')
        if self.state_file_path.exists():
            with open(self.state_file_path, 'r', encoding='utf-8') as f:
                self.processed_state = json.load(f)
            processed_count = len(self.processed_state.get('files', {}))
            self.stdout.write(f" ✅ (найдено {processed_count} обработанных файлов)")
        else:
            self.processed_state = {'files': {}}
            self.stdout.write(" ✅ (состояние не найдено, создано новое)")

        if self.force_restart:
            self.processed_state = {'files': {}}
            self.stdout.write(self.style.WARNING("  ⚠️  Принудительный перезапуск: состояние сброшено"))

        # Этап 4: Загрузка кэшей
        self.stdout.write("💾 [4/6] Загрузка кэшей из БД...")
        self._load_caches()

        # Этап 5: Поиск файлов
        self.stdout.write("🔍 [5/6] Поиск файлов для обработки...", ending='')

        if specific_file:
            files = [self.data_dir / specific_file]
            if not files[0].exists():
                self.stderr.write(self.style.ERROR(f"\n❌ Файл не найден: {specific_file}"))
                return
            self.stdout.write(f" ✅ (указан файл: {specific_file})")
        else:
            # Поиск всех JSON файлов
            files = sorted(self.data_dir.glob('*.json'))

            # Также ищем .txt файлы для обратной совместимости
            txt_files = sorted(self.data_dir.glob('*.txt'))
            files.extend([f for f in txt_files if f not in files])
            files = sorted(set(files))

            if files:
                self.stdout.write(f" ✅ (найдено {len(files)} файлов)")
                if not self.verbose:
                    for f in files[:5]:
                        self.stdout.write(f"     📄 {f.name}")
                    if len(files) > 5:
                        self.stdout.write(f"     ... и еще {len(files) - 5} файлов")
            else:
                self.stdout.write(self.style.WARNING(f" ⚠️  (файлов не найдено)"))
                self.stdout.write(f"\n📁 Проверьте папку: {self.data_dir}")
                self.stdout.write(f"   Поддерживаемые форматы: .json, .txt")
                return

        # Этап 6: Обработка файлов
        self.stdout.write("\n⚙️  [6/6] Обработка файлов...")
        self.stdout.write("=" * 70)

        for idx, file_path in enumerate(files, 1):
            if self.interrupted:
                break

            # Проверка состояния файла
            is_processed, reason = self._is_file_processed(file_path)
            if is_processed and not self.force_restart:
                self.stdout.write(self.style.WARNING(f"\n⏭️  [{idx}/{len(files)}] Пропущен {file_path.name}: {reason}"))
                self.stats['skipped_files'].append(file_path.name)
                continue

            # Обработка файла
            self.stdout.write(f"\n📄 [{idx}/{len(files)}] Обработка: {file_path.name}")
            stats = self._process_file(file_path)
            self.stats['processed_files'].append({
                'name': file_path.name,
                'games_updated': stats['updated_games']
            })

            if not self.dry_run:
                self._mark_file_processed(file_path, stats['updated_games'])

            # Общий прогресс
            if not self.verbose and len(files) > 1:
                elapsed = time.time() - self.start_time
                progress_pct = (idx / len(files)) * 100
                self.stdout.write(
                    f"\n📊 Общий прогресс: {idx}/{len(files)} файлов ({progress_pct:.1f}%) | "
                    f"✅ {self.stats['updated_games']} игр обновлено | "
                    f"⏱️  {elapsed:.1f}с"
                )

        # Итоговая статистика
        self._print_summary()

    def _signal_handler(self, signum, frame):
        """Обработчик прерывания"""
        self.interrupted = True
        self.stdout.write(self.style.WARNING("\n\n⚠️  Прерывание..."))
        self._print_summary()
        sys.exit(0)
