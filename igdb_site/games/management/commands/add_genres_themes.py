"""
Команда для добавления жанров и тем к играм из JSONL-файлов.
Максимально оптимизированная версия с массовыми операциями.
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
    help = 'Добавляет жанры и темы к играм из JSONL-файлов'

    def add_arguments(self, parser):
        parser.add_argument('--dry-run', action='store_true', help='Режим просмотра без сохранения')
        parser.add_argument('--file', type=str, help='Конкретный файл для обработки')
        parser.add_argument('--force-restart', action='store_true', help='Сбросить состояние')
        parser.add_argument('--batch-size', type=int, default=1000, help='Размер пакета (по умолчанию 1000)')
        parser.add_argument('--verbose', action='store_true', help='Подробный вывод')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dry_run = False
        self.force_restart = False
        self.batch_size = 1000
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

        # Кэши
        self.genre_map = {}  # name -> id
        self.theme_map = {}  # name -> id
        self.game_map = {}  # igdb_id -> game_id

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

        self.genres_set = set([
            'Action', 'Adventure', 'Arcade', 'Base Building', 'Card & Board Game',
            'Fighting', 'MOBA', 'Music', 'Open World', 'Pinball', 'Platform',
            'Point-and-click', 'Precision Combat', 'Puzzle', 'Quiz/Trivia', 'Racing',
            'Real Time Strategy (RTS)', 'Role-playing (RPG)', 'Sandbox', 'Shooter',
            'Simulator', 'Sport', 'Squad Management', 'Strategy', 'Survival',
            'Tactical', 'Turn-based', 'Turn-based strategy (TBS)', 'Visual Novel'
        ])

        self.themes_set = set([
            '4X (explore, expand, exploit, and exterminate)', 'Business', 'Comedy',
            'Crafting & Gathering', 'Drama', 'Educational', 'Erotic', 'Fantasy',
            'Fire Emblem', 'Gothic', 'Historical', 'Horror', 'Indie', 'Kids',
            'Medieval', 'Mystery', 'Non-fiction', 'Party', 'Post-apocalyptic',
            'Romance', 'Science fiction', 'Stealth', 'Thriller', 'Warfare'
        ])

    def _setup_paths(self):
        self.base_dir = Path(os.getcwd())
        self.data_dir = self.base_dir / 'add_genres_themes'
        self.log_dir = self.data_dir / 'logs'
        self.state_file_path = self.log_dir / 'processed_files_state.json'
        self.additions_global_path = self.log_dir / 'additions_global.log'
        self.additions_current_path = self.log_dir / 'additions_current.log'
        self.errors_log_path = self.log_dir / 'errors.log'
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def _get_file_hash(self, file_path: Path) -> str:
        hash_md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(8192), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def _is_file_processed(self, file_path: Path) -> Tuple[bool, str]:
        file_name = file_path.name
        if file_name not in self.processed_state.get('files', {}):
            return False, ""

        file_state = self.processed_state['files'][file_name]
        current_hash = self._get_file_hash(file_path)

        if file_state.get('file_hash') != current_hash:
            return False, "файл изменен"

        return True, f"обработан {file_state.get('processed_at')}"

    def _mark_file_processed(self, file_path: Path, games_updated: int):
        file_name = file_path.name
        self.processed_state['files'][file_name] = {
            'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'file_hash': self._get_file_hash(file_path),
            'games_updated': games_updated
        }
        with open(self.state_file_path, 'w', encoding='utf-8') as f:
            json.dump(self.processed_state, f, ensure_ascii=False, indent=2)

    def _load_caches(self):
        """Загружает все необходимые кэши одним запросом"""
        if self.verbose:
            self.stdout.write("Загрузка кэша...")

        # Загружаем все жанры
        for genre in Genre.objects.all():
            self.genre_map[genre.name] = genre.id
            # Добавляем варианты с разным регистром
            self.genre_map[genre.name.lower()] = genre.id

        # Загружаем все темы
        for theme in Theme.objects.all():
            self.theme_map[theme.name] = theme.id
            self.theme_map[theme.name.lower()] = theme.id

        # Загружаем все игры
        for game in Game.objects.all():
            self.game_map[game.igdb_id] = game.id

        if self.verbose:
            self.stdout.write(
                f"  Жанров: {len(self.genre_map) // 2}, Тем: {len(self.theme_map) // 2}, Игр: {len(self.game_map)}")

    def _get_genre_id(self, name: str) -> int:
        """Быстрый поиск ID жанра"""
        if not name:
            return None
        if name in self.genre_map:
            return self.genre_map[name]
        lower = name.lower()
        if lower in self.genre_map:
            return self.genre_map[lower]
        return None

    def _get_theme_id(self, name: str) -> int:
        """Быстрый поиск ID темы"""
        if not name:
            return None
        if name in self.theme_map:
            return self.theme_map[name]
        lower = name.lower()
        if lower in self.theme_map:
            return self.theme_map[lower]
        return None

    def _process_file(self, file_path: Path) -> Dict:
        """Обрабатывает файл с максимальной скоростью"""
        file_start = time.time()

        # Читаем все строки
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [l.strip() for l in f if l.strip()]

        total = len(lines)
        if self.verbose:
            self.stdout.write(f"\n📁 {file_path.name} ({total} игр)")

        # Разбиваем на пакеты
        batches = [lines[i:i + self.batch_size] for i in range(0, total, self.batch_size)]

        file_updated = 0
        file_skipped = 0
        file_errors = 0
        file_genres = 0
        file_themes = 0

        # Для массовых вставок
        all_genre_updates = []  # [(game_id, genre_id), ...]
        all_theme_updates = []  # [(game_id, theme_id), ...]

        # Для логов
        additions_log = []
        errors_log = []

        for batch_num, batch in enumerate(batches, 1):
            if self.interrupted:
                break

            # Парсим и анализируем
            game_ids = []
            games_data = []
            parse_errors = 0

            for line in batch:
                try:
                    data = json.loads(line)
                    igdb_id = data.get('id')
                    if igdb_id and igdb_id in self.game_map:
                        game_ids.append(igdb_id)
                        games_data.append(data)
                    else:
                        parse_errors += 1
                        if igdb_id:
                            errors_log.append(
                                (igdb_id, data.get('name', 'Unknown'), f"Игра не найдена в БД (igdb_id={igdb_id})"))
                        else:
                            errors_log.append((0, 'Unknown', "Отсутствует поле id"))
                except json.JSONDecodeError as e:
                    parse_errors += 1
                    errors_log.append((0, 'JSON', f"Ошибка парсинга строки: {e}"))

            if not games_data:
                file_errors += parse_errors
                continue

            # Загружаем текущие связи для этих игр одним запросом
            current_genres = defaultdict(set)
            current_themes = defaultdict(set)
            game_db_ids = [self.game_map[gid] for gid in game_ids]

            if game_db_ids:
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

            # Анализируем каждую игру
            batch_updates_genres = []
            batch_updates_themes = []
            batch_additions = []
            batch_errors = []

            for data in games_data:
                igdb_id = data['id']
                game_db_id = self.game_map[igdb_id]
                game_name = data.get('name', 'Unknown')

                # Собираем новые жанры и темы
                new_genre_ids = set()
                new_theme_ids = set()
                new_genre_names = set()
                new_theme_names = set()
                missing_genres = []
                missing_themes = []

                # Обрабатываем жанры
                for name in data.get('genres', []):
                    if name in self.genres_set:
                        gid = self._get_genre_id(name)
                        if gid:
                            new_genre_ids.add(gid)
                            new_genre_names.add(name)
                        else:
                            missing_genres.append(name)
                    elif name in self.themes_set:
                        tid = self._get_theme_id(name)
                        if tid:
                            new_theme_ids.add(tid)
                            new_theme_names.add(name)
                        else:
                            missing_themes.append(name)
                    else:
                        if name.lower() == 'stealth':
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
                                missing_genres.append(name)

                # Обрабатываем темы
                for name in data.get('themes', []):
                    if name in self.themes_set:
                        tid = self._get_theme_id(name)
                        if tid:
                            new_theme_ids.add(tid)
                            new_theme_names.add(name)
                        else:
                            missing_themes.append(name)
                    elif name in self.genres_set:
                        gid = self._get_genre_id(name)
                        if gid:
                            new_genre_ids.add(gid)
                            new_genre_names.add(name)
                        else:
                            missing_genres.append(name)
                    else:
                        if name.lower() == 'stealth':
                            tid = self._get_theme_id(name)
                            if tid:
                                new_theme_ids.add(tid)
                                new_theme_names.add(name)
                            else:
                                missing_themes.append(name)
                        else:
                            tid = self._get_theme_id(name)
                            if tid:
                                new_theme_ids.add(tid)
                                new_theme_names.add(name)
                            else:
                                missing_themes.append(name)

                # Логируем пропущенные жанры/темы
                for missing in missing_genres:
                    batch_errors.append((igdb_id, game_name, f"Жанр '{missing}' не найден в БД"))
                for missing in missing_themes:
                    batch_errors.append((igdb_id, game_name, f"Тема '{missing}' не найдена в БД"))

                # Определяем что нужно добавить
                current_genre_ids = current_genres.get(game_db_id, set())
                current_theme_ids = current_themes.get(game_db_id, set())

                added_genres = new_genre_ids - current_genre_ids
                added_themes = new_theme_ids - current_theme_ids
                added_genre_names_list = [n for n in new_genre_names if self._get_genre_id(n) in added_genres]
                added_theme_names_list = [n for n in new_theme_names if self._get_theme_id(n) in added_themes]

                if added_genres or added_themes:
                    for gid in added_genres:
                        batch_updates_genres.append((game_db_id, gid))
                    for tid in added_themes:
                        batch_updates_themes.append((game_db_id, tid))
                    file_updated += 1
                    file_genres += len(added_genres)
                    file_themes += len(added_themes)

                    # Сохраняем для лога
                    if added_genre_names_list or added_theme_names_list:
                        batch_additions.append((igdb_id, game_name, added_genre_names_list, added_theme_names_list))
                else:
                    file_skipped += 1

                # Если есть ошибки, увеличиваем счетчик
                if missing_genres or missing_themes:
                    file_errors += 1

            file_errors += parse_errors

            # Сохраняем в общий список для массовой вставки
            all_genre_updates.extend(batch_updates_genres)
            all_theme_updates.extend(batch_updates_themes)

            # Добавляем в лог
            additions_log.extend(batch_additions)
            errors_log.extend(batch_errors)

            # Прогресс
            elapsed = time.time() - file_start
            progress = batch_num / len(batches)
            if not self.verbose:
                bar = "█" * int(progress * 20) + "░" * (20 - int(progress * 20))
                self.stdout.write(
                    f"\r[{bar}] {batch_num}/{len(batches)} | "
                    f"✅ {file_updated} ⏭️ {file_skipped} ❌ {file_errors} | "
                    f"📈 {file_updated / elapsed:.1f} игр/с" if elapsed > 0 else "",
                    ending=''
                )

        # Массовая вставка (только не в dry-run)
        if all_genre_updates and not self.dry_run and not self.interrupted:
            with connection.cursor() as cursor:
                unique_genres = list(set(all_genre_updates))
                for i in range(0, len(unique_genres), 1000):
                    batch = unique_genres[i:i + 1000]
                    values = ','.join([f"({gid}, {tid})" for gid, tid in batch])
                    cursor.execute(f"""
                        INSERT INTO games_game_genres (game_id, genre_id) 
                        VALUES {values} 
                        ON CONFLICT DO NOTHING
                    """)

        if all_theme_updates and not self.dry_run and not self.interrupted:
            with connection.cursor() as cursor:
                unique_themes = list(set(all_theme_updates))
                for i in range(0, len(unique_themes), 1000):
                    batch = unique_themes[i:i + 1000]
                    values = ','.join([f"({gid}, {tid})" for gid, tid in batch])
                    cursor.execute(f"""
                        INSERT INTO games_game_themes (game_id, theme_id) 
                        VALUES {values} 
                        ON CONFLICT DO NOTHING
                    """)

        # Записываем лог добавлений (всегда, даже в dry-run)
        if additions_log:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            prefix = "[DRY-RUN] " if self.dry_run else ""

            # Пишем в текущий лог
            with open(self.additions_current_path, 'a', encoding='utf-8') as f:
                for igdb_id, game_name, genres, themes in additions_log:
                    if genres:
                        f.write(
                            f"{timestamp} | {prefix}Game {igdb_id} | {game_name} | Added genres: {', '.join(genres)}\n")
                    if themes:
                        f.write(
                            f"{timestamp} | {prefix}Game {igdb_id} | {game_name} | Added themes: {', '.join(themes)}\n")

            # В реальном режиме пишем и в глобальный лог
            if not self.dry_run:
                with open(self.additions_global_path, 'a', encoding='utf-8') as f:
                    for igdb_id, game_name, genres, themes in additions_log:
                        if genres:
                            f.write(f"{timestamp} | Game {igdb_id} | {game_name} | Added genres: {', '.join(genres)}\n")
                        if themes:
                            f.write(f"{timestamp} | Game {igdb_id} | {game_name} | Added themes: {', '.join(themes)}\n")

        # Записываем лог ошибок (всегда, даже в dry-run)
        if errors_log:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            prefix = "[DRY-RUN] " if self.dry_run else ""

            with open(self.errors_log_path, 'a', encoding='utf-8') as f:
                for igdb_id, game_name, error in errors_log:
                    f.write(f"{timestamp} | {prefix}Game {igdb_id} | {game_name} | ERROR: {error}\n")

        # Обновляем статистику
        self.stats['total_games'] += total
        self.stats['updated_games'] += file_updated
        self.stats['skipped_games'] += file_skipped
        self.stats['error_games'] += file_errors
        self.stats['total_genres_added'] += file_genres
        self.stats['total_themes_added'] += file_themes

        if self.verbose:
            total_time = time.time() - file_start
            self.stdout.write(f"\n  ✅ {file_updated} | ⏭️ {file_skipped} | ❌ {file_errors}")
            self.stdout.write(f"  📈 {total_time:.1f} сек, {total / total_time:.1f} игр/сек")

        return {'updated_games': file_updated, 'total_lines': total}

    def _print_summary(self):
        total_time = time.time() - self.start_time if self.start_time else 0
        self.stdout.write(self.style.SUCCESS("\n" + "=" * 50))
        self.stdout.write(self.style.SUCCESS("ИТОГОВАЯ СТАТИСТИКА"))
        self.stdout.write(self.style.SUCCESS("=" * 50))
        self.stdout.write(f"Общее время: {total_time:.1f} сек")
        self.stdout.write(f"Обработано игр: {self.stats['total_games']}")
        self.stdout.write(f"Обновлено игр: {self.stats['updated_games']}")
        self.stdout.write(f"Пропущено: {self.stats['skipped_games']}")
        self.stdout.write(f"Ошибок: {self.stats['error_games']}")
        self.stdout.write(f"Добавлено жанров: {self.stats['total_genres_added']}")
        self.stdout.write(f"Добавлено тем: {self.stats['total_themes_added']}")
        if self.stats['total_games'] > 0:
            self.stdout.write(f"Скорость: {self.stats['total_games'] / total_time:.1f} игр/сек")
        if self.dry_run:
            self.stdout.write(self.style.WARNING("\n[DRY-RUN] Изменения не сохранены"))

    def handle(self, *args, **options):
        signal.signal(signal.SIGINT, self._signal_handler)
        self.start_time = time.time()

        self.dry_run = options['dry_run']
        self.force_restart = options['force_restart']
        self.batch_size = options['batch_size']
        self.verbose = options.get('verbose', False)
        specific_file = options.get('file')

        self._setup_paths()

        # Очищаем текущие логи (всегда)
        with open(self.additions_current_path, 'w', encoding='utf-8') as f:
            f.write("")
        with open(self.errors_log_path, 'w', encoding='utf-8') as f:
            f.write("")

        # Загружаем состояние
        if self.state_file_path.exists():
            with open(self.state_file_path, 'r', encoding='utf-8') as f:
                self.processed_state = json.load(f)
        else:
            self.processed_state = {'files': {}}

        if self.force_restart:
            self.processed_state = {'files': {}}

        # Загружаем кэши
        self._load_caches()

        # Получаем файлы для обработки
        if specific_file:
            files = [self.data_dir / specific_file]
            if not files[0].exists():
                self.stderr.write(self.style.ERROR(f"Файл не найден: {specific_file}"))
                return
        else:
            files = sorted(self.data_dir.glob('*.txt'))

        if not files:
            self.stdout.write(self.style.WARNING("Нет файлов для обработки"))
            return

        self.stdout.write(f"Найдено файлов: {len(files)}")

        for file_path in files:
            if self.interrupted:
                break

            is_processed, reason = self._is_file_processed(file_path)
            if is_processed and not self.force_restart:
                self.stdout.write(self.style.WARNING(f"Пропущен {file_path.name}: {reason}"))
                self.stats['skipped_files'].append(file_path.name)
                continue

            stats = self._process_file(file_path)
            self.stats['processed_files'].append({
                'name': file_path.name,
                'games_updated': stats['updated_games']
            })

            if not self.dry_run:
                self._mark_file_processed(file_path, stats['updated_games'])

        self._print_summary()

    def _signal_handler(self, signum, frame):
        self.interrupted = True
        self.stdout.write(self.style.WARNING("\n\nПрерывание..."))
        self._print_summary()
        sys.exit(0)