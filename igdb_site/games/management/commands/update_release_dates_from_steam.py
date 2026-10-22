# games/management/commands/update_release_dates_from_steam.py

"""
Команда для обновления дат релиза (first_release_date) у игр, у которых они отсутствуют,
с использованием локального дампа SteamDB (steamdb.min.json).

Больше не делает HTTP-запросов к Steam. Использует:
- steamdb.min.json — готовый дамп с полями steam_appid, name и release_date.
- steam_found.txt — сопоставление Game.id -> Steam App ID (опционально).

Если для игры нет app_id в steam_found.txt, выполняется fallback-поиск по названию в дампе.
Обновление происходит через Game.objects.filter(...).update(), чтобы не вызывать сигналы
и не засорять вывод DEBUG-сообщениями.

Установлены statement_timeout и lock_timeout, чтобы update() не зависал на блокировках.
"""

import re
import time
import json
import os
import sys
import signal
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List, Tuple

from django.core.management.base import BaseCommand, CommandParser
from django.utils import timezone
from django.db import connection
from django.db.models import Q

from games.models import Game

try:
    import ijson

    HAS_IJSON = True
except ImportError:
    HAS_IJSON = False

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    """
    Обновляет даты релиза игр, используя локальный дамп SteamDB.
    """

    help = 'Обновляет first_release_date у игр без даты, используя steamdb.min.json'

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('--limit', type=int, default=None, help='Максимальное количество игр для обработки')
        parser.add_argument('--offset', type=int, default=0, help='Пропустить первые N игр')
        parser.add_argument('--game-id', type=int, nargs='+', help='Обработать конкретные игры по ID')
        parser.add_argument('--game-name', type=str, help='Обработать игру по названию (частичное совпадение)')
        parser.add_argument('--dry-run', action='store_true',
                            help='Режим просмотра: показать, что будет обновлено, но не сохранять в БД')
        parser.add_argument('--force', action='store_true', help='Обновить дату, даже если она уже есть (перезаписать)')
        parser.add_argument('--batch-size', type=int, default=500,
                            help='Частота сохранения прогресса (по умолчанию 500)')
        parser.add_argument('--resume-file', type=str, default='steam_release_dates_progress.json',
                            help='Файл для сохранения прогресса (только для реального режима)')
        parser.add_argument('--no-resume', action='store_true', help='Не загружать прогресс, начать заново')
        parser.add_argument('--found-list-file', type=str, default='steam_found.txt',
                            help='Файл со списком найденных игр (Game ID -> Steam App ID)')
        parser.add_argument('--steamdb-file', type=str, default='steamdb.min.json',
                            help='Путь к JSON-дампу SteamDB (по умолчанию steamdb.min.json)')
        parser.add_argument('--use-ijson', action='store_true', default=None,
                            help='Использовать ijson для потокового чтения (по умолчанию автоопределение)')
        parser.add_argument('--verbose', action='store_true', help='Подробный вывод')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dry_run = False
        self.force = False
        self.verbose = False
        self.save_progress_every = 500
        self.processed_ids = set()
        self.resume_file_path = None
        self.found_list_file_path = None
        self.steamdb_file_path = None
        self.found_app_ids = {}
        self.steamdb_dates = {}  # {app_id: datetime}
        self.steamdb_by_name = {}  # {name.lower(): app_id} для fallback
        self.stats = {
            'total': 0,
            'processed': 0,
            'updated': 0,
            'skipped_has_date': 0,
            'skipped_no_app_id': 0,
            'skipped_no_date_in_dump': 0,
            'errors': 0,
            'start_time': None,
        }
        self._interrupted = False
        self._last_progress_line_len = 0

    def _load_progress(self) -> None:
        if self.dry_run:
            return
        if not self.resume_file_path or not os.path.exists(self.resume_file_path):
            return
        try:
            with open(self.resume_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.processed_ids = set(data.get('processed_ids', []))
            if self.verbose:
                self.stdout.write(
                    self.style.WARNING(f"📂 Загружен прогресс: {len(self.processed_ids)} обработанных игр"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"⚠️ Ошибка загрузки прогресса: {e}"))

    def _save_progress(self) -> None:
        if self.dry_run:
            return
        if not self.resume_file_path:
            return
        try:
            data = {
                'processed_ids': list(self.processed_ids),
                'updated_at': datetime.now().isoformat(),
            }
            with open(self.resume_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            if self.verbose:
                self.stderr.write(f"⚠️ Ошибка сохранения прогресса: {e}")

    def _load_found_app_ids(self, file_path: str) -> Dict[int, int]:
        if not os.path.exists(file_path):
            self.stdout.write(
                self.style.WARNING(f"⚠️ Файл {file_path} не найден, будет использован fallback по названию"))
            return {}

        result = {}
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('='):
                        continue
                    game_match = re.search(r'Game ID:\s*(\d+)', line)
                    if not game_match:
                        continue
                    game_id = int(game_match.group(1))
                    app_match = re.search(r'Steam App ID:\s*(\d+)', line)
                    if not app_match:
                        continue
                    app_id = int(app_match.group(1))
                    result[game_id] = app_id
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"⚠️ Ошибка загрузки {file_path}: {e}"))

        self.stdout.write(self.style.SUCCESS(f"📂 Загружено {len(result)} app_id из {file_path}"))
        return result

    def _parse_release_date(self, date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        date_str = date_str.strip()
        if not date_str:
            return None
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
            return timezone.make_aware(dt, timezone.get_current_timezone())
        except ValueError:
            for fmt in ('%Y-%m', '%Y'):
                try:
                    dt = datetime.strptime(date_str, fmt)
                    if fmt == '%Y-%m':
                        dt = dt.replace(day=1)
                    elif fmt == '%Y':
                        dt = dt.replace(month=1, day=1)
                    return timezone.make_aware(dt, timezone.get_current_timezone())
                except ValueError:
                    continue
        return None

    def _load_steamdb_dates(self, steamdb_file: str, found_app_ids: Dict[int, int]) -> Dict[int, datetime]:
        if not os.path.exists(steamdb_file):
            self.stdout.write(self.style.ERROR(f"❌ Файл {steamdb_file} не найден"))
            return {}

        self.stdout.write(f"📖 Загрузка дат из {steamdb_file}...")
        start_time = time.time()
        result = {}
        self.steamdb_by_name = {}

        use_ijson = self.use_ijson if hasattr(self, 'use_ijson') else False
        if use_ijson is None:
            use_ijson = HAS_IJSON

        if use_ijson and HAS_IJSON:
            self.stdout.write("   🔄 Используется ijson (потоковое чтение)")
            try:
                with open(steamdb_file, 'rb') as f:
                    for item in ijson.items(f, 'item'):
                        app_id = item.get('steam_appid')
                        name = item.get('name')
                        if name:
                            self.steamdb_by_name[name.lower()] = app_id
                        date_str = item.get('release_date')
                        parsed = self._parse_release_date(date_str)
                        if parsed:
                            result[app_id] = parsed
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Ошибка ijson: {e}"))
                return {}
        else:
            self.stdout.write("   🔄 Используется json.load (загрузка в память)")
            try:
                with open(steamdb_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                for item in data:
                    app_id = item.get('steam_appid')
                    name = item.get('name')
                    if name:
                        self.steamdb_by_name[name.lower()] = app_id
                    date_str = item.get('release_date')
                    parsed = self._parse_release_date(date_str)
                    if parsed:
                        result[app_id] = parsed
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Ошибка json.load: {e}"))
                return {}

        elapsed = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(
            f"   ✅ Загружено {len(result)} дат за {elapsed:.1f}с, построено {len(self.steamdb_by_name)} названий"))
        return result

    def _get_games_queryset(self, options: Dict[str, Any]):
        if options.get('game_id'):
            return Game.objects.filter(id__in=options['game_id'])
        if options.get('game_name'):
            name = options['game_name']
            return Game.objects.filter(name__icontains=name).order_by('id')

        qs = Game.objects.all()
        if not self.force:
            qs = Game.objects.filter(first_release_date__isnull=True)
        return qs.order_by('id')

    def _signal_handler(self, signum, frame):
        self._interrupted = True
        self.stdout.write(self.style.WARNING('\n\n⚠️ Прерывание...'))
        if self.dry_run:
            self.stdout.write(self.style.WARNING('Прогресс не сохранён (режим dry-run)'))
        else:
            self._save_progress()
            self.stdout.write(self.style.WARNING('Прогресс сохранён.'))
        self.stdout.write(self.style.WARNING('Выход.'))
        self.stdout.flush()
        os._exit(0)

    def _render_progress_bar(self, total: int, elapsed: float):
        s = self.stats
        processed = s['processed']
        percent = (processed / total * 100) if total > 0 else 0.0
        speed = processed / elapsed if elapsed > 0 else 0.0
        eta = (total - processed) / speed if speed > 0 else 0.0

        line = (
            f"\r🔄 {processed}/{total} ({percent:5.1f}%) | "
            f"✅ {s['updated']} | "
            f"⏭️  {s['skipped_has_date'] + s['skipped_no_app_id'] + s['skipped_no_date_in_dump']} | "
            f"❌ {s['errors']} | "
            f"⚡ {speed:.1f}/с | "
            f"⏳ {eta:.0f}с"
        )

        if self._last_progress_line_len > len(line):
            line += ' ' * (self._last_progress_line_len - len(line))

        self._last_progress_line_len = len(line)
        self.stdout.write(line, ending='')
        self.stdout.flush()

    def handle(self, *args, **options):
        signal.signal(signal.SIGINT, self._signal_handler)

        # Устанавливаем таймауты для сессии PostgreSQL, чтобы update() не зависал на блокировках
        try:
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout = '5s'")
                cursor.execute("SET lock_timeout = '5s'")
            self.stdout.write(
                self.style.SUCCESS("✅ Установлены таймауты PostgreSQL: statement_timeout=5s, lock_timeout=5s"))
        except Exception as e:
            self.stdout.write(self.style.WARNING(f"⚠️ Не удалось установить таймауты PostgreSQL: {e}"))

        self.dry_run = options.get('dry_run', False)
        self.force = options.get('force', False)
        self.verbose = options.get('verbose', False)
        self.save_progress_every = options.get('batch_size', 500)
        self.resume_file_path = options.get('resume_file')
        self.found_list_file_path = options.get('found_list_file', 'steam_found.txt')
        self.steamdb_file_path = options.get('steamdb_file', 'steamdb.min.json')
        self.use_ijson = options.get('use_ijson', None)

        self.stats['start_time'] = time.time()

        self.found_app_ids = self._load_found_app_ids(self.found_list_file_path)
        if not self.found_app_ids:
            self.stdout.write(self.style.WARNING("⚠️ Нет app_id в found-list, будет использован fallback по названию"))

        self.steamdb_dates = self._load_steamdb_dates(self.steamdb_file_path, self.found_app_ids)
        if not self.steamdb_dates:
            self.stdout.write(self.style.WARNING("⚠️ Не удалось загрузить ни одной даты из дампа."))
            return

        if not self.dry_run and not options.get('no_resume', False):
            self._load_progress()

        games_qs = self._get_games_queryset(options)

        offset = options.get('offset', 0)
        limit = options.get('limit', None)
        if offset:
            games_qs = games_qs[offset:]
        if limit:
            games_qs = games_qs[:limit]

        if not self.dry_run and self.processed_ids:
            games_qs = games_qs.exclude(id__in=self.processed_ids)

        total = games_qs.count()
        self.stats['total'] = total

        if total == 0:
            self.stdout.write(self.style.SUCCESS('✅ Нет игр для обработки.'))
            return

        self.stdout.write(self.style.SUCCESS(f'🚀 Начинаю обработку {total} игр...'))
        if self.dry_run:
            self.stdout.write(
                self.style.WARNING('🔍 РЕЖИМ DRY-RUN: изменения не будут сохранены в БД, прогресс не сохраняется'))
        if self.force:
            self.stdout.write(self.style.WARNING('⚠️  РЕЖИМ FORCE: даты будут перезаписаны, даже если уже есть'))
        self.stdout.write('')

        processed_count = 0
        last_report = time.time()

        for game in games_qs.iterator(chunk_size=1000):
            if self._interrupted:
                break

            if game.first_release_date and not self.force:
                with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                    self.stats['skipped_has_date'] += 1
                self.processed_ids.add(game.id)
                with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                    self.stats['processed'] += 1
                processed_count += 1
                continue

            app_id = self.found_app_ids.get(game.id)
            if not app_id:
                name_lower = game.name.lower()
                app_id = self.steamdb_by_name.get(name_lower)
                if not app_id:
                    with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                        self.stats['skipped_no_app_id'] += 1
                    self.processed_ids.add(game.id)
                    with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                        self.stats['processed'] += 1
                    processed_count += 1
                    continue

            new_date = self.steamdb_dates.get(app_id)
            if not new_date:
                with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                    self.stats['skipped_no_date_in_dump'] += 1
                self.processed_ids.add(game.id)
                with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                    self.stats['processed'] += 1
                processed_count += 1
                continue

            # Обновляем через update() — не вызывает сигналы, не засоряет DEBUG
            if not self.dry_run:
                try:
                    Game.objects.filter(id=game.id).update(first_release_date=new_date)
                    with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                        self.stats['updated'] += 1
                except Exception as e:
                    with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                        self.stats['errors'] += 1
                    if self.verbose:
                        self.stderr.write(f"Ошибка обновления {game.id}: {e}")
            else:
                with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                    self.stats['updated'] += 1

            self.processed_ids.add(game.id)
            with self.stats_lock if hasattr(self, 'stats_lock') else _dummy_lock():
                self.stats['processed'] += 1
            processed_count += 1

            now = time.time()
            if now - last_report >= 0.5 or processed_count == total:
                elapsed = now - self.stats['start_time']
                self._render_progress_bar(total, elapsed)
                last_report = now

                if not self.dry_run and processed_count % self.save_progress_every == 0:
                    self._save_progress()

        self.stdout.write('')
        if not self.dry_run:
            self._save_progress()

        elapsed = time.time() - self.stats['start_time']
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write(self.style.SUCCESS('📊 ИТОГОВАЯ СТАТИСТИКА'))
        self.stdout.write('=' * 60)
        self.stdout.write(f'🔄 Всего игр: {self.stats["total"]}')
        self.stdout.write(f'✅ Обработано: {self.stats["processed"]}')
        self.stdout.write(f'💾 Обновлено в БД: {self.stats["updated"]} ({"(будет обновлено)" if self.dry_run else ""})')
        self.stdout.write(f'⏭️  Пропущено (уже есть дата): {self.stats["skipped_has_date"]}')
        self.stdout.write(f'⏭️  Пропущено (нет app_id): {self.stats["skipped_no_app_id"]}')
        self.stdout.write(f'⏭️  Пропущено (нет даты в дампе): {self.stats["skipped_no_date_in_dump"]}')
        self.stdout.write(f'❌ Ошибок: {self.stats["errors"]}')
        self.stdout.write(f'⏱️  Время выполнения: {elapsed:.1f} секунд')
        if elapsed > 0:
            self.stdout.write(f'⚡ Средняя скорость: {self.stats["processed"] / elapsed:.1f} игр/сек')
        if self.dry_run:
            self.stdout.write(
                self.style.WARNING('\n🔍 РЕЖИМ DRY-RUN: изменения НЕ сохранены в БД, прогресс НЕ сохранён'))
        self.stdout.write('=' * 60)


class _dummy_lock:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass