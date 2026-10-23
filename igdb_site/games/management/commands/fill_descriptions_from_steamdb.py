# games/management/commands/fill_descriptions_from_steamdb.py

"""
Команда для заполнения rawg_description из локального дампа SteamDB
(steamdb.min.json или steamdb.min.json.gz) для игр, у которых описание отсутствует.

Используется как фолбэк после fetch_steam_descriptions (Steam API),
когда часть игр так и не получила описание.

Поддерживаются оба формата:
  - обычный JSON:      steamdb.min.json
  - сжатый gzip:       steamdb.min.json.gz
Определение по расширению .gz (автоматически).
Для .gz принудительно используется ijson (потоково, без распаковки в память),
если ijson доступен.

Сопоставление:
  1. По steam_appid (если известен из steam_found.txt или передан явно).
  2. Fallback по нормализованному названию (нижний регистр, без суффиксов
     Enhanced/Remastered/GOTY и т.п., без спецсимволов).

Фильтр "магазинных" описаний:
  В дампе SteamDB у части app_id поле description содержит не описание игры,
  а описание магазина/каталога (GOG, "Discover the grim dark universes", "Buy now",
  "Add to cart" и т.п.). Такие описания ОТБРАСЫВАЮТСЯ автоматически.

Вывод описаний:
  В тестовый файл (по умолчанию descriptions_test.txt) пишутся ТОЛЬКО
  найденные и загруженные описания (reason=ok).

Важно про --limit:
  --limit N означает "остановиться после N УСПЕШНО заполненных описаний",
  а не "взять первые N игр из БД".

Примеры:
    python manage.py fill_descriptions_from_steamdb --dry-run
    python manage.py fill_descriptions_from_steamdb
    python manage.py fill_descriptions_from_steamdb --steamdb-file steamdb.min.json
    python manage.py fill_descriptions_from_steamdb --limit 100 --output test_100.txt
    python manage.py fill_descriptions_from_steamdb --game-ids-file temp_game_ids.txt
    python manage.py fill_descriptions_from_steamdb --force
    python manage.py fill_descriptions_from_steamdb --offset 500 --limit 1000
"""

import os
import re
import gzip
import json
import time
from typing import Dict, Optional, List, Tuple, Set, TextIO

from django.core.management.base import BaseCommand, CommandParser
from django.db import transaction
from django.db.models import Q

from games.models import Game

try:
    import ijson

    HAS_IJSON = True
except ImportError:
    HAS_IJSON = False

# Суффиксы, которые нужно удалять при нормализации названий
SUFFIXES_TO_REMOVE = [
    'enhanced',
    'remastered',
    'complete edition',
    'definitive edition',
    'the definitive edition',
    'goty',
    'game of the year',
    'legacy',
    'digital deluxe',
    'deluxe edition',
    'ultimate edition',
    'gold edition',
    'premium edition',
    'collector\'s edition',
    'special edition',
    'anniversary edition',
]

# Маркеры "магазинных" описаний (описание каталога/магазина, а не игры).
STORE_DESCRIPTION_MARKERS = [
    re.compile(r'\bGOG\.com\b', re.IGNORECASE),
    re.compile(r'\bDiscover the grim dark universes\b', re.IGNORECASE),
    re.compile(r'\bFrom Warhammer 40,000 to Warhammer Fantasy and more\b', re.IGNORECASE),
    re.compile(r'\bdiscover it all on GOG\b', re.IGNORECASE),
    re.compile(r'\bBuy now\b', re.IGNORECASE),
    re.compile(r'\bAdd to cart\b', re.IGNORECASE),
    re.compile(r'\bAdd to wishlist\b', re.IGNORECASE),
    re.compile(r'\bAvailable on (Steam|GOG|Epic)\b', re.IGNORECASE),
    re.compile(r'\bWishlist now\b', re.IGNORECASE),
    re.compile(r'\bGet it on GOG\b', re.IGNORECASE),
    re.compile(r'\bThis (game|product) is available on\b', re.IGNORECASE),
]


def normalize_name(name: str) -> str:
    """Нормализует название игры: нижний регистр, убирает суффиксы и спецсимволы."""
    if not name:
        return ''
    result = name.lower().strip()
    for suffix in SUFFIXES_TO_REMOVE:
        if result.endswith(suffix):
            result = result[:-len(suffix)].strip()
    result = re.sub(r'[™®:—\-&]', ' ', result)
    result = re.sub(r'\s+', ' ', result).strip()
    return result


def is_store_description(text: str) -> bool:
    """True, если описание похоже на магазинное/каталожное, а не на описание игры."""
    if not text:
        return False
    for marker in STORE_DESCRIPTION_MARKERS:
        if marker.search(text):
            return True
    return False


def _fmt_int(n: int) -> str:
    """Форматирует число с разделителями тысяч: 1234567 -> '1,234,567'."""
    return f'{n:,}'


class Command(BaseCommand):
    """
    Заполняет rawg_description из локального дампа SteamDB
    (steamdb.min.json или steamdb.min.json.gz) для игр без описания.

    --limit ограничивает количество УСПЕШНО заполненных описаний.

    В тестовый файл пишутся ТОЛЬКО найденные и загруженные описания (reason=ok).
    В консоли: прогресс загрузки дампа + легенда + прогресс-бар обработки.
    """

    help = (
        'Заполняет rawg_description из steamdb.min.json(.gz) для игр без описания '
        '(фолбэк после fetch_steam_descriptions). '
        '--limit N = остановиться после N успешно заполненных описаний. '
        'Магазинные описания (GOG/каталог) отбрасываются. '
        'В тестовый файл пишутся только найденные описания (reason=ok).'
    )

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            '--steamdb-file', type=str, default='steamdb.min.json.gz',
            help='Путь к JSON-дампу SteamDB (.json или .json.gz, по умолчанию steamdb.min.json.gz)'
        )
        parser.add_argument(
            '--dry-run', action='store_true',
            help='Только показать, что будет заполнено, без записи в БД'
        )
        parser.add_argument(
            '--force', action='store_true',
            help='Перезаписывать rawg_description, даже если описание уже есть'
        )
        parser.add_argument(
            '--limit', type=int, default=None,
            help='Остановиться после N УСПЕШНО заполненных описаний (не выборка игр!)'
        )
        parser.add_argument(
            '--offset', type=int, default=0,
            help='Пропустить первые N игр из списка кандидатов'
        )
        parser.add_argument(
            '--batch-size', type=int, default=500,
            help='Размер батча для bulk_update (по умолчанию 500)'
        )
        parser.add_argument(
            '--min-length', type=int, default=20,
            help='Минимальная длина описания из дампа (по умолчанию 20)'
        )
        parser.add_argument(
            '--use-ijson', action='store_true', default=None,
            help='Использовать ijson для потокового чтения (по умолчанию автоопределение; '
                 'для .gz принудительно ijson, если доступен)'
        )
        parser.add_argument(
            '--match-by', type=str, choices=['name', 'app_id', 'both'], default='both',
            help='Способ сопоставления: name, app_id или both (по умолчанию both)'
        )
        parser.add_argument(
            '--game-ids-file', type=str, default=None,
            help='Файл со списком ID игр (по одному на строку) для обработки'
        )
        parser.add_argument(
            '--found-list-file', type=str, default='steam_found.txt',
            help='Файл сопоставления Game.id -> Steam App ID (по умолчанию steam_found.txt)'
        )
        parser.add_argument(
            '--output', type=str, default='descriptions_test.txt',
            help='Тестовый файл с полными текстами найденных описаний '
                 '(по умолчанию descriptions_test.txt)'
        )
        parser.add_argument(
            '--verbose', action='store_true',
            help='Подробный вывод'
        )

    # ------------------------------------------------------------------ #
    #  Загрузка дампа (поддержка .json и .json.gz) + прогресс загрузки
    # ------------------------------------------------------------------ #

    def _load_steamdb(self, steamdb_file: str) -> Tuple[Dict[str, str], Dict[int, str], Set[str]]:
        """
        Загружает дамп SteamDB (.json или .json.gz).
        Магазинные описания не попадают в словари.

        Автоопределение gzip по расширению .gz.
        Для .gz принудительно используется ijson (если доступен) —
        чтобы не держать распакованный JSON в памяти.

        Показывает прогресс: сколько записей прочитано, скорость.
        """
        desc_by_name: Dict[str, str] = {}
        desc_by_appid: Dict[int, str] = {}
        all_names = set()
        store_skipped = 0

        if not os.path.exists(steamdb_file):
            self.stdout.write(self.style.ERROR(f'❌ Файл {steamdb_file} не найден'))
            return desc_by_name, desc_by_appid, all_names

        is_gz = steamdb_file.lower().endswith('.gz')
        format_label = 'gzip' if is_gz else 'plain'

        # Для .gz принудительно ijson (если доступен)
        use_ijson = self.use_ijson
        if use_ijson is None:
            use_ijson = HAS_IJSON
        if is_gz and HAS_IJSON:
            use_ijson = True

        self.stdout.write(f'📖 Загрузка {steamdb_file} ({format_label})...')
        if use_ijson and HAS_IJSON:
            self.stdout.write('   🔄 Используется ijson (потоковое чтение)')
        else:
            self.stdout.write('   🔄 Используется json.load (загрузка в память)')
            if is_gz:
                self.stdout.write(self.style.WARNING(
                    '   ⚠️ ijson недоступен — gzip будет распакован в память'
                ))

        start = time.time()
        last_report = start
        count = 0
        progress_step = 5000  # минимум записей между обновлениями
        progress_interval = 0.5  # минимум секунд между обновлениями
        last_count_shown = 0

        def process_item(item):
            nonlocal store_skipped
            name = item.get('name')
            app_id = item.get('steam_appid')
            desc = item.get('description')

            if name:
                all_names.add(normalize_name(name))

            if desc and isinstance(desc, str):
                desc = desc.strip()
                if len(desc) < self.min_length:
                    return
                if is_store_description(desc):
                    store_skipped += 1
                    return
                if name:
                    desc_by_name[normalize_name(name)] = desc
                if app_id:
                    try:
                        desc_by_appid[int(app_id)] = desc
                    except (TypeError, ValueError):
                        pass

        def maybe_report_progress(force: bool = False):
            nonlocal last_report, last_count_shown
            now = time.time()
            if (not force
                    and count - last_count_shown < progress_step
                    and now - last_report < progress_interval):
                return
            elapsed = now - start
            speed = count / elapsed if elapsed > 0 else 0
            line = (
                f'\r   ⬇️ Прочитано записей: {_fmt_int(count)} | '
                f'{elapsed:5.1f}с | {_fmt_int(int(speed))}/с   '
            )
            self.stdout.write(line, ending='')
            self.stdout.flush()
            last_report = now
            last_count_shown = count

        try:
            if use_ijson and HAS_IJSON:
                if is_gz:
                    f_obj = gzip.open(steamdb_file, 'rb')
                else:
                    f_obj = open(steamdb_file, 'rb')
                with f_obj as f:
                    for item in ijson.items(f, 'item'):
                        count += 1
                        process_item(item)
                        maybe_report_progress()
                # Финальная строка прогресса загрузки
                maybe_report_progress(force=True)
                self.stdout.write('')
            else:
                # json.load — атомарная загрузка, прогресса посередине нет.
                # Показываем "загрузка..." и итог после.
                if is_gz:
                    with gzip.open(steamdb_file, 'rt', encoding='utf-8') as f:
                        data = json.load(f)
                else:
                    with open(steamdb_file, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                total = len(data) if isinstance(data, list) else 0
                self.stdout.write(f'   ⬇️ Прочитано записей: {_fmt_int(total)} (json.load)')
                for item in data:
                    count += 1
                    process_item(item)
        except Exception as e:
            self.stdout.write('')
            self.stdout.write(self.style.ERROR(f'❌ Ошибка загрузки: {e}'))
            return desc_by_name, desc_by_appid, all_names

        elapsed = time.time() - start
        speed = count / elapsed if elapsed > 0 else 0
        self.stdout.write(self.style.SUCCESS(
            f'   ✅ Загружено: по имени {_fmt_int(len(desc_by_name))}, '
            f'по app_id {_fmt_int(len(desc_by_appid))}, '
            f'всего записей {_fmt_int(count)} за {elapsed:.1f}с '
            f'({_fmt_int(int(speed))}/с)'
        ))
        if store_skipped:
            self.stdout.write(self.style.WARNING(
                f'   🚫 Отброшено магазинных описаний при загрузке: {_fmt_int(store_skipped)}'
            ))
        return desc_by_name, desc_by_appid, all_names

    # ------------------------------------------------------------------ #
    #  Загрузка сопоставления Game.id -> Steam App ID
    # ------------------------------------------------------------------ #

    def _load_found_app_ids(self, file_path: str) -> Dict[int, int]:
        """Читает steam_found.txt: Game ID: N - Name (Steam App ID: M)."""
        result: Dict[int, int] = {}
        if not file_path or not os.path.exists(file_path):
            if self.verbose:
                self.stdout.write(self.style.WARNING(
                    f'⚠️ Файл {file_path} не найден — сопоставление по app_id ограничено'
                ))
            return result

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith('='):
                        continue
                    game_match = re.search(r'Game ID:\s*(\d+)', line)
                    if not game_match:
                        continue
                    app_match = re.search(r'Steam App ID:\s*(\d+)', line)
                    if not app_match:
                        continue
                    try:
                        result[int(game_match.group(1))] = int(app_match.group(1))
                    except ValueError:
                        continue
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'⚠️ Ошибка загрузки {file_path}: {e}'))

        if self.verbose:
            self.stdout.write(self.style.SUCCESS(f'📂 Загружено {len(result)} app_id из {file_path}'))
        return result

    # ------------------------------------------------------------------ #
    #  Получение игр для обработки
    # ------------------------------------------------------------------ #

    def _load_game_ids_filter(self, file_path: Optional[str]) -> Optional[set]:
        if not file_path:
            return None
        if not os.path.exists(file_path):
            self.stdout.write(self.style.ERROR(f'❌ Файл ID не найден: {file_path}'))
            return set()
        ids = set()
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.isdigit():
                        ids.add(int(line))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Ошибка загрузки ID: {e}'))
            return set()
        self.stdout.write(self.style.SUCCESS(f'📂 Загружено {len(ids)} ID из {file_path}'))
        return ids

    def _get_games(self, game_ids_filter: Optional[set]) -> List[Game]:
        """
        Загружает игры-кандидаты.

        ВАЖНО: self.limit здесь НЕ применяется — лимит означает
        "сколько описаний заполнить", а не "сколько игр взять".
        """
        qs = Game.objects.all()
        if not self.force:
            qs = qs.filter(Q(rawg_description__isnull=True) | Q(rawg_description=''))
        if game_ids_filter is not None:
            qs = qs.filter(id__in=game_ids_filter)
        qs = qs.order_by('id')
        if self.offset:
            qs = qs[self.offset:]
        return list(qs.only('id', 'name', 'rawg_description'))

    # ------------------------------------------------------------------ #
    #  Запись блока в тестовый файл (ТОЛЬКО reason=ok)
    # ------------------------------------------------------------------ #

    def _write_ok_block(
            self,
            fh: Optional[TextIO],
            game_id: int,
            game_name: str,
            source: Optional[str],
            text: str
    ) -> None:
        """Пишет блок найденного/загруженного описания (reason=ok) в файл."""
        if fh is None:
            return
        header = (
            f'ID={game_id} | name={game_name!r} | source={source or "-"} | '
            f'reason=ok | len={len(text)}'
        )
        fh.write('=' * 80 + '\n')
        fh.write(header + '\n')
        fh.write('-' * 80 + '\n')
        fh.write(text + '\n')
        fh.write('=' * 80 + '\n\n')
        fh.flush()

    # ------------------------------------------------------------------ #
    #  Легенда и прогресс-бар
    # ------------------------------------------------------------------ #

    def _print_legend(self) -> None:
        """Печатает легенду со значками перед прогресс-баром."""
        self.stdout.write('')
        self.stdout.write(self.style.NOTICE('📖 ЛЕГЕНДА:'))
        self.stdout.write(self.style.NOTICE('  📊 просканировано — всего игр просмотрено'))
        self.stdout.write(self.style.NOTICE('  ✅ найдено       — описаний записано'))
        self.stdout.write(self.style.NOTICE('  🚫 магазин       — отброшено магазинных описаний'))
        self.stdout.write(self.style.NOTICE('  ⏭️ короткие      — слишком короткие описания'))
        self.stdout.write(self.style.NOTICE('  ❌ не найдено    — нет в дампе'))
        if self.limit:
            self.stdout.write(self.style.NOTICE(
                f'  🎯 лимит         — цель: {self.limit} заполненных описаний'
            ))
        self.stdout.write(self.style.NOTICE(
            '  📄 в файл        — только reason=ok (найденные и загруженные)'
        ))
        self.stdout.write('')

    def _print_progress(self, stats: Dict, start_time: float, force: bool = False) -> None:
        """Печатает строку прогресса с иконками."""
        elapsed = time.time() - start_time
        speed = stats['scanned'] / elapsed if elapsed > 0 else 0

        if self.limit:
            target = self.limit
            current = stats['matched']
            percent = (current / target * 100) if target else 0
            bar_len = 25
            filled = int(bar_len * current / target) if target else 0
            bar = '█' * filled + '░' * (bar_len - filled)
            line = (
                f'\r🎯 [{bar}] {percent:5.1f}% | ✅ {current}/{target} | '
                f'📊 {stats["scanned"]} | '
                f'🚫 {stats["skipped_store"]} | '
                f'⏭️ {stats["too_short"]} | '
                f'❌ {stats["not_found"]} | '
                f'⚡ {speed:.0f}/с   '
            )
        else:
            scanned = stats['scanned']
            total = stats['total_candidates']
            percent = (scanned / total * 100) if total else 0
            bar_len = 25
            filled = int(bar_len * scanned / total) if total else 0
            bar = '█' * filled + '░' * (bar_len - filled)
            line = (
                f'\r📊 [{bar}] {percent:5.1f}% | '
                f'✅ {stats["matched"]} | '
                f'📊 {scanned}/{total} | '
                f'🚫 {stats["skipped_store"]} | '
                f'⏭️ {stats["too_short"]} | '
                f'❌ {stats["not_found"]} | '
                f'⚡ {speed:.0f}/с   '
            )

        self.stdout.write(line, ending='')
        self.stdout.flush()

        if force:
            self.stdout.write('')

    # ------------------------------------------------------------------ #
    #  Основная логика
    # ------------------------------------------------------------------ #

    def handle(self, *args, **options) -> None:
        self.dry_run = options.get('dry_run', False)
        self.force = options.get('force', False)
        self.verbose = options.get('verbose', False)
        self.limit = options.get('limit')
        self.offset = options.get('offset', 0)
        self.batch_size = options.get('batch_size', 500)
        self.min_length = options.get('min_length', 20)
        self.use_ijson = options.get('use_ijson', None)
        self.match_by = options.get('match_by', 'both')
        steamdb_file = options.get('steamdb_file', 'steamdb.min.json.gz')
        found_list_file = options.get('found_list_file', 'steam_found.txt')
        output_file = options.get('output', 'descriptions_test.txt')
        game_ids_file = options.get('game_ids_file')

        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('📥 FILL DESCRIPTIONS FROM STEAMDB (fallback)'))
        self.stdout.write(self.style.SUCCESS('=' * 60))

        if self.dry_run:
            self.stdout.write(self.style.WARNING('🔍 РЕЖИМ DRY-RUN: в БД ничего не записывается'))
        if self.force:
            self.stdout.write(self.style.WARNING('⚠️ РЕЖИМ FORCE: существующие описания будут перезаписаны'))
        if self.limit:
            self.stdout.write(self.style.WARNING(
                f'🎯 ЛИМИТ: остановиться после {self.limit} УСПЕШНО заполненных описаний'
            ))

        # 0. Тестовый файл описаний — открываем сразу
        report_fh: Optional[TextIO] = None
        try:
            directory = os.path.dirname(os.path.abspath(output_file))
            if directory:
                os.makedirs(directory, exist_ok=True)
            report_fh = open(output_file, 'w', encoding='utf-8')
            report_fh.write(
                f'# FILL DESCRIPTIONS FROM STEAMDB\n'
                f'# started: {time.strftime("%Y-%m-%d %H:%M:%S")}\n'
                f'# steamdb_file: {steamdb_file}\n'
                f'# dry_run: {self.dry_run} | force: {self.force} | limit: {self.limit}\n'
                f'# match_by: {self.match_by}\n'
                f'# only matched (reason=ok)\n'
                f'\n'
            )
            self.stdout.write(self.style.SUCCESS(f'📄 Тестовый файл описаний: {output_file}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'❌ Не удалось открыть {output_file}: {e}'))
            report_fh = None

        try:
            # 1. Загрузка дампа (с автоопределением .gz и прогрессом загрузки)
            desc_by_name, desc_by_appid, all_names = self._load_steamdb(steamdb_file)
            if not desc_by_name and not desc_by_appid:
                self.stdout.write(self.style.ERROR('❌ Не удалось загрузить ни одного описания из дампа'))
                return

            # 2. Сопоставление Game.id -> app_id
            found_app_ids = self._load_found_app_ids(found_list_file)

            # 3. Фильтр по файлу ID
            game_ids_filter = self._load_game_ids_filter(game_ids_file)

            # 4. Игры из БД (без применения limit!)
            self.stdout.write('\n📊 Загрузка игр-кандидатов из БД...')
            games = self._get_games(game_ids_filter)
            total_candidates = len(games)
            self.stdout.write(self.style.SUCCESS(f'   ✅ Игр-кандидатов: {_fmt_int(total_candidates)}'))
            if total_candidates == 0:
                self.stdout.write(self.style.WARNING('⚠️ Нет игр для обработки'))
                return

            # 4.1 Легенда (до прогресс-бара)
            self._print_legend()

            # 5. Обработка
            stats = {
                'total_candidates': total_candidates,
                'scanned': 0,
                'matched': 0,
                'by_app_id': 0,
                'by_name': 0,
                'not_found': 0,
                'too_short': 0,
                'skipped_store': 0,
                'errors': 0,
                'stopped_by_limit': False,
            }
            games_to_update: List[Game] = []
            start_time = time.time()
            last_report = start_time
            last_scanned_for_report = 0

            # Первичная отрисовка прогресс-бара
            self._print_progress(stats, start_time)

            for game in games:
                # Проверка лимита
                if self.limit and stats['matched'] >= self.limit:
                    stats['stopped_by_limit'] = True
                    break

                stats['scanned'] += 1
                desc: Optional[str] = None
                source: Optional[str] = None

                # 5.1 По app_id
                if self.match_by in ('app_id', 'both'):
                    app_id = found_app_ids.get(game.id)
                    if app_id and app_id in desc_by_appid:
                        desc = desc_by_appid[app_id]
                        source = f'app_id={app_id}'

                # 5.2 По нормализованному имени
                if desc is None and self.match_by in ('name', 'both'):
                    norm = normalize_name(game.name)
                    if norm in desc_by_name:
                        desc = desc_by_name[norm]
                        source = 'name'

                if desc is None:
                    stats['not_found'] += 1
                    # В файл НЕ пишем — только счётчик
                elif len(desc.strip()) < self.min_length:
                    stats['too_short'] += 1
                    # В файл НЕ пишем — только счётчик
                elif is_store_description(desc):
                    stats['skipped_store'] += 1
                    # В файл НЕ пишем — только счётчик
                else:
                    stats['matched'] += 1
                    if source and source.startswith('app_id='):
                        stats['by_app_id'] += 1
                    else:
                        stats['by_name'] += 1

                    # В файл пишем ТОЛЬКО reason=ok
                    self._write_ok_block(report_fh, game.id, game.name, source, desc)

                    if self.dry_run:
                        if self.verbose and stats['matched'] <= 20:
                            self.stdout.write(
                                f'\n   [DRY] {game.name[:50]} ← {source} ({len(desc)} симв.)'
                            )
                    else:
                        game.rawg_description = desc
                        games_to_update.append(game)

                        if len(games_to_update) >= self.batch_size:
                            self._flush_batch(games_to_update, stats)
                            games_to_update = []

                # Обновление прогресса: каждые 50 игр ИЛИ раз в 0.5с
                now = time.time()
                if (stats['scanned'] - last_scanned_for_report >= 50
                        or now - last_report >= 0.5):
                    self._print_progress(stats, start_time)
                    last_report = now
                    last_scanned_for_report = stats['scanned']

            # Финальный батч
            if games_to_update and not self.dry_run:
                self._flush_batch(games_to_update, stats)

            # Финальная отрисовка прогресс-бара (force)
            self._print_progress(stats, start_time, force=True)

            self._print_summary(stats, start_time)

        finally:
            if report_fh is not None:
                try:
                    report_fh.write(f'\n# finished: {time.strftime("%Y-%m-%d %H:%M:%S")}\n')
                    report_fh.close()
                except Exception:
                    pass

    # ------------------------------------------------------------------ #
    #  Вспомогательные методы
    # ------------------------------------------------------------------ #

    def _flush_batch(self, games: List[Game], stats: Dict) -> None:
        """Сохраняет батч в БД. Ошибки учитываются в stats['errors']."""
        try:
            with transaction.atomic():
                Game.objects.bulk_update(games, ['rawg_description'])
        except Exception as e:
            stats['errors'] += len(games)
            self.stderr.write(f'⚠️ Ошибка bulk_update ({len(games)} игр): {e}')

    def _print_summary(self, stats: Dict, start_time: float) -> None:
        elapsed = time.time() - start_time
        self.stdout.write(self.style.SUCCESS('=' * 60))
        self.stdout.write(self.style.SUCCESS('📊 ИТОГОВАЯ СТАТИСТИКА'))
        self.stdout.write('=' * 60)
        self.stdout.write(f'📦 Всего игр-кандидатов: {_fmt_int(stats["total_candidates"])}')
        self.stdout.write(f'🔍 Просканировано игр: {_fmt_int(stats["scanned"])}')
        self.stdout.write(f'✅ Найдено/заполнено описаний: {_fmt_int(stats["matched"])} '
                          f'(по app_id: {_fmt_int(stats["by_app_id"])}, '
                          f'по имени: {_fmt_int(stats["by_name"])})')
        self.stdout.write(f'🚫 Отброшено магазинных описаний: {_fmt_int(stats["skipped_store"])}')
        self.stdout.write(f'❌ Не найдено в дампе (среди просканированных): {_fmt_int(stats["not_found"])}')
        self.stdout.write(f'⏭️  Слишком короткие: {_fmt_int(stats["too_short"])}')
        self.stdout.write(f'❌ Ошибок: {_fmt_int(stats["errors"])}')
        if self.limit and stats['stopped_by_limit']:
            self.stdout.write(self.style.WARNING(
                f'⏹️  Остановлено по лимиту: достигнуто {self.limit} заполненных описаний'
            ))
        self.stdout.write(f'⏱️  Время: {elapsed:.1f}с')
        if elapsed > 0:
            self.stdout.write(f'⚡ Скорость: {stats["scanned"] / elapsed:.1f} игр/с')
        if self.dry_run:
            self.stdout.write(self.style.WARNING('\n🔍 DRY-RUN: в БД ничего не записано'))
        else:
            self.stdout.write(self.style.SUCCESS(
                f'\n✅ Обновлено в БД: {_fmt_int(stats["matched"])} игр'
            ))
        self.stdout.write('=' * 60)