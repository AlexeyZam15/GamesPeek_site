"""
Команда для добавления жанров и тем к играм из JSONL-файлов.
Поддерживает dry-run режим, отслеживание обработанных файлов и логирование изменений.
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

from django.core.management.base import BaseCommand, CommandError

from games.models import Game, Genre, Theme


class Command(BaseCommand):
    help = 'Добавляет жанры и темы к играм из JSONL-файлов в папке add_genres_themes'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Режим предварительного просмотра без сохранения изменений в БД'
        )
        parser.add_argument(
            '--file',
            type=str,
            help='Конкретный файл для обработки (если не указан, обрабатываются все .txt файлы)'
        )
        parser.add_argument(
            '--force-restart',
            action='store_true',
            help='Сбросить состояние обработанных файлов и обработать всё заново'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=200,
            help='Размер пакета для обработки (по умолчанию 200)'
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.dry_run = False
        self.force_restart = False
        self.batch_size = 100
        self.base_dir = None
        self.data_dir = None
        self.log_dir = None
        self.state_file_path = None
        self.additions_current_path = None
        self.additions_global_path = None
        self.errors_log_path = None
        self.processed_state = {}
        self.interrupted = False
        self.stats = {
            'total_games': 0,
            'updated_games': 0,
            'skipped_games': 0,
            'error_games': 0,
            'total_genres_added': 0,
            'total_themes_added': 0,
            'processed_files': [],
            'skipped_files': [],
            'errors': []
        }

    def _write_note(self, game_id: int, game_name: str, note: str):
        """Записывает информационное сообщение в лог (не ошибку)."""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(self.additions_current_path, 'a', encoding='utf-8') as f:
                f.write(f"{timestamp} | Game {game_id} | {game_name} | NOTE: {note}\n")
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Ошибка записи в лог: {e}"))

    def _signal_handler(self, signum, frame):
        """Обработчик сигнала прерывания."""
        self.interrupted = True
        self.stdout.write(self.style.WARNING("\n\nПрерывание..."))
        self._print_summary()
        sys.exit(0)

    def _setup_paths(self):
        """Настраивает пути к директориям."""
        self.base_dir = Path(os.getcwd())
        self.data_dir = self.base_dir / 'add_genres_themes'
        self.log_dir = self.data_dir / 'logs'
        self.state_file_path = self.log_dir / 'processed_files_state.json'
        self.additions_current_path = self.log_dir / 'additions_current.log'
        self.additions_global_path = self.log_dir / 'additions_global.log'
        self.errors_log_path = self.log_dir / 'errors.log'

        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def _reset_current_logs(self):
        """Очищает файлы текущей сессии."""
        for log_path in [self.additions_current_path, self.errors_log_path]:
            try:
                log_path.parent.mkdir(parents=True, exist_ok=True)
                with open(log_path, 'w', encoding='utf-8') as f:
                    f.write("")
            except Exception as e:
                self.stderr.write(self.style.ERROR(f"Ошибка очистки {log_path.name}: {e}"))

    def _write_addition(self, game_id: int, game_name: str, added_genres: List[str], added_themes: List[str]):
        """Записывает информацию о добавленных жанрах/темах в лог."""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            with open(self.additions_global_path, 'a', encoding='utf-8') as f:
                if added_genres:
                    f.write(f"{timestamp} | Game {game_id} | {game_name} | Added genres: {', '.join(added_genres)}\n")
                if added_themes:
                    f.write(f"{timestamp} | Game {game_id} | {game_name} | Added themes: {', '.join(added_themes)}\n")

            with open(self.additions_current_path, 'a', encoding='utf-8') as f:
                if added_genres:
                    f.write(f"{timestamp} | Game {game_id} | {game_name} | Added genres: {', '.join(added_genres)}\n")
                if added_themes:
                    f.write(f"{timestamp} | Game {game_id} | {game_name} | Added themes: {', '.join(added_themes)}\n")
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Ошибка записи в лог: {e}"))

    def _write_addition_dry_run(self, game_id: int, game_name: str, added_genres: List[str], added_themes: List[str]):
        """Записывает информацию о добавленных жанрах/темах только в текущий лог для dry-run."""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            with open(self.additions_current_path, 'a', encoding='utf-8') as f:
                if added_genres:
                    f.write(
                        f"{timestamp} | [DRY-RUN] Game {game_id} | {game_name} | Added genres: {', '.join(added_genres)}\n")
                if added_themes:
                    f.write(
                        f"{timestamp} | [DRY-RUN] Game {game_id} | {game_name} | Added themes: {', '.join(added_themes)}\n")
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Ошибка записи в dry-run лог: {e}"))

    def _write_error(self, game_id: int, game_name: str, error: str):
        """Записывает ошибку в лог."""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(self.errors_log_path, 'a', encoding='utf-8') as f:
                f.write(f"{timestamp} | Game {game_id} | {game_name} | ERROR: {error}\n")
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Ошибка записи в errors.log: {e}"))

    def _load_processed_state(self):
        """Загружает состояние обработанных файлов."""
        if self.state_file_path.exists():
            try:
                with open(self.state_file_path, 'r', encoding='utf-8') as f:
                    self.processed_state = json.load(f)
                self.stdout.write(f"Загружено состояние: {len(self.processed_state.get('files', {}))} файлов")
            except Exception as e:
                self.stdout.write(self.style.WARNING(f"Не удалось загрузить состояние: {e}"))
                self.processed_state = {'files': {}}
        else:
            self.processed_state = {'files': {}}

        if 'files' not in self.processed_state:
            self.processed_state['files'] = {}

    def _save_processed_state(self):
        """Сохраняет состояние обработанных файлов."""
        try:
            self.processed_state['last_updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            with open(self.state_file_path, 'w', encoding='utf-8') as f:
                json.dump(self.processed_state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Ошибка сохранения состояния: {e}"))

    def _get_file_hash(self, file_path: Path) -> str:
        """Вычисляет хеш файла."""
        hash_md5 = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Ошибка вычисления хеша: {e}"))
            return None

    def _is_file_processed(self, file_path: Path) -> Tuple[bool, Optional[str]]:
        """Проверяет, был ли файл обработан."""
        file_name = file_path.name

        if file_name not in self.processed_state['files']:
            return False, None

        file_state = self.processed_state['files'][file_name]
        current_hash = self._get_file_hash(file_path)

        if current_hash is None:
            return True, "не удалось вычислить хеш"

        if file_state.get('file_hash') != current_hash:
            return False, f"файл изменен"

        return True, f"обработан {file_state.get('processed_at')}"

    def _mark_file_processed(self, file_path: Path, games_updated: int):
        """Отмечает файл как обработанный."""
        file_name = file_path.name
        file_hash = self._get_file_hash(file_path)
        file_size = file_path.stat().st_size

        self.processed_state['files'][file_name] = {
            'processed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'file_hash': file_hash,
            'file_size': file_size,
            'games_updated': games_updated,
            'file_path': str(file_path)
        }
        self._save_processed_state()

    def _reset_state(self):
        """Сбрасывает состояние обработанных файлов."""
        if self.force_restart:
            self.stdout.write("Сброс состояния...")
            self.processed_state = {'files': {}}
            if self.state_file_path.exists():
                backup_path = self.state_file_path.with_suffix('.json.bak')
                try:
                    import shutil
                    shutil.copy2(self.state_file_path, backup_path)
                except Exception as e:
                    pass
            self._save_processed_state()

    def _get_genres_by_names(self, genre_names: List[str]) -> List[Genre]:
        """
        Получает существующие жанры по именам с регистронезависимым поиском.
        Использует нормализацию названий для соответствия формату в БД.
        """
        genres = []
        for name in genre_names:
            if not name:
                continue

            # Нормализация названия: приводим к нижнему регистру и убираем лишние пробелы
            normalized_name = name.strip()

            try:
                # Сначала пытаемся найти точное совпадение
                genre = Genre.objects.get(name=normalized_name)
                genres.append(genre)
                continue
            except Genre.DoesNotExist:
                pass

            try:
                # Если точное совпадение не найдено, ищем регистронезависимо
                genre = Genre.objects.get(name__iexact=normalized_name)
                genres.append(genre)
                continue
            except Genre.DoesNotExist:
                pass

            # Если не нашли по точному совпадению, пробуем нормализовать распространенные варианты
            # Приводим к формату, который используется в БД: "Turn-based strategy (TBS)"
            normalized_for_db = self._normalize_genre_name(normalized_name)

            if normalized_for_db != normalized_name:
                try:
                    genre = Genre.objects.get(name=normalized_for_db)
                    genres.append(genre)
                    self._write_note(0, 'System', f"Жанр '{name}' нормализован в '{normalized_for_db}'")
                    continue
                except Genre.DoesNotExist:
                    pass

            # Если все попытки не удались, логируем ошибку
            self._write_error(0, 'System', f"Жанр '{name}' не найден в БД")
            self.stats['error_games'] += 1

        return genres

    def _normalize_genre_name(self, name: str) -> str:
        """
        Нормализует название жанра к формату, принятому в базе данных.

        Примеры:
        - "Turn-based Strategy (TBS)" -> "Turn-based strategy (TBS)"
        - "Real Time Strategy (RTS)" -> "Real Time Strategy (RTS)"
        - "Hack and slash/Beat 'em up" -> "Hack and slash/Beat 'em up"
        """
        # Словарь специальных нормализаций для жанров
        genre_normalizations = {
            'turn-based strategy (tbs)': 'Turn-based strategy (TBS)',
            'turn-based strategy': 'Turn-based strategy (TBS)',
            'turn-based (tbs)': 'Turn-based strategy (TBS)',
            'turn based strategy': 'Turn-based strategy (TBS)',
            'tbs': 'Turn-based strategy (TBS)',
            'real time strategy (rts)': 'Real Time Strategy (RTS)',
            'real time strategy': 'Real Time Strategy (RTS)',
            'rts': 'Real Time Strategy (RTS)',
            'hack and slash/beat \'em up': 'Hack and slash/Beat \'em up',
            'hack and slash': 'Hack and slash/Beat \'em up',
            'beat \'em up': 'Hack and slash/Beat \'em up',
            'role-playing (rpg)': 'Role-playing (RPG)',
            'role playing game': 'Role-playing (RPG)',
            'rpg': 'Role-playing (RPG)',
        }

        # Приводим к нижнему регистру для сравнения
        lower_name = name.lower().strip()

        # Проверяем, есть ли нормализация для этого названия
        if lower_name in genre_normalizations:
            return genre_normalizations[lower_name]

        # Для остальных жанров: первая буква каждого слова заглавная, остальные строчные
        # Но сохраняем скобки и их содержимое как есть
        words = name.split()
        normalized_words = []
        in_parentheses = False
        parentheses_content = []

        for word in words:
            if '(' in word:
                in_parentheses = True
                # Начинаем собирать содержимое скобок
                parentheses_content.append(word)
            elif ')' in word:
                in_parentheses = False
                parentheses_content.append(word)
                # Добавляем все содержимое скобок как есть
                normalized_words.append(' '.join(parentheses_content))
                parentheses_content = []
            elif in_parentheses:
                parentheses_content.append(word)
            else:
                # Капитализируем первую букву каждого слова
                normalized_words.append(word.capitalize())

        return ' '.join(normalized_words)

    def _get_themes_by_names(self, theme_names: List[str]) -> List[Theme]:
        """
        Получает существующие темы по именам с регистронезависимым поиском.
        Использует нормализацию названий для соответствия формату в БД.
        """
        themes = []
        for name in theme_names:
            if not name:
                continue

            # Нормализация названия: приводим к нижнему регистру и убираем лишние пробелы
            normalized_name = name.strip()

            try:
                # Сначала пытаемся найти точное совпадение
                theme = Theme.objects.get(name=normalized_name)
                themes.append(theme)
                continue
            except Theme.DoesNotExist:
                pass

            try:
                # Если точное совпадение не найдено, ищем регистронезависимо
                theme = Theme.objects.get(name__iexact=normalized_name)
                themes.append(theme)
                continue
            except Theme.DoesNotExist:
                pass

            # Если не нашли по точному совпадению, пробуем нормализовать распространенные варианты
            normalized_for_db = self._normalize_theme_name(normalized_name)

            if normalized_for_db != normalized_name:
                try:
                    theme = Theme.objects.get(name=normalized_for_db)
                    themes.append(theme)
                    self._write_note(0, 'System', f"Тема '{name}' нормализована в '{normalized_for_db}'")
                    continue
                except Theme.DoesNotExist:
                    pass

            # Если все попытки не удались, логируем ошибку
            self._write_error(0, 'System', f"Тема '{name}' не найдена в БД")
            self.stats['error_games'] += 1

        return themes

    def _normalize_theme_name(self, name: str) -> str:
        """
        Нормализует название темы к формату, принятому в базе данных.

        Примеры:
        - "4x" -> "4X (explore, expand, exploit, and exterminate)"
        - "stealth" -> "Stealth"
        - "science fiction" -> "Science fiction"
        """
        # Словарь специальных нормализаций для тем
        theme_normalizations = {
            '4x': '4X (explore, expand, exploit, and exterminate)',
            '4x (explore, expand, exploit, and exterminate)': '4X (explore, expand, exploit, and exterminate)',
            'stealth': 'Stealth',
            'science fiction': 'Science fiction',
            'sci-fi': 'Science fiction',
            'post apocalyptic': 'Post-apocalyptic',
            'postapocalyptic': 'Post-apocalyptic',
            'post-apocalyptic': 'Post-apocalyptic',
            'fantasy': 'Fantasy',
            'horror': 'Horror',
            'mystery': 'Mystery',
            'thriller': 'Thriller',
            'historical': 'Historical',
            'medieval': 'Medieval',
            'gothic': 'Gothic',
            'warfare': 'Warfare',
            'comedy': 'Comedy',
            'drama': 'Drama',
            'romance': 'Romance',
            'educational': 'Educational',
            'erotic': 'Erotic',
            'party': 'Party',
            'kids': 'Kids',
            'indie': 'Indie',
            'business': 'Business',
            'non-fiction': 'Non-fiction',
            'non fiction': 'Non-fiction',
            'crafting & gathering': 'Crafting & Gathering',
            'crafting and gathering': 'Crafting & Gathering',
            'fire emblem': 'Fire Emblem',
            'fire emblem-like': 'Fire Emblem',
        }

        # Приводим к нижнему регистру для сравнения
        lower_name = name.lower().strip()

        # Проверяем, есть ли нормализация для этого названия
        if lower_name in theme_normalizations:
            return theme_normalizations[lower_name]

        # Для остальных тем: первая буква заглавная, остальные строчные
        # При этом сохраняем специальные символы и скобки
        if name.isupper():
            return name.capitalize()

        return name.capitalize()

    def _update_game_genres_themes(self, game: Game, new_genres: List[Genre],
                                   new_themes: List[Theme], dry_run: bool) -> Tuple[bool, List[str], List[str]]:
        """Обновляет связи игры."""
        current_genre_names = {g.name for g in game.genres.all()}
        current_theme_names = {t.name for t in game.themes.all()}

        new_genre_names = {g.name for g in new_genres}
        new_theme_names = {t.name for t in new_themes}

        added_genre_names = list(new_genre_names - current_genre_names)
        added_theme_names = list(new_theme_names - current_theme_names)

        if not added_genre_names and not added_theme_names:
            return False, [], []

        if dry_run:
            return True, added_genre_names, added_theme_names

        if added_genre_names:
            genres_to_add = Genre.objects.filter(name__in=added_genre_names)
            game.genres.add(*genres_to_add)

        if added_theme_names:
            themes_to_add = Theme.objects.filter(name__in=added_theme_names)
            game.themes.add(*themes_to_add)

        return True, added_genre_names, added_theme_names

    def _process_game_data(self, game_data: Dict, dry_run: bool) -> Dict:
        """Обрабатывает данные одной игры."""
        result = {
            'success': False,
            'game_id': game_data.get('id'),
            'game_name': game_data.get('name', 'Unknown'),
            'genres': game_data.get('genres', []),
            'themes': game_data.get('themes', []),
            'added_genres': [],
            'added_themes': [],
            'error': None
        }

        try:
            game_id = result['game_id']
            if not game_id:
                result['error'] = "Отсутствует поле id"
                self._write_error(0, 'Unknown', result['error'])
                self.stats['error_games'] += 1
                return result

            try:
                game = Game.objects.get(igdb_id=game_id)
            except Game.DoesNotExist:
                result['error'] = f"Игра не найдена"
                self._write_error(game_id, result['game_name'], result['error'])
                self.stats['error_games'] += 1
                return result

            result['game_name'] = game.name

            # Сохраняем текущее количество ошибок до обработки жанров/тем
            errors_before = self.stats['error_games']

            # Разделяем жанры и темы по их реальному назначению
            # Создаем множества для быстрого поиска
            genres_set = set([
                'Action', 'Adventure', 'Arcade', 'Base Building', 'Card & Board Game',
                'Fighting', 'Hack and slash/Beat \'em up', 'MOBA', 'Music', 'Open World',
                'Pinball', 'Platform', 'Point-and-click', 'Precision Combat', 'Puzzle',
                'Quiz/Trivia', 'Racing', 'Real Time Strategy (RTS)', 'Role-playing (RPG)',
                'Sandbox', 'Shooter', 'Simulator', 'Sport', 'Squad Management', 'Strategy',
                'Survival', 'Tactical', 'Turn-based', 'Turn-based strategy (TBS)',
                'Visual Novel'
            ])

            themes_set = set([
                '4X (explore, expand, exploit, and exterminate)', 'Business', 'Comedy',
                'Crafting & Gathering', 'Drama', 'Educational', 'Erotic', 'Fantasy',
                'Fire Emblem', 'Gothic', 'Historical', 'Horror', 'Indie', 'Kids',
                'Medieval', 'Mystery', 'Non-fiction', 'Party', 'Post-apocalyptic',
                'Romance', 'Science fiction', 'Stealth', 'Thriller', 'Warfare'
            ])

            # Разделяем полученные названия
            actual_genres = []
            actual_themes = []

            # Обрабатываем все названия из поля 'genres' в JSONL
            for name in result['genres']:
                if name in genres_set:
                    actual_genres.append(name)
                elif name in themes_set:
                    actual_themes.append(name)
                else:
                    # Если название не найдено ни в одном списке, пробуем определить по контексту
                    # Например, 'Stealth' точно тема, а не жанр
                    if name.lower() == 'stealth':
                        actual_themes.append(name)
                    elif name.lower() in ['turn-based strategy (tbs)', 'real time strategy (rts)']:
                        actual_genres.append(name)
                    else:
                        self._write_note(0, 'System', f"Неопределенная категория '{name}' - добавляется как жанр")
                        actual_genres.append(name)

            # Обрабатываем все названия из поля 'themes' в JSONL
            for name in result['themes']:
                if name in themes_set:
                    actual_themes.append(name)
                elif name in genres_set:
                    actual_genres.append(name)
                else:
                    # Если название не найдено ни в одном списке
                    if name.lower() == 'stealth':
                        actual_themes.append(name)
                    else:
                        self._write_note(0, 'System', f"Неопределенная категория '{name}' - добавляется как тема")
                        actual_themes.append(name)

            # Получаем объекты жанров и тем
            genres = self._get_genres_by_names(actual_genres)
            themes = self._get_themes_by_names(actual_themes)

            # Вычисляем количество новых ошибок от ненайденных жанров/тем
            new_errors = self.stats['error_games'] - errors_before

            has_changes, added_genres, added_themes = self._update_game_genres_themes(
                game, genres, themes, dry_run
            )

            result['added_genres'] = added_genres
            result['added_themes'] = added_themes

            if has_changes:
                result['success'] = True
                self.stats['total_genres_added'] += len(added_genres)
                self.stats['total_themes_added'] += len(added_themes)

                if dry_run:
                    self._write_addition_dry_run(game_id, game.name, added_genres, added_themes)
                else:
                    self._write_addition(game_id, game.name, added_genres, added_themes)
            else:
                self.stats['skipped_games'] += 1

            # Если были ошибки с жанрами/темами, считаем игру как с ошибкой
            if new_errors > 0:
                self.stats['error_games'] += 1
                result['error'] = f"Пропущены ненайденные жанры/темы ({new_errors} шт.)"

            return result

        except Exception as e:
            result['error'] = str(e)
            self._write_error(result['game_id'], result['game_name'], str(e))
            self.stats['errors'].append({
                'game_id': result['game_id'],
                'game_name': result['game_name'],
                'error': str(e)
            })
            self.stats['error_games'] += 1
            return result

    def _process_file(self, file_path: Path, dry_run: bool) -> Dict:
        """Обрабатывает один файл пакетами."""
        file_start_time = time.time()

        file_stats = {
            'file_name': file_path.name,
            'total_lines': 0,
            'valid_lines': 0,
            'error_lines': 0,
            'updated_games': 0,
            'skipped_games': 0,
            'genres_added': 0,
            'themes_added': 0
        }

        self.stdout.write(f"\nОбработка файла: {file_path.name}")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = [l.strip() for l in f if l.strip()]

            file_stats['total_lines'] = len(lines)
            self.stdout.write(f"Всего игр: {file_stats['total_lines']}")

            batches = [lines[i:i + self.batch_size] for i in range(0, len(lines), self.batch_size)]
            total_batches = len(batches)

            # Сбрасываем локальные счетчики для этого файла
            file_updated = 0
            file_skipped = 0
            file_errors = 0

            # Сохраняем глобальные счетчики для вычисления разницы
            global_updated_before = self.stats['updated_games']
            global_skipped_before = self.stats['skipped_games']
            global_errors_before = self.stats['error_games']

            for batch_num, batch in enumerate(batches, 1):
                if self.interrupted:
                    break

                for line in batch:
                    try:
                        game_data = json.loads(line)
                        file_stats['valid_lines'] += 1

                        result = self._process_game_data(game_data, dry_run)

                    except json.JSONDecodeError as e:
                        self._write_error(0, file_path.name, f"Строка: {e}")
                        self.stats['error_games'] += 1

                # Обновляем локальные счетчики
                file_updated = self.stats['updated_games'] - global_updated_before
                file_skipped = self.stats['skipped_games'] - global_skipped_before
                file_errors = self.stats['error_games'] - global_errors_before

                # Обновляем file_stats
                file_stats['updated_games'] = file_updated
                file_stats['skipped_games'] = file_skipped
                file_stats['error_lines'] = file_errors

                # Вычисляем прошедшее время
                elapsed = time.time() - file_start_time
                elapsed_str = f"{int(elapsed // 60)}:{int(elapsed % 60):02d}"

                # Выводим прогресс
                progress = int(batch_num / total_batches * 20)
                bar = "█" * progress + "░" * (20 - progress)
                self.stdout.write(
                    f"\r{batch_num}/{total_batches} {bar} ✅ {file_updated} ⏭️ {file_skipped} ❌ {file_errors} [{elapsed_str}]",
                    ending='')

            self.stdout.write("")

            # Итоговое время для файла
            total_elapsed = time.time() - file_start_time
            self.stdout.write(f"Время обработки файла: {int(total_elapsed // 60)}:{int(total_elapsed % 60):02d}")

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"Ошибка: {e}"))
            file_stats['error_lines'] = file_stats['total_lines']

        return file_stats

    def _get_files_to_process(self, specific_file: str = None) -> List[Path]:
        """Получает список файлов для обработки."""
        files_to_process = []

        if specific_file:
            file_path = self.data_dir / specific_file
            if not file_path.exists():
                raise CommandError(f"Файл не найден: {specific_file}")

            is_processed, reason = self._is_file_processed(file_path)
            if is_processed and not self.force_restart:
                self.stdout.write(self.style.WARNING(f"Пропущен {specific_file}: {reason}"))
                self.stats['skipped_files'].append({'name': specific_file, 'reason': reason})
                return []
            else:
                files_to_process.append(file_path)
        else:
            all_files = list(self.data_dir.glob('*.txt'))
            if not all_files:
                raise CommandError(f"Нет .txt файлов в {self.data_dir}")

            for file_path in sorted(all_files):
                is_processed, reason = self._is_file_processed(file_path)

                if is_processed and not self.force_restart:
                    self.stdout.write(self.style.WARNING(f"Пропущен {file_path.name}: {reason}"))
                    self.stats['skipped_files'].append({'name': file_path.name, 'reason': reason})
                else:
                    files_to_process.append(file_path)

        return files_to_process

    def _print_summary(self):
        """Выводит итоговую статистику."""
        self.stdout.write(self.style.SUCCESS("\n" + "=" * 50))
        self.stdout.write(self.style.SUCCESS("ИТОГОВАЯ СТАТИСТИКА"))
        self.stdout.write(self.style.SUCCESS("=" * 50))

        self.stdout.write(f"Обработано игр: {self.stats['total_games']}")
        self.stdout.write(f"Обновлено игр: {self.stats['updated_games']}")
        self.stdout.write(f"Пропущено игр (нет новых данных): {self.stats['skipped_games']}")
        self.stdout.write(f"Ошибок: {self.stats['error_games']}")
        self.stdout.write(f"Добавлено жанров: {self.stats['total_genres_added']}")
        self.stdout.write(f"Добавлено тем: {self.stats['total_themes_added']}")

        if self.stats['skipped_files']:
            self.stdout.write(self.style.WARNING(f"\nПропущено файлов: {len(self.stats['skipped_files'])}"))

        if self.stats['processed_files']:
            self.stdout.write(self.style.SUCCESS(f"\nОбработано файлов: {len(self.stats['processed_files'])}"))
            for f in self.stats['processed_files']:
                self.stdout.write(f"  - {f['name']}: {f['games_updated']} игр")

        if self.stats['errors']:
            self.stdout.write(self.style.ERROR(f"\nОшибок: {len(self.stats['errors'])}"))

        self.stdout.write(f"\nЛог текущей сессии: {self.additions_current_path}")
        self.stdout.write(f"Лог всех добавлений: {self.additions_global_path}")
        self.stdout.write(f"Лог ошибок: {self.errors_log_path}")

        if self.dry_run:
            self.stdout.write(self.style.WARNING("\n[DRY-RUN] Изменения не сохранены"))

    def handle(self, *args, **options):
        """Основной метод."""
        signal.signal(signal.SIGINT, self._signal_handler)

        self.dry_run = options['dry_run']
        self.force_restart = options['force_restart']
        self.batch_size = options['batch_size']
        specific_file = options.get('file')

        self._setup_paths()

        # Очищаем логи ВСЕГДА
        self.stdout.write("\nОчистка логов...")
        self._reset_current_logs()

        if self.dry_run:
            self.stdout.write(self.style.WARNING("\n[DRY-RUN] Режим просмотра, изменения в БД не сохраняются"))

        self._load_processed_state()

        if self.force_restart:
            self._reset_state()

        self.stdout.write("=" * 50)
        self.stdout.write("НАЧАЛО ОБРАБОТКИ")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Режим: {'DRY-RUN' if self.dry_run else 'REAL'}")
        self.stdout.write(f"Размер пакета: {self.batch_size}")

        try:
            files_to_process = self._get_files_to_process(specific_file)
        except CommandError as e:
            self.stderr.write(self.style.ERROR(str(e)))
            return

        if not files_to_process:
            self.stdout.write(self.style.WARNING("\nНет файлов для обработки"))
            self._print_summary()
            return

        self.stdout.write(f"\nНайдено файлов: {len(files_to_process)}")

        for file_path in files_to_process:
            if self.interrupted:
                break

            file_stats = self._process_file(file_path, self.dry_run)

            self.stats['total_games'] += file_stats['valid_lines']
            self.stats['updated_games'] += file_stats['updated_games']
            self.stats['skipped_games'] += file_stats['skipped_games']
            self.stats['error_games'] += file_stats['error_lines']
            self.stats['total_genres_added'] += file_stats['genres_added']
            self.stats['total_themes_added'] += file_stats['themes_added']

            self.stats['processed_files'].append({
                'name': file_path.name,
                'games_updated': file_stats['updated_games'],
                'games_total': file_stats['valid_lines']
            })

            self.stdout.write(self.style.SUCCESS(
                f"\nФайл {file_path.name}: ✅ {file_stats['updated_games']} ⏭️ {file_stats['skipped_games']} ❌ {file_stats['error_lines']}"
            ))

            if not self.dry_run and not self.interrupted:
                self._mark_file_processed(file_path, file_stats['updated_games'])

        self._print_summary()