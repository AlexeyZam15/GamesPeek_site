# games/management/commands/analyze_game_criteria_fast.py
"""
МАКСИМАЛЬНО УСКОРЕННАЯ команда анализа ТОЛЬКО критериев
Использует все возможные оптимизации: многопоточность, bulk_create, предзагрузку
"""

import sys
import os
import time
import threading
import signal
from typing import Dict, Any, List, Optional, Set, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.management.base import BaseCommand
from django.db import transaction
from django.db.models import Q

from games.models import Game, Genre, Theme, PlayerPerspective, GameMode
from games.analyze.range_cache import RangeCacheManager
from games.analyze.pattern_manager import PatternManager


class Command(BaseCommand):
    """
    МАКСИМАЛЬНО УСКОРЕННАЯ команда анализа критериев

    Оптимизации:
    1. Предзагрузка ВСЕХ паттернов в память (один раз)
    2. Предзагрузка ВСЕХ критериев из БД в память (id, name)
    3. Многопоточная обработка с пулом потоков
    4. Массовое обновление через bulk_create (одна транзакция)
    5. Кэширование через RangeCacheManager
    6. Минимизация обращений к БД

    Примеры:
        python manage.py analyze_game_criteria_fast --update-game --threads 16
        python manage.py analyze_game_criteria_fast --limit 100000 --threads 12 --batch-save 10000
        python manage.py analyze_game_criteria_fast --genre "Grid-Based" --auto-save
        python manage.py analyze_game_criteria_fast --theme "Fantasy" --auto-save
        python manage.py analyze_game_criteria_fast --genre "Grid-Based" --theme "Fantasy" --auto-save
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.progress_bar = None
        self.output_file = None
        self.original_stdout = None
        self.limit = None
        self.offset = 0
        self.game_name = None
        self.genre = None
        self.filter_genre = None
        self.theme = None
        self.verbose = False
        self.threads = 16
        self.batch_save = 10000
        self.force_restart = False
        self.output_path = None
        self.only_found = False
        self.combine_all_texts = False
        self.no_progress = False
        self.custom_pattern = None
        self.patterns = None
        self.criteria_cache = None
        self.criteria_by_name = None
        self.compiled_patterns = None
        self._interrupted = False

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=None,
                            help='Максимальное количество игр для анализа')
        parser.add_argument('--offset', type=int, default=0,
                            help='Пропустить первые N игр')
        parser.add_argument('--game-name', type=str, default=None,
                            help='Название игры для анализа (поиск по частичному совпадению)')
        parser.add_argument('--genre', type=str, default=None,
                            help='Анализировать ТОЛЬКО этот жанр (искать паттерны только для указанного жанра)')
        parser.add_argument('--filter-genre', type=str, default=None,
                            help='Фильтровать игры, у которых уже есть этот жанр (анализировать только игры с этим жанром)')
        parser.add_argument('--theme', type=str, default=None,
                            help='Анализировать ТОЛЬКО эту тему (искать паттерны только для указанной темы)')
        parser.add_argument('--verbose', action='store_true',
                            help='Подробный вывод')
        parser.add_argument('--threads', type=int, default=24,
                            help='Количество потоков для обработки (по умолчанию 24)')
        parser.add_argument('--force-restart', action='store_true',
                            help='Начать обработку заново, игнорируя кэш')
        parser.add_argument('--output', type=str,
                            help='Путь к файлу для экспорта результатов')
        parser.add_argument('--only-found', action='store_true',
                            help='Показывать только игры где были найдены критерии')
        parser.add_argument('--no-progress', action='store_true',
                            help='Отключить отображение прогресс-бара')
        parser.add_argument('--auto-save', action='store_true',
                            help='Автоматически сохранять в БД без подтверждения')
        parser.add_argument('--no-combine-texts', action='store_true',
                            help='НЕ объединять все источники текста (использовать только summary)')
        parser.add_argument('--custom-pattern', type=str, default=None,
                            help='Пользовательский паттерн для поиска (будет добавлен к указанному жанру)')

    def _filter_games_by_genre(self, games: List[Dict]) -> List[Dict]:
        """Фильтрует игры, у которых уже есть указанный жанр"""
        if not self.filter_genre:
            return games

        sys.stderr.write(f"\n   🎭 Фильтрация игр по жанру: '{self.filter_genre}'\n")
        sys.stderr.flush()

        # Получаем ID жанра
        try:
            genre_obj = Genre.objects.filter(name__iexact=self.filter_genre).first()
            if not genre_obj:
                sys.stderr.write(f"   ⚠️ Жанр '{self.filter_genre}' не найден в БД\n")
                sys.stderr.flush()
                return []

            genre_id = genre_obj.id
            sys.stderr.write(f"   🔍 ID жанра: {genre_id}\n")
            sys.stderr.flush()

            # Получаем ID игр с этим жанром через сырой SQL для скорости
            from django.db import connection
            game_ids = set()
            with connection.cursor() as cursor:
                cursor.execute("""
                               SELECT game_id
                               FROM games_game_genres
                               WHERE genre_id = %s
                               """, [genre_id])
                for row in cursor.fetchall():
                    game_ids.add(row[0])

            sys.stderr.write(f"   📊 Найдено игр с жанром '{self.filter_genre}': {len(game_ids)}\n")
            sys.stderr.flush()

            # Фильтруем переданные игры
            filtered_games = [g for g in games if g['id'] in game_ids]

            sys.stderr.write(f"   ✅ Отфильтровано: {len(filtered_games)} игр из {len(games)}\n")
            sys.stderr.flush()

            return filtered_games

        except Exception as e:
            sys.stderr.write(f"   ⚠️ Ошибка фильтрации по жанру: {e}\n")
            sys.stderr.flush()
            return games

    def _find_pattern_and_context_for_criteria(self, pattern_info: Dict, criteria_type: str, criteria_id: int,
                                               criteria_name: str) -> Optional[Dict]:
        """Находит паттерн и контекст для критерия"""
        if criteria_type not in pattern_info:
            return None

        matches_for_category = pattern_info.get(criteria_type, [])
        if not matches_for_category:
            return None

        criteria_name_lower = criteria_name.lower()

        for match in matches_for_category:
            if match.get('status') != 'found':
                continue

            match_name = match.get('name', '').lower()
            matched_text = match.get('matched_text', '').lower()
            pattern = match.get('pattern', '')
            context = match.get('context', '')
            original_matched_text = match.get('matched_text', '')

            # Сравниваем по имени или по совпавшему тексту
            if match_name == criteria_name_lower or matched_text == criteria_name_lower:
                return {
                    'pattern': pattern,
                    'matched_text': original_matched_text,
                    'context': context
                }

        return None

    def _load_existing_relations(self) -> Dict[str, set]:
        """Загружает все существующие связи из БД для быстрой проверки"""
        from django.db import connection
        from games.models import Game, Genre, Theme, PlayerPerspective, GameMode

        games_genres_table = Game.genres.through._meta.db_table
        games_themes_table = Game.themes.through._meta.db_table
        games_perspectives_table = Game.player_perspectives.through._meta.db_table
        games_modes_table = Game.game_modes.through._meta.db_table

        existing_relations = {
            'genres': set(),
            'themes': set(),
            'perspectives': set(),
            'game_modes': set()
        }

        with connection.cursor() as cursor:
            cursor.execute(f"SELECT game_id, genre_id FROM {games_genres_table}")
            for row in cursor.fetchall():
                existing_relations['genres'].add((row[0], row[1]))

            cursor.execute(f"SELECT game_id, theme_id FROM {games_themes_table}")
            for row in cursor.fetchall():
                existing_relations['themes'].add((row[0], row[1]))

            cursor.execute(f"SELECT game_id, playerperspective_id FROM {games_perspectives_table}")
            for row in cursor.fetchall():
                existing_relations['perspectives'].add((row[0], row[1]))

            cursor.execute(f"SELECT game_id, gamemode_id FROM {games_modes_table}")
            for row in cursor.fetchall():
                existing_relations['game_modes'].add((row[0], row[1]))

        return existing_relations

    def _confirm_save(self, count: int, results_count: int, elements_count: int) -> bool:
        """Запрашивает подтверждение сохранения с принудительной синхронизацией"""
        import sys
        import os

        # Показываем информацию
        sys.stderr.write(f"\n💾 Готово к сохранению в БД: {count} игр\n")
        sys.stderr.write(f"   🎯 Игр с новыми критериями: {results_count}\n")
        sys.stderr.write(f"   📈 Всего элементов: {elements_count}\n")
        sys.stderr.flush()

        if self.output_path:
            sys.stderr.write(f"   📄 Результаты сохранены в: {self.output_path}\n")
            sys.stderr.flush()

        # Авто-сохранение
        if self.auto_save:
            sys.stderr.write("\n✅ Авто-сохранение включено\n")
            sys.stderr.flush()
            return True

        # Принудительно отключаем буферизацию для stdin
        sys.stderr.write("\n⚠️ Сохранить результаты в БД? (y/N): ")
        sys.stderr.flush()

        # Очищаем буфер stdin перед чтением
        try:
            # Пробуем прочитать все ожидающие данные из stdin
            import select
            if select.select([sys.stdin], [], [], 0)[0]:
                sys.stdin.read()
        except:
            pass

        try:
            # Читаем ответ с таймаутом
            answer = sys.stdin.readline().strip().lower()
        except (EOFError, KeyboardInterrupt, OSError):
            sys.stderr.write("\n")
            sys.stderr.flush()
            return False

        if answer in ['y', 'yes', 'да', 'д', '1']:
            sys.stderr.write("\n✅ Сохраняем...\n")
            sys.stderr.flush()
            return True

        # Только один вывод при отмене
        sys.stderr.write("\n⏭️ Сохранение отменено\n")
        sys.stderr.flush()
        return False

    def _preload_all_data(self):
        """Предзагрузка ВСЕХ данных в память с максимальной оптимизацией и фильтрацией по жанру/теме"""
        import re
        from collections import defaultdict

        self.patterns = PatternManager.get_all_patterns()

        self.criteria_by_id = {
            'genres': {},
            'themes': {},
            'perspectives': {},
            'game_modes': {}
        }

        self.criteria_by_name = {
            'genres': {},
            'themes': {},
            'perspectives': {},
            'game_modes': {}
        }

        # Загружаем данные с фильтрацией по жанру (если указан)
        if self.genre:
            sys.stderr.write(f"   🎭 Фильтрация: загружаем ТОЛЬКО жанр '{self.genre}'\n")
            sys.stderr.flush()
            genres = Genre.objects.filter(name__iexact=self.genre).only('id', 'name')
            for g in genres:
                self.criteria_by_id['genres'][g.id] = g.name
                self.criteria_by_name['genres'][g.name.lower()] = g.id
            if not self.criteria_by_id['genres']:
                sys.stderr.write(f"   ⚠️ Жанр '{self.genre}' не найден в БД\n")
                sys.stderr.flush()
        else:
            for g in Genre.objects.all().only('id', 'name'):
                self.criteria_by_id['genres'][g.id] = g.name
                self.criteria_by_name['genres'][g.name.lower()] = g.id

        # Загружаем данные с фильтрацией по теме (если указана)
        if self.theme:
            sys.stderr.write(f"   📚 Фильтрация: загружаем ТОЛЬКО тему '{self.theme}'\n")
            sys.stderr.flush()
            themes = Theme.objects.filter(name__iexact=self.theme).only('id', 'name')
            for t in themes:
                self.criteria_by_id['themes'][t.id] = t.name
                self.criteria_by_name['themes'][t.name.lower()] = t.id
            if not self.criteria_by_id['themes']:
                sys.stderr.write(f"   ⚠️ Тема '{self.theme}' не найдена в БД\n")
                sys.stderr.flush()
        else:
            for t in Theme.objects.all().only('id', 'name'):
                self.criteria_by_id['themes'][t.id] = t.name
                self.criteria_by_name['themes'][t.name.lower()] = t.id

        # Перспективы и режимы всегда загружаем полностью
        for p in PlayerPerspective.objects.all().only('id', 'name'):
            self.criteria_by_id['perspectives'][p.id] = p.name
            self.criteria_by_name['perspectives'][p.name.lower()] = p.id

        for m in GameMode.objects.all().only('id', 'name'):
            self.criteria_by_id['game_modes'][m.id] = m.name
            self.criteria_by_name['game_modes'][m.name.lower()] = m.id

        # Компилируем паттерны в плоский список для быстрого доступа
        self.compiled_patterns = []

        # Определяем, какие типы критериев нужно обрабатывать
        criteria_types_to_process = []
        if self.genre:
            criteria_types_to_process.append(('genres', self.genre))
        if self.theme:
            criteria_types_to_process.append(('themes', self.theme))

        # Если не указаны фильтры, обрабатываем все типы
        if not criteria_types_to_process:
            criteria_types_to_process = [('genres', None), ('themes', None), ('perspectives', None),
                                         ('game_modes', None)]

        for criteria_type, filter_name in criteria_types_to_process:
            # Получаем паттерны для этого типа
            type_patterns = self.patterns.get(criteria_type, {})

            # Если есть фильтр по имени, берем только паттерны для конкретного имени
            if filter_name:
                filter_name_lower = filter_name.lower()
                # Ищем точное совпадение имени в паттернах
                matched_name = None
                for name in type_patterns.keys():
                    if name.lower() == filter_name_lower:
                        matched_name = name
                        break

                if matched_name:
                    patterns_dict = {matched_name: type_patterns[matched_name]}
                    sys.stderr.write(
                        f"   🔍 Найдены паттерны для '{matched_name}': {len(type_patterns[matched_name])} шт.\n")
                    sys.stderr.flush()
                else:
                    patterns_dict = {}
                    sys.stderr.write(f"   ⚠️ Паттерны для '{filter_name}' не найдены\n")
                    sys.stderr.flush()
            else:
                patterns_dict = type_patterns

            # Добавляем пользовательский паттерн если указан
            if self.custom_pattern and criteria_type == 'genres' and filter_name:
                sys.stderr.write(f"   🔧 Добавляем пользовательский паттерн: '{self.custom_pattern}'\n")
                sys.stderr.flush()

                # Создаем копию словаря с добавленным паттерном
                patterns_dict = dict(patterns_dict)
                if filter_name in patterns_dict:
                    patterns_dict[filter_name] = list(patterns_dict[filter_name]) + [self.custom_pattern]
                else:
                    patterns_dict[filter_name] = [self.custom_pattern]

            # Компилируем паттерны
            for name, patterns in patterns_dict.items():
                name_lower = name.lower()
                criteria_id = self.criteria_by_name.get(criteria_type, {}).get(name_lower)
                if criteria_id:
                    for pattern_str in patterns:
                        try:
                            if hasattr(pattern_str, 'search'):
                                compiled = pattern_str
                                is_case_sensitive = compiled.flags & re.IGNORECASE == 0
                            else:
                                # Компилируем с поддержкой регистра через префикс (?c)
                                if isinstance(pattern_str, str) and pattern_str.startswith('(?c)'):
                                    actual_pattern = pattern_str[4:].lstrip()
                                    compiled = re.compile(actual_pattern, re.UNICODE)
                                    is_case_sensitive = True
                                else:
                                    compiled = re.compile(pattern_str, re.IGNORECASE | re.UNICODE)
                                    is_case_sensitive = False

                            self.compiled_patterns.append({
                                'type': criteria_type,
                                'id': criteria_id,
                                'name': name,
                                'pattern': compiled,
                                'pattern_str': pattern_str if isinstance(pattern_str, str) else pattern_str.pattern,
                                'is_case_sensitive': is_case_sensitive
                            })
                        except Exception as e:
                            continue

        sys.stderr.write(f"   ✅ Скомпилировано паттернов: {len(self.compiled_patterns)}\n")
        sys.stderr.flush()

    def _get_games_to_analyze(self) -> List[Dict]:
        """Максимально быстрая загрузка игр через сырой SQL или по имени"""
        import sys
        from django.db import connection

        # ПРОВЕРКА: если указано имя игры, используем поиск по имени
        if self.game_name:
            sys.stderr.write(f"\n   🔍 Поиск игр по имени: '{self.game_name}'\n")
            sys.stderr.flush()

            search_term = f"%{self.game_name}%"
            fields = ['id', 'name', 'summary', 'storyline', 'rawg_description', 'wiki_description']
            fields_str = ', '.join(fields)

            sql = f"""
                SELECT {fields_str}
                FROM games_game
                WHERE name ILIKE %s
                ORDER BY 
                    CASE WHEN name ILIKE %s THEN 1 ELSE 2 END,
                    rating DESC NULLS LAST,
                    id
                LIMIT %s
                OFFSET %s
            """

            actual_limit = self.limit if self.limit else 50
            actual_offset = self.offset if self.offset else 0

            games = []
            with connection.cursor() as cursor:
                exact_term = self.game_name
                cursor.execute(sql, [search_term, exact_term, actual_limit, actual_offset])

                for row in cursor.fetchall():
                    games.append({
                        'id': row[0],
                        'name': row[1],
                        'summary': row[2],
                        'storyline': row[3],
                        'rawg_description': row[4],
                        'wiki_description': row[5]
                    })

            sys.stderr.write(f"   ✅ Найдено игр по имени: {len(games)}\n")
            sys.stderr.flush()
            return games

        # Массовая загрузка всех игр
        sys.stderr.write(f"\n   📊 Подсчет количества игр... ")
        sys.stderr.flush()

        with connection.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM games_game")
            total_count = cursor.fetchone()[0]

        sys.stderr.write(f"{total_count} игр\n")
        sys.stderr.flush()

        if self.limit:
            actual_limit = self.limit
        else:
            actual_limit = total_count - self.offset if self.offset > 0 else total_count

        sys.stderr.write(f"   🎯 Будет загружено: {actual_limit} игр\n")
        sys.stderr.flush()

        fields = ['id', 'name', 'summary', 'storyline', 'rawg_description', 'wiki_description']
        fields_str = ', '.join(fields)

        sql = f"""
            SELECT {fields_str}
            FROM games_game
            ORDER BY id
            OFFSET %s
            LIMIT %s
        """

        games = []
        batch_size = 20000

        sys.stderr.write(f"\n   ⬇️ Загрузка батчами по {batch_size} записей (сырой SQL)...\n")
        sys.stderr.flush()

        for offset in range(0, actual_limit, batch_size):
            current_limit = min(batch_size, actual_limit - offset)
            current_offset = self.offset + offset

            with connection.cursor() as cursor:
                cursor.execute(sql, [current_offset, current_limit])

                batch = []
                for row in cursor.fetchall():
                    batch.append({
                        'id': row[0],
                        'name': row[1],
                        'summary': row[2],
                        'storyline': row[3],
                        'rawg_description': row[4],
                        'wiki_description': row[5]
                    })

                games.extend(batch)

            processed = len(games)
            percent = (processed / actual_limit) * 100 if actual_limit > 0 else 0

            sys.stderr.write(f"\r   ⬇️ Загружено: {processed}/{actual_limit} игр ({percent:.1f}%)")
            sys.stderr.flush()

        sys.stderr.write(f"\n   ✅ Загружено {len(games)} игр (сырой SQL, {batch_size} записей/батч)\n")
        sys.stderr.flush()

        return games

    def _get_game_text(self, game: Game) -> str:
        """Максимально быстрое получение текста"""
        if self.combine_all_texts:
            parts = []
            if game.summary:
                parts.append(game.summary)
            if game.storyline:
                parts.append(game.storyline)
            if game.rawg_description:
                parts.append(game.rawg_description)
            if game.wiki_description:
                parts.append(game.wiki_description)
            return ' '.join(parts)

        return game.summary or game.storyline or game.rawg_description or game.wiki_description or ''

    def _analyze_game_fast(self, game_dict: Dict, existing_relations: Dict[str, set] = None) -> Dict[str, Any]:
        """Максимально быстрый анализ одной игры - оптимизированная версия с EXISTS и поддержкой регистра"""

        if self.combine_all_texts:
            summary = game_dict.get('summary', '') or ''
            storyline = game_dict.get('storyline', '') or ''
            rawg = game_dict.get('rawg_description', '') or ''
            wiki = game_dict.get('wiki_description', '') or ''

            if summary or storyline or rawg or wiki:
                text = f"{summary} {storyline} {rawg} {wiki}"
            else:
                text = ''
        else:
            text = game_dict.get('summary', '') or game_dict.get('storyline', '') or \
                   game_dict.get('rawg_description', '') or game_dict.get('wiki_description', '') or ''

        if not text or len(text.strip()) < 10:
            return {
                'id': game_dict['id'],
                'name': game_dict['name'],
                'has_results': False,
                'skipped': True,
                'reason': 'no_text',
                'count': 0,
                'found': {'genres': [], 'themes': [], 'perspectives': [], 'game_modes': []},
                'pattern_info': {},
                'has_new': False
            }

        text_lower = text.lower()
        game_id = game_dict['id']

        found_ids = {
            'genres': set(),
            'themes': set(),
            'perspectives': set(),
            'game_modes': set()
        }

        pattern_info = {
            'genres': [],
            'themes': [],
            'perspectives': [],
            'game_modes': []
        }

        total_found = 0

        for p in self.compiled_patterns:
            try:
                is_case_sensitive = p.get('is_case_sensitive', False)

                if is_case_sensitive:
                    match = p['pattern'].search(text)
                    search_text = text
                else:
                    match = p['pattern'].search(text_lower)
                    search_text = text_lower

                if match:
                    crit_type = p['type']
                    crit_id = p['id']

                    if crit_id not in found_ids[crit_type]:
                        found_ids[crit_type].add(crit_id)
                        total_found += 1

                        if is_case_sensitive:
                            match_start = match.start()
                            match_end = match.end()

                            # Извлекаем полное слово, в котором найдено совпадение
                            word_start = match_start
                            while word_start > 0 and (text[word_start - 1].isalnum() or text[word_start - 1] in '-_'):
                                word_start -= 1

                            word_end = match_end
                            while word_end < len(text) and (text[word_end].isalnum() or text[word_end] in '-_'):
                                word_end += 1

                            matched_word = text[word_start:word_end]

                            # Получаем контекст
                            context_start = max(0, word_start - 40)
                            context_end = min(len(text), word_end + 40)
                            context = text[context_start:context_end]
                            if context_start > 0:
                                context = "..." + context
                            if context_end < len(text):
                                context = context + "..."

                            pattern_info[crit_type].append({
                                'name': p['name'],
                                'id': crit_id,
                                'matched_text': matched_word[:100],
                                'context': context[:200],
                                'pattern': p['pattern_str'],
                                'status': 'found'
                            })
                        else:
                            match_start_lower = match.start()
                            match_end_lower = match.end()

                            original_match_start = match_start_lower
                            original_match_end = match_end_lower

                            word_start = original_match_start
                            while word_start > 0 and (text[word_start - 1].isalnum() or text[word_start - 1] in '-_'):
                                word_start -= 1

                            word_end = original_match_end
                            while word_end < len(text) and (text[word_end].isalnum() or text[word_end] in '-_'):
                                word_end += 1

                            full_word = text[word_start:word_end]

                            context_start = max(0, word_start - 40)
                            context_end = min(len(text), word_end + 40)
                            context = text[context_start:context_end]
                            if context_start > 0:
                                context = "..." + context
                            if context_end < len(text):
                                context = context + "..."

                            pattern_info[crit_type].append({
                                'name': p['name'],
                                'id': crit_id,
                                'matched_text': full_word[:100],
                                'context': context[:200],
                                'pattern': p['pattern_str'],
                                'status': 'found'
                            })
            except Exception:
                continue

        found = {'genres': [], 'themes': [], 'perspectives': [], 'game_modes': []}
        has_new = False

        if total_found > 0 and self.update_game:
            from django.db import connection

            for crit_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                if not found_ids[crit_type]:
                    continue

                if crit_type == 'genres':
                    table = 'games_game_genres'
                    field = 'genre_id'
                elif crit_type == 'themes':
                    table = 'games_game_themes'
                    field = 'theme_id'
                elif crit_type == 'perspectives':
                    table = 'games_game_player_perspectives'
                    field = 'playerperspective_id'
                elif crit_type == 'game_modes':
                    table = 'games_game_game_modes'
                    field = 'gamemode_id'
                else:
                    continue

                crit_ids_list = list(found_ids[crit_type])
                placeholders = ','.join(['%s'] * len(crit_ids_list))

                with connection.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT DISTINCT {field}
                        FROM {table}
                        WHERE game_id = %s AND {field} IN ({placeholders})
                    """, [game_id] + crit_ids_list)

                    existing_ids = {row[0] for row in cursor.fetchall()}

                for crit_id in found_ids[crit_type]:
                    is_new = crit_id not in existing_ids
                    if is_new:
                        has_new = True

                    found[crit_type].append({
                        'id': crit_id,
                        'name': self.criteria_by_id[crit_type].get(crit_id, 'Unknown'),
                        'is_new': is_new
                    })
        else:
            for crit_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                for crit_id in found_ids[crit_type]:
                    found[crit_type].append({
                        'id': crit_id,
                        'name': self.criteria_by_id[crit_type].get(crit_id, 'Unknown'),
                        'is_new': True
                    })
            has_new = total_found > 0

        return {
            'id': game_id,
            'name': game_dict['name'],
            'has_results': total_found > 0,
            'skipped': False,
            'count': total_found,
            'found': found,
            'pattern_info': pattern_info,
            'has_new': has_new
        }

    def _parallel_analysis(self, games: List[Dict], existing_relations: Dict[str, set] = None) -> Dict[str, Any]:
        """Максимально быстрый многопоточный анализ - с полной статистикой и прерыванием"""
        import sys
        from queue import Queue, Empty
        import threading
        import time
        import signal

        total_games = len(games)
        stats = {
            'total': total_games,
            'processed': 0,
            'with_results': 0,
            'with_new_results': 0,
            'total_found': 0,
            'total_new_found': 0,
            'skipped': 0,
            'errors': 0,
            'to_save': [],
            'all_results': []
        }

        task_queue = Queue()
        for game in games:
            task_queue.put(game)

        result_queue = Queue()
        stats_lock = threading.Lock()
        stop_flag = threading.Event()
        active_workers = 0
        workers_lock = threading.Lock()
        interrupted = False

        existing_sets = existing_relations if existing_relations else {
            'genres': set(), 'themes': set(), 'perspectives': set(), 'game_modes': set()
        }

        def signal_handler(signum, frame):
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                stop_flag.set()

        original_handler = signal.signal(signal.SIGINT, signal_handler)

        def worker():
            nonlocal active_workers
            with workers_lock:
                active_workers += 1

            while not stop_flag.is_set():
                try:
                    game_dict = task_queue.get(timeout=0.5)
                except Empty:
                    break

                try:
                    result = self._analyze_game_fast(game_dict, existing_sets)
                    result_queue.put(result)
                except Exception as e:
                    result_queue.put({
                        'id': game_dict['id'],
                        'name': game_dict['name'],
                        'has_results': False,
                        'skipped': True,
                        'reason': 'error',
                        'count': 0,
                        'found': {'genres': [], 'themes': [], 'perspectives': [], 'game_modes': []},
                        'has_new': False
                    })

            with workers_lock:
                active_workers -= 1

        threads = []
        num_threads = min(self.threads, total_games)
        for _ in range(num_threads):
            t = threading.Thread(target=worker)
            t.daemon = True
            t.start()
            threads.append(t)

        processed = 0
        start_time = time.time()
        last_report = time.time()

        def format_time(seconds):
            if seconds < 60:
                return f"{seconds:.0f}сек"
            elif seconds < 3600:
                return f"{seconds / 60:.1f}мин"
            else:
                return f"{seconds / 3600:.1f}ч"

        try:
            while processed < total_games and not stop_flag.is_set():
                try:
                    result = result_queue.get(timeout=0.5)
                    processed += 1

                    with stats_lock:
                        stats['processed'] = processed
                        stats['all_results'].append(result)

                        if result.get('skipped'):
                            stats['skipped'] += 1
                        elif result.get('has_results'):
                            stats['with_results'] += 1
                            stats['total_found'] += result.get('count', 0)

                            if result.get('has_new'):
                                stats['with_new_results'] += 1
                                stats['total_new_found'] += result.get('count', 0)
                                if self.update_game:
                                    stats['to_save'].append({
                                        'id': result['id'],
                                        'found': result.get('found', {})
                                    })

                    now = time.time()
                    if now - last_report >= 1.0:
                        elapsed = now - start_time
                        percent = (processed / total_games) * 100
                        speed = processed / elapsed if elapsed > 0 else 0

                        if speed > 0:
                            remaining = (total_games - processed) / speed
                            eta = format_time(remaining)
                        else:
                            eta = "расчет..."

                        progress_line = (f"\r📊 {processed}/{total_games} ({percent:.1f}%) | "
                                         f"Скорость: {speed:.1f} игр/сек | "
                                         f"Осталось: {eta} | "
                                         f"Найдено: {stats['total_found']} | "
                                         f"Новых: {stats['total_new_found']}    ")

                        sys.stderr.write(progress_line)
                        sys.stderr.flush()
                        last_report = now

                except Empty:
                    with workers_lock:
                        workers_alive = active_workers > 0

                    if not workers_alive and task_queue.empty():
                        try:
                            while True:
                                result = result_queue.get_nowait()
                                processed += 1
                                with stats_lock:
                                    stats['processed'] = processed
                                    stats['all_results'].append(result)
                                    if result.get('skipped'):
                                        stats['skipped'] += 1
                                    elif result.get('has_results'):
                                        stats['with_results'] += 1
                                        stats['total_found'] += result.get('count', 0)
                                        if result.get('has_new'):
                                            stats['with_new_results'] += 1
                                            stats['total_new_found'] += result.get('count', 0)
                                            if self.update_game:
                                                stats['to_save'].append({
                                                    'id': result['id'],
                                                    'found': result.get('found', {})
                                                })
                        except Empty:
                            pass
                        break
                    continue

        except KeyboardInterrupt:
            if not interrupted:
                interrupted = True
                stop_flag.set()
            sys.stderr.write("\n⏹️ Прерывание... Ожидание завершения потоков\n")
            sys.stderr.flush()

        finally:
            signal.signal(signal.SIGINT, original_handler)

            for t in threads:
                t.join(timeout=2)

            try:
                while True:
                    result = result_queue.get_nowait()
                    with stats_lock:
                        stats['processed'] += 1
                        stats['all_results'].append(result)
                        if result.get('skipped'):
                            stats['skipped'] += 1
                        elif result.get('has_results'):
                            stats['with_results'] += 1
                            stats['total_found'] += result.get('count', 0)
                            if result.get('has_new'):
                                stats['with_new_results'] += 1
                                stats['total_new_found'] += result.get('count', 0)
                                if self.update_game:
                                    stats['to_save'].append({
                                        'id': result['id'],
                                        'found': result.get('found', {})
                                    })
            except Empty:
                pass

            elapsed = time.time() - start_time
            sys.stderr.write(f"\n✅ Обработано: {stats['processed']}/{total_games} игр за {format_time(elapsed)}\n")
            sys.stderr.flush()

            stats['analysis_time'] = elapsed
            stats['games_per_second'] = stats['processed'] / elapsed if elapsed > 0 else 0
            stats['interrupted'] = interrupted

            return stats

    def _init_progress_bar(self, total_games: int):
        """Инициализирует прогресс-бар без немедленного отображения"""
        if self.no_progress or total_games <= 1:
            return

        try:
            from .analyzer.progress_bar import ProgressBar

            stat_width = max(4, len(str(total_games)))

            self.progress_bar = ProgressBar(
                total=total_games,
                desc="Анализ игр",
                bar_length=30,
                update_interval=0.1,
                stat_width=stat_width,
                emoji_spacing=1
            )

            if hasattr(self.progress_bar, 'current'):
                self.progress_bar.current = 0
            if hasattr(self.progress_bar, '_progress_bar'):
                self.progress_bar._progress_bar.current = 0

            self.progress_bar.update_stats({
                'found_count': 0,
                'total_criteria_found': 0,
                'skipped_total': 0,
                'not_found_count': 0,
                'errors': 0,
                'updated': 0,
                'in_batch': 0
            })

            if hasattr(self.progress_bar, '_progress_bar'):
                self.progress_bar._progress_bar._force_update = lambda: None

            if hasattr(self.progress_bar, '_progress_bar') and hasattr(self.progress_bar._progress_bar, 'display'):
                self.progress_bar._progress_bar.display = lambda: None

        except Exception:
            self.progress_bar = None
            self.no_progress = True

    def _bulk_save_results(self, to_save: List[Dict]):
        """Максимально быстрое сохранение для PostgreSQL с индикацией прогресса"""
        if not to_save:
            return

        import sys
        from django.db import connection, transaction

        total_games = len(to_save)
        sys.stderr.write(f"\n💾 Сохранение {total_games} игр в БД...\n")
        sys.stderr.flush()

        save_start = time.time()

        from games.models import Game, Genre, Theme, PlayerPerspective, GameMode

        games_genres_table = Game.genres.through._meta.db_table
        games_themes_table = Game.themes.through._meta.db_table
        games_perspectives_table = Game.player_perspectives.through._meta.db_table
        games_modes_table = Game.game_modes.through._meta.db_table

        genre_relations = []
        theme_relations = []
        perspective_relations = []
        mode_relations = []

        for item in to_save:
            game_id = item['id']

            for criteria_type, items in item['found'].items():
                if not items:
                    continue
                for crit in items:
                    crit_id = crit['id']
                    if criteria_type == 'genres':
                        genre_relations.append((game_id, crit_id))
                    elif criteria_type == 'themes':
                        theme_relations.append((game_id, crit_id))
                    elif criteria_type == 'perspectives':
                        perspective_relations.append((game_id, crit_id))
                    elif criteria_type == 'game_modes':
                        mode_relations.append((game_id, crit_id))

        total_relations = len(genre_relations) + len(theme_relations) + len(perspective_relations) + len(mode_relations)
        sys.stderr.write(f"   📊 Связей для сохранения: {total_relations}\n")
        sys.stderr.flush()

        def bulk_insert(relations, table_name, id_field, type_name):
            if not relations:
                return 0

            with connection.cursor() as cursor:
                game_ids = list(set(r[0] for r in relations))
                crit_ids = list(set(r[1] for r in relations))

                cursor.execute(f"""
                    SELECT game_id, {id_field}_id FROM {table_name}
                    WHERE game_id = ANY(%s) AND {id_field}_id = ANY(%s)
                """, [game_ids, crit_ids])
                existing = set(cursor.fetchall())

                new_relations = [(g, c) for g, c in relations if (g, c) not in existing]

                if not new_relations:
                    return 0

                import io
                data_buffer = io.StringIO()
                for game_id, crit_id in new_relations:
                    data_buffer.write(f"{game_id}\t{crit_id}\n")
                data_buffer.seek(0)

                cursor.copy_from(data_buffer, table_name,
                                 columns=('game_id', f'{id_field}_id'))

                return len(new_relations)

        with transaction.atomic():
            sys.stderr.write(f"   📖 Сохранение жанров... ")
            sys.stderr.flush()
            start = time.time()
            genres_added = bulk_insert(genre_relations, games_genres_table, 'genre', 'Жанры')
            sys.stderr.write(f"{genres_added} связей за {time.time() - start:.1f}с\n")
            sys.stderr.flush()

            sys.stderr.write(f"   📖 Сохранение тем... ")
            sys.stderr.flush()
            start = time.time()
            themes_added = bulk_insert(theme_relations, games_themes_table, 'theme', 'Темы')
            sys.stderr.write(f"{themes_added} связей за {time.time() - start:.1f}с\n")
            sys.stderr.flush()

            sys.stderr.write(f"   📖 Сохранение перспектив... ")
            sys.stderr.flush()
            start = time.time()
            perspectives_added = bulk_insert(perspective_relations, games_perspectives_table, 'playerperspective',
                                             'Перспективы')
            sys.stderr.write(f"{perspectives_added} связей за {time.time() - start:.1f}с\n")
            sys.stderr.flush()

            sys.stderr.write(f"   📖 Сохранение режимов... ")
            sys.stderr.flush()
            start = time.time()
            modes_added = bulk_insert(mode_relations, games_modes_table, 'gamemode', 'Режимы')
            sys.stderr.write(f"{modes_added} связей за {time.time() - start:.1f}с\n")
            sys.stderr.flush()

        save_time = time.time() - save_start
        total_added = genres_added + themes_added + perspectives_added + modes_added

        sys.stderr.write(f"\n   ✅ Сохранено за {save_time:.1f}с:\n")
        sys.stderr.write(f"      Жанры: {genres_added} связей\n")
        sys.stderr.write(f"      Темы: {themes_added} связей\n")
        sys.stderr.write(f"      Перспективы: {perspectives_added} связей\n")
        sys.stderr.write(f"      Режимы: {modes_added} связей\n")
        sys.stderr.write(f"      Всего: {total_added} связей\n")
        if save_time > 0 and total_added > 0:
            sys.stderr.write(f"      ⚡ Скорость: {total_added / save_time:.0f} связей/сек\n")
        sys.stderr.flush()

    def _output_results(self, results: List[Dict]):
        """Вывод результатов в файл (только игры с новыми критериями, паттерны и контекст)"""
        if not self.output_file:
            return

        games_with_new = [r for r in results if r.get('has_new', False)]

        self.output_file.flush()

        if not games_with_new:
            self.output_file.write("=" * 60 + "\n")
            self.output_file.write("📊 РЕЗУЛЬТАТЫ АНАЛИЗА\n")
            self.output_file.write("=" * 60 + "\n")
            self.output_file.write("\n✅ Нет игр с новыми критериями для сохранения\n")
            self.output_file.write("=" * 60 + "\n")
            self.output_file.flush()
            return

        self.output_file.write("=" * 60 + "\n")
        self.output_file.write("📊 РЕЗУЛЬТАТЫ АНАЛИЗА (только НОВЫЕ критерии)\n")
        self.output_file.write("=" * 60 + "\n")
        self.output_file.write(f"\n📈 Всего игр с новыми критериями: {len(games_with_new)}\n")
        self.output_file.write("=" * 60 + "\n")
        self.output_file.flush()

        index = 1
        for r in games_with_new:
            self.output_file.write(f"\n{index}. 🎮 {r['name']} (ID: {r['id']})\n")

            if r.get('skipped'):
                if r.get('reason') == 'no_text':
                    self.output_file.write("   ⏭️ Пропущено: нет текста для анализа\n")
                elif r.get('reason') == 'short':
                    self.output_file.write("   ⏭️ Пропущено: текст слишком короткий\n")
                elif r.get('reason') == 'error':
                    self.output_file.write(f"   ❌ Ошибка: {r.get('error', 'неизвестная ошибка')}\n")
                index += 1
                continue

            if not r['has_results']:
                self.output_file.write("   ℹ️ Критерии не найдены\n")
                index += 1
                continue

            pattern_info = r.get('pattern_info', {})
            has_any_new = False

            for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                items = r['found'].get(criteria_type, [])
                new_items = [item for item in items if item.get('is_new', False)]

                if not new_items:
                    continue

                has_any_new = True
                display_name = self._get_display_name(criteria_type)
                self.output_file.write(f"   📌 {display_name}:\n")

                for item in new_items:
                    item_name = item['name']
                    item_id = item['id']

                    pattern_context = self._find_pattern_and_context_for_criteria(pattern_info, criteria_type, item_id,
                                                                                  item_name)

                    self.output_file.write(f"      • {item_name} (ID: {item_id})\n")
                    if pattern_context:
                        self.output_file.write(f"        🔍 Паттерн: {pattern_context['pattern']}\n")
                        self.output_file.write(f"        📖 Совпавший текст: \"{pattern_context['matched_text']}\"\n")
                        self.output_file.write(f"        📝 Контекст: {pattern_context['context']}\n")
                    else:
                        self.output_file.write(f"        ⚠️ Информация о паттерне не найдена\n")
                    self.output_file.write("\n")

                self.output_file.write("\n")

            if not has_any_new:
                self.output_file.write("   ℹ️ Нет новых критериев (только существующие)\n\n")

            index += 1
            self.output_file.flush()

        self.output_file.write("=" * 60 + "\n")
        self.output_file.write(f"✅ Вывод завершен. Показано игр с новыми критериями: {len(games_with_new)}\n")
        self.output_file.write("=" * 60 + "\n")
        self.output_file.flush()

    def _find_match_context(self, pattern_info: Dict, criteria_type: str, item_id: int, item_name: str) -> str:
        """Находит контекст для конкретного элемента"""
        if criteria_type not in pattern_info:
            return ""

        matches_for_category = pattern_info[criteria_type]
        if not matches_for_category:
            return ""

        item_name_lower = item_name.lower()

        for match in matches_for_category:
            if match.get('status') != 'found':
                continue

            match_name = match.get('name', '')
            matched_text = match.get('matched_text', '')
            context = match.get('context', '')
            pattern = match.get('pattern', '')

            if match_name and match_name.lower() == item_name_lower:
                if context:
                    clean_context = ' '.join(context.split())
                    if pattern:
                        return f'("{pattern}" как "{matched_text}" в: "{clean_context}")'
                    else:
                        return f'(найдено как "{matched_text}" в: "{clean_context}")'
                elif matched_text:
                    if pattern:
                        return f'("{pattern}" как "{matched_text}")'
                    else:
                        return f'(найдено как "{matched_text}")'

            if matched_text and matched_text.lower() == item_name_lower:
                if context:
                    clean_context = ' '.join(context.split())
                    if pattern:
                        return f'("{pattern}" как "{matched_text}" в: "{clean_context}")'
                    else:
                        return f'(найдено как "{matched_text}" в: "{clean_context}")'
                else:
                    if pattern:
                        return f'("{pattern}" как "{matched_text}")'
                    else:
                        return f'(найдено как "{matched_text}")'

        return ""

    def _get_display_name(self, key: str) -> str:
        """Возвращает читаемое имя для типа критерия"""
        names = {
            'genres': 'Жанры',
            'themes': 'Темы',
            'perspectives': 'Перспективы',
            'game_modes': 'Режимы игры',
        }
        return names.get(key, key)

    def _print_statistics(self, stats: Dict, start_time: float, interrupted: bool = False):
        """Вывод финальной статистики"""
        total_time = time.time() - start_time

        saved_count = len(stats.get('to_save', [])) if self.update_game else 0

        not_found_count = stats['processed'] - stats['with_results'] - stats['skipped']
        if not_found_count < 0:
            not_found_count = 0

        sys.stderr.write("\n" + "=" * 70 + "\n")
        if interrupted:
            sys.stderr.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО)\n")
        else:
            sys.stderr.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write(f"🔄 Обработано игр: {stats['processed']}\n")
        sys.stderr.write(f"🎯 Игр с найденными критериями: {stats['with_results']}\n")
        sys.stderr.write(f"📈 Всего критериев найдено: {stats['total_found']}\n")
        sys.stderr.write(f"⏭️ Пропущено (нет текста/короткий текст): {stats['skipped']}\n")
        sys.stderr.write(f"⚪ Игр без найденных элементов: {not_found_count}\n")
        sys.stderr.write(f"❌ Ошибок: {stats['errors']}\n")

        if self.update_game and saved_count > 0:
            sys.stderr.write(f"💾 Сохранено в БД: {saved_count} игр\n")

        sys.stderr.write(f"⏱️ Время выполнения: {total_time:.1f} секунд\n")

        if stats.get('games_per_second', 0) > 0:
            sys.stderr.write(f"⚡ Средняя скорость: {stats['games_per_second']:.0f} игр/сек\n")

        sys.stderr.write("=" * 70 + "\n")

        if self.output_path:
            sys.stderr.write(f"✅ Результаты сохранены в: {self.output_path}\n")

        if interrupted:
            sys.stderr.write("\n⚠️ Анализ был прерван. Частичные результаты сохранены.\n")
        else:
            sys.stderr.write("\n✨ Анализ успешно завершен!\n")
        sys.stderr.flush()

    def _setup_output_file(self):
        try:
            directory = os.path.dirname(self.output_path)
            if directory:
                os.makedirs(directory, exist_ok=True)
            self.original_stdout = self.stdout._out
            self.output_file = open(self.output_path, 'w', encoding='utf-8')
            self.stdout._out = self.output_file
        except Exception as e:
            self.stderr.write(f"❌ Ошибка открытия файла: {e}")

    def _cleanup(self):
        """Закрытие файла вывода и восстановление stdout"""
        if self.output_file:
            try:
                self.output_file.flush()
                self.output_file.close()
                if self.original_stdout and hasattr(self.stdout, '_out'):
                    self.stdout._out = self.original_stdout
            except:
                pass
            finally:
                self.output_file = None
                self.original_stdout = None

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        import signal
        import threading

        stop_flag = threading.Event()

        start_time = time.time()
        results = None
        interrupted = False

        def signal_handler(signum, frame):
            nonlocal interrupted
            if not interrupted:
                interrupted = True
                stop_flag.set()
                import os
                os.write(2, b"\n[INTERRUPTED] Stopping...\n")

        original_handler = signal.signal(signal.SIGINT, signal_handler)

        try:
            self.limit = options.get('limit')
            self.offset = options.get('offset', 0)
            self.game_name = options.get('game_name')
            self.genre = options.get('genre')
            self.filter_genre = options.get('filter_genre')
            self.theme = options.get('theme')
            self.verbose = options.get('verbose', False)
            self.threads = min(options.get('threads', 16), 32)
            self.force_restart = options.get('force_restart', False)
            self.output_path = options.get('output')
            self.only_found = options.get('only_found', False)
            self.no_progress = options.get('no_progress', False)
            self.auto_save = options.get('auto_save', False)
            self.custom_pattern = options.get('custom_pattern')

            self.update_game = True
            self.combine_all_texts = not options.get('no_combine_texts', False)

            self.output_file = None
            self.original_stdout = None
            if self.output_path:
                self._setup_output_file()

            self._print_startup_info()

            if self.force_restart:
                sys.stderr.write("\n🧹 Очищаем кэш...\n")
                sys.stderr.flush()
                RangeCacheManager.clear_all_games()
                sys.stderr.write("   ✅ Готово\n")
                sys.stderr.flush()

            sys.stderr.write("\n📚 Загрузка данных...\n")
            sys.stderr.flush()
            self._preload_all_data()

            sys.stderr.write("\n🎮 Загрузка игр...\n")
            sys.stderr.flush()
            games_to_analyze = self._get_games_to_analyze()

            # Фильтруем игры по жанру если указан
            if self.filter_genre:
                games_to_analyze = self._filter_games_by_genre(games_to_analyze)

            sys.stderr.write(f"   ✅ Получено {len(games_to_analyze)} игр\n")
            sys.stderr.flush()

            if not games_to_analyze:
                sys.stderr.write("\n✅ Нет игр для анализа\n")
                sys.stderr.flush()
                self._cleanup()
                return

            sys.stderr.write("\n📚 Загрузка существующих связей для проверки...\n")
            sys.stderr.flush()
            existing_relations = self._load_existing_relations()
            sys.stderr.write(f"   ✅ Загружено связей: жанры={len(existing_relations['genres'])}, "
                             f"темы={len(existing_relations['themes'])}, "
                             f"перспективы={len(existing_relations['perspectives'])}, "
                             f"режимы={len(existing_relations['game_modes'])}\n")
            sys.stderr.flush()

            self.progress_bar = None
            results = self._parallel_analysis(games_to_analyze, existing_relations)

            if results:
                self._process_results(results, start_time)

        except KeyboardInterrupt:
            self._handle_interrupt(results)
        except Exception as e:
            sys.stderr.write(f"\n❌ Ошибка: {e}\n")
            sys.stderr.flush()
            import traceback
            traceback.print_exc(file=sys.stderr)
        finally:
            signal.signal(signal.SIGINT, original_handler)
            self._cleanup()
            sys.stderr.write("\n✨ Завершено\n")
            sys.stderr.flush()
            sys.exit(0)

    def _print_startup_info(self):
        """Выводит информацию о запуске"""
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write("🚀 МАКСИМАЛЬНО УСКОРЕННЫЙ АНАЛИЗ КРИТЕРИЕВ\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write(f"🔧 Потоков: {self.threads}\n")
        sys.stderr.write(f"🔄 Обновление БД: {'✅' if self.update_game else '❌'}\n")
        if self.update_game:
            sys.stderr.write(
                f"💾 Режим сохранения: {'Авто' if self.auto_save else 'С подтверждением после анализа'}\n")
        sys.stderr.write(f"📄 Вывод в файл: {'✅' if self.output_path else '❌'}\n")
        sys.stderr.write(f"📚 Объединять все тексты: {'✅' if self.combine_all_texts else '❌'}\n")
        if self.game_name:
            sys.stderr.write(f"🎮 Поиск по имени: '{self.game_name}'\n")
        if self.genre:
            sys.stderr.write(f"🎭 Анализировать ТОЛЬКО жанр: '{self.genre}'\n")
        if self.filter_genre:
            sys.stderr.write(f"🎭 Фильтровать игры по жанру: '{self.filter_genre}'\n")
        if self.theme:
            sys.stderr.write(f"📚 Анализировать ТОЛЬКО тему: '{self.theme}'\n")
        if self.custom_pattern:
            sys.stderr.write(f"🔧 Пользовательский паттерн: '{self.custom_pattern}'\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.flush()

    def _process_results(self, results: Dict[str, Any], start_time: float):
        """Обрабатывает результаты анализа: сохраняет файл, выводит статистику, спрашивает про БД"""

        if self.output_path and results.get('all_results'):
            sys.stderr.write(f"\n📝 Запись результатов в файл...\n")
            sys.stderr.flush()
            self._output_results(results['all_results'])
            sys.stderr.write(f"   ✅ Результаты сохранены в: {self.output_path}\n")
            sys.stderr.flush()

            self._cleanup()
            sys.stderr.write(f"   ✅ Файл закрыт и сохранен на диск\n")
            sys.stderr.flush()

        total_time = time.time() - start_time
        not_found = results['processed'] - results['with_results'] - results['skipped']
        if not_found < 0:
            not_found = 0

        def format_time(seconds):
            if seconds < 60:
                return f"{seconds:.0f} сек"
            elif seconds < 3600:
                minutes = seconds / 60
                return f"{minutes:.1f} мин"
            else:
                hours = seconds / 3600
                return f"{hours:.1f} ч"

        sys.stderr.write("\n" + "=" * 70 + "\n")
        if results.get('interrupted'):
            sys.stderr.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО)\n")
        else:
            sys.stderr.write("📊 ИТОГОВАЯ СТАТИСТИКА\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.write(f"🔄 Обработано: {results['processed']} игр\n")
        sys.stderr.write(f"🎯 Игр с найденными критериями: {results['with_results']}\n")
        sys.stderr.write(f"✨ Игр с НОВЫМИ критериями: {results['with_new_results']}\n")
        sys.stderr.write(f"📈 Всего критериев: {results['total_found']} (новых: {results['total_new_found']})\n")
        sys.stderr.write(f"⏭️ Пропущено: {results['skipped']}\n")
        sys.stderr.write(f"⚪ Без элементов: {not_found}\n")
        sys.stderr.write(f"❌ Ошибок: {results['errors']}\n")
        sys.stderr.write(f"⏱️ Время: {format_time(total_time)}\n")
        if results.get('games_per_second', 0) > 0:
            sys.stderr.write(f"⚡ Скорость: {results['games_per_second']:.0f} игр/сек\n")
        sys.stderr.write("=" * 70 + "\n")
        sys.stderr.flush()

        if not self.auto_save and results.get('to_save') and not results.get('interrupted'):
            sys.stderr.write(
                f"\n💾 Найдено {len(results['to_save'])} игр с новыми критериями ({results['total_new_found']} новых элементов).\n")
            sys.stderr.write("Сохранить результаты в БД? (y/N): ")
            sys.stderr.flush()

            try:
                answer = sys.stdin.readline().strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = 'n'

            if answer in ['y', 'yes', 'да', 'д', '1']:
                sys.stderr.write("\n✅ Сохраняем в БД...\n")
                sys.stderr.flush()
                self._bulk_save_results(results['to_save'])
                sys.stderr.write("   ✅ Сохранение в БД завершено\n")
                sys.stderr.flush()
            else:
                sys.stderr.write("\n⏭️ Сохранение в БД отменено\n")
                sys.stderr.flush()
        elif self.auto_save and results.get('to_save') and not results.get('interrupted'):
            sys.stderr.write(f"\n💾 Авто-сохранение {len(results['to_save'])} игр в БД...\n")
            sys.stderr.flush()
            self._bulk_save_results(results['to_save'])
            sys.stderr.write("   ✅ Сохранение в БД завершено\n")
            sys.stderr.flush()
        elif results.get('interrupted') and results.get('to_save'):
            if self.auto_save:
                sys.stderr.write(f"\n💾 Авто-сохранение {len(results['to_save'])} игр в БД (прервано)...\n")
                sys.stderr.flush()
                self._bulk_save_results(results['to_save'])
                sys.stderr.write("   ✅ Сохранение в БД завершено\n")
                sys.stderr.flush()
            else:
                sys.stderr.write(
                    f"\n💾 Найдено {len(results['to_save'])} игр с новыми критериями ({results['total_new_found']} новых элементов).\n")
                sys.stderr.write("Сохранить результаты в БД? (y/N): ")
                sys.stderr.flush()

                try:
                    answer = sys.stdin.readline().strip().lower()
                except (EOFError, KeyboardInterrupt):
                    answer = 'n'

                if answer in ['y', 'yes', 'да', 'д', '1']:
                    sys.stderr.write("\n✅ Сохраняем в БД...\n")
                    sys.stderr.flush()
                    self._bulk_save_results(results['to_save'])
                    sys.stderr.write("   ✅ Сохранение в БД завершено\n")
                    sys.stderr.flush()
                else:
                    sys.stderr.write("\n⏭️ Сохранение в БД отменено\n")
                    sys.stderr.flush()

    def _handle_interrupt(self, results: Dict[str, Any]):
        """Обрабатывает прерывание пользователя"""
        sys.stderr.write("\n⏹️ Прервано пользователем\n")
        sys.stderr.flush()

        if results and results.get('all_results') and self.output_path:
            sys.stderr.write(f"\n📝 Сохранение результатов в файл (прервано)...\n")
            sys.stderr.flush()
            try:
                self._output_results(results['all_results'])
                self._cleanup()
                sys.stderr.write(f"   ✅ Результаты сохранены в: {self.output_path}\n")
                sys.stderr.flush()
            except Exception as e:
                sys.stderr.write(f"   ⚠️ Ошибка сохранения файла: {e}\n")
                sys.stderr.flush()

        if results and results.get('to_save'):
            sys.stderr.write(f"\n💾 Найдено {len(results['to_save'])} игр с новыми критериями.\n")
            sys.stderr.write("Сохранить результаты в БД? (y/N): ")
            sys.stderr.flush()

            try:
                answer = sys.stdin.readline().strip().lower()
            except (EOFError, KeyboardInterrupt):
                answer = 'n'

            if answer in ['y', 'yes', 'да', 'д', '1']:
                sys.stderr.write("\n✅ Сохраняем в БД...\n")
                sys.stderr.flush()
                try:
                    self._bulk_save_results(results['to_save'])
                    sys.stderr.write("   ✅ Сохранение в БД завершено\n")
                    sys.stderr.flush()
                except Exception as e:
                    sys.stderr.write(f"   ⚠️ Ошибка: {e}\n")
                    sys.stderr.flush()
            else:
                sys.stderr.write("\n⏭️ Сохранение в БД отменено\n")
                sys.stderr.flush()