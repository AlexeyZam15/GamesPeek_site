# games/management/commands/analyze_game_criteria_fas_src/loading.py

import os
import re
import sys
import threading
from collections import defaultdict
from typing import Dict, Any, List, Optional, Set

from django.db import connection

from games.models import Genre, Theme, PlayerPerspective, GameMode, Keyword
from games.analyze.pattern_manager import PatternManager


class LoadingMixin:
    """Загрузка данных: кэши критериев, паттерны, игры, связи."""

    def _filter_games_by_genre(self, games: List[Dict]) -> List[Dict]:
        """Фильтрует игры, у которых уже есть указанный жанр"""
        if not self.filter_genre:
            return games

        sys.stderr.write(f"\n   🎭 Фильтрация игр по жанру: '{self.filter_genre}'\n")
        sys.stderr.flush()

        try:
            genre_obj = Genre.objects.filter(name__iexact=self.filter_genre).first()
            if not genre_obj:
                sys.stderr.write(f"   ⚠️ Жанр '{self.filter_genre}' не найден в БД\n")
                sys.stderr.flush()
                return []

            genre_id = genre_obj.id
            sys.stderr.write(f"   🔍 ID жанра: {genre_id}\n")
            sys.stderr.flush()

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

            filtered_games = [g for g in games if g['id'] in game_ids]

            sys.stderr.write(f"   ✅ Отфильтровано: {len(filtered_games)} игр из {len(games)}\n")
            sys.stderr.flush()

            return filtered_games

        except Exception as e:
            sys.stderr.write(f"   ⚠️ Ошибка фильтрации по жанру: {e}\n")
            sys.stderr.flush()
            return games

    def _load_existing_relations(self) -> Dict[str, set]:
        """Загружает все существующие связи из БД для быстрой проверки"""
        from games.models import Game

        games_genres_table = Game.genres.through._meta.db_table
        games_themes_table = Game.themes.through._meta.db_table
        games_perspectives_table = Game.player_perspectives.through._meta.db_table
        games_modes_table = Game.game_modes.through._meta.db_table

        existing_relations = {
            'genres': set(),
            'themes': set(),
            'perspectives': set(),
            'game_modes': set(),
            'keywords': set(),
            'keywords_by_game': {},
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

            cursor.execute("SELECT id, keyword_ids FROM games_game")
            kw_by_game = existing_relations['keywords_by_game']
            kw_flat = existing_relations['keywords']
            for game_id, keyword_ids in cursor.fetchall():
                if not keyword_ids:
                    continue
                s = set(keyword_ids)
                kw_by_game[game_id] = s
                for kw_id in s:
                    kw_flat.add((game_id, kw_id))

        return existing_relations

    def _preload_all_data(self):
        """Предзагрузка ВСЕХ данных в память с максимальной оптимизацией и фильтрацией по критерию"""
        self.patterns = PatternManager.get_all_patterns()

        # Защита: поля могут отсутствовать, если миксин используется без command.py
        if not hasattr(self, '_keyword_trie'):
            self._keyword_trie = None
        if not hasattr(self, '_keyword_trie_lock') or self._keyword_trie_lock is None:
            self._keyword_trie_lock = threading.Lock()
        if not hasattr(self, 'max_keyword_id_before'):
            self.max_keyword_id_before = 0
        if not hasattr(self, 'new_keyword_ids'):
            self.new_keyword_ids = set()

        self.criteria_by_id = {
            'genres': {},
            'themes': {},
            'perspectives': {},
            'game_modes': {},
            'keywords': {}
        }

        self.criteria_by_name = {
            'genres': {},
            'themes': {},
            'perspectives': {},
            'game_modes': {},
            'keywords': {}
        }

        for g in Genre.objects.all().only('id', 'name'):
            self.criteria_by_id['genres'][g.id] = g.name
            self.criteria_by_name['genres'][g.name.lower()] = g.id

        for t in Theme.objects.all().only('id', 'name'):
            self.criteria_by_id['themes'][t.id] = t.name
            self.criteria_by_name['themes'][t.name.lower()] = t.id

        for p in PlayerPerspective.objects.all().only('id', 'name'):
            self.criteria_by_id['perspectives'][p.id] = p.name
            self.criteria_by_name['perspectives'][p.name.lower()] = p.id

        for m in GameMode.objects.all().only('id', 'name'):
            self.criteria_by_id['game_modes'][m.id] = m.name
            self.criteria_by_name['game_modes'][m.name.lower()] = m.id

        for k in Keyword.objects.all().only('id', 'name'):
            self.criteria_by_id['keywords'][k.id] = k.name
            self.criteria_by_name['keywords'][k.name.lower()] = k.id

        criteria_types_to_process = []

        if hasattr(self, 'keywords_mode') and self.keywords_mode:
            sys.stderr.write(f"   🔑 Режим анализа КЛЮЧЕВЫХ СЛОВ\n")
            sys.stderr.flush()

            if self.keyword_ids_filter is not None:
                keywords_qs = Keyword.objects.filter(id__in=self.keyword_ids_filter).only('id', 'name')
                sys.stderr.write(
                    f"   🎯 Ограничение по ключевым словам: {len(self.keyword_ids_filter)} ID из файла\n"
                )
                sys.stderr.flush()
            else:
                keywords_qs = Keyword.objects.all().only('id', 'name')

            keywords_patterns = {}
            for keyword in keywords_qs:
                name = keyword.name
                keywords_patterns[name] = {
                    'patterns': [re.compile(re.escape(name), re.IGNORECASE | re.UNICODE)],
                    'stop_words': []
                }
            self.patterns['keywords'] = keywords_patterns
            criteria_types_to_process = [('keywords', None)]

            sys.stderr.write(
                f"   🔑 Загружено ключевых слов для поиска: {len(keywords_patterns)}\n"
            )
            sys.stderr.flush()
        else:
            if hasattr(self, 'criteria_name') and self.criteria_name:
                target_name = self.criteria_name.lower()
                sys.stderr.write(f"   🔍 Универсальный поиск критерия: '{self.criteria_name}'\n")
                sys.stderr.flush()

                found_in_db = False
                for crit_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                    if target_name in self.criteria_by_name[crit_type]:
                        criteria_types_to_process.append((crit_type, self.criteria_name))
                        found_in_db = True
                        sys.stderr.write(f"   ✅ Найден в {crit_type}: '{self.criteria_name}'\n")
                        sys.stderr.flush()

                if not found_in_db:
                    sys.stderr.write(
                        f"   ⚠️ Критерий '{self.criteria_name}' не найден в БД\n")
                    sys.stderr.flush()
            elif self.theme:
                criteria_types_to_process.append(('themes', self.theme))
                sys.stderr.write(f"   📚 Фильтрация: загружаем ТОЛЬКО тему '{self.theme}'\n")
                sys.stderr.flush()
            else:
                criteria_types_to_process = [
                    ('genres', None),
                    ('themes', None),
                    ('perspectives', None),
                    ('game_modes', None),
                ]

        self.compiled_patterns = []
        self.pattern_match_counter_new = defaultdict(int)
        self.pattern_match_counter_existing = defaultdict(int)
        self.criteria_stop_words_cache = {}

        for criteria_type, filter_name in criteria_types_to_process:
            type_patterns = self.patterns.get(criteria_type, {})

            if filter_name:
                filter_name_lower = filter_name.lower()
                matched_name = None
                for name in type_patterns.keys():
                    if name.lower() == filter_name_lower:
                        matched_name = name
                        break

                if matched_name:
                    patterns_dict = {matched_name: type_patterns[matched_name]}
                    sys.stderr.write(
                        f"   🔍 Найдены паттерны для '{matched_name}': {len(type_patterns[matched_name]['patterns'])} шт.\n")
                    sys.stderr.flush()
                else:
                    patterns_dict = {}
                    sys.stderr.write(f"   ⚠️ Паттерны для '{filter_name}' не найдены в {criteria_type}\n")
                    sys.stderr.flush()
            else:
                patterns_dict = type_patterns

            if self.custom_pattern and filter_name:
                sys.stderr.write(f"   🔧 Добавляем пользовательский паттерн: '{self.custom_pattern}'\n")
                sys.stderr.flush()

                patterns_dict = dict(patterns_dict)
                if filter_name in patterns_dict:
                    if self.custom_pattern not in [getattr(p, 'pattern', str(p)) for p in
                                                   patterns_dict[filter_name]['patterns']]:
                        new_pattern = re.compile(self.custom_pattern, re.IGNORECASE | re.UNICODE)
                        patterns_dict[filter_name]['patterns'].append(new_pattern)
                else:
                    patterns_dict[filter_name] = {
                        'patterns': [re.compile(self.custom_pattern, re.IGNORECASE | re.UNICODE)],
                        'stop_words': []
                    }

            for name, pattern_data in patterns_dict.items():
                name_lower = name.lower()
                criteria_id = self.criteria_by_name.get(criteria_type, {}).get(name_lower)
                if criteria_id:
                    patterns = pattern_data.get('patterns', [])
                    stop_words = pattern_data.get('stop_words', [])

                    criteria_key = (criteria_type, criteria_id, name)
                    self.criteria_stop_words_cache[criteria_key] = stop_words

                    for compiled_pattern in patterns:
                        try:
                            if hasattr(compiled_pattern, 'flags'):
                                is_case_sensitive = (compiled_pattern.flags & re.IGNORECASE == 0)
                            else:
                                is_case_sensitive = False

                            self.compiled_patterns.append({
                                'type': criteria_type,
                                'id': criteria_id,
                                'name': name,
                                'pattern': compiled_pattern,
                                'pattern_str': compiled_pattern.pattern if hasattr(compiled_pattern,
                                                                                   'pattern') else str(
                                    compiled_pattern),
                                'is_case_sensitive': is_case_sensitive,
                            })
                        except Exception:
                            continue

        sys.stderr.write(f"   ✅ Скомпилировано паттернов: {len(self.compiled_patterns)}\n")
        if hasattr(self, 'criteria_name') and self.criteria_name:
            patterns_by_name = {}
            for p in self.compiled_patterns:
                if p['name'] == self.criteria_name:
                    if p['name'] not in patterns_by_name:
                        patterns_by_name[p['name']] = {
                            'patterns': [],
                            'stop_words': self.criteria_stop_words_cache.get((p['type'], p['id'], p['name']), [])
                        }
                    patterns_by_name[p['name']]['patterns'].append(p['pattern_str'])

            sys.stderr.write(
                f"   🔍 Найдено паттернов для '{self.criteria_name}': {len(patterns_by_name.get(self.criteria_name, {}).get('patterns', []))}\n"
            )

            for crit_name, data in patterns_by_name.items():
                stop_words = data['stop_words']
                if stop_words:
                    sys.stderr.write(f"      🛑 Стоп-слова для '{crit_name}': {stop_words}\n")
                else:
                    sys.stderr.write(f"      🛑 Стоп-слова для '{crit_name}': []\n")

                for pattern in data['patterns']:
                    sys.stderr.write(f"      - {pattern}\n")
        sys.stderr.flush()

    def _load_game_ids_file(self, path: str) -> Set[int]:
        """Читает файл с ID игр (по одному на строку)."""
        if not path or not os.path.exists(path):
            return set()
        ids = set()
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.isdigit():
                        ids.add(int(line))
        except Exception as e:
            sys.stderr.write(f"   ⚠️ Ошибка чтения {path}: {e}\n")
            sys.stderr.flush()
        return ids

    def _load_keyword_ids_file(self, path: str) -> Set[int]:
        """Читает файл с ID ключевых слов (по одному на строку)."""
        if not path or not os.path.exists(path):
            return set()
        ids = set()
        try:
            with open(path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line.isdigit():
                        ids.add(int(line))
        except Exception as e:
            sys.stderr.write(f"   ⚠️ Ошибка чтения {path}: {e}\n")
            sys.stderr.flush()
        return ids

    def _get_games_to_analyze(self) -> List[Dict]:
        """Максимально быстрая загрузка игр через сырой SQL или по имени"""

        # НОВОЕ: если задан --game-ids-file — берём только эти игры
        if self.game_ids_filter:
            ids_list = list(self.game_ids_filter)
            sys.stderr.write(f"\n   🎯 Обработка только игр из --game-ids-file: {len(ids_list)}\n")
            sys.stderr.flush()

            fields = ['id', 'name', 'summary', 'storyline', 'rawg_description', 'wiki_description']
            fields_str = ', '.join(fields)

            games = []
            batch_size = 5000
            for i in range(0, len(ids_list), batch_size):
                batch = ids_list[i:i + batch_size]
                with connection.cursor() as cursor:
                    cursor.execute(
                        f"SELECT {fields_str} FROM games_game WHERE id = ANY(%s) ORDER BY id",
                        [batch]
                    )
                    for row in cursor.fetchall():
                        games.append({
                            'id': row[0],
                            'name': row[1],
                            'summary': row[2],
                            'storyline': row[3],
                            'rawg_description': row[4],
                            'wiki_description': row[5]
                        })

            sys.stderr.write(f"   ✅ Загружено {len(games)} игр по фильтру ID\n")
            sys.stderr.flush()
            return games

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