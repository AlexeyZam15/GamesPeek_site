"""
МАКСИМАЛЬНО УСКОРЕННАЯ команда для поиска частотных сочетаний слов в текстах игр

Находит самые частые слова и фразы, которые встречаются в радиусе N символов
(по умолчанию 30) от заданного слова/группы слов в описаниях игр.
РАЗДЕЛЯЕТ статистику для позиций ДО и ПОСЛЕ целевого слова.
Текст всегда берется из всех доступных источников.
Объединяет слова с общим корнем (member, members, membership -> member).

Поддерживает группы слов через символ | (логическое ИЛИ).
Пример: "squad|party|team|group|company" будет искать любое из этих слов.

Примеры использования:
    python manage.py find_word_cooccurrences "action" --threads 16 --radius 30 --min-frequency 3 --output results.txt
    python manage.py find_word_cooccurrences "squad|party|team|group|company" --threads 24 --radius 30
    python manage.py find_word_cooccurrences "survival" --radius 30 --min-frequency 2 --max-words 30
"""

import re
import sys
import time
import threading
import signal
from collections import defaultdict
from typing import Dict, Any, List, Set, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from django.core.management.base import BaseCommand
from django.db import connection

from games.models import Game


class Command(BaseCommand):
    """МАКСИМАЛЬНО УСКОРЕННАЯ команда для поиска частотных сочетаний слов"""

    help = 'Находит самые частые сочетания с заданным словом в радиусе N символов'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.target_pattern = None
        self.target_words = []
        self.is_group = False
        self.is_regex = False
        self.radius = 30
        self.min_frequency = 2
        self.max_results = 50
        self.max_words = 20
        self.max_phrases = 20
        self.limit = None
        self.offset = 0
        self.game_id = None
        self.game_name = None
        self.output_path = None
        self.verbose = False
        self.include_positions = False
        self.case_sensitive = False
        self.threads = 16
        self.batch_size = 20000
        self.output_file = None
        self.original_stdout = None
        self.compiled_pattern = None
        self._interrupted = False

    def add_arguments(self, parser):
        """Добавляет аргументы командной строки"""
        parser.add_argument(
            'target_pattern',
            type=str,
            help='Целевое слово или группа слов. Для группы используйте |: squad|party|team'
        )

        parser.add_argument(
            '--radius',
            type=int,
            default=30,
            help='Радиус поиска в символах (до и после целевого слова), по умолчанию 30'
        )

        parser.add_argument(
            '--min-frequency',
            type=int,
            default=2,
            help='Минимальная частота сочетания, по умолчанию 2'
        )

        parser.add_argument(
            '--max-results',
            type=int,
            default=100,
            help='Максимум результатов в статистике, по умолчанию 50'
        )

        parser.add_argument(
            '--max-words',
            type=int,
            default=100,
            help='Максимальное количество слов для вывода, по умолчанию 20'
        )

        parser.add_argument(
            '--max-phrases',
            type=int,
            default=20,
            help='Максимальное количество фраз для вывода, по умолчанию 20'
        )

        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Максимум игр для анализа'
        )

        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Пропустить первых N игр'
        )

        parser.add_argument(
            '--game-id',
            type=int,
            help='ID конкретной игры'
        )

        parser.add_argument(
            '--game-name',
            type=str,
            help='Название игры (частичное совпадение)'
        )

        parser.add_argument(
            '--output',
            type=str,
            help='Путь к файлу для результатов'
        )

        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод'
        )

        parser.add_argument(
            '--include-positions',
            action='store_true',
            help='Включить позиции вхождений'
        )

        parser.add_argument(
            '--case-sensitive',
            action='store_true',
            help='Учитывать регистр'
        )

        parser.add_argument(
            '--threads',
            type=int,
            default=16,
            help='Количество потоков, по умолчанию 16'
        )

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        start_time = time.time()

        self.target_pattern = options['target_pattern']
        self.radius = options['radius']
        self.min_frequency = options['min_frequency']
        self.max_results = options['max_results']
        self.max_words = options.get('max_words', 20)
        self.max_phrases = options.get('max_phrases', 20)
        self.limit = options.get('limit')
        self.offset = options.get('offset', 0)
        self.game_id = options.get('game_id')
        self.game_name = options.get('game_name')
        self.output_path = options.get('output')
        self.verbose = options.get('verbose', False)
        self.include_positions = options.get('include_positions', False)
        self.case_sensitive = options.get('case_sensitive', False)
        self.threads = min(options.get('threads', 16), 32)

        self._parse_target_pattern()

        if self.output_path:
            self._setup_output_file()

        def signal_handler(signum, frame):
            if not self._interrupted:
                self._interrupted = True
                sys.stderr.write("\n⏹️ Прерывание...\n")
                sys.stderr.flush()

        signal.signal(signal.SIGINT, signal_handler)

        try:
            self._print_startup_info()

            games = self._get_games_to_analyze()

            if not games:
                self._print_error("Нет игр для анализа")
                return

            results = self._analyze_games_parallel(games)

            self._print_results_with_patterns(results, start_time)

        except Exception as e:
            self._print_error(f"Ошибка: {e}")
            if self.verbose:
                import traceback
                traceback.print_exc()
        finally:
            self._cleanup()

    def _parse_target_pattern(self):
        """
        Разбирает целевой паттерн.
        Если паттерн начинается с r' , то используется как есть (полноценное регулярное выражение).
        Иначе - обычное слово или группа слов через |.
        """
        target = self.target_pattern.strip()

        # Проверяем, является ли паттерн полноценным регулярным выражением
        # Паттерн может начинаться с r' или просто содержать regex спецсимволы
        if target.startswith("r'") and target.endswith("'"):
            # Извлекаем regex из строки вида r'pattern'
            pattern_str = target[2:-1]
            self.is_regex = True
            self.is_group = False
            self.target_words = [pattern_str]

            flags = re.UNICODE
            if not self.case_sensitive:
                flags |= re.IGNORECASE

            try:
                self.compiled_pattern = re.compile(pattern_str, flags)
                if self.verbose:
                    self._print_message(f"🔧 Используется пользовательский regex паттерн: {pattern_str}")
            except re.error as e:
                self._print_error(f"❌ Ошибка в regex паттерне: {e}")
                raise

        # Проверяем, содержит ли паттерн regex спецсимволы (не экранированные)
        elif any(c in target for c in '\\[](){}?*+.|^$'):
            self.is_regex = True
            self.is_group = False
            self.target_words = [target]

            flags = re.UNICODE
            if not self.case_sensitive:
                flags |= re.IGNORECASE

            try:
                self.compiled_pattern = re.compile(target, flags)
                if self.verbose:
                    self._print_message(f"🔧 Используется regex паттерн: {target}")
            except re.error as e:
                self._print_error(f"❌ Ошибка в regex паттерне: {e}")
                raise

        # Обычная группа слов через |
        elif '|' in target:
            self.is_regex = False
            self.is_group = True
            self.target_words = [w.strip() for w in target.split('|') if w.strip()]

            pattern = r'\b(' + '|'.join(re.escape(w) for w in self.target_words) + r')\b'
            flags = re.UNICODE
            if not self.case_sensitive:
                flags |= re.IGNORECASE
            self.compiled_pattern = re.compile(pattern, flags)

        # Обычное слово
        else:
            self.is_regex = False
            self.is_group = False
            self.target_words = [target]

            flags = re.UNICODE
            if not self.case_sensitive:
                flags |= re.IGNORECASE
            self.compiled_pattern = re.compile(r'\b' + re.escape(target) + r'\b', flags)

    def _get_word_root(self, word: str) -> str:
        """
        Возвращает корень слова путем удаления распространенных окончаний.
        member, members, membership -> member
        manage, management, managing -> manage
        build, building, builder -> build
        """
        word_lower = word.lower()

        # Список окончаний для удаления (от самых длинных к коротким)
        suffixes = ['ship', 'ment', 'tion', 'sion', 'ing', 'ed', 'er', 'ers', 'es', 's']

        for suffix in suffixes:
            if word_lower.endswith(suffix) and len(word_lower) > len(suffix) + 3:
                root = word_lower[:-len(suffix)]
                if len(root) >= 4:
                    return root

        return word_lower

    def _get_game_text(self, game_dict: Dict) -> str:
        """Получает объединенный текст из всех источников"""
        text_parts = []

        if game_dict.get('summary'):
            text_parts.append(game_dict['summary'])
        if game_dict.get('storyline'):
            text_parts.append(game_dict['storyline'])
        if game_dict.get('rawg_description'):
            text_parts.append(game_dict['rawg_description'])
        if game_dict.get('wiki_description'):
            text_parts.append(game_dict['wiki_description'])

        return " ".join(text_parts) if text_parts else ""

    def _get_games_to_analyze(self) -> List[Dict]:
        """Максимально быстрая загрузка игр через сырой SQL"""
        import sys

        if self.game_id:
            with connection.cursor() as cursor:
                cursor.execute("""
                               SELECT id, name, summary, storyline, rawg_description, wiki_description
                               FROM games_game
                               WHERE id = %s
                               """, [self.game_id])
                row = cursor.fetchone()
                if row:
                    return [{
                        'id': row[0],
                        'name': row[1],
                        'summary': row[2] or '',
                        'storyline': row[3] or '',
                        'rawg_description': row[4] or '',
                        'wiki_description': row[5] or ''
                    }]
                return []

        if self.game_name:
            with connection.cursor() as cursor:
                cursor.execute("""
                               SELECT id, name, summary, storyline, rawg_description, wiki_description
                               FROM games_game
                               WHERE name ILIKE %s
                               ORDER BY id
                               """, [f'%{self.game_name}%'])
                games = []
                for row in cursor.fetchall():
                    games.append({
                        'id': row[0],
                        'name': row[1],
                        'summary': row[2] or '',
                        'storyline': row[3] or '',
                        'rawg_description': row[4] or '',
                        'wiki_description': row[5] or ''
                    })
                if self.limit:
                    games = games[:self.limit]
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
            if self._interrupted:
                break

            current_limit = min(batch_size, actual_limit - offset)
            current_offset = self.offset + offset

            with connection.cursor() as cursor:
                cursor.execute(sql, [current_offset, current_limit])

                batch = []
                for row in cursor.fetchall():
                    batch.append({
                        'id': row[0],
                        'name': row[1],
                        'summary': row[2] or '',
                        'storyline': row[3] or '',
                        'rawg_description': row[4] or '',
                        'wiki_description': row[5] or ''
                    })

                games.extend(batch)

            processed = len(games)
            percent = (processed / actual_limit) * 100 if actual_limit > 0 else 0
            sys.stderr.write(f"\r   ⬇️ Загружено: {processed}/{actual_limit} игр ({percent:.1f}%)")
            sys.stderr.flush()

        sys.stderr.write(f"\n   ✅ Загружено {len(games)} игр\n")
        sys.stderr.flush()

        return games

    def _find_cooccurrences_in_text(
            self,
            game: Dict,
            text: str
    ) -> List[Dict]:
        """
        Находит сочетания в радиусе N символов (независимо от порядка слов)
        """
        if not text or len(text) < 10:
            return []

        # Черный список слов, которые нужно исключить
        blacklist = {
            'your', 'with', 'and', 'the', 'for', 'you', 'are', 'can', 'will',
            'from', 'have', 'has', 'was', 'were', 'been', 'that', 'this', 'these',
            'those', 'they', 'them', 'their', 'there', 'what', 'which', 'where',
            'when', 'why', 'how', 'not', 'but', 'or', 'so', 'as', 'at', 'by',
            'in', 'into', 'on', 'to', 'of', 'off', 'up', 'down', 'out', 'over',
            'under', 'again', 'then', 'now', 'than', 'then', 'only', 'very',
            'just', 'but', 'do', 'does', 'did', 'doing', 'be', 'being', 'is',
            'are', 'am', 'some', 'any', 'no', 'yes', 'such', 'each', 'both',
            'all', 'most', 'more', 'much', 'many', 'little', 'few', 'own', 'same',
            'different', 'such', 'which', 'while', 'during', 'without', 'through',
            'between', 'among', 'within', 'across', 'along', 'after', 'before',
            'above', 'below', 'between', 'throughout', 'upon', 'via', 'versus',
            'against', 'amongst', 'through', 'toward', 'towards', 'underneath',
            'unlike', 'until', 'per', 'pro', 'con'
        }

        search_text = text if self.case_sensitive else text.lower()

        # Находим все вхождения целевого паттерна
        target_matches = []
        for match in self.compiled_pattern.finditer(search_text):
            matched_word = match.group(1) if self.is_group else match.group()
            target_matches.append({
                'start': match.start(),
                'end': match.end(),
                'matched_word': matched_word
            })

        if not target_matches:
            return []

        cooccurrences = []

        for target in target_matches:
            target_start = target['start']
            target_end = target['end']
            matched_word = target['matched_word']

            search_start = max(0, target_start - self.radius)
            search_end = min(len(search_text), target_end + self.radius)

            context = search_text[search_start:search_end]

            # Находим все слова в контексте
            for word_match in re.finditer(r'\b\w+(?:[-]\w+)*\b', context):
                original_word = word_match.group()
                word_lower = original_word.lower()
                word_abs_start = search_start + word_match.start()
                word_abs_end = search_start + word_match.end()

                if word_abs_start >= target_start and word_abs_end <= target_end:
                    continue

                if len(original_word) <= 3:
                    continue

                if word_lower in blacklist:
                    continue

                word_root = self._get_word_root(original_word)

                if word_abs_end <= target_start:
                    position = 'before'
                elif word_abs_start >= target_end:
                    position = 'after'
                else:
                    continue

                cooccurrences.append({
                    'type': 'single',
                    'word': word_root,
                    'original_word': original_word,
                    'position': position,
                    'target_word': matched_word,
                    'game_id': game['id'],
                    'game_name': game['name']
                })

            # Находим биграммы
            words_in_context = []
            for word_match in re.finditer(r'\b\w+(?:[-]\w+)*\b', context):
                original_word = word_match.group()
                word_lower = original_word.lower()
                word_abs_start = search_start + word_match.start()
                word_abs_end = search_start + word_match.end()

                if word_abs_start >= target_start and word_abs_end <= target_end:
                    continue

                if len(original_word) <= 3 or word_lower in blacklist:
                    continue

                word_root = self._get_word_root(original_word)

                words_in_context.append({
                    'word': word_root,
                    'original_word': original_word,
                    'start': word_abs_start,
                    'end': word_abs_end
                })

            for i in range(len(words_in_context) - 1):
                word1_root = words_in_context[i]['word']
                word2_root = words_in_context[i + 1]['word']
                word1_original = words_in_context[i]['original_word']
                word2_original = words_in_context[i + 1]['original_word']

                if words_in_context[i + 1]['start'] - words_in_context[i]['end'] <= 2:
                    bigram_root = f"{word1_root} {word2_root}"
                    bigram_original = f"{word1_original} {word2_original}"

                    word1_start = words_in_context[i]['start']
                    word2_end = words_in_context[i + 1]['end']

                    if word2_end <= target_start:
                        position = 'both_before'
                    elif word1_start >= target_end:
                        position = 'both_after'
                    elif words_in_context[i]['end'] <= target_start and words_in_context[i + 1]['start'] >= target_end:
                        position = 'split'
                    else:
                        continue

                    cooccurrences.append({
                        'type': 'bigram',
                        'word': bigram_root,
                        'original_phrase': bigram_original,
                        'position': position,
                        'target_word': matched_word,
                        'game_id': game['id'],
                        'game_name': game['name']
                    })

        return cooccurrences

    def _process_batch(self, batch: List[Dict]) -> Dict:
        """Обрабатывает батч игр в одном потоке"""
        batch_singles = defaultdict(lambda: {
            'count': 0,
            'before_count': 0,
            'after_count': 0,
            'games': set(),
            'before_games': set(),
            'after_games': set(),
            'target_words': defaultdict(int),
            'before_target_words': defaultdict(int),
            'after_target_words': defaultdict(int),
            'examples': [],
            'before_example': None,
            'after_example': None,
            'original_words': defaultdict(int)
        })

        batch_bigrams = defaultdict(lambda: {
            'count': 0,
            'both_before_count': 0,
            'both_after_count': 0,
            'split_count': 0,
            'games': set(),
            'target_words': defaultdict(int),
            'examples': [],
            'both_before_example': None,
            'both_after_example': None,
            'split_example': None,
            'original_phrases': defaultdict(int)
        })

        games_with_target = 0

        for game in batch:
            if self._interrupted:
                break

            text = self._get_game_text(game)

            if not text:
                continue

            cooccurrences = self._find_cooccurrences_in_text(game, text)

            if not cooccurrences:
                continue

            games_with_target += 1

            for cooc in cooccurrences:
                if cooc['type'] == 'single':
                    stats = batch_singles[cooc['word']]
                    stats['count'] += 1
                    stats['games'].add(cooc['game_id'])
                    stats['target_words'][cooc['target_word']] += 1
                    stats['original_words'][cooc['original_word']] += 1

                    if cooc['position'] == 'before':
                        stats['before_count'] += 1
                        stats['before_games'].add(cooc['game_id'])
                        stats['before_target_words'][cooc['target_word']] += 1
                        if stats['before_example'] is None:
                            stats['before_example'] = {
                                'game_name': cooc['game_name'],
                                'word': cooc['original_word'],
                                'target_word': cooc['target_word']
                            }
                    else:
                        stats['after_count'] += 1
                        stats['after_games'].add(cooc['game_id'])
                        stats['after_target_words'][cooc['target_word']] += 1
                        if stats['after_example'] is None:
                            stats['after_example'] = {
                                'game_name': cooc['game_name'],
                                'word': cooc['original_word'],
                                'target_word': cooc['target_word']
                            }

                    if len(stats['examples']) < 5:
                        stats['examples'].append({
                            'game_name': cooc['game_name'],
                            'word': cooc['original_word'],
                            'target_word': cooc['target_word']
                        })

                else:
                    stats = batch_bigrams[cooc['word']]
                    stats['count'] += 1
                    stats['games'].add(cooc['game_id'])
                    stats['target_words'][cooc['target_word']] += 1
                    stats['original_phrases'][cooc['original_phrase']] += 1

                    if cooc['position'] == 'both_before':
                        stats['both_before_count'] += 1
                        if stats['both_before_example'] is None:
                            stats['both_before_example'] = {
                                'game_name': cooc['game_name'],
                                'phrase': cooc['original_phrase'],
                                'target_word': cooc['target_word']
                            }
                    elif cooc['position'] == 'both_after':
                        stats['both_after_count'] += 1
                        if stats['both_after_example'] is None:
                            stats['both_after_example'] = {
                                'game_name': cooc['game_name'],
                                'phrase': cooc['original_phrase'],
                                'target_word': cooc['target_word']
                            }
                    else:
                        stats['split_count'] += 1
                        if stats['split_example'] is None:
                            stats['split_example'] = {
                                'game_name': cooc['game_name'],
                                'phrase': cooc['original_phrase'],
                                'target_word': cooc['target_word']
                            }

                    if len(stats['examples']) < 5:
                        stats['examples'].append({
                            'game_name': cooc['game_name'],
                            'phrase': cooc['original_phrase'],
                            'target_word': cooc['target_word']
                        })

        result = {
            'singles': {},
            'bigrams': {},
            'games_with_target': games_with_target,
            'games_processed': len(batch)
        }

        for word_root, stats in batch_singles.items():
            result['singles'][word_root] = {
                'count': stats['count'],
                'before_count': stats['before_count'],
                'after_count': stats['after_count'],
                'games': list(stats['games']),
                'before_games': len(stats['before_games']),
                'after_games': len(stats['after_games']),
                'games_count': len(stats['games']),
                'target_words': dict(stats['target_words']),
                'before_target_words': dict(stats['before_target_words']),
                'after_target_words': dict(stats['after_target_words']),
                'examples': stats['examples'],
                'before_example': stats['before_example'],
                'after_example': stats['after_example'],
                'original_words': dict(stats['original_words'])
            }

        for phrase_root, stats in batch_bigrams.items():
            result['bigrams'][phrase_root] = {
                'count': stats['count'],
                'both_before_count': stats['both_before_count'],
                'both_after_count': stats['both_after_count'],
                'split_count': stats['split_count'],
                'games': list(stats['games']),
                'games_count': len(stats['games']),
                'target_words': dict(stats['target_words']),
                'examples': stats['examples'],
                'both_before_example': stats['both_before_example'],
                'both_after_example': stats['both_after_example'],
                'split_example': stats['split_example'],
                'original_phrases': dict(stats['original_phrases'])
            }

        return result

    def _analyze_games_parallel(self, games: List[Dict]) -> Dict:
        """Многопоточный анализ игр с прогресс-баром"""
        total_games = len(games)

        batch_size_for_threads = max(1, total_games // self.threads)
        batches = [games[i:i + batch_size_for_threads] for i in range(0, total_games, batch_size_for_threads)]

        global_singles = defaultdict(lambda: {
            'count': 0,
            'before_count': 0,
            'after_count': 0,
            'games': set(),
            'before_games': set(),
            'after_games': set(),
            'target_words': defaultdict(int),
            'before_target_words': defaultdict(int),
            'after_target_words': defaultdict(int),
            'examples': [],
            'before_example': None,
            'after_example': None,
            'original_words': defaultdict(int)
        })

        global_bigrams = defaultdict(lambda: {
            'count': 0,
            'both_before_count': 0,
            'both_after_count': 0,
            'split_count': 0,
            'games': set(),
            'target_words': defaultdict(int),
            'examples': [],
            'both_before_example': None,
            'both_after_example': None,
            'split_example': None,
            'original_phrases': defaultdict(int)
        })

        total_games_with_target = 0
        stats_lock = threading.Lock()
        processed = 0
        start_time = time.time()
        last_report = time.time()
        last_processed = 0
        last_speed_time = start_time
        smooth_speed = 0.0
        last_percent_displayed = -1

        def format_time(seconds):
            if seconds < 60:
                return f"{seconds:.0f}сек"
            elif seconds < 3600:
                return f"{seconds / 60:.1f}мин"
            else:
                return f"{seconds / 3600:.1f}ч"

        def merge_results(batch_result):
            nonlocal total_games_with_target
            with stats_lock:
                total_games_with_target += batch_result['games_with_target']

                for word, data in batch_result['singles'].items():
                    target = global_singles[word]
                    target['count'] += data['count']
                    target['before_count'] += data['before_count']
                    target['after_count'] += data['after_count']
                    target['games'].update(data['games'])
                    target['before_games'].update(data['games'])
                    target['after_games'].update(data['games'])
                    for tw, cnt in data['target_words'].items():
                        target['target_words'][tw] += cnt
                    for tw, cnt in data['before_target_words'].items():
                        target['before_target_words'][tw] += cnt
                    for tw, cnt in data['after_target_words'].items():
                        target['after_target_words'][tw] += cnt
                    for orig_word, cnt in data['original_words'].items():
                        target['original_words'][orig_word] += cnt
                    if len(target['examples']) < 5 and data['examples']:
                        target['examples'].extend(data['examples'][:5 - len(target['examples'])])
                    if target['before_example'] is None and data.get('before_example'):
                        target['before_example'] = data['before_example']
                    if target['after_example'] is None and data.get('after_example'):
                        target['after_example'] = data['after_example']

                for phrase, data in batch_result['bigrams'].items():
                    target = global_bigrams[phrase]
                    target['count'] += data['count']
                    target['both_before_count'] += data['both_before_count']
                    target['both_after_count'] += data['both_after_count']
                    target['split_count'] += data['split_count']
                    target['games'].update(data['games'])
                    for tw, cnt in data['target_words'].items():
                        target['target_words'][tw] += cnt
                    for orig_phrase, cnt in data['original_phrases'].items():
                        target['original_phrases'][orig_phrase] += cnt
                    if len(target['examples']) < 5 and data['examples']:
                        target['examples'].extend(data['examples'][:5 - len(target['examples'])])
                    if target['both_before_example'] is None and data.get('both_before_example'):
                        target['both_before_example'] = data['both_before_example']
                    if target['both_after_example'] is None and data.get('both_after_example'):
                        target['both_after_example'] = data['both_after_example']
                    if target['split_example'] is None and data.get('split_example'):
                        target['split_example'] = data['split_example']

        sys.stderr.write(f"\n🔍 Анализируем {total_games} игр в {self.threads} потоков...\n")
        sys.stderr.write(f"📏 Радиус поиска: {self.radius} символов\n")
        sys.stderr.flush()

        with ThreadPoolExecutor(max_workers=self.threads) as executor:
            futures = [executor.submit(self._process_batch, batch) for batch in batches]

            for future in as_completed(futures):
                if self._interrupted:
                    break

                try:
                    batch_result = future.result()
                    merge_results(batch_result)

                    processed += batch_result['games_processed']

                    now = time.time()
                    if now - last_report >= 1.0 or processed == total_games:
                        percent = (processed / total_games) * 100 if total_games > 0 else 0

                        time_delta = now - last_speed_time
                        processed_delta = processed - last_processed

                        if time_delta > 0 and processed_delta > 0:
                            instant_speed = processed_delta / time_delta
                            if smooth_speed == 0:
                                smooth_speed = instant_speed
                            else:
                                smooth_speed = smooth_speed * 0.7 + instant_speed * 0.3

                        if smooth_speed > 0 and processed < total_games:
                            remaining_seconds = (total_games - processed) / smooth_speed
                            eta = format_time(remaining_seconds)
                        else:
                            eta = "расчет..."

                        current_percent_int = int(percent)
                        if current_percent_int != last_percent_displayed or processed == total_games:
                            progress_line = (f"\r📊 {processed}/{total_games} ({percent:.1f}%) | "
                                             f"Скорость: {smooth_speed:.1f} игр/сек | "
                                             f"Осталось: {eta} | "
                                             f"Найдено слов: {len(global_singles)} | "
                                             f"Найдено фраз: {len(global_bigrams)}    ")

                            sys.stderr.write(progress_line)
                            sys.stderr.flush()
                            last_percent_displayed = current_percent_int

                        last_report = now
                        last_processed = processed
                        last_speed_time = now

                except Exception as e:
                    if self.verbose:
                        sys.stderr.write(f"\n   ⚠️ Ошибка батча: {e}\n")

        elapsed = time.time() - start_time
        final_speed = processed / elapsed if elapsed > 0 else 0

        sys.stderr.write(f"\r📊 {processed}/{total_games} (100.0%) | "
                         f"Скорость: {final_speed:.1f} игр/сек | "
                         f"Осталось: 0сек | "
                         f"Найдено слов: {len(global_singles)} | "
                         f"Найдено фраз: {len(global_bigrams)}    \n")
        sys.stderr.flush()

        for stats in global_singles.values():
            stats['games'] = list(stats['games'])
            stats['before_games'] = len(stats['before_games'])
            stats['after_games'] = len(stats['after_games'])
            stats['games_count'] = len(stats['games'])
            stats['target_words'] = dict(stats['target_words'])
            stats['before_target_words'] = dict(stats['before_target_words'])
            stats['after_target_words'] = dict(stats['after_target_words'])
            stats['original_words'] = dict(stats['original_words'])

        for stats in global_bigrams.values():
            stats['games'] = list(stats['games'])
            stats['games_count'] = len(stats['games'])
            stats['target_words'] = dict(stats['target_words'])
            stats['original_phrases'] = dict(stats['original_phrases'])

        filtered_singles = [
            {
                'word': word,
                'count': data['count'],
                'before_count': data['before_count'],
                'after_count': data['after_count'],
                'games_count': data['games_count'],
                'before_games': data['before_games'],
                'after_games': data['after_games'],
                'examples': data['examples'],
                'before_example': data['before_example'],
                'after_example': data['after_example'],
                'target_words': data['target_words'],
                'before_target_words': data['before_target_words'],
                'after_target_words': data['after_target_words'],
                'original_words': data['original_words']
            }
            for word, data in global_singles.items()
            if data['count'] >= self.min_frequency
        ]
        filtered_singles.sort(key=lambda x: x['count'], reverse=True)

        # Используем max_words для ограничения количества слов
        words_limit = self.max_words if hasattr(self, 'max_words') and self.max_words > 0 else self.max_results
        filtered_singles = filtered_singles[:words_limit]

        filtered_bigrams = [
            {
                'word': word,
                'count': data['count'],
                'both_before_count': data['both_before_count'],
                'both_after_count': data['both_after_count'],
                'split_count': data['split_count'],
                'games_count': data['games_count'],
                'examples': data['examples'],
                'both_before_example': data['both_before_example'],
                'both_after_example': data['both_after_example'],
                'split_example': data['split_example'],
                'target_words': data['target_words'],
                'original_phrases': data['original_phrases']
            }
            for word, data in global_bigrams.items()
            if data['count'] >= self.min_frequency
        ]
        filtered_bigrams.sort(key=lambda x: x['count'], reverse=True)

        # Используем max_phrases для ограничения количества фраз
        phrases_limit = self.max_phrases if hasattr(self, 'max_phrases') and self.max_phrases > 0 else self.max_results
        filtered_bigrams = filtered_bigrams[:phrases_limit]

        return {
            'target_pattern': self.target_pattern,
            'target_words': self.target_words,
            'is_group': self.is_group,
            'radius': self.radius,
            'min_frequency': self.min_frequency,
            'total_games_analyzed': total_games,
            'games_with_target': total_games_with_target,
            'single_words': filtered_singles,
            'bigrams': filtered_bigrams,
            'interrupted': self._interrupted,
            'elapsed_time': elapsed,
            'games_per_second': final_speed
        }

    def _print_startup_info(self):
        """Выводит информацию о запуске в текстовом формате"""
        self._print_message("=" * 70)
        self._print_message("МАКСИМАЛЬНО УСКОРЕННЫЙ ПОИСК СОЧЕТАНИЙ СЛОВ")
        self._print_message("=" * 70)
        self._print_message("")

        if hasattr(self, 'is_regex') and self.is_regex:
            self._print_message(f"   Regex паттерн: {self.target_pattern}")
        elif self.is_group:
            self._print_message(f"   Группа слов: {', '.join(self.target_words)}")
        else:
            self._print_message(f"   Целевое слово: '{self.target_pattern}'")

        self._print_message(f"   Радиус поиска: {self.radius} символов (до и после)")
        self._print_message(f"   Мин. частота: {self.min_frequency}")
        self._print_message(f"   Макс. результатов: {self.max_results}")
        self._print_message(f"   Макс. слов для вывода: {self.max_words}")
        self._print_message(f"   Макс. фраз для вывода: {self.max_phrases}")
        self._print_message(f"   Регистр: {'Учитывается' if self.case_sensitive else 'Игнорируется'}")
        self._print_message(f"   Потоков: {self.threads}")

        if self.game_id:
            self._print_message(f"   Игра: ID {self.game_id}")
        elif self.game_name:
            self._print_message(f"   Поиск: '{self.game_name}'")
        else:
            self._print_message(f"   Диапазон: оффсет={self.offset}, лимит={self.limit or 'все'}")

        self._print_message("")
        self._print_message("=" * 70)
        self._print_message("")

    def _print_results_with_patterns(self, results: Dict, start_time: float):
        """Выводит результаты в текстовом формате с раздельной статистикой для позиций ДО и ПОСЛЕ"""
        elapsed = time.time() - start_time

        self._print_message("=" * 70)
        self._print_message("РЕЗУЛЬТАТЫ АНАЛИЗА")
        self._print_message("=" * 70)

        if results.get('interrupted'):
            self._print_message("")
            self._print_message("ВНИМАНИЕ: Анализ был прерван (частичные результаты)")
            self._print_message("")

        self._print_message("")
        self._print_message("Статистика:")
        self._print_message("-" * 40)
        if results['is_group']:
            self._print_message(f"   Группа: {', '.join(results['target_words'])}")
        else:
            self._print_message(f"   Слово: '{results['target_pattern']}'")
        self._print_message(f"   Радиус поиска: {results['radius']} символов")
        self._print_message(f"   Мин. частота: {results['min_frequency']}")
        self._print_message(f"   Проанализировано: {results['total_games_analyzed']} игр")
        self._print_message(f"   Найдено в: {results['games_with_target']} играх")
        self._print_message(f"   Время: {elapsed:.2f} сек")
        if results.get('games_per_second', 0) > 0:
            self._print_message(f"   Средняя скорость: {results['games_per_second']:.0f} игр/сек")

        # Одиночные слова
        self._print_message("")
        self._print_message(f"САМЫЕ ЧАСТЫЕ СЛОВА В РАДИУСЕ {results['radius']} СИМВОЛОВ (топ {self.max_words})")
        self._print_message("-" * 70)

        if not results['single_words']:
            self._print_message(f"   Нет сочетаний с частотой >= {results['min_frequency']}")
        else:
            for i, item in enumerate(results['single_words'][:self.max_words], 1):
                word_root = item['word']
                original_words = item.get('original_words', {})

                if original_words:
                    sorted_words = sorted(original_words.items(), key=lambda x: x[1], reverse=True)
                    words_list = []
                    for orig_word, freq in sorted_words[:5]:
                        words_list.append(f"'{orig_word}' ({freq})")
                    words_str = ", ".join(words_list)
                    if len(sorted_words) > 5:
                        words_str += f" и еще {len(sorted_words) - 5}"
                else:
                    words_str = f"'{word_root}'"

                before_count = item.get('before_count', 0)
                after_count = item.get('after_count', 0)
                before_games = item.get('before_games', 0)
                after_games = item.get('after_games', 0)

                self._print_message("")
                self._print_message(f"   {i}. Корень: '{word_root}'")
                self._print_message(f"       Слова: {words_str}")
                self._print_message(f"       Всего вхождений: {item['count']} раз(а)")
                self._print_message(f"       ДО целевого слова: {before_count} раз(а) (в {before_games} играх)")
                self._print_message(f"       ПОСЛЕ целевого слова: {after_count} раз(а) (в {after_games} играх)")
                self._print_message(f"       В играх: {item['games_count']}")

                if item.get('before_example'):
                    ex = item['before_example']
                    self._print_message(
                        f"       Пример (ДО): {ex['game_name']} (слово '{ex['word']}' рядом с '{ex['target_word']}')")
                elif item.get('after_example'):
                    ex = item['after_example']
                    self._print_message(
                        f"       Пример (ПОСЛЕ): {ex['game_name']} (слово '{ex['word']}' рядом с '{ex['target_word']}')")

        # Биграммы
        self._print_message("")
        self._print_message("")
        self._print_message(
            f"САМЫЕ ЧАСТЫЕ ФРАЗЫ (2 слова) В РАДИУСЕ {results['radius']} СИМВОЛОВ (топ {self.max_phrases})")
        self._print_message("-" * 70)

        if not results['bigrams']:
            self._print_message(f"   Нет фраз с частотой >= {results['min_frequency']}")
        else:
            for i, item in enumerate(results['bigrams'][:self.max_phrases], 1):
                phrase_root = item['word']
                original_phrases = item.get('original_phrases', {})

                if original_phrases:
                    sorted_phrases = sorted(original_phrases.items(), key=lambda x: x[1], reverse=True)
                    phrases_list = []
                    for orig_phrase, freq in sorted_phrases[:3]:
                        phrases_list.append(f"'{orig_phrase}' ({freq})")
                    phrases_str = ", ".join(phrases_list)
                    if len(sorted_phrases) > 3:
                        phrases_str += f" и еще {len(sorted_phrases) - 3}"
                else:
                    phrases_str = f"'{phrase_root}'"

                both_before_count = item.get('both_before_count', 0)
                both_after_count = item.get('both_after_count', 0)
                split_count = item.get('split_count', 0)

                self._print_message("")
                self._print_message(f"   {i}. Корни: '{phrase_root}'")
                self._print_message(f"       Фразы: {phrases_str}")
                self._print_message(f"       Всего вхождений: {item['count']} раз(а)")
                self._print_message(f"       Оба слова ДО целевого: {both_before_count} раз(а)")
                self._print_message(f"       Оба слова ПОСЛЕ целевого: {both_after_count} раз(а)")
                self._print_message(f"       Первое ДО, второе ПОСЛЕ: {split_count} раз(а)")
                self._print_message(f"       В играх: {item['games_count']}")

                if item.get('both_before_example'):
                    ex = item['both_before_example']
                    self._print_message(
                        f"       Пример (оба ДО): {ex['game_name']} (фраза '{ex['phrase']}' рядом с '{ex['target_word']}')")
                elif item.get('both_after_example'):
                    ex = item['both_after_example']
                    self._print_message(
                        f"       Пример (оба ПОСЛЕ): {ex['game_name']} (фраза '{ex['phrase']}' рядом с '{ex['target_word']}')")
                elif item.get('split_example'):
                    ex = item['split_example']
                    self._print_message(
                        f"       Пример (первое ДО, второе ПОСЛЕ): {ex['game_name']} (фраза '{ex['phrase']}' рядом с '{ex['target_word']}')")

        self._print_message("")
        self._print_message("=" * 70)

        if self.output_path:
            self._print_message("")
            self._print_message(f"Результаты сохранены в: {self.output_path}")

    def _print_message(self, message: str):
        """Выводит сообщение"""
        if self.output_file and not self.output_file.closed:
            self.output_file.write(message + "\n")
            self.output_file.flush()

        if self.original_stdout:
            self.original_stdout.write(message + "\n")
            self.original_stdout.flush()
        else:
            self.stdout.write(message)

    def _print_error(self, message: str):
        """Выводит ошибку"""
        if self.output_file and not self.output_file.closed:
            self.output_file.write(message + "\n")
            self.output_file.flush()

        if self.original_stdout:
            self.original_stdout.write(message + "\n")
            self.original_stdout.flush()
        else:
            self.stderr.write(message)

    def _setup_output_file(self):
        """Настраивает вывод в файл"""
        import os

        try:
            directory = os.path.dirname(self.output_path)
            if directory:
                os.makedirs(directory, exist_ok=True)

            self.original_stdout = sys.stdout
            self.output_file = open(self.output_path, 'w', encoding='utf-8')
            sys.stdout = self.output_file

        except Exception as e:
            self._print_error(f"Ошибка открытия файла: {e}")

    def _cleanup(self):
        """Очистка ресурсов"""
        if self.output_file:
            try:
                self.output_file.flush()
                self.output_file.close()
            except:
                pass
            self.output_file = None

        if self.original_stdout:
            sys.stdout = self.original_stdout
            self.original_stdout = None