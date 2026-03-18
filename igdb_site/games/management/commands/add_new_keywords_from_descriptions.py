# games/management/commands/add_new_keywords_from_descriptions.py
"""
Команда для добавления новых ключевых слов в игры на основе их описаний.
Находит все слова в тексте, нормализует их через WordNet и создаёт новые ключевые слова,
если их ещё нет в базе данных. Результаты записываются в файл.
"""

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db import transaction, IntegrityError
from games.models import Game, Keyword
from games.analyze import GameAnalyzerAPI
from games.analyze.wordnet_api import get_wordnet_api
import time
import os
import re
from datetime import datetime
from typing import Dict, Any, List, Set, Optional
from collections import defaultdict


class Command(BaseCommand):
    help = 'Добавляет новые ключевые слова в игры на основе их описаний'

    def add_arguments(self, parser):
        # Основные параметры
        parser.add_argument(
            '--output',
            type=str,
            default='add_new_keywords_results.txt',
            help='Путь к файлу для сохранения результатов (по умолчанию: add_new_keywords_results.txt)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Режим просмотра: найти ключевые слова, но не добавлять в БД (файл сохраняется)'
        )

        # Параметры для поиска конкретной игры
        parser.add_argument(
            '--game-name',
            type=str,
            help='Анализировать конкретную игру по названию (всегда берётся одна самая популярная)'
        )

        # Параметры массовой обработки
        parser.add_argument(
            '--limit',
            type=int,
            default=None,
            help='Максимальное количество игр для анализа'
        )
        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Пропустить первые N игр'
        )
        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Размер батча для обработки (по умолчанию 100)'
        )

        # Параметры текста
        parser.add_argument(
            '--min-text-length',
            type=int,
            default=50,
            help='Минимальная длина текста для анализа (по умолчанию 50 символов)'
        )
        parser.add_argument(
            '--min-word-length',
            type=int,
            default=3,
            help='Минимальная длина слова для добавления как ключевого (по умолчанию 3)'
        )

        # Источники текста
        parser.add_argument(
            '--use-wiki',
            action='store_true',
            help='Использовать ТОЛЬКО описание из Wikipedia (переопределяет умолчание)'
        )
        parser.add_argument(
            '--use-rawg',
            action='store_true',
            help='Использовать ТОЛЬКО описание из RAWG.io (переопределяет умолчание)'
        )
        parser.add_argument(
            '--use-storyline',
            action='store_true',
            help='Использовать ТОЛЬКО сторилайн игры (переопределяет умолчание)'
        )
        parser.add_argument(
            '--prefer-wiki',
            action='store_true',
            help='Предпочитать Wikipedia описание другим источникам (переопределяет умолчание)'
        )
        parser.add_argument(
            '--prefer-storyline',
            action='store_true',
            help='Предпочитать сторилайн основному описанию (переопределяет умолчание)'
        )
        parser.add_argument(
            '--combine-texts',
            action='store_true',
            help='Объединить описание и сторилайн (только IGDB) (переопределяет умолчание)'
        )
        parser.add_argument(
            '--combine-all-texts',
            action='store_true',
            help='Явно указать использование объединённого текста из всех источников (по умолчанию включено)'
        )

        # Дополнительные опции
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод в консоль'
        )
        parser.add_argument(
            '--no-progress',
            action='store_true',
            help='Отключить прогресс-бар'
        )
        parser.add_argument(
            '--clear-cache',
            action='store_true',
            help='Очистить кэш WordNet перед началом'
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.api = None
        self.wordnet_api = None
        self.stats = {
            'games_processed': 0,
            'games_with_text': 0,
            'games_with_new_keywords': 0,
            'games_skipped_no_text': 0,
            'games_skipped_short_text': 0,
            'total_words_found': 0,
            'unique_words_found': 0,
            'new_keywords_created': 0,
            'existing_keywords_found': 0,
            'words_not_in_wordnet': 0,
            'words_stop_words': 0,
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
        self.found_words_global: Set[str] = set()
        self.words_not_in_wordnet: Set[str] = set()
        self.stop_words_found: Set[str] = set()
        self.created_keywords_global: Set[int] = set()
        self.games_results: List[Dict[str, Any]] = []
        # По умолчанию используем комбинированный текст
        self.combine_all_texts = True

    def _get_stop_words(self) -> Set[str]:
        """
        Возвращает множество стоп-слов, которые не должны добавляться как ключевые слова.
        Только совсем ничего не значащие слова без контекста.
        """
        return {
            # Артикли и указатели
            'a', 'an', 'the', 'this', 'that', 'these', 'those',

            # Предлоги
            'about', 'above', 'across', 'after', 'against', 'along', 'among', 'around', 'at',
            'before', 'behind', 'below', 'beneath', 'beside', 'between', 'beyond', 'by',
            'down', 'during', 'except', 'for', 'from', 'in', 'inside', 'into', 'like',
            'near', 'of', 'off', 'on', 'onto', 'out', 'outside', 'over', 'past',
            'through', 'throughout', 'to', 'toward', 'under', 'underneath', 'until', 'up',
            'upon', 'with', 'within', 'without',

            # Союзы
            'and', 'but', 'or', 'nor', 'so', 'yet', 'although', 'because', 'since',
            'unless', 'until', 'while', 'as', 'if', 'than', 'that', 'though', 'when',

            # Местоимения
            'i', 'you', 'he', 'she', 'it', 'we', 'they',
            'me', 'him', 'her', 'us', 'them',
            'my', 'your', 'his', 'its', 'our', 'their',
            'who', 'whom', 'whose', 'which', 'what',

            # Вспомогательные глаголы
            'be', 'am', 'is', 'are', 'was', 'were', 'been',
            'have', 'has', 'had',
            'do', 'does', 'did',
            'will', 'would', 'can', 'could', 'may', 'might', 'must',

            # Количественные и отрицательные
            'both', 'each', 'every', 'either', 'neither', 'any', 'some', 'all',
            'much', 'many', 'few', 'little', 'most', 'more', 'less',
            'other', 'another', 'such', 'same',
            'no', 'nor', 'not', 'none', 'nothing', 'non',

            # Наречия
            'now', 'then', 'always', 'never', 'sometimes', 'often', 'usually',
            'already', 'yet', 'still', 'ever', 'once', 'twice',
            'here', 'there', 'near', 'nearby', 'far',
            'very', 'too', 'quite', 'almost', 'nearly', 'just', 'only', 'even',
            'how', 'why', 'when', 'where', 'however',

            # Прочие
            'also', 'too', 'as', 'well', 'either',

            # Числительные (базовые)
            'one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten',
            'first', 'second', 'third', 'fourth', 'fifth',
        }

    def _create_text_preparer(self):
        """Создаёт объект TextPreparer с нашими настройками"""
        # Создаём класс-обёртку с нужными атрибутами
        class CommandWrapper:
            def __init__(self, cmd):
                self.use_wiki = cmd.use_wiki
                self.use_rawg = cmd.use_rawg
                self.use_storyline = cmd.use_storyline
                self.prefer_wiki = cmd.prefer_wiki
                self.prefer_storyline = cmd.prefer_storyline
                self.combine_texts = cmd.combine_texts
                self.combine_all_texts = cmd.combine_all_texts

        from games.management.commands.analyzer.text_preparer import TextPreparer
        return TextPreparer(CommandWrapper(self))

    def _get_source_description(self) -> str:
        """Возвращает описание источника текста"""
        if self.use_wiki:
            return "Wikipedia (only)"
        if self.use_rawg:
            return "RAWG (only)"
        if self.use_storyline:
            return "Storyline (only)"
        if self.prefer_wiki:
            return "Wikipedia (preferred)"
        if self.prefer_storyline:
            return "Storyline (preferred)"
        if self.combine_texts:
            return "IGDB texts combined"
        if self.combine_all_texts:
            return "All texts combined (explicit)"
        # По умолчанию - комбинируем все тексты
        return "All texts combined (default)"

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        # Сохраняем опции
        self.output_path = options['output']
        self.dry_run = options['dry_run']
        self.game_name = options.get('game_name')
        self.limit = options.get('limit')
        self.offset = options.get('offset', 0)
        self.batch_size = options.get('batch_size', 100)
        self.min_text_length = options.get('min_text_length', 50)
        self.min_word_length = options.get('min_word_length', 3)
        self.verbose = options.get('verbose', False)
        self.no_progress = options.get('no_progress', False)
        self.clear_cache = options.get('clear_cache', False)

        # Сохраняем настройки текста
        self.use_wiki = options.get('use_wiki', False)
        self.use_rawg = options.get('use_rawg', False)
        self.use_storyline = options.get('use_storyline', False)
        self.prefer_wiki = options.get('prefer_wiki', False)
        self.prefer_storyline = options.get('prefer_storyline', False)
        self.combine_texts = options.get('combine_texts', False)

        # Явно указанный --combine-all-texts переопределяет умолчание
        explicit_combine_all = options.get('combine_all_texts', False)

        # Если ни одна опция не указана, используем combine-all-texts по умолчанию
        if not any([self.use_wiki, self.use_rawg, self.use_storyline,
                    self.prefer_wiki, self.prefer_storyline, self.combine_texts]):
            self.combine_all_texts = True
        else:
            # Если указана какая-то другая опция, combine-all-texts выключается
            self.combine_all_texts = explicit_combine_all

        try:
            # Выводим информацию о режиме
            self.stdout.write("=" * 70)
            self.stdout.write("🔍 ДОБАВЛЕНИЕ НОВЫХ КЛЮЧЕВЫХ СЛОВ ИЗ ОПИСАНИЙ ИГР")
            self.stdout.write("=" * 70)

            if self.dry_run:
                self.stdout.write(self.style.WARNING("🔧 РЕЖИМ ПРОСМОТРА (--dry-run)"))
                self.stdout.write(self.style.WARNING("   Ключевые слова НЕ будут добавлены в БД"))
            else:
                self.stdout.write(f"📁 Результаты будут сохранены в БД и в файл: {self.output_path}")

            self.stdout.write(f"📚 Источник текста: {self._get_source_description()}")
            self.stdout.write(f"📏 Мин. длина слова: {self.min_word_length}")
            self.stdout.write("")

            # Инициализируем компоненты
            self.stdout.write("🔧 Инициализация компонентов...")
            self.api = GameAnalyzerAPI(verbose=False)
            self.wordnet_api = get_wordnet_api(verbose=self.verbose)
            self.text_preparer = self._create_text_preparer()

            if not self.wordnet_api.is_available():
                self.stdout.write(self.style.WARNING("⚠️ WordNetAPI недоступен, нормализация будет ограничена"))

            self.stdout.write("✅ Компоненты готовы")
            self.stdout.write("")

            # Очищаем кэш если нужно
            if self.clear_cache and self.wordnet_api:
                self.stdout.write("🧹 Очистка кэша WordNet...")
                self.wordnet_api.clear_cache()
                self.stdout.write("✅ Кэш очищен")
                self.stdout.write("")

            # Получаем игры для анализа
            games_queryset = self._get_games_queryset()
            total_games = games_queryset.count()

            if total_games == 0:
                self.stdout.write(self.style.WARNING("⚠️ Нет игр для анализа"))
                return

            self.stdout.write(f"📊 Найдено игр для анализа: {total_games}")
            if self.offset > 0:
                self.stdout.write(f"📍 Пропускаем первые {self.offset} игр")
            if self.limit:
                self.stdout.write(f"🎯 Лимит: {self.limit} игр")
            self.stdout.write("")

            # Запускаем анализ
            self.stats['start_time'] = time.time()
            self._analyze_games(games_queryset, total_games)

            # Выводим итоговую статистику
            self._print_final_stats()

            # Сохраняем результаты в файл ВСЕГДА (и в dry-run тоже)
            self._save_results_to_file()

            if self.dry_run:
                self.stdout.write(self.style.WARNING("\n🔧 РЕЖИМ ПРОСМОТРА (--dry-run)"))
                self.stdout.write(self.style.WARNING("   Ключевые слова НЕ были добавлены в БД"))
                self.stdout.write(self.style.WARNING(f"   Но результаты сохранены в: {self.output_path}"))

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("\n⏹️ Обработка прервана пользователем"))
            self.stats['end_time'] = time.time()
            self._print_final_stats()
            self._save_results_to_file()
        except Exception as e:
            self.stderr.write(self.style.ERROR(f"\n❌ Ошибка: {e}"))
            if self.verbose:
                import traceback
                traceback.print_exc()
            raise

    def _get_games_queryset(self):
        """Возвращает QuerySet игр для анализа"""
        from django.db.models import Count

        if self.game_name:
            # Сначала ищем точное совпадение
            exact_matches = Game.objects.filter(name__iexact=self.game_name)

            if exact_matches.exists():
                # Точное совпадение есть - берём самую популярную из них
                games = exact_matches.order_by(
                    '-rating',
                    '-rating_count',
                    'id'
                )[:1]  # Только одну игру
                match_type = "точному названию (самая популярная)"
            else:
                # Точного совпадения нет - ищем частичное и берём самую популярную
                games = Game.objects.filter(
                    Q(name__icontains=self.game_name)
                ).order_by(
                    '-rating',
                    '-rating_count',
                    'id'
                )[:1]  # Только одну игру
                match_type = "частичному названию (самая популярная)"

            if self.verbose:
                count = games.count()
                if count > 0:
                    game = games.first()
                    rating_info = f" (рейтинг: {game.rating:.1f}, оценок: {game.rating_count})" if game.rating else ""
                    self.stdout.write(f"🔍 Найдена 1 игра по {match_type}: {game.name}{rating_info}")
                else:
                    self.stdout.write(f"🔍 Игр по названию '{self.game_name}' не найдено")

            return games

        # Все игры с сортировкой по ID
        return Game.objects.all().order_by('id')

    def _analyze_games(self, games_queryset, total_games):
        """Анализирует игры и добавляет ключевые слова"""
        # Применяем offset и limit
        if self.offset:
            games_queryset = games_queryset[self.offset:]
        if self.limit:
            games_queryset = games_queryset[:self.limit]

        games_list = list(games_queryset)
        games_to_process = len(games_list)

        if self.verbose:
            self.stdout.write(f"\n🔍 Начинаем анализ {games_to_process} игр...")
            if not self.no_progress:
                self.stdout.write("")

        # Создаём прогресс-бар если нужно
        if not self.no_progress and games_to_process > 1:
            from tqdm import tqdm
            pbar = tqdm(total=games_to_process, desc="Анализ игр", unit="game")
        else:
            pbar = None

        # Обрабатываем игры батчами для оптимизации
        for i in range(0, games_to_process, self.batch_size):
            batch = games_list[i:i + self.batch_size]

            for game in batch:
                try:
                    self._analyze_single_game(game)
                except Exception as e:
                    self.stats['errors'] += 1
                    if self.verbose:
                        self.stderr.write(f"\n❌ Ошибка при анализе игры {game.id} ({game.name}): {e}")

                if pbar:
                    pbar.update(1)

            # Обновляем прогресс в stats
            self.stats['games_processed'] = min(i + self.batch_size, games_to_process)

        if pbar:
            pbar.close()

    def _extract_words_from_text(self, text: str) -> List[str]:
        """
        Извлекает отдельные слова из текста.
        Возвращает список уникальных слов в нижнем регистре.
        """
        # Разбиваем текст на слова (только буквы, минимум self.min_word_length символов)
        words = re.findall(r'\b[a-zA-Z]{%d,}\b' % self.min_word_length, text.lower())

        # Убираем дубликаты, но сохраняем порядок для консистентности
        seen = set()
        unique_words = []
        for word in words:
            if word not in seen:
                seen.add(word)
                unique_words.append(word)

        return unique_words

    def _normalize_word(self, word: str) -> str:
        """
        Нормализует слово через WordNetAPI.
        Возвращает нормализованную форму.
        """
        if self.wordnet_api and self.wordnet_api.is_available():
            return self.wordnet_api.get_best_base_form(word)
        return word.lower()

    def _get_or_create_keyword(self, word: str, normalized: str) -> Optional[Keyword]:
        """
        Получает существующее ключевое слово или создаёт новое.
        Возвращает объект Keyword или None в случае ошибки.
        """
        # Проверяем, не является ли слово стоп-словом
        stop_words = self._get_stop_words()
        if normalized.lower() in stop_words:
            self.stats['words_stop_words'] += 1
            self.stop_words_found.add(normalized)
            if self.verbose:
                self.stdout.write(f"      ⚪ Слово '{normalized}' в списке стоп-слов, пропускаем")
            return None

        # Проверяем, есть ли слово в WordNet
        if self.wordnet_api and self.wordnet_api.is_available():
            # Проверяем наличие synsets для разных частей речи
            has_verb = len(self.wordnet_api.wordnet.synsets(normalized, pos='v')) > 0
            has_noun = len(self.wordnet_api.wordnet.synsets(normalized, pos='n')) > 0
            has_adj = len(self.wordnet_api.wordnet.synsets(normalized, pos='a')) > 0
            has_adv = len(self.wordnet_api.wordnet.synsets(normalized, pos='r')) > 0

            if not (has_verb or has_noun or has_adj or has_adv):
                # Слова нет в WordNet - не добавляем как ключевое слово
                self.stats['words_not_in_wordnet'] += 1
                self.words_not_in_wordnet.add(normalized)
                if self.verbose:
                    self.stdout.write(f"      ⚪ Слово '{normalized}' отсутствует в WordNet, пропускаем")
                return None

        # Сначала ищем по нормализованной форме
        existing = Keyword.objects.filter(name__iexact=normalized).first()
        if existing:
            self.stats['existing_keywords_found'] += 1
            return existing

        # Если не нашли, ищем по исходному слову (на всякий случай)
        existing = Keyword.objects.filter(name__iexact=word).first()
        if existing:
            self.stats['existing_keywords_found'] += 1
            return existing

        # Создаём новое ключевое слово
        if self.dry_run:
            # В режиме dry-run просто возвращаем фиктивный объект
            dummy = Keyword(id=-len(self.found_words_global), name=normalized)
            self.stats['new_keywords_created'] += 1
            self.created_keywords_global.add(dummy.id)
            return dummy

        try:
            with transaction.atomic():
                # Генерируем временный igdb_id (отрицательный, чтобы не конфликтовать с реальными)
                from django.db import models
                min_igdb = Keyword.objects.filter(igdb_id__lt=0).aggregate(models.Min('igdb_id'))['igdb_id__min']
                new_igdb_id = (min_igdb or 0) - 1

                # Создаём новое ключевое слово
                keyword = Keyword.objects.create(
                    igdb_id=new_igdb_id,
                    name=normalized,
                    category=None  # Без категории
                )

                self.stats['new_keywords_created'] += 1
                self.created_keywords_global.add(keyword.id)

                if self.verbose:
                    self.stdout.write(f"      ✨ Создано новое ключевое слово: '{normalized}' (ID: {keyword.id})")

                return keyword

        except IntegrityError as e:
            # Возможно, кто-то другой создал такое же слово в параллельном процессе
            existing = Keyword.objects.filter(name__iexact=normalized).first()
            if existing:
                self.stats['existing_keywords_found'] += 1
                return existing

            self.stats['errors'] += 1
            if self.verbose:
                self.stderr.write(f"      ❌ Ошибка при создании ключевого слова '{normalized}': {e}")
            return None
        except Exception as e:
            self.stats['errors'] += 1
            if self.verbose:
                self.stderr.write(f"      ❌ Ошибка при создании ключевого слова '{normalized}': {e}")
            return None

    def _analyze_single_game(self, game: Game):
        """Анализирует одну игру и добавляет найденные ключевые слова"""
        # Получаем текст для анализа
        text = self.text_preparer.prepare_text(game)

        if not text:
            self.stats['games_skipped_no_text'] += 1
            if self.verbose:
                self.stdout.write(f"  ⏭️ {game.name}: нет текста")
            return

        if len(text) < self.min_text_length:
            self.stats['games_skipped_short_text'] += 1
            if self.verbose:
                self.stdout.write(f"  ⏭️ {game.name}: текст слишком короткий ({len(text)} < {self.min_text_length})")
            return

        self.stats['games_with_text'] += 1

        # Извлекаем слова из текста
        raw_words = self._extract_words_from_text(text)

        if not raw_words:
            if self.verbose:
                self.stdout.write(f"  ⚪ {game.name}: нет слов для анализа")
            return

        # Нормализуем слова и собираем уникальные
        normalized_words = {}
        for word in raw_words:
            normalized = self._normalize_word(word)
            if normalized and len(normalized) >= self.min_word_length:
                normalized_words[normalized] = word

        if not normalized_words:
            if self.verbose:
                self.stdout.write(f"  ⚪ {game.name}: нет слов после нормализации")
            return

        self.stats['total_words_found'] += len(normalized_words)

        # Получаем существующие ключевые слова игры
        existing_game_keywords = set()
        for kw in game.keywords.all():
            existing_game_keywords.add(kw.name.lower())

        # Для каждого нормализованного слова
        keywords_to_add = []
        game_keywords_info = []

        for normalized, original in normalized_words.items():
            # Пропускаем если слово уже есть у игры
            if normalized.lower() in existing_game_keywords:
                continue

            # Получаем или создаём ключевое слово
            keyword = self._get_or_create_keyword(original, normalized)
            if keyword:
                keywords_to_add.append(keyword)
                game_keywords_info.append({
                    'id': keyword.id,
                    'name': keyword.name,
                    'original': original,
                    'normalized': normalized,
                    'is_new': keyword.id in self.created_keywords_global or keyword.id < 0
                })
                self.found_words_global.add(normalized)

        if keywords_to_add:
            self.stats['games_with_new_keywords'] += 1

            # Добавляем ключевые слова к игре (если не dry-run)
            if not self.dry_run:
                try:
                    with transaction.atomic():
                        game.keywords.add(*keywords_to_add)
                        game.update_cached_counts()

                        # Обновляем счётчики использования для ключевых слов
                        for kw in keywords_to_add:
                            if kw.id > 0:  # Только для реальных (не dry-run фиктивных)
                                kw.update_cached_count()
                except Exception as e:
                    self.stats['errors'] += 1
                    if self.verbose:
                        self.stderr.write(f"    ❌ Ошибка при добавлении ключевых слов к игре {game.id}: {e}")

            if self.verbose:
                new_count = sum(1 for k in game_keywords_info if k['is_new'])
                existing_count = len(game_keywords_info) - new_count
                self.stdout.write(f"  ✅ {game.name}: +{len(keywords_to_add)} ключевых слов "
                                  f"(новых: {new_count}, существовали в БД: {existing_count})")
        else:
            if self.verbose:
                self.stdout.write(f"  ⚪ {game.name}: нет новых ключевых слов для добавления")

        # Сохраняем результат для отчёта
        game_result = {
            'game_id': game.id,
            'game_name': game.name,
            'text_length': len(text),
            'unique_words_found': len(normalized_words),
            'keywords_added': len(keywords_to_add),
            'keywords': game_keywords_info
        }
        self.games_results.append(game_result)

    def _print_final_stats(self):
        """Выводит итоговую статистику"""
        elapsed = 0
        if self.stats['start_time'] and self.stats['end_time']:
            elapsed = self.stats['end_time'] - self.stats['start_time']

        self.stdout.write("\n" + "=" * 70)
        self.stdout.write("📊 ИТОГОВАЯ СТАТИСТИКА")
        self.stdout.write("=" * 70)
        self.stdout.write(f"🔄 Обработано игр: {self.stats['games_processed']}")
        self.stdout.write(f"📝 Игр с текстом: {self.stats['games_with_text']}")
        self.stdout.write(f"🎯 Игр с новыми ключевыми словами: {self.stats['games_with_new_keywords']}")
        self.stdout.write(f"⏭️ Пропущено (нет текста): {self.stats['games_skipped_no_text']}")
        self.stdout.write(f"⏭️ Пропущено (короткий текст): {self.stats['games_skipped_short_text']}")
        self.stdout.write(f"📊 Всего уникальных слов найдено в текстах: {self.stats['total_words_found']}")
        self.stdout.write(f"🔑 Уникальных нормализованных слов: {len(self.found_words_global)}")
        self.stdout.write(f"✨ Новых ключевых слов создано: {self.stats['new_keywords_created']}")
        self.stdout.write(f"📚 Существующих ключевых слов найдено: {self.stats['existing_keywords_found']}")
        self.stdout.write(f"⚪ Слов не из WordNet (пропущено): {self.stats['words_not_in_wordnet']}")
        self.stdout.write(f"⏹️ Стоп-слов (пропущено): {self.stats['words_stop_words']}")
        self.stdout.write(f"❌ Ошибок: {self.stats['errors']}")

        if elapsed > 0:
            games_per_second = self.stats['games_processed'] / elapsed
            self.stdout.write(f"⏱️ Время выполнения: {elapsed:.1f} секунд")
            self.stdout.write(f"⚡ Скорость: {games_per_second:.1f} игр/сек")

        if self.dry_run:
            self.stdout.write(self.style.WARNING("\n🔧 РЕЖИМ ПРОСМОТРА - изменения не сохранены в БД"))
        else:
            self.stdout.write(f"\n✅ Изменения сохранены в БД и в файл: {self.output_path}")

        self.stdout.write("=" * 70)

    def _save_results_to_file(self):
        """Сохраняет результаты анализа в файл"""
        try:
            # Создаём директорию если нужно
            os.makedirs(os.path.dirname(os.path.abspath(self.output_path)), exist_ok=True)

            with open(self.output_path, 'w', encoding='utf-8') as f:
                # Заголовок
                f.write("=" * 80 + "\n")
                f.write("РЕЗУЛЬТАТЫ АНАЛИЗА КЛЮЧЕВЫХ СЛОВ\n")
                f.write(f"Дата: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Режим: {'ПРОСМОТР (dry-run)' if self.dry_run else 'ОБЫЧНЫЙ'}\n")
                f.write(f"Источник текста: {self._get_source_description()}\n")
                f.write(f"Минимальная длина текста: {self.min_text_length}\n")
                f.write(f"Минимальная длина слова: {self.min_word_length}\n")
                f.write("=" * 80 + "\n\n")

                # Статистика
                f.write("📊 СТАТИСТИКА\n")
                f.write("-" * 40 + "\n")
                f.write(f"Обработано игр: {self.stats['games_processed']}\n")
                f.write(f"Игр с текстом: {self.stats['games_with_text']}\n")
                f.write(f"Игр с новыми ключевыми словами: {self.stats['games_with_new_keywords']}\n")
                f.write(f"Всего уникальных слов найдено: {self.stats['total_words_found']}\n")
                f.write(f"Уникальных нормализованных слов: {len(self.found_words_global)}\n")
                f.write(f"Новых ключевых слов создано: {self.stats['new_keywords_created']}\n")
                f.write(f"Существующих ключевых слов найдено: {self.stats['existing_keywords_found']}\n")
                f.write(f"Слов не из WordNet (пропущено): {self.stats['words_not_in_wordnet']}\n")
                f.write(f"Стоп-слов (пропущено): {self.stats['words_stop_words']}\n")
                f.write(f"Ошибок: {self.stats['errors']}\n\n")

                # Стоп-слова (пропущенные)
                if self.stop_words_found:
                    f.write("⏹️ СТОП-СЛОВА (ПРОПУЩЕНЫ)\n")
                    f.write("-" * 40 + "\n")
                    for word in sorted(self.stop_words_found):
                        f.write(f"  • {word}\n")
                    f.write("\n")

                # Слова не из WordNet (пропущенные)
                if self.words_not_in_wordnet:
                    f.write("⚪ СЛОВА НЕ ИЗ WORDNET (ПРОПУЩЕНЫ)\n")
                    f.write("-" * 40 + "\n")
                    for word in sorted(self.words_not_in_wordnet):
                        f.write(f"  • {word}\n")
                    f.write("\n")

                # Все найденные уникальные слова
                if self.found_words_global:
                    f.write("🔑 ВСЕ НАЙДЕННЫЕ УНИКАЛЬНЫЕ СЛОВА\n")
                    f.write("-" * 40 + "\n")
                    for word in sorted(self.found_words_global):
                        # Проверяем, было ли слово создано как новое или уже существовало
                        exists = Keyword.objects.filter(name__iexact=word).exists()
                        status = "✅" if exists else "⚪"
                        f.write(f"  {status} {word}\n")
                    f.write("\n")

                # Результаты по играм
                if self.games_results:
                    f.write("🎮 РЕЗУЛЬТАТЫ ПО ИГРАМ\n")
                    f.write("-" * 40 + "\n")

                    for game_result in sorted(self.games_results, key=lambda x: x['game_name']):
                        f.write(f"\n📌 {game_result['game_name']} (ID: {game_result['game_id']})\n")
                        f.write(f"   Длина текста: {game_result['text_length']} символов\n")
                        f.write(f"   Уникальных слов найдено: {game_result['unique_words_found']}\n")
                        f.write(f"   Ключевых слов добавлено: {game_result['keywords_added']}\n")

                        if game_result['keywords']:
                            f.write("   Ключевые слова:\n")
                            for kw in sorted(game_result['keywords'], key=lambda x: x['name']):
                                status = "✅" if kw['is_new'] else "📚"
                                f.write(f"      {status} {kw['name']} (ID: {kw['id']}) "
                                        f"[{kw['original']} → {kw['normalized']}]\n")

                        f.write("\n")

                f.write("=" * 80 + "\n")

            if self.verbose:
                self.stdout.write(self.style.SUCCESS(f"\n✅ Файл успешно сохранён: {self.output_path}"))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"\n❌ Ошибка при сохранении файла: {e}"))
            if self.verbose:
                import traceback
                traceback.print_exc()