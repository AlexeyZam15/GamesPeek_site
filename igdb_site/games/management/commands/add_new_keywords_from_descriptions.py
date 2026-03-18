# games/management/commands/add_new_keywords_from_descriptions.py
"""
Команда для добавления новых ключевых слов в игры на основе их описаний.
Находит все слова в тексте, нормализует их через WordNet и создаёт новые ключевые слова,
если их ещё нет в базе данных. Результаты записываются в файл.
"""

from django.core.management.base import BaseCommand
from django.db.models import Q
from django.db import transaction, IntegrityError, models
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
            'new_keywords_created': 0,  # Теперь это уникальные новые ключевые слова
            'existing_keywords_found': 0,  # Уникальные существующие ключевые слова
            'words_not_in_wordnet': 0,  # Уникальные слова не из WordNet
            'words_stop_words': 0,  # Уникальные стоп-слова
            'errors': 0,
            'start_time': None,
            'end_time': None
        }
        self.found_words_global: Set[str] = set()  # Все найденные уникальные слова
        self.new_keywords_global: Set[str] = set()  # Уникальные новые ключевые слова
        self.words_not_in_wordnet: Set[str] = set()  # Уникальные слова не из WordNet
        self.stop_words_found: Set[str] = set()  # Уникальные стоп-слова
        self.created_keywords_global: Set[int] = set()
        # По умолчанию используем комбинированный текст
        self.combine_all_texts = True

        # Кэши для оптимизации
        self.wordnet_cache = {}  # Кэш для результатов проверки WordNet
        self.keyword_cache = {}  # Кэш для существующих ключевых слов
        self.keyword_id_cache = {}  # Кэш {название: id} для быстрого доступа
        self.stop_words = self._get_stop_words()  # Кэшируем стоп-слова

        # Компилируем регулярное выражение для скорости
        self.word_regex = None

    def _get_or_create_keywords_batch(self, words_with_norm: Dict[str, str], game_unique_existing: Set[str]) -> Dict[
        str, Optional[Keyword]]:
        """
        Пакетное получение или создание ключевых слов с использованием кэша.
        Возвращает словарь {нормализованное_слово: объект Keyword}
        """
        result = {}
        normalized_to_original = {}

        # Группируем слова по нормализованной форме
        for original, normalized in words_with_norm.items():
            normalized_lower = normalized.lower()
            if normalized_lower not in normalized_to_original:
                normalized_to_original[normalized_lower] = original

        normalized_words = list(normalized_to_original.keys())

        # Фильтруем стоп-слова (используем кэшированный set для скорости)
        valid_normalized = []
        for norm in normalized_words:
            if norm in self.stop_words:
                # Уникальные стоп-слова
                if norm not in self.stop_words_found:
                    self.stop_words_found.add(norm)
                    self.stats['words_stop_words'] += 1
                result[norm] = None
            else:
                valid_normalized.append(norm)

        if not valid_normalized:
            return result

        # Проверяем наличие в WordNet (пакетно)
        wordnet_check = self._check_word_in_wordnet_batch(valid_normalized)

        # Фильтруем слова не из WordNet
        wordnet_valid = []
        for norm in valid_normalized:
            if not wordnet_check.get(norm, True):
                # Уникальные слова не из WordNet
                if norm not in self.words_not_in_wordnet:
                    self.words_not_in_wordnet.add(norm)
                    self.stats['words_not_in_wordnet'] += 1
                result[norm] = None
            else:
                wordnet_valid.append(norm)

        if not wordnet_valid:
            return result

        # Используем предзагруженный кэш для существующих ключевых слов
        existing_keywords = {}
        keywords_to_create = []

        for norm in wordnet_valid:
            if norm in self.keyword_cache:
                existing_keywords[norm] = self.keyword_cache[norm]
                result[norm] = self.keyword_cache[norm]
                # Добавляем в множество уникальных существующих ключевых слов
                game_unique_existing.add(norm)
            else:
                keywords_to_create.append(norm)

        # Обрабатываем новые ключевые слова (для создания или dry-run)
        if keywords_to_create:
            # Уникальные новые ключевые слова
            for norm in keywords_to_create:
                if norm not in self.new_keywords_global:
                    self.new_keywords_global.add(norm)
                    self.stats['new_keywords_created'] += 1

            if self.dry_run:
                # В dry-run режиме создаём фиктивные объекты для статистики
                for norm in keywords_to_create:
                    dummy_id = -(len(self.found_words_global) + len(keywords_to_create) + 1)
                    dummy = Keyword(id=dummy_id, name=norm)
                    result[norm] = dummy
                    self.found_words_global.add(norm)
                    self.created_keywords_global.add(dummy_id)
            else:
                # Реальное создание ключевых слов
                try:
                    # Получаем минимальный отрицательный igdb_id
                    min_igdb = Keyword.objects.filter(igdb_id__lt=0).aggregate(models.Min('igdb_id'))['igdb_id__min']
                    next_igdb_id = (min_igdb or 0) - 1

                    # Создаём все ключевые слова одним запросом
                    created_kws = []
                    for norm in keywords_to_create:
                        kw = Keyword(
                            igdb_id=next_igdb_id,
                            name=norm,
                            category=None
                        )
                        created_kws.append(kw)
                        next_igdb_id -= 1

                    # Bulk create
                    Keyword.objects.bulk_create(created_kws)

                    # Получаем созданные ключевые слова и обновляем кэш
                    for kw in Keyword.objects.filter(name__iexact__in=keywords_to_create):
                        kw_name_lower = kw.name.lower()
                        self.keyword_cache[kw_name_lower] = kw
                        self.keyword_id_cache[kw_name_lower] = kw.id
                        self.created_keywords_global.add(kw.id)
                        result[kw_name_lower] = kw
                        self.found_words_global.add(kw_name_lower)

                except Exception as e:
                    self.stats['errors'] += 1
                    if self.verbose:
                        self.stderr.write(f"      ❌ Ошибка при пакетном создании ключевых слов: {e}")

        return result

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

        # Компилируем регулярное выражение для извлечения слов
        self.word_regex = re.compile(r'\b[a-zA-Z]{%d,}\b' % self.min_word_length)

        # Предзагружаем все существующие ключевые слова в кэш (огромная оптимизация)
        self.stdout.write("📚 Предзагрузка всех существующих ключевых слов в кэш...")
        all_keywords = Keyword.objects.all().only('id', 'name')
        for kw in all_keywords:
            kw_name_lower = kw.name.lower()
            self.keyword_cache[kw_name_lower] = kw
            self.keyword_id_cache[kw_name_lower] = kw.id
        self.stdout.write(f"✅ Загружено {len(self.keyword_cache)} ключевых слов")
        self.stdout.write("")

        # Создаём прогресс-бар если нужно
        if not self.no_progress and games_to_process > 1:
            from tqdm import tqdm

            # Выводим легенду значков перед прогресс-баром
            self.stdout.write("📊 Легенда статистики (уникальные слова):")
            self.stdout.write("  ✅ = игры с новыми ключевыми словами")
            self.stdout.write("  ✨ = новые уникальные ключевые слова (созданы)")
            self.stdout.write("  📚 = существующие ключевые слова (уникальные)")
            self.stdout.write("  ⚪ = уникальные слова пропущено (не в WordNet или стоп-слова)")
            self.stdout.write("")

            # Функция для форматирования статистики в прогресс-баре
            def format_stats(stats):
                return (f"✅:{stats['games_with_new_keywords']} "
                        f"✨:{stats['new_keywords_created']} "
                        f"📚:{stats['existing_keywords_found']} "
                        f"⚪:{stats['words_not_in_wordnet'] + stats['words_stop_words']}")

            pbar = tqdm(
                total=games_to_process,
                desc="Анализ игр",
                unit="game",
                bar_format='{l_bar}{bar:20}{r_bar}',
                postfix=format_stats(self.stats)
            )
        else:
            pbar = None

        # Кэш для существующих ключевых слов игр (оптимизация)
        games_keywords_cache = {}

        # Множество для отслеживания уникальных существующих ключевых слов
        unique_existing_keywords = set()

        # Обрабатываем игры батчами для оптимизации
        for i in range(0, games_to_process, self.batch_size):
            batch = games_list[i:i + self.batch_size]

            # Предзагружаем ключевые слова для всех игр в текущем батче одной выборкой
            if not self.dry_run:
                game_ids = [game.id for game in batch]
                # Используем prefetch_related для оптимизации
                games_with_keywords = Game.objects.filter(id__in=game_ids).prefetch_related('keywords')
                for game in games_with_keywords:
                    # Используем кэш ID для быстрого доступа
                    games_keywords_cache[game.id] = set(kw.name.lower() for kw in game.keywords.all())
            else:
                # В dry-run режиме просто заполняем пустыми множествами
                for game in batch:
                    games_keywords_cache[game.id] = set()

            # Собираем все ключевые слова для пакетного создания связей
            all_game_keywords_to_add = []  # Список кортежей (game_id, keyword_id)

            for game in batch:
                try:
                    # Передаём кэшированные ключевые слова в метод анализа
                    keywords_to_add, game_existing_keywords = self._analyze_single_game_with_cache(
                        game, games_keywords_cache.get(game.id, set())
                    )

                    # Обновляем статистику уникальных существующих ключевых слов
                    unique_existing_keywords.update(game_existing_keywords)

                    # Собираем для пакетного добавления
                    if keywords_to_add and not self.dry_run:
                        for keyword in keywords_to_add:
                            if keyword.id > 0:  # Только реальные ключевые слова
                                all_game_keywords_to_add.append((game.id, keyword.id))

                except Exception as e:
                    self.stats['errors'] += 1
                    if self.verbose:
                        self.stderr.write(f"\n❌ Ошибка при анализе игры {game.id} ({game.name}): {e}")

                if pbar:
                    pbar.update(1)
                    # Обновляем статистику существующих ключевых слов (уникальные)
                    self.stats['existing_keywords_found'] = len(unique_existing_keywords)
                    # Обновляем статистику в постфиксе прогресс-бара
                    pbar.set_postfix_str(format_stats(self.stats))

            # Пакетное добавление связей many-to-many
            if all_game_keywords_to_add and not self.dry_run:
                try:
                    self._bulk_add_game_keywords(all_game_keywords_to_add)
                except Exception as e:
                    self.stats['errors'] += 1
                    if self.verbose:
                        self.stderr.write(f"❌ Ошибка при пакетном добавлении связей: {e}")

            # Обновляем прогресс в stats
            self.stats['games_processed'] = min(i + self.batch_size, games_to_process)

        if pbar:
            pbar.close()

        # Финальное обновление статистики существующих ключевых слов
        self.stats['existing_keywords_found'] = len(unique_existing_keywords)

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
        self.stdout.write(f"🔑 Уникальных ключевых слов найдено: {len(self.found_words_global)}")
        self.stdout.write(f"✨ Новых уникальных ключевых слов создано: {self.stats['new_keywords_created']}")
        self.stdout.write(f"📚 Уникальных существующих ключевых слов найдено: {self.stats['existing_keywords_found']}")
        self.stdout.write(f"⚪ Уникальных слов не из WordNet (пропущено): {self.stats['words_not_in_wordnet']}")
        self.stdout.write(f"⏹️ Уникальных стоп-слов (пропущено): {self.stats['words_stop_words']}")
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

    def _analyze_single_game_with_cache(self, game: Game, existing_game_keywords: Set[str]) -> tuple:
        """
        Анализирует одну игру и возвращает:
        - список ключевых слов для добавления
        - множество уникальных существующих ключевых слов, найденных в этой игре
        """
        # Получаем текст для анализа
        text = self.text_preparer.prepare_text(game)

        if not text:
            self.stats['games_skipped_no_text'] += 1
            if self.verbose:
                self.stdout.write(f"  ⏭️ {game.name}: нет текста")
            return [], set()

        if len(text) < self.min_text_length:
            self.stats['games_skipped_short_text'] += 1
            if self.verbose:
                self.stdout.write(f"  ⏭️ {game.name}: текст слишком короткий ({len(text)} < {self.min_text_length})")
            return [], set()

        self.stats['games_with_text'] += 1

        # Быстрое извлечение слов из текста
        raw_words = self._extract_words_from_text_fast(text)

        if not raw_words:
            if self.verbose:
                self.stdout.write(f"  ⚪ {game.name}: нет слов для анализа")
            return [], set()

        # Пакетная нормализация слов
        normalized_dict = self._normalize_word_batch(raw_words)

        # Множество для уникальных существующих ключевых слов в этой игре
        game_unique_existing = set()

        # Фильтруем по длине и уникальности
        words_to_process = {}
        for original, normalized in normalized_dict.items():
            if normalized and len(normalized) >= self.min_word_length:
                normalized_lower = normalized.lower()
                # Проверяем, нет ли уже такого слова у игры
                if normalized_lower not in existing_game_keywords:
                    words_to_process[original] = normalized
                else:
                    # Это существующее ключевое слово игры - добавляем в статистику
                    if normalized_lower in self.keyword_cache:
                        game_unique_existing.add(normalized_lower)

        if not words_to_process and not game_unique_existing:
            if self.verbose:
                self.stdout.write(f"  ⚪ {game.name}: нет новых слов для анализа")
            return [], game_unique_existing

        # Пакетное получение/создание ключевых слов
        keyword_results = self._get_or_create_keywords_batch(words_to_process, game_unique_existing)

        # Собираем ключевые слова для добавления
        keywords_to_add = []

        for normalized, keyword in keyword_results.items():
            if keyword:
                keywords_to_add.append(keyword)
                # В dry-run режиме тоже добавляем в found_words_global (уже добавлено в _get_or_create_keywords_batch)
                if not self.dry_run:
                    self.found_words_global.add(normalized)

        if keywords_to_add:
            self.stats['games_with_new_keywords'] += 1

            if self.verbose:
                new_count = sum(1 for k in keywords_to_add if k.id in self.created_keywords_global or k.id < 0)
                existing_count = len(keywords_to_add) - new_count
                self.stdout.write(f"  ✅ {game.name}: +{len(keywords_to_add)} ключевых слов "
                                  f"(новых: {new_count}, существовали в БД: {existing_count})")
        else:
            if self.verbose and game_unique_existing:
                self.stdout.write(f"  📚 {game.name}: найдено {len(game_unique_existing)} существующих ключевых слов")
            elif self.verbose:
                self.stdout.write(f"  ⚪ {game.name}: нет новых ключевых слов для добавления")

        return keywords_to_add, game_unique_existing

    def _save_results_to_file(self):
        """Сохраняет результаты анализа в файл (только уникальные ключевые слова)"""
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

                # Статистика (только уникальные ключевые слова)
                f.write("📊 СТАТИСТИКА\n")
                f.write("-" * 40 + "\n")
                f.write(f"🔄 Обработано игр: {self.stats['games_processed']}\n")
                f.write(f"📝 Игр с текстом: {self.stats['games_with_text']}\n")
                f.write(f"✅ Игр с новыми ключевыми словами: {self.stats['games_with_new_keywords']}\n")
                f.write(f"⏭️ Пропущено (нет текста): {self.stats['games_skipped_no_text']}\n")
                f.write(f"⏭️ Пропущено (короткий текст): {self.stats['games_skipped_short_text']}\n")
                f.write(f"🔑 Уникальных ключевых слов найдено: {len(self.found_words_global)}\n")
                f.write(f"✨ Новых уникальных ключевых слов создано: {self.stats['new_keywords_created']}\n")
                f.write(f"📚 Уникальных существующих ключевых слов найдено: {self.stats['existing_keywords_found']}\n")
                f.write(f"⚪ Уникальных слов не из WordNet (пропущено): {self.stats['words_not_in_wordnet']}\n")
                f.write(f"⏹️ Уникальных стоп-слов (пропущено): {self.stats['words_stop_words']}\n")
                f.write(f"❌ Ошибок: {self.stats['errors']}\n\n")

                # Стоп-слова (пропущенные)
                if self.stop_words_found:
                    f.write("⏹️ СТОП-СЛОВА (ПРОПУЩЕНЫ)\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Всего: {len(self.stop_words_found)}\n")
                    for word in sorted(self.stop_words_found):
                        f.write(f"  ⚪ {word}\n")
                    f.write("\n")

                # Слова не из WordNet (пропущенные)
                if self.words_not_in_wordnet:
                    f.write("⚪ СЛОВА НЕ ИЗ WORDNET (ПРОПУЩЕНЫ)\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Всего: {len(self.words_not_in_wordnet)}\n")
                    for word in sorted(self.words_not_in_wordnet):
                        f.write(f"  ⚪ {word}\n")
                    f.write("\n")

                # Все найденные уникальные ключевые слова
                if self.found_words_global:
                    f.write("🔑 ВСЕ НАЙДЕННЫЕ УНИКАЛЬНЫЕ КЛЮЧЕВЫЕ СЛОВА\n")
                    f.write("-" * 40 + "\n")
                    f.write(f"Всего: {len(self.found_words_global)}\n")

                    # Разделяем на существующие и новые
                    existing_words = []
                    new_words = []

                    for word in self.found_words_global:
                        if word in self.keyword_cache:
                            existing_words.append(word)
                        else:
                            new_words.append(word)

                    # Сортируем оба списка
                    existing_words.sort()
                    new_words.sort()

                    # Сначала новые ключевые слова (✨)
                    if new_words:
                        f.write("\n  ✨ НОВЫЕ КЛЮЧЕВЫЕ СЛОВА:\n")
                        for word in new_words:
                            f.write(f"    ✨ {word}\n")

                    # Потом существующие ключевые слова (📚)
                    if existing_words:
                        f.write("\n  📚 СУЩЕСТВУЮЩИЕ КЛЮЧЕВЫЕ СЛОВА:\n")
                        for word in existing_words:
                            f.write(f"    📚 {word}\n")

                    f.write("\n")

                # Дополнительная статистика по уникальности
                f.write("📊 ДЕТАЛЬНАЯ СТАТИСТИКА УНИКАЛЬНОСТИ\n")
                f.write("-" * 40 + "\n")
                f.write(f"✨ Новые уникальные ключевые слова: {len(new_words) if 'new_words' in locals() else 0}\n")
                f.write(
                    f"📚 Существующие уникальные ключевые слова: {len(existing_words) if 'existing_words' in locals() else 0}\n")
                f.write(f"⚪ Уникальные слова не из WordNet: {len(self.words_not_in_wordnet)}\n")
                f.write(f"⏹️ Уникальные стоп-слова: {len(self.stop_words_found)}\n")
                f.write(
                    f"🔑 Всего уникальных слов обработано: {len(self.found_words_global) + len(self.words_not_in_wordnet) + len(self.stop_words_found)}\n\n")

                f.write("=" * 80 + "\n")

            if self.verbose:
                self.stdout.write(self.style.SUCCESS(f"\n✅ Файл успешно сохранён: {self.output_path}"))

        except Exception as e:
            self.stderr.write(self.style.ERROR(f"\n❌ Ошибка при сохранении файла: {e}"))
            if self.verbose:
                import traceback
                traceback.print_exc()

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
        self.batch_size = options.get('batch_size', 10000)
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

    def _bulk_add_game_keywords(self, game_keyword_pairs: List[tuple]):
        """
        Пакетное добавление связей many-to-many между играми и ключевыми словами.
        Использует сырой SQL для максимальной производительности.
        """
        from django.db import connection

        if not game_keyword_pairs:
            return

        # Группируем по game_id для обновления кэшей
        game_to_keywords = defaultdict(list)
        for game_id, keyword_id in game_keyword_pairs:
            game_to_keywords[game_id].append(keyword_id)

        # Используем сырой SQL для массовой вставки (гораздо быстрее чем add())
        with connection.cursor() as cursor:
            # PostgreSQL синтаксис
            values = []
            for game_id, keyword_id in game_keyword_pairs:
                values.append(f"({game_id}, {keyword_id})")

            # Вставляем пачкой, игнорируя дубликаты
            sql = f"""
                INSERT INTO games_game_keywords (game_id, keyword_id)
                VALUES {', '.join(values)}
                ON CONFLICT (game_id, keyword_id) DO NOTHING
            """
            cursor.execute(sql)

        # Обновляем только кэши игр, поле count у Keyword не обновляем
        for game_id, keyword_ids in game_to_keywords.items():
            try:
                game = Game.objects.get(id=game_id)
                game.update_cached_counts()
            except Game.DoesNotExist:
                pass

    def _extract_words_from_text_fast(self, text: str) -> List[str]:
        """
        Быстрое извлечение слов из текста с использованием скомпилированного regex.
        """
        # Используем скомпилированное регулярное выражение
        words = self.word_regex.findall(text.lower())

        # Используем dict.fromkeys для быстрого удаления дубликатов с сохранением порядка
        return list(dict.fromkeys(words))

    def _normalize_word_batch(self, words: List[str]) -> Dict[str, str]:
        """
        Пакетная нормализация слов через WordNetAPI с максимальным кэшированием.
        Возвращает словарь {исходное_слово: нормализованное_слово}
        """
        if not self.wordnet_api or not self.wordnet_api.is_available():
            return {word: word.lower() for word in words}

        result = {}
        words_to_process = []

        # Сначала проверяем кэш
        for word in words:
            if word in self.wordnet_cache:
                result[word] = self.wordnet_cache[word]
            else:
                words_to_process.append(word)

        # Пакетная обработка через WordNet (если поддерживается)
        if words_to_process and hasattr(self.wordnet_api, 'get_best_base_form_batch'):
            # Если есть пакетный метод, используем его
            normalized_batch = self.wordnet_api.get_best_base_form_batch(words_to_process)
            for word, normalized in zip(words_to_process, normalized_batch):
                self.wordnet_cache[word] = normalized
                result[word] = normalized
        else:
            # По одному с кэшированием
            for word in words_to_process:
                normalized = self.wordnet_api.get_best_base_form(word)
                self.wordnet_cache[word] = normalized
                result[word] = normalized

        return result

    def _check_word_in_wordnet_batch(self, words: List[str]) -> Dict[str, bool]:
        """
        Пакетная проверка наличия слов в WordNet с максимальным кэшированием.
        Возвращает словарь {слово: есть_в_wordnet}
        """
        if not self.wordnet_api or not self.wordnet_api.is_available():
            return {word: True for word in words}

        result = {}
        words_to_process = []

        # Проверяем кэш
        for word in words:
            cache_key = f"wordnet_exists_{word}"
            if cache_key in self.wordnet_cache:
                result[word] = self.wordnet_cache[cache_key]
            else:
                words_to_process.append(word)

        if not words_to_process:
            return result

        # Пакетная проверка
        for word in words_to_process:
            cache_key = f"wordnet_exists_{word}"
            # Проверяем наличие synsets для разных частей речи
            has_verb = len(self.wordnet_api.wordnet.synsets(word, pos='v')) > 0
            has_noun = len(self.wordnet_api.wordnet.synsets(word, pos='n')) > 0
            has_adj = len(self.wordnet_api.wordnet.synsets(word, pos='a')) > 0
            has_adv = len(self.wordnet_api.wordnet.synsets(word, pos='r')) > 0

            exists = has_verb or has_noun or has_adj or has_adv
            self.wordnet_cache[cache_key] = exists
            result[word] = exists

        return result

    def _analyze_single_game(self, game: Game):
        """Анализирует одну игру (обёртка для совместимости)"""
        # Получаем существующие ключевые слова игры
        existing_game_keywords = set(kw.name.lower() for kw in game.keywords.all())
        return self._analyze_single_game_with_cache(game, existing_game_keywords)

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
