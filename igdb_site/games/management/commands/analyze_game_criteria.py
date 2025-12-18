# games/management/commands/analyze_game_criteria.py
from games.models import Game
import os
from django.core.cache import cache
from django.db import transaction
from django.db.models import QuerySet
from games.models import Game

try:
    from .analyzer import AnalyzerCommandBase, GameAnalyzer, GameProcessor

    BaseCommand = AnalyzerCommandBase
except ImportError:
    from django.core.management.base import BaseCommand

    BaseCommand = BaseCommand


class Command(BaseCommand):
    help = 'Анализирует описание игры и определяет критерии (жанры, темы, перспективы, режимы) или ключевые слова'

    def add_arguments(self, parser):
        parser.add_argument('--game-id', type=int, help='ID игры в базе данных для анализа')
        parser.add_argument('--game-name', type=str, help='Название игры для анализа')
        parser.add_argument('--description', type=str, help='Текст описания для анализа')
        parser.add_argument('--limit', type=int, default=None, help='Лимит игр для анализа')
        parser.add_argument('--offset', type=int, default=0, help='Пропустить первые N игр')
        parser.add_argument('--update-game', action='store_true', help='Обновить найденные критерии')
        parser.add_argument('--min-text-length', type=int, default=10, help='Минимальная длина текста для анализа')
        parser.add_argument('--verbose', action='store_true', help='Подробный вывод процесса анализа')
        parser.add_argument('--output', type=str, help='Экспорт вывода в файл')
        parser.add_argument('--only-found', action='store_true',
                            help='Показывать только игры где были найдены критерии')
        parser.add_argument('--batch-size', type=int, default=1000, help='Размер батча для обработки')
        parser.add_argument('--hide-skipped', action='store_true',
                            help='Скрыть пропущенные критерии (уже существующие у игры)')
        parser.add_argument('--no-progress', action='store_true',
                            help='Отключить прогресс-бар')

        # НОВАЯ ОПЦИЯ: принудительный перезапуск
        parser.add_argument('--force-restart', action='store_true',
                            help='Начать обработку заново, игнорируя ранее обработанные игры')

        # ОПЦИЯ ДЛЯ РЕЖИМА КЛЮЧЕВЫХ СЛОВ
        parser.add_argument('--keywords', action='store_true',
                            help='Анализировать ТОЛЬКО ключевые слова (вместо обычных критериев)')

        text_source_group = parser.add_mutually_exclusive_group()
        text_source_group.add_argument('--use-storyline', action='store_true',
                                       help='Анализировать только сторилайн (игнорируя описание)')
        text_source_group.add_argument('--prefer-storyline', action='store_true',
                                       help='Предпочитать сторилайн описанию (если оба доступны)')
        text_source_group.add_argument('--combine-texts', action='store_true',
                                       help='Объединять описание и сторилайн для анализа')
        text_source_group.add_argument('--use-rawg', action='store_true',
                                       help='Анализировать только описание из RAWG.io')

        parser.add_argument('--ignore-existing', action='store_true',
                            help='Игнорировать существующие критерии и искать все паттерны')
        parser.add_argument('--clear-cache', action='store_true',
                            help='Очистить кеш перед началом обработки')

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        # Сохраняем оригинальные stdout/stderr
        self.original_stdout = self.stdout._out
        self.original_stderr = self.stderr._out
        self.output_file = None
        self.output_path = None  # Инициализируем путь к файлу

        # Настраиваем вывод в файл если указан
        if options.get('output'):
            try:
                output_path = options['output']
                self.output_path = output_path  # Сохраняем путь как атрибут
                self.output_file = open(output_path, 'w', encoding='utf-8')
                self.stdout._out = self.output_file
                self.stderr._out = self.output_file
                self.stdout.write(f"📁 Вывод будет сохранен в: {output_path}")
                self.stdout.write("-" * 60)
            except Exception as e:
                self.stderr.write(f"❌ Ошибка открытия файла: {e}")
                # Восстанавливаем потоки при ошибке
                self.stdout._out = self.original_stdout
                self.stderr._out = self.original_stderr

        try:
            # Инициализируем анализатор
            self.analyzer = GameAnalyzer(self)

            if options.get('clear_cache'):
                cache.clear()
                if self.output_file:
                    self.stdout.write("✅ Кеш очищен")
                else:
                    self.original_stdout.write("✅ Кеш очищен\n")

            # Сохраняем опции
            self._store_options(options)

            if self.verbose:
                self._print_options_summary()

            # Обрабатываем команду
            self.process_command()

        except ValueError as e:
            if self.output_file:
                self.stderr.write(f"❌ Ошибка в опциях: {e}")
            else:
                self.original_stderr.write(f"❌ Ошибка в опциях: {e}\n")
        except KeyboardInterrupt:
            if self.output_file:
                self.stdout.write("\n⏹️ Обработка прервана пользователем")
            else:
                self.original_stdout.write("\n⏹️ Обработка прервана пользователем\n")

            # Сохраняем состояние при прерывании
            if hasattr(self, 'analyzer') and hasattr(self.analyzer, 'clear_caches'):
                self.analyzer.clear_caches()

            # Выводим статистику если она есть
            if hasattr(self, 'stats'):
                self.print_stats("ПРЕРВАНО - ЧАСТИЧНАЯ СТАТИСТИКА")
        except Exception as e:
            if self.output_file:
                self.stderr.write(f"❌ Неожиданная ошибка: {e}")
                import traceback
                traceback.print_exc()
            else:
                self.original_stderr.write(f"❌ Неожиданная ошибка: {e}\n")
                import traceback
                traceback.print_exc(file=self.original_stderr)
        finally:
            self.cleanup()

    def cleanup(self):
        """Очистка ресурсов"""
        if hasattr(self, 'analyzer'):
            self.analyzer.clear_caches()

        # Восстанавливаем оригинальные потоки и закрываем файл
        if hasattr(self, 'output_file') and self.output_file:
            try:
                self.output_file.close()
                self.stdout._out = self.original_stdout
                self.stderr._out = self.original_stderr

                # Выводим сообщение в терминал о завершении
                if self.output_path:
                    self.original_stdout.write(f"\n✅ Результаты экспортированы в: {self.output_path}\n")

                    # Сообщаем о файле состояния
                    state_file = os.path.splitext(self.output_path)[0] + '_state.json'
                    if os.path.exists(state_file):
                        self.original_stdout.write(f"📝 Состояние сохранено в: {state_file}\n")

            except Exception as e:
                if hasattr(self, 'original_stderr'):
                    self.original_stderr.write(f"⚠️ Ошибка закрытия файла: {e}\n")

    def process_command(self):
        """Обрабатывает команду в зависимости от аргументов"""
        if self.game_id:
            self.analyze_single_game_by_id(self.game_id)
        elif self.game_name:
            self.analyze_games_by_name(self.game_name)
        elif self.description:
            self.analyze_description(self.description)
        else:
            self.analyze_all_games()

    def analyze_all_games(self):
        """Анализирует все игры в базе данных с батчингом"""
        from games.models import Game

        base_query = self._get_base_query()
        total_games = base_query.count()

        games_queryset = base_query[self.offset:]
        if self.limit:
            games_queryset = games_queryset[:self.limit]

        actual_count = games_queryset.count()

        if not self.only_found:
            mode = '🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'
            self.stdout.write(f"🔍 Анализируем {actual_count} игр из {total_games}...")
            self.stdout.write(f"📚 Источник: {self._get_text_source_description()}")
            self.stdout.write(f"⚙️ Режим: {mode}")
            self.stdout.write(f"🔄 Обновление: {'✅ ВКЛ' if self.update_game else '❌ ВЫКЛ'}")
            self.stdout.write(f"👁️ Игнорировать существующие: {'✅ ДА' if self.ignore_existing else '❌ НЕТ'}")
            self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ДА' if self.hide_skipped else '❌ НЕТ'}")
            self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")
            self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}")
            self.stdout.write("")

        # Создаем процессор и обрабатываем игры
        processor = GameProcessor(self)
        stats = processor.process_games_batch(games_queryset)

        # Сохраняем статистику
        self.stats = stats

        # Выводим статистику
        self.print_stats("СТАТИСТИКА АНАЛИЗА")

    def analyze_single_game_by_id(self, game_id: int):
        """Анализирует одну игру по ID"""
        from games.models import Game

        try:
            game = Game.objects.get(id=game_id)
            mode = 'ключевые слова' if self.keywords else 'критерии'
            self.stdout.write(f"🎮 Анализируем игру: {game.name}")
            self.stdout.write(f"📊 Режим: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
            self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")

            existing_criteria = self._get_existing_criteria_summary(game)
            self.stdout.write(f"📋 Существующие {mode}: {existing_criteria}")
            self.stdout.write(f"👁️ Игнорировать существующие: {'✅ ДА' if self.ignore_existing else '❌ НЕТ'}")
            self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ДА' if self.hide_skipped else '❌ НЕТ'}")

            text_to_analyze = self.get_text_to_analyze(game)
            if not text_to_analyze:
                self.stderr.write("❌ У игры нет текста для анализа")
                return

            # Используем ВСЕ паттерны сразу
            results, pattern_info = self.analyzer.analyze_all_patterns(
                text_to_analyze,
                game=game,
                ignore_existing=self.ignore_existing,
                collect_patterns=self.verbose,
                keywords_mode=self.keywords
            )

            # Если режим keywords, фильтруем результаты
            if self.keywords:
                results = {'keywords': results.get('keywords', [])}
                pattern_info = {'keywords': pattern_info.get('keywords', [])}

            criteria_count = sum(len(results[key]) for key in results)
            self._print_game_results(game, results, criteria_count, pattern_info)

            if self.update_game:
                if self.update_game_criteria(game, results):
                    mode_text = "ключевые слова" if self.keywords else "критерии"
                    self.stdout.write(self.style.SUCCESS(f"✅ {mode_text.capitalize()} обновлены в базе данных"))

                    final_criteria = self._get_existing_criteria_summary(game)
                    self.stdout.write(f"📋 Итоговые {mode_text}: {final_criteria}")
                else:
                    mode_text = "ключевых слов" if self.keywords else "критериев"
                    self.stdout.write(f"ℹ️ Нет {mode_text} для обновления")

        except Game.DoesNotExist:
            self.stderr.write(f"❌ Игра с ID {game_id} не найдена")

    def analyze_games_by_name(self, game_name: str):
        """Анализирует игры по названию (частичное совпадение)"""
        from games.models import Game

        games = Game.objects.filter(name__icontains=game_name)

        if not games.exists():
            self.stderr.write(f"❌ Игры с названием содержащим '{game_name}' не найдены")
            return

        self.stdout.write(f"🔍 Найдено {games.count()} игр с названием содержащим '{game_name}'")

        for game in games:
            self.analyze_single_game_by_id(game.id)

    def analyze_description(self, description: str):
        """Анализирует произвольный текст описания"""
        mode = 'ключевые слова' if self.keywords else 'критерии'
        self.stdout.write(f"🔍 Анализируем произвольный текст описания...")
        self.stdout.write(f"📊 Режим: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
        self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")

        # Используем ВСЕ паттерны сразу
        results, pattern_info = self.analyzer.analyze_all_patterns(
            description,
            ignore_existing=True,
            collect_patterns=self.verbose,
            keywords_mode=self.keywords
        )

        # Если режим keywords, фильтруем результаты
        if self.keywords:
            results = {'keywords': results.get('keywords', [])}
            pattern_info = {'keywords': pattern_info.get('keywords', [])}

        criteria_count = sum(len(results[key]) for key in results)

        if criteria_count == 0:
            self.stdout.write(f"ℹ️ {mode.capitalize()} не найдены")
            return

        self.stdout.write(f"\n🎯 Найдено {mode}: {criteria_count}")

        for criteria_type, items in results.items():
            if items:
                display_name = self._get_display_name(criteria_type)
                item_names = [item.name for item in items]
                self.stdout.write(f"  📌 {display_name} ({len(items)}): {item_names}")

                if self.verbose and criteria_type in pattern_info:
                    self._print_pattern_details({criteria_type: pattern_info[criteria_type]})

    def _store_options(self, options):
        """Сохраняет опции в атрибуты класса"""
        self.game_id = options.get('game_id')
        self.game_name = options.get('game_name')
        self.description = options.get('description')
        self.limit = options.get('limit')
        self.offset = options.get('offset')
        self.update_game = options.get('update_game', False)
        self.min_text_length = 0  # Оставляем 0, так как проверка в другом месте
        self.verbose = options.get('verbose', False)
        self.only_found = options.get('only_found', False)
        self.batch_size = options.get('batch_size', 1000)
        self.ignore_existing = options.get('ignore_existing', False)
        self.hide_skipped = options.get('hide_skipped', False)
        self.no_progress = options.get('no_progress', False)
        self.force_restart = options.get('force_restart', False)  # Новая опция

        # Ключевая опция
        self.keywords = options.get('keywords', False)

        # Опции источников текста
        self.use_storyline = options.get('use_storyline', False)
        self.prefer_storyline = options.get('prefer_storyline', False)
        self.combine_texts = options.get('combine_texts', False)
        self.use_rawg = options.get('use_rawg', False)

        # Разрешаем приоритет источников текста
        self.text_source_mode = self._resolve_text_source_priority()

    def _resolve_text_source_priority(self) -> str:
        """Разрешает приоритет опций источника текста"""
        if self.use_rawg:
            return 'use_rawg'
        if self.use_storyline:
            return 'use_storyline'
        elif self.prefer_storyline:
            return 'prefer_storyline'
        elif self.combine_texts:
            return 'combine_texts'
        else:
            return 'default'

    def get_text_to_analyze(self, game: Game) -> str:
        """Возвращает текст для анализа в зависимости от настроек"""
        has_summary = bool(game.summary and game.summary.strip())
        has_storyline = bool(game.storyline and game.storyline.strip())
        has_rawg = bool(game.rawg_description and game.rawg_description.strip())

        if self.text_source_mode == 'use_rawg':
            return game.rawg_description if has_rawg else ""

        if self.text_source_mode == 'use_storyline':
            return game.storyline if has_storyline else (game.summary if has_summary else "")

        if self.text_source_mode == 'prefer_storyline':
            if has_storyline:
                return game.storyline
            return game.summary if has_summary else ""

        if self.text_source_mode == 'combine_texts':
            texts = []
            if has_summary:
                texts.append(game.summary)
            if has_storyline:
                texts.append(game.storyline)
            return " ".join(texts) if texts else ""

        # default mode
        if has_summary:
            return game.summary
        return game.storyline if has_storyline else ""

    def _get_text_source_description(self) -> str:
        """Возвращает описание источника текста для анализа"""
        descriptions = {
            'use_rawg': 'ТОЛЬКО описание RAWG',
            'use_storyline': "ТОЛЬКО сторилайн",
            'prefer_storyline': "ПРЕДПОЧТИТЕЛЬНО сторилайн",
            'combine_texts': "ОБЪЕДИНЕННЫЙ текст",
            'default': "ПРЕДПОЧТИТЕЛЬНО описание IGDB"
        }
        return descriptions.get(self.text_source_mode, "Неизвестно")

    def _get_text_source_for_game(self, game: Game, text_to_analyze: str) -> str:
        """Определяет источник текста для отладочной информации"""
        if self.text_source_mode == 'combine_texts':
            return "объединенный текст"
        elif text_to_analyze == game.storyline:
            return "сторилайн"
        elif text_to_analyze == game.summary:
            return "описание IGDB"
        elif text_to_analyze == game.rawg_description:
            return "описание RAWG"
        else:
            return "неизвестный источник"

    def _get_base_query(self) -> QuerySet:
        """Возвращает базовый QuerySet для анализа"""
        return Game.objects.all().order_by('id')

    def _get_display_name(self, criteria_type: str) -> str:
        """Возвращает читаемое имя для типа критерия"""
        names = {
            'genres': 'Жанры',
            'themes': 'Темы',
            'perspectives': 'Перспективы',
            'game_modes': 'Режимы',
            'keywords': 'Ключевые слова'
        }
        return names.get(criteria_type, criteria_type)

    def _get_existing_criteria_summary(self, game: Game) -> str:
        """Возвращает строку с существующими критериями игры"""
        criteria_parts = []

        if self.keywords:
            # Только ключевые слова
            if game.keywords.exists():
                keyword_names = [keyword.name for keyword in game.keywords.all()[:5]]
                criteria_parts.append(f"ключевые слова: {keyword_names}" + ("..." if game.keywords.count() > 5 else ""))
        else:
            # Обычные критерии
            if game.genres.exists():
                genre_names = [genre.name for genre in game.genres.all()[:3]]
                criteria_parts.append(f"жанры: {genre_names}" + ("..." if game.genres.count() > 3 else ""))
            if game.themes.exists():
                theme_names = [theme.name for theme in game.themes.all()[:3]]
                criteria_parts.append(f"темы: {theme_names}" + ("..." if game.themes.count() > 3 else ""))
            if game.player_perspectives.exists():
                perspective_names = [perspective.name for perspective in game.player_perspectives.all()[:2]]
                criteria_parts.append(
                    f"перспективы: {perspective_names}" + ("..." if game.player_perspectives.count() > 2 else ""))
            if game.game_modes.exists():
                mode_names = [mode.name for mode in mode.game_modes.all()[:2]]
                criteria_parts.append(f"режимы: {mode_names}" + ("..." if game.game_modes.count() > 2 else ""))

        return ", ".join(criteria_parts) if criteria_parts else "нет"

    @transaction.atomic
    def update_game_criteria(self, game: Game, results: dict) -> bool:
        """Обновляет критерии игры в базе данных"""
        updated = False

        if self.keywords:
            # Обновляем только ключевые слова
            if 'keywords' in results:
                current_items = set(game.keywords.all())
                new_items = set(results['keywords'])

                if self.ignore_existing:
                    items_to_add = new_items - current_items
                else:
                    items_to_add = new_items - current_items

                if items_to_add:
                    if self.verbose:
                        self.stdout.write(f"   ➕ Добавлены ключевые слова: {[item.name for item in items_to_add]}")
                    game.keywords.add(*items_to_add)
                    updated = True
        else:
            # Обновляем обычные критерии
            field_mapping = {
                'genres': ('genres', game.genres),
                'themes': ('themes', game.themes),
                'perspectives': ('player_perspectives', game.player_perspectives),
                'game_modes': ('game_modes', game.game_modes),
            }

            for result_key, (field_name, current_manager) in field_mapping.items():
                if result_key in results:
                    current_items = set(current_manager.all())
                    new_items = set(results[result_key])

                    if self.ignore_existing:
                        items_to_add = new_items - current_items
                    else:
                        items_to_add = new_items - current_items

                    if items_to_add:
                        if self.verbose:
                            self.stdout.write(f"   ➕ Добавлены {field_name}: {[item.name for item in items_to_add]}")
                        getattr(game, field_name).add(*items_to_add)
                        updated = True

        if updated:
            game.save()

        return updated

    def _print_pattern_details(self, pattern_info: dict):
        """Выводит детальную информацию о совпадениях паттернов"""
        has_found_matches = False
        has_skipped_matches = False and not self.hide_skipped

        # Проверяем, есть ли что выводить
        for criteria_type, matches in pattern_info.items():
            for match in matches:
                if match.get('status') == 'found':
                    has_found_matches = True
                elif match.get('status') == 'skipped' and not self.hide_skipped:
                    has_skipped_matches = True

        if not (has_found_matches or has_skipped_matches):
            return

        # Выводим найденные совпадения
        if has_found_matches:
            self.stdout.write("  🔍 Совпадения паттернов:")
            seen_matches = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'found':
                        match_key = (match['pattern'], match['matched_text'], criteria_type)
                        if match_key not in seen_matches:
                            seen_matches.add(match_key)
                            pattern_display = match['pattern']
                            if len(pattern_display) > 80:
                                pattern_display = pattern_display[:77] + "..."
                            self.stdout.write(
                                f"    • '{match['matched_text']}' ← {self._get_display_name(criteria_type)}: {pattern_display}")

        # Выводим пропущенные критерии (если не скрыто)
        if has_skipped_matches and not self.hide_skipped:
            self.stdout.write("  ⏭️ Пропущенные критерии (уже существуют):")
            seen_skipped = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'skipped':
                        if match['name'] not in seen_skipped:
                            seen_skipped.add(match['name'])
                            self.stdout.write(f"    • {match['name']} ({self._get_display_name(criteria_type)})")

    def _print_game_results(self, game, results, criteria_count: int, pattern_info: dict):
        """Выводит результаты анализа для игры с информацией о паттернами"""
        # В режиме ignore-existing показываем только те критерии, которые будут обновлены
        if self.ignore_existing and self.update_game:
            filtered_results = {}
            actual_criteria_count = 0

            for criteria_type, items in results.items():
                existing_items = self.analyzer._get_existing_objects(game, criteria_type)
                existing_names = {item.name for item in existing_items}
                new_items = [item for item in items if item.name not in existing_names]

                if new_items:
                    filtered_results[criteria_type] = new_items
                    actual_criteria_count += len(new_items)

            if actual_criteria_count == 0:
                return

            criteria_count = actual_criteria_count
            results = filtered_results

        mode = 'ключевые слова' if self.keywords else 'критерии'
        mode_text = f"Найдены {mode}" if self.ignore_existing else f"Найдены новые {mode}"

        self.stdout.write(f"🎯 {mode_text} для '{game.name}' ({criteria_count}):")

        # Сначала выводим все найденные критерии
        for criteria_type, items in results.items():
            if items:
                display_name = self._get_display_name(criteria_type)
                item_names = [item.name for item in items]
                self.stdout.write(f"  📌 {display_name} ({len(items)}): {item_names}")

        # Затем выводим информацию о паттернах
        if self.verbose:
            self._print_pattern_details(pattern_info)

    def _print_options_summary(self):
        """Выводит сводку по опциям"""
        self.stdout.write("=" * 60)
        self.stdout.write("🎮 НАСТРОЙКИ АНАЛИЗА ИГР")
        self.stdout.write("=" * 60)
        self.stdout.write(f"📊 Режим анализа: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
        self.stdout.write(f"🔄 Режим обновления: {'✅ ВКЛ' if self.update_game else '❌ ВЫКЛ'}")
        self.stdout.write(f"🔍 Игнорировать существующие: {'✅ ВКЛ' if self.ignore_existing else '❌ ВЫКЛ'}")
        self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ВКЛ' if self.hide_skipped else '❌ ВЫКЛ'}")
        self.stdout.write(f"📏 Проверка текста: {'❌ ВЫКЛ'}")
        self.stdout.write(f"🗣️ Подробный вывод: {'✅ ВКЛ' if self.verbose else '❌ ВЫКЛ'}")
        self.stdout.write(f"🎯 Только с найденными: {'✅ ВКЛ' if self.only_found else '❌ ВЫКЛ'}")
        self.stdout.write(f"📚 Источник текста: {self._get_text_source_description()}")
        self.stdout.write(f"📦 Размер батча: {self.batch_size}")
        self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")
        self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}")
        self.stdout.write("=" * 60)
        self.stdout.write("")

    def print_stats(self, title: str = "СТАТИСТИКА"):
        """Выводит статистику"""
        if not hasattr(self, 'stats') or not self.stats:
            return

        mode = 'ключевых слов' if self.keywords else 'критериев'
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(f"📊 {title} ({mode})")
        self.stdout.write("=" * 60)

        # Добавляем время выполнения если есть
        if 'execution_time' in self.stats:
            self.stdout.write(f"⏱️  Время выполнения: {self.stats['execution_time']:.2f} секунд")
            self.stdout.write("-" * 60)

        for key, value in self.stats.items():
            if isinstance(value, (int, float)) and key != 'execution_time':
                display_key = self._format_stat_key(key)
                self.stdout.write(f"{display_key}: {value}")

    def _format_stat_key(self, key: str) -> str:
        """Форматирует ключ статистики для вывода"""
        formats = {
            'processed': '🔄 Обработано игр',
            'updated': '💾 Обновлено игр',
            'skipped_no_text': '⏭️ Пропущено (нет текста)',
            'errors': '❌ Ошибок',
            'found_count': '🎯 Игр с найденными критериями',
            'total_criteria_found': '📈 Всего критериев найдено',
            'displayed_count': '👁️ Показано игр',
        }
        return formats.get(key, key.capitalize())

    def init_stats(self, keys: list):
        """Инициализирует статистику"""
        self.stats = {key: 0 for key in keys}

    def update_stat(self, key: str, value: int = 1):
        """Обновляет статистику"""
        if hasattr(self, 'stats') and key in self.stats:
            self.stats[key] += value

    def get_stat(self, key: str) -> int:
        """Получает значение статистики"""
        return self.stats.get(key, 0) if hasattr(self, 'stats') else 0
