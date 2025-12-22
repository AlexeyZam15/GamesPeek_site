# games/management/commands/analyzer/analyzer_command_base.py
import os
import sys
from typing import Optional, TextIO, Dict, Any, List, Set, Tuple
from django.core.management.base import BaseCommand
from django.db import transaction
from django.core.cache import cache
from django.db.models import QuerySet

try:
    from games.models import Game
except ImportError:
    class Game:
        pass


class AnalyzerCommandBase(BaseCommand):
    """Базовый класс для команд анализа с расширенной функциональностью"""

    def __init__(self, stdout: Optional[TextIO] = None, stderr: Optional[TextIO] = None, **kwargs):
        super().__init__(stdout, stderr, **kwargs)
        self.output_path = None  # <-- ДОБАВИТЬ эту строку
        self.output_file = None
        self.file_output = None
        self.original_stdout = None
        self.original_stderr = None
        self.stats = {}
        self.analyzer = None

    def _generate_output_paths(self, base_name: str, keywords_mode: bool = False) -> Tuple[str, str]:
        """
        Генерирует пути для файла результатов и файла состояния.

        Args:
            base_name: Базовое название папки (например "keywords", "результаты")
            keywords_mode: True если режим ключевых слов

        Returns:
            Tuple[str, str]: (output_file_path, state_file_path)

        Примеры:
            "results", True -> ("results/keywords_20250115_143022.txt", "results/state_keywords.json")
            "results", False -> ("results/criteria_20250115_143022.txt", "results/state_criteria.json")
        """
        import os
        from datetime import datetime

        # Определяем режим анализа из параметра
        mode = 'keywords' if keywords_mode else 'criteria'

        # Создаем папку если нужно
        output_dir = base_name
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        # Генерируем имя файла результатов с временной меткой
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"{mode}_{timestamp}.txt"
        output_file_path = os.path.join(output_dir, output_filename) if output_dir else output_filename

        # Файл состояния всегда один для папки
        state_filename = f"state_{mode}.json"
        state_file_path = os.path.join(output_dir, state_filename) if output_dir else state_filename

        return output_file_path, state_file_path

    def _generate_unique_output_path(self, base_path: str) -> str:
        """
        Создает папку и генерирует уникальное имя файла.

        Примеры:
        - "результаты" -> "результаты/keywords_YYYYMMDD_HHMMSS.txt"
        - "результаты/анализ" -> "результаты/анализ/keywords_YYYYMMDD_HHMMSS.txt"
        """
        import os
        from datetime import datetime

        # Если base_path - это просто имя папки без расширения
        if '.' not in os.path.basename(base_path):
            # Создаем имя файла на основе режима и времени
            mode = 'keywords' if hasattr(self, 'keywords') and self.keywords else 'criteria'
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{mode}_{timestamp}.txt"

            # Создаем полный путь
            unique_path = os.path.join(base_path, filename)
        else:
            # Если указано конкретное имя файла
            directory = os.path.dirname(base_path)
            filename = os.path.basename(base_path)

            # Разделяем имя и расширение
            if '.' in filename:
                name_part, ext_part = filename.rsplit('.', 1)
                ext = '.' + ext_part
            else:
                name_part = filename
                ext = ''

            # Добавляем временную метку
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            unique_filename = f"{name_part}_{timestamp}{ext}"
            unique_path = os.path.join(directory, unique_filename) if directory else unique_filename

        return unique_path

    @transaction.atomic
    def update_game_criteria(self, games_results: List[Tuple['Game', Dict[str, List]]]) -> int:
        """Пакетное обновление критериев игр в базе данных"""
        if not games_results:
            return 0

        updated_count = 0

        try:
            # ВАЖНО: ManyToMany нельзя обновить через bulk_update
            # Нужно обновлять каждую игру отдельно
            for game, results in games_results:
                if self._update_single_game_criteria(game, results):
                    updated_count += 1

            return updated_count

        except Exception as e:
            if self.verbose:
                self.stderr.write(f"⚠️ Ошибка при пакетном обновлении: {e}")
            return 0

    def _store_options(self, options: Dict[str, Any]):
        """Сохраняет опции в атрибуты класса"""
        self.game_id = options.get('game_id')
        self.game_name = options.get('game_name')
        self.description = options.get('description')
        self.limit = options.get('limit')
        self.offset = options.get('offset')
        self.update_game = options.get('update_game', False)
        self.min_text_length = options.get('min_text_length', 0)
        self.verbose = options.get('verbose', False)
        self.only_found = options.get('only_found', False)
        self.batch_size = options.get('batch_size', 1000)
        self.ignore_existing = options.get('ignore_existing', False)
        self.hide_skipped = options.get('hide_skipped', False)
        self.no_progress = options.get('no_progress', False)
        self.force_restart = options.get('force_restart', False)
        self.keywords = options.get('keywords', False)

        self.use_storyline = options.get('use_storyline', False)
        self.prefer_storyline = options.get('prefer_storyline', False)
        self.combine_texts = options.get('combine_texts', False)
        self.use_rawg = options.get('use_rawg', False)
        self.use_wiki = options.get('use_wiki', False)
        self.prefer_wiki = options.get('prefer_wiki', False)
        self.combine_all_texts = options.get('combine_all_texts', False)

        self.text_source_mode = self._resolve_text_source_priority()

    def _resolve_text_source_priority(self) -> str:
        """Разрешает приоритет опций источника текста"""
        if self.use_wiki:
            return 'use_wiki'
        elif self.use_rawg:
            return 'use_rawg'
        elif self.use_storyline:
            return 'use_storyline'
        elif self.prefer_wiki:
            return 'prefer_wiki'
        elif self.prefer_storyline:
            return 'prefer_storyline'
        elif self.combine_all_texts:
            return 'combine_all_texts'
        elif self.combine_texts:
            return 'combine_texts'
        else:
            return 'default'

    def get_text_to_analyze(self, game: Game) -> str:
        """Возвращает текст для анализа в зависимости от настроек"""
        has_summary = bool(game.summary and game.summary.strip())
        has_storyline = bool(game.storyline and game.storyline.strip())
        has_rawg = bool(game.rawg_description and game.rawg_description.strip())
        has_wiki = bool(game.wiki_description and game.wiki_description.strip())

        if self.text_source_mode == 'use_wiki':
            return game.wiki_description if has_wiki else ""
        elif self.text_source_mode == 'use_rawg':
            return game.rawg_description if has_rawg else ""
        elif self.text_source_mode == 'prefer_wiki':
            if has_wiki:
                return game.wiki_description
            if has_rawg:
                return game.rawg_description
            if has_summary:
                return game.summary
            return game.storyline if has_storyline else ""
        elif self.text_source_mode == 'combine_all_texts':
            texts = []
            if has_summary:
                texts.append(game.summary)
            if has_storyline:
                texts.append(game.storyline)
            if has_rawg:
                texts.append(game.rawg_description)
            if has_wiki:
                texts.append(game.wiki_description)
            return " ".join(texts) if texts else ""
        elif self.text_source_mode == 'use_storyline':
            return game.storyline if has_storyline else (game.summary if has_summary else "")
        elif self.text_source_mode == 'prefer_storyline':
            if has_storyline:
                return game.storyline
            return game.summary if has_summary else ""
        elif self.text_source_mode == 'combine_texts':
            texts = []
            if has_summary:
                texts.append(game.summary)
            if has_storyline:
                texts.append(game.storyline)
            return " ".join(texts) if texts else ""
        else:
            if has_summary:
                return game.summary
            if has_storyline:
                return game.storyline
            if has_rawg:
                return game.rawg_description
            if has_wiki:
                return game.wiki_description
            return ""

    def _get_text_source_description(self) -> str:
        """Возвращает описание источника текста для анализа"""
        descriptions = {
            'use_wiki': "ТОЛЬКО Wikipedia описание",
            'use_rawg': "ТОЛЬКО описание RAWG",
            'use_storyline': "ТОЛЬКО сторилайн",
            'prefer_wiki': "ПРЕДПОЧТИТЕЛЬНО Wikipedia",
            'prefer_storyline': "ПРЕДПОЧТИТЕЛЬНО сторилайн",
            'combine_all_texts': "ОБЪЕДИНЕННЫЙ ВЕСЬ текст",
            'combine_texts': "ОБЪЕДИНЕННЫЙ текст (IGDB)",
            'default': "ПРЕДПОЧТИТЕЛЬНО описание IGDB"
        }
        return descriptions.get(self.text_source_mode, "Неизвестно")

    def _get_text_source_for_game(self, game: Game, text_to_analyze: str) -> str:
        """Определяет источник текста для отладочной информации"""
        if self.text_source_mode == 'combine_all_texts':
            return "объединенный весь текст"
        elif self.text_source_mode == 'combine_texts':
            return "объединенный текст IGDB"
        elif text_to_analyze == game.wiki_description:
            return "Wikipedia описание"
        elif text_to_analyze == game.storyline:
            return "сторилайн"
        elif text_to_analyze == game.summary:
            return "описание IGDB"
        elif text_to_analyze == game.rawg_description:
            return "описание RAWG"
        else:
            return "неизвестный источник"

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
            'keywords_processed': '🔄 Обработано игр (ключ. слова)',
            'keywords_found': '🎯 Игр с найденными ключ. словами',
            'keywords_count': '📈 Всего ключевых слов найдено',
        }
        return formats.get(key, key.capitalize())

    def setup_file_output(self, output_path: str):
        """Настраивает вывод в файл"""
        if output_path:
            try:
                # Создаем директорию если нужно
                directory = os.path.dirname(output_path)
                if directory:  # если путь содержит директорию
                    os.makedirs(directory, exist_ok=True)

                self.output_path = output_path  # Сохраняем путь
                self.output_file = open(output_path, 'w', encoding='utf-8')
                self.file_output = self.output_file  # Для обратной совместимости

                self.original_stdout = self.stdout._out
                self.original_stderr = self.stderr._out
                self.stdout._out = self.output_file
                self.stderr._out = self.output_file

                self.stdout.write(f"📁 Вывод будет сохранен в: {output_path}")
                self.stdout.write("-" * 60)
                return True
            except Exception as e:
                # Используем оригинальный stderr для вывода ошибки
                if self.original_stderr:
                    self.original_stderr.write(f"❌ Ошибка открытия файла {output_path}: {e}\n")
                else:
                    sys.stderr.write(f"❌ Ошибка открытия файла {output_path}: {e}\n")
                return False
        return False

    def close_file_output(self):
        """Закрывает файл вывода"""
        if self.output_file:
            try:
                self.output_file.close()
                if self.original_stdout:
                    self.stdout._out = self.original_stdout
                if self.original_stderr:
                    self.stderr._out = self.original_stderr
                # Проверить что output_path существует
                if hasattr(self, 'output_path') and self.output_path:
                    self.stdout.write(f"\n✅ Результаты экспортированы в: {self.output_path}")
            except Exception as e:
                self.stderr.write(f"⚠️ Ошибка закрытия файла: {e}")

    def init_stats(self, keys: list):
        """Инициализирует статистику"""
        self.stats = {key: 0 for key in keys}

    def update_stat(self, key: str, value: int = 1):
        """Обновляет статистику"""
        if key in self.stats:
            self.stats[key] += value

    def get_stat(self, key: str) -> int:
        """Получает значение статистики"""
        return self.stats.get(key, 0)

    def print_stats(self, title: str = "СТАТИСТИКА"):
        """Выводит статистику"""
        if not self.stats:
            return

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(f"📊 {title}")
        self.stdout.write("=" * 60)

        for key, value in self.stats.items():
            if isinstance(value, (int, float)):
                self.stdout.write(f"{self._format_stat_key(key)}: {value}")

    def _get_base_query(self) -> QuerySet:
        """Возвращает базовый QuerySet для анализа"""
        from games.models import Game
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
            if game.keywords.exists():
                keyword_names = [keyword.name for keyword in game.keywords.all()[:5]]
                criteria_parts.append(f"ключевые слова: {keyword_names}" + ("..." if game.keywords.count() > 5 else ""))
        else:
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
                mode_names = [mode.name for mode in game.game_modes.all()[:2]]
                criteria_parts.append(f"режимы: {mode_names}" + ("..." if game.game_modes.count() > 2 else ""))

        return ", ".join(criteria_parts) if criteria_parts else "нет"

    def _update_single_game_criteria(self, game: 'Game', results: Dict[str, List]) -> bool:
        """Обновляет критерии одной игры - БЕЗ транзакций"""
        updated = False

        if self.keywords:
            if 'keywords' in results and results['keywords']:
                current_items = set(game.keywords.all())
                new_items = set(results['keywords'])

                if self.ignore_existing:
                    items_to_add = new_items - current_items
                else:
                    items_to_add = new_items - current_items

                if items_to_add:
                    game.keywords.add(*items_to_add)
                    updated = True
        else:
            field_mapping = {
                'genres': ('genres', game.genres),
                'themes': ('themes', game.themes),
                'perspectives': ('player_perspectives', game.player_perspectives),
                'game_modes': ('game_modes', game.game_modes),
            }

            for result_key, (field_name, current_manager) in field_mapping.items():
                if result_key in results and results[result_key]:
                    current_items = set(current_manager.all())
                    new_items = set(results[result_key])

                    if self.ignore_existing:
                        items_to_add = new_items - current_items
                    else:
                        items_to_add = new_items - current_items

                    if items_to_add:
                        getattr(game, field_name).add(*items_to_add)
                        updated = True

        return updated

    def _print_pattern_details(self, pattern_info: Dict[str, List[Dict]]):
        """Выводит детальную информацию о совпадениях паттернов"""
        has_found_matches = False
        has_skipped_matches = False

        for criteria_type, matches in pattern_info.items():
            for match in matches:
                if match.get('status') == 'found':
                    has_found_matches = True
                elif match.get('status') == 'skipped' and not self.hide_skipped:
                    has_skipped_matches = True

        if not (has_found_matches or has_skipped_matches):
            return

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

        if has_skipped_matches and not self.hide_skipped:
            self.stdout.write("  ⏭️ Пропущенные критерии (уже существуют):")
            seen_skipped = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'skipped':
                        if match['name'] not in seen_skipped:
                            seen_skipped.add(match['name'])
                            self.stdout.write(f"    • {match['name']} ({self._get_display_name(criteria_type)})")

    def _print_game_results(self, game, results, criteria_count: int, pattern_info: Dict[str, List[Dict]]):
        """Выводит результаты анализа для игры с информацией о паттернами"""
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

        for criteria_type, items in results.items():
            if items:
                display_name = self._get_display_name(criteria_type)
                item_names = [item.name for item in items]
                self.stdout.write(f"  📌 {display_name} ({len(items)}): {item_names}")

        if self.verbose:
            self._print_pattern_details(pattern_info)

    def _print_options_summary(self):
        """Выводит сводку по опциям"""
        # Выводим в stdout (в файл)
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
        self.stdout.write(f"📦 Размер батча для анализа: {self.batch_size}")
        self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")
        self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}")
        self.stdout.write("=" * 60)
        self.stdout.write("")

        # Также выводим в терминал если не идет вывод в файл
        if not hasattr(self, 'output_file') or not self.output_file:
            original_out = self.original_stdout or sys.stdout
            original_out.write("=" * 60 + "\n")
            original_out.write("🎮 НАСТРОЙКИ АНАЛИЗА ИГР\n")
            original_out.write("=" * 60 + "\n")
            # ... остальные опции
