# games/management/commands/analyzer/analyzer_command_base.py
import os
import sys
from typing import Optional, TextIO, Dict, Any, List, Set
from django.core.management.base import BaseCommand
from django.db import transaction
from django.core.cache import cache
from django.db.models import QuerySet

# Импортируем Game для аннотаций типов
try:
    from games.models import Game
except ImportError:
    # Заглушка для аннотаций
    class Game:
        pass


class AnalyzerCommandBase(BaseCommand):
    """
    Базовый класс для команд анализа с расширенной функциональностью
    """

    def __init__(self, stdout: Optional[TextIO] = None, stderr: Optional[TextIO] = None, **kwargs):
        super().__init__(stdout, stderr, **kwargs)
        self.output_file = None
        self.file_output = None
        self.original_stdout = None
        self.original_stderr = None
        self.stats = {}
        self.analyzer = None

    # ========== МЕТОДЫ ДЛЯ РАБОТЫ С ФАЙЛАМИ ==========

    def setup_file_output(self, output_path: str):
        """Настраивает вывод в файл"""
        if output_path:
            try:
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                self.output_file = output_path
                self.file_output = open(output_path, 'w', encoding='utf-8')
                self.original_stdout = self.stdout._out
                self.original_stderr = self.stderr._out
                self.stdout._out = self.file_output
                self.stderr._out = self.file_output
                self.stdout.write(f"📁 Вывод будет сохранен в: {output_path}")
                self.stdout.write("-" * 60)
                return True
            except Exception as e:
                self.stderr.write(f"❌ Ошибка открытия файла {output_path}: {e}")
                return False
        return False

    def close_file_output(self):
        """Закрывает файл вывода"""
        if self.file_output:
            try:
                self.file_output.close()
                if self.original_stdout:
                    self.stdout._out = self.original_stdout
                if self.original_stderr:
                    self.stderr._out = self.original_stderr
                self.stdout.write(f"\n✅ Результаты экспортированы в: {self.output_file}")
            except Exception as e:
                self.stderr.write(f"⚠️ Ошибка закрытия файла: {e}")

    # ========== МЕТОДЫ ДЛЯ СТАТИСТИКИ ==========

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

    # ========== МЕТОДЫ ДЛЯ АНАЛИЗА ИГР ==========

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

        # Параметры для сторилайна
        self.use_storyline = options.get('use_storyline', False)
        self.prefer_storyline = options.get('prefer_storyline', False)
        self.combine_texts = options.get('combine_texts', False)

        self.text_source_mode = self._resolve_text_source_priority()

    def _resolve_text_source_priority(self) -> str:
        """Разрешает приоритет опций источника текста"""
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
            'use_storyline': "ТОЛЬКО сторилайн",
            'prefer_storyline': "ПРЕДПОЧТИТЕЛЬНО сторилайн",
            'combine_texts': "ОБЪЕДИНЕННЫЙ текст",
            'default': "ПРЕДПОЧТИТЕЛЬНО описание"
        }
        return descriptions.get(self.text_source_mode, "Неизвестно")

    def _get_text_source_for_game(self, game: Game, text_to_analyze: str) -> str:
        """Определяет источник текста для отладочной информации"""
        if self.text_source_mode == 'combine_texts':
            return "объединенный текст"
        elif text_to_analyze == game.storyline:
            return "сторилайн"
        elif text_to_analyze == game.summary:
            return "описание"
        else:
            return "неизвестный источник"

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
            'game_modes': 'Режимы'
        }
        return names.get(criteria_type, criteria_type)

    def _get_existing_criteria_summary(self, game: Game) -> str:
        """Возвращает строку с существующими критериями игры"""
        criteria_parts = []

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

    @transaction.atomic
    def update_game_criteria(self, game: Game, results: Dict[str, List]) -> bool:
        """Обновляет критерии игры в базе данных"""
        updated = False

        field_mapping = {
            'genres': ('genres', game.genres),
            'themes': ('themes', game.themes),
            'perspectives': ('player_perspectives', game.player_perspectives),
            'game_modes': ('game_modes', game.game_modes),
        }

        for result_key, (field_name, current_manager) in field_mapping.items():
            current_items = set(current_manager.all())
            new_items = set(results[result_key])

            if self.ignore_existing:
                items_to_add = new_items - current_items
                if items_to_add:
                    if self.verbose:
                        self.stdout.write(f"   ➕ Добавлены {field_name}: {[item.name for item in items_to_add]}")
                    getattr(game, field_name).add(*items_to_add)
                    updated = True
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

    def _print_pattern_details(self, pattern_info: Dict[str, List[Dict]]):
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

    def _print_game_results(self, game, results, criteria_count: int, pattern_info: Dict[str, List[Dict]]):
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

        mode_text = "Найдены критерии" if self.ignore_existing else "Найдены новые критерии"
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
        self.stdout.write(f"📊 Режим обновления: {'✅ ВКЛ' if self.update_game else '❌ ВЫКЛ'}")
        self.stdout.write(f"🔍 Игнорировать существующие: {'✅ ВКЛ' if self.ignore_existing else '❌ ВЫКЛ'}")
        self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ВКЛ' if self.hide_skipped else '❌ ВЫКЛ'}")
        self.stdout.write(
            f"📏 Проверка текста: {'❌ ВЫКЛ' if self.min_text_length == 0 else f'✅ ВКЛ ({self.min_text_length} символов)'}")
        self.stdout.write(f"🗣️ Подробный вывод: {'✅ ВКЛ' if self.verbose else '❌ ВЫКЛ'}")
        self.stdout.write(f"🎯 Только с найденными: {'✅ ВКЛ' if self.only_found else '❌ ВЫКЛ'}")
        self.stdout.write(f"📚 Источник текста: {self._get_text_source_description()}")
        self.stdout.write(f"📦 Размер батча: {self.batch_size}")
        self.stdout.write("=" * 60)
        self.stdout.write("")
