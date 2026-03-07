# games/management/commands/analyzer/output_formatter.py
"""
Форматировщик вывода (полная совместимость со старой версией)
"""

from typing import Dict, Any
from games.models import Game


class OutputFormatter:
    """Форматирует вывод команды как в старой версии"""

    def __init__(self, command_instance):
        self.command = command_instance
        # text_preparer будет получаться из command_instance

    def _print_pattern_details_to_file(self, pattern_info: Dict[str, Any]):
        """Выводит детальную информацию о совпадениях паттернов в файл"""
        if not pattern_info:
            return

        if not hasattr(self.command, 'output_file') or self.command.output_file.closed:
            return

        has_found_matches = False
        has_skipped_matches = False

        for criteria_type, matches in pattern_info.items():
            for match in matches:
                if match.get('status') == 'found':
                    has_found_matches = True
                elif match.get('status') == 'skipped':
                    has_skipped_matches = True

        if not (has_found_matches or has_skipped_matches):
            return

        if has_found_matches:
            self.command.output_file.write("      🔍 Совпадения паттернов:\n")
            seen_matches = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'found':
                        match_key = (match['pattern'], match.get('matched_text', ''), criteria_type)
                        if match_key not in seen_matches:
                            seen_matches.add(match_key)
                            pattern_display = match['pattern']
                            if len(pattern_display) > 80:
                                pattern_display = pattern_display[:77] + "..."
                            self.command.output_file.write(
                                f"         • '{match.get('matched_text', '')}' ← {self._get_display_name(criteria_type)}: {pattern_display}\n")

        if has_skipped_matches and not getattr(self.command, 'hide_skipped', False):
            self.command.output_file.write("      ⏭️ Пропущенные критерии (уже существуют):\n")
            seen_skipped = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'skipped':
                        if match['name'] not in seen_skipped:
                            seen_skipped.add(match['name'])
                            self.command.output_file.write(
                                f"         • {match['name']} ({self._get_display_name(criteria_type)})\n")

    def print_game_header(self, game: Game, keywords_mode: bool):
        """Выводит заголовок для игры"""
        existing_criteria = self._get_existing_criteria_summary(game, keywords_mode)
        mode = 'ключевые слова' if keywords_mode else 'критерии'

        self.command.stdout.write(f"🎮 Анализируем игру: {game.name}")
        self.command.stdout.write(f"📊 Режим: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if keywords_mode else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
        self.command.stdout.write(f"📋 Существующие {mode}: {existing_criteria}")
        self.command.stdout.write(
            f"👁️ Игнорировать существующие: {'✅ ДА' if self.command.ignore_existing else '❌ НЕТ'}")
        self.command.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ДА' if self.command.hide_skipped else '❌ НЕТ'}")

    def print_game_results(self, game: Game, result: Dict[str, Any], keywords_mode: bool):
        """Выводит результаты анализа игры"""
        if not result['success']:
            self.command.stderr.write(f"❌ {result['error_message']}")
            return

        mode = 'ключевые слова' if keywords_mode else 'критерии'

        if result['has_results']:
            found_count = result['summary'].get('found_count', 0)
            mode_text = f"Найдены {mode}" if not self.command.ignore_existing else f"Найдены новые {mode}"
            self.command.stdout.write(f"🎯 {mode_text} для '{game.name}' ({found_count}):")

            # Отображаем найденные элементы
            for key, data in result['results'].items():
                if data['count'] > 0:
                    display_name = self._get_display_name(key)
                    item_names = [item['name'] for item in data['items']]
                    self.command.stdout.write(f"  📌 {display_name} ({data['count']}): {item_names}")

            # Выводим паттерны если verbose
            if self.command.verbose and 'pattern_info' in result:
                self._print_pattern_details(result['pattern_info'])
        else:
            self.command.stdout.write(f"ℹ️ {mode.capitalize()} не найдены")

    def print_game_in_batch(self, game: Game, index: int, result: Dict[str, Any],
                            stats: Dict[str, Any], **options):
        """Выводит результат игры в пакетной обработке - только найденные слова"""
        only_found = options.get('only_found', False)
        verbose = options.get('verbose', False)
        keywords = options.get('keywords', False)
        ignore_existing = options.get('ignore_existing', False)
        update_game = options.get('update_game', False)
        comprehensive_mode = options.get('comprehensive_mode', False)
        combined_mode = options.get('combined_mode', False)
        exclude_existing = options.get('exclude_existing', False)

        # Проверяем наличие text_preparer
        if not hasattr(self.command, 'text_preparer') or self.command.text_preparer is None:
            return

        # Проверяем условия для вывода
        should_skip = (
                only_found and not result['has_results'] and
                not (exclude_existing and update_game)
        )

        if should_skip:
            return

        stats['displayed_count'] += 1

        # Пишем в файл при наличии результатов
        has_output_file = (
                hasattr(self.command, 'output_file') and
                self.command.output_file and
                not self.command.output_file.closed
        )

        if has_output_file and result['has_results']:
            try:
                # Заголовок игры
                self.command.output_file.write(f"{index}. 🎮 {game.name} (ID: {game.id})\n")

                # Получаем текст игры для поиска
                game_text = self.command.text_preparer.prepare_text(game)
                game_text_lower = game_text.lower() if game_text else ""

                # Выводим найденные элементы
                for key, data in result['results'].items():
                    if data.get('count', 0) > 0:
                        display_name = self._get_display_name(key)
                        self.command.output_file.write(f"   📌 {display_name}:\n")

                        for item in data.get('items', []):
                            item_name = item['name']
                            item_id = item['id']
                            self.command.output_file.write(f"      • {item_name}")

                            # Для ключевых слов показываем где найдено
                            if key == 'keywords' and game_text:
                                # Ищем вхождение в тексте - улучшенный поиск
                                item_lower = item_name.lower()

                                # Разбиваем текст на слова для лучшего поиска
                                import re
                                words = re.findall(r'\b\w+\b', game_text_lower)

                                found_exact = False
                                context = ""

                                # Сначала ищем точное совпадение слова
                                if item_lower in words:
                                    found_exact = True
                                    # Находим позицию для контекста
                                    pos = game_text_lower.find(item_lower)
                                    if pos != -1:
                                        start = max(0, pos - 20)
                                        end = min(len(game_text), pos + len(item_name) + 20)
                                        context = game_text[start:end]
                                        if start > 0:
                                            context = "..." + context
                                        if end < len(game_text):
                                            context = context + "..."

                                # Если точного совпадения нет, ищем как часть слова
                                if not found_exact:
                                    # Используем регулярное выражение для поиска слова как части другого слова
                                    pattern = r'\b\w*' + re.escape(item_lower) + r'\w*\b'
                                    matches = list(re.finditer(pattern, game_text_lower))

                                    if matches:
                                        # Берем первое совпадение для контекста
                                        match = matches[0]
                                        start_pos = match.start()
                                        end_pos = match.end()

                                        # Находим позицию в оригинальном тексте с учетом регистра
                                        # Ищем оригинальный фрагмент
                                        matched_text = game_text[start_pos:end_pos]

                                        start = max(0, start_pos - 20)
                                        end = min(len(game_text), end_pos + 20)
                                        context = game_text[start:end]

                                        if start > 0:
                                            context = "..." + context
                                        if end < len(game_text):
                                            context = context + "..."

                                        # Подсвечиваем найденное слово в контексте
                                        self.command.output_file.write(
                                            f" (в слове: \"{context}\" → найдено как \"{matched_text}\")")
                                    else:
                                        # Если не нашли даже через regex, помечаем как форму слова
                                        self.command.output_file.write(f" (⚠️ форма слова)")
                                else:
                                    # Точное совпадение найдено
                                    self.command.output_file.write(f" (в слове: \"{context}\")")

                            self.command.output_file.write("\n")

                    self.command.output_file.write("\n")
                    self.command.output_file.flush()

            except Exception as e:
                # В случае ошибки выводим базовую информацию
                try:
                    self.command.output_file.write(f"{index}. 🎮 {game.name} (ID: {game.id})\n")
                    for key, data in result['results'].items():
                        if data.get('count', 0) > 0:
                            display_name = self._get_display_name(key)
                            item_names = [item['name'] for item in data.get('items', [])]
                            self.command.output_file.write(f"   📌 {display_name}: {', '.join(item_names)}\n")
                    self.command.output_file.write("\n")
                    self.command.output_file.flush()
                except:
                    pass

        # Вывод в терминал только если нет прогресс-бара и verbose
        show_in_terminal = (
                not self.command.progress_bar and
                verbose
        )

        if show_in_terminal and result['has_results']:
            found_count = result['summary'].get('found_count', 0)
            mode = 'ключевые слова' if keywords else 'критерии'

            if ignore_existing:
                mode = f"новые {mode}"

            self.command.stdout.write(f"🎯 Найдены {mode} для '{game.name}' ({found_count}):\n")

            for key, data in result['results'].items():
                if data.get('count', 0) > 0:
                    display_name = self._get_display_name(key)
                    item_names = [item['name'] for item in data.get('items', [])]
                    self.command.stdout.write(f"  📌 {display_name} ({data['count']}): {item_names}\n")

    def print_final_statistics(self, stats: Dict[str, Any], already_processed: int, total_games: int):
        """Выводит финальную статистику"""
        self.command.stdout.write("\n" + "=" * 60)

        if self.command.keywords:
            self.command.stdout.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КЛЮЧЕВЫЕ СЛОВА)")
        else:
            self.command.stdout.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КРИТЕРИИ)")

        self.command.stdout.write("=" * 60)

        # Показываем пропущенные ранее обработанные игры
        if already_processed > 0:
            self.command.stdout.write(f"⏭️ Пропущено ранее обработанных игр: {already_processed}")

        if self.command.keywords:
            processed_count = stats.get('keywords_processed', stats.get('processed', 0))
            self.command.stdout.write(f"🔄 Обработано новых игр: {processed_count}")
            self.command.stdout.write(f"🎯 Игр с найденными ключ. словами: {stats.get('keywords_found', 0)}")
            self.command.stdout.write(f"📈 Всего ключевых слов найдено: {stats.get('keywords_count', 0)}")

            if stats.get('keywords_not_found', 0) > 0:
                self.command.stdout.write(f"⚡ Игр без ключевых слов: {stats['keywords_not_found']}")
        else:
            self.command.stdout.write(f"🔄 Обработано новых игр: {stats.get('processed', 0)}")
            self.command.stdout.write(f"🎯 Игр с найденными критериями: {stats.get('found_count', 0)}")
            self.command.stdout.write(f"📈 Всего критериев найдено: {stats.get('total_criteria_found', 0)}")

            if stats.get('not_found_count', 0) > 0:
                self.command.stdout.write(f"⚡ Игр без критериев: {stats['not_found_count']}")

        total_skipped = stats['skipped_no_text'] + stats.get('skipped_short_text', 0) + (
            stats['keywords_not_found'] if self.command.keywords else stats['not_found_count']
        )

        self.command.stdout.write(f"⏭️ Всего пропущено игр: {total_skipped}")
        self.command.stdout.write(f"⏭️ Игр без текста: {stats['skipped_no_text']}")

        if 'skipped_short_text' in stats and stats['skipped_short_text'] > 0:
            self.command.stdout.write(f"⏭️ Игр с коротким текстом: {stats['skipped_short_text']}")

        if self.command.keywords and stats.get('keywords_not_found', 0) > 0:
            self.command.stdout.write(f"⏭️ Игр без ключевых слов: {stats['keywords_not_found']}")
        elif not self.command.keywords and stats.get('not_found_count', 0) > 0:
            self.command.stdout.write(f"⏭️ Игр без критериев: {stats['not_found_count']}")

        self.command.stdout.write(f"❌ Ошибок: {stats['errors']}")
        self.command.stdout.write(f"💾 Обновлено игр: {stats['updated']}")

        if stats['execution_time'] > 0:
            self.command.stdout.write(f"⏱️ Время выполнения: {stats['execution_time']:.1f} секунд")

        self.command.stdout.write("=" * 60)
        self.command.stdout.write("✅ Анализ успешно завершен")
        self.command.stdout.write("=" * 60)

    def print_interruption_statistics(self, stats: Dict[str, Any], already_processed: int):
        """Выводит статистику при прерывании"""
        self.command.stdout.write("📊 Частичная статистика (прервано):")

        total_skipped = stats['skipped_no_text'] + stats.get('skipped_short_text', 0) + already_processed

        if self.command.keywords:
            key_stats = ['keywords_processed', 'keywords_found', 'keywords_count', 'errors', 'updated']
        else:
            key_stats = ['processed', 'found_count', 'total_criteria_found', 'errors', 'updated']

        for key in key_stats:
            if key in stats:
                display_name = self._format_stat_key(key)
                self.command.stdout.write(f"{display_name}: {stats[key]}")

        self.command.stdout.write(f"⏭️ Всего пропущено игр: {total_skipped}")
        self.command.stdout.write(f"   ↳ без текста: {stats['skipped_no_text']}")

        if 'skipped_short_text' in stats and stats['skipped_short_text'] > 0:
            self.command.stdout.write(f"   ↳ с коротким текстом: {stats['skipped_short_text']}")

        if already_processed > 0:
            self.command.stdout.write(f"   ↳ ранее обработанных: {already_processed}")

        if stats['execution_time'] > 0:
            self.command.stdout.write(f"⏱️ Время выполнения до прерывания: {stats['execution_time']:.1f} секунд")

    def print_text_analysis_result(self, result: Dict[str, Any], keywords_mode: bool):
        """Выводит результат анализа текста"""
        mode = 'ключевые слова' if keywords_mode else 'критерии'

        self.command.stdout.write(f"\n📏 Длина текста: {result.get('text_length', 0)} символов")

        if result['has_results']:
            found_count = result['summary'].get('found_count', 0)
            self.command.stdout.write(f"🎯 Найдено {mode}: {found_count}")

            # Отображаем найденные элементы
            for key, data in result['results'].items():
                if data['count'] > 0:
                    display_name = self._get_display_name(key)
                    items = [item['name'] for item in data['items']]
                    self.command.stdout.write(f"  📌 {display_name}: {', '.join(items)}")
        else:
            self.command.stdout.write(f"⚡ {mode.capitalize()} не найдены")

    def _print_pattern_details(self, pattern_info: Dict[str, Any]):
        """Выводит детальную информацию о совпадениях паттернов"""
        if not pattern_info:
            return

        has_found_matches = False
        has_skipped_matches = False

        for criteria_type, matches in pattern_info.items():
            for match in matches:
                if match.get('status') == 'found':
                    has_found_matches = True
                elif match.get('status') == 'skipped' and not self.command.hide_skipped:
                    has_skipped_matches = True

        if not (has_found_matches or has_skipped_matches):
            return

        if has_found_matches:
            self.command.stdout.write("  🔍 Совпадения паттернов:")
            seen_matches = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'found':
                        match_key = (match['pattern'], match.get('matched_text', ''), criteria_type)
                        if match_key not in seen_matches:
                            seen_matches.add(match_key)
                            pattern_display = match['pattern']
                            if len(pattern_display) > 80:
                                pattern_display = pattern_display[:77] + "..."
                            self.command.stdout.write(
                                f"    • '{match.get('matched_text', '')}' ← {self._get_display_name(criteria_type)}: {pattern_display}")

        if has_skipped_matches and not self.command.hide_skipped:
            self.command.stdout.write("  ⏭️ Пропущенные критерии (уже существуют):")
            seen_skipped = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'skipped':
                        if match['name'] not in seen_skipped:
                            seen_skipped.add(match['name'])
                            self.command.stdout.write(
                                f"    • {match['name']} ({self._get_display_name(criteria_type)})")

    def _get_existing_criteria_summary(self, game: Game, keywords_mode: bool) -> str:
        """Возвращает строку с существующими критериями игры"""
        criteria_parts = []

        if keywords_mode:
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

    def _get_display_name(self, key: str) -> str:
        """Возвращает читаемое имя для типа критерия"""
        names = {
            'genres': 'Жанры',
            'themes': 'Темы',
            'perspectives': 'Перспективы',
            'game_modes': 'Режимы игры',
            'keywords': 'Ключевые слова'
        }
        return names.get(key, key)

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