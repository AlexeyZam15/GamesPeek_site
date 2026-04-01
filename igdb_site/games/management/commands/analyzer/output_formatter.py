# games/management/commands/analyzer/output_formatter.py
"""
Форматировщик вывода (полная совместимость со старой версией)
"""

from typing import Dict, Any, List
from games.models import Game


class OutputFormatter:
    """Форматирует вывод команды как в старой версии"""

    def __init__(self, command_instance):
        self.command = command_instance
        # text_preparer будет получаться из command_instance

    def _find_context_for_item(self, pattern_info: Dict, category: str, item_id: int, item_name: str) -> str:
        """
        Находит контекст для конкретного элемента по ID или имени.
        Возвращает строку с информацией о том, где был найден элемент.
        """
        if category not in pattern_info:
            return ""

        matches_for_category = pattern_info[category]
        if not matches_for_category:
            return ""

        item_name_lower = item_name.lower()

        # Ищем совпадение для этого элемента
        for match in matches_for_category:
            if match.get('status') != 'found':
                continue

            match_name = match.get('name', '')
            matched_text = match.get('matched_text', '')
            context = match.get('context', '')
            pattern = match.get('pattern', '')

            # Проверяем по имени
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

            # Проверяем по matched_text
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

    def _format_match_context(self, match: Dict) -> str:
        """
        Форматирует информацию о совпадении в читаемую строку.
        """
        matched_text = match.get('matched_text', '')
        context = match.get('context', '')
        matched_lemma = match.get('matched_lemma', '')

        if matched_text and context:
            # Очищаем контекст от лишних пробелов и переносов
            clean_context = ' '.join(context.split())
            if len(clean_context) > 60:
                clean_context = clean_context[:57] + "..."

            if matched_lemma and matched_lemma != matched_text:
                return f"(найдено как \"{matched_text}\" → лемма \"{matched_lemma}\" в: \"{clean_context}\")"
            else:
                return f"(найдено как \"{matched_text}\" в: \"{clean_context}\")"

        elif matched_text:
            if matched_lemma and matched_lemma != matched_text:
                return f"(найдено как \"{matched_text}\" → лемма \"{matched_lemma}\")"
            else:
                return f"(найдено как \"{matched_text}\")"

        elif context:
            clean_context = ' '.join(context.split())
            if len(clean_context) > 60:
                clean_context = clean_context[:57] + "..."
            return f"(найдено в: \"{clean_context}\")"

        return ""

    def _print_detailed_matches(self, pattern_info: Dict):
        """
        Выводит детальную информацию о ВСЕХ совпадениях (для --verbose)
        """
        if not pattern_info:
            return

        if not hasattr(self.command, 'output_file') or self.command.output_file.closed:
            return

        self.command.output_file.write("   🔍 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О СОВПАДЕНИЯХ:\n")

        has_any = False
        for category, matches in pattern_info.items():
            if not matches:
                continue

            display_name = self._get_display_name(category)
            self.command.output_file.write(f"      📌 {display_name}:\n")

            for match in matches:
                status = match.get('status', 'unknown')

                if status == 'found':
                    has_any = True
                    name = match.get('name', 'N/A')
                    matched_text = match.get('matched_text', '')
                    context = match.get('context', '')
                    matched_lemma = match.get('matched_lemma', '')

                    # Формируем строку вывода
                    output = f"         • {name}"

                    if matched_text:
                        output += f" → найдено как \"{matched_text}\""

                    if matched_lemma and matched_lemma != matched_text:
                        output += f" (лемма: \"{matched_lemma}\")"

                    if context:
                        output += f"\n            в контексте: \"{context}\""

                    self.command.output_file.write(output + "\n")

                elif status == 'skipped' and not getattr(self.command, 'hide_skipped', False):
                    name = match.get('name', 'N/A')
                    reason = match.get('reason', 'already_exists')
                    self.command.output_file.write(f"         ⏭️ {name} (пропущено: {reason})\n")

        if not has_any:
            self.command.output_file.write("         Нет найденных совпадений\n")

        self.command.output_file.write("\n")
        self.command.output_file.flush()

    def _find_match_context(self, pattern_info: Dict, category: str, item_id: int, item_name: str,
                            full_text: str) -> str:
        """
        Находит контекст, в котором было найдено совпадение для элемента.
        Возвращает строку с информацией о найденном тексте.
        """
        if category not in pattern_info:
            return ""

        for match in pattern_info[category]:
            # Проверяем, относится ли это совпадение к нашему элементу
            match_item_id = None
            match_item_name = None

            if category == 'keywords':
                match_item_id = match.get('keyword_id')
                match_item_name = match.get('name')
            else:
                match_item_name = match.get('name')

            # Сравниваем по ID (для ключевых слов) или по имени
            if (match_item_id and match_item_id == item_id) or (match_item_name and match_item_name == item_name):
                # Нашли совпадение
                matched_text = match.get('matched_text', '')
                context = match.get('context', '')

                if matched_text:
                    if context:
                        # Очищаем контекст от лишних пробелов
                        clean_context = ' '.join(context.split())
                        if len(clean_context) > 50:
                            clean_context = clean_context[:47] + "..."
                        return f"(найдено как \"{matched_text}\" в: \"{clean_context}\")"
                    else:
                        return f"(найдено как \"{matched_text}\")"
                elif context:
                    clean_context = ' '.join(context.split())
                    if len(clean_context) > 50:
                        clean_context = clean_context[:47] + "..."
                    return f"(найдено в: \"{clean_context}\")"
                break

        return ""

    def _print_all_pattern_matches(self, pattern_info: Dict, full_text: str):
        """
        Выводит все найденные паттерны с контекстом (для --verbose)
        """
        if not pattern_info:
            return

        if not hasattr(self.command, 'output_file') or self.command.output_file.closed:
            return

        self.command.output_file.write("   🔍 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О СОВПАДЕНИЯХ:\n")

        for category, matches in pattern_info.items():
            if not matches:
                continue

            display_name = self._get_display_name(category)
            self.command.output_file.write(f"      📌 {display_name}:\n")

            for match in matches:
                status = match.get('status', 'unknown')

                if status == 'found':
                    name = match.get('name', 'N/A')
                    matched_text = match.get('matched_text', '')
                    context = match.get('context', '')

                    # Формируем строку вывода
                    output = f"         • {name}"

                    if matched_text:
                        output += f" → найдено как \"{matched_text}\""

                    if context:
                        clean_context = ' '.join(context.split())
                        if len(clean_context) > 60:
                            clean_context = clean_context[:57] + "..."
                        output += f" в контексте: \"{clean_context}\""

                    # Добавляем паттерн если он есть (для очень подробного вывода)
                    if 'pattern' in match and len(match['pattern']) < 50:
                        output += f" (паттерн: {match['pattern']})"

                    self.command.output_file.write(output + "\n")

                elif status == 'skipped' and not getattr(self.command, 'hide_skipped', False):
                    name = match.get('name', 'N/A')
                    reason = match.get('reason', 'already_exists')
                    self.command.output_file.write(f"         ⏭️ {name} (пропущено: {reason})\n")

        self.command.output_file.write("\n")
        self.command.output_file.flush()

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
                if data.get('count', 0) > 0:
                    display_name = self._get_display_name(key)
                    # Для ключевых слов показываем с контекстом если есть
                    if key == 'keywords' and 'pattern_info' in result:
                        items_with_context = []
                        pattern_info = result.get('pattern_info', {}).get('keywords', [])

                        for item in data.get('items', []):
                            # Ищем контекст для этого ключевого слова
                            context = ""
                            for match in pattern_info:
                                if match.get('name') == item['name'] and match.get('status') == 'found':
                                    matched_text = match.get('matched_text', '')
                                    if matched_text:
                                        context = f" → найдено как \"{matched_text}\""
                                    break
                            items_with_context.append(f"{item['name']}{context}")

                        self.command.stdout.write(f"  📌 {display_name} ({data['count']}):")
                        for item_with_context in items_with_context:
                            self.command.stdout.write(f"    • {item_with_context}")
                    else:
                        item_names = [item['name'] for item in data.get('items', [])]
                        self.command.stdout.write(f"  📌 {display_name} ({data['count']}): {', '.join(item_names)}")

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

                # Получаем информацию о совпадениях из результата
                pattern_info = result.get('pattern_info', {})

                # Для каждого типа критериев
                for key, data in result['results'].items():
                    if data.get('count', 0) > 0:
                        display_name = self._get_display_name(key)
                        self.command.output_file.write(f"   📌 {display_name}:\n")

                        # Для каждого найденного элемента в этой категории
                        for item in data.get('items', []):
                            item_name = item['name']
                            item_id = item.get('id')

                            # Базовая строка с названием элемента
                            line = f"      • {item_name}"

                            # Ищем контекст для этого конкретного элемента
                            context_info = self._find_context_for_item(
                                pattern_info, key, item_id, item_name
                            )

                            if context_info:
                                # Для ключевых слов показываем только найденную форму в той же строке
                                if keywords:
                                    # Извлекаем только matched_text из context_info
                                    import re
                                    # Ищем паттерн (найдено как "текст") или ("паттерн" как "текст")
                                    match = re.search(r'(?:найдено как|"[^"]+" как) "([^"]+)"', context_info)
                                    if match:
                                        matched_text = match.group(1)
                                        line += f" (найдено как \"{matched_text}\")"
                                    else:
                                        # Если не удалось извлечь, показываем как есть, но убираем контекст
                                        simplified = re.sub(r'\s+в:.*$', '', context_info)
                                        line += f" {simplified}"
                                else:
                                    # Для обычных критериев добавляем контекст с новой строки
                                    self.command.output_file.write(line + "\n")
                                    self.command.output_file.write(f"        {context_info}\n")
                                    continue  # Переходим к следующему элементу, так как уже записали

                            # Записываем строку (для ключевых слов или если нет контекста)
                            self.command.output_file.write(line + "\n")

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

    def _find_match_info_for_item(self, pattern_info: Dict, category: str, item_id: int, item_name: str,
                                  text_lower: str, original_text: str) -> str:
        """
        Ищет информацию о том, где и как было найдено совпадение для элемента.
        Возвращает строку для добавления к выводу элемента.
        """
        # Проверяем, есть ли информация о паттернах для этой категории
        if category in pattern_info:
            for match in pattern_info[category]:
                # Проверяем, относится ли это совпадение к нашему элементу
                match_item_id = match.get('keyword_id' if category == 'keywords' else 'id')
                match_item_name = match.get('name')

                if (match_item_id and match_item_id == item_id) or (match_item_name and match_item_name == item_name):
                    # Нашли совпадение для этого элемента
                    matched_text = match.get('matched_text', '')
                    matched_lemma = match.get('matched_lemma', '')
                    context = match.get('context', '')

                    if matched_text:
                        # Если есть контекст, используем его для более информативного вывода
                        if context:
                            return f"(найдено в: \"{context}\" → как \"{matched_text}\")"
                        else:
                            return f"(найдено как: \"{matched_text}\")"
                    elif context:
                        return f"(найдено в: \"{context}\")"
                    else:
                        # Если ничего нет, но это ключевое слово, попробуем найти в тексте (fallback)
                        if category == 'keywords' and original_text:
                            return self._fallback_keyword_search(item_name, text_lower, original_text)
                    break  # Нашли первое совпадение для этого элемента

        # Fallback для ключевых слов, если нет pattern_info
        if category == 'keywords' and original_text:
            return self._fallback_keyword_search(item_name, text_lower, original_text)

        return ""

    def _fallback_keyword_search(self, item_name: str, text_lower: str, original_text: str) -> str:
        """Запасной метод поиска ключевого слова в тексте, если нет pattern_info."""
        import re
        item_lower = item_name.lower()

        # Ищем точное вхождение слова как отдельного слова
        pattern = r'\b' + re.escape(item_lower) + r'\b'
        match = re.search(pattern, text_lower)

        if match:
            start = max(0, match.start() - 20)
            end = min(len(original_text), match.end() + 20)
            context = original_text[start:end]
            if start > 0:
                context = "..." + context
            if end < len(original_text):
                context = context + "..."
            return f"(найдено в слове: \"{context}\")"
        else:
            # Ищем как часть другого слова
            pattern = r'\b\w*' + re.escape(item_lower) + r'\w*\b'
            match = re.search(pattern, text_lower)
            if match:
                start_pos = match.start()
                end_pos = match.end()
                matched_text = original_text[start_pos:end_pos]
                start = max(0, start_pos - 20)
                end = min(len(original_text), end_pos + 20)
                context = original_text[start:end]
                if start > 0:
                    context = "..." + context
                if end < len(original_text):
                    context = context + "..."
                return f"(найдено как часть слова: \"{context}\" → как \"{matched_text}\")"
        return ""

    def _print_detailed_pattern_matches_to_file(self, pattern_info: Dict[str, Any]):
        """Выводит детальную информацию о ВСЕХ совпадениях паттернов в файл (для --verbose)."""
        if not pattern_info:
            return

        if not hasattr(self.command, 'output_file') or self.command.output_file.closed:
            return

        self.command.output_file.write("   🔍 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О СОВПАДЕНИЯХ:\n")

        for criteria_type, matches in pattern_info.items():
            if not matches:
                continue

            display_name = self._get_display_name(criteria_type)
            self.command.output_file.write(f"      📌 {display_name}:\n")

            for match in matches:
                status = match.get('status', 'unknown')
                if status == 'found':
                    name = match.get('name', 'N/A')
                    matched_text = match.get('matched_text', '')
                    context = match.get('context', '')
                    pattern = match.get('pattern', '')

                    # Формируем строку вывода
                    output_line = f"         • {name}"
                    if matched_text:
                        output_line += f" → найдено как \"{matched_text}\""
                    if context:
                        # Убираем лишние пробелы и переносы для компактности
                        clean_context = ' '.join(context.split())
                        if len(clean_context) > 60:
                            clean_context = clean_context[:57] + "..."
                        output_line += f" в контексте: \"{clean_context}\""
                    if pattern and self.command.verbose:  # Показываем паттерн только при очень подробном выводе
                        # Паттерны могут быть длинными, показываем их не всегда
                        if len(pattern) < 50:
                            output_line += f" (паттерн: {pattern})"

                    self.command.output_file.write(output_line + "\n")

                elif status == 'skipped' and not getattr(self.command, 'hide_skipped', False):
                    name = match.get('name', 'N/A')
                    reason = match.get('reason', 'already_exists')
                    self.command.output_file.write(f"         ⏭️ {name} (пропущено: {reason})\n")

        self.command.output_file.write("\n")
        self.command.output_file.flush()

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