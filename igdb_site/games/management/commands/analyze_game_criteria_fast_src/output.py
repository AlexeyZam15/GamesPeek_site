# games/management/commands/analyze_game_criteria_fas_src/output.py

import os
import sys
from typing import Dict, List

from django.db import connection


class OutputMixin:
    """Вывод результатов в файл и управление output-файлом."""

    def _output_results_grouped_by_pattern(self, results: List[Dict]):
        """Вывод результатов с группировкой по паттернам в формате Markdown"""

        if not self.output_file:
            return

        self.output_file.write("# 📊 РЕЗУЛЬТАТЫ АНАЛИЗА (ГРУППИРОВКА ПО ПАТТЕРНАМ)\n\n")
        self.output_file.write("---\n\n")

        self.output_file.write("## 📊 СТАТИСТИКА СРАБАТЫВАНИЙ ПАТТЕРНОВ\n\n")

        if not hasattr(self, 'compiled_patterns') or not self.compiled_patterns:
            self.output_file.write("⚠️ Нет загруженных паттернов для анализа\n\n")
        else:
            patterns_by_type = {}

            for p in self.compiled_patterns:
                crit_type = p['type']
                name = p['name']
                pattern_str = p['pattern_str']
                stop_words = p.get('stop_words', [])

                if crit_type not in patterns_by_type:
                    patterns_by_type[crit_type] = {}

                if name not in patterns_by_type[crit_type]:
                    patterns_by_type[crit_type][name] = {
                        'stop_words': stop_words,
                        'patterns': []
                    }

                new_count = 0
                existing_count = 0
                pattern_key = (crit_type, name, pattern_str)

                if hasattr(self, 'pattern_match_counter_new'):
                    new_count = self.pattern_match_counter_new.get(pattern_key, 0)
                if hasattr(self, 'pattern_match_counter_existing'):
                    existing_count = self.pattern_match_counter_existing.get(pattern_key, 0)

                total_count = new_count + existing_count

                patterns_by_type[crit_type][name]['patterns'].append({
                    'pattern_str': pattern_str,
                    'new_count': new_count,
                    'existing_count': existing_count,
                    'total_count': total_count
                })

            type_display_names = {
                'genres': 'Жанры',
                'themes': 'Темы',
                'perspectives': 'Перспективы',
                'game_modes': 'Режимы игры'
            }

            total_new_matches = 0
            total_existing_matches = 0
            total_patterns_count = 0

            for crit_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                if crit_type not in patterns_by_type:
                    continue

                display_name = type_display_names.get(crit_type, crit_type)
                self.output_file.write(f"### {display_name}\n\n")

                for name, data in patterns_by_type[crit_type].items():
                    stop_words = data.get('stop_words', [])
                    patterns = data.get('patterns', [])

                    self.output_file.write(f"#### {name}\n\n")
                    if stop_words:
                        self.output_file.write(f"🛑 Стоп-слова: `{', '.join(stop_words)}`\n\n")

                    for pattern in patterns:
                        self.output_file.write(f"- Паттерн: `{pattern['pattern_str']}`\n")
                        self.output_file.write(f"- Срабатываний (НОВЫЕ критерии): {pattern['new_count']}\n")
                        self.output_file.write(f"- Срабатываний (СУЩЕСТВУЮЩИЕ критерии): {pattern['existing_count']}\n")
                        self.output_file.write(f"- Всего срабатываний: {pattern['total_count']}\n\n")

                        total_new_matches += pattern['new_count']
                        total_existing_matches += pattern['existing_count']
                        total_patterns_count += 1

            total_matches = total_new_matches + total_existing_matches

            self.output_file.write(f"**Итого:**\n")
            self.output_file.write(f"- Всего срабатываний (НОВЫЕ): {total_new_matches}\n")
            self.output_file.write(f"- Всего срабатываний (СУЩЕСТВУЮЩИЕ): {total_existing_matches}\n")
            self.output_file.write(f"- Всего срабатываний (ОБЩЕЕ): {total_matches}\n")
            self.output_file.write(f"- Уникальных паттернов: {total_patterns_count}\n\n")

        self.output_file.write("---\n\n")
        self.output_file.write("## 📊 РЕЗУЛЬТАТЫ ПО ПАТТЕРНАМ\n\n")

        pattern_to_occurrences = {}

        for result in results:
            if result.get('skipped') or not result.get('has_results'):
                continue

            game_id = result['id']
            game_name = result['name']
            pattern_info = result.get('pattern_info', {})

            for crit_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                if crit_type not in pattern_info:
                    continue

                for match in pattern_info[crit_type]:
                    if match.get('status') != 'found':
                        continue

                    crit_name = match.get('name')
                    pattern_str = match.get('pattern')
                    matched_text = match.get('matched_text', '')
                    context = match.get('context', '')
                    position = match.get('position', 0)

                    key = (crit_type, pattern_str, crit_name)

                    if key not in pattern_to_occurrences:
                        pattern_to_occurrences[key] = []

                    pattern_to_occurrences[key].append({
                        'game_id': game_id,
                        'game_name': game_name,
                        'matched_text': matched_text,
                        'context': context,
                        'crit_name': crit_name,
                        'position': position
                    })

        if not pattern_to_occurrences:
            self.output_file.write("⚠️ Нет данных для группировки по паттернам\n\n")
            return

        type_display_names = {
            'genres': 'Жанры',
            'themes': 'Темы',
            'perspectives': 'Перспективы',
            'game_modes': 'Режимы игры'
        }

        criteria_groups = {}

        for (crit_type, pattern_str, crit_name), occurrences in pattern_to_occurrences.items():
            group_key = (crit_type, crit_name)
            if group_key not in criteria_groups:
                criteria_groups[group_key] = []
            criteria_groups[group_key].append((pattern_str, occurrences))

        for group_key in sorted(criteria_groups.keys(), key=lambda x: (x[0], x[1])):
            crit_type, crit_name = group_key
            display_name = type_display_names.get(crit_type, crit_type)

            self.output_file.write(f"## {display_name}: {crit_name}\n\n")

            ordered_patterns = []
            seen_patterns = set()

            for p in self.compiled_patterns:
                if p['type'] == crit_type and p['name'] == crit_name:
                    pattern_str = p['pattern_str']
                    if pattern_str not in seen_patterns:
                        for existing_pattern, occurrences in criteria_groups[group_key]:
                            if existing_pattern == pattern_str:
                                ordered_patterns.append((pattern_str, occurrences))
                                seen_patterns.add(pattern_str)
                                break

            for pattern_str, occurrences in criteria_groups[group_key]:
                if pattern_str not in seen_patterns:
                    ordered_patterns.append((pattern_str, occurrences))

            for pattern_str, occurrences in ordered_patterns:
                self.output_file.write(f"### Паттерн: `{pattern_str}`\n\n")
                self.output_file.write(f"**Найдено вхождений: {len(occurrences)}**\n\n")

                sorted_occurrences = sorted(occurrences, key=lambda x: (x['game_id'], x['position']))

                current_game_id = None
                for occ in sorted_occurrences:
                    if current_game_id != occ['game_id']:
                        current_game_id = occ['game_id']
                        self.output_file.write(f"\n{occ['game_name']} (ID: {occ['game_id']})\n")

                    self.output_file.write(f": Совпавший текст \"{occ['matched_text']}\"\n")
                    self.output_file.write(f": {occ['context']}\n\n")

            self.output_file.write("---\n\n")

        self.output_file.flush()

    def _output_results(self, results: List[Dict]):
        """Вывод результатов в файл (игры с новыми критериями, паттерны, контекст, статистика)"""
        if not self.output_file:
            return

        games_with_new = [r for r in results if r.get('has_new', False)]

        self.output_file.write("=" * 60 + "\n")
        self.output_file.write("📊 РЕЗУЛЬТАТЫ АНАЛИЗА\n")
        self.output_file.write("=" * 60 + "\n")

        self.output_file.write("\n📊 СТАТИСТИКА СРАБАТЫВАНИЙ ПАТТЕРНОВ\n")
        self.output_file.write("=" * 60 + "\n")

        all_pattern_keys = set()
        if hasattr(self, 'pattern_match_counter_new') and self.pattern_match_counter_new:
            all_pattern_keys.update(self.pattern_match_counter_new.keys())
        if hasattr(self, 'pattern_match_counter_existing') and self.pattern_match_counter_existing:
            all_pattern_keys.update(self.pattern_match_counter_existing.keys())

        if not all_pattern_keys and hasattr(self, 'compiled_patterns') and self.compiled_patterns:
            for p in self.compiled_patterns:
                all_pattern_keys.add((p['type'], p['name'], p['pattern_str']))

        if not all_pattern_keys:
            self.output_file.write("\n⚠️ Нет загруженных паттернов для анализа\n")
        else:
            sorted_patterns = sorted(all_pattern_keys, key=lambda x: (x[0], x[1]))

            patterns_by_type = {}
            for crit_type, name, pattern_str in sorted_patterns:
                if crit_type not in patterns_by_type:
                    patterns_by_type[crit_type] = []

                new_count = 0
                existing_count = 0

                if hasattr(self, 'pattern_match_counter_new'):
                    new_count = self.pattern_match_counter_new.get((crit_type, name, pattern_str), 0)
                if hasattr(self, 'pattern_match_counter_existing'):
                    existing_count = self.pattern_match_counter_existing.get((crit_type, name, pattern_str), 0)

                total_count = new_count + existing_count

                patterns_by_type[crit_type].append((name, pattern_str, new_count, existing_count, total_count))

            type_display_names = {
                'genres': 'Жанры',
                'themes': 'Темы',
                'perspectives': 'Перспективы',
                'game_modes': 'Режимы игры'
            }

            total_new_matches = 0
            total_existing_matches = 0

            for crit_type, patterns in patterns_by_type.items():
                display_name = type_display_names.get(crit_type, crit_type)
                self.output_file.write(f"\n📌 {display_name}:\n")

                for name, pattern_str, new_count, existing_count, total_count in patterns:
                    self.output_file.write(f"   • {name}\n")
                    self.output_file.write(f"     Паттерн: {pattern_str}\n")
                    self.output_file.write(f"     Срабатываний (НОВЫЕ критерии): {new_count}\n")
                    self.output_file.write(f"     Срабатываний (СУЩЕСТВУЮЩИЕ критерии): {existing_count}\n")
                    self.output_file.write(f"     Всего срабатываний: {total_count}\n")
                    self.output_file.write("\n")

                    total_new_matches += new_count
                    total_existing_matches += existing_count

            total_matches = total_new_matches + total_existing_matches

            self.output_file.write(f"\n📊 Всего срабатываний (НОВЫЕ): {total_new_matches}\n")
            self.output_file.write(f"📊 Всего срабатываний (СУЩЕСТВУЮЩИЕ): {total_existing_matches}\n")
            self.output_file.write(f"📊 Всего срабатываний (ОБЩЕЕ): {total_matches}\n")
            self.output_file.write(f"🎯 Уникальных паттернов: {len(all_pattern_keys)}\n")

        self.output_file.write("=" * 60 + "\n")

        self.output_file.write("\n📊 СТАТИСТИКА ОХВАТА ПО КРИТЕРИЯМ\n")
        self.output_file.write("=" * 60 + "\n")

        criteria_coverage = {
            'genres': {},
            'themes': {},
            'perspectives': {},
            'game_modes': {}
        }

        for result in results:
            if result.get('skipped') or not result.get('has_results'):
                continue

            game_id = result['id']
            found_items = result.get('found', {})

            for crit_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                items = found_items.get(crit_type, [])
                for item in items:
                    crit_id = item['id']
                    crit_name = item['name']

                    if crit_name not in criteria_coverage[crit_type]:
                        criteria_coverage[crit_type][crit_name] = {
                            'id': crit_id,
                            'games_found': set()
                        }
                    criteria_coverage[crit_type][crit_name]['games_found'].add(game_id)

        type_to_table = {
            'genres': ('games_game_genres', 'genre_id'),
            'themes': ('games_game_themes', 'theme_id'),
            'perspectives': ('games_game_player_perspectives', 'playerperspective_id'),
            'game_modes': ('games_game_game_modes', 'gamemode_id')
        }

        type_display_names = {
            'genres': 'Жанры',
            'themes': 'Темы',
            'perspectives': 'Перспективы',
            'game_modes': 'Режимы игры'
        }

        for crit_type, criteria_dict in criteria_coverage.items():
            if not criteria_dict:
                continue

            display_name = type_display_names.get(crit_type, crit_type)
            self.output_file.write(f"\n📌 {display_name}:\n")

            table_name, id_field = type_to_table[crit_type]

            for crit_name, data in sorted(criteria_dict.items()):
                crit_id = data['id']
                games_found_count = len(data['games_found'])

                with connection.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT COUNT(DISTINCT game_id)
                        FROM {table_name}
                        WHERE {id_field} = %s
                    """, [crit_id])
                    total_games_in_db = cursor.fetchone()[0]

                coverage_percent = (games_found_count / total_games_in_db * 100) if total_games_in_db > 0 else 0

                self.output_file.write(f"   • {crit_name} (ID: {crit_id})\n")
                self.output_file.write(f"     Найдено анализом игр: {games_found_count}\n")
                self.output_file.write(f"     Всего игр в БД с этим критерием: {total_games_in_db}\n")
                self.output_file.write(f"     Охват: {coverage_percent:.1f}%\n")
                self.output_file.write("\n")

        self.output_file.write("=" * 60 + "\n")

        if not games_with_new:
            self.output_file.write("\n✅ Нет игр с новыми критериями для сохранения\n")
            self.output_file.write("=" * 60 + "\n")
            self.output_file.flush()
            return

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

                    matches_by_pattern = {}
                    if criteria_type in pattern_info:
                        for match in pattern_info[criteria_type]:
                            if match.get('name') == item_name and match.get('status') == 'found':
                                pattern = match.get('pattern', 'unknown')
                                if pattern not in matches_by_pattern:
                                    matches_by_pattern[pattern] = []
                                matches_by_pattern[pattern].append(match)

                    self.output_file.write(f"      • {item_name} (ID: {item_id})\n")

                    if matches_by_pattern:
                        for pattern, matches in matches_by_pattern.items():
                            match_count = len(matches)
                            if match_count == 1:
                                self.output_file.write(f"        🔍 Паттерн: {pattern}\n")
                                self.output_file.write(
                                    f"        📖 Совпавший текст: \"{matches[0].get('matched_text', '')}\"\n")
                                self.output_file.write(f"        📝 Контекст: {matches[0].get('context', '')}\n")
                            else:
                                self.output_file.write(f"        🔍 Паттерн: {pattern} (найдено {match_count} раза)\n")
                                for idx, match in enumerate(matches, 1):
                                    self.output_file.write(
                                        f"           {idx}. 📖 Совпавший текст: \"{match.get('matched_text', '')}\"\n")
                                    self.output_file.write(f"           📝 Контекст: {match.get('context', '')}\n")
                    else:
                        self.output_file.write(f"        ⚠️ Информация о паттерне не найдена\n")

            if not has_any_new:
                self.output_file.write("   ℹ️ Нет новых критериев (только существующие)\n")

            index += 1

        self.output_file.write("\n" + "=" * 60 + "\n")
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

    def _setup_output_file(self):
        """Открывает файл вывода и подменяет stdout"""
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
            except Exception:
                pass
            finally:
                self.output_file = None
                self.original_stdout = None