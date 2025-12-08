# games/analyzer/game_processor.py
from typing import Dict, List
from django.db.models import QuerySet
from games.models import Game


class GameProcessor:
    """Класс для обработки игр в батчах"""

    def __init__(self, command_instance):
        self.command = command_instance
        self.analyzer = command_instance.analyzer

    def process_games_batch(self, games_queryset: QuerySet) -> Dict:
        """Обрабатывает игры батчами для экономии памяти"""
        stats = {
            'processed': 0,
            'updated': 0,
            'skipped_no_text': 0,
            'errors': 0,
            'found_count': 0,
            'total_criteria_found': 0,
            'displayed_count': 0,
        }

        games_iterator = games_queryset.iterator(chunk_size=self.command.batch_size)

        for game in games_iterator:
            stats['processed'] += 1
            current_index = self.command.offset + stats['processed']

            try:
                self._process_single_game(game, current_index, stats)
            except Exception as e:
                stats['errors'] += 1
                if not self.command.only_found:
                    self.command.stderr.write(f"   ❌ Ошибка при анализе {game.name}: {str(e)}")

        return stats

    def _process_single_game(self, game, index: int, stats: Dict) -> bool:
        """Обрабатывает одну игру"""
        text_to_analyze = self.command.get_text_to_analyze(game)

        # Проверяем только на полное отсутствие текста
        if not text_to_analyze or text_to_analyze.strip() == "":
            stats['skipped_no_text'] += 1
            if self.command.verbose and not self.command.only_found:
                self.command.stdout.write(f"{index}. {game.name} - ⏭️ ПРОПУЩЕНО (текста вообще нет)")
            return True

        try:
            # Анализируем текст
            results, pattern_info = self.analyzer.analyze_text(
                text_to_analyze,
                game=game,
                ignore_existing=self.command.ignore_existing,
                collect_patterns=self.command.verbose,
                keywords_mode=self.command.keywords
            )

            # В РЕЖИМЕ КЛЮЧЕВЫХ СЛОВ
            if self.command.keywords:
                keywords_results = results.get('keywords', [])
                has_found_criteria = len(keywords_results) > 0
                criteria_count = len(keywords_results)

                # Если ignore-existing, проверяем только новые
                if self.command.ignore_existing:
                    existing_keywords = self.analyzer._get_existing_objects(game, 'keywords')
                    existing_names = {kw.name for kw in existing_keywords}
                    new_keywords = [kw for kw in keywords_results if kw.name not in existing_names]
                    actual_found_count = len(new_keywords)
                    actual_has_found_criteria = actual_found_count > 0
                else:
                    new_keywords = keywords_results
                    actual_found_count = criteria_count
                    actual_has_found_criteria = has_found_criteria

            # В ОБЫЧНОМ РЕЖИМЕ
            else:
                has_found_criteria = any(
                    len(results[key]) > 0 for key in ['genres', 'themes', 'perspectives', 'game_modes'] if
                    key in results)
                criteria_count = sum(
                    len(results.get(key, [])) for key in ['genres', 'themes', 'perspectives', 'game_modes'])

                if self.command.ignore_existing and has_found_criteria:
                    actual_found_count = 0
                    actual_has_found_criteria = False
                    for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                        if criteria_type in results:
                            existing_items = self.analyzer._get_existing_objects(game, criteria_type)
                            existing_names = {item.name for item in existing_items}
                            new_items = [item for item in results[criteria_type] if item.name not in existing_names]
                            actual_found_count += len(new_items)
                            if new_items:
                                actual_has_found_criteria = True
                else:
                    actual_found_count = criteria_count
                    actual_has_found_criteria = has_found_criteria

            # ОБНОВЛЯЕМ СТАТИСТИКУ
            stats['total_criteria_found'] += actual_found_count

            if actual_has_found_criteria:
                stats['found_count'] += 1

            # В режиме only-found пропускаем игры без найденных критериев
            if self.command.only_found and not actual_has_found_criteria:
                return True

            # Пропускаем игру если нет реально новых критериев в режиме ignore-existing + update-game
            if self.command.ignore_existing and self.command.update_game and not actual_has_found_criteria:
                return True

            # ОДИН отступ перед каждой ВЫВЕДЕННОЙ игрой
            if stats['displayed_count'] > 0:
                self.command.stdout.write("")

            stats['displayed_count'] += 1

            if self.command.verbose and not self.command.only_found:
                self.command.stdout.write(f"{index}. 🔍 Анализируем: {game.name}")
                text_source = self.command._get_text_source_for_game(game, text_to_analyze)
                text_length = len(text_to_analyze)
                self.command.stdout.write(f"   📝 Используется: {text_source} ({text_length} символов)")

            # ВЫВОДИМ РЕЗУЛЬТАТЫ (ИСПРАВЛЕННАЯ ЧАСТЬ)
            if actual_has_found_criteria:
                # Для режима ключевых слов фильтруем результаты
                if self.command.keywords:
                    display_results = {'keywords': new_keywords} if self.command.ignore_existing else {
                        'keywords': keywords_results}
                    display_pattern_info = {'keywords': pattern_info.get('keywords', [])}
                    self.command._print_game_results(game, display_results, actual_found_count, display_pattern_info)
                else:
                    # Для обычного режима
                    if self.command.ignore_existing:
                        # Фильтруем только новые
                        filtered_results = {}
                        for criteria_type in ['genres', 'themes', 'perspectives', 'game_modes']:
                            if criteria_type in results:
                                existing_items = self.analyzer._get_existing_objects(game, criteria_type)
                                existing_names = {item.name for item in existing_items}
                                new_items = [item for item in results[criteria_type] if item.name not in existing_names]
                                if new_items:
                                    filtered_results[criteria_type] = new_items
                        self.command._print_game_results(game, filtered_results, actual_found_count, pattern_info)
                    else:
                        self.command._print_game_results(game, results, actual_found_count, pattern_info)

                if self.command.update_game:
                    if self.command.update_game_criteria(game, results):
                        stats['updated'] += 1
                    elif self.command.verbose:
                        mode = 'ключевых слов' if self.command.keywords else 'критериев'
                        self.command.stdout.write(f"   ℹ️ Нет новых {mode} для обновления")

            elif not self.command.only_found:
                # ИСПРАВЛЕННЫЙ ВЫВОД КОГДА НЕ НАЙДЕНО
                mode = 'ключевые слова' if self.command.keywords else 'новые критерии'
                if self.command.ignore_existing:
                    mode = 'новые ключевые слова' if self.command.keywords else 'новые критерии'
                self.command.stdout.write(f"   ⚠️ {mode.capitalize()} не найдены")

            return True

        except Exception as e:
            stats['errors'] += 1
            self.command.stdout.write(f"❌ ОШИБКА при анализе {game.name} (ID: {game.id}):")
            self.command.stdout.write(f"   📝 Текст: {text_to_analyze[:100]}...")
            self.command.stdout.write(f"   🔍 Ошибка: {str(e)}")
            import traceback
            self.command.stdout.write(f"   🕵️ Трассировка: {traceback.format_exc()}")
            self.command.stdout.write("")
            return True
