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
                collect_patterns=self.command.verbose
            )

            has_found_criteria = any(len(results[key]) > 0 for key in results)
            criteria_count = sum(len(results[key]) for key in results)
            stats['total_criteria_found'] += criteria_count

            # В режиме only-found пропускаем игры без найденных критериев
            if self.command.only_found and not has_found_criteria:
                return True

            # В режиме ignore-existing + update-game проверяем, есть ли действительно новые критерии
            actual_has_found_criteria = has_found_criteria
            if self.command.ignore_existing and self.command.update_game and has_found_criteria:
                has_new_criteria = False
                for criteria_type, items in results.items():
                    try:
                        existing_items = self.analyzer._get_existing_objects(game, criteria_type)
                        existing_names = {item.name for item in existing_items}
                        new_items = [item for item in items if item.name not in existing_names]
                        if new_items:
                            has_new_criteria = True
                            break
                    except Exception as e:
                        self.command.stdout.write(
                            f"   ⚠️ Ошибка проверки существующих {criteria_type} для {game.name}: {e}")
                        has_new_criteria = True
                        break
                actual_has_found_criteria = has_new_criteria

            # Пропускаем игру если нет реально новых критериев в режиме ignore-existing + update-game
            if self.command.ignore_existing and self.command.update_game and not actual_has_found_criteria:
                return True

            # ОДИН отступ перед каждой ВЫВЕДЕННОЙ игрой (кроме первой)
            if stats['displayed_count'] > 0:
                self.command.stdout.write("")

            stats['displayed_count'] += 1

            if self.command.verbose and not self.command.only_found:
                self.command.stdout.write(f"{index}. 🔍 Анализируем: {game.name}")
                text_source = self.command._get_text_source_for_game(game, text_to_analyze)
                text_length = len(text_to_analyze)
                self.command.stdout.write(f"   📝 Используется: {text_source} ({text_length} символов)")

            # Выводим результаты
            if actual_has_found_criteria:
                stats['found_count'] += 1
                self.command._print_game_results(game, results, criteria_count, pattern_info)

                if self.command.update_game:
                    if self.command.update_game_criteria(game, results):
                        stats['updated'] += 1
                    elif self.command.verbose:
                        self.command.stdout.write(f"   ℹ️ Нет новых критериев для обновления")
            elif not self.command.only_found:
                mode_text = "критерии" if self.command.ignore_existing else "новые критерии"
                self.command.stdout.write(f"   ⚠️ {mode_text} не найдены")

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