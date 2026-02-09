# games/management/commands/analyzer/state_manager.py
"""
Менеджер состояния команды с поддержкой проверенных критериев
"""

import os
import json
import time
from typing import Set, Dict, Any, List
import sys
from django.core.cache import cache


class StateManager:
    """Управляет состоянием обработки с хранением проверенных критериев"""

    def __init__(self, output_path: str = None, keywords_mode: bool = False, force_restart: bool = False):
        self.output_path = output_path
        self.keywords_mode = keywords_mode
        self.force_restart = force_restart
        self.processed_games: Set[int] = set()
        self.checked_criteria: Set[str] = set()  # Храним ID проверенных критериев
        self.state_file = None
        self.criteria_state_file = None  # Отдельный файл для критериев

        self._init_state_files()

    def _init_state_files(self):
        """Инициализирует файлы состояния"""
        if not self.output_path:
            return

        try:
            # Создаем имя файла состояния как в старой версии
            if '.' in os.path.basename(self.output_path):
                # Если output - это файл
                directory = os.path.dirname(self.output_path)
                filename = os.path.basename(self.output_path)
                name_part = filename.rsplit('.', 1)[0]
                mode = 'keywords' if self.keywords_mode else 'criteria'
                self.state_file = os.path.join(directory, f"state_{mode}.json")
                self.criteria_state_file = os.path.join(directory, f"state_{mode}_criteria.json")
            else:
                # Если output - это папка
                mode = 'keywords' if self.keywords_mode else 'criteria'
                self.state_file = os.path.join(self.output_path, f"state_{mode}.json")
                self.criteria_state_file = os.path.join(self.output_path, f"state_{mode}_criteria.json")

            # Создаем директорию если она не существует
            directory = os.path.dirname(self.state_file)
            if directory:
                os.makedirs(directory, exist_ok=True)

            print(f"📝 Файл состояния: {self.state_file}", file=sys.stderr)
            print(f"📝 Файл состояния критериев: {self.criteria_state_file}", file=sys.stderr)

        except Exception as e:
            print(f"⚠️ Ошибка инициализации файлов состояния: {e}", file=sys.stderr)
            self.state_file = None
            self.criteria_state_file = None

    def load_state(self) -> int:
        """Загружает состояние из файлов"""
        if not self.state_file or self.force_restart:
            self.processed_games.clear()
            self.checked_criteria.clear()
            return 0

        try:
            total_processed = 0

            # 1. Загружаем обработанные игры из state_file
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)

                saved_mode = state_data.get('mode', '')
                current_mode = 'keywords' if self.keywords_mode else 'criteria'

                if saved_mode != current_mode:
                    # Режим изменился, начинаем заново
                    self.processed_games.clear()
                    self.checked_criteria.clear()
                    print(f"♻️ Режим изменился с '{saved_mode}' на '{current_mode}'. Очищаем состояние.",
                          file=sys.stderr)
                else:
                    # Загружаем обработанные игры
                    self.processed_games = set(state_data.get('processed_games', []))
                    total_processed = len(self.processed_games)
                    print(f"📖 Загружено состояние: {total_processed} ранее обработанных игр", file=sys.stderr)

            # 2. Загружаем проверенные критерии из criteria_state_file
            if self.criteria_state_file and os.path.exists(self.criteria_state_file):
                with open(self.criteria_state_file, 'r', encoding='utf-8') as f:
                    criteria_data = json.load(f)

                saved_criteria_mode = criteria_data.get('mode', '')
                current_criteria_mode = 'keywords' if self.keywords_mode else 'criteria'

                if saved_criteria_mode == current_criteria_mode:
                    self.checked_criteria = set(criteria_data.get('checked_criteria', []))
                    criteria_count = len(self.checked_criteria)
                    print(f"📊 Загружено проверенных критериев: {criteria_count}", file=sys.stderr)
                else:
                    print(f"♻️ Режим критериев изменился: '{saved_criteria_mode}' -> '{current_criteria_mode}'",
                          file=sys.stderr)
                    self.checked_criteria.clear()

            return total_processed

        except Exception as e:
            print(f"⚠️ Ошибка загрузки состояния: {e}", file=sys.stderr)
            self.processed_games.clear()
            self.checked_criteria.clear()
            return 0

    def save_state(self, processed_count: int = 0):
        """Сохраняет состояние в файлы - ТОЛЬКО обработанные игры"""
        self._save_games_state(processed_count)
        self._save_criteria_state()

    def _save_games_state(self, processed_count: int = 0):
        """Сохраняет состояние обработанных игр - БЕЗ статистики пропусков"""
        if not self.state_file:
            return

        try:
            current_mode = 'keywords' if self.keywords_mode else 'criteria'

            # ВАЖНО: Сохраняем ТОЛЬКО обработанные игры
            state_data = {
                'processed_games': list(self.processed_games),
                'timestamp': time.time(),
                'mode': current_mode,
                'keywords_mode': self.keywords_mode
                # НЕ сохраняем никакую статистику пропусков!
            }

            # Создаем директорию если нужно
            directory = os.path.dirname(self.state_file)
            if directory:
                os.makedirs(directory, exist_ok=True)

            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)

        except Exception as e:
            print(f"⚠️ Ошибка сохранения состояния игр: {e}", file=sys.stderr)

    def _save_criteria_state(self):
        """Сохраняет состояние проверенных критериев"""
        if not self.criteria_state_file:
            return

        try:
            current_mode = 'keywords' if self.keywords_mode else 'criteria'

            criteria_data = {
                'checked_criteria': list(self.checked_criteria),
                'timestamp': time.time(),
                'criteria_count': len(self.checked_criteria),
                'mode': current_mode,
                'keywords_mode': self.keywords_mode
            }

            # Создаем директорию если нужно
            directory = os.path.dirname(self.criteria_state_file)
            if directory:
                os.makedirs(directory, exist_ok=True)

            with open(self.criteria_state_file, 'w', encoding='utf-8') as f:
                json.dump(criteria_data, f, ensure_ascii=False, indent=2)

            if self.verbose:
                print(f"💾 Сохранено {len(self.checked_criteria)} проверенных критериев", file=sys.stderr)

        except Exception as e:
            print(f"⚠️ Ошибка сохранения состояния критериев: {e}", file=sys.stderr)

    def add_checked_criteria(self, criteria_ids: List[str]):
        """Добавляет критерии в список проверенных"""
        self.checked_criteria.update(criteria_ids)

    def get_checked_criteria(self) -> Set[str]:
        """Возвращает множество проверенных критериев"""
        return self.checked_criteria.copy()

    def has_checked_criteria(self) -> bool:
        """Проверяет, есть ли сохраненные проверенные критерии"""
        return len(self.checked_criteria) > 0

    def clear_checked_criteria(self):
        """Очищает список проверенных критериев"""
        self.checked_criteria.clear()
        if self.criteria_state_file and os.path.exists(self.criteria_state_file):
            try:
                os.remove(self.criteria_state_file)
            except Exception as e:
                print(f"⚠️ Ошибка удаления файла критериев: {e}", file=sys.stderr)

    def add_processed_game(self, game_id: int):
        """Добавляет игру в обработанные"""
        self.processed_games.add(game_id)

    def is_game_processed(self, game_id: int) -> bool:
        """Проверяет, обработана ли игра"""
        return game_id in self.processed_games

    def get_processed_count(self) -> int:
        """Возвращает количество обработанных игр"""
        return len(self.processed_games)

    def reset_state(self):
        """Полностью сбрасывает состояние"""
        self.processed_games.clear()
        self.checked_criteria.clear()

        # Удаляем файлы состояния
        if self.state_file and os.path.exists(self.state_file):
            try:
                os.remove(self.state_file)
            except:
                pass

        if self.criteria_state_file and os.path.exists(self.criteria_state_file):
            try:
                os.remove(self.criteria_state_file)
            except:
                pass