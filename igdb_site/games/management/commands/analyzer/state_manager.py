# games/management/commands/analyzer/state_manager.py
"""
Менеджер состояния команды (полная совместимость)
"""

import os
import json
import time
from typing import Set, Dict, Any
import sys


class StateManager:
    """Управляет состоянием обработки (как в старой версии)"""

    def __init__(self, output_path: str = None, keywords_mode: bool = False, force_restart: bool = False):
        self.output_path = output_path
        self.keywords_mode = keywords_mode
        self.force_restart = force_restart
        self.processed_games: Set[int] = set()
        self.state_file = None

        self._init_state_file()

    def _init_state_file(self):
        """Инициализирует файл состояния (как в старой версии)"""
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
            else:
                # Если output - это папка
                mode = 'keywords' if self.keywords_mode else 'criteria'
                self.state_file = os.path.join(self.output_path, f"state_{mode}.json")

            # Создаем директорию если она не существует
            directory = os.path.dirname(self.state_file)
            if directory:
                os.makedirs(directory, exist_ok=True)

            print(f"📝 Файл состояния: {self.state_file}", file=sys.stderr)

        except Exception as e:
            print(f"⚠️ Ошибка инициализации файла состояния: {e}", file=sys.stderr)
            self.state_file = None

    def load_state(self) -> int:
        """Загружает состояние из файла"""
        if not self.state_file or self.force_restart:
            self.processed_games.clear()
            return 0

        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state_data = json.load(f)

                saved_mode = state_data.get('mode', '')
                current_mode = 'keywords' if self.keywords_mode else 'criteria'

                if saved_mode != current_mode:
                    # Режим изменился, начинаем заново
                    self.processed_games.clear()
                    return 0

                self.processed_games = set(state_data.get('processed_games', []))
                return len(self.processed_games)

            except Exception as e:
                print(f"⚠️ Ошибка загрузки состояния: {e}")
                self.processed_games.clear()
                return 0

        return 0

    def save_state(self, processed_count: int = 0):
        """Сохраняет состояние в файл"""
        if not self.state_file:
            return

        try:
            current_mode = 'keywords' if self.keywords_mode else 'criteria'

            state_data = {
                'processed_games': list(self.processed_games),
                'timestamp': time.time(),
                'total_processed': processed_count,
                'mode': current_mode,
                'keywords_mode': self.keywords_mode
            }

            # Создаем директорию если нужно
            directory = os.path.dirname(self.state_file)
            if directory:
                os.makedirs(directory, exist_ok=True)

            # ИСПРАВЛЕНИЕ: Выводим только если verbose_mode=True
            # Добавьте параметр verbose_mode в __init__ если нужно
            if hasattr(self, 'verbose_mode') and self.verbose_mode:
                import sys
                print(f"💾 Сохраняем состояние в файл: {self.state_file}", file=sys.stderr)
                print(f"   Директория: {directory}", file=sys.stderr)
                print(f"   Обработано игр: {processed_count}", file=sys.stderr)

            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(state_data, f, ensure_ascii=False, indent=2)

            if hasattr(self, 'verbose_mode') and self.verbose_mode:
                import sys
                print(f"✅ Состояние сохранено", file=sys.stderr)

        except Exception as e:
            import sys
            print(f"⚠️ Ошибка сохранения состояния: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc(file=sys.stderr)

    def add_processed_game(self, game_id: int):
        """Добавляет игру в обработанные"""
        self.processed_games.add(game_id)

    def is_game_processed(self, game_id: int) -> bool:
        """Проверяет, обработана ли игра"""
        return game_id in self.processed_games

    def get_processed_count(self) -> int:
        """Возвращает количество обработанных игр"""
        return len(self.processed_games)