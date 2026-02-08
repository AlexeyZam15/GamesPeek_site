# games/management/commands/analyzer/output_manager.py
"""
Менеджер вывода только для прогресс-баров
"""

import sys
import time
from typing import Optional, Dict, Any
import shutil


class TerminalController:
    """Контроллер терминала только для прогресс-баров"""

    def __init__(self):
        self.terminal = sys.stderr

        # Получаем размер терминала
        self.terminal_height = self._get_terminal_height()

        # Располагаем прогресс-бары внизу
        self.batch_progress_line = self.terminal_height - 1  # Последняя строка
        self.main_progress_line = self.terminal_height - 2  # Предпоследняя строка

        # Сохраняем текущую позицию курсора
        self._save_cursor_position()

    def _get_terminal_height(self) -> int:
        """Получает высоту терминала"""
        try:
            size = shutil.get_terminal_size()
            return size.lines
        except:
            return 24  # Стандартная высота терминала

    def _save_cursor_position(self):
        """Сохраняет текущую позицию курсора"""
        self.terminal.write("\033[s")
        self.terminal.flush()

    def _restore_cursor_position(self):
        """Восстанавливает сохраненную позицию курсора"""
        self.terminal.write("\033[u")
        self.terminal.flush()

    def move_to_line(self, line: int):
        """Перемещает курсор на указанную строку (0-индексированная)"""
        self.terminal.write(f"\033[{line + 1};1H")
        self.terminal.flush()

    def clear_line(self, line: int):
        """Очищает указанную строку"""
        self._save_cursor_position()
        self.move_to_line(line)
        self.terminal.write("\033[2K")  # Очистить всю строку
        self._restore_cursor_position()

    def write_at_line(self, line: int, text: str):
        """Записывает текст на указанной строке"""
        self._save_cursor_position()
        self.move_to_line(line)
        self.terminal.write("\033[2K")  # Очистить строку
        self.terminal.write(text[:200])  # Ограничиваем длину
        self._restore_cursor_position()

    def clear_progress_bars(self):
        """Очищает строки прогресс-баров"""
        self.clear_line(self.main_progress_line)
        self.clear_line(self.batch_progress_line)

    def write_final_message(self, text: str):
        """Записывает финальное сообщение на место основного прогресс-бара"""
        self.clear_line(self.main_progress_line)
        self.move_to_line(self.main_progress_line)
        self.terminal.write("\033[2K")  # Очистить строку
        self.terminal.write(text)
        self.terminal.flush()


class UnifiedProgressBar:
    """Унифицированный прогресс-бар, работающий через TerminalController"""

    def __init__(self, total: int, desc: str = "Анализ игр",
                 bar_length: int = 30, update_interval: float = 0.1,
                 stat_width: int = 5, emoji_spacing: int = 1,
                 is_batch: bool = False):

        self.total = total
        self.desc = desc
        self.bar_length = bar_length
        self.update_interval = update_interval
        self.stat_width = stat_width
        self.emoji_spacing = emoji_spacing
        self.is_batch = is_batch

        self.current = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self._enabled = True

        self.stats = {
            'found_count': 0,
            'total_criteria_found': 0,
            'skipped_total': 0,
            'errors': 0,
            'updated': 0,
        }

        self.filled_char = '█'
        self.empty_char = '░'
        self.terminal = TerminalController()

    def get_current_message(self):
        """Возвращает текущее сообщение прогресс-бара с полной статистикой"""
        # Рассчитываем процент от ОБЩЕГО количества (обработанные + пропущенные)
        total_processed_including_skipped = self.current

        percentage = (total_processed_including_skipped / self.total) * 100 if self.total > 0 else 0
        if percentage > 100:
            percentage = 100

        # Рассчитываем заполненную часть
        filled_length = int(
            self.bar_length * total_processed_including_skipped // self.total) if self.total > 0 else self.bar_length
        if filled_length > self.bar_length:
            filled_length = self.bar_length

        bar = self.filled_char * filled_length + self.empty_char * (self.bar_length - filled_length)

        # Рассчитываем время
        elapsed_time = time.time() - self.start_time
        if total_processed_including_skipped > 0 and total_processed_including_skipped < self.total:
            remaining_time = (elapsed_time / total_processed_including_skipped) * (
                        self.total - total_processed_including_skipped)
            time_str = f"{elapsed_time:.0f}s < {remaining_time:.0f}s"
        else:
            time_str = f"{elapsed_time:.0f}s"

        # Форматируем сообщение
        if self.is_batch:
            message = f"💾 Обновление батча: {percentage:3.0f}% [{total_processed_including_skipped}/{self.total}] [{bar}] ({time_str})"
        else:
            message = f"{self.desc}: {percentage:3.0f}% [{total_processed_including_skipped}/{self.total}] [{bar}] "

            # Рассчитываем статистику по ВСЕМ обработанным играм
            found_count = self.stats['found_count']
            skipped_total = self.stats['skipped_total']
            errors = self.stats['errors']
            updated = self.stats['updated']

            # Рассчитываем количество игр без результатов
            total_processed = found_count + skipped_total + errors + self.stats.get('not_found_count', 0)

            spacing = " " * self.emoji_spacing

            # Показываем статистику по всем обработанным играм:
            # 🎯 - игры с найденными критериями
            # 📈 - общее количество найденных элементов
            # ⏭️ - игры пропущенные (по любым причинам)
            # 🔍 - игры обработанные без результатов
            # ❌ - ошибки
            # 💾 - обновленные игры

            message += f"🎯{spacing}{found_count:>{self.stat_width}} "
            message += f"📈{spacing}{self.stats['total_criteria_found']:>{self.stat_width}} "
            message += f"⏭️{spacing}{skipped_total:>{self.stat_width}} "
            message += f"🔍{spacing}{self.stats.get('not_found_count', 0):>{self.stat_width}} "
            message += f"❌{spacing}{errors:>{self.stat_width}} "
            message += f"💾{spacing}{updated:>{self.stat_width}} "
            message += f"({time_str})"

        return message

    def update(self, n: int = 1):
        """Обновить прогресс"""
        if not self._enabled:
            return

        self.current += n

        # Не позволяем current превышать total
        if self.current > self.total:
            self.current = self.total

        current_time = time.time()

        # Обновляем не чаще чем update_interval секунд
        if current_time - self.last_update_time < self.update_interval and self.current < self.total:
            return

        self.last_update_time = current_time

        # Получаем сообщение
        message = self.get_current_message()

        # Определяем линию для этого прогресс-бара
        line = self.terminal.batch_progress_line if self.is_batch else self.terminal.main_progress_line

        # Выводим на фиксированной позиции внизу
        self.terminal.write_at_line(line, message)

    def update_stats(self, stats: Dict):
        """Обновить статистику"""
        self.stats.update(stats)

    def finish(self, final_message: Optional[str] = None):
        """Завершить прогресс-бар"""
        if not self._enabled:
            return

        # НЕ меняем текущий прогресс - оставляем как есть
        # Получаем финальное сообщение с ТЕКУЩИМ прогрессом
        if final_message is None:
            final_message = self.get_current_message()  # Используем текущий прогресс

        # НЕ очищаем строки - просто останавливаем обновление
        self._enabled = False