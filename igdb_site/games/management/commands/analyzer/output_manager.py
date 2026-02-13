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

        # ВАЖНО: Сохраняем текущую позицию курсора
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
        # Сохраняем позицию курсора
        self.terminal.write("\033[s")
        self.terminal.flush()

    def _restore_cursor_position(self):
        """Восстанавливает сохраненную позицию курсора"""
        # Восстанавливаем позицию курсора
        self.terminal.write("\033[u")
        self.terminal.flush()

    def move_to_line(self, line: int):
        """Перемещает курсор на указанную строку (0-индексированная)"""
        # Перемещаем курсор на нужную строку
        self.terminal.write(f"\033[{line + 1};1H")
        self.terminal.flush()

    def clear_line(self, line: int):
        """Очищает указанную строку"""
        self.move_to_line(line)
        self.terminal.write("\033[2K")  # Очистить всю строку
        self.terminal.flush()
        self._restore_cursor_position()

    def write_at_line(self, line: int, text: str):
        """Записывает текст на указанной строке"""
        # Сохраняем текущую позицию
        self._save_cursor_position()

        # Перемещаемся на нужную строку
        self.move_to_line(line)

        # Очищаем строку
        self.terminal.write("\033[2K")  # Очистить строку

        # Записываем текст (ограничиваем длину)
        max_width = self._get_terminal_width()
        if len(text) > max_width:
            text = text[:max_width - 3] + "..."

        self.terminal.write(text)

        # Восстанавливаем позицию
        self._restore_cursor_position()
        self.terminal.flush()

    def _get_terminal_width(self) -> int:
        """Получает ширину терминала"""
        try:
            size = shutil.get_terminal_size()
            return size.columns
        except:
            return 80  # Стандартная ширина терминала

    def clear_progress_bars(self):
        """Очищает строки прогресс-баров"""
        self.clear_line(self.main_progress_line)
        self.clear_line(self.batch_progress_line)

    def write_final_message(self, text: str):
        """Записывает финальное сообщение на место основного прогресс-бара"""
        self.clear_line(self.main_progress_line)
        self.move_to_line(self.main_progress_line)
        self.terminal.write("\033[2K")  # Очистить строку

        # Ограничиваем длину текста
        max_width = self._get_terminal_width()
        if len(text) > max_width:
            text = text[:max_width - 3] + "..."

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

        self.current = 0  # ВАЖНО: ВСЕГДА начинаем с 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self._enabled = True

        # Статистика для прогресс-бара (значки)
        self.stats = {
            'found_count': 0,  # 🎯
            'total_criteria_found': 0,  # 📈
            'skipped_total': 0,  # ⏭️
            'errors': 0,  # ❌
            'updated': 0,  # 💾
            'in_batch': 0,  # 📦
            'not_found_count': 0,  # 🔍
        }

        self.filled_char = '█'
        self.empty_char = '░'
        self.terminal = TerminalController()

    def reset(self):
        """Сбросить прогресс-бар к начальному состоянию"""
        self.current = 0  # ВАЖНО: сбрасываем в 0
        self.start_time = time.time()
        self.last_update_time = time.time()

        # Сбрасываем статистику
        self.stats = {
            'found_count': 0,
            'total_criteria_found': 0,
            'skipped_total': 0,
            'errors': 0,
            'updated': 0,
            'in_batch': 0,
            'not_found_count': 0,
        }

        # Принудительно обновляем отображение
        self._force_update()

    def get_current_message(self):
        """Возвращает текущее сообщение прогресс-бара с полной статистикой"""
        current_value = self.current
        total_value = self.total

        if total_value == 0:
            percentage = 0
        else:
            percentage = (current_value / total_value) * 100
            if percentage > 100:
                percentage = 100

        # Рассчитываем заполненную часть
        if total_value > 0:
            filled_length = int(self.bar_length * current_value // total_value)
        else:
            filled_length = self.bar_length

        if filled_length > self.bar_length:
            filled_length = self.bar_length

        bar = self.filled_char * filled_length + self.empty_char * (self.bar_length - filled_length)

        # Рассчитываем время
        elapsed_time = time.time() - self.start_time
        if current_value > 0 and current_value < total_value:
            remaining_time = (elapsed_time / current_value) * (total_value - current_value)
            time_str = f"{elapsed_time:.0f}s < {remaining_time:.0f}s"
        else:
            time_str = f"{elapsed_time:.0f}s"

        # Форматируем сообщение
        if self.is_batch:
            message = f"💾 Обновление батча: {percentage:3.0f}% [{current_value}/{total_value}] [{bar}] ({time_str})"
        else:
            message = f"{self.desc}: {percentage:3.0f}% [{current_value}/{total_value}] [{bar}] "

            # ИСПОЛЬЗУЕМ СТАТИСТИКУ ИЗ self.stats
            found_count = self.stats.get('found_count', 0)
            total_criteria_found = self.stats.get('total_criteria_found', 0)
            in_batch = self.stats.get('in_batch', 0)
            skipped_total = self.stats.get('skipped_total', 0)
            not_found_count = self.stats.get('not_found_count', 0)
            errors = self.stats.get('errors', 0)
            updated = self.stats.get('updated', 0)

            spacing = " " * self.emoji_spacing

            # Формируем строку со статистикой для каждого значка
            message += f"🎯{spacing}{found_count:>{self.stat_width}} "
            message += f"📈{spacing}{total_criteria_found:>{self.stat_width}} "
            message += f"📦{spacing}{in_batch:>{self.stat_width}} "
            message += f"⏭️{spacing}{skipped_total:>{self.stat_width}} "
            message += f"🔍{spacing}{not_found_count:>{self.stat_width}} "
            message += f"❌{spacing}{errors:>{self.stat_width}} "
            message += f"💾{spacing}{updated:>{self.stat_width}} "
            message += f"({time_str})"

        return message

    def update(self, n: int = 1):
        """Обновить прогресс - ВСЕГДА обновляем отображение"""
        if not self._enabled:
            return

        self.current += n

        # Не позволяем current превышать total
        if self.current > self.total:
            self.current = self.total

        # ВАЖНО: ОБНОВЛЯЕМ ВСЕГДА, независимо от интервала!
        self.last_update_time = time.time()

        # Получаем сообщение
        message = self.get_current_message()

        # Определяем линию для этого прогресс-бара
        line = self.terminal.batch_progress_line if self.is_batch else self.terminal.main_progress_line

        # Выводим на фиксированной позиции внизу
        self.terminal.write_at_line(line, message)

        # ВАЖНО: Не вызываем flush здесь - это делает write_at_line

    def update_stats(self, stats: Dict[str, Any]):
        """Обновить статистику прогресс-бара"""
        for key, value in stats.items():
            if key in self.stats:
                self.stats[key] = value

        # НЕМЕДЛЕННО ОБНОВЛЯЕМ ОТОБРАЖЕНИЕ
        self._force_update()

    def _force_update(self):
        """Принудительно обновить отображение прогресс-бара"""
        if not self._enabled:
            return

        # Получаем сообщение
        message = self.get_current_message()

        # Определяем линию для этого прогресс-бара
        line = self.terminal.batch_progress_line if self.is_batch else self.terminal.main_progress_line

        # Выводим на фиксированной позиции внизу
        self.terminal.write_at_line(line, message)

    def finish(self, final_message: Optional[str] = None):
        """Завершить прогресс-бар"""
        if not self._enabled:
            return

        # НЕ меняем текущий прогресс - оставляем как есть
        # Получаем финальное сообщение с ТЕКУЩИМ прогрессом
        if final_message is None:
            final_message = self.get_current_message()  # Используем текущий прогресс

        # Записываем финальное сообщение
        line = self.terminal.batch_progress_line if self.is_batch else self.terminal.main_progress_line
        self.terminal.write_at_line(line, final_message)

        # СИНХРОНИЗИРУЕМ
        sys.stdout.flush()
        sys.stderr.flush()

        self._enabled = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()
