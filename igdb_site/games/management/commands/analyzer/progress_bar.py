# games/management/commands/analyzer/progress_bar.py
import sys
import time
from typing import Optional, Dict


class ProgressBar:
    """Класс для отображения прогресс-бара в терминале (stderr)"""

    def __init__(self, total: int, desc: str = "Анализ игр",
                 bar_length: int = 30, update_interval: float = 0.1,
                 stat_width: int = 5, emoji_spacing: int = 1):  # Добавляем параметр для отступа
        self.total = total
        self.desc = desc
        self.bar_length = bar_length
        self.update_interval = update_interval
        self.stat_width = stat_width
        self.emoji_spacing = emoji_spacing  # Отступ между эмодзи и цифрами

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
        self.terminal_stderr = sys.stderr

    def set_enabled(self, enabled: bool):
        """Включить/выключить прогресс-бар"""
        self._enabled = enabled

    def update_stats(self, stats: Dict):
        """Обновить статистику"""
        self.stats.update(stats)

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

        # Рассчитываем процент (но не больше 100%)
        percentage = (self.current / self.total) * 100 if self.total > 0 else 0
        if percentage > 100:
            percentage = 100

        # Рассчитываем заполненную часть
        filled_length = int(self.bar_length * self.current // self.total) if self.total > 0 else self.bar_length
        if filled_length > self.bar_length:
            filled_length = self.bar_length

        bar = self.filled_char * filled_length + self.empty_char * (self.bar_length - filled_length)

        # Рассчитываем время
        elapsed_time = current_time - self.start_time
        if self.current > 0 and self.current < self.total:
            # Оставшееся время
            remaining_time = (elapsed_time / self.current) * (self.total - self.current)
            time_str = f"{elapsed_time:.0f}s < {remaining_time:.0f}s"
        else:
            time_str = f"{elapsed_time:.0f}s"

        # Форматируем сообщение с фиксированными отступами
        message = f"\r{self.desc}: {percentage:3.0f}% [{self.current}/{self.total}] [{bar}] "

        # Добавляем отступы между эмодзи и цифрами
        # self.emoji_spacing пробелов после каждого эмодзи
        spacing = " " * self.emoji_spacing

        message += f"🎯{spacing}{self.stats['found_count']:>{self.stat_width}} "
        message += f"📈{spacing}{self.stats['total_criteria_found']:>{self.stat_width}} "
        message += f"⏭️{spacing}{self.stats['skipped_total']:>{self.stat_width}} "
        message += f"❌{spacing}{self.stats['errors']:>{self.stat_width}} "
        message += f"💾{spacing}{self.stats['updated']:>{self.stat_width}} "
        message += f"({time_str})"

        # Добавляем пробелы для очистки остатков предыдущей строки
        message += " " * 30

        self.terminal_stderr.write(message)
        self.terminal_stderr.flush()

        # Если завершено, переходим на новую строку
        if self.current >= self.total:
            self.terminal_stderr.write("\n")
            self.terminal_stderr.flush()

    def finish(self):
        """Завершить прогресс-бар"""
        if not self._enabled:
            return

        # Убедимся что показываем актуальный прогресс
        if self.current < self.total:
            # Выводим финальное состояние
            current_time = time.time()
            elapsed_time = current_time - self.start_time

            # Форматируем сообщение с фактическим прогрессом
            percentage = (self.current / self.total) * 100 if self.total > 0 else 0
            if percentage > 100:
                percentage = 100

            filled_length = int(self.bar_length * self.current // self.total) if self.total > 0 else 0
            if filled_length > self.bar_length:
                filled_length = self.bar_length

            bar = self.filled_char * filled_length + self.empty_char * (self.bar_length - filled_length)

            message = f"\r{self.desc}: {percentage:3.0f}% [{self.current}/{self.total}] [{bar}] "

            # Добавляем отступы между эмодзи и цифрами
            spacing = " " * self.emoji_spacing

            message += f"🎯{spacing}{self.stats['found_count']:>{self.stat_width}} "
            message += f"📈{spacing}{self.stats['total_criteria_found']:>{self.stat_width}} "
            message += f"⏭️{spacing}{self.stats['skipped_total']:>{self.stat_width}} "
            message += f"❌{spacing}{self.stats['errors']:>{self.stat_width}} "
            message += f"💾{spacing}{self.stats['updated']:>{self.stat_width}} "
            message += f"({elapsed_time:.0f}s)"

            message += " " * 30

            self.terminal_stderr.write(message)
            self.terminal_stderr.flush()

            # Переходим на новую строку
            self.terminal_stderr.write("\n")
            self.terminal_stderr.flush()
        else:
            # Если уже на 100%, просто очищаем строку
            self.terminal_stderr.write("\r" + " " * 150 + "\r")
            self.terminal_stderr.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()
