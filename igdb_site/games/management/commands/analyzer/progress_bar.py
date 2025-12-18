# games/management/commands/analyzer/progress_bar.py
import sys
import time
from typing import Optional, Dict

class ProgressBar:
    """Класс для отображения прогресс-бара в терминале (stderr)"""

    def __init__(self, total: int, desc: str = "Анализ игр",
                 bar_length: int = 30, update_interval: float = 0.1):
        """
        Инициализация прогресс-бара

        Args:
            total: Общее количество элементов
            desc: Описание процесса
            bar_length: Длина полоски прогресса в символах
            update_interval: Минимальный интервал обновления в секундах
        """
        self.total = total
        self.desc = desc
        self.bar_length = bar_length
        self.update_interval = update_interval

        self.current = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self._enabled = True

        # Статистика
        self.stats = {
            'found_count': 0,
            'total_criteria_found': 0,
            'skipped_no_text': 0,
            'errors': 0,
            'updated': 0,
        }

        # Настройки для стилей
        self.filled_char = '█'
        self.empty_char = '░'

        # Всегда используем stderr для прогресс-бара
        self.terminal_stderr = sys.stderr

    def set_enabled(self, enabled: bool):
        """Включить/выключить прогресс-бар"""
        self._enabled = enabled

    def update_stats(self, stats: Dict):
        """Обновить статистику"""
        self.stats.update(stats)

    def update(self, n: int = 1):
        """
        Обновить прогресс
        """
        if not self._enabled:
            return

        self.current += n
        current_time = time.time()

        # Обновляем не чаще чем update_interval секунд
        if current_time - self.last_update_time < self.update_interval and self.current < self.total:
            return

        self.last_update_time = current_time

        # Рассчитываем процент
        percentage = (self.current / self.total) * 100 if self.total > 0 else 0

        # Рассчитываем заполненную часть
        filled_length = int(self.bar_length * self.current // self.total)
        bar = self.filled_char * filled_length + self.empty_char * (self.bar_length - filled_length)

        # Рассчитываем время
        elapsed_time = current_time - self.start_time
        if self.current > 0 and self.current < self.total:
            # Оставшееся время
            remaining_time = (elapsed_time / self.current) * (self.total - self.current)
            time_str = f"{elapsed_time:.0f}s < {remaining_time:.0f}s"
        else:
            time_str = f"{elapsed_time:.0f}s"

        # Более короткий формат без пробелов между эмодзи и цифрами
        message = f"\r{self.desc}: {percentage:3.0f}% [{self.current}/{self.total}] [{bar}] "
        message += f"🎯{self.stats['found_count']:3} 📈{self.stats['total_criteria_found']:3} "
        message += f"⏭️{self.stats['skipped_no_text']:3} ❌{self.stats['errors']:3} 💾{self.stats['updated']:3} "
        message += f"({time_str})"

        # Добавляем много пробелов для очистки
        message += " " * 50

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

        # Убедимся что показываем 100%
        if self.current < self.total:
            self.update(self.total - self.current)

        # Очищаем строку
        self.terminal_stderr.write("\r" + " " * 150 + "\r")
        self.terminal_stderr.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()