import sys
import time
from typing import Optional, Dict
from .output_manager import UnifiedProgressBar


class ProgressBar:
    """Класс для отображения прогресс-бара в терминале (обертка)"""

    def __init__(self, total: int, desc: str = "Анализ игр",
                 bar_length: int = 30, update_interval: float = 0.1,
                 stat_width: int = 5, emoji_spacing: int = 1,
                 terminal_stream=None):
        # Создаем унифицированный прогресс-бар
        self._progress_bar = UnifiedProgressBar(
            total=total,
            desc=desc,
            bar_length=bar_length,
            update_interval=update_interval,
            stat_width=stat_width,
            emoji_spacing=emoji_spacing,
            is_batch=False
        )

        # Публичные атрибуты для обратной совместимости
        self.total = total
        self.current = 0
        self.desc = desc

    def update(self, n: int = 1):
        """Обновить прогресс"""
        self.current += n
        self._progress_bar.update(n)

    def set_enabled(self, enabled: bool):
        """Включить/выключить прогресс-бар"""
        self._progress_bar._enabled = enabled

    def update_stats(self, stats: Dict):
        """Обновить статистику"""
        # Исправлено: правильно вызываем метод update_stats у UnifiedProgressBar
        if hasattr(self._progress_bar, 'stats'):
            # Обновляем статистику в словаре stats
            for key, value in stats.items():
                if key in self._progress_bar.stats:
                    self._progress_bar.stats[key] = value
        else:
            # Если нет атрибута stats, создаем его
            self._progress_bar.stats = stats

    def finish(self):
        """Завершить прогресс-бар - просто останавливаем"""
        # НЕ вызываем finish у внутреннего прогресс-бара
        # Просто останавливаем обновления
        self.set_enabled(False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.finish()
