# games/management/commands/analyzer/__init__.py
"""
Модуль для команды анализа игр - УПРОЩЕННАЯ ВЕРСИЯ для совместимости со старым кодом

Основные компоненты:
    analyzer_command - основной класс команды с поддержкой нового API
    progress_bar - прогресс-бар для отображения хода выполнения
    text_preparer - подготовщик текста для анализа
    state_manager - менеджер состояния обработки
    batch_updater - батч-апдейтер для обновления БД
    output_formatter - форматировщик вывода

Примечание:
    - PatternManager теперь находится в games/analyze/pattern_manager.py
    - Основная логика анализа в games/analyze/
    - Этот модуль только для обратной совместимости со старой структурой команд
"""

from .progress_bar import ProgressBar
# PatternManager удален из импорта - используйте games.analyze.pattern_manager
from .analyzer_command import AnalyzerCommand
from .text_preparer import TextPreparer
from .state_manager import StateManager
from .batch_updater import BatchUpdater
from .output_formatter import OutputFormatter

__all__ = [
    'ProgressBar',
    'AnalyzerCommand',
    'TextPreparer',
    'StateManager',
    'BatchUpdater',
    'OutputFormatter',
]

__version__ = '2.0.0'
__author__ = 'Game Analysis Team'
__description__ = 'Команда анализа игр с поддержкой нового API анализа'