# games/management/commands/analyze_game_criteria.py
import sys
import os
from django.core.management.base import BaseCommand
from games.management.commands.analyzer.analyzer_command import AnalyzerCommand


class Command(AnalyzerCommand):
    help = 'Анализирует описание игры и определяет критерии или ключевые слова'

    def add_arguments(self, parser):
        """Аргументы командной строки (полная совместимость)"""
        parser.add_argument('--game-id', type=int, help='ID игры в базе данных для анализа')
        parser.add_argument('--game-name', type=str, help='Название игры для анализа')
        parser.add_argument('--description', type=str, help='Текст описания для анализа')
        parser.add_argument('--limit', type=int, default=None, help='Лимит игр для анализа')
        parser.add_argument('--offset', type=int, default=0, help='Пропустить первые N игр')
        parser.add_argument('--update-game', action='store_true', help='Обновить найденные критерии')
        parser.add_argument('--min-text-length', type=int, default=10, help='Минимальная длина текста для анализа')
        parser.add_argument('--verbose', action='store_true', help='Подробный вывод процесса анализа')
        parser.add_argument('--output', type=str, help='Экспорт вывода в файл')
        parser.add_argument('--only-found', action='store_true',
                            help='Показывать только игры где были найдены критерии')
        parser.add_argument('--batch-size', type=int, default=1000, help='Размер батча для обработки')
        parser.add_argument('--ignore-existing', action='store_true',
                            help='Игнорировать существующие критерии и искать все паттерны')
        parser.add_argument('--hide-skipped', action='store_true',
                            help='Скрыть пропущенные критерии (уже существующие у игры)')
        parser.add_argument('--no-progress', action='store_true', help='Отключить прогресс-бар')
        parser.add_argument('--force-restart', action='store_true',
                            help='Начать обработку заново, игнорируя ранее обработанные игры')
        parser.add_argument('--keywords', action='store_true',
                            help='Анализировать ТОЛЬКО ключевые слова (вместо обычных критериев)')

        text_source_group = parser.add_mutually_exclusive_group()
        text_source_group.add_argument('--use-wiki', action='store_true',
                                       help='Анализировать только описание из Wikipedia')
        text_source_group.add_argument('--use-rawg', action='store_true',
                                       help='Анализировать только описание из RAWG.io')
        text_source_group.add_argument('--use-storyline', action='store_true',
                                       help='Анализировать только сторилайн (игнорируя описание)')
        text_source_group.add_argument('--prefer-wiki', action='store_true',
                                       help='Предпочитать Wikipedia описание другим источникам')
        text_source_group.add_argument('--prefer-storyline', action='store_true',
                                       help='Предпочитать сторилайн описанию (если оба доступны)')
        text_source_group.add_argument('--combine-all-texts', action='store_true',
                                       help='Объединять ВСЕ источники текста для анализа')
        text_source_group.add_argument('--combine-texts', action='store_true',
                                       help='Объединять описание и сторилайн для анализа')

        parser.add_argument('--clear-cache', action='store_true',
                            help='Очистить кеш перед началом обработки')