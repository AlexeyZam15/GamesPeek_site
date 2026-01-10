# games/management/commands/analyze_game_criteria.py
"""
Команда анализа игр - полная интеграция с новым API
Поддерживает все режимы: обычный, ключевые слова, комбинированный и комплексный
"""

import sys
import os
from django.core.management.base import BaseCommand
from games.management.commands.analyzer.analyzer_command import AnalyzerCommand


class Command(AnalyzerCommand):
    help = """
    Анализирует описание игры и определяет критерии или ключевые слова

    Режимы работы:
    1. Обычный анализ (по умолчанию): жанры, темы, перспективы, режимы игры
    2. Анализ ключевых слов (--keywords)
    3. Комбинированный анализ (--combined): критерии + ключевые слова
    4. Комплексный анализ (--comprehensive): находит ВСЕ вхождения в тексте

    Примеры использования:
        python manage.py analyze_game_criteria --game-id 123
        python manage.py analyze_game_criteria --keywords --update-game
        python manage.py analyze_game_criteria --comprehensive --verbose
        python manage.py analyze_game_criteria --combined --output results.txt
        python manage.py analyze_game_criteria --limit 100 --exclude-existing
    """

    def add_arguments(self, parser):
        """Аргументы командной строки с поддержкой всех режимов нового API"""
        # Основные параметры
        parser.add_argument('--game-id', type=int,
                            help='ID конкретной игры для анализа')
        parser.add_argument('--game-name', type=str,
                            help='Поиск игр по названию (частичное совпадение)')
        parser.add_argument('--description', type=str,
                            help='Текст описания для анализа (без поиска в БД)')

        # Параметры массовой обработки
        parser.add_argument('--limit', type=int, default=None,
                            help='Максимальное количество игр для анализа')
        parser.add_argument('--offset', type=int, default=0,
                            help='Пропустить первые N игр')
        parser.add_argument('--batch-size', type=int, default=1000,
                            help='Размер батча для обработки в памяти')

        # Флаги обработки
        parser.add_argument('--update-game', action='store_true',
                            help='Автоматически обновить найденные критерии в базе данных')
        parser.add_argument('--verbose', action='store_true',
                            help='Подробный вывод с информацией о паттернах')
        parser.add_argument('--only-found', action='store_true',
                            help='Показывать только игры где были найдены критерии')
        parser.add_argument('--ignore-existing', action='store_true',
                            help='Игнорировать проверку существующих критериев (добавлять все найденные)')
        parser.add_argument('--exclude-existing', action='store_true', default=True,
                            help='Исключать уже существующие критерии из результатов анализа (по умолчанию ВКЛЮЧЕНО)')
        parser.add_argument('--hide-skipped', action='store_true',
                            help='Скрыть информацию о пропущенных критериях')
        parser.add_argument('--no-progress', action='store_true',
                            help='Отключить отображение прогресс-бара')
        parser.add_argument('--force-restart', action='store_true',
                            help='Начать обработку заново, игнорируя состояние предыдущих запусков')
        parser.add_argument('--clear-cache', action='store_true',
                            help='Очистить кеш анализатора перед запуском')

        # Параметры текста
        parser.add_argument('--min-text-length', type=int, default=10,
                            help='Минимальная длина текста для анализа (символов)')
        parser.add_argument('--output', type=str,
                            help='Путь к файлу для экспорта результатов')

        # РЕЖИМЫ АНАЛИЗА (взаимоисключающие)
        mode_group = parser.add_mutually_exclusive_group()
        mode_group.add_argument('--keywords', action='store_true',
                                help='Анализировать ТОЛЬКО ключевые слова')
        mode_group.add_argument('--combined', action='store_true',
                                help='Анализировать КАК критерии, ТАК и ключевые слова')
        mode_group.add_argument('--comprehensive', action='store_true',
                                help='Комплексный анализ с поиском ВСЕХ вхождений элементов')
        # По умолчанию: обычный анализ критериев

        # ИСТОЧНИКИ ТЕКСТА (взаимоисключающие)
        text_source_group = parser.add_mutually_exclusive_group()
        text_source_group.add_argument('--use-wiki', action='store_true',
                                       help='Использовать ТОЛЬКО описание из Wikipedia')
        text_source_group.add_argument('--use-rawg', action='store_true',
                                       help='Использовать ТОЛЬКО описание из RAWG.io')
        text_source_group.add_argument('--use-storyline', action='store_true',
                                       help='Использовать ТОЛЬКО сторилайн игры')
        text_source_group.add_argument('--prefer-wiki', action='store_true',
                                       help='Предпочитать Wikipedia описание другим источникам')
        text_source_group.add_argument('--prefer-storyline', action='store_true',
                                       help='Предпочитать сторилайн основному описанию')
        text_source_group.add_argument('--combine-texts', action='store_true',
                                       help='Объединить описание и сторилайн (только IGDB)')
        text_source_group.add_argument('--combine-all-texts', action='store_true',
                                       help='Объединить ВСЕ доступные источники текста')

    def handle(self, *args, **options):
        """
        Основной обработчик команды с логикой выбора режима анализа
        """
        # ДОБАВИМ обработку режимов из нового API
        if options.get('comprehensive'):
            self.stdout.write("🔍 Включен режим КОМПЛЕКСНОГО анализа (все вхождения)")
        elif options.get('combined'):
            self.stdout.write("🔍 Включен режим КОМБИНИРОВАННОГО анализа (критерии + ключевые слова)")
        elif options.get('keywords'):
            self.stdout.write("🔍 Включен режим анализа КЛЮЧЕВЫХ СЛОВ")
        else:
            self.stdout.write("🔍 Включен режим анализа ОБЫЧНЫХ КРИТЕРИЕВ")

        # Сохраняем новые опции
        self.comprehensive_mode = options.get('comprehensive', False)
        self.combined_mode = options.get('combined', False)
        self.exclude_existing = options.get('exclude_existing', False)

        # Вызываем родительский обработчик
        super().handle(*args, **options)