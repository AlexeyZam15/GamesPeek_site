# games/management/commands/analyze_game_criteria.py
from typing import Dict, List, Optional, Any
from games.models import Game
import os
import sys
from django.core.cache import cache
from django.db import transaction
from django.db.models import QuerySet

try:
    from .analyzer import AnalyzerCommandBase, GameAnalyzer, GameProcessor

    BaseCommand = AnalyzerCommandBase
except ImportError:
    from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Анализирует описание игры и определяет критерии (жанры, темы, перспективы, режимы) или ключевые слова'

    def add_arguments(self, parser):
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
        parser.add_argument('--hide-skipped', action='store_true',
                            help='Скрыть пропущенные критерии (уже существующие у игры)')
        parser.add_argument('--no-progress', action='store_true',
                            help='Отключить прогресс-бар')

        # НОВАЯ ОПЦИЯ: принудительный перезапуск
        parser.add_argument('--force-restart', action='store_true',
                            help='Начать обработку заново, игнорируя ранее обработанные игры')

        # ОПЦИЯ ДЛЯ РЕЖИМА КЛЮЧЕВЫХ СЛОВ
        parser.add_argument('--keywords', action='store_true',
                            help='Анализировать ТОЛЬКО ключевые слова (вместо обычных критериев)')

        text_source_group = parser.add_mutually_exclusive_group()
        text_source_group.add_argument('--use-storyline', action='store_true',
                                       help='Анализировать только сторилайн (игнорируя описание)')
        text_source_group.add_argument('--prefer-storyline', action='store_true',
                                       help='Предпочитать сторилайн описанию (если оба доступны)')
        text_source_group.add_argument('--combine-texts', action='store_true',
                                       help='Объединять описание и сторилайн для анализа')
        text_source_group.add_argument('--use-rawg', action='store_true',
                                       help='Анализировать только описание из RAWG.io')

        parser.add_argument('--ignore-existing', action='store_true',
                            help='Игнорировать существующие критерии и искать все паттерны')
        parser.add_argument('--clear-cache', action='store_true',
                            help='Очистить кеш перед началом обработки')

    def _store_options(self, options):
        """Сохраняет опции в атрибуты класса"""
        self.game_id = options.get('game_id')
        self.game_name = options.get('game_name')
        self.description = options.get('description')
        self.limit = options.get('limit')
        self.offset = options.get('offset')
        self.update_game = options.get('update_game', False)
        self.min_text_length = 0  # Оставляем 0, так как проверка в другом месте
        self.verbose = options.get('verbose', False)
        self.only_found = options.get('only_found', False)
        self.batch_size = options.get('batch_size', 1000)
        self.ignore_existing = options.get('ignore_existing', False)
        self.hide_skipped = options.get('hide_skipped', False)
        self.no_progress = options.get('no_progress', False)
        self.force_restart = options.get('force_restart', False)  # Важно: сохраняем как атрибут класса

        # Ключевая опция
        self.keywords = options.get('keywords', False)

        # Опции источников текста
        self.use_storyline = options.get('use_storyline', False)
        self.prefer_storyline = options.get('prefer_storyline', False)
        self.combine_texts = options.get('combine_texts', False)
        self.use_rawg = options.get('use_rawg', False)

        # Разрешаем приоритет источников текста
        self.text_source_mode = self._resolve_text_source_priority()

    def cleanup(self):
        """Очистка ресурсов"""
        if hasattr(self, 'analyzer'):
            self.analyzer.clear_caches()

        # Восстанавливаем оригинальные потоки и закрываем файл
        if hasattr(self, 'output_file') and self.output_file:
            try:
                # Сбрасываем буфер перед закрытием
                self.output_file.flush()

                # Закрываем файл
                self.output_file.close()
                self.stdout._out = self.original_stdout
                self.stderr._out = self.original_stderr

                # Выводим сообщение в терминал о завершении
                if self.output_path:
                    self.original_stdout.write(f"\n✅ Результаты экспортированы в: {self.output_path}\n")

                    # Сообщаем о файле состояния (соответствующем режиму)
                    # ТОЛЬКО если НЕ был использован force-restart
                    if not getattr(self, 'force_restart', False):
                        state_suffix = "_keywords" if hasattr(self, 'keywords') and self.keywords else "_criteria"
                        state_file = os.path.splitext(self.output_path)[0] + f'_state{state_suffix}.json'
                        if os.path.exists(state_file):
                            self.original_stdout.write(f"📝 Состояние сохранено в: {state_file}\n")

            except Exception as e:
                if hasattr(self, 'original_stderr'):
                    self.original_stderr.write(f"⚠️ Ошибка закрытия файла: {e}\n")

    def _init_stats_before_processing(self):
        """Инициализирует статистику перед началом обработки"""
        self.stats = {
            'processed': 0,
            'updated': 0,
            'skipped_no_text': 0,
            'errors': 0,
            'found_count': 0,
            'total_criteria_found': 0,
            'displayed_count': 0,
            'execution_time': 0,
            # Добавляем отдельный счетчик для ключевых слов
            'keywords_processed': 0,
            'keywords_found': 0,
            'keywords_count': 0,
        }

    def _print_final_terminal_stats(self, stats):
        """Выводит финальную статистику в терминал"""
        if not stats:
            return

        original_out = self.original_stdout or sys.stdout

        original_out.write("\n" + "=" * 60 + "\n")

        # Определяем заголовок в зависимости от режима
        if self.keywords:
            original_out.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КЛЮЧЕВЫЕ СЛОВА)\n")
        else:
            original_out.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КРИТЕРИИ)\n")

        original_out.write("=" * 60 + "\n")

        if self.keywords:
            original_out.write(f"Режим анализа: 🔑 КЛЮЧЕВЫЕ СЛОВА\n")
        else:
            original_out.write(f"Режим анализа: 📋 ОБЫЧНЫЕ КРИТЕРИИ\n")

        original_out.write(f"Источник текста: {self._get_text_source_description()}\n")
        original_out.write("-" * 60 + "\n")

        # Выводим соответствующую статистику
        if self.keywords:
            # Статистика для ключевых слов
            original_out.write(f"🔄 Обработано игр: {stats.get('keywords_processed', stats.get('processed', 0))}\n")
            original_out.write(
                f"🎯 Игр с найденными ключевыми словами: {stats.get('keywords_found', stats.get('found_count', 0))}\n")
            original_out.write(
                f"📈 Всего ключевых слов найдено: {stats.get('keywords_count', stats.get('total_criteria_found', 0))}\n")
        else:
            # Статистика для обычных критериев
            original_out.write(f"🔄 Обработано игр: {stats.get('processed', 0)}\n")
            original_out.write(f"🎯 Игр с найденными критериями: {stats.get('found_count', 0)}\n")
            original_out.write(f"📈 Всего критериев найдено: {stats.get('total_criteria_found', 0)}\n")

        original_out.write(f"⏭️ Игр без текста: {stats.get('skipped_no_text', 0)}\n")
        original_out.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")
        original_out.write(f"💾 Обновлено игр: {stats.get('updated', 0)}\n")

        if 'execution_time' in stats and stats['execution_time'] > 0:
            original_out.write(f"⏱️ Общее время выполнения: {stats['execution_time']:.1f} секунд\n")

        original_out.write("=" * 60 + "\n")
        original_out.flush()

    def _write_interruption_stats_to_file(self):
        """Записывает статистику прерывания в файл"""
        if not hasattr(self, 'stats') or not self.stats:
            # Если статистики нет, создаем пустую
            self.stats = {
                'processed': 0,
                'updated': 0,
                'skipped_no_text': 0,
                'errors': 0,
                'found_count': 0,
                'total_criteria_found': 0,
                'displayed_count': 0,
                'execution_time': 0,
                'keywords_processed': 0,
                'keywords_found': 0,
                'keywords_count': 0,
            }
            return

        try:
            self.stdout.write("\n" + "=" * 60 + "\n")

            # Определяем заголовок в зависимости от режима
            if hasattr(self, 'keywords') and self.keywords:
                self.stdout.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО) - КЛЮЧЕВЫЕ СЛОВА\n")
            else:
                self.stdout.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО) - КРИТЕРИИ\n")

            self.stdout.write("=" * 60 + "\n")

            # Определяем режим анализа
            mode = 'КЛЮЧЕВЫЕ СЛОВА' if hasattr(self, 'keywords') and self.keywords else 'КРИТЕРИИ'
            self.stdout.write(f"⚡ Режим анализа: {mode}\n")
            self.stdout.write("=" * 60 + "\n")

            # Используем self.stats
            if hasattr(self, 'keywords') and self.keywords:
                processed = self.stats.get('keywords_processed', self.stats.get('processed', 0))
                if processed > 0:
                    self.stdout.write(f"🔄 Обработано игр: {processed}\n")
                    self.stdout.write(f"🎯 Игр с найденными ключ. словами: {self.stats.get('keywords_found', 0)}\n")
                    self.stdout.write(f"📈 Всего ключ. слов найдено: {self.stats.get('keywords_count', 0)}\n")
                else:
                    self.stdout.write("ℹ️  Игры не были обработаны (прервано до начала анализа)\n")
            else:
                processed = self.stats.get('processed', 0)
                if processed > 0:
                    self.stdout.write(f"🔄 Обработано игр: {processed}\n")
                    self.stdout.write(f"🎯 Игр с найденными критериями: {self.stats.get('found_count', 0)}\n")
                    self.stdout.write(f"📈 Всего критериев найдено: {self.stats.get('total_criteria_found', 0)}\n")
                else:
                    self.stdout.write("ℹ️  Игры не были обработаны (прервано до начала анализа)\n")

            self.stdout.write(f"⏭️ Игр без текста: {self.stats.get('skipped_no_text', 0)}\n")
            self.stdout.write(f"❌ Ошибок: {self.stats.get('errors', 0)}\n")
            self.stdout.write(f"💾 Обновлено игр: {self.stats.get('updated', 0)}\n")

            if 'execution_time' in self.stats and self.stats['execution_time'] > 0:
                self.stdout.write(f"⏱️  Время выполнения до прерывания: {self.stats['execution_time']:.1f} секунд\n")

            self.stdout.write("=" * 60 + "\n")
            self.stdout.write("ℹ️  Обработка была прервана пользователем (Ctrl+C)\n")
            self.stdout.write(
                "ℹ️  Для продолжения используйте: python manage.py analyze_game_criteria --force-restart\n")
            self.stdout.write("=" * 60 + "\n")

        except Exception as e:
            # Выводим ошибку в терминал
            original_out = self.original_stdout or sys.stdout
            original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")

    def _print_final_stats_to_file(self):
        """Выводит финальную статистику в файл вывода"""
        if hasattr(self, 'output_file') and self.output_file and hasattr(self, 'stats') and self.stats:
            try:
                # Проверяем, что файл еще открыт
                if not self.output_file.closed:
                    # Используем self.stdout, так как он уже перенаправлен в файл
                    self.stdout.write("\n" + "=" * 60 + "\n")

                    # Определяем режим анализа
                    if hasattr(self, 'keywords') and self.keywords:
                        mode = 'КЛЮЧЕВЫЕ СЛОВА'
                    else:
                        mode = 'КРИТЕРИИ'

                    self.stdout.write(f"📊 ИТОГОВАЯ СТАТИСТИКА АНАЛИЗА ({mode})\n")
                    self.stdout.write("=" * 60 + "\n")

                    # Выводим соответствующую статистику
                    if hasattr(self, 'keywords') and self.keywords:
                        # Статистика для ключевых слов
                        stats_to_show = {
                            'keywords_processed': '🔄 Обработано игр',
                            'keywords_found': '🎯 Игр с найденными ключ. словами',
                            'keywords_count': '📈 Всего ключевых слов найдено',
                            'skipped_no_text': '⏭️ Игр без текста',
                            'errors': '❌ Ошибок',
                            'updated': '💾 Обновлено игр'
                        }
                    else:
                        # Статистика для обычных критериев
                        stats_to_show = {
                            'processed': '🔄 Обработано игр',
                            'found_count': '🎯 Игр с найденными критериями',
                            'total_criteria_found': '📈 Всего критериев найдено',
                            'skipped_no_text': '⏭️ Игр без текста',
                            'errors': '❌ Ошибок',
                            'updated': '💾 Обновлено игр'
                        }

                    for key, display_name in stats_to_show.items():
                        if key in self.stats:
                            self.stdout.write(f"{display_name}: {self.stats[key]}\n")

                    if 'execution_time' in self.stats and self.stats['execution_time'] > 0:
                        self.stdout.write(f"⏱️ Общее время выполнения: {self.stats['execution_time']:.1f} секунд\n")

                    self.stdout.write("=" * 60 + "\n")
                    self.stdout.write("✅ Анализ успешно завершен\n")
                    self.stdout.write("=" * 60 + "\n")

                    # Сбрасываем буфер
                    self.output_file.flush()
            except Exception as e:
                # Игнорируем ошибки при выводе статистики, но пишем в терминал
                original_out = self.original_stdout or sys.stdout
                original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")
                original_out.flush()

    def _format_stat_key(self, key: str) -> str:
        """Форматирует ключ статистики для вывода"""
        formats = {
            'processed': '🔄 Обработано игр',
            'updated': '💾 Обновлено игр',
            'skipped_no_text': '⏭️ Пропущено (нет текста)',
            'errors': '❌ Ошибок',
            'found_count': '🎯 Игр с найденными критериями',
            'total_criteria_found': '📈 Всего критериев найдено',
            'displayed_count': '👁️ Показано игр',
            # Новые ключи для ключевых слов
            'keywords_processed': '🔄 Обработано игр (ключ. слова)',
            'keywords_found': '🎯 Игр с найденными ключ. словами',
            'keywords_count': '📈 Всего ключевых слов найдено',
        }
        return formats.get(key, key.capitalize())

    def print_stats(self, title: str = "СТАТИСТИКА"):
        """Выводит статистику"""
        if not hasattr(self, 'stats') or not self.stats:
            return

        # Определяем заголовок в зависимости от режима
        if hasattr(self, 'keywords') and self.keywords:
            mode_title = "КЛЮЧЕВЫЕ СЛОВА"
        else:
            mode_title = "КРИТЕРИИ"

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(f"📊 {title} ({mode_title})")
        self.stdout.write("=" * 60)

        # Добавляем время выполнения если есть
        if 'execution_time' in self.stats:
            self.stdout.write(f"⏱️  Время выполнения: {self.stats['execution_time']:.2f} секунд")
            self.stdout.write("-" * 60)

        for key, value in self.stats.items():
            if isinstance(value, (int, float)) and key != 'execution_time':
                display_key = self._format_stat_key(key)
                # Показываем только релевантную статистику
                if hasattr(self, 'keywords') and self.keywords:
                    # Для режима ключевых слов показываем специфичную статистику
                    if key in ['keywords_processed', 'keywords_found', 'keywords_count',
                               'skipped_no_text', 'errors', 'updated', 'processed']:
                        self.stdout.write(f"{display_key}: {value}")
                else:
                    # Для обычного режима показываем обычную статистику
                    if key in ['processed', 'found_count', 'total_criteria_found',
                               'skipped_no_text', 'errors', 'updated']:
                        self.stdout.write(f"{display_key}: {value}")


    def analyze_all_games(self):
        """Анализирует все игры в базе данных с батчингом"""
        from games.models import Game

        base_query = self._get_base_query()
        total_games = base_query.count()

        games_queryset = base_query[self.offset:]
        if self.limit:
            games_queryset = games_queryset[:self.limit]

        actual_count = games_queryset.count()

        if not self.only_found:
            mode = '🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'
            self.stdout.write(f"🔍 Анализируем {actual_count} игр из {total_games}...\n")
            self.stdout.write(f"📚 Источник: {self._get_text_source_description()}\n")
            self.stdout.write(f"⚙️ Режим: {mode}\n")
            self.stdout.write(f"🔄 Обновление: {'✅ ВКЛ' if self.update_game else '❌ ВЫКЛ'}\n")
            self.stdout.write(f"👁️ Игнорировать существующие: {'✅ ДА' if self.ignore_existing else '❌ НЕТ'}\n")
            self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ДА' if self.hide_skipped else '❌ НЕТ'}\n")
            self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу\n")
            self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}\n")
            self.stdout.write("\n")

        # ИНИЦИАЛИЗИРУЕМ СТАТИСТИКУ ПЕРЕД НАЧАЛОМ
        self._init_stats_before_processing()

        # Создаем процессор и обрабатываем игры
        processor = GameProcessor(self)
        self.processor = processor  # Сохраняем ссылку на процессор для доступа к статистике

        stats = processor.process_games_batch(games_queryset)

        # ОБНОВЛЯЕМ СТАТИСТИКУ В КОМАНДЕ
        self._update_stats_from_processor(stats)

        # Выводим статистику в терминал
        self._print_final_terminal_stats(stats)

        # ВЫВОДИМ СТАТИСТИКУ В ФАЙЛ
        self._print_final_stats_to_file()

    def _update_stats_from_processor(self, processor_stats):
        """Обновляет статистику команды из статистики процессора"""
        if not processor_stats:
            return

        for key in self.stats.keys():
            if key in processor_stats:
                self.stats[key] = processor_stats[key]



    def handle(self, *args, **options):
        """Основной обработчик команды"""
        # Сохраняем оригинальные stdout/stderr
        self.original_stdout = self.stdout._out
        self.original_stderr = self.stderr._out
        self.output_file = None
        self.output_path = None  # Инициализируем путь к файлу

        # Настраиваем вывод в файл если указан
        if options.get('output'):
            try:
                output_path = options['output']
                self.output_path = output_path  # Сохраняем путь как атрибут
                self.output_file = open(output_path, 'w', encoding='utf-8')
                self.stdout._out = self.output_file
                self.stderr._out = self.output_file
                self.stdout.write(f"📁 Вывод будет сохранен в: {output_path}\n")
                self.stdout.write("-" * 60 + "\n")
            except Exception as e:
                self.stderr.write(f"❌ Ошибка открытия файла: {e}\n")
                # Восстанавливаем потоки при ошибке
                self.stdout._out = self.original_stdout
                self.stderr._out = self.original_stderr

        try:
            # Инициализируем анализатор
            self.analyzer = GameAnalyzer(self)

            if options.get('clear_cache'):
                cache.clear()
                if self.output_file:
                    self.stdout.write("✅ Кеш очищен\n")
                else:
                    self.original_stdout.write("✅ Кеш очищен\n")

            # Сохраняем опции
            self._store_options(options)

            if self.verbose:
                self._print_options_summary()

            # Обрабатываем команду
            self.process_command()

        except ValueError as e:
            if self.output_file:
                self.stderr.write(f"❌ Ошибка в опциях: {e}\n")
            else:
                self.original_stderr.write(f"❌ Ошибка в опциях: {e}\n")
        except KeyboardInterrupt:
            # ВЫВОДИМ СООБЩЕНИЕ О ПРЕРЫВАНИИ В ТЕРМИНАЛ
            original_out = self.original_stdout or sys.stdout
            original_out.write("\n⏹️ Обработка прервана пользователем\n")

            # ВЫВОДИМ СТАТИСТИКУ В ТЕРМИНАЛ
            if hasattr(self, 'stats'):
                self._display_interruption_stats_terminal(original_out)

            # ВЫВОДИМ СТАТИСТИКУ В ФАЙЛ (ОЧЕНЬ ВАЖНО!)
            if hasattr(self, 'output_file') and self.output_file and not self.output_file.closed:
                try:
                    # Сначала выводим сообщение о прерывании в файл
                    self.stdout.write("\n⏹️ Обработка прервана пользователем\n")

                    # Затем выводим статистику прерывания
                    self._write_interruption_stats_to_file()

                    # Сбрасываем буфер
                    self.output_file.flush()
                except Exception as e:
                    original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")

            # Сохраняем состояние при прерывании
            if hasattr(self, 'analyzer') and hasattr(self.analyzer, 'clear_caches'):
                self.analyzer.clear_caches()
        except Exception as e:
            if self.output_file:
                self.stderr.write(f"❌ Неожиданная ошибка: {e}\n")
                import traceback
                traceback.print_exc()
            else:
                self.original_stderr.write(f"❌ Неожиданная ошибка: {e}\n")
                import traceback
                traceback.print_exc(file=self.original_stderr)
        finally:
            self.cleanup()
            # Выводим финальную статистику в файл (для нормального завершения)
            if not isinstance(sys.exc_info()[0], KeyboardInterrupt):
                self._print_final_stats_to_file()

    def _init_output_streams(self, options):
        """Инициализирует выходные потоки"""
        self.original_stdout = self.stdout._out
        self.original_stderr = self.stderr._out
        self.output_file = None
        self.output_path = None

        # Настраиваем вывод в файл если указан
        if options.get('output'):
            self._setup_file_output(options['output'])

    def _setup_file_output(self, output_path):
        """Настраивает вывод в файл"""
        try:
            self.output_path = output_path
            self.output_file = open(output_path, 'w', encoding='utf-8')
            self.stdout._out = self.output_file
            self.stderr._out = self.output_file
            self.stdout.write(f"📁 Вывод будет сохранен в: {output_path}")
            self.stdout.write("-" * 60)
        except Exception as e:
            self.stderr.write(f"❌ Ошибка открытия файла: {e}")
            # Восстанавливаем потоки при ошибке
            self.stdout._out = self.original_stdout
            self.stderr._out = self.original_stderr

    def _setup_and_run_analysis(self, options):
        """Настраивает и запускает анализ"""
        from .analyzer import GameAnalyzer

        self.analyzer = GameAnalyzer(self)

        # ИНИЦИАЛИЗИРУЕМ СТАТИСТИКУ ПЕРЕД НАЧАЛОМ
        self.stats = {
            'processed': 0,
            'updated': 0,
            'skipped_no_text': 0,
            'errors': 0,
            'found_count': 0,
            'total_criteria_found': 0,
            'displayed_count': 0,
            'execution_time': 0,
        }

        if options.get('clear_cache'):
            cache.clear()
            self._write_to_output("✅ Кеш очищен\n")

        # Сохраняем опции
        self._store_options(options)

        if self.verbose:
            self._print_options_summary()

        # Обрабатываем команду
        self.process_command()

    def _init_stats(self, keys: list):
        """Инициализирует статистику"""
        self.stats = {key: 0 for key in keys}

    def _write_to_output(self, message):
        """Записывает сообщение в соответствующий вывод"""
        if self.output_file:
            self.stdout.write(message)
        else:
            self.original_stdout.write(f"{message}\n")

    def _handle_value_error(self, error):
        """Обрабатывает ошибку в опциях"""
        if self.output_file:
            self.stderr.write(f"❌ Ошибка в опциях: {error}")
        else:
            self.original_stderr.write(f"❌ Ошибка в опциях: {error}\n")

    def _handle_keyboard_interrupt(self):
        """Обрабатывает прерывание пользователем"""
        # Только одна строка - сообщение о прерывании
        if hasattr(self, 'original_stdout') and self.original_stdout:
            original_out = self.original_stdout
            original_out.write("\n⏹️ Обработка прервана пользователем\n")
            original_out.flush()

        # ВЫВОДИМ СТАТИСТИКУ ПРЕРЫВАНИЯ В ФАЙЛ
        if hasattr(self, 'output_file') and self.output_file and not self.output_file.closed:
            try:
                # Выводим статистику прерывания
                self._write_interruption_stats_to_file()
                self.output_file.flush()
            except Exception:
                # Игнорируем ошибки записи в файл
                pass

        # Сохраняем состояние при прерывании
        if hasattr(self, 'analyzer') and hasattr(self.analyzer, 'clear_caches'):
            self.analyzer.clear_caches()


    def _display_interruption_stats_terminal(self, output_stream):
        """Отображает статистику прерывания в терминале"""
        if hasattr(self, 'stats') and self.stats:
            output_stream.write("📊 Частичная статистика (прервано):\n")

            # Только ключевые статистики
            key_stats = ['processed', 'found_count', 'total_criteria_found',
                         'skipped_no_text', 'errors', 'updated']

            for key in key_stats:
                if key in self.stats:
                    display_name = self._format_stat_key(key)
                    output_stream.write(f"{display_name}: {self.stats[key]}\n")

            if 'execution_time' in self.stats and self.stats['execution_time'] > 0:
                output_stream.write(f"⏱️ Время выполнения до прерывания: {self.stats['execution_time']:.1f} секунд\n")

    def _handle_unexpected_error(self, error):
        """Обрабатывает неожиданную ошибку"""
        if self.output_file:
            self.stderr.write(f"❌ Неожиданная ошибка: {error}")
            import traceback
            traceback.print_exc()
        else:
            self.original_stderr.write(f"❌ Неожиданная ошибка: {error}\n")
            import traceback
            traceback.print_exc(file=self.original_stderr)

    def _cleanup_and_finalize(self):
        """Завершает работу и выводит финальную статистику"""
        self.cleanup()

        # Выводим финальную статистику в файл (для нормального завершения)
        if not isinstance(sys.exc_info()[0], KeyboardInterrupt):
            self._print_final_stats_to_file()

    def _write_stats_to_file(self, stats):
        """Записывает статистику в файл"""
        stats_to_show = {
            'processed': '🔄 Обработано игр',
            'found_count': '🎯 Игр с найденными критериями',
            'total_criteria_found': '📈 Всего критериев найдено',
            'skipped_no_text': '⏭️ Игр без текста',
            'errors': '❌ Ошибок',
            'updated': '💾 Обновлено игр'
        }

        for key, display_name in stats_to_show.items():
            if key in stats:
                self.stdout.write(f"{display_name}: {stats[key]}")

    def _print_interruption_stats_to_file(self):
        """Выводит статистику при прерывании в файл вывода"""
        if hasattr(self, 'output_file') and self.output_file and hasattr(self, 'stats') and self.stats:
            try:
                # Проверяем, что файл еще открыт
                if not self.output_file.closed:
                    # Используем self.stdout, так как он уже перенаправлен в файл
                    self.stdout.write("\n" + "=" * 60)
                    mode = 'ключевых слов' if self.keywords else 'критериев'
                    self.stdout.write(f"📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО)")
                    self.stdout.write(f"⚡ Режим анализа: {'КЛЮЧЕВЫЕ СЛОВА' if self.keywords else 'КРИТЕРИИ'}")
                    self.stdout.write("=" * 60)

                    # Выводим статистику
                    stats_to_show = {
                        'processed': '🔄 Обработано игр',
                        'found_count': '🎯 Игр с найденными критериями',
                        'total_criteria_found': '📈 Всего критериев найдено',
                        'skipped_no_text': '⏭️ Игр без текста',
                        'errors': '❌ Ошибок',
                        'updated': '💾 Обновлено игр',
                    }

                    for key, display_name in stats_to_show.items():
                        if key in self.stats:
                            self.stdout.write(f"{display_name}: {self.stats[key]}")

                    if 'execution_time' in self.stats:
                        self.stdout.write(
                            f"⏱️  Время выполнения до прерывания: {self.stats['execution_time']:.1f} секунд")

                    self.stdout.write("=" * 60)
                    self.stdout.write("ℹ️  Обработка была прервана пользователем (Ctrl+C)")
                    self.stdout.write(
                        "ℹ️  Для продолжения используйте: python manage.py analyze_game_criteria --force-restart")
                    self.stdout.write("=" * 60)

                    # Сбрасываем буфер
                    self.output_file.flush()
            except Exception:
                # Игнорируем ошибки при выводе статистики
                pass

    def process_command(self):
        """Обрабатывает команду в зависимости от аргументов"""
        if self.game_id:
            self.analyze_single_game_by_id(self.game_id)
        elif self.game_name:
            self.analyze_games_by_name(self.game_name)
        elif self.description:
            self.analyze_description(self.description)
        else:
            self.analyze_all_games()


    def analyze_single_game_by_id(self, game_id: int):
        """Анализирует одну игру по ID"""
        from games.models import Game

        try:
            game = Game.objects.get(id=game_id)
            mode = 'ключевые слова' if self.keywords else 'критерии'
            self.stdout.write(f"🎮 Анализируем игру: {game.name}")
            self.stdout.write(f"📊 Режим: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
            self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")

            existing_criteria = self._get_existing_criteria_summary(game)
            self.stdout.write(f"📋 Существующие {mode}: {existing_criteria}")
            self.stdout.write(f"👁️ Игнорировать существующие: {'✅ ДА' if self.ignore_existing else '❌ НЕТ'}")
            self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ДА' if self.hide_skipped else '❌ НЕТ'}")

            text_to_analyze = self.get_text_to_analyze(game)
            if not text_to_analyze:
                self.stderr.write("❌ У игры нет текста для анализа")
                return

            # Используем ВСЕ паттерны сразу
            results, pattern_info = self.analyzer.analyze_all_patterns(
                text_to_analyze,
                game=game,
                ignore_existing=self.ignore_existing,
                collect_patterns=self.verbose,
                keywords_mode=self.keywords
            )

            # Если режим keywords, фильтруем результаты
            if self.keywords:
                results = {'keywords': results.get('keywords', [])}
                pattern_info = {'keywords': pattern_info.get('keywords', [])}

            criteria_count = sum(len(results[key]) for key in results)
            self._print_game_results(game, results, criteria_count, pattern_info)

            if self.update_game:
                if self.update_game_criteria(game, results):
                    mode_text = "ключевые слова" if self.keywords else "критерии"
                    self.stdout.write(self.style.SUCCESS(f"✅ {mode_text.capitalize()} обновлены в базе данных"))

                    final_criteria = self._get_existing_criteria_summary(game)
                    self.stdout.write(f"📋 Итоговые {mode_text}: {final_criteria}")
                else:
                    mode_text = "ключевых слов" if self.keywords else "критериев"
                    self.stdout.write(f"ℹ️ Нет {mode_text} для обновления")

        except Game.DoesNotExist:
            self.stderr.write(f"❌ Игра с ID {game_id} не найдена")

    def analyze_games_by_name(self, game_name: str):
        """Анализирует игры по названию (частичное совпадение)"""
        from games.models import Game

        games = Game.objects.filter(name__icontains=game_name)

        if not games.exists():
            self.stderr.write(f"❌ Игры с названием содержащим '{game_name}' не найдены")
            return

        self.stdout.write(f"🔍 Найдено {games.count()} игр с названием содержащим '{game_name}'")

        for game in games:
            self.analyze_single_game_by_id(game.id)

    def analyze_description(self, description: str):
        """Анализирует произвольный текст описания"""
        mode = 'ключевые слова' if self.keywords else 'критерии'
        self.stdout.write(f"🔍 Анализируем произвольный текст описания...")
        self.stdout.write(f"📊 Режим: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
        self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")

        # Используем ВСЕ паттерны сразу
        results, pattern_info = self.analyzer.analyze_all_patterns(
            description,
            ignore_existing=True,
            collect_patterns=self.verbose,
            keywords_mode=self.keywords
        )

        # Если режим keywords, фильтруем результаты
        if self.keywords:
            results = {'keywords': results.get('keywords', [])}
            pattern_info = {'keywords': pattern_info.get('keywords', [])}

        criteria_count = sum(len(results[key]) for key in results)

        if criteria_count == 0:
            self.stdout.write(f"ℹ️ {mode.capitalize()} не найдены")
            return

        self.stdout.write(f"\n🎯 Найдено {mode}: {criteria_count}")

        for criteria_type, items in results.items():
            if items:
                display_name = self._get_display_name(criteria_type)
                item_names = [item.name for item in items]
                self.stdout.write(f"  📌 {display_name} ({len(items)}): {item_names}")

                if self.verbose and criteria_type in pattern_info:
                    self._print_pattern_details({criteria_type: pattern_info[criteria_type]})

    def _resolve_text_source_priority(self) -> str:
        """Разрешает приоритет опций источника текста"""
        if self.use_rawg:
            return 'use_rawg'
        if self.use_storyline:
            return 'use_storyline'
        elif self.prefer_storyline:
            return 'prefer_storyline'
        elif self.combine_texts:
            return 'combine_texts'
        else:
            return 'default'

    def get_text_to_analyze(self, game: Game) -> str:
        """Возвращает текст для анализа в зависимости от настроек"""
        has_summary = bool(game.summary and game.summary.strip())
        has_storyline = bool(game.storyline and game.storyline.strip())
        has_rawg = bool(game.rawg_description and game.rawg_description.strip())

        if self.text_source_mode == 'use_rawg':
            return game.rawg_description if has_rawg else ""

        if self.text_source_mode == 'use_storyline':
            return game.storyline if has_storyline else (game.summary if has_summary else "")

        if self.text_source_mode == 'prefer_storyline':
            if has_storyline:
                return game.storyline
            return game.summary if has_summary else ""

        if self.text_source_mode == 'combine_texts':
            texts = []
            if has_summary:
                texts.append(game.summary)
            if has_storyline:
                texts.append(game.storyline)
            return " ".join(texts) if texts else ""

        # default mode
        if has_summary:
            return game.summary
        return game.storyline if has_storyline else ""

    def _get_text_source_description(self) -> str:
        """Возвращает описание источника текста для анализа"""
        descriptions = {
            'use_rawg': 'ТОЛЬКО описание RAWG',
            'use_storyline': "ТОЛЬКО сторилайн",
            'prefer_storyline': "ПРЕДПОЧТИТЕЛЬНО сторилайн",
            'combine_texts': "ОБЪЕДИНЕННЫЙ текст",
            'default': "ПРЕДПОЧТИТЕЛЬНО описание IGDB"
        }
        return descriptions.get(self.text_source_mode, "Неизвестно")

    def _get_text_source_for_game(self, game: Game, text_to_analyze: str) -> str:
        """Определяет источник текста для отладочной информации"""
        if self.text_source_mode == 'combine_texts':
            return "объединенный текст"
        elif text_to_analyze == game.storyline:
            return "сторилайн"
        elif text_to_analyze == game.summary:
            return "описание IGDB"
        elif text_to_analyze == game.rawg_description:
            return "описание RAWG"
        else:
            return "неизвестный источник"

    def _get_base_query(self) -> QuerySet:
        """Возвращает базовый QuerySet для анализа"""
        return Game.objects.all().order_by('id')

    def _get_display_name(self, criteria_type: str) -> str:
        """Возвращает читаемое имя для типа критерия"""
        names = {
            'genres': 'Жанры',
            'themes': 'Темы',
            'perspectives': 'Перспективы',
            'game_modes': 'Режимы',
            'keywords': 'Ключевые слова'
        }
        return names.get(criteria_type, criteria_type)

    def _get_existing_criteria_summary(self, game: Game) -> str:
        """Возвращает строку с существующими критериями игры"""
        criteria_parts = []

        if self.keywords:
            # Только ключевые слова
            if game.keywords.exists():
                keyword_names = [keyword.name for keyword in game.keywords.all()[:5]]
                criteria_parts.append(f"ключевые слова: {keyword_names}" + ("..." if game.keywords.count() > 5 else ""))
        else:
            # Обычные критерии
            if game.genres.exists():
                genre_names = [genre.name for genre in game.genres.all()[:3]]
                criteria_parts.append(f"жанры: {genre_names}" + ("..." if game.genres.count() > 3 else ""))
            if game.themes.exists():
                theme_names = [theme.name for theme in game.themes.all()[:3]]
                criteria_parts.append(f"темы: {theme_names}" + ("..." if game.themes.count() > 3 else ""))
            if game.player_perspectives.exists():
                perspective_names = [perspective.name for perspective in game.player_perspectives.all()[:2]]
                criteria_parts.append(
                    f"перспективы: {perspective_names}" + ("..." if game.player_perspectives.count() > 2 else ""))
            if game.game_modes.exists():
                mode_names = [mode.name for mode in mode.game_modes.all()[:2]]
                criteria_parts.append(f"режимы: {mode_names}" + ("..." if game.game_modes.count() > 2 else ""))

        return ", ".join(criteria_parts) if criteria_parts else "нет"

    @transaction.atomic
    def update_game_criteria(self, game: Game, results: dict) -> bool:
        """Обновляет критерии игры в базе данных"""
        updated = False

        if self.keywords:
            # Обновляем только ключевые слова
            if 'keywords' in results:
                current_items = set(game.keywords.all())
                new_items = set(results['keywords'])

                if self.ignore_existing:
                    items_to_add = new_items - current_items
                else:
                    items_to_add = new_items - current_items

                if items_to_add:
                    if self.verbose:
                        self.stdout.write(f"   ➕ Добавлены ключевые слова: {[item.name for item in items_to_add]}")
                    game.keywords.add(*items_to_add)
                    updated = True
        else:
            # Обновляем обычные критерии
            field_mapping = {
                'genres': ('genres', game.genres),
                'themes': ('themes', game.themes),
                'perspectives': ('player_perspectives', game.player_perspectives),
                'game_modes': ('game_modes', game.game_modes),
            }

            for result_key, (field_name, current_manager) in field_mapping.items():
                if result_key in results:
                    current_items = set(current_manager.all())
                    new_items = set(results[result_key])

                    if self.ignore_existing:
                        items_to_add = new_items - current_items
                    else:
                        items_to_add = new_items - current_items

                    if items_to_add:
                        if self.verbose:
                            self.stdout.write(f"   ➕ Добавлены {field_name}: {[item.name for item in items_to_add]}")
                        getattr(game, field_name).add(*items_to_add)
                        updated = True

        if updated:
            game.save()

        return updated

    def _print_pattern_details(self, pattern_info: dict):
        """Выводит детальную информацию о совпадениях паттернов"""
        has_found_matches = False
        has_skipped_matches = False and not self.hide_skipped

        # Проверяем, есть ли что выводить
        for criteria_type, matches in pattern_info.items():
            for match in matches:
                if match.get('status') == 'found':
                    has_found_matches = True
                elif match.get('status') == 'skipped' and not self.hide_skipped:
                    has_skipped_matches = True

        if not (has_found_matches or has_skipped_matches):
            return

        # Выводим найденные совпадения
        if has_found_matches:
            self.stdout.write("  🔍 Совпадения паттернов:")
            seen_matches = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'found':
                        match_key = (match['pattern'], match['matched_text'], criteria_type)
                        if match_key not in seen_matches:
                            seen_matches.add(match_key)
                            pattern_display = match['pattern']
                            if len(pattern_display) > 80:
                                pattern_display = pattern_display[:77] + "..."
                            self.stdout.write(
                                f"    • '{match['matched_text']}' ← {self._get_display_name(criteria_type)}: {pattern_display}")

        # Выводим пропущенные критерии (если не скрыто)
        if has_skipped_matches and not self.hide_skipped:
            self.stdout.write("  ⏭️ Пропущенные критерии (уже существуют):")
            seen_skipped = set()

            for criteria_type, matches in pattern_info.items():
                for match in matches:
                    if match.get('status') == 'skipped':
                        if match['name'] not in seen_skipped:
                            seen_skipped.add(match['name'])
                            self.stdout.write(f"    • {match['name']} ({self._get_display_name(criteria_type)})")

    def _print_game_results(self, game, results, criteria_count: int, pattern_info: dict):
        """Выводит результаты анализа для игры с информацией о паттернами"""
        # В режиме ignore-existing показываем только те критерии, которые будут обновлены
        if self.ignore_existing and self.update_game:
            filtered_results = {}
            actual_criteria_count = 0

            for criteria_type, items in results.items():
                existing_items = self.analyzer._get_existing_objects(game, criteria_type)
                existing_names = {item.name for item in existing_items}
                new_items = [item for item in items if item.name not in existing_names]

                if new_items:
                    filtered_results[criteria_type] = new_items
                    actual_criteria_count += len(new_items)

            if actual_criteria_count == 0:
                return

            criteria_count = actual_criteria_count
            results = filtered_results

        mode = 'ключевые слова' if self.keywords else 'критерии'
        mode_text = f"Найдены {mode}" if self.ignore_existing else f"Найдены новые {mode}"

        self.stdout.write(f"🎯 {mode_text} для '{game.name}' ({criteria_count}):")

        # Сначала выводим все найденные критерии
        for criteria_type, items in results.items():
            if items:
                display_name = self._get_display_name(criteria_type)
                item_names = [item.name for item in items]
                self.stdout.write(f"  📌 {display_name} ({len(items)}): {item_names}")

        # Затем выводим информацию о паттернах
        if self.verbose:
            self._print_pattern_details(pattern_info)

    def _print_options_summary(self):
        """Выводит сводку по опциям"""
        self.stdout.write("=" * 60)
        self.stdout.write("🎮 НАСТРОЙКИ АНАЛИЗА ИГР")
        self.stdout.write("=" * 60)
        self.stdout.write(f"📊 Режим анализа: {'🔑 КЛЮЧЕВЫЕ СЛОВА' if self.keywords else '📋 ОБЫЧНЫЕ КРИТЕРИИ'}")
        self.stdout.write(f"🔄 Режим обновления: {'✅ ВКЛ' if self.update_game else '❌ ВЫКЛ'}")
        self.stdout.write(f"🔍 Игнорировать существующие: {'✅ ВКЛ' if self.ignore_existing else '❌ ВЫКЛ'}")
        self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ВКЛ' if self.hide_skipped else '❌ ВЫКЛ'}")
        self.stdout.write(f"📏 Проверка текста: {'❌ ВЫКЛ'}")
        self.stdout.write(f"🗣️ Подробный вывод: {'✅ ВКЛ' if self.verbose else '❌ ВЫКЛ'}")
        self.stdout.write(f"🎯 Только с найденными: {'✅ ВКЛ' if self.only_found else '❌ ВЫКЛ'}")
        self.stdout.write(f"📚 Источник текста: {self._get_text_source_description()}")
        self.stdout.write(f"📦 Размер батча: {self.batch_size}")
        self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")
        self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}")
        self.stdout.write("=" * 60)
        self.stdout.write("")


    def init_stats(self, keys: list):
        """Инициализирует статистику"""
        self.stats = {key: 0 for key in keys}

    def update_stat(self, key: str, value: int = 1):
        """Обновляет статистику"""
        if hasattr(self, 'stats') and key in self.stats:
            self.stats[key] += value

    def get_stat(self, key: str) -> int:
        """Получает значение статистики"""
        return self.stats.get(key, 0) if hasattr(self, 'stats') else 0
