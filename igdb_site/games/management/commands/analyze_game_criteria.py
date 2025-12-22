# games/management/commands/analyze_game_criteria.py
from typing import Dict, List, Optional, Any, Tuple
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

        parser.add_argument('--ignore-existing', action='store_true',
                            help='Игнорировать существующие критерии и искать все паттерны')
        parser.add_argument('--clear-cache', action='store_true',
                            help='Очистить кеш перед началом обработки')

    def _store_options(self, options):
        """Сохраняет опции в атрибуты класса"""
        super()._store_options(options)

    def cleanup(self):
        """Очистка ресурсов"""
        if hasattr(self, 'analyzer'):
            self.analyzer.clear_caches()

        if hasattr(self, 'output_file') and self.output_file:
            try:
                self.output_file.flush()
                self.output_file.close()
                self.stdout._out = self.original_stdout
                self.stderr._out = self.original_stderr

                if hasattr(self, 'output_path') and self.output_path:
                    self.original_stdout.write(f"\n✅ Результаты экспортированы в: {self.output_path}\n")

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
            'keywords_processed': 0,
            'keywords_found': 0,
            'keywords_count': 0,
        }

    def handle(self, *args, **options):
        """Основной обработчик команды"""
        self.original_stdout = self.stdout._out
        self.original_stderr = self.stderr._out
        self.output_file = None
        self.output_path = None

        if options.get('output'):
            try:
                output_path = options['output']
                self.output_path = output_path
                self.setup_file_output(output_path)
            except Exception as e:
                self.stderr.write(f"❌ Ошибка открытия файла: {e}\n")
                self.stdout._out = self.original_stdout
                self.stderr._out = self.original_stderr

        try:
            self.analyzer = GameAnalyzer(self)

            if options.get('clear_cache'):
                cache.clear()
                if self.output_file:
                    self.stdout.write("✅ Кеш очищен\n")
                else:
                    self.original_stdout.write("✅ Кеш очищен\n")

            self._store_options(options)

            if self.verbose:
                self._print_options_summary()

            self.process_command()

        except ValueError as e:
            if self.output_file:
                self.stderr.write(f"❌ Ошибка в опциях: {e}\n")
            else:
                self.original_stderr.write(f"❌ Ошибка в опциях: {e}\n")
        except KeyboardInterrupt:
            # Сначала завершаем прогресс-бар
            if hasattr(self, 'processor') and hasattr(self.processor, 'progress_bar'):
                self.processor._finish_progress_bar()

            original_out = self.original_stdout or sys.stdout
            original_out.write("\n⏹️ Обработка прервана пользователем\n")

            if hasattr(self, 'stats'):
                self._display_interruption_stats_terminal(original_out)

            if hasattr(self, 'output_file') and self.output_file and not self.output_file.closed:
                try:
                    self.stdout.write("\n⏹️ Обработка прервана пользователем\n")
                    self._write_interruption_stats_to_file()
                    self.output_file.flush()
                except Exception as e:
                    original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")

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
            if not isinstance(sys.exc_info()[0], KeyboardInterrupt):
                self._print_final_stats_to_file()

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
            self.stdout.write(f"🔄 Обновление: {'✅ ВКЛ' if self.update_game else '❌ ВЫКЛ'}")
            self.stdout.write(f"👁️ Игнорировать существующие: {'✅ ДА' if self.ignore_existing else '❌ НЕТ'}")
            self.stdout.write(f"👁️ Скрыть пропущенные: {'✅ ДА' if self.hide_skipped else '❌ НЕТ'}")
            self.stdout.write(f"⚡ Стратегия: ВСЕ паттерны сразу")
            self.stdout.write(f"📊 Прогресс-бар: {'✅ ВКЛ' if not self.no_progress else '❌ ВЫКЛ'}")
            self.stdout.write("\n")

        self._init_stats_before_processing()

        processor = GameProcessor(self)
        self.processor = processor

        stats = processor.process_games_batch(games_queryset)

        self._update_stats_from_processor(stats)

        self._print_final_terminal_stats(stats)
        self._print_final_stats_to_file()

    def _update_stats_from_processor(self, processor_stats):
        """Обновляет статистику команды из статистики процессора"""
        if not processor_stats:
            return

        for key in self.stats.keys():
            if key in processor_stats:
                self.stats[key] = processor_stats[key]

    def _print_final_terminal_stats(self, stats):
        """Выводит финальную статистику в терминал"""
        if not stats:
            return

        original_out = self.original_stdout or sys.stdout

        original_out.write("\n" + "=" * 60 + "\n")

        if self.keywords:
            original_out.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КЛЮЧЕВЫЕ СЛОВА)\n")
        else:
            original_out.write("📊 ФИНАЛЬНАЯ СТАТИСТИКА АНАЛИЗА (КРИТЕРИИ)\n")

        original_out.write("=" * 60 + "\n")

        # ВАЖНО: показываем РЕАЛЬНО обработанные игры, а не общее количество в базе
        if self.keywords:
            processed_count = stats.get('keywords_processed', stats.get('processed', 0))
            original_out.write(f"🔄 Обработано игр: {processed_count}\n")
            original_out.write(
                f"🎯 Игр с найденными ключ. словами: {stats.get('keywords_found', stats.get('found_count', 0))}\n")
            original_out.write(
                f"📈 Всего ключевых слов найдено: {stats.get('keywords_count', stats.get('total_criteria_found', 0))}\n")
        else:
            processed_count = stats.get('processed', 0)
            original_out.write(f"🔄 Обработано игр: {processed_count}\n")
            original_out.write(f"🎯 Игр с найденными критериями: {stats.get('found_count', 0)}\n")
            original_out.write(f"📈 Всего критериев найдено: {stats.get('total_criteria_found', 0)}\n")

        original_out.write(f"⏭️ Игр без текста: {stats.get('skipped_no_text', 0)}\n")
        original_out.write(f"❌ Ошибок: {stats.get('errors', 0)}\n")
        original_out.write(f"💾 Обновлено игр: {stats.get('updated', 0)}\n")

        if 'execution_time' in stats and stats['execution_time'] > 0:
            original_out.write(f"⏱️ Общее время выполнения: {stats['execution_time']:.1f} секунд\n")

        original_out.write("=" * 60 + "\n")
        original_out.flush()

    def _print_final_stats_to_file(self):
        """Выводит финальную статистику в файл вывода"""
        if hasattr(self, 'output_file') and self.output_file and hasattr(self, 'stats') and self.stats:
            try:
                if not self.output_file.closed:
                    self.stdout.write("\n" + "=" * 60 + "\n")

                    mode = 'КЛЮЧЕВЫЕ СЛОВА' if hasattr(self, 'keywords') and self.keywords else 'КРИТЕРИИ'
                    self.stdout.write(f"📊 ИТОГОВАЯ СТАТИСТИКА АНАЛИЗА ({mode})\n")
                    self.stdout.write("=" * 60 + "\n")

                    if self.keywords:
                        stats_to_show = {
                            'keywords_processed': '🔄 Обработано игр',
                            'keywords_found': '🎯 Игр с найденными ключ. словами',
                            'keywords_count': '📈 Всего ключевых слов найдено',
                            'skipped_no_text': '⏭️ Игр без текста',
                            'errors': '❌ Ошибок',
                            'updated': '💾 Обновлено игр'
                        }
                    else:
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

                    self.output_file.flush()
            except Exception as e:
                original_out = self.original_stdout or sys.stdout
                original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")
                original_out.flush()

    def _write_interruption_stats_to_file(self):
        """Записывает статистику прерывания в файл"""
        if not hasattr(self, 'stats') or not self.stats:
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

            if hasattr(self, 'keywords') and self.keywords:
                self.stdout.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО) - КЛЮЧЕВЫЕ СЛОВА\n")
            else:
                self.stdout.write("📊 ЧАСТИЧНАЯ СТАТИСТИКА (ПРЕРВАНО) - КРИТЕРИИ\n")

            self.stdout.write("=" * 60 + "\n")

            mode = 'КЛЮЧЕВЫЕ СЛОВА' if hasattr(self, 'keywords') and self.keywords else 'КРИТЕРИИ'
            self.stdout.write(f"⚡ Режим анализа: {mode}\n")
            self.stdout.write("=" * 60 + "\n")

            if hasattr(self, 'keywords') and self.keywords:
                processed = self.stats.get('keywords_processed', self.stats.get('processed', 0))
                if processed > 0:
                    self.stdout.write(f"🔄 Обработано игр: {processed}\n")
                    self.stdout.write(f"🎯 Игр с найденными ключ. словами: {self.stats.get('keywords_found', 0)}\n")
                    self.stdout.write(f"📈 Всего ключ. слов найдено: {self.stats.get('keywords_count', 0)}\n")
                else:
                    self.stdout.write("ℹ️ Игры не были обработаны (прервано до начала анализа)\n")
            else:
                processed = self.stats.get('processed', 0)
                if processed > 0:
                    self.stdout.write(f"🔄 Обработано игр: {processed}\n")
                    self.stdout.write(f"🎯 Игр с найденными критериями: {self.stats.get('found_count', 0)}\n")
                    self.stdout.write(f"📈 Всего критериев найдено: {self.stats.get('total_criteria_found', 0)}\n")
                else:
                    self.stdout.write("ℹ️ Игры не были обработаны (прервано до начала анализа)\n")

            self.stdout.write(f"⏭️ Игр без текста: {self.stats.get('skipped_no_text', 0)}\n")
            self.stdout.write(f"❌ Ошибок: {self.stats.get('errors', 0)}\n")
            self.stdout.write(f"💾 Обновлено игр: {self.stats.get('updated', 0)}\n")

            if 'execution_time' in self.stats and self.stats['execution_time'] > 0:
                self.stdout.write(f"⏱️ Время выполнения до прерывания: {self.stats['execution_time']:.1f} секунд\n")

            self.stdout.write("=" * 60 + "\n")
            self.stdout.write("ℹ️ Обработка была прервана пользователем (Ctrl+C)\n")
            self.stdout.write(
                "ℹ️ Для продолжения используйте: python manage.py analyze_game_criteria --force-restart\n")
            self.stdout.write("=" * 60 + "\n")

        except Exception as e:
            original_out = self.original_stdout or sys.stdout
            original_out.write(f"⚠️ Ошибка записи статистики в файл: {e}\n")

    def _display_interruption_stats_terminal(self, output_stream):
        """Отображает статистику прерывания в терминале"""
        if hasattr(self, 'stats') and self.stats:
            output_stream.write("📊 Частичная статистика (прервано):\n")

            # Рассчитываем общее количество пропущенных игр
            total_skipped = self.stats.get('skipped_no_text', 0) + self.stats.get('skipped_already_processed', 0)

            key_stats = ['processed', 'found_count', 'total_criteria_found', 'errors', 'updated']

            for key in key_stats:
                if key in self.stats:
                    display_name = self._format_stat_key(key)
                    output_stream.write(f"{display_name}: {self.stats[key]}\n")

            # Показываем пропущенные игры
            output_stream.write(f"⏭️ Всего пропущено игр: {total_skipped}\n")
            if self.stats.get('skipped_no_text', 0) > 0:
                output_stream.write(f"   ↳ без текста: {self.stats['skipped_no_text']}\n")
            if self.stats.get('skipped_already_processed', 0) > 0:
                output_stream.write(f"   ↳ ранее обработанных: {self.stats['skipped_already_processed']}\n")

            if 'execution_time' in self.stats and self.stats['execution_time'] > 0:
                output_stream.write(f"⏱️ Время выполнения до прерывания: {self.stats['execution_time']:.1f} секунд\n")

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

            results, pattern_info = self.analyzer.analyze_all_patterns(
                text_to_analyze,
                game=game,
                ignore_existing=self.ignore_existing,
                collect_patterns=self.verbose,
                keywords_mode=self.keywords
            )

            if self.keywords:
                results = {'keywords': results.get('keywords', [])}
                pattern_info = {'keywords': pattern_info.get('keywords', [])}

            criteria_count = sum(len(results[key]) for key in results)
            self._print_game_results(game, results, criteria_count, pattern_info)

            if self.update_game:
                if self.update_game_criteria([(game, results)]):
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

        results, pattern_info = self.analyzer.analyze_all_patterns(
            description,
            ignore_existing=True,
            collect_patterns=self.verbose,
            keywords_mode=self.keywords
        )

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
