import asyncio
import time
import sys
from typing import List, Dict
from django.core.management.base import BaseCommand

from .async_importer import AsyncWikiImporter
from .progress_bar import ProgressBar
from .failed_logger import FailedGamesLogger
from .async_helpers import save_batch_async, get_games_for_processing


class Command(BaseCommand):
    help = 'Асинхронный импорт описаний из Wikipedia'

    def add_arguments(self, parser):
        parser.add_argument(
            '--limit', type=int, default=0,
            help='Лимит на количество обрабатываемых игр (0 = без лимита)'
        )
        parser.add_argument(
            '--game-id', type=int, help='Обработать конкретную игру'
        )
        parser.add_argument(
            '--chunk-size', type=int, default=200, help='Размер батча'
        )
        parser.add_argument(
            '--max-concurrent', type=int, default=50,
            help='Максимальное количество одновременных соединений'
        )
        parser.add_argument(
            '--max-save-concurrent', type=int, default=1,
            help='Максимальное количество одновременных сохранений (по умолчанию: 1)'
        )
        parser.add_argument(
            '--skip-existing', action='store_true', default=True
        )
        parser.add_argument(
            '--only-empty', action='store_true'
        )
        parser.add_argument(
            '--force-update', action='store_true'
        )
        parser.add_argument(
            '--lang', default='en'
        )
        parser.add_argument(
            '--delay', type=float, default=0.3, help='Задержка между батчами'
        )
        parser.add_argument(
            '--stats', action='store_true'
        )
        parser.add_argument(
            '--skip-errors', action='store_true', default=False
        )
        parser.add_argument(
            '--no-progress', action='store_false', dest='progress', default=True
        )
        parser.add_argument(
            '--failed-file',
            default="failed_wiki_games.csv",  # Изменено с logs/failed_wiki_games.csv
            help='Файл для ошибок (по умолчанию: failed_wiki_games.csv)'
        )
        parser.add_argument(
            '--no-save-failed', action='store_true', default=False
        )
        parser.add_argument(
            '--skip-failed', action='store_true', default=False,
            help='Пропускать игры, которые уже в файле ошибок'
        )
        parser.add_argument(
            '--retry-failed', action='store_true', default=False,
            help='Обработать только игры из файла ошибок'
        )
        parser.add_argument(
            '--clear-failed', action='store_true', default=False,
            help='Очистить файл ошибок перед началом'
        )
        parser.add_argument(
            '--save-each-batch', action='store_true', default=True,
            help='Сохранять результаты после каждого батча (по умолчанию: True)'
        )
        # Новый аргумент
        parser.add_argument(
            '--include-not-found',
            action='store_true',
            default=False,
            help='Включить игры из файла ненайденных (по умолчанию: False)'
        )

    def handle(self, *args, **options):
        """Основной метод обработки команды"""
        # Запускаем асинхронный event loop
        asyncio.run(self.async_handle(*args, **options))

    async def async_handle(self, *args, **options):
        """Асинхронный обработчик команды"""
        self.show_header()

        # Определяем лимит
        limit = options.get('limit', 0)

        # Получаем статистику асинхронно
        stats = await self.get_statistics_async()
        self.show_statistics(stats, options, limit)

        # Если нужна только статистика
        if options.get('stats'):
            return

        # Подготовка игр для обработки с учетом лимита
        games_to_process = await self.prepare_games_async(options, limit)
        if not games_to_process:
            self.stdout.write(self.style.WARNING("\n⚠️  Нет игр для обработки"))
            return

        total = len(games_to_process)

        # Показываем настройки
        self.show_settings(options, limit, total)

        # Создаем прогресс-бар
        progress_bar = self.create_progress_bar(options, total)

        # Запуск импорта
        try:
            result = await self.run_async_import(games_to_process, options, progress_bar)
            await self.process_results_async(result, total, options, progress_bar, stats)

        except KeyboardInterrupt:
            self.handle_interruption(progress_bar)
            return
        except Exception as e:
            self.handle_error(e, progress_bar)
            return

    def show_header(self):
        """Показать заголовок команды"""
        print(f"\n{'=' * 70}")
        print(f"🚀 АСИНХРОННЫЙ ИМПОРТ WIKIPEDIA - {time.strftime('%H:%M:%S')}")
        print(f"{'=' * 70}")

    def show_statistics(self, stats, options, limit):
        """Показать статистику"""
        print(f"📊 Всего игр в базе: {stats['total_games']:,}")
        print(f"📝 Без описания Wikipedia: {stats['games_without_wiki']:,}")
        print(f"🚫 Без любого описания: {stats['games_without_any_desc']:,}")

        if limit > 0:
            print(f"🎯 Лимит обработки: {limit:,} игр")

    async def get_statistics_async(self):
        """Асинхронно получить статистику по играм"""
        # Используем асинхронные хелперы
        from .async_helpers import get_total_games_count, get_games_without_wiki_count, get_games_without_any_desc_count

        total_games = await get_total_games_count()
        games_without_wiki = await get_games_without_wiki_count()
        games_without_any_desc = await get_games_without_any_desc_count()

        return {
            'total_games': total_games,
            'games_without_wiki': games_without_wiki,
            'games_without_any_desc': games_without_any_desc
        }

    def show_settings(self, options, limit, total_games):
        """Показать настройки импорта"""
        settings_line = f"⚙️  Батч: {options.get('chunk_size', 200)} | "
        settings_line += f"Соединений: {options.get('max_concurrent', 50)} | "
        settings_line += f"Язык: {options.get('lang', 'en')} | "
        settings_line += f"Задержка: {options.get('delay', 0.3)}с"

        # Добавляем статус включения ненайденных
        if options.get('include_not_found', False):
            settings_line += " | ⚠️ Включены ненайденные"
        else:
            settings_line += " | ⏭️ Пропускаем ненайденные"

        if options.get('save_each_batch', True):
            settings_line += " | 💾 Сохранение в реальном времени"

        if limit > 0:
            settings_line += f" | Лимит: {limit:,}"

        print(settings_line)
        print(f"{'─' * 70}")
        print(f"🎮 Начинаем обработку {total_games:,} игр...")
        print(f"{'─' * 70}")

    def create_progress_bar(self, options, total):
        """Создать прогресс-бар"""
        if options.get('progress', True):
            return ProgressBar(
                total=total,
                prefix='📈',
                length=40,
                fill='█',
                empty_fill='░',
                show_percent=True,
                show_eta=True,
                show_speed=True,
                show_errors=True,
                show_chunk=True,
                show_saved=True  # Показывать счетчик сохраненных
            )
        return None

    async def run_async_import(self, games: List[Dict], options, progress_bar=None) -> Dict:
        """Запуск асинхронного импорта"""
        # Инициализируем логгер ошибок
        failed_logger = FailedGamesLogger(options.get('failed_file', "logs/failed_wiki_games.csv"))

        # Если это режим retry-failed, очищаем файл перед началом
        if options.get('retry_failed', False):
            print(f"🗑️  Очищаем файл ошибок перед повторной попыткой...")
            failed_logger.clear_failures()

        async with AsyncWikiImporter(
                lang=options.get('lang', 'en'),
                max_concurrent=options.get('max_concurrent', 50)
        ) as importer:

            results = {}
            chunk_size = options.get('chunk_size', 200)
            delay = options.get('delay', 0.3)
            total_chunks = (len(games) + chunk_size - 1) // chunk_size
            total_saved = 0
            total_failed = 0
            start_time = time.time()

            for chunk_num in range(total_chunks):
                start_idx = chunk_num * chunk_size
                end_idx = min(start_idx + chunk_size, len(games))
                chunk = games[start_idx:end_idx]

                if progress_bar:
                    progress_bar.set_chunk_info(f"Батч {chunk_num + 1}/{total_chunks}")

                # Обработка батча
                chunk_start = time.time()
                chunk_results = await importer.process_batch(chunk)
                chunk_time = time.time() - chunk_start

                # Добавляем успешные результаты
                results.update(chunk_results)

                # Сохраняем после каждого батча
                if chunk_results and options.get('save_each_batch', True):
                    saved_in_batch = await save_batch_async(chunk_results)
                    total_saved += saved_in_batch

                    # Обновляем счетчик сохраненных в прогресс-баре
                    if progress_bar:
                        progress_bar.increment_saved(saved_in_batch)

                # Логируем только те, что не удалось найти
                if not options.get('skip_errors', False) and not options.get('no_save_failed', False):
                    successful_ids = set(chunk_results.keys())
                    for game in chunk:
                        if game['id'] not in successful_ids:
                            failed_logger.add_failed_game(
                                game['id'],
                                game['name'],
                                "Не найдена страница Wikipedia"
                            )
                            total_failed += 1

                # Обновление прогресса
                if progress_bar:
                    processed = len(chunk_results)
                    failed = len(chunk) - processed
                    progress_bar.current += len(chunk)
                    progress_bar.errors += failed
                    progress_bar.speed = len(chunk) / chunk_time if chunk_time > 0 else 0
                    progress_bar.update()

                # Задержка между батчами
                if chunk_num + 1 < total_chunks:
                    await asyncio.sleep(delay)

            elapsed_time = time.time() - start_time

            # Возвращаем результаты
            return {
                'results': results,
                'total_saved': total_saved,
                'total_failed': total_failed,
                'elapsed_time': elapsed_time,
                'importer_stats': {
                    'api_calls': importer.api_calls,
                    'cache_hits': importer.cache_hits,
                    'failed_requests': importer.failed_requests
                }
            }

    async def process_results_async(self, result, total, options, progress_bar, initial_stats):
        """Обработать и показать результаты импорта"""
        # Завершаем прогресс-бар
        if progress_bar:
            progress_bar.complete()

        # Получаем данные из результата
        saved_count = result.get('total_saved', 0)
        failed_count = result.get('total_failed', 0)
        elapsed_time = result.get('elapsed_time', 0)

        # Текущая статистика
        from .async_helpers import get_games_without_wiki_count
        current_without_wiki = await get_games_without_wiki_count()

        # Показываем итоги
        self.show_results_summary(
            total, saved_count, failed_count, elapsed_time,
            initial_stats, current_without_wiki, options
        )

        # Показываем статистику импортера если есть
        if 'importer_stats' in result:
            self.show_importer_stats(result['importer_stats'])

    def show_results_summary(self, total, saved, failed, elapsed, initial_stats, current_without, options):
        """Показать сводку результатов"""
        print(f"\n{'=' * 70}")
        print(f"📊 РЕЗУЛЬТАТЫ ИМПОРТА")
        print(f"{'=' * 70}")

        # Основные метрики
        print(f"\n✅  Завершено: {time.strftime('%H:%M:%S')}")
        print(f"⏱️  Время выполнения: {elapsed:.1f}с")
        print()
        print(f"📈  Обработано игр:    {total:,}")
        print(f"✅  Получено описаний: {saved:,}")
        print(f"🚫  Не удалось получить: {failed:,}")

        if total > 0:
            success_rate = (saved / total) * 100
            print(f"📊  Процент успеха:   {success_rate:.1f}%")

        if elapsed > 0:
            speed = total / elapsed
            print(f"⚡  Скорость:         {speed:.1f} игр/с")

        # Статус базы данных
        print(f"\n📋  СТАТУС БАЗЫ ДАННЫХ")
        print(f"   📦  Было без описания:   {initial_stats['games_without_wiki']:,}")
        print(f"   📦  Осталось без описания: {current_without:,}")

        if initial_stats['total_games'] > 0:
            before_fill = ((initial_stats['total_games'] - initial_stats['games_without_wiki']) / initial_stats[
                'total_games']) * 100
            after_fill = ((initial_stats['total_games'] - current_without) / initial_stats['total_games']) * 100
            improvement = after_fill - before_fill

            print(f"   📊  Было заполнено:      {before_fill:.1f}%")
            print(f"   📊  Стало заполнено:     {after_fill:.1f}%")
            print(f"   📈  Улучшение:           +{improvement:.1f}%")

        # Информация об ошибках
        if failed > 0:
            print(f"\n⚠️  Не удалось получить описания для {failed:,} игр")
            failed_file = options.get('failed_file', "logs/failed_wiki_games.csv")
            print(f"   📄  Проверьте файл: {failed_file}")

        print(f"\n{'=' * 70}")

    def show_importer_stats(self, stats):
        """Показать статистику импортера"""
        print(f"\n📡 СТАТИСТИКА ИМПОРТЕРА")
        print(f"   📞  API вызовов:       {stats.get('api_calls', 0):,}")
        print(f"   💾  Кэш попаданий:     {stats.get('cache_hits', 0):,}")
        print(f"   ❌  Ошибок запросов:   {stats.get('failed_requests', 0):,}")

        if stats.get('cache_hits', 0) > 0:
            cache_rate = (stats['cache_hits'] / (stats['cache_hits'] + stats['api_calls'])) * 100
            print(f"   📊  Эффективность кэша: {cache_rate:.1f}%")

    def handle_interruption(self, progress_bar):
        """Обработать прерывание пользователем"""
        if progress_bar:
            progress_bar.complete()
        print(f"\n\n{'=' * 70}")
        print(f"⚠️  ИМПОРТ ПРЕРВАН ПОЛЬЗОВАТЕЛЕМ")
        print(f"{'=' * 70}")

    def handle_error(self, error, progress_bar):
        """Обработать ошибку"""
        if progress_bar:
            progress_bar.complete()
        print(f"\n\n{'=' * 70}")
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {error}")
        print(f"{'=' * 70}")

    async def prepare_games_async(self, options, limit: int = 0) -> List[Dict]:
        """Асинхронно подготовить список игр для обработки с учетом лимита"""
        # Инициализируем логгер ошибок
        failed_logger = FailedGamesLogger(options.get('failed_file', "logs/failed_wiki_games.csv"))

        # Определяем флаг включения ненайденных игр
        include_not_found = options.get('include_not_found', False)

        # Загружаем ID из файла ненайденных игр
        not_found_ids = set()
        if not include_not_found:  # Только если НЕ включаем ненайденные
            not_found_ids = failed_logger.get_failed_ids_from_file()
            if not_found_ids:
                print(f"⏭️  Пропускаем {len(not_found_ids):,} игр из файла ненайденных")
        else:
            print(f"⚠️  Включены игры из файла ненайденных (--include-not-found)")

        # Очищаем файл ошибок если нужно
        if options.get('clear_failed', False):
            failed_logger.clear_failures()
            print(f"🗑️  Файл ошибок очищен: {failed_logger.get_filename()}")

        # Режим обработки только неудачных игр
        if options.get('retry_failed', False):
            games_for_retry = failed_logger.load_failed_games_for_retry()
            if not games_for_retry:
                print(f"ℹ️  В файле ошибок нет игр для повторной обработки")
                return []

            # Применяем лимит
            if limit > 0:
                games_for_retry = games_for_retry[:limit]

            total_to_retry = len(games_for_retry)
            if total_to_retry > 0:
                print(f"🔄 Повторная обработка {total_to_retry:,} игр из файла ошибок")
                if len(games_for_retry) < len(failed_logger.get_failed_games()):
                    print(f"   (лимит: {limit:,} из {len(failed_logger.get_failed_games()):,})")

                # Показываем примеры
                print(f"📋 Примеры для повторной обработки:")
                for i, game in enumerate(games_for_retry[:3], 1):
                    print(f"   {i}. {game['name']} (ID: {game['id']})")
                if len(games_for_retry) > 3:
                    print(f"   ... и ещё {len(games_for_retry) - 3} игр")

            return games_for_retry

        # Обработка одной игры
        if options.get('game_id'):
            try:
                # Используем асинхронный хелпер
                from .async_helpers import get_game_by_id_async
                game = await get_game_by_id_async(options['game_id'])

                if not game:
                    print(f"\n❌ Игра с ID {options['game_id']} не найдена")
                    return []

                # Проверяем, не в списке ли ненайденных игр
                if not include_not_found and game['id'] in not_found_ids:
                    print(f"⏭️  Игра в файле ненайденных, пропускаем: {game['name']}")
                    print(f"   Используйте --include-not-found чтобы включить её")
                    return []

                # Проверяем, не в списке ли ошибок (если включен skip-failed)
                if options.get('skip_failed', False) and failed_logger.is_failed(game['id']):
                    print(f"⏭️  Игра уже в списке ошибок, пропускаем: {game['name']}")
                    return []

                print(f"\n🎯 Обработка одной игры: {game['name']}")
                return [{'id': game['id'], 'name': game['name']}]
            except Exception as e:
                print(f"\n❌ Ошибка при получении игры: {e}")
                return []

        # Массовая обработка - используем асинхронный хелпер
        games = await get_games_for_processing(
            skip_existing=options.get('skip_existing', True) and not options.get('force_update'),
            only_empty=options.get('only_empty', False),
            skip_failed=options.get('skip_failed', False),
            failed_ids=list(failed_logger.get_failed_ids()) if options.get('skip_failed', False) else [],
            limit=limit,
            include_not_found=include_not_found,  # Передаем флаг
            not_found_ids=not_found_ids  # Передаем ID ненайденных
        )

        if games:
            print(f"\n📋 Всего для обработки: {len(games):,} игр")

            if limit > 0 and len(games) == limit:
                print(f"   (достигнут лимит {limit:,} игр)")

            if not include_not_found and not_found_ids:
                print(f"   ⏭️  Пропущено из файла ненайденных: {len(not_found_ids):,} игр")
            elif include_not_found:
                print(f"   ⚠️  Включены игры из файла ненайденных")

            if len(games) > 0:
                print(f"   📝 Примеры:")
                for i, game in enumerate(games[:3], 1):
                    print(f"   {i}. {game['name']}")
                if len(games) > 3:
                    print(f"   ... и ещё {len(games) - 3} игр")

        elif not games and not_found_ids and not include_not_found:
            # Специальное сообщение, если все игры в списке ненайденных
            print(f"\n⚠️  Все подходящие игры находятся в файле ненайденных")
            print(f"   Используйте --include-not-found чтобы обработать их")
            print(f"   Или --retry-failed чтобы обработать только ненайденные")

        return games
