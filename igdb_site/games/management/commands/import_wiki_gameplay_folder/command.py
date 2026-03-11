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
        """Аргументы теперь определены в основном файле команды"""
        # Оставляем пустым или с комментарием
        pass

    def handle(self, *args, **options):
        """Основной метод обработки команды"""
        # Запускаем асинхронный event loop
        asyncio.run(self.async_handle(*args, **options))

    def show_interruption_tips(self, options):
        """Подсказки теперь в show_results_summary"""
        pass

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
        settings_line += f"Язык: {options.get('lang', 'en')}"

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
        debug_mode = options.get('debug', False)

        # Показываем легенду значков перед прогресс-баром (только если не debug режим)
        if not debug_mode and progress_bar:
            print(f"\n📊 Легенда значков:")
            print(f"   📈 - прогресс обработки")
            print(f"   💾 - сохранено описаний в БД")
            print(f"   ⚪ - не найдено в Wikipedia")
            print(f"   ❌ - ошибки запросов")
            print(f"   ⚡ - скорость обработки (игр/сек)")
            print(f"   ⏱️  - оставшееся время (ETA)")
            print(f"   ↻ - текущий батч")
            print()

        # Если это режим retry-failed, очищаем файл перед началом
        if options.get('retry_failed', False):
            if debug_mode:
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
            total_not_found = 0
            total_errors = 0
            start_time = time.time()
            interrupted = False

            try:
                for chunk_num in range(total_chunks):
                    # Проверяем прерывание в начале каждой итерации
                    if interrupted:
                        break

                    start_idx = chunk_num * chunk_size
                    end_idx = min(start_idx + chunk_size, len(games))
                    chunk = games[start_idx:end_idx]

                    if progress_bar:
                        progress_bar.set_chunk_info(f"Батч {chunk_num + 1}/{total_chunks}")

                    # Обработка батча
                    chunk_start = time.time()
                    try:
                        chunk_results = await importer.process_batch(chunk)
                    except asyncio.CancelledError:
                        interrupted = True
                        break
                    except Exception as e:
                        if debug_mode:
                            print(f"\n❌ Ошибка обработки батча: {e}")
                        total_errors += len(chunk)
                        if progress_bar:
                            progress_bar.errors += len(chunk)
                            progress_bar.current += len(chunk)
                            progress_bar.update()
                        continue

                    chunk_time = time.time() - chunk_start

                    # Добавляем успешные результаты
                    results.update(chunk_results)

                    # Сохраняем после каждого батча
                    if chunk_results and options.get('save_each_batch', True):
                        try:
                            saved_in_batch = await save_batch_async(chunk_results)
                            total_saved += saved_in_batch

                            # Обновляем счетчик сохраненных в прогресс-баре
                            if progress_bar:
                                progress_bar.increment_saved(saved_in_batch)
                        except Exception as e:
                            if debug_mode:
                                print(f"\n❌ Ошибка сохранения батча: {e}")
                            total_errors += len(chunk_results)

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
                                total_not_found += 1

                    # Обновление прогресса
                    if progress_bar:
                        processed = len(chunk_results)
                        not_found = len(chunk) - processed
                        progress_bar.current += len(chunk)
                        progress_bar.not_found += not_found
                        progress_bar.speed = len(chunk) / chunk_time if chunk_time > 0 else 0
                        progress_bar.update()

                    # Задержка между батчами
                    if chunk_num + 1 < total_chunks and not interrupted:
                        try:
                            await asyncio.sleep(delay)
                        except asyncio.CancelledError:
                            interrupted = True
                            break

            except KeyboardInterrupt:
                interrupted = True
                # Даем время на завершение текущих задач
                await asyncio.sleep(0.5)

            elapsed_time = time.time() - start_time

            # Возвращаем результаты
            return {
                'results': results,
                'total_saved': total_saved,
                'total_not_found': total_not_found,
                'total_errors': total_errors,
                'elapsed_time': elapsed_time,
                'interrupted': interrupted,
                'games_processed': progress_bar.current if progress_bar else total_saved + total_not_found + total_errors,
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
        not_found_count = result.get('total_not_found', 0)
        errors_count = result.get('total_errors', 0)
        elapsed_time = result.get('elapsed_time', 0)
        games_processed = result.get('games_processed', saved_count + not_found_count + errors_count)
        was_interrupted = result.get('interrupted', False)

        # Текущая статистика
        from .async_helpers import get_games_without_wiki_count
        current_without_wiki = await get_games_without_wiki_count()

        # Показываем итоги
        self.show_results_summary(
            games_processed, saved_count, not_found_count, errors_count, elapsed_time,
            initial_stats, current_without_wiki, options,
            was_interrupted=was_interrupted
        )

        # Показываем статистику импортера если есть (только в debug режиме)
        if options.get('debug', False) and 'importer_stats' in result:
            self.show_importer_stats(result['importer_stats'])

    def show_results_summary(self, processed, saved, not_found, errors, elapsed, initial_stats, current_without,
                             options, was_interrupted=False):
        """Показать сводку результатов"""
        print(f"\n{'=' * 70}")
        if was_interrupted:
            print(f"📊 ИМПОРТ ПРЕРВАН - ПРОМЕЖУТОЧНЫЕ РЕЗУЛЬТАТЫ")
        else:
            print(f"📊 РЕЗУЛЬТАТЫ ИМПОРТА")
        print(f"{'=' * 70}")

        # Основные метрики
        print(f"\n✅  Завершено: {time.strftime('%H:%M:%S')}")
        print(f"⏱️  Время выполнения: {elapsed:.1f}с")
        print()
        print(f"📈  Обработано игр:    {processed:,}")
        print(f"✅  Получено описаний: {saved:,}")
        print(f"⚪  Не найдено в Wikipedia: {not_found:,}")
        print(f"❌  Ошибок запросов:   {errors:,}")

        if processed > 0:
            success_rate = (saved / processed) * 100
            not_found_rate = (not_found / processed) * 100
            error_rate = (errors / processed) * 100
            print(f"\n📊  Процент успеха:     {success_rate:.1f}%")
            print(f"📊  Процент не найдено: {not_found_rate:.1f}%")
            print(f"📊  Процент ошибок:     {error_rate:.1f}%")

        if elapsed > 0:
            speed = processed / elapsed
            print(f"\n⚡  Средняя скорость:   {speed:.1f} игр/с")

        # Статус базы данных
        print(f"\n📋  СТАТУС БАЗЫ ДАННЫХ")
        print(f"   📦  Было без описания:   {initial_stats['games_without_wiki']:,}")
        print(f"   📦  Осталось без описания: {current_without:,}")

        if initial_stats['total_games'] > 0 and not was_interrupted:
            before_fill = ((initial_stats['total_games'] - initial_stats['games_without_wiki']) / initial_stats[
                'total_games']) * 100
            after_fill = ((initial_stats['total_games'] - current_without) / initial_stats['total_games']) * 100
            improvement = after_fill - before_fill

            print(f"   📊  Было заполнено:      {before_fill:.1f}%")
            print(f"   📊  Стало заполнено:     {after_fill:.1f}%")
            print(f"   📈  Улучшение:           +{improvement:.1f}%")

        # Информация об ошибках
        if not_found > 0 or errors > 0:
            print(f"\n⚠️  Проблемные игры:")
            if not_found > 0:
                print(f"   ⚪ Не найдено: {not_found:,} игр")
            if errors > 0:
                print(f"   ❌ Ошибки: {errors:,} игр")

            failed_file = options.get('failed_file', "logs/failed_wiki_games.csv")
            print(f"   📄  Список сохранен в: {failed_file}")

        # Если было прерывание, показываем простую подсказку
        if was_interrupted:
            print(f"\n💡 Для продолжения используйте: python manage.py import_wiki_gameplay --skip-failed")

        print(f"\n{'=' * 70}")

    def show_importer_stats(self, stats):
        """Показать статистику импортера"""
        # Эта статистика будет показываться только при --debug
        print(f"\n📡 СТАТИСТИКА ИМПОРТЕРА")
        print(f"   📞  API вызовов:       {stats.get('api_calls', 0):,}")
        print(f"   💾  Кэш попаданий:     {stats.get('cache_hits', 0):,}")
        print(f"   ❌  Ошибок запросов:   {stats.get('failed_requests', 0):,}")

        if stats.get('cache_hits', 0) > 0:
            cache_rate = (stats['cache_hits'] / (stats['cache_hits'] + stats['api_calls'])) * 100
            print(f"   📊  Эффективность кэша: {cache_rate:.1f}%")

    def handle_interruption(self, progress_bar):
        """Обработать прерывание пользователем - ничего не делаем, все в run_async_import"""
        pass

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
        debug_mode = options.get('debug', False)

        # Определяем флаг включения ненайденных игр
        include_not_found = options.get('include_not_found', False)

        # Загружаем ID из файла ненайденных игр
        not_found_ids = set()
        if not include_not_found:
            not_found_ids = failed_logger.get_failed_ids_from_file()
            if not_found_ids and debug_mode:
                print(f"⏭️  Пропускаем {len(not_found_ids):,} игр из файла ненайденных")
        elif debug_mode:
            print(f"⚠️  Включены игры из файла ненайденных (--include-not-found)")

        # Очищаем файл ошибок если нужно
        if options.get('clear_failed', False):
            failed_logger.clear_failures()
            if debug_mode:
                print(f"🗑️  Файл ошибок очищен: {failed_logger.get_filename()}")

        # Режим обработки только неудачных игр
        if options.get('retry_failed', False):
            games_for_retry = failed_logger.load_failed_games_for_retry()
            if not games_for_retry:
                print(f"ℹ️  В файле ошибок нет игр для повторной обработки")
                return []

            if limit > 0:
                games_for_retry = games_for_retry[:limit]

            total_to_retry = len(games_for_retry)
            if total_to_retry > 0:
                print(f"🔄 Повторная обработка {total_to_retry:,} игр из файла ошибок")

                if debug_mode and len(games_for_retry) < len(failed_logger.get_failed_games()):
                    print(f"   (лимит: {limit:,} из {len(failed_logger.get_failed_games()):,})")

            return games_for_retry

        # Обработка одной игры
        if options.get('game_id'):
            try:
                from .async_helpers import get_game_by_id_async
                game = await get_game_by_id_async(options['game_id'])

                if not game:
                    print(f"\n❌ Игра с ID {options['game_id']} не найдена")
                    return []

                if not include_not_found and game['id'] in not_found_ids:
                    if debug_mode:
                        print(f"⏭️  Игра в файле ненайденных, пропускаем: {game['name']}")
                    else:
                        print(f"\n❌ Игра в списке ненайденных, используйте --include-not-found")
                    return []

                if options.get('skip_failed', False) and failed_logger.is_failed(game['id']):
                    if debug_mode:
                        print(f"⏭️  Игра уже в списке ошибок, пропускаем: {game['name']}")
                    else:
                        print(f"\n❌ Игра в списке ошибок")
                    return []

                print(f"\n🎯 Обработка одной игры: {game['name']}")
                return [{'id': game['id'], 'name': game['name']}]
            except Exception as e:
                print(f"\n❌ Ошибка при получении игры: {e}")
                return []

        # Массовая обработка
        games = await get_games_for_processing(
            skip_existing=options.get('skip_existing', True) and not options.get('force_update'),
            only_empty=options.get('only_empty', False),
            skip_failed=options.get('skip_failed', False),
            failed_ids=list(failed_logger.get_failed_ids()) if options.get('skip_failed', False) else [],
            limit=limit,
            include_not_found=include_not_found,
            not_found_ids=not_found_ids
        )

        if games:
            print(f"\n📋 Всего для обработки: {len(games):,} игр")

            if limit > 0 and len(games) == limit:
                print(f"   (достигнут лимит {limit:,} игр)")

            if debug_mode and not include_not_found and not_found_ids:
                print(f"   ⏭️  Пропущено из файла ненайденных: {len(not_found_ids):,} игр")
            elif debug_mode and include_not_found:
                print(f"   ⚠️  Включены игры из файла ненайденных")

            if debug_mode and len(games) > 0:
                print(f"   📝 Примеры:")
                for i, game in enumerate(games[:3], 1):
                    print(f"   {i}. {game['name']}")
                if len(games) > 3:
                    print(f"   ... и ещё {len(games) - 3} игр")

        elif not games and not_found_ids and not include_not_found and debug_mode:
            print(f"\n⚠️  Все подходящие игры находятся в файле ненайденных")
            print(f"   Используйте --include-not-found чтобы обработать их")

        return games
