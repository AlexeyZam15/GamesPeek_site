"""
Команда Django для создания кэшированных игровых карточек.
"""

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction, models
from django.utils import timezone
from django.template.loader import render_to_string
from django.db.models import Prefetch
from typing import List, Dict, Any, Tuple
import time
import logging
import math
import os
import json
import sys

from games.models import Game, GameCardCache, Genre, Platform, PlayerPerspective, Keyword, Theme, GameMode
from games.models_parts.enums import GameTypeEnum

logger = logging.getLogger(__name__)


class ProgressBar:
    """Простой прогресс-бар для отображения хода выполнения с статистикой"""

    def __init__(self, total: int, desc: str = "Обработка", bar_length: int = 40):
        self.total = total
        self.desc = desc
        self.bar_length = bar_length
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = time.time()
        self.update_interval = 0.1  # Минимальный интервал между обновлениями в секундах

        # Статистика
        self.saved_games = 0
        self.created_cards = 0
        self.updated_cards = 0  # Новый счетчик для обновленных карточек
        self.skipped_games = 0
        self.error_games = 0

    def update(self, n: int = 1):
        """Обновить прогресс"""
        self.current += n
        current_time = time.time()

        # Обновляем отображение только если прошел достаточный интервал
        if current_time - self.last_update_time > self.update_interval or self.current == self.total:
            self._display()
            self.last_update_time = current_time

    def update_stats(self, saved: int = 0, created: int = 0, updated: int = 0, skipped: int = 0, errors: int = 0):
        """Обновить статистику"""
        self.saved_games += saved
        self.created_cards += created
        self.updated_cards += updated
        self.skipped_games += skipped
        self.error_games += errors

    def _display(self):
        """Отобразить прогресс-бар со статистикой"""
        if self.total == 0:
            percentage = 100
        else:
            percentage = min(100, (self.current / self.total) * 100)

        # Рассчитываем заполненную часть
        filled_length = int(self.bar_length * self.current // self.total)
        bar = '█' * filled_length + '░' * (self.bar_length - filled_length)

        # Рассчитываем время
        elapsed_time = time.time() - self.start_time
        if self.current > 0 and self.current < self.total:
            remaining_time = (elapsed_time / self.current) * (self.total - self.current)
            time_str = f"{elapsed_time:.0f}s < {remaining_time:.0f}s"
        else:
            time_str = f"{elapsed_time:.0f}s"

        # Форматируем статистику с пробелами после иконок
        stats_str = f"💾 {self.saved_games} 🆕 {self.created_cards} 🔄 {self.updated_cards} ⏭️ {self.skipped_games} ❌ {self.error_games}"

        # Форматируем сообщение
        message = f"\r{self.desc}: {percentage:3.0f}% [{self.current}/{self.total}] [{bar}] {stats_str} ({time_str})"

        sys.stderr.write(message)
        sys.stderr.flush()

    def finish(self):
        """Завершить прогресс-бар"""
        self._display()
        sys.stderr.write("\n")
        sys.stderr.flush()


class Command(BaseCommand):
    """
    Создание кэшированных игровых карточек для улучшения производительности.

    Возможности:
    - Создает предварительно отрендеренные HTML-карточки для игр
    - Поддерживает различные конфигурации карточек (с/без показа сходства)
    - Пакетная обработка для производительности
    - Отслеживание прогресса и возможность возобновления
    """

    help = 'Создание кэшированных игровых карточек для улучшения производительности'

    def add_arguments(self, parser):
        """Определение аргументов команды."""
        parser.add_argument(
            '--game-ids',
            type=str,
            help='Список ID игр для обработки, разделенных запятыми'
        )

        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Пропустить первые N игр (для пакетной обработки)'
        )

        parser.add_argument(
            '--batch-size',
            type=int,
            default=100,
            help='Количество игр для обработки в каждом пакете (по умолчанию: 100)'
        )

        parser.add_argument(
            '--limit',
            type=int,
            help='Максимальное количество игр для обработки'
        )

        parser.add_argument(
            '--types',
            type=str,
            default='all',
            choices=['all', 'primary', 'main', 'dlc', 'expansions'],
            help='Типы игр для обработки (по умолчанию: primary)'
        )

        parser.add_argument(
            '--config',
            type=str,
            default='normal',
            choices=['normal', 'all', 'similarity'],
            help='Конфигурации карточек для создания (по умолчанию: normal)'
        )

        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительное пересоздание существующих карточек'
        )

        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать, что будет создано, без фактического создания'
        )

        parser.add_argument(
            '--resume',
            action='store_true',
            help='Продолжить с последней обработанной игры'
        )

        parser.add_argument(
            '--skip-existing',
            action='store_true',
            help='Пропустить игры, у которых уже есть кэшированные карточки'
        )

    def handle(self, *args, **options):
        """Основной обработчик команды - обрабатываем ВСЕ игры без фильтров."""
        start_time = time.time()
        progress_bar = None

        # Сохраняем текущие настройки для сообщений о продолжении
        self.current_types = options.get('types', 'primary')
        self.current_config = options.get('config', 'normal')

        try:
            # Настройка логирования
            verbosity = options.get('verbosity', 1)
            if verbosity >= 2:
                logging.getLogger().setLevel(logging.DEBUG)

            # Разбор ID игр, если предоставлены
            game_ids = self._parse_game_ids(options.get('game_ids'))

            # Получаем оффсет из аргументов
            offset = options.get('offset', 0)

            # Если указан --force, всегда сбрасываем offset на 0
            force = options.get('force', False)
            if force:
                offset = 0
                self.stdout.write(self.style.WARNING("🔥 ФОРСИРОВАННЫЙ РЕЖИМ: оффсет сброшен на 0"))
                # Сохраняем сброшенный offset
                self._save_offset_to_file(offset)

            # Определяем, был ли оффсет явно указан пользователем
            import sys
            offset_specified = any(arg.startswith('--offset') for arg in sys.argv)

            # Если пользователь явно указал оффсет (любой, включая 0) - сохраняем его
            if offset_specified:
                if not force:  # Если не force режим
                    self._save_offset_to_file(offset)
                    self.stdout.write(f"📍 ИСПОЛЬЗУЕТСЯ УКАЗАННЫЙ ОФФСЕТ: {offset}")
                else:
                    self.stdout.write(f"📍 ФОРС-РЕЖИМ: игнорируем указанный оффсет {offset}")
            # Если пользователь НЕ указал оффсет и не force режим - загружаем из файла
            elif not force and not options.get('resume') and not game_ids:
                loaded_offset = self._load_offset_from_file()
                if loaded_offset > 0:
                    offset = loaded_offset
                    self.stdout.write(f"📍 ЗАГРУЖЕН ОФФСЕТ ИЗ ПРЕДЫДУЩЕГО ЗАПУСКА: {offset}")
                else:
                    self.stdout.write(f"📍 НАЧИНАЕМ С НАЧАЛА (оффсет: 0)")
            else:
                self.stdout.write(f"📍 ИСПОЛЬЗУЕТСЯ ОФФСЕТ: {offset}")

            # Очищаем дубликаты перед началом обработки
            if not options.get('dry_run') and verbosity >= 1:
                self.stdout.write("🧹 Проверка и очистка дубликатов...")
                deleted_duplicates = self._clean_duplicates_before_processing(game_ids, verbosity)
                if deleted_duplicates > 0:
                    self.stdout.write(f"  ✅ Удалено {deleted_duplicates} дубликатов")

            # Если force режим, удаляем все существующие карточки
            if force and not options.get('dry_run'):
                deleted_count = self._clear_existing_cards(verbosity)
                if deleted_count > 0:
                    self.stdout.write(
                        self.style.SUCCESS(f"🔥 Удалено {deleted_count} карточек для полного пересоздания"))

            # Получение ВСЕХ игр для обработки
            games = self._get_games_to_process(
                game_ids=game_ids,
                game_types=options.get('types'),
                limit=options.get('limit'),
                resume=options.get('resume'),
                offset=offset
            )

            total_games = games.count()

            if total_games == 0:
                # Проверяем, может быть из-за большого offset
                total_all_games = Game.objects.count()
                if offset >= total_all_games:
                    self.stdout.write(self.style.WARNING(
                        f"⚠️ Offset {offset} больше общего количества игр ({total_all_games})"
                    ))
                    self.stdout.write(self.style.WARNING("🔄 Сбрасываем offset на 0..."))
                    offset = 0
                    self._save_offset_to_file(0)
                    # Пробуем снова с offset 0
                    games = self._get_games_to_process(
                        game_ids=game_ids,
                        game_types=options.get('types'),
                        limit=options.get('limit'),
                        resume=False,
                        offset=0
                    )
                    total_games = games.count()

                    if total_games == 0:
                        self.stdout.write(self.style.WARNING("❌ Не найдено игр для обработки даже с offset 0."))
                        return
                    else:
                        self.stdout.write(self.style.SUCCESS(f"✅ Найдено {total_games} игр с offset 0"))
                else:
                    self.stdout.write(self.style.WARNING("❌ Не найдено игр для обработки."))
                    return

            self.stdout.write(self.style.SUCCESS(f"✅ Найдено {total_games} игр для обработки"))

            if offset > 0:
                self.stdout.write(f"📍 Начинаем с оффсета: {offset}")

            # Показываем общую статистику на русском
            total_all_games = Game.objects.count()
            if total_all_games > total_games:
                self.stdout.write(f"📊 Всего игр в базе данных: {total_all_games}")
                if offset > 0:
                    processed_before = offset
                    remaining = total_all_games - (offset + total_games)
                    self.stdout.write(
                        f"📈 Прогресс: {processed_before} обработано ранее, {total_games} для обработки сейчас, {remaining} осталось")

            # Определение конфигураций для создания
            configs = self._get_card_configurations(options.get('config'))

            self.stdout.write(f"⚙️ Создание карточек с конфигурациями: {', '.join(configs)}")

            # Обработка игр пакетами
            batch_size = options.get('batch_size')
            total_batches = math.ceil(total_games / batch_size)

            total_created = 0
            total_updated = 0
            total_skipped = 0
            total_errors = 0

            # Счетчик ОБРАБОТАННЫХ игр (созданные + обновленные + пропущенные + ошибки)
            processed_games = 0

            # Флаг для отображения сохранения в прогресс-баре
            last_saved_checkpoint = 0

            # Инициализируем прогресс-бар
            if not options.get('dry_run') and total_games > 1:
                progress_bar = ProgressBar(
                    total=total_games,
                    desc="Создание игровых карточек",
                    bar_length=40
                )
            else:
                progress_bar = None

            for batch_num in range(total_batches):
                batch_start = batch_num * batch_size
                batch_end = batch_start + batch_size

                batch_games = games[batch_start:batch_end]
                batch_game_ids = [game.id for game in batch_games]

                if verbosity >= 1 and not progress_bar:
                    self.stdout.write(
                        f"🔄 Обработка пакета {batch_num + 1}/{total_batches} "
                        f"(игры {batch_start + 1}-{min(batch_end, total_games)})"
                    )

                # Загрузка игр с предзагруженными данными
                games_with_data = self._load_games_with_data(batch_game_ids)

                batch_created, batch_skipped, batch_errors, batch_saved = self._process_batch(
                    games_with_data,
                    configs,
                    options.get('force'),
                    options.get('skip_existing'),
                    options.get('dry_run'),
                    verbosity,
                    progress_bar
                )

                total_created += batch_created
                total_skipped += batch_skipped
                total_errors += batch_errors

                # ВАЖНО: Увеличиваем счетчик на ВСЕ игры в батче (сохраненные + пропущенные + с ошибками)
                # batch_saved уже включает созданные и обновленные
                batch_processed = batch_saved + batch_skipped + batch_errors
                processed_games += batch_processed

                # Обновляем статистику в прогресс-баре
                if progress_bar:
                    # Обновляем прогресс на количество обработанных игр
                    progress_bar.update(len(batch_games))
                    progress_bar.update_stats(
                        saved=batch_saved,
                        created=batch_created,
                        skipped=batch_skipped,
                        errors=batch_errors
                    )

                # Промежуточное сохранение оффсета каждые 1000 ОБРАБОТАННЫХ игр
                if not options.get('dry_run') and processed_games - last_saved_checkpoint >= 1000:
                    current_offset = offset + processed_games
                    self._save_offset_to_file(current_offset)
                    last_saved_checkpoint = processed_games

                    if verbosity >= 2:
                        self.stdout.write(
                            f"💾 Сохранен оффсет: {current_offset} (обработано игр: {processed_games})"
                        )

                # Показываем прогресс
                # Показываем прогресс ТОЛЬКО при высокой детализации
                if not options.get('dry_run') and verbosity >= 2 and not progress_bar:
                    self.stdout.write(
                        f"  Создано: {batch_created}, "
                        f"Пропущено: {batch_skipped}, "
                        f"Ошибок: {batch_errors}, "
                        f"Сохранено: {batch_saved}"
                    )

            # Завершаем прогресс-бар
            if progress_bar:
                progress_bar.finish()

            # Сохраняем финальный оффсет для следующего запуска
            # ВАЖНО: оффсет увеличивается на ВСЕ обработанные игры
            if not options.get('dry_run') and not game_ids and not options.get('resume'):
                next_offset = offset + processed_games
                self._save_offset_to_file(next_offset)

                self.stdout.write(
                    f"💾 Финальный оффсет сохранен: {next_offset} "
                    f"(обработано игр: {processed_games} = сохранено: {batch_saved} + пропущено: {total_skipped} + ошибок: {total_errors})"
                )

            # Финальная статистика на русском
            elapsed_time = time.time() - start_time
            self.stdout.write("\n" + "=" * 60)
            self.stdout.write(self.style.SUCCESS("✅ СОЗДАНИЕ ИГРОВЫХ КАРТОЧЕК ЗАВЕРШЕНО"))
            self.stdout.write("=" * 60)
            self.stdout.write(f"🔄 Всего игр в батче: {total_games}")
            self.stdout.write(
                f"📊 Обработано игр: {processed_games} (создано: {total_created} + пропущено: {total_skipped} + ошибок: {total_errors})")
            self.stdout.write(f"🎯 Создано карточек: {total_created}")
            self.stdout.write(f"⏭️ Пропущено игр: {total_skipped}")
            self.stdout.write(f"❌ Ошибок: {total_errors}")

            if total_created > 0:
                efficiency = (total_created / processed_games) * 100
                self.stdout.write(f"📈 Эффективность: {efficiency:.1f}%")

            self.stdout.write(f"⏱️ Время выполнения: {elapsed_time:.2f} секунд")
            self.stdout.write("=" * 60)

            if not options.get('dry_run'):
                # Обновление статистики кэша
                self._update_cache_stats()

        except KeyboardInterrupt:
            self._handle_interrupt(offset, processed_games, progress_bar)
        except Exception as e:
            self._handle_error(e, progress_bar)

    def _clear_existing_cards(self, verbosity: int) -> int:
        """Полная очистка всех существующих карточек."""
        try:
            if verbosity >= 1:
                self.stdout.write("🗑️ Начинаем полную очистку существующих карточек...")

            # Получаем количество до удаления
            count_before = GameCardCache.objects.count()

            # Удаляем все записи
            deleted_info = GameCardCache.objects.all().delete()
            deleted_count = deleted_info[0] if deleted_info else 0

            if verbosity >= 1:
                self.stdout.write(f"  ✅ Удалено {deleted_count} карточек (было: {count_before})")

            # Также чистим файл offset
            self._save_offset_to_file(0)

            if verbosity >= 1:
                self.stdout.write("  ✅ Файл offset сброшен на 0")

            return deleted_count

        except Exception as e:
            logger.error(f"Ошибка при очистке карточек: {str(e)}")
            if verbosity >= 1:
                self.stdout.write(f"  ❌ Ошибка при очистке: {str(e)}")
            return 0

    def _clean_duplicates_before_processing(self, game_ids: List[int], verbosity: int) -> int:
        """
        Очистка дубликатов перед началом обработки.

        Args:
            game_ids: Список ID игр для обработки
            verbosity: Уровень детализации

        Returns:
            Количество удаленных дубликатов
        """
        try:
            # Находим дубликаты cache_key среди обрабатываемых игр
            from django.db.models import Count

            # Сначала находим все дубликаты cache_key в БД
            all_duplicates = (
                GameCardCache.objects
                .values('cache_key')
                .annotate(count=Count('id'))
                .filter(count__gt=1)
            )

            deleted_count = 0

            for dup in all_duplicates:
                cache_key = dup['cache_key']
                count = dup['count']

                if count > 1:
                    # Получаем все записи с этим ключом
                    cards = GameCardCache.objects.filter(cache_key=cache_key).order_by('-updated_at', '-created_at')

                    # Оставляем самую свежую активную запись
                    keep_card = None
                    for card in cards:
                        if card.is_active:
                            keep_card = card
                            break

                    if not keep_card:
                        keep_card = cards[0]  # Берем самую свежую

                    # Удаляем остальные
                    cards_to_delete = cards.exclude(id=keep_card.id)
                    delete_count = cards_to_delete.count()

                    if delete_count > 0:
                        cards_to_delete.delete()
                        deleted_count += delete_count

                        if verbosity >= 2:
                            self.stdout.write(
                                f"  🗑️ Удалено {delete_count} дубликатов для cache_key: {cache_key[:20]}..."
                            )

            if deleted_count > 0 and verbosity >= 1:
                self.stdout.write(f"  ✅ Удалено {deleted_count} дубликатов перед началом обработки")

            return deleted_count

        except Exception as e:
            logger.warning(f"Ошибка при очистке дубликатов: {str(e)}")
            return 0

    def _is_offset_provided_by_user(self) -> bool:
        """Определяет, был ли параметр --offset явно указан пользователем."""
        import sys

        # Проверяем аргументы командной строки
        for arg in sys.argv:
            if arg == '--offset' or arg.startswith('--offset='):
                return True
        return False

    def _handle_interrupt(self, offset: int, processed_games: int, progress_bar):
        """Обработка прерывания пользователем - сохраняем прогресс на основе ОБРАБОТАННЫХ игр."""
        # Останавливаем прогресс-бар если есть
        if progress_bar:
            progress_bar.finish()

        # Рассчитываем текущий оффсет на основе ОБРАБОТАННЫХ игр
        current_offset = offset + processed_games

        # Сохраняем оффсет для продолжения
        self._save_offset_to_file(current_offset)

        # Выводим информацию о прерывании на русском
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.WARNING("⏹️ ОБРАБОТКА ПРЕРВАНА ПОЛЬЗОВАТЕЛЕМ"))
        self.stdout.write("=" * 60)
        self.stdout.write(f"📍 ТЕКУЩИЙ ОФФСЕТ: {current_offset}")
        self.stdout.write(f"📊 ОБРАБОТАНО ИГР В ЭТОМ ЗАПУСКЕ: {processed_games}")
        self.stdout.write(f"💾 Оффсет сохранен для продолжения (на основе обработанных игр)")

        # Предупреждение
        self.stdout.write(f"⚠️  Следующий запуск начнется с игры ID {current_offset}")

        # Показываем команду для продолжения
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.SUCCESS("🚀 КОМАНДА ДЛЯ ПРОДОЛЖЕНИЯ:"))
        self.stdout.write(f"python manage.py create_game_cards --offset {current_offset}")

        # Если есть другие важные опции, добавляем их
        self.stdout.write("=" * 60)
        self.stdout.write("ℹ️  Для возобновления с сохраненными настройками используйте:")
        self.stdout.write(
            f"python manage.py create_game_cards --offset {current_offset} --types {self.current_types} --config {self.current_config}")
        self.stdout.write("=" * 60)

        # Выходим с кодом 130 (стандартный для прерывания)
        import sys
        sys.exit(130)

    def _handle_error(self, error: Exception, progress_bar):
        """Обработка непредвиденных ошибок."""
        # Останавливаем прогресс-бар если есть
        if progress_bar:
            progress_bar.finish()

        # Выводим информацию об ошибке на русском
        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(self.style.ERROR("❌ НЕПРЕДВИДЕННАЯ ОШИБКА"))
        self.stdout.write("=" * 60)
        self.stdout.write(f"Ошибка: {str(error)}")
        self.stdout.write("=" * 60)
        self.stdout.write("⚠️  Обратитесь к администратору или проверьте логи для деталей")
        self.stdout.write("=" * 60)

        # Логируем полный traceback для отладки
        import traceback
        logger.error(f"Ошибка в create_game_cards: {str(error)}\n{traceback.format_exc()}")

        # Выходим с кодом ошибки
        import sys
        sys.exit(1)

    def _save_offset_to_file(self, offset: int):
        """Сохраняет оффсет в файл для истории."""
        try:
            offset_file = os.path.join(os.getcwd(), "last_card_offset.txt")
            with open(offset_file, 'w', encoding='utf-8') as f:
                f.write(str(offset))
                f.write(f"\n# Сохранено: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                f.write(f"\n# Команда: create_game_cards --offset {offset}")
        except Exception as e:
            logger.warning(f"Не удалось сохранить оффсет: {e}")

    def _load_offset_from_file(self) -> int:
        """Загружает оффсет из файла для истории."""
        try:
            offset_file = os.path.join(os.getcwd(), "last_card_offset.txt")
            if os.path.exists(offset_file):
                with open(offset_file, 'r', encoding='utf-8') as f:
                    first_line = f.readline().strip()
                    if first_line and first_line.isdigit():
                        return int(first_line)
        except Exception as e:
            logger.warning(f"Не удалось загрузить оффсет: {e}")

        return 0

    def _parse_game_ids(self, game_ids_str: str) -> List[int]:
        """Разбор строки с ID игр, разделенных запятыми."""
        if not game_ids_str:
            return []

        try:
            return [int(id_str.strip()) for id_str in game_ids_str.split(',') if id_str.strip()]
        except ValueError as e:
            raise CommandError(f"Неверный формат ID игр: {e}")

    def _get_games_to_process(
            self,
            game_ids: List[int] = None,
            game_types: str = 'primary',
            limit: int = None,
            resume: bool = False,
            offset: int = 0
    ) -> Any:
        """Получение набора данных ВСЕХ игр для обработки."""
        # Начинаем с базового набора данных - ВСЕ игры
        if game_ids:
            games = Game.objects.filter(id__in=game_ids)
        else:
            games = Game.objects.all()

        # Фильтрация по типу игры если нужно
        if game_types == 'primary':
            games = games.filter(game_type__in=GameTypeEnum._PRIMARY_GAME_TYPES)
        elif game_types == 'main':
            games = games.filter(game_type=GameTypeEnum.MAIN_GAME)
        elif game_types == 'dlc':
            games = games.filter(game_type=GameTypeEnum.DLC_ADDON)
        elif game_types == 'expansions':
            games = games.filter(game_type__in=[GameTypeEnum.EXPANSION, GameTypeEnum.STANDALONE_EXPANSION])
        # 'all' включает все игры - НИКАКОЙ фильтрации

        # Применение смещения - сортируем по ID для стабильного оффсета
        games = games.order_by('id')

        if offset > 0:
            games = games[offset:]  # Пропускаем первые N игр

        # Продолжение с последней обработанной игры
        elif resume and not game_ids:
            last_game_id = self._get_last_processed_game_id()
            if last_game_id:
                games = games.filter(id__gt=last_game_id)
                self.stdout.write(f"📍 Продолжаем с игры ID: {last_game_id}")

        # Применение лимита если указан
        if limit:
            games = games[:limit]

        return games

    def _get_last_processed_game_id(self) -> int:
        """Получение ID последней игры с кэшированными карточками."""
        try:
            last_card = GameCardCache.objects.filter(is_active=True).order_by('-game_id').first()
            return last_card.game_id if last_card else None
        except Exception:
            return None

    def _get_card_configurations(self, config: str) -> List[str]:
        """Получение списка конфигураций карточек для создания."""
        if config == 'all':
            return ['normal', 'similarity']
        elif config == 'similarity':
            return ['similarity']
        else:  # normal
            return ['normal']

    def _load_games_with_data(self, game_ids: List[int]) -> Dict[int, Game]:
        """Загрузка ВСЕХ игр с требуемыми предзагруженными данными."""
        # Создание объектов предзагрузки для эффективной загрузки
        genre_prefetch = Prefetch('genres', queryset=Genre.objects.only('id', 'name'))
        platform_prefetch = Prefetch('platforms', queryset=Platform.objects.only('id', 'name', 'slug'))
        perspective_prefetch = Prefetch('player_perspectives', queryset=PlayerPerspective.objects.only('id', 'name'))

        # Для ключевых слов нужна информация о категориях
        keyword_prefetch = Prefetch(
            'keywords',
            queryset=Keyword.objects.select_related('category').only(
                'id', 'name', 'category__id', 'category__name'
            )
        )

        theme_prefetch = Prefetch('themes', queryset=Theme.objects.only('id', 'name'))
        game_mode_prefetch = Prefetch('game_modes', queryset=GameMode.objects.only('id', 'name'))

        # Загрузка ВСЕХ игр со всеми предзагруженными данными (без фильтрации по рейтингу)
        games = Game.objects.filter(id__in=game_ids).prefetch_related(
            genre_prefetch,
            platform_prefetch,
            perspective_prefetch,
            keyword_prefetch,
            theme_prefetch,
            game_mode_prefetch
        ).only(
            'id', 'name', 'rating', 'rating_count',
            'first_release_date', 'cover_url', 'game_type'
        )

        # Преобразование в словарь для удобного доступа
        return {game.id: game for game in games}

    def _process_batch(
            self,
            games_dict: Dict[int, Game],
            configs: List[str],
            force: bool,
            skip_existing: bool,
            dry_run: bool,
            verbosity: int,
            progress_bar: ProgressBar = None
    ) -> Tuple[int, int, int, int]:
        """Обработка пакета игр с использованием новых методов GameCardCache."""
        created = 0
        skipped = 0
        errors = 0
        saved = 0  # Счетчик успешно сохраненных игр

        cards_to_create = []

        for game_id, game in games_dict.items():
            try:
                # Обработка каждой конфигурации
                for config in configs:
                    show_similarity = (config == 'similarity')

                    # Проверка, существует ли карточка уже (только если не force и skip_existing)
                    if not force and skip_existing:
                        cache_key = GameCardCache._generate_key(
                            game_id, show_similarity, None, 'normal'
                        )

                        try:
                            existing_card = GameCardCache.objects.get(cache_key=cache_key, is_active=True)
                            skipped += 1
                            continue
                        except GameCardCache.DoesNotExist:
                            pass  # Карточка не существует, продолжаем

                    # Рендеринг карточки
                    rendered_card = self._render_game_card(game, show_similarity)

                    # Подготовка связанных данных
                    related_data = self._extract_related_data(game)

                    # Создание объекта кэша карточки
                    if not dry_run:
                        cards_to_create.append((
                            game,
                            rendered_card,
                            show_similarity,
                            None,  # similarity_percent
                            'normal',  # card_size
                            related_data
                        ))

                    created += 1

                    if verbosity >= 3:  # Только при очень высокой детализации
                        self.stdout.write(
                            f"  🎯 Подготовлена карточка для игры {game_id} ({game.name}) "
                            f"(конфигурация: {config}, размер: {len(rendered_card)} байт)"
                        )

            except Exception as e:
                errors += 1
                logger.error(f"Ошибка обработки игры {game_id}: {str(e)}", exc_info=True)

                if verbosity >= 2:
                    self.stdout.write(
                        f"  ❌ Ошибка обработки игры {game_id}: {str(e)}"
                    )

        # Массовое создание карточек если не режим dry-run
        if not dry_run and cards_to_create:
            try:
                with transaction.atomic():
                    # Используем новый метод bulk_create_or_update_cards для обработки уникальности
                    stats = GameCardCache.bulk_create_or_update_cards(cards_to_create, batch_size=50)

                    # Обновляем счетчики на основе статистики
                    created = stats['created']
                    updated = stats['updated']
                    skipped += stats['skipped']
                    errors += stats['errors']
                    saved = stats['created'] + stats['updated']  # Успешно создано + обновлено

                    if verbosity >= 2:  # Только при высокой детализации
                        self.stdout.write(
                            f"  ✅ Пакет обработан: создано={stats['created']}, "
                            f"обновлено={stats['updated']}, пропущено={stats['skipped']}, "
                            f"ошибок={stats['errors']}"
                        )

            except Exception as e:
                errors += len(cards_to_create)
                logger.error(f"Ошибка массового создания карточек: {str(e)}", exc_info=True)

                if verbosity >= 1:
                    self.stdout.write(
                        f"  ❌ Ошибка массового создания карточек: {str(e)}"
                    )
        elif dry_run and cards_to_create:
            # В режиме dry-run считаем все карточки как бы сохраненными
            saved += len(cards_to_create)

        return created, skipped, errors, saved

    def _render_game_card(self, game: Game, show_similarity: bool = False) -> str:
        """Рендеринг HTML-карточки игры."""
        # Подготовка контекста для шаблона
        context = {
            'game': game,
            'show_similarity': show_similarity,
        }

        # Для режима сходства добавляем фиктивное значение сходства
        if show_similarity:
            if not hasattr(game, 'similarity'):
                game.similarity = 0  # Значение по умолчанию

        # Рендеринг шаблона
        return render_to_string('games/partials/_game_card.html', context)

    def _extract_related_data(self, game: Game) -> Dict[str, List[Dict]]:
        """Извлечение связанных данных из игры для хранения в JSON."""
        return {
            'genres': [
                {'id': genre.id, 'name': genre.name}
                for genre in game.genres.all()
            ],
            'platforms': [
                {'id': platform.id, 'name': platform.name, 'slug': platform.slug}
                for platform in game.platforms.all()
            ],
            'perspectives': [
                {'id': perspective.id, 'name': perspective.name}
                for perspective in game.player_perspectives.all()
            ],
            'keywords': [
                {
                    'id': keyword.id,
                    'name': keyword.name,
                    'category_id': keyword.category.id if keyword.category else None,
                    'category_name': keyword.category.name if keyword.category else None
                }
                for keyword in game.keywords.all()
            ],
            'themes': [
                {'id': theme.id, 'name': theme.name}
                for theme in game.themes.all()
            ],
            'game_modes': [
                {'id': game_mode.id, 'name': game_mode.name}
                for game_mode in game.game_modes.all()
            ],
        }

    def _update_cache_stats(self):
        """Обновление статистики кэша."""
        try:
            total_cards = GameCardCache.objects.filter(is_active=True).count()
            total_hits = GameCardCache.objects.filter(is_active=True).aggregate(
                total=models.Sum('hit_count')
            )['total'] or 0

            self.stdout.write(
                self.style.SUCCESS(
                    f"📊 Статистика кэша обновлена: {total_cards} активных карточек, "
                    f"{total_hits} всего обращений"
                )
            )
        except Exception as e:
            logger.warning(f"Не удалось обновить статистику кэша: {str(e)}")