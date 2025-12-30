# games/management/commands/cache_data.py
import time
import sys
from datetime import timedelta
from django.core.management.base import BaseCommand
from django.db import transaction, connection
from django.db.models import Count
from django.utils import timezone
from games.models import Game, Keyword, Platform, Theme, PlayerPerspective, Company, GameMode
from django.core.cache import cache


class Command(BaseCommand):
    help = 'Сверхбыстрое кэширование данных с прогресс-баром'

    def handle(self, *args, **options):
        total_start = time.time()

        print("\n" + "=" * 60)
        print("🚀 ЗАПУСК КЭШИРОВАНИЯ ДАННЫХ".center(60))
        print("=" * 60 + "\n")

        # Шаг 1: Кэширование фильтров
        self._progress("Сбор данных фильтров", 0, 1, total_start)
        self._cache_filters(total_start)
        self._progress("Сбор данных фильтров", 1, 1, total_start)

        # Шаг 2: Кэширование счетчиков игр
        print("\n📊 КЭШИРОВАНИЕ СЧЕТЧИКОВ ИГР")
        self._cache_game_counts(total_start)

        # Шаг 3: Кэширование ключевых слов
        print("\n🏷️  ОБНОВЛЕНИЕ КЛЮЧЕВЫХ СЛОВ")
        self._cache_keywords(total_start)

        # Итог
        total_time = time.time() - total_start
        print("\n" + "=" * 60)
        print(f"✅ ВСЕ ДАННЫЕ ЗАКЭШИРОВАНЫ ЗА {total_time:.2f} СЕК".center(60))
        print("=" * 60 + "\n")

    def _progress(self, text, current, total, start_time, bar_width=40):
        """Универсальный прогресс-бар с оценкой времени."""
        percent = current / total if total > 0 else 0
        filled = int(bar_width * percent)
        bar = '█' * filled + '░' * (bar_width - filled)

        # Расчет времени
        elapsed = time.time() - start_time
        if current > 0:
            estimated_total = elapsed / percent if percent > 0 else 0
            remaining = estimated_total - elapsed if estimated_total > elapsed else 0

            # Форматирование времени
            elapsed_str = self._format_time(elapsed)
            remaining_str = self._format_time(remaining) if remaining > 0 else "00:00"

            sys.stdout.write(
                f'\r{text}: |{bar}| {percent * 100:6.1f}% | Время: {elapsed_str} | Осталось: {remaining_str}')
        else:
            sys.stdout.write(f'\r{text}: |{bar}| {percent * 100:6.1f}% | Запуск...')

        sys.stdout.flush()

        if current == total:
            print()

    def _format_time(self, seconds):
        """Форматирование времени в удобный вид."""
        if seconds < 60:
            return f"{seconds:.0f} сек"
        elif seconds < 3600:
            minutes = int(seconds // 60)
            secs = int(seconds % 60)
            return f"{minutes:02d}:{secs:02d} мин"
        else:
            hours = int(seconds // 3600)
            minutes = int((seconds % 3600) // 60)
            return f"{hours:02d}:{minutes:02d} ч"

    def _cache_filters(self, total_start):
        """Кэширование всех данных для фильтров."""
        data = {}
        steps = 6  # Общее количество шагов
        current_step = 0

        # Платформы
        self._progress("  • Платформы", current_step, steps, total_start)
        platforms = list(Platform.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('-game_count', 'name')[:50])
        data['platforms'] = platforms
        current_step += 1
        self._progress("  • Платформы", current_step, steps, total_start)

        # Ключевые слова
        keywords = list(Keyword.objects.filter(
            cached_usage_count__gt=0
        ).select_related('category').order_by('-cached_usage_count')[:50])
        data['popular_keywords'] = keywords
        current_step += 1
        self._progress("  • Ключевые слова", current_step, steps, total_start)

        # Режимы игры
        game_modes = list(GameMode.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('name')[:30])
        data['game_modes'] = game_modes
        current_step += 1
        self._progress("  • Режимы игры", current_step, steps, total_start)

        # Темы
        themes = list(Theme.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('name')[:30])
        data['themes'] = themes
        current_step += 1
        self._progress("  • Темы", current_step, steps, total_start)

        # Разработчики и перспективы
        perspectives = list(PlayerPerspective.objects.annotate(
            game_count=Count('game')
        ).filter(game_count__gt=0).order_by('name')[:20])
        data['perspectives'] = perspectives

        developers = list(Company.objects.annotate(
            developed_game_count=Count('developed_games')
        ).filter(developed_game_count__gt=0).order_by('name')[:30])
        data['developers'] = developers
        current_step += 1
        self._progress("  • Разработчики и перспективы", current_step, steps, total_start)

        cache.set('game_list_filters_data', data, 86400)
        current_step += 1
        self._progress("  • Сохранение в кэш", current_step, steps, total_start)

    def _cache_game_counts(self, total_start):
        """Массовое кэширование счетчиков для игр."""
        print("  ⚡ Получение списка игр...")

        all_ids = list(Game.objects.values_list('id', flat=True))
        total = len(all_ids)

        print(f"  Всего игр: {total:,}")
        print("  ⚡ Обновление счетчиков...")

        if total == 0:
            return

        # Разбиваем на батчи по 1000 для безопасности
        batch_size = 1000
        processed = 0
        batch_start_time = time.time()

        for i in range(0, total, batch_size):
            batch_start = time.time()
            batch_ids = all_ids[i:i + batch_size]
            self._safe_update_batch(batch_ids)
            processed += len(batch_ids)

            # Расчет оставшегося времени
            batch_time = time.time() - batch_start
            avg_time_per_batch = (time.time() - batch_start_time) / ((i // batch_size) + 1)
            remaining_batches = (total - processed + batch_size - 1) // batch_size
            estimated_remaining = avg_time_per_batch * remaining_batches

            # Обновляем прогресс-бар с оценкой времени
            percent = processed / total
            filled = int(40 * percent)
            bar = '█' * filled + '░' * (40 - filled)

            elapsed = time.time() - total_start
            elapsed_str = self._format_time(elapsed)
            remaining_str = self._format_time(estimated_remaining)

            sys.stdout.write(
                f'\r  Прогресс: |{bar}| {percent * 100:6.1f}% '
                f'({processed:,}/{total:,}) | Время: {elapsed_str} | Осталось: {remaining_str}'
            )
            sys.stdout.flush()

        print()

    def _safe_update_batch(self, game_ids):
        """Безопасное обновление счетчиков через ORM."""
        if not game_ids:
            return

        with transaction.atomic():
            # Используем ORM для безопасности
            from django.db.models import Count

            games = Game.objects.filter(id__in=game_ids).annotate(
                genre_count=Count('genres'),
                keyword_count=Count('keywords'),
                platform_count=Count('platforms'),
                developer_count=Count('developers')
            )

            updates = []
            for game in games:
                game._cached_genre_count = game.genre_count
                game._cached_keyword_count = game.keyword_count
                game._cached_platform_count = game.platform_count
                game._cached_developer_count = game.developer_count
                game._cache_updated_at = timezone.now()
                updates.append(game)

            if updates:
                Game.objects.bulk_update(
                    updates,
                    [
                        '_cached_genre_count', '_cached_keyword_count',
                        '_cached_platform_count', '_cached_developer_count',
                        '_cache_updated_at'
                    ]
                )

    def _cache_keywords(self, total_start):
        """Быстрое обновление счетчиков ключевых слов."""
        print("  ⚡ Получение списка ключевых слов...")

        keywords = Keyword.objects.all()
        total = keywords.count()

        print(f"  Всего ключевых слов: {total:,}")
        print("  ⚡ Обновление счетчиков...")

        if total == 0:
            return

        start_time = time.time()
        processed = 0

        with transaction.atomic():
            for i, keyword in enumerate(keywords):
                # Обновляем счетчик без сохранения (оптимизировано)
                actual_count = keyword.game_set.count()
                if keyword.cached_usage_count != actual_count:
                    keyword.cached_usage_count = actual_count
                    keyword.last_count_update = timezone.now()
                    keyword.save(update_fields=['cached_usage_count', 'last_count_update'])

                processed += 1

                # Показываем прогресс каждые 100 записей или на последней
                if processed % 100 == 0 or processed == total:
                    # Расчет оставшегося времени
                    elapsed_batch = time.time() - start_time
                    avg_time_per_item = elapsed_batch / processed if processed > 0 else 0
                    remaining_items = total - processed
                    estimated_remaining = avg_time_per_item * remaining_items

                    # Обновляем прогресс-бар
                    percent = processed / total
                    filled = int(40 * percent)
                    bar = '█' * filled + '░' * (40 - filled)

                    elapsed_total = time.time() - total_start
                    elapsed_str = self._format_time(elapsed_total)
                    remaining_str = self._format_time(estimated_remaining)

                    sys.stdout.write(
                        f'\r  Прогресс: |{bar}| {percent * 100:6.1f}% '
                        f'({processed:,}/{total:,}) | Время: {elapsed_str} | Осталось: {remaining_str}'
                    )
                    sys.stdout.flush()

        print()