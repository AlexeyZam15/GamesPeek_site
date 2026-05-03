# games/management/commands/watch_update_vectors.py

from django.core.management.base import BaseCommand
from django.db import connection
import time
import sys
import signal
from datetime import datetime, timedelta


class Command(BaseCommand):
    help = 'Отслеживает изменения в БД, которые делает команда update_vectors'

    def add_arguments(self, parser):
        parser.add_argument(
            '--interval',
            type=int,
            default=2,
            help='Интервал обновления в секундах (по умолчанию: 2)'
        )

    def handle(self, *args, **options):
        self.interval = options['interval']

        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.stdout.write(self.style.SUCCESS(f'\n{"=" * 60}'))
        self.stdout.write(self.style.SUCCESS('🔍 МОНИТОРИНГ ИЗМЕНЕНИЙ update_vectors'))
        self.stdout.write(self.style.SUCCESS(f'{"=" * 60}\n'))

        self.stdout.write(f'Интервал обновления: {self.interval} сек')
        self.stdout.write(self.style.WARNING('Нажмите Ctrl+C для выхода\n'))

        previous_stats = self._get_current_stats()
        start_time = time.time()
        last_update = time.time()

        try:
            while self.running:
                current_time = time.time()
                if current_time - last_update >= self.interval:
                    current_stats = self._get_current_stats()
                    changes = self._calculate_changes(previous_stats, current_stats)
                    self._print_stats(current_stats, changes, start_time)
                    previous_stats = current_stats
                    last_update = current_time
                time.sleep(0.1)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'\n❌ Ошибка: {e}'))
        finally:
            self.stdout.write(self.style.WARNING('\n👋 Мониторинг остановлен'))

    def signal_handler(self, signum, frame):
        self.stdout.write(self.style.WARNING('\n\n👋 Получен сигнал завершения...'))
        self.running = False

    def _get_current_stats(self):
        """Получает текущую статистику из базы данных (без games_game_keywords)"""
        stats = {
            'timestamp': datetime.now(),
            'total_games': 0,
            'games_with_keywords': 0,
            'games_with_genres': 0,
            'games_with_themes': 0,
            'games_with_perspectives': 0,
            'games_with_developers': 0,
            'games_with_modes': 0,
            'games_with_engines': 0,
            'total_keyword_relations': 0,
            'total_genre_relations': 0,
            'total_theme_relations': 0,
            'total_perspective_relations': 0,
            'total_developer_relations': 0,
            'total_mode_relations': 0,
            'total_engine_relations': 0,
            'last_game_id': 0,
            'active_locks': 0,
            'blocked_processes': 0,
            'long_running_queries': 0
        }

        with connection.cursor() as cursor:
            # Общее количество игр
            cursor.execute("SELECT COUNT(*) FROM games_game")
            stats['total_games'] = cursor.fetchone()[0]

            # Игры с непустыми векторами (используем array_length)
            cursor.execute("""
                           SELECT COUNT(CASE WHEN array_length(keyword_ids, 1) > 0 THEN 1 END)     as with_keywords,
                                  COUNT(CASE WHEN array_length(genre_ids, 1) > 0 THEN 1 END)       as with_genres,
                                  COUNT(CASE WHEN array_length(theme_ids, 1) > 0 THEN 1 END)       as with_themes,
                                  COUNT(CASE WHEN array_length(perspective_ids, 1) > 0 THEN 1 END) as with_perspectives,
                                  COUNT(CASE WHEN array_length(developer_ids, 1) > 0 THEN 1 END)   as with_developers,
                                  COUNT(CASE WHEN array_length(game_mode_ids, 1) > 0 THEN 1 END)   as with_modes,
                                  COUNT(CASE WHEN array_length(engine_ids, 1) > 0 THEN 1 END)      as with_engines
                           FROM games_game
                           """)
            row = cursor.fetchone()
            stats['games_with_keywords'] = row[0] or 0
            stats['games_with_genres'] = row[1] or 0
            stats['games_with_themes'] = row[2] or 0
            stats['games_with_perspectives'] = row[3] or 0
            stats['games_with_developers'] = row[4] or 0
            stats['games_with_modes'] = row[5] or 0
            stats['games_with_engines'] = row[6] or 0

            # Общее количество связей через array_length
            cursor.execute("""
                           SELECT COALESCE(SUM(array_length(keyword_ids, 1)), 0)     as total_keywords,
                                  COALESCE(SUM(array_length(genre_ids, 1)), 0)       as total_genres,
                                  COALESCE(SUM(array_length(theme_ids, 1)), 0)       as total_themes,
                                  COALESCE(SUM(array_length(perspective_ids, 1)), 0) as total_perspectives,
                                  COALESCE(SUM(array_length(developer_ids, 1)), 0)   as total_developers,
                                  COALESCE(SUM(array_length(game_mode_ids, 1)), 0)   as total_modes,
                                  COALESCE(SUM(array_length(engine_ids, 1)), 0)      as total_engines
                           FROM games_game
                           """)
            row = cursor.fetchone()
            stats['total_keyword_relations'] = row[0] or 0
            stats['total_genre_relations'] = row[1] or 0
            stats['total_theme_relations'] = row[2] or 0
            stats['total_perspective_relations'] = row[3] or 0
            stats['total_developer_relations'] = row[4] or 0
            stats['total_mode_relations'] = row[5] or 0
            stats['total_engine_relations'] = row[6] or 0

            # Последняя обработанная игра (с непустым keyword_ids)
            cursor.execute("""
                           SELECT COALESCE(MAX(id), 0)
                           FROM games_game
                           WHERE array_length(keyword_ids, 1) > 0
                           """)
            stats['last_game_id'] = cursor.fetchone()[0]

            # Блокировки
            cursor.execute("SELECT COUNT(*) FROM pg_locks WHERE NOT granted")
            stats['active_locks'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM pg_stat_activity WHERE wait_event IS NOT NULL")
            stats['blocked_processes'] = cursor.fetchone()[0]

            # Долгие запросы (больше 5 секунд)
            cursor.execute("""
                           SELECT COUNT(*)
                           FROM pg_stat_activity
                           WHERE state = 'active'
                             AND query_start < now() - interval '5 seconds'
                             AND query NOT LIKE '%pg_stat_activity%'
                           """)
            stats['long_running_queries'] = cursor.fetchone()[0]

        return stats

    def _calculate_changes(self, prev, curr):
        """Вычисляет изменения между двумя замерами"""
        return {
            'games_with_keywords': curr['games_with_keywords'] - prev['games_with_keywords'],
            'games_with_genres': curr['games_with_genres'] - prev['games_with_genres'],
            'games_with_themes': curr['games_with_themes'] - prev['games_with_themes'],
            'games_with_perspectives': curr['games_with_perspectives'] - prev['games_with_perspectives'],
            'games_with_developers': curr['games_with_developers'] - prev['games_with_developers'],
            'games_with_modes': curr['games_with_modes'] - prev['games_with_modes'],
            'games_with_engines': curr['games_with_engines'] - prev['games_with_engines'],
            'keyword_relations': curr['total_keyword_relations'] - prev['total_keyword_relations'],
            'genre_relations': curr['total_genre_relations'] - prev['total_genre_relations'],
            'theme_relations': curr['total_theme_relations'] - prev['total_theme_relations'],
            'perspective_relations': curr['total_perspective_relations'] - prev['total_perspective_relations'],
            'developer_relations': curr['total_developer_relations'] - prev['total_developer_relations'],
            'mode_relations': curr['total_mode_relations'] - prev['total_mode_relations'],
            'engine_relations': curr['total_engine_relations'] - prev['total_engine_relations'],
            'last_game_id': curr['last_game_id'] - prev['last_game_id'],
            'active_locks': curr['active_locks'] - prev['active_locks']
        }

    def _print_stats(self, stats, changes, start_time):
        """Выводит статистику"""
        self.stdout.write('\033[2J\033[H')

        elapsed = time.time() - start_time
        progress = (stats['games_with_keywords'] / stats['total_games']) * 100 if stats['total_games'] > 0 else 0

        self.stdout.write('=' * 100)
        self.stdout.write(self.style.SUCCESS(f'📊 МОНИТОРИНГ update_vectors'))
        self.stdout.write('=' * 100)
        self.stdout.write(f'Время мониторинга: {timedelta(seconds=int(elapsed))}')
        self.stdout.write(f'Последнее обновление: {stats["timestamp"].strftime("%H:%M:%S")}')
        self.stdout.write('-' * 100)

        self.stdout.write(f'\n🎮 ОБЩИЙ ПРОГРЕСС:')
        self.stdout.write(f'   Всего игр: {stats["total_games"]}')
        self.stdout.write(f'   Обновлено (keyword_ids): {stats["games_with_keywords"]} ({progress:.1f}%)')
        self.stdout.write(f'   Последняя игра ID: {stats["last_game_id"]}')

        if changes['last_game_id'] > 0:
            speed = changes['last_game_id'] / self.interval
            self.stdout.write(f'   Скорость: {speed:.1f} игр/сек')

        self.stdout.write(f'\n⚡ ИЗМЕНЕНИЯ ЗА ПОСЛЕДНИЕ {self.interval} СЕК:')
        has_changes = False

        if changes['games_with_keywords'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["games_with_keywords"]} игр обновлено'))
            has_changes = True
        if changes['keyword_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["keyword_relations"]} ключевых слов'))
            has_changes = True
        if changes['genre_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["genre_relations"]} жанров'))
            has_changes = True
        if changes['theme_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["theme_relations"]} тем'))
            has_changes = True
        if changes['perspective_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["perspective_relations"]} перспектив'))
            has_changes = True
        if changes['developer_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["developer_relations"]} разработчиков'))
            has_changes = True
        if changes['mode_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["mode_relations"]} режимов игры'))
            has_changes = True
        if changes['engine_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["engine_relations"]} движков'))
            has_changes = True

        if not has_changes:
            self.stdout.write(self.style.WARNING('   Нет изменений'))

        self.stdout.write(f'\n📈 ТЕКУЩАЯ СТАТИСТИКА:')
        self.stdout.write(
            f'   Жанры:          {stats["games_with_genres"]} игр, {stats["total_genre_relations"]} связей')
        self.stdout.write(
            f'   Ключевые слова: {stats["games_with_keywords"]} игр, {stats["total_keyword_relations"]} связей')
        self.stdout.write(
            f'   Темы:           {stats["games_with_themes"]} игр, {stats["total_theme_relations"]} связей')
        self.stdout.write(
            f'   Перспективы:    {stats["games_with_perspectives"]} игр, {stats["total_perspective_relations"]} связей')
        self.stdout.write(
            f'   Разработчики:   {stats["games_with_developers"]} игр, {stats["total_developer_relations"]} связей')
        self.stdout.write(f'   Режимы игры:    {stats["games_with_modes"]} игр, {stats["total_mode_relations"]} связей')
        self.stdout.write(
            f'   Движки:         {stats["games_with_engines"]} игр, {stats["total_engine_relations"]} связей')

        self.stdout.write(f'\n🔒 СОСТОЯНИЕ БД:')
        if stats['active_locks'] > 0:
            self.stdout.write(self.style.ERROR(f'   Активных блокировок: {stats["active_locks"]}'))
        else:
            self.stdout.write(self.style.SUCCESS('   ✅ Блокировок нет'))

        if stats['blocked_processes'] > 0:
            self.stdout.write(self.style.ERROR(f'   Заблокированных процессов: {stats["blocked_processes"]}'))

        if stats['long_running_queries'] > 0:
            self.stdout.write(self.style.WARNING(f'   Долгих запросов (>5 сек): {stats["long_running_queries"]}'))

        self.stdout.write(f'\n🔍 ПОСЛЕДНЯЯ ОБРАБОТАННАЯ ИГРА:')
        self.stdout.write(f'   ID: {stats["last_game_id"]}')

        self.stdout.write('\n' + '=' * 100)
        self.stdout.write(self.style.WARNING('Нажмите Ctrl+C для выхода'))
        self.stdout.flush()