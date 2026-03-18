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

        # Устанавливаем обработчик сигнала
        self.running = True
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.stdout.write(self.style.SUCCESS(f'\n{"=" * 60}'))
        self.stdout.write(self.style.SUCCESS('🔍 МОНИТОРИНГ ИЗМЕНЕНИЙ update_vectors'))
        self.stdout.write(self.style.SUCCESS(f'{"=" * 60}\n'))

        self.stdout.write(f'Интервал обновления: {self.interval} сек')
        self.stdout.write(self.style.WARNING('Нажмите Ctrl+C для выхода\n'))

        # Сохраняем предыдущие значения для отслеживания изменений
        previous_stats = self._get_current_stats()
        start_time = time.time()
        last_update = time.time()

        try:
            while self.running:
                # Проверяем, не пора ли обновить статистику
                current_time = time.time()
                if current_time - last_update >= self.interval:
                    # Получаем текущую статистику
                    current_stats = self._get_current_stats()

                    # Вычисляем изменения
                    changes = self._calculate_changes(previous_stats, current_stats)

                    # Выводим статистику
                    self._print_stats(current_stats, changes, start_time)

                    # Обновляем предыдущие значения и время
                    previous_stats = current_stats
                    last_update = current_time

                # Короткий сон для возможности обработки сигнала
                time.sleep(0.1)

        except Exception as e:
            self.stdout.write(self.style.ERROR(f'\n❌ Ошибка: {e}'))
        finally:
            self.stdout.write(self.style.WARNING('\n👋 Мониторинг остановлен'))

    def signal_handler(self, signum, frame):
        """Обработчик сигналов"""
        self.stdout.write(self.style.WARNING('\n\n👋 Получен сигнал завершения...'))
        self.running = False

    def _get_current_stats(self):
        """Получает текущую статистику из базы данных"""
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
            'last_keyword_relation_id': 0,
            'last_genre_relation_id': 0,
            'last_theme_relation_id': 0,
            'last_perspective_relation_id': 0,
            'last_developer_relation_id': 0,
            'last_mode_relation_id': 0,
            'last_engine_relation_id': 0,
            'active_locks': 0,
            'blocked_processes': 0,
            'long_running_queries': 0
        }

        with connection.cursor() as cursor:
            # Общее количество игр
            cursor.execute("SELECT COUNT(*) FROM games_game")
            stats['total_games'] = cursor.fetchone()[0]

            # Игры с непустыми векторами
            cursor.execute("""
                           SELECT COUNT(CASE WHEN keyword_ids != '{}' THEN 1 END)     as with_keywords,
                                  COUNT(CASE WHEN genre_ids != '{}' THEN 1 END)       as with_genres,
                                  COUNT(CASE WHEN theme_ids != '{}' THEN 1 END)       as with_themes,
                                  COUNT(CASE WHEN perspective_ids != '{}' THEN 1 END) as with_perspectives,
                                  COUNT(CASE WHEN developer_ids != '{}' THEN 1 END)   as with_developers,
                                  COUNT(CASE WHEN game_mode_ids != '{}' THEN 1 END)   as with_modes,
                                  COUNT(CASE WHEN engine_ids != '{}' THEN 1 END)      as with_engines
                           FROM games_game
                           """)
            row = cursor.fetchone()
            stats['games_with_keywords'] = row[0]
            stats['games_with_genres'] = row[1]
            stats['games_with_themes'] = row[2]
            stats['games_with_perspectives'] = row[3]
            stats['games_with_developers'] = row[4]
            stats['games_with_modes'] = row[5]
            stats['games_with_engines'] = row[6]

            # Общее количество связей
            cursor.execute("SELECT COUNT(*) FROM games_game_keywords")
            stats['total_keyword_relations'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM games_game_genres")
            stats['total_genre_relations'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM games_game_themes")
            stats['total_theme_relations'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM games_game_player_perspectives")
            stats['total_perspective_relations'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM games_game_developers")
            stats['total_developer_relations'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM games_game_game_modes")
            stats['total_mode_relations'] = cursor.fetchone()[0]

            cursor.execute("SELECT COUNT(*) FROM games_game_engines")
            stats['total_engine_relations'] = cursor.fetchone()[0]

            # Последние обработанные ID
            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM games_game WHERE keyword_ids != '{}'")
            stats['last_game_id'] = cursor.fetchone()[0]

            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM games_game_keywords")
            stats['last_keyword_relation_id'] = cursor.fetchone()[0]

            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM games_game_genres")
            stats['last_genre_relation_id'] = cursor.fetchone()[0]

            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM games_game_themes")
            stats['last_theme_relation_id'] = cursor.fetchone()[0]

            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM games_game_player_perspectives")
            stats['last_perspective_relation_id'] = cursor.fetchone()[0]

            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM games_game_developers")
            stats['last_developer_relation_id'] = cursor.fetchone()[0]

            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM games_game_game_modes")
            stats['last_mode_relation_id'] = cursor.fetchone()[0]

            cursor.execute("SELECT COALESCE(MAX(id), 0) FROM games_game_engines")
            stats['last_engine_relation_id'] = cursor.fetchone()[0]

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
        # Очищаем экран
        self.stdout.write('\033[2J\033[H')

        elapsed = time.time() - start_time
        progress = (stats['games_with_keywords'] / stats['total_games']) * 100 if stats['total_games'] > 0 else 0

        # Заголовок
        self.stdout.write('=' * 100)
        self.stdout.write(self.style.SUCCESS(f'📊 МОНИТОРИНГ update_vectors'))
        self.stdout.write('=' * 100)
        self.stdout.write(f'Время мониторинга: {timedelta(seconds=int(elapsed))}')
        self.stdout.write(f'Последнее обновление: {stats["timestamp"].strftime("%H:%M:%S")}')
        self.stdout.write('-' * 100)

        # Общий прогресс
        self.stdout.write(f'\n🎮 ОБЩИЙ ПРОГРЕСС:')
        self.stdout.write(f'   Всего игр: {stats["total_games"]}')
        self.stdout.write(f'   Обновлено: {stats["games_with_keywords"]} ({progress:.1f}%)')
        self.stdout.write(f'   Последняя игра ID: {stats["last_game_id"]}')

        if changes['last_game_id'] > 0:
            speed = changes['last_game_id'] / self.interval
            self.stdout.write(f'   Скорость: {speed:.1f} игр/сек')

        # Изменения за последний интервал
        self.stdout.write(f'\n⚡ ИЗМЕНЕНИЯ ЗА ПОСЛЕДНИЕ {self.interval} СЕК:')
        has_changes = False

        if changes['games_with_keywords'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["games_with_keywords"]} игр обновлено'))
            has_changes = True
        if changes['keyword_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["keyword_relations"]} связей ключевых слов'))
            has_changes = True
        if changes['genre_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["genre_relations"]} связей жанров'))
            has_changes = True
        if changes['theme_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["theme_relations"]} связей тем'))
            has_changes = True
        if changes['perspective_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["perspective_relations"]} связей перспектив'))
            has_changes = True
        if changes['developer_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["developer_relations"]} связей разработчиков'))
            has_changes = True
        if changes['mode_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["mode_relations"]} связей режимов игры'))
            has_changes = True
        if changes['engine_relations'] > 0:
            self.stdout.write(self.style.SUCCESS(f'   +{changes["engine_relations"]} связей движков'))
            has_changes = True

        if not has_changes:
            self.stdout.write(self.style.WARNING('   Нет изменений'))

        # Детальная статистика
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

        # Блокировки и проблемы
        self.stdout.write(f'\n🔒 СОСТОЯНИЕ БД:')
        if stats['active_locks'] > 0:
            self.stdout.write(self.style.ERROR(f'   Активных блокировок: {stats["active_locks"]}'))
        else:
            self.stdout.write(self.style.SUCCESS('   ✅ Блокировок нет'))

        if stats['blocked_processes'] > 0:
            self.stdout.write(self.style.ERROR(f'   Заблокированных процессов: {stats["blocked_processes"]}'))

        if stats['long_running_queries'] > 0:
            self.stdout.write(self.style.WARNING(f'   Долгих запросов (>5 сек): {stats["long_running_queries"]}'))

        # Последние ID для отслеживания прогресса
        self.stdout.write(f'\n🔍 ПОСЛЕДНИЕ ID:')
        self.stdout.write(f'   Последняя игра с ключевыми словами: {stats["last_game_id"]}')
        self.stdout.write(f'   Последняя связь ключевого слова: {stats["last_keyword_relation_id"]}')

        self.stdout.write('\n' + '=' * 100)
        self.stdout.write(self.style.WARNING('Нажмите Ctrl+C для выхода'))
        self.stdout.flush()