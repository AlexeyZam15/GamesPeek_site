# FILE: cache_manager.py
import sqlite3
import json
import os
from pathlib import Path
from datetime import datetime


class CacheManager:
    """Управление кэшем и статистикой"""

    def __init__(self, debug=False):
        self.debug = debug
        self.stats_db = None
        self.init_stats_db()

    def init_stats_db(self):
        """Инициализация БД для статистики"""
        try:
            stats_dir = Path('stats')
            stats_dir.mkdir(exist_ok=True)

            self.stats_db = sqlite3.connect(stats_dir / 'api_stats.db', timeout=10)
            cursor = self.stats_db.cursor()

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS request_stats
                           (
                               date
                               TEXT
                               PRIMARY
                               KEY,
                               total_requests
                               INTEGER
                               DEFAULT
                               0,
                               cache_hits
                               INTEGER
                               DEFAULT
                               0,
                               cache_misses
                               INTEGER
                               DEFAULT
                               0,
                               search_requests
                               INTEGER
                               DEFAULT
                               0,
                               detail_requests
                               INTEGER
                               DEFAULT
                               0,
                               rate_limited
                               INTEGER
                               DEFAULT
                               0,
                               avg_response_time
                               REAL
                               DEFAULT
                               0
                           )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS game_stats
                           (
                               game_hash
                               TEXT
                               PRIMARY
                               KEY,
                               game_name
                               TEXT
                               NOT
                               NULL,
                               found_count
                               INTEGER
                               DEFAULT
                               0,
                               not_found_count
                               INTEGER
                               DEFAULT
                               0,
                               error_count
                               INTEGER
                               DEFAULT
                               0,
                               last_checked
                               TIMESTAMP,
                               first_found
                               TIMESTAMP
                           )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS efficiency_stats
                           (
                               timestamp
                               TEXT
                               PRIMARY
                               KEY,
                               cache_efficiency
                               REAL,
                               requests_saved
                               INTEGER,
                               avg_requests_per_game
                               REAL
                           )
                           ''')

            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS interruption_logs
                           (
                               timestamp
                               TEXT
                               PRIMARY
                               KEY,
                               repeat_num
                               INTEGER,
                               games_processed
                               INTEGER,
                               games_saved
                               INTEGER,
                               reason
                               TEXT,
                               interrupted_at
                               TEXT
                           )
                           ''')

            self.stats_db.commit()

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка инициализации статистики: {e}')
            self.stats_db = None

    def update_stats(self, stat_type, value=1):
        """Обновление статистики"""
        if not self.stats_db:
            return

        try:
            today = datetime.now().strftime('%Y-%m-%d')
            cursor = self.stats_db.cursor()

            cursor.execute('SELECT 1 FROM request_stats WHERE date = ?', (today,))
            exists = cursor.fetchone()

            updates = {
                'cache_hit': 'cache_hits',
                'cache_miss': 'cache_misses',
                'search_request': 'search_requests',
                'detail_request': 'detail_requests',
                'rate_limited': 'rate_limited'
            }

            if exists:
                if stat_type in updates:
                    column = updates[stat_type]
                    cursor.execute(f'''
                        UPDATE request_stats 
                        SET {column} = {column} + ?
                        WHERE date = ?
                    ''', (value, today))

                cursor.execute('''
                               UPDATE request_stats
                               SET total_requests = total_requests + ?
                               WHERE date = ?
                               ''', (value, today))
            else:
                initial_values = {
                    'cache_hits': 1 if stat_type == 'cache_hit' else 0,
                    'cache_misses': 1 if stat_type == 'cache_miss' else 0,
                    'search_requests': 1 if stat_type == 'search_request' else 0,
                    'detail_requests': 1 if stat_type == 'detail_request' else 0,
                    'rate_limited': 1 if stat_type == 'rate_limited' else 0,
                    'total_requests': 1
                }

                cursor.execute('''
                               INSERT INTO request_stats (date, cache_hits, cache_misses, search_requests,
                                                          detail_requests, rate_limited, total_requests)
                               VALUES (?, ?, ?, ?, ?, ?, ?)
                               ''', (today, initial_values['cache_hits'], initial_values['cache_misses'],
                                     initial_values['search_requests'], initial_values['detail_requests'],
                                     initial_values['rate_limited'], initial_values['total_requests']))

            self.stats_db.commit()

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка обновления статистики: {e}')

    def save_efficiency_stats(self, stats, repeat_num):
        """Сохранение статистики эффективности"""
        if not self.stats_db:
            return

        try:
            timestamp = datetime.now().isoformat()
            total_processed = stats.get('total', 0)
            cache_hits = stats.get('cache_hits', 0)

            cache_efficiency = (cache_hits / total_processed * 100) if total_processed > 0 else 0

            requests_without_cache = total_processed * 1.5
            requests_with_cache = (total_processed - cache_hits) * 1.5
            requests_saved = int(requests_without_cache - requests_with_cache)

            cursor = self.stats_db.cursor()
            cursor.execute('''
                           INSERT INTO efficiency_stats (timestamp, cache_efficiency, requests_saved,
                                                         avg_requests_per_game)
                           VALUES (?, ?, ?, ?)
                           ''', (timestamp, cache_efficiency, requests_saved,
                                 requests_with_cache / total_processed if total_processed > 0 else 0))

            self.stats_db.commit()

        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка сохранения статистики эффективности: {e}')

    def log_interruption(self, reason, repeat_num, stats):
        """Логирование прерывания"""
        if not self.stats_db:
            return

        try:
            cursor = self.stats_db.cursor()
            timestamp = datetime.now().isoformat()

            cursor.execute('''
                           INSERT INTO interruption_logs (timestamp, repeat_num, games_processed, games_saved, reason,
                                                          interrupted_at)
                           VALUES (?, ?, ?, ?, ?, ?)
                           ''', (timestamp, repeat_num, stats.get('total', 0),
                                 stats.get('updated', 0), reason, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

            self.stats_db.commit()
        except Exception as e:
            if self.debug:
                print(f'⚠️ Ошибка логирования прерывания: {e}')

    def reset_cache(self):
        """Удаление кэша RAWG API"""
        cache_paths = [
            Path('cache') / 'rawg_cache.db',
            Path('cache') / 'rawg_cache.db-wal',
            Path('cache') / 'rawg_cache.db-shm',
            Path('cache') / 'rawg_cache.db-journal'
        ]

        deleted_count = 0
        for cache_path in cache_paths:
            if cache_path.exists():
                try:
                    os.remove(cache_path)
                    deleted_count += 1
                except Exception as e:
                    print(f'   ⚠️ Не удалось удалить {cache_path}: {e}')

        return deleted_count

    def close(self):
        """Закрывает соединение с БД"""
        if self.stats_db:
            try:
                self.stats_db.close()
            except:
                pass