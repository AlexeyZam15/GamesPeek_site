# FILE: game_cache.py
# PATH: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\management\commands\load_igdb\game_cache.py

from django.core.cache import cache


class GameCacheManager:
    """Менеджер кэша для проверенных игр"""

    CACHE_PREFIX = "games_checked_"
    CACHE_TIMEOUT = 60 * 60 * 24 * 7  # 7 дней

    @classmethod
    def is_game_checked(cls, igdb_id):
        """Проверяет, проверялась ли игра ранее"""
        if not igdb_id:
            return False
        cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
        return cache.get(cache_key) is not None

    @classmethod
    def mark_game_checked(cls, igdb_id):
        """Отмечает игру как проверенную"""
        if not igdb_id:
            return
        cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
        cache.set(cache_key, True, cls.CACHE_TIMEOUT)

    @classmethod
    def get_checked_count(cls):
        """Получает количество проверенных игр"""
        try:
            # Для Redis
            keys = cache.keys(f"{cls.CACHE_PREFIX}*")
            return len(keys) if keys else 0
        except:
            # Для других бэкендов
            return 0

    @classmethod
    def clear_cache(cls):
        """Очищает кэш"""
        try:
            # Для Redis
            keys = cache.keys(f"{cls.CACHE_PREFIX}*")
            if keys:
                cache.delete_many(keys)
                return len(keys)
            return 0
        except:
            try:
                # Для других бэкендов
                cache.clear()
                return True
            except:
                return False

    @classmethod
    def batch_check_games(cls, igdb_ids):
        """Массовая проверка игр в кэше"""
        if not igdb_ids:
            return {}

        result = {}
        try:
            # Создаем ключи кэша
            cache_keys = {}
            for igdb_id in igdb_ids:
                if igdb_id:
                    cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
                    cache_keys[cache_key] = igdb_id

            # Массовое получение из кэша
            cached_values = cache.get_many(cache_keys.keys())

            # Сопоставляем результаты
            for cache_key, igdb_id in cache_keys.items():
                result[igdb_id] = cached_values.get(cache_key) is not None

        except Exception as e:
            # Fallback: по одному
            for igdb_id in igdb_ids:
                if igdb_id:
                    result[igdb_id] = cls.is_game_checked(igdb_id)

        return result

    @classmethod
    def batch_mark_checked(cls, igdb_ids):
        """Массовая пометка игр как проверенных"""
        if not igdb_ids:
            return

        try:
            cache_timeout = cls.CACHE_TIMEOUT
            cache_data = {}

            for igdb_id in igdb_ids:
                if igdb_id:
                    cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
                    cache_data[cache_key] = True

            # Массовая установка
            if cache_data:
                cache.set_many(cache_data, cache_timeout)

        except Exception:
            # Fallback: по одному
            for igdb_id in igdb_ids:
                if igdb_id:
                    cls.mark_game_checked(igdb_id)