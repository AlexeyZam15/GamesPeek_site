# FILE: game_cache.py
# PATH: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\management\commands\load_igdb\game_cache.py

from django.core.cache import cache
import hashlib


class GameCacheManager:
    """Менеджер кэша для проверенных игр с поддержкой файлового кэша"""

    CACHE_PREFIX = "games_checked_"
    CACHE_TIMEOUT = 60 * 60 * 24 * 7  # 7 дней

    # Специальный ключ для хранения множества всех ID в файловом кэше
    _SET_KEY = "games_checked_set"

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

        # Также добавляем ID в специальное множество для совместимости с файловым кэшем
        cls._add_id_to_set(igdb_id)

    @classmethod
    def _add_id_to_set(cls, igdb_id):
        """Добавляет ID в множество проверенных игр (для совместимости с файловым кэшем)"""
        try:
            checked_set = cache.get(cls._SET_KEY, set())
            if isinstance(checked_set, set):
                checked_set.add(igdb_id)
                cache.set(cls._SET_KEY, checked_set, cls.CACHE_TIMEOUT)
        except Exception:
            pass  # Игнорируем ошибки при работе с множеством

    @classmethod
    def get_checked_count(cls):
        """Получает количество проверенных игр"""
        try:
            # Сначала пробуем получить через keys (для Redis и подобных)
            try:
                keys = cache.keys(f"{cls.CACHE_PREFIX}*")
                if keys is not None:
                    return len(keys)
            except:
                pass

            # Fallback: используем сохраненное множество
            checked_set = cache.get(cls._SET_KEY, set())
            if isinstance(checked_set, set):
                return len(checked_set)
            return 0
        except Exception as e:
            # Если ничего не работает, возвращаем 0
            return 0

    @classmethod
    def clear_cache(cls):
        """Очищает кэш"""
        cleared_count = 0

        try:
            # Сначала пробуем получить через keys (для Redis и подобных)
            try:
                keys = cache.keys(f"{cls.CACHE_PREFIX}*")
                if keys:
                    cache.delete_many(keys)
                    cleared_count = len(keys)
            except:
                pass

            # Очищаем множество
            cache.delete(cls._SET_KEY)

            # Если keys не сработал, но множество было, считаем его размер
            if cleared_count == 0:
                checked_set = cache.get(cls._SET_KEY, set())
                if isinstance(checked_set, set):
                    cleared_count = len(checked_set)

            return cleared_count
        except Exception as e:
            try:
                # Последняя попытка - очистить всё
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
            # Пробуем использовать get_many
            cache_keys = {}
            for igdb_id in igdb_ids:
                if igdb_id:
                    cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
                    cache_keys[cache_key] = igdb_id

            cached_values = cache.get_many(cache_keys.keys())

            for cache_key, igdb_id in cache_keys.items():
                result[igdb_id] = cached_values.get(cache_key) is not None

            # Для тех ID, что не нашлись в get_many, проверяем через множество
            missing_ids = [igdb_id for igdb_id in igdb_ids if igdb_id and not result.get(igdb_id, False)]
            if missing_ids:
                checked_set = cache.get(cls._SET_KEY, set())
                if isinstance(checked_set, set):
                    for igdb_id in missing_ids:
                        if igdb_id in checked_set:
                            result[igdb_id] = True
                            # Восстанавливаем отдельный ключ для будущих get_many
                            cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
                            cache.set(cache_key, True, cls.CACHE_TIMEOUT)

        except Exception as e:
            # Fallback: проверяем каждый ID отдельно
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
            # Используем set_many для отдельных ключей
            cache_timeout = cls.CACHE_TIMEOUT
            cache_data = {}

            valid_ids = []
            for igdb_id in igdb_ids:
                if igdb_id:
                    cache_key = f"{cls.CACHE_PREFIX}{igdb_id}"
                    cache_data[cache_key] = True
                    valid_ids.append(igdb_id)

            if cache_data:
                cache.set_many(cache_data, cache_timeout)

                # Также добавляем все ID в множество
                checked_set = cache.get(cls._SET_KEY, set())
                if isinstance(checked_set, set):
                    checked_set.update(valid_ids)
                    cache.set(cls._SET_KEY, checked_set, cache_timeout)

        except Exception:
            # Fallback: помечаем каждый ID отдельно
            for igdb_id in igdb_ids:
                if igdb_id:
                    cls.mark_game_checked(igdb_id)

    @classmethod
    def get_all_checked_ids(cls):
        """Возвращает все проверенные ID (для отладки)"""
        try:
            # Пробуем через keys
            try:
                keys = cache.keys(f"{cls.CACHE_PREFIX}*")
                if keys:
                    return {int(key.replace(cls.CACHE_PREFIX, '')) for key in keys if
                            key.replace(cls.CACHE_PREFIX, '').isdigit()}
            except:
                pass

            # Fallback: используем множество
            checked_set = cache.get(cls._SET_KEY, set())
            if isinstance(checked_set, set):
                return checked_set
            return set()
        except:
            return set()