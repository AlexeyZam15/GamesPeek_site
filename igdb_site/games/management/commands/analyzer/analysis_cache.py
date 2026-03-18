# games/management/commands/analyzer/analysis_cache.py
"""
Кэширование результатов анализа игр на диске
"""

import os
import hashlib
import pickle
import tempfile
import time
from typing import Optional, Dict, Any
import threading


class AnalysisCache:
    """Кэш результатов анализа на диске с поддержкой многопоточности"""

    def __init__(self, cache_dir: Optional[str] = None, max_memory_entries: int = 1000):
        """
        Инициализирует кэш

        Args:
            cache_dir: Директория для хранения кэша
            max_memory_entries: Максимальное количество записей в памяти
        """
        if cache_dir is None:
            cache_dir = os.path.join(tempfile.gettempdir(), 'igdb_analysis_cache')

        self.cache_dir = cache_dir
        self.max_memory_entries = max_memory_entries
        self.memory_cache: Dict[str, Any] = {}
        self.memory_cache_times: Dict[str, float] = {}  # Для LRU
        self.hits = 0
        self.misses = 0
        self._lock = threading.RLock()

        # Создаем директорию если её нет
        os.makedirs(self.cache_dir, exist_ok=True)

    def _get_cache_key(self, text: str, keywords_mode: bool, exclude_existing: bool) -> str:
        """
        Создает ключ кэша из текста и параметров

        Args:
            text: Текст анализа
            keywords_mode: Режим ключевых слов
            exclude_existing: Исключать существующие

        Returns:
            Уникальный ключ для кэша
        """
        # Берем хеш от текста (первые 1000 символов достаточно для уникальности)
        text_sample = text[:1000] if len(text) > 1000 else text
        text_hash = hashlib.md5(text_sample.encode('utf-8')).hexdigest()

        # Добавляем длину текста для дополнительной уникальности
        text_len = len(text)

        mode = 'kw' if keywords_mode else 'cr'
        excl = '1' if exclude_existing else '0'

        return f"{text_hash}_{text_len}_{mode}_{excl}"

    def _get_cache_path(self, cache_key: str) -> str:
        """Возвращает путь к файлу кэша"""
        return os.path.join(self.cache_dir, f"{cache_key}.pkl")

    def _prune_memory_cache(self):
        """Удаляет старые записи из memory cache (LRU)"""
        if len(self.memory_cache) <= self.max_memory_entries:
            return

        # Сортируем по времени доступа и удаляем самые старые
        sorted_keys = sorted(
            self.memory_cache_times.keys(),
            key=lambda k: self.memory_cache_times[k]
        )

        # Удаляем 20% самых старых записей
        to_remove = int(self.max_memory_entries * 0.2)
        for key in sorted_keys[:to_remove]:
            if key in self.memory_cache:
                del self.memory_cache[key]
            if key in self.memory_cache_times:
                del self.memory_cache_times[key]

    def get(self, text: str, keywords_mode: bool, exclude_existing: bool) -> Optional[Dict]:
        """
        Получает результат из кэша

        Args:
            text: Текст анализа
            keywords_mode: Режим ключевых слов
            exclude_existing: Исключать существующие

        Returns:
            Результат анализа или None
        """
        cache_key = self._get_cache_key(text, keywords_mode, exclude_existing)

        with self._lock:
            # Проверяем в памяти
            if cache_key in self.memory_cache:
                self.hits += 1
                # Обновляем время доступа
                self.memory_cache_times[cache_key] = time.time()
                return self.memory_cache[cache_key]

            # Проверяем на диске
            cache_path = self._get_cache_path(cache_key)
            if os.path.exists(cache_path):
                try:
                    with open(cache_path, 'rb') as f:
                        result = pickle.load(f)

                    # Сохраняем в память
                    self.memory_cache[cache_key] = result
                    self.memory_cache_times[cache_key] = time.time()
                    self._prune_memory_cache()

                    self.hits += 1
                    return result
                except Exception as e:
                    # При ошибке чтения удаляем битый файл
                    try:
                        os.remove(cache_path)
                    except:
                        pass

            self.misses += 1
            return None

    def set(self, text: str, keywords_mode: bool, exclude_existing: bool, result: Dict):
        """
        Сохраняет результат в кэш

        Args:
            text: Текст анализа
            keywords_mode: Режим ключевых слов
            exclude_existing: Исключать существующие
            result: Результат анализа для сохранения
        """
        cache_key = self._get_cache_key(text, keywords_mode, exclude_existing)

        with self._lock:
            # Сохраняем в памяти
            self.memory_cache[cache_key] = result
            self.memory_cache_times[cache_key] = time.time()
            self._prune_memory_cache()

            # Сохраняем на диск (в отдельном потоке чтобы не тормозить)
            def _save_to_disk():
                cache_path = self._get_cache_path(cache_key)
                try:
                    with open(cache_path, 'wb') as f:
                        pickle.dump(result, f)
                except Exception:
                    pass

            thread = threading.Thread(target=_save_to_disk)
            thread.daemon = True
            thread.start()

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику кэша"""
        with self._lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total * 100) if total > 0 else 0

            # Считаем файлы на диске
            try:
                disk_files = [f for f in os.listdir(self.cache_dir) if f.endswith('.pkl')]
                disk_entries = len(disk_files)
                disk_size = sum(os.path.getsize(os.path.join(self.cache_dir, f)) for f in disk_files)
            except:
                disk_entries = 0
                disk_size = 0

            return {
                'hits': self.hits,
                'misses': self.misses,
                'total_requests': total,
                'hit_rate': f"{hit_rate:.1f}%",
                'memory_entries': len(self.memory_cache),
                'disk_entries': disk_entries,
                'disk_size_mb': disk_size / (1024 * 1024),
                'cache_dir': self.cache_dir
            }

    def clear(self):
        """Очищает весь кэш"""
        with self._lock:
            self.memory_cache.clear()
            self.memory_cache_times.clear()

            # Удаляем все файлы
            try:
                for f in os.listdir(self.cache_dir):
                    if f.endswith('.pkl'):
                        os.remove(os.path.join(self.cache_dir, f))
            except:
                pass

    def clear_old(self, max_age_days: int = 30):
        """Удаляет старые записи кэша"""
        with self._lock:
            cutoff = time.time() - (max_age_days * 24 * 3600)

            # Очищаем память
            keys_to_remove = [
                k for k, t in self.memory_cache_times.items()
                if t < cutoff
            ]
            for k in keys_to_remove:
                if k in self.memory_cache:
                    del self.memory_cache[k]
                if k in self.memory_cache_times:
                    del self.memory_cache_times[k]

            # Очищаем диск
            try:
                for f in os.listdir(self.cache_dir):
                    if f.endswith('.pkl'):
                        path = os.path.join(self.cache_dir, f)
                        if os.path.getmtime(path) < cutoff:
                            os.remove(path)
            except:
                pass