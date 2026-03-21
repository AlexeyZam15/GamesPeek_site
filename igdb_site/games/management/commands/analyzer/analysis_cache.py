# games/management/commands/analyzer/analysis_cache.py
"""
Кэширование результатов анализа игр - КОМПАКТНАЯ ВЕРСИЯ
Хранит ВСЕ результаты в одном файле, а не в тысячах отдельных
"""

import os
import hashlib
import pickle
import tempfile
import time
import json
from typing import Optional, Dict, Any
import threading


class AnalysisCache:
    """
    Компактный кэш результатов анализа - все данные в одном файле
    Использует единый JSON/бинарный файл вместо тысяч мелких
    """

    def __init__(self, cache_dir: Optional[str] = None, max_memory_entries: int = 1000):
        """
        Инициализирует кэш

        Args:
            cache_dir: Директория для хранения кэша
            max_memory_entries: Максимальное количество записей в памяти
        """
        import sys

        if cache_dir is None:
            cache_dir = os.path.join(tempfile.gettempdir(), 'igdb_analysis_cache')

        self.cache_dir = cache_dir
        self.cache_file = os.path.join(cache_dir, 'analysis_cache.db')
        self.max_memory_entries = max_memory_entries
        self.memory_cache: Dict[str, Any] = {}
        self.memory_cache_times: Dict[str, float] = {}
        self.hits = 0
        self.misses = 0
        self._lock = threading.RLock()
        self._dirty = False  # Флаг, что есть изменения для сохранения

        # Создаем директорию
        try:
            os.makedirs(self.cache_dir, exist_ok=True)
            # Загружаем существующий кэш
            self._load_from_disk()
        except Exception as e:
            sys.stderr.write(f"⚠️ Ошибка инициализации кэша: {e}\n")
            sys.stderr.flush()

    def _get_cache_key(self, text: str, keywords_mode: bool, exclude_existing: bool) -> str:
        """
        Создает компактный ключ кэша
        """
        # Берем хеш от всего текста (16 символов)
        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()[:16]
        mode = 'kw' if keywords_mode else 'cr'
        excl = '1' if exclude_existing else '0'
        return f"{text_hash}_{mode}_{excl}"

    def _load_from_disk(self):
        """Загружает кэш из единого файла"""
        import sys

        if not os.path.exists(self.cache_file):
            sys.stderr.write(f"      Создаем новый файл кэша: {self.cache_file}\n")
            sys.stderr.flush()
            return

        try:
            with open(self.cache_file, 'rb') as f:
                data = pickle.load(f)

            if isinstance(data, dict):
                # Загружаем в память только последние max_memory_entries записей
                sorted_items = sorted(data.items(), key=lambda x: x[1].get('timestamp', 0), reverse=True)
                for key, value in sorted_items[:self.max_memory_entries]:
                    self.memory_cache[key] = value['result']
                    self.memory_cache_times[key] = value.get('timestamp', 0)

                sys.stderr.write(f"      Загружено {len(self.memory_cache)} записей из кэша\n")
                sys.stderr.flush()

        except Exception as e:
            sys.stderr.write(f"      ⚠️ Ошибка загрузки кэша: {e}\n")
            sys.stderr.flush()
            # Создаем новый файл
            self._save_to_disk()

    def _save_to_disk(self):
        """Сохраняет кэш в единый файл"""
        if not self._dirty:
            return

        try:
            # Собираем все данные для сохранения
            data_to_save = {}
            for key in self.memory_cache:
                data_to_save[key] = {
                    'result': self.memory_cache[key],
                    'timestamp': self.memory_cache_times.get(key, time.time())
                }

            # Сохраняем в файл
            with open(self.cache_file, 'wb') as f:
                pickle.dump(data_to_save, f, protocol=pickle.HIGHEST_PROTOCOL)

            self._dirty = False

            # НИКАКОГО ВЫВОДА - полностью убрано

        except Exception as e:
            import sys
            sys.stderr.write(f"\n⚠️ Ошибка сохранения кэша: {e}\n")
            sys.stderr.flush()

    def get(self, text: str, keywords_mode: bool, exclude_existing: bool) -> Optional[Dict]:
        """
        Получает результат из кэша
        """
        if not text:
            return None

        cache_key = self._get_cache_key(text, keywords_mode, exclude_existing)

        with self._lock:
            # Проверяем в памяти
            if cache_key in self.memory_cache:
                self.hits += 1
                self.memory_cache_times[cache_key] = time.time()
                return self.memory_cache[cache_key]

            self.misses += 1
            return None

    def set(self, text: str, keywords_mode: bool, exclude_existing: bool, result: Dict):
        """
        Сохраняет результат в кэш
        """
        if not text:
            return

        cache_key = self._get_cache_key(text, keywords_mode, exclude_existing)

        with self._lock:
            # Сохраняем в памяти
            self.memory_cache[cache_key] = result
            self.memory_cache_times[cache_key] = time.time()
            self._dirty = True

            # LRU: удаляем старые записи если превышен лимит
            if len(self.memory_cache) > self.max_memory_entries:
                sorted_keys = sorted(
                    self.memory_cache_times.keys(),
                    key=lambda k: self.memory_cache_times[k]
                )
                to_remove = int(self.max_memory_entries * 0.2)
                for key in sorted_keys[:to_remove]:
                    if key in self.memory_cache:
                        del self.memory_cache[key]
                    if key in self.memory_cache_times:
                        del self.memory_cache_times[key]

            # Сохраняем на диск синхронно, но без вывода
            if self._dirty:
                self._save_to_disk()

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику кэша"""
        with self._lock:
            total = self.hits + self.misses
            hit_rate = (self.hits / total * 100) if total > 0 else 0

            # Размер файла кэша
            cache_size = 0
            if os.path.exists(self.cache_file):
                cache_size = os.path.getsize(self.cache_file)

            return {
                'hits': self.hits,
                'misses': self.misses,
                'total_requests': total,
                'hit_rate': f"{hit_rate:.1f}%",
                'memory_entries': len(self.memory_cache),
                'cache_file': self.cache_file,
                'cache_file_size_mb': cache_size / (1024 * 1024)
            }

    def clear(self):
        """Очищает весь кэш - БЫСТРОЕ УДАЛЕНИЕ ПАПКИ"""
        import sys
        import shutil

        sys.stderr.write("\n")
        sys.stderr.write("      🧹 Очистка кэша анализа...\n")
        sys.stderr.flush()

        with self._lock:
            self.memory_cache.clear()
            self.memory_cache_times.clear()
            self._dirty = False

            # БЫСТРОЕ УДАЛЕНИЕ - просто удаляем всю папку
            if self.cache_dir and os.path.exists(self.cache_dir):
                try:
                    # Пытаемся удалить всю папку
                    shutil.rmtree(self.cache_dir)
                    sys.stderr.write(f"      🗑️ Удалена папка кэша: {self.cache_dir}\n")
                    sys.stderr.flush()

                    # Создаем новую пустую папку
                    os.makedirs(self.cache_dir, exist_ok=True)
                    sys.stderr.write(f"      ✅ Создана новая пустая папка кэша\n")
                    sys.stderr.flush()

                except Exception as e:
                    sys.stderr.write(f"      ⚠️ Ошибка удаления папки: {e}\n")
                    sys.stderr.flush()
                    # Fallback: удаляем файлы по одному
                    try:
                        for f in os.listdir(self.cache_dir):
                            if f.endswith('.pkl') or f == 'analysis_cache.db':
                                os.remove(os.path.join(self.cache_dir, f))
                        sys.stderr.write(f"      ✅ Удалены файлы в папке\n")
                        sys.stderr.flush()
                    except Exception as e2:
                        sys.stderr.write(f"      ⚠️ Ошибка удаления файлов: {e2}\n")
                        sys.stderr.flush()

        sys.stderr.write("      ✅ AnalysisCache.clear() завершен\n")
        sys.stderr.flush()

    def clear_old(self, max_age_days: int = 30):
        """
        Удаляет старые записи из кэша
        Не используется в компактной версии, так как LRU уже управляет размером
        """
        pass
