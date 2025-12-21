# FILE: offset_manager.py
# PATH: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\management\commands\load_igdb\offset_manager.py

import os
import json
import hashlib
from django.conf import settings


class OffsetManager:
    """Менеджер для сохранения и загрузки offset"""

    OFFSET_FILE = os.path.join(settings.BASE_DIR, 'last_offset.json')

    @classmethod
    def get_offset_info(cls, query_key):
        """Получает информацию о сохраненном offset"""
        try:
            if not os.path.exists(cls.OFFSET_FILE):
                return {'exists': False, 'offset': None, 'file': cls.OFFSET_FILE}

            with open(cls.OFFSET_FILE, 'r', encoding='utf-8') as f:
                all_offsets = json.load(f)

            offset = all_offsets.get(query_key)

            return {
                'exists': offset is not None,
                'offset': offset,
                'file': cls.OFFSET_FILE,
                'total_keys': len(all_offsets),
                'has_start_offset': f"{query_key}_start" in all_offsets
            }
        except Exception as e:
            return {'error': str(e), 'file': cls.OFFSET_FILE}

    @classmethod
    def get_query_key(cls, where_clause, **params):
        """Создает уникальный ключ для запроса"""
        # Создаем строку из всех параметров
        key_parts = [where_clause]
        for key, value in sorted(params.items()):
            if value is not None:
                key_parts.append(f"{key}={value}")

        # Хешируем для получения уникального ключа
        key_string = "|".join(str(part) for part in key_parts)
        return hashlib.md5(key_string.encode()).hexdigest()[:16]

    @classmethod
    def save_offset(cls, query_key, offset):
        """Сохраняет offset для конкретного запроса"""
        try:
            # Загружаем текущие offset
            if os.path.exists(cls.OFFSET_FILE):
                with open(cls.OFFSET_FILE, 'r', encoding='utf-8') as f:
                    all_offsets = json.load(f)
            else:
                all_offsets = {}

            # Сохраняем offset для этого запроса
            all_offsets[query_key] = offset

            # Сохраняем в файл
            with open(cls.OFFSET_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_offsets, f, indent=2)

            print(f"✅ Offset сохранен: {query_key} = {offset}")  # Для отладки
            return True
        except Exception as e:
            print(f"❌ Error saving offset: {e}")
            return False

    @classmethod
    def load_offset(cls, query_key):
        """Загружает offset для конкретного запроса"""
        try:
            if not os.path.exists(cls.OFFSET_FILE):
                return None

            with open(cls.OFFSET_FILE, 'r', encoding='utf-8') as f:
                all_offsets = json.load(f)

            return all_offsets.get(query_key)
        except Exception:
            return None

    @classmethod
    def clear_offset(cls, query_key=None):
        """Очищает offset для запроса или все offset"""
        try:
            if not os.path.exists(cls.OFFSET_FILE):
                return True

            with open(cls.OFFSET_FILE, 'r', encoding='utf-8') as f:
                all_offsets = json.load(f)

            if query_key:
                # Удаляем конкретный offset
                if query_key in all_offsets:
                    del all_offsets[query_key]
            else:
                # Очищаем все
                all_offsets = {}

            # Сохраняем изменения
            with open(cls.OFFSET_FILE, 'w', encoding='utf-8') as f:
                json.dump(all_offsets, f, indent=2)

            return True
        except Exception:
            return False