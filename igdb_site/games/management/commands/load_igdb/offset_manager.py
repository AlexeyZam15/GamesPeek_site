# FILE: offset_manager.py
# PATH: P:\Users\Alexey\Desktop\igdb_site\igdb_site\games\management\commands\load_igdb\offset_manager.py

import os
import json
import hashlib
from django.conf import settings
from collections import OrderedDict


class OffsetManager:
    """Менеджер для сохранения и загрузки offset с отдельными файлами"""

    OFFSET_DIR = os.path.join(settings.BASE_DIR, 'offset_data')
    OFFSET_FILE_PREFIX = 'offset_'
    CACHE = {}  # Кэш для быстрого доступа

    @classmethod
    def _ensure_offset_dir(cls):
        """Создает директорию для offset файлов если ее нет"""
        if not os.path.exists(cls.OFFSET_DIR):
            os.makedirs(cls.OFFSET_DIR, exist_ok=True)

    @classmethod
    def _normalize_params(cls, params_dict):
        """Нормализует параметры для создания стабильного ключа"""
        # Сортируем ключи в алфавитном порядке
        sorted_items = sorted(params_dict.items())

        # Преобразуем в упорядоченный словарь
        normalized = OrderedDict()
        for key, value in sorted_items:
            # Преобразуем None в пустую строку
            if value is None:
                normalized[key] = ''
            # Для строк удаляем пробелы
            elif isinstance(value, str):
                normalized[key] = value.strip()
            else:
                normalized[key] = str(value)

        return normalized

    @classmethod
    def _generate_params_hash(cls, params_dict):
        """Генерирует хэш из параметров"""
        normalized = cls._normalize_params(params_dict)

        # Создаем строку для хэширования
        param_string = ""
        for key, value in normalized.items():
            param_string += f"{key}={value}|"

        # Удаляем последний разделитель
        param_string = param_string.rstrip('|')

        # Создаем MD5 хэш
        return hashlib.md5(param_string.encode()).hexdigest()[:12]

    @classmethod
    def _get_offset_filename(cls, params_dict):
        """Получает имя файла для сохранения offset"""
        cls._ensure_offset_dir()

        # Генерируем хэш параметров
        params_hash = cls._generate_params_hash(params_dict)

        # Создаем удобочитаемую часть имени
        readable_part = ""
        if params_dict.get('game_modes'):
            mode = params_dict['game_modes'].split(',')[0][:20]
            readable_part = f"_mode_{mode}"
        elif params_dict.get('game_names'):
            name = params_dict['game_names'].split(',')[0][:20]
            readable_part = f"_name_{name}"
        elif params_dict.get('genres'):
            genre = params_dict['genres'].split(',')[0][:20]
            readable_part = f"_gen_{genre}"
        elif params_dict.get('description_contains'):
            desc = params_dict['description_contains'][:20]
            readable_part = f"_desc_{desc}"
        elif params_dict.get('keywords'):
            keyword = params_dict['keywords'].split(',')[0][:20]
            readable_part = f"_kw_{keyword}"
        else:
            readable_part = "_popular"

        # Очищаем от недопустимых символов в имени файла
        import re
        readable_part = re.sub(r'[^\w\-_]', '_', readable_part)

        # Формируем имя файла
        filename = f"{cls.OFFSET_FILE_PREFIX}{params_hash}{readable_part}.json"
        return os.path.join(cls.OFFSET_DIR, filename)

    @classmethod
    def _get_cache_key(cls, params_dict):
        """Получает ключ для кэша"""
        return cls._generate_params_hash(params_dict)

    @classmethod
    def save_offset(cls, params_dict, offset):
        """Сохраняет offset для конкретного набора параметров"""
        try:
            filename = cls._get_offset_filename(params_dict)

            # Данные для сохранения
            offset_data = {
                'params': cls._normalize_params(params_dict),
                'offset': offset,
                'timestamp': time.time()
            }

            # Сохраняем в файл
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(offset_data, f, indent=2, ensure_ascii=False)

            # Обновляем кэш
            cache_key = cls._get_cache_key(params_dict)
            cls.CACHE[cache_key] = offset_data

            if os.environ.get('DEBUG', False):
                print(f"✅ Offset сохранен в {os.path.basename(filename)}: {offset}")

            return True

        except Exception as e:
            if os.environ.get('DEBUG', False):
                print(f"❌ Error saving offset: {e}")
            return False

    @classmethod
    def load_offset(cls, params_dict):
        """Загружает offset для конкретного набора параметров"""
        try:
            # Проверяем кэш
            cache_key = cls._get_cache_key(params_dict)
            if cache_key in cls.CACHE:
                return cls.CACHE[cache_key].get('offset')

            filename = cls._get_offset_filename(params_dict)

            if not os.path.exists(filename):
                return None

            with open(filename, 'r', encoding='utf-8') as f:
                offset_data = json.load(f)

            # Обновляем кэш
            cls.CACHE[cache_key] = offset_data

            return offset_data.get('offset')

        except Exception:
            return None

    @classmethod
    def clear_offset(cls, params_dict=None):
        """Очищает offset для конкретных параметров или все offset"""
        try:
            cls._ensure_offset_dir()

            if params_dict:
                # Удаляем конкретный offset файл
                filename = cls._get_offset_filename(params_dict)
                if os.path.exists(filename):
                    os.remove(filename)
                    # Удаляем из кэша
                    cache_key = cls._get_cache_key(params_dict)
                    cls.CACHE.pop(cache_key, None)
                    return True
                return False
            else:
                # Очищаем всю директорию
                deleted_count = 0
                for filename in os.listdir(cls.OFFSET_DIR):
                    if filename.startswith(cls.OFFSET_FILE_PREFIX) and filename.endswith('.json'):
                        filepath = os.path.join(cls.OFFSET_DIR, filename)
                        os.remove(filepath)
                        deleted_count += 1

                # Очищаем кэш
                cls.CACHE.clear()

                if os.environ.get('DEBUG', False):
                    print(f"🗑️  Удалено offset файлов: {deleted_count}")

                return deleted_count > 0

        except Exception:
            return False

    @classmethod
    def list_offset_files(cls):
        """Список всех offset файлов"""
        try:
            cls._ensure_offset_dir()
            offset_files = []

            for filename in os.listdir(cls.OFFSET_DIR):
                if filename.startswith(cls.OFFSET_FILE_PREFIX) and filename.endswith('.json'):
                    filepath = os.path.join(cls.OFFSET_DIR, filename)
                    try:
                        with open(filepath, 'r', encoding='utf-8') as f:
                            data = json.load(f)

                        offset_files.append({
                            'filename': filename,
                            'params': data.get('params', {}),
                            'offset': data.get('offset', 0),
                            'size': os.path.getsize(filepath),
                            'modified': os.path.getmtime(filepath)
                        })
                    except:
                        continue

            return offset_files

        except Exception:
            return []

    @classmethod
    def get_offset_info(cls, params_dict):
        """Получает информацию о сохраненном offset"""
        try:
            filename = cls._get_offset_filename(params_dict)

            if not os.path.exists(filename):
                return {'exists': False, 'filename': os.path.basename(filename)}

            with open(filename, 'r', encoding='utf-8') as f:
                data = json.load(f)

            return {
                'exists': True,
                'filename': os.path.basename(filename),
                'params': data.get('params', {}),
                'offset': data.get('offset', 0),
                'size': os.path.getsize(filename)
            }

        except Exception as e:
            return {'error': str(e)}


# Импортируем time здесь, чтобы избежать ошибки
import time