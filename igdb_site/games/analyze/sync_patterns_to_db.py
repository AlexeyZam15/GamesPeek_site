# games/analyze/sync_patterns_to_db.py
"""
Автоматическая синхронизация паттернов с базой данных - добавление отсутствующих элементов
"""

from typing import Dict, Set
from django.db import models
from django.utils.text import slugify

from games.models import Genre, Theme, PlayerPerspective, GameMode
from .pattern_manager import PatternManager
from .range_cache import RangeCacheManager


class PatternAutoSyncer:
    """Автоматический синхронизатор паттернов с базой данных"""

    def __init__(self, verbose: bool = False):
        self.verbose = verbose
        self.patterns = PatternManager.get_all_patterns()

    def ensure_all_patterns_in_db(self) -> Dict[str, Dict]:
        """
        Гарантирует, что ВСЕ элементы из паттернов есть в базе данных
        Вызывается автоматически перед анализом

        Returns:
            Статистика по добавленным элементам
        """
        if self.verbose:
            print("=== Начало автоматической проверки паттернов ===")
            print(f"Количество тем в паттернах: {len(self.patterns['themes'])}")

        results = {
            'genres': self._ensure_category_in_db('genres', Genre),
            'themes': self._ensure_category_in_db('themes', Theme),
            'perspectives': self._ensure_category_in_db('perspectives', PlayerPerspective),
            'game_modes': self._ensure_category_in_db('game_modes', GameMode),
        }

        # Выводим статистику только если что-то добавлено
        total_added = sum(stats['added'] for stats in results.values())

        if self.verbose:
            if total_added > 0:
                print(f"\n✅ Автоматически добавлено {total_added} элементов в базу данных:")
                for category, stats in results.items():
                    if stats['added'] > 0:
                        category_name = self._get_category_display_name(category)
                        print(f"   {category_name}: {stats['added']} добавлено")

                        # Для тем покажем какие именно добавлены
                        if category == 'themes' and stats.get('added_names'):
                            print(f"     Добавленные темы: {', '.join(stats['added_names'])}")
            else:
                print("ℹ️ Все элементы уже есть в базе данных.")

            print("=== Проверка паттернов завершена ===")

        return results

    def _ensure_category_in_db(self, category: str, model_class) -> Dict:
        """
        Гарантирует, что все элементы категории есть в базе данных

        Args:
            category: Название категории (genres, themes, etc.)
            model_class: Класс модели Django

        Returns:
            Статистика по добавленным элементам
        """
        pattern_items = set(self.patterns[category].keys())

        # Получаем существующие элементы из базы
        existing_items = set(model_class.objects.values_list('name', flat=True))

        if self.verbose and category == 'themes':
            print(f"\n=== Проверка тем ===")
            print(f"Темы в паттернах ({len(pattern_items)}):")
            for i, theme in enumerate(sorted(pattern_items), 1):
                print(f"  {i:2}. {theme}")

        # Находим элементы, которые есть в паттернах, но нет в базе
        missing_items = pattern_items - existing_items

        if self.verbose and category == 'themes' and missing_items:
            print(f"\nОтсутствующие темы ({len(missing_items)}):")
            for i, theme in enumerate(sorted(missing_items), 1):
                print(f"  {i:2}. {theme}")

        added_count = 0
        added_names = []

        # Добавляем отсутствующие элементы в базу
        for item_name in missing_items:
            try:
                if self.verbose and category == 'themes':
                    print(f"Попытка создать тему: '{item_name}'")

                created = self._create_item_in_db(item_name, model_class)
                if created:
                    added_count += 1
                    added_names.append(item_name)

                    if self.verbose:
                        print(f"✅ Автоматически создан {self._get_category_display_name(category, False)}: {item_name}")
                else:
                    if self.verbose:
                        print(f"⚠️ Не удалось создать {self._get_category_display_name(category, False)}: {item_name}")

            except Exception as e:
                if self.verbose:
                    print(f"❌ Ошибка при создании {self._get_category_display_name(category, False)} {item_name}: {e}")

        result = {
            'total_in_patterns': len(pattern_items),
            'existing_in_db': len(existing_items),
            'missing_in_db': len(missing_items),
            'added': added_count
        }

        if added_names:
            result['added_names'] = added_names

        return result

    def _create_item_in_db(self, name: str, model_class) -> bool:
        """
        Создает элемент в базе данных
        ИСПРАВЛЕНИЕ: Учитываем все поля модели
        """
        try:
            # Проверяем, существует ли уже
            if model_class.objects.filter(name__iexact=name).exists():
                return True

            # Для всех моделей создаем с нужными полями
            from django.db.models import Max

            # Получаем поля модели
            model_fields = [field.name for field in model_class._meta.fields]

            create_kwargs = {'name': name}

            # Добавляем igdb_id если поле существует
            if 'igdb_id' in model_fields:
                try:
                    max_igdb_id = model_class.objects.aggregate(Max('igdb_id'))['igdb_id__max'] or 1000000
                    new_igdb_id = max_igdb_id + 1
                    create_kwargs['igdb_id'] = new_igdb_id
                except Exception as e:
                    if self.verbose:
                        print(f"⚠️ Не удалось получить max igdb_id: {e}")
                    create_kwargs['igdb_id'] = 999999

            # Добавляем cached_usage_count если поле существует
            if 'cached_usage_count' in model_fields:
                create_kwargs['cached_usage_count'] = 0

            # ИСПРАВЛЕНИЕ: Для тем добавляем slug
            if 'slug' in model_fields and model_class.__name__ == 'Theme':
                from django.utils.text import slugify
                create_kwargs['slug'] = slugify(name)

            # Создаем элемент
            created_object = model_class.objects.create(**create_kwargs)

            # После создания элемента обновляем кэш диапазонов
            # Получаем категорию для модели
            category_map = {
                'Genre': 'genres',
                'Theme': 'themes',
                'PlayerPerspective': 'perspectives',
                'GameMode': 'game_modes',
                'Keyword': 'keywords'
            }

            category = category_map.get(model_class.__name__, 'unknown')

            # Помечаем категорию как содержащую новые элементы
            RangeCacheManager.mark_criteria_as_new(category)

            if self.verbose:
                print(f"⚠️ Категория {category} помечена как содержащая новые элементы")
                print(f"✅ Создан элемент {model_class.__name__}: '{name}' (ID: {created_object.id})")

            return True

        except Exception as e:
            if self.verbose:
                print(f"❌ Ошибка при создании элемента {name} ({model_class.__name__}): {e}")
                import traceback
                traceback.print_exc()
            return False

    def _get_category_display_name(self, category: str, capitalize: bool = True) -> str:
        """Возвращает отображаемое имя категории"""
        names = {
            'genres': 'жанр' if not capitalize else 'Жанр',
            'themes': 'тема' if not capitalize else 'Тема',
            'perspectives': 'перспектива' if not capitalize else 'Перспектива',
            'game_modes': 'режим игры' if not capitalize else 'Режим игры'
        }
        return names.get(category, category)

    def get_missing_items_count(self) -> int:
        """Возвращает количество отсутствующих элементов в базе данных"""
        total_missing = 0

        categories = ['genres', 'themes', 'perspectives', 'game_modes']
        model_classes = [Genre, Theme, PlayerPerspective, GameMode]

        for category, model_class in zip(categories, model_classes):
            pattern_items = set(self.patterns[category].keys())
            existing_items = set(model_class.objects.values_list('name', flat=True))
            missing_items = pattern_items - existing_items
            total_missing += len(missing_items)

        return total_missing


# Глобальная функция для использования в других модулях
def ensure_patterns_in_db(verbose: bool = False) -> Dict[str, Dict]:
    """
    Главная функция для гарантии, что все паттерны есть в базе данных

    Args:
        verbose: Подробный вывод

    Returns:
        Статистика синхронизации
    """
    syncer = PatternAutoSyncer(verbose=verbose)
    return syncer.ensure_all_patterns_in_db()
