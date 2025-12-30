"""
Команда для проверки статуса кэша.
Использование: python manage.py cache_status
"""

from django.core.management.base import BaseCommand
from django.core.cache import cache
from django.db import connection
import json


class Command(BaseCommand):
    help = 'Показывает статус и статистику кэшей системы'

    def add_arguments(self, parser):
        parser.add_argument(
            '--detailed',
            action='store_true',
            help='Подробная информация о кэшах'
        )

    def handle(self, *args, **options):
        detailed = options['detailed']

        self.stdout.write(self.style.SUCCESS('📊 СТАТУС КЭШЕЙ СИСТЕМЫ'))
        self.stdout.write('=' * 50)

        # 1. Django кэш
        self.stdout.write('\n1. 🗄️  DJANGO КЭШ:')
        try:
            # Простой тест кэша
            test_key = 'cache_test_key'
            cache.set(test_key, 'test_value', 60)
            test_result = cache.get(test_key)

            if test_result == 'test_value':
                self.stdout.write(self.style.SUCCESS('   ✅ Работает корректно'))
            else:
                self.stdout.write(self.style.ERROR('   ❌ Проблемы с кэшем'))

            cache.delete(test_key)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

        # 2. Кэш алгоритма схожести
        self.stdout.write('\n2. 🔍 КЭШ АЛГОРИТМА СХОЖЕСТИ:')
        try:
            from games.similarity import GameSimilarity
            similarity_engine = GameSimilarity()

            cache_size = len(getattr(similarity_engine, '_similarity_cache', {}))
            game_data_cache_size = len(getattr(similarity_engine, '_game_data_cache', {}))

            self.stdout.write(f'   • Кэш схожести: {cache_size} записей')
            self.stdout.write(f'   • Кэш данных игр: {game_data_cache_size} записей')

            if cache_size == 0 and game_data_cache_size == 0:
                self.stdout.write(self.style.WARNING('   ⚠️  Кэш пуст (возможно, уже очищен)'))
            else:
                self.stdout.write(self.style.SUCCESS('   ✅ Есть кэшированные данные'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

        # 3. Кэш фильтров
        self.stdout.write('\n3. 🎛️  КЭШ ФИЛЬТРОВ:')
        try:
            filters_cache = cache.get('game_list_filters_data_v3')
            genres_cache = cache.get('genres_list')

            if filters_cache:
                platforms_count = len(filters_cache.get('platforms', []))
                keywords_count = len(filters_cache.get('popular_keywords', []))
                self.stdout.write(f'   • Данные фильтров: {platforms_count} платформ, {keywords_count} ключевых слов')
            else:
                self.stdout.write(self.style.WARNING('   ⚠️  Кэш фильтров пуст'))

            if genres_cache:
                self.stdout.write(f'   • Список жанров: {len(genres_cache)} жанров')
            else:
                self.stdout.write(self.style.WARNING('   ⚠️  Кэш жанров пуст'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

        # 4. LRU кэши views.py
        self.stdout.write('\n4. 🔄 LRU КЭШИ VIEWS:')
        try:
            from games.views import get_filter_data, _cached_string_to_int_list

            filter_cache_info = get_filter_data.cache_info()
            string_cache_info = _cached_string_to_int_list.cache_info()

            self.stdout.write(f'   • get_filter_data: hits={filter_cache_info.hits}, misses={filter_cache_info.misses}')
            self.stdout.write(
                f'   • string_to_int_list: hits={string_cache_info.hits}, misses={string_cache_info.misses}')

            if filter_cache_info.hits > 0:
                hit_rate = filter_cache_info.hits / (filter_cache_info.hits + filter_cache_info.misses) * 100
                self.stdout.write(f'   • Эффективность кэша: {hit_rate:.1f}% попаданий')
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {e}'))

        # 5. Подробная информация (опционально)
        if detailed:
            self.stdout.write('\n' + '=' * 50)
            self.stdout.write(self.style.SUCCESS('🔍 ПОДРОБНАЯ ИНФОРМАЦИЯ:'))

            # Проверка ключей в кэше
            try:
                # Попробуем найти ключи по паттерну
                from django.core.cache.backends.locmem import LocMemCache
                if isinstance(cache, LocMemCache):
                    cache_data = cache._cache
                    similarity_keys = [k for k in cache_data.keys() if 'similar' in str(k).lower()]
                    game_keys = [k for k in cache_data.keys() if 'game' in str(k).lower()]

                    self.stdout.write(f'   • Ключей "similar": {len(similarity_keys)}')
                    self.stdout.write(f'   • Ключей "game": {len(game_keys)}')

                    if similarity_keys:
                        self.stdout.write('   • Примеры ключей схожести:')
                        for key in list(similarity_keys)[:3]:
                            self.stdout.write(f'     - {key}')
            except:
                pass

        self.stdout.write('\n' + '=' * 50)
        self.stdout.write(self.style.SUCCESS('✅ Проверка завершена'))
        self.stdout.write('\n💡 РЕКОМЕНДАЦИИ:')
        self.stdout.write('• Для очистки всех кэшей: python manage.py clear_cache')
        self.stdout.write('• Для очистки только кэша схожести: python manage.py clear_cache --type=similarity')