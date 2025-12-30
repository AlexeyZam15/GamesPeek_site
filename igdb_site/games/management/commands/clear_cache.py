"""
Команда для очистки всех кэшей системы.
Использование: python manage.py clear_cache [опции]
"""

from django.core.management.base import BaseCommand
from django.core.cache import cache
from django.db import connection
import time
from games.similarity import GameSimilarity


class Command(BaseCommand):
    help = 'Очищает все кэши системы: Django кэш, кэш алгоритма схожести, батчинг кэши'

    def add_arguments(self, parser):
        parser.add_argument(
            '--type',
            type=str,
            choices=['all', 'similarity', 'database', 'template', 'views'],
            default='all',
            help='Тип кэша для очистки (all, similarity, database, template, views)'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод информации'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительная очистка без подтверждения'
        )

    def handle(self, *args, **options):
        cache_type = options['type']
        verbose = options['verbose']
        force = options['force']

        if not force and cache_type == 'all':
            self.stdout.write(self.style.WARNING('⚠️  ВНИМАНИЕ: Вы собираетесь очистить ВСЕ кэши системы!'))
            confirm = input('Продолжить? (yes/no): ')
            if confirm.lower() != 'yes':
                self.stdout.write(self.style.ERROR('Отменено пользователем'))
                return

        start_time = time.time()
        cleared_count = 0

        self.stdout.write(self.style.SUCCESS('🔄 Начинаю очистку кэшей...'))

        # 1. Очистка Django кэша
        if cache_type in ['all', 'database', 'views']:
            try:
                cache.clear()
                cleared_count += 1
                if verbose:
                    self.stdout.write(self.style.SUCCESS('✅ Очищен Django кэш'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Ошибка очистки Django кэша: {e}'))

        # 2. Очистка кэша алгоритма схожести
        if cache_type in ['all', 'similarity']:
            try:
                similarity_engine = GameSimilarity()
                similarity_engine.clear_cache()
                cleared_count += 1
                if verbose:
                    self.stdout.write(self.style.SUCCESS('✅ Очищен кэш алгоритма схожести'))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f'❌ Ошибка очистки кэша алгоритма схожести: {e}'))

        # 3. Очистка кэша шаблонов (если используется)
        if cache_type in ['all', 'template']:
            try:
                from django.template.loaders.cached import Loader
                Loader.reset()
                cleared_count += 1
                if verbose:
                    self.stdout.write(self.style.SUCCESS('✅ Очищен кэш шаблонов'))
            except Exception as e:
                if verbose:
                    self.stdout.write(self.style.WARNING(f'⚠️  Кэш шаблонов не очищен: {e}'))

        # 4. Очистка Prepared Statement кэша PostgreSQL
        if cache_type in ['all', 'database']:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("DISCARD ALL")
                cleared_count += 1
                if verbose:
                    self.stdout.write(self.style.SUCCESS('✅ Очищен Prepared Statements кэш БД'))
            except Exception as e:
                if verbose:
                    self.stdout.write(self.style.WARNING(f'⚠️  Не удалось очистить Prepared Statements: {e}'))

        # 5. Очистка специальных кэшей из views.py
        if cache_type in ['all', 'views']:
            try:
                # Очистка кэша фильтров
                cache.delete('game_list_filters_data_v3')
                cache.delete('genres_list')

                # Очистка LRU кэшей
                from games.views import get_filter_data
                get_filter_data.cache_clear()

                from games.views import _cached_string_to_int_list
                _cached_string_to_int_list.cache_clear()

                cleared_count += 3  # Примерное количество
                if verbose:
                    self.stdout.write(self.style.SUCCESS('✅ Очищены кэши views.py'))
            except Exception as e:
                if verbose:
                    self.stdout.write(self.style.WARNING(f'⚠️  Ошибка очистки кэшей views: {e}'))

        end_time = time.time()
        elapsed_time = end_time - start_time

        # Вывод статистики
        self.stdout.write('\n' + '=' * 50)
        self.stdout.write(self.style.SUCCESS('🎯 РЕЗУЛЬТАТЫ ОЧИСТКИ КЭША:'))
        self.stdout.write(f'• Тип очистки: {cache_type.upper()}')
        self.stdout.write(f'• Очищено категорий кэша: {cleared_count}')
        self.stdout.write(f'• Время выполнения: {elapsed_time:.2f} сек')

        if cache_type == 'all':
            self.stdout.write(self.style.WARNING('\n⚠️  ПРИМЕЧАНИЯ:'))
            self.stdout.write('• Первый запрос после очистки может быть медленнее')
            self.stdout.write('• Кэш алгоритма схожести будет пересчитан при следующем поиске')
            self.stdout.write('• Для пересчета схожести закройте вкладки браузера и обновите страницу')

        self.stdout.write(self.style.SUCCESS('\n✅ Очистка кэша завершена!'))

        # Рекомендации
        self.stdout.write('\n📋 РЕКОМЕНДАЦИИ ПО ИСПОЛЬЗОВАНИЮ:')
        self.stdout.write('python manage.py clear_cache --type=similarity  # Только кэш схожести')
        self.stdout.write('python manage.py clear_cache --type=database   # Только кэш БД')
        self.stdout.write('python manage.py clear_cache --type=all --verbose  # Подробная очистка всего')
        self.stdout.write('python manage.py clear_cache --type=all --force    # Без подтверждения')