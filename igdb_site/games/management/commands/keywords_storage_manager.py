# games/management/commands/keywords_storage_manager.py
from django.core.management.base import BaseCommand
from games.models import Keyword
from .keywords_storage.data import (
    KEYWORDS_BY_CATEGORY,
    get_all_keywords,
    get_keywords_by_category,
    get_categories
)
import os
from datetime import datetime


class Command(BaseCommand):
    help = 'Управление ключевыми словами из хранилища'

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest='command', help='Команда')

        # Удаление
        delete_parser = subparsers.add_parser('delete', help='Удалить ключевые слова')
        delete_parser.add_argument(
            '--category',
            type=str,
            help='Категория (если не указана - все)'
        )
        delete_parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Только показать что будет удалено'
        )

        # Показать
        show_parser = subparsers.add_parser('show', help='Показать ключевые слова')
        show_parser.add_argument(
            '--category',
            type=str,
            help='Категория'
        )

        # Экспорт
        export_parser = subparsers.add_parser('export', help='Экспортировать в файл')
        export_parser.add_argument(
            '--category',
            type=str,
            help='Категория'
        )
        export_parser.add_argument(
            '--output',
            type=str,
            default='keywords_export.txt',
            help='Файл для экспорта'
        )

    def handle(self, *args, **options):
        command = options.get('command')

        if command == 'delete':
            self.handle_delete(options)
        elif command == 'show':
            self.handle_show(options)
        elif command == 'export':
            self.handle_export(options)
        else:
            self.print_help()

    def handle_delete(self, options):
        """Удаление ключевых слов"""
        category = options.get('category')
        dry_run = options.get('dry_run', False)

        # Создаем папку для логов
        logs_dir = os.path.join('keywords_deletion_logs')
        os.makedirs(logs_dir, exist_ok=True)

        # Создаем имя файла лога с учетом dry-run
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        if dry_run:
            prefix = 'dry_run_'
        else:
            prefix = ''

        if category:
            log_filename = f'{prefix}delete_{category}_{timestamp}.log'
        else:
            log_filename = f'{prefix}delete_all_{timestamp}.log'

        log_path = os.path.join(logs_dir, log_filename)

        if category:
            if category not in KEYWORDS_BY_CATEGORY:
                self.stdout.write(f'Категория "{category}" не найдена')
                self.stdout.write(f'Доступные: {", ".join(get_categories())}')
                return
            keywords_to_delete = get_keywords_by_category(category)
            self.stdout.write(f'Категория: {category}')
        else:
            keywords_to_delete = get_all_keywords()
            self.stdout.write('Все категории')

        if not keywords_to_delete:
            self.stdout.write('Нет ключевых слов для удаления')
            return

        self.stdout.write(f'Найдено ключевых слов: {len(keywords_to_delete)}')

        # Проверяем в БД
        existing_keywords = Keyword.objects.filter(name__in=keywords_to_delete)
        existing_count = existing_keywords.count()

        if existing_count == 0:
            self.stdout.write('Ни одного ключевого слова не найдено в БД')
            return

        self.stdout.write(f'Из них найдено в БД: {existing_count}')

        # Получаем игры связанные с ключевыми словами ДО удаления
        self.stdout.write('\nСбор информации об играх...')

        # Словарь: ключевое слово -> список игр
        keyword_games = {}
        games_info = {}  # ID игры -> информация об игре

        for keyword in existing_keywords:
            # Получаем игры через промежуточную таблицу
            games = keyword.game_set.all().select_related()
            game_list = []

            for game in games:
                # Исправляем получение display значения для game_type
                game_type_display = None
                if game.game_type is not None:
                    # Получаем display значение через choices
                    from games.models import GameTypeEnum
                    game_type_dict = dict(GameTypeEnum.CHOICES)
                    game_type_display = game_type_dict.get(game.game_type, f"Unknown ({game.game_type})")

                games_info[game.id] = {
                    'id': game.id,
                    'name': game.name,
                    'igdb_id': game.igdb_id,
                    'rating': game.rating,
                    'game_type': game_type_display,
                }
                game_list.append(game.id)

            keyword_games[keyword.id] = {
                'keyword_name': keyword.name,
                'keyword_id': keyword.id,
                'game_ids': game_list,
                'game_count': len(game_list),
            }

        # Получаем детальную информацию для лога
        keyword_details = []
        for kw in existing_keywords:
            keyword_details.append({
                'id': kw.id,
                'name': kw.name,
                'usage_count': kw.usage_count,
                'game_count': keyword_games.get(kw.id, {}).get('game_count', 0),
                'category': kw.category.name if kw.category else None,
                'created_at': kw.created_at,
            })

        # Сортируем по количеству использований
        keyword_details.sort(key=lambda x: x['usage_count'], reverse=True)

        # Выводим предварительный список
        self.stdout.write('\nКлючевые слова для удаления:')

        # Выводим столбиками (как в show)
        COLUMNS = 5
        rows = (existing_count + COLUMNS - 1) // COLUMNS

        # Подготовка данных для столбцов
        sorted_names = [kw['name'] for kw in keyword_details]
        sorted_counts = [kw['usage_count'] for kw in keyword_details]

        column_data = []
        for col in range(COLUMNS):
            column_items = []
            for row in range(rows):
                idx = row * COLUMNS + col
                if idx < existing_count:
                    column_items.append(f'{sorted_names[idx]} ({sorted_counts[idx]})')
            if column_items:
                column_data.append(column_items)

        if column_data:
            column_widths = []
            for col_items in column_data:
                max_width = max(len(item) for item in col_items)
                column_widths.append(max_width + 1)

            for row in range(rows):
                line_parts = []
                for col_idx, col_items in enumerate(column_data):
                    if row < len(col_items):
                        item = col_items[row]
                        line_parts.append(item.ljust(column_widths[col_idx]))
                if line_parts:
                    self.stdout.write(' '.join(line_parts).rstrip())

        # Создаем лог файл
        with open(log_path, 'w', encoding='utf-8') as log_file:
            # Заголовок лога
            log_file.write(f'Лог удаления ключевых слов\n')
            log_file.write(f'Дата: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
            log_file.write(f'Категория: {category if category else "Все"}\n')
            if dry_run:
                log_file.write(f'Режим: DRY RUN (тестовый режим, удаление НЕ выполнено)\n')
            else:
                log_file.write(f'Режим: РЕАЛЬНОЕ УДАЛЕНИЕ\n')
            log_file.write(f'Всего ключевых слов для удаления: {len(keywords_to_delete)}\n')
            log_file.write(f'Найдено в БД: {existing_count}\n')
            log_file.write(f'Общее количество использований: {sum(kw["usage_count"] for kw in keyword_details)}\n')
            log_file.write(f'Общее количество связанных игр: {sum(kw["game_count"] for kw in keyword_details)}\n')
            log_file.write(f'Уникальных игр затронуто: {len(games_info)}\n')
            log_file.write('=' * 80 + '\n\n')

            # Детальная информация о каждом ключевом слове
            log_file.write('ДЕТАЛЬНАЯ ИНФОРМАЦИЯ О КЛЮЧЕВЫХ СЛОВАХ:\n')
            log_file.write('=' * 80 + '\n')

            for i, kw in enumerate(keyword_details, 1):
                log_file.write(f'\n{i}. {kw["name"]}\n')
                log_file.write(f'   ID: {kw["id"]}\n')
                log_file.write(f'   Использований: {kw["usage_count"]}\n')
                log_file.write(f'   Связанных игр: {kw["game_count"]}\n')
                log_file.write(f'   Категория: {kw["category"] or "Нет"}\n')
                log_file.write(
                    f'   Дата создания: {kw["created_at"].strftime("%Y-%m-%d %H:%M:%S") if kw["created_at"] else "Нет данных"}\n')

            # Информация об играх
            log_file.write('\n' + '=' * 80 + '\n')
            log_file.write('ИГРЫ, У КОТОРЫХ БУДУТ УДАЛЕНЫ КЛЮЧЕВЫЕ СЛОВА:\n')
            log_file.write('=' * 80 + '\n')

            # Группируем игры по количеству удаляемых ключевых слов
            game_keyword_count = {}
            for keyword_id, data in keyword_games.items():
                for game_id in data['game_ids']:
                    if game_id not in game_keyword_count:
                        game_keyword_count[game_id] = 0
                    game_keyword_count[game_id] += 1

            # Сортируем игры по количеству удаляемых ключевых слов
            sorted_game_ids = sorted(game_keyword_count.keys(),
                                     key=lambda gid: game_keyword_count[gid],
                                     reverse=True)

            for game_id in sorted_game_ids:
                game = games_info[game_id]
                keyword_count = game_keyword_count[game_id]

                log_file.write(f'\nИгра: {game["name"]}\n')
                log_file.write(f'   ID игры: {game["id"]}\n')
                log_file.write(f'   IGDB ID: {game["igdb_id"]}\n')
                log_file.write(f'   Рейтинг: {game["rating"]}\n')
                log_file.write(f'   Тип игры: {game["game_type"]}\n')
                log_file.write(f'   Удаляемых ключевых слов: {keyword_count}\n')

                # Какие именно ключевые слова удаляются у этой игры
                log_file.write('   Удаляемые ключевые слова:\n')
                for keyword_id, data in keyword_games.items():
                    if game_id in data['game_ids']:
                        log_file.write(f'     - {data["keyword_name"]}\n')

            # Статистика
            log_file.write('\n' + '=' * 80 + '\n')
            log_file.write('СТАТИСТИКА:\n')
            log_file.write('=' * 80 + '\n')

            log_file.write(f'\nВсего затронуто игр: {len(games_info)}\n')

            # Распределение по количеству удаляемых ключевых слов
            log_file.write('\nРаспределение игр по количеству удаляемых ключевых слов:\n')
            distribution = {}
            for game_id, count in game_keyword_count.items():
                if count not in distribution:
                    distribution[count] = 0
                distribution[count] += 1

            for count in sorted(distribution.keys(), reverse=True):
                log_file.write(f'   {count} ключ. слов: {distribution[count]} игр\n')

            # Статистика по категориям ключевых слов
            log_file.write('\nСтатистика по категориям ключевых слов:\n')
            categories_stats = {}
            for kw in keyword_details:
                cat = kw['category'] or 'Без категории'
                if cat not in categories_stats:
                    categories_stats[cat] = {'count': 0, 'usage': 0, 'games': 0}
                categories_stats[cat]['count'] += 1
                categories_stats[cat]['usage'] += kw['usage_count']
                categories_stats[cat]['games'] += kw['game_count']

            for cat, stats in sorted(categories_stats.items()):
                log_file.write(f'\n{cat}:\n')
                log_file.write(f'   Ключевых слов: {stats["count"]}\n')
                log_file.write(f'   Использований: {stats["usage"]}\n')
                log_file.write(f'   Связанных игр: {stats["games"]}\n')

        self.stdout.write(f'\nДетальный лог создан: {log_path}')

        if dry_run:
            self.stdout.write(self.style.WARNING(f'\nDRY RUN: {existing_count} ключевых слов НЕ будут удалены'))
            self.stdout.write(f'Затронуто игр: {len(games_info)}')

            # Дописываем в лог для dry-run
            with open(log_path, 'a', encoding='utf-8') as log_file:
                log_file.write('\n' + '=' * 80 + '\n')
                log_file.write('РЕЖИМ DRY RUN - УДАЛЕНИЕ НЕ ВЫПОЛНЕНО\n')
                log_file.write('=' * 80 + '\n')
                log_file.write('Это тестовый запуск. Ключевые слова НЕ были удалены.\n')
                log_file.write(f'Было бы удалено ключевых слов: {existing_count}\n')
                log_file.write(f'Было бы затронуто игр: {len(games_info)}\n')
                log_file.write(f'Время завершения анализа: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')

            return

        # Подтверждение
        confirm = input(f'\nУдалить {existing_count} ключевых слов (затронет {len(games_info)} игр)? (y/N): ')
        if confirm.lower() != 'y':
            self.stdout.write('Отменено')
            return

        # Удаляем
        self.stdout.write('Удаление ключевых слов...')
        deleted_count, deletion_details = existing_keywords.delete()

        # Дописываем в лог результат удаления
        with open(log_path, 'a', encoding='utf-8') as log_file:
            log_file.write('\n' + '=' * 80 + '\n')
            log_file.write('РЕЗУЛЬТАТ УДАЛЕНИЯ:\n')
            log_file.write('=' * 80 + '\n')
            log_file.write(f'Удалено ключевых слов: {deleted_count}\n')
            log_file.write(f'Затронуто игр: {len(games_info)}\n')

            if deletion_details:
                log_file.write('\nДетали удаления:\n')
                for model, count in deletion_details.items():
                    log_file.write(f'  {model}: {count}\n')

            log_file.write(f'\nВремя завершения: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')

        self.stdout.write(self.style.SUCCESS(f'Удалено {deleted_count} ключевых слов'))
        self.stdout.write(f'Затронуто игр: {len(games_info)}')
        self.stdout.write(f'Детальный лог сохранен в: {log_path}')

    def handle_show(self, options):
        """Показать ключевые слова"""
        category = options.get('category')
        COLUMNS = 5  # Константа для количества столбиков

        if category:
            if category not in KEYWORDS_BY_CATEGORY:
                self.stdout.write(f'Категория "{category}" не найдена')
                self.stdout.write(f'Доступные: {", ".join(get_categories())}')
                return

            keywords = get_keywords_by_category(category)

            # Получаем счетчики из БД
            db_keywords = Keyword.objects.filter(name__in=keywords)
            keyword_counts = {kw.name: kw.usage_count for kw in db_keywords}

            # Сортируем ключевые слова по счетчикам (по убыванию)
            sorted_keywords = sorted(
                keywords,
                key=lambda kw: keyword_counts.get(kw, 0),
                reverse=True
            )

            # Выводим статистику
            exists_count = len(keyword_counts)
            total_uses = sum(keyword_counts.values())

            self.stdout.write(f'{category}: {len(keywords)} ключевых слов')
            self.stdout.write(f'Найдено в БД: {exists_count}')
            self.stdout.write(f'Всего использований: {total_uses}\n')

            # Выводим столбиками по COLUMNS с выравниванием
            rows = (len(sorted_keywords) + COLUMNS - 1) // COLUMNS

            # Создаем список для каждого столбца
            column_data = []
            for col in range(COLUMNS):
                column_items = []
                for row in range(rows):
                    idx = row * COLUMNS + col
                    if idx < len(sorted_keywords):
                        kw = sorted_keywords[idx]
                        count = keyword_counts.get(kw, 0)
                        column_items.append(f'{kw} ({count})')
                if column_items:  # Только непустые столбцы
                    column_data.append(column_items)

            # Находим максимальную ширину для каждого столбца
            column_widths = []
            for col_items in column_data:
                max_width = max(len(item) for item in col_items)
                column_widths.append(max_width + 1)  # +1 для отступа

            # Выводим построчно
            for row in range(rows):
                line_parts = []
                for col_idx, col_items in enumerate(column_data):
                    if row < len(col_items):
                        item = col_items[row]
                        line_parts.append(item.ljust(column_widths[col_idx]))
                if line_parts:
                    self.stdout.write(' '.join(line_parts).rstrip())

        else:
            # Для всех категорий
            self.stdout.write('Все категории:')
            for cat_name in get_categories():
                keywords = get_keywords_by_category(cat_name)

                # Получаем счетчики и сортируем
                db_keywords = Keyword.objects.filter(name__in=keywords)
                keyword_counts = {kw.name: kw.usage_count for kw in db_keywords}
                sorted_keywords = sorted(
                    keywords,
                    key=lambda kw: keyword_counts.get(kw, 0),
                    reverse=True
                )
                total_uses = sum(keyword_counts.values())

                # Выводим статистику
                self.stdout.write(f'\n{cat_name}: {len(keywords)} ключевых слов ({total_uses})')

                # Выводим компактно (первые 15)
                if sorted_keywords:
                    SHOW_COUNT = 15
                    SHOW_COLUMNS = 5

                    show_items = sorted_keywords[:SHOW_COUNT]
                    show_rows = (min(SHOW_COUNT, len(show_items)) + SHOW_COLUMNS - 1) // SHOW_COLUMNS

                    # Создаем столбцы
                    show_columns = []
                    for col in range(SHOW_COLUMNS):
                        col_items = []
                        for row in range(show_rows):
                            idx = row * SHOW_COLUMNS + col
                            if idx < len(show_items):
                                kw = show_items[idx]
                                count = keyword_counts.get(kw, 0)
                                col_items.append(f'{kw} ({count})')
                        if col_items:
                            show_columns.append(col_items)

                    # Выравниваем
                    if show_columns:
                        col_widths = []
                        for col_items in show_columns:
                            max_width = max(len(item) for item in col_items)
                            col_widths.append(max_width + 1)

                        for row in range(show_rows):
                            line_parts = []
                            for col_idx, col_items in enumerate(show_columns):
                                if row < len(col_items):
                                    item = col_items[row]
                                    line_parts.append(item.ljust(col_widths[col_idx]))
                            if line_parts:
                                self.stdout.write(' '.join(line_parts))

                        if len(sorted_keywords) > SHOW_COUNT:
                            self.stdout.write(f'... и еще {len(sorted_keywords) - SHOW_COUNT}')

    def handle_export(self, options):
        """Экспорт ключевых слов"""
        category = options.get('category')
        output_file = options.get('output', 'keywords_export.txt')

        if category:
            if category not in KEYWORDS_BY_CATEGORY:
                self.stdout.write(f'Категория "{category}" не найдена')
                self.stdout.write(f'Доступные: {", ".join(get_categories())}')
                return
            keywords = get_keywords_by_category(category)
            self.stdout.write(f'Экспорт категории: {category}')
        else:
            keywords = get_all_keywords()
            self.stdout.write('Экспорт всех категорий')

        if not keywords:
            self.stdout.write('Нет ключевых слов для экспорта')
            return

        # Экспорт через запятую
        with open(output_file, 'w', encoding='utf-8') as f:
            for i in range(0, len(keywords), 50):  # 50 слов на строку
                chunk = keywords[i:i + 50]
                line = ', '.join(chunk)
                f.write(f'{line}\n')

        self.stdout.write(f'Экспортировано {len(keywords)} ключевых слов в {output_file}')

    def print_help(self):
        """Вывод справки"""
        self.stdout.write('\nКоманды:')
        self.stdout.write('  python manage.py keywords_storage_manager show [--category NAME]')
        self.stdout.write('  python manage.py keywords_storage_manager delete [--category NAME] [--dry-run]')
        self.stdout.write('  python manage.py keywords_storage_manager export [--category NAME] [--output FILE]')
        self.stdout.write('\nТекущие категории:')
        for cat in get_categories():
            count = len(get_keywords_by_category(cat))
            self.stdout.write(f'  {cat}: {count} ключевых слов')