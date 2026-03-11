# games/management/commands/backup_game_keywords.py
"""
Команда для управления ключевыми словами игр:
- создание единого бэкапа всех игр (1 файл)
- восстановление из бэкапа
- очистка старых бэкапов
- удаление связей ключевых слов у игр (БЕЗ ОБНОВЛЕНИЯ КЭША)
"""

import json
import os
import glob
from datetime import datetime, timedelta
from django.core.management.base import BaseCommand
from django.core.serializers.json import DjangoJSONEncoder
from django.db.models import Q, Count
from django.db import transaction
from games.models import Game, Keyword


class Command(BaseCommand):
    help = """
    Управление ключевыми словами игр: единый бэкап, восстановление, очистка

    Примеры использования:
        # СОЗДАНИЕ ЕДИНОГО БЭКАПА ВСЕХ ИГР (1 файл)
        python manage.py backup_game_keywords --create-backup
        python manage.py backup_game_keywords --create-backup --min-keywords 5
        python manage.py backup_game_keywords --create-backup --batch-size 1000

        # Восстановление из бэкапа
        python manage.py backup_game_keywords --restore
        python manage.py backup_game_keywords --restore --restore-file backup_20240101_120000.json
        python manage.py backup_game_keywords --restore --game-ids 1,2,3
        python manage.py backup_game_keywords --restore --game-names "Fallout 3,Grand Theft Auto V"

        # Просмотр доступных бэкапов
        python manage.py backup_game_keywords --list
        python manage.py backup_game_keywords --list --verbose

        # ОЧИСТКА старых бэкапов
        python manage.py backup_game_keywords --cleanup --keep 5
        python manage.py backup_game_keywords --cleanup --days 30

        # УДАЛЕНИЕ СВЯЗЕЙ ключевых слов у игр (БЕЗ ОБНОВЛЕНИЯ КЭША)
        python manage.py backup_game_keywords --clear-keywords --game-names "Fallout 3"
        python manage.py backup_game_keywords --clear-keywords --all-games
    """

    def add_arguments(self, parser):
        # ОСНОВНЫЕ РЕЖИМЫ
        parser.add_argument('--create-backup', action='store_true',
                            help='СОЗДАТЬ ЕДИНЫЙ БЭКАП ВСЕХ ИГР')
        parser.add_argument('--restore', action='store_true',
                            help='Восстановить ключевые слова из бэкапа')
        parser.add_argument('--list', action='store_true',
                            help='Показать все доступные бэкапы')
        parser.add_argument('--cleanup', action='store_true',
                            help='Очистить старые бэкапы')
        parser.add_argument('--clear-keywords', action='store_true',
                            help='УДАЛИТЬ связи ключевых слов у игр (БЕЗ ОБНОВЛЕНИЯ КЭША)')

        # ПАРАМЕТРЫ ДЛЯ БЭКАПА
        parser.add_argument('--batch-size', type=int, default=1000,
                            help='Размер батча для обработки (по умолчанию 1000)')
        parser.add_argument('--min-keywords', type=int, default=0,
                            help='Минимальное количество ключевых слов для включения в бэкап')
        parser.add_argument('--output-dir', type=str, default='game_keywords_backups',
                            help='Папка для сохранения бэкапов')
        parser.add_argument('--compress', action='store_true',
                            help='Сжимать бэкап (добавит .gz к имени файла)')

        # ПАРАМЕТРЫ ДЛЯ ВОССТАНОВЛЕНИЯ
        parser.add_argument('--restore-file', type=str,
                            help='Конкретный файл для восстановления')
        parser.add_argument('--game-ids', type=str,
                            help='ID игр через запятую для восстановления')
        parser.add_argument('--game-names', type=str,
                            help='Названия игр через запятую для восстановления')
        parser.add_argument('--all-games', action='store_true',
                            help='Восстановить все игры из бэкапа')
        parser.add_argument('--backup-first', action='store_true',
                            help='Создать бэкап ПЕРЕД восстановлением')

        # ПАРАМЕТРЫ ДЛЯ ОЧИСТКИ
        parser.add_argument('--days', type=int, default=30,
                            help='Удалять бэкапы старше N дней (по умолчанию 30)')
        parser.add_argument('--keep', type=int, default=5,
                            help='Оставить последние N бэкапов (по умолчанию 5)')

        # ОБЩИЕ ПАРАМЕТРЫ
        parser.add_argument('--force', action='store_true',
                            help='Принудительное выполнение без подтверждений')
        parser.add_argument('--verbose', action='store_true',
                            help='Подробный вывод')
        parser.add_argument('--dry-run', action='store_true',
                            help='Показать что будет сделано без изменений')

    def handle(self, *args, **options):
        # Основные параметры
        create_backup = options.get('create_backup')
        restore_mode = options.get('restore')
        list_mode = options.get('list')
        cleanup_mode = options.get('cleanup')
        clear_keywords = options.get('clear_keywords')

        output_dir = options.get('output_dir')
        force = options.get('force')
        verbose = options.get('verbose')
        dry_run = options.get('dry_run')

        # Создаём папку для бэкапов
        os.makedirs(output_dir, exist_ok=True)

        # Проверка: взаимоисключающие режимы
        mode_count = sum([create_backup, restore_mode, cleanup_mode, clear_keywords, list_mode])
        if mode_count > 1:
            self.stderr.write(
                "❌ Укажите только один режим: --create-backup, --restore, --cleanup, --clear-keywords или --list"
            )
            return

        # Режим просмотра бэкапов
        if list_mode:
            self._list_backups(output_dir, verbose)
            return

        # Режим создания единого бэкапа
        if create_backup:
            self._create_single_backup(options)
            return

        # Режим восстановления
        if restore_mode:
            self._restore_from_backup(options)
            return

        # Режим очистки бэкапов
        if cleanup_mode:
            self._cleanup_backups(output_dir, options.get('days'),
                                  options.get('keep'), verbose, force, dry_run)
            return

        # Режим удаления связей ключевых слов (БЕЗ ОБНОВЛЕНИЯ КЭША)
        if clear_keywords:
            self._clear_game_keywords(options)
            return

        # Если ничего не указано, показываем помощь
        self.print_help('manage.py', 'backup_game_keywords')

    def _create_single_backup(self, options):
        """Создаёт ЕДИНЫЙ файл бэкапа для всех игр (хранит только ID ключевых слов)"""

        output_dir = options.get('output_dir')
        batch_size = options.get('batch_size')
        min_keywords = options.get('min_keywords')
        compress = options.get('compress')
        force = options.get('force')
        verbose = options.get('verbose')
        dry_run = options.get('dry_run')

        self.stdout.write("\n📦 СОЗДАНИЕ ЕДИНОГО БЭКАПА ВСЕХ ИГР")
        self.stdout.write("=" * 60)

        # Получаем все игры с ключевыми словами
        games_query = Game.objects.annotate(
            kw_count=Count('keywords')
        ).filter(kw_count__gte=min_keywords).order_by('id')

        total_games = games_query.count()

        if total_games == 0:
            self.stdout.write("❌ Нет игр с ключевыми словами для бэкапа")
            return

        # Формируем имя файла с датой и временем
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = f"games_backup_{timestamp}_{total_games}games.json"
        if compress:
            base_filename += '.gz'

        filepath = os.path.join(output_dir, base_filename)

        self.stdout.write(f"\n📊 Информация о бэкапе:")
        self.stdout.write(f"   📁 Папка: {output_dir}")
        self.stdout.write(f"   📄 Файл: {base_filename}")
        self.stdout.write(f"   🎮 Всего игр: {total_games}")
        self.stdout.write(f"   🔑 Мин. ключевых слов: {min_keywords}")
        self.stdout.write(f"   📦 Размер батча: {batch_size}")
        self.stdout.write(f"   🗜️ Сжатие: {'да' if compress else 'нет'}")

        if not force and not dry_run and self.stdout.isatty():
            estimated_size = total_games * 0.5  # примерно 0.5 KB на игру (только ID)
            self.stdout.write(f"\n📊 Примерный размер: ~{estimated_size // 1024} MB")
            confirm = input(f"\nСоздать бэкап для {total_games} игр? (y/N): ").strip().lower()
            if confirm != 'y':
                self.stdout.write("❌ Операция отменена")
                return

        # Собираем данные по играм
        backup_data = {
            'metadata': {
                'version': '1.0',
                'created_at': datetime.now().isoformat(),
                'total_games': total_games,
                'min_keywords': min_keywords,
                'source': 'django_backup_command'
            },
            'games': {}
        }

        processed = 0
        games_with_keywords = 0
        total_keywords = 0

        self.stdout.write("")

        # Обрабатываем игры батчами для экономии памяти
        for i in range(0, total_games, batch_size):
            batch = games_query[i:i + batch_size]

            for game in batch:
                if dry_run:
                    processed += 1
                    continue

                # Получаем ID ключевых слов игры (используем values_list для скорости)
                keyword_ids = list(game.keywords.values_list('id', flat=True))

                # Сохраняем только ID ключевых слов
                backup_data['games'][str(game.id)] = {
                    'id': game.id,
                    'igdb_id': game.igdb_id,
                    'name': game.name,
                    'keyword_ids': keyword_ids,
                    'keywords_count': len(keyword_ids)
                }

                games_with_keywords += 1
                total_keywords += len(keyword_ids)
                processed += 1

                # Показываем прогресс
                if processed % 100 == 0 or processed == total_games:
                    percent = (processed / total_games) * 100
                    bar_length = 30
                    filled_length = int(bar_length * processed // total_games)
                    bar = '█' * filled_length + '░' * (bar_length - filled_length)

                    self.stdout.write(
                        f"   🔄 Прогресс: |{bar}| {processed}/{total_games} игр ({percent:.1f}%)",
                        ending='\r'
                    )

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"\n🔍 РЕЖИМ ПРОСМОТРА: будет создан бэкап для {total_games} игр"
                )
            )
            return

        self.stdout.write("")  # Новая строка после прогресса

        # Сохраняем бэкап
        try:
            self.stdout.write("\n💾 Сохранение бэкапа...")

            if compress:
                import gzip
                with gzip.open(filepath, 'wt', encoding='utf-8') as f:
                    json.dump(backup_data, f, ensure_ascii=False, indent=2, cls=DjangoJSONEncoder)
            else:
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(backup_data, f, ensure_ascii=False, indent=2, cls=DjangoJSONEncoder)

            file_size = os.path.getsize(filepath) / (1024 * 1024)  # в MB

            self.stdout.write(
                self.style.SUCCESS(
                    f"\n✅ БЭКАП УСПЕШНО СОЗДАН:\n"
                    f"   📄 Файл: {base_filename}\n"
                    f"   💾 Размер: {file_size:.2f} MB\n"
                    f"   🎮 Игр в бэкапе: {games_with_keywords}\n"
                    f"   🔑 Всего ключевых слов: {total_keywords}"
                )
            )

        except Exception as e:
            self.stderr.write(f"❌ Ошибка при сохранении бэкапа: {e}")
            if os.path.exists(filepath):
                os.remove(filepath)

    def _restore_from_backup(self, options):
        """Восстанавливает ключевые слова из единого бэкапа"""

        output_dir = options.get('output_dir')
        restore_file = options.get('restore_file')
        game_ids = options.get('game_ids')
        game_names = options.get('game_names')
        all_games = options.get('all_games')
        backup_first = options.get('backup_first')
        force = options.get('force')
        verbose = options.get('verbose')
        dry_run = options.get('dry_run')

        # Если файл не указан, берём самый свежий
        if not restore_file:
            backup_files = glob.glob(os.path.join(output_dir, "games_backup_*.json*"))
            if not backup_files:
                self.stderr.write(f"❌ Нет файлов бэкапов в папке {output_dir}")
                return

            # Сортируем по времени создания (по убыванию)
            backup_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)
            restore_file = backup_files[0]

            self.stdout.write(f"\n📦 Используется последний бэкап: {os.path.basename(restore_file)}")
        else:
            if not os.path.exists(restore_file):
                restore_file = os.path.join(output_dir, restore_file)
                if not os.path.exists(restore_file):
                    self.stderr.write(f"❌ Файл не найден: {restore_file}")
                    return

        # Загружаем бэкап
        self.stdout.write(f"\n📦 Загрузка бэкапа: {os.path.basename(restore_file)}")

        try:
            if restore_file.endswith('.gz'):
                import gzip
                with gzip.open(restore_file, 'rt', encoding='utf-8') as f:
                    backup_data = json.load(f)
            else:
                with open(restore_file, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
        except Exception as e:
            self.stderr.write(f"❌ Ошибка чтения файла: {e}")
            return

        # Проверяем структуру
        if 'games' not in backup_data:
            self.stderr.write("❌ Неподдерживаемый формат бэкапа")
            return

        metadata = backup_data.get('metadata', {})
        self.stdout.write(f"\n📊 Информация о бэкапе:")
        self.stdout.write(f"   📅 Создан: {metadata.get('created_at', 'Unknown')}")
        self.stdout.write(f"   🎮 Всего игр в бэкапе: {metadata.get('total_games', len(backup_data['games']))}")

        # Определяем игры для восстановления
        games_to_restore = []

        if all_games:
            games_to_restore = list(backup_data['games'].values())
            self.stdout.write(f"   🔄 Режим: восстановление ВСЕХ игр ({len(games_to_restore)})")

        elif game_ids:
            id_list = [id.strip() for id in game_ids.split(',')]
            for game_id in id_list:
                if game_id in backup_data['games']:
                    games_to_restore.append(backup_data['games'][game_id])
                else:
                    self.stdout.write(f"   ⚠️ Игра с ID {game_id} не найдена в бэкапе")

        elif game_names:
            name_list = [name.strip().lower() for name in game_names.split(',')]
            for game_data in backup_data['games'].values():
                game_name = game_data.get('name', '').lower()
                for search_name in name_list:
                    if search_name in game_name:
                        games_to_restore.append(game_data)
                        break

        else:
            # Если ничего не указано, показываем интерактивный выбор
            self._interactive_restore(backup_data, options)
            return

        if not games_to_restore:
            self.stderr.write("❌ Нет игр для восстановления")
            return

        self._perform_restore(games_to_restore, backup_first, force, dry_run, verbose, output_dir)

    def _perform_restore(self, games_to_restore, backup_first, force, dry_run, verbose, output_dir):
        """Выполняет восстановление для списка игр"""

        self.stdout.write(f"\n📊 Будет восстановлено игр: {len(games_to_restore)}")

        if verbose:
            self.stdout.write("\n   Список игр:")
            for game_data in games_to_restore[:20]:
                self.stdout.write(
                    f"      • {game_data['name']} (ID: {game_data['id']}) - {game_data['keywords_count']} ключ. слов")
            if len(games_to_restore) > 20:
                self.stdout.write(f"      • ... и ещё {len(games_to_restore) - 20} игр")

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "\n🔍 РЕЖИМ ПРОСМОТРА - восстановление не выполнено"
                )
            )
            return

        if not force and self.stdout.isatty():
            confirm = input("\nПродолжить восстановление? (y/N): ").strip().lower()
            if confirm != 'y':
                self.stdout.write("❌ Восстановление отменено")
                return

        # Создаём бэкап текущего состояния если нужно
        if backup_first:
            self.stdout.write("\n📦 Создание резервного бэкапа текущего состояния...")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(output_dir, f"pre_restore_backup_{timestamp}.json")

            current_data = {'games': {}}
            for game_data in games_to_restore:
                game_id = game_data['id']
                try:
                    game = Game.objects.get(id=game_id)
                    keyword_ids = list(game.keywords.values_list('id', flat=True))
                    current_data['games'][str(game_id)] = {
                        'id': game_id,
                        'name': game.name,
                        'keyword_ids': keyword_ids,
                        'keywords_count': len(keyword_ids)
                    }
                except Game.DoesNotExist:
                    pass

            with open(backup_file, 'w', encoding='utf-8') as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)

            self.stdout.write(f"   ✅ Создан бэкап: {os.path.basename(backup_file)}")

        # Восстанавливаем
        self.stdout.write("\n🔄 Восстановление...")

        restored = 0
        errors = 0
        total_keywords = 0
        total_games = len(games_to_restore)

        for i, game_data in enumerate(games_to_restore, 1):
            try:
                game_id = game_data['id']
                keyword_ids = game_data['keyword_ids']

                # Получаем игру
                try:
                    game = Game.objects.get(id=game_id)

                    with transaction.atomic():
                        # Очищаем текущие ключевые слова
                        game.keywords.clear()
                        # Добавляем новые по ID
                        if keyword_ids:
                            game.keywords.add(*keyword_ids)

                        # НЕ обновляем кэши!

                    restored += 1
                    total_keywords += len(keyword_ids)

                    # Показываем прогресс
                    if i % 10 == 0 or i == total_games:
                        percent = (i / total_games) * 100
                        bar_length = 30
                        filled_length = int(bar_length * i // total_games)
                        bar = '█' * filled_length + '░' * (bar_length - filled_length)

                        self.stdout.write(
                            f"   🔄 Прогресс: |{bar}| {i}/{total_games} игр ({percent:.1f}%)",
                            ending='\r'
                        )

                except Game.DoesNotExist:
                    self.stderr.write(f"\n   ❌ Игра с ID {game_id} не найдена в БД")
                    errors += 1

            except Exception as e:
                self.stderr.write(f"\n   ❌ Ошибка при восстановлении игры {game_data.get('name', game_id)}: {e}")
                errors += 1

        self.stdout.write("")  # Новая строка после прогресса

        self.stdout.write(
            self.style.SUCCESS(
                f"\n✅ ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО:\n"
                f"   ✅ Восстановлено игр: {restored}\n"
                f"   🔑 Всего ключевых слов: {total_keywords}\n"
                f"   ❌ Ошибок: {errors}"
            )
        )

    def _interactive_restore(self, backup_data, options):
        """Интерактивный режим выбора игр для восстановления"""

        self.stdout.write("\n🎮 Выберите игры для восстановления:")

        games_list = list(backup_data['games'].values())
        games_list.sort(key=lambda x: x['name'])

        for i, game_data in enumerate(games_list[:20], 1):
            self.stdout.write(
                f"   {i:3d}. {game_data['name']} (ID: {game_data['id']}) - {game_data['keywords_count']} ключ. слов")

        if len(games_list) > 20:
            self.stdout.write(f"   ... и ещё {len(games_list) - 20} игр")

        self.stdout.write("\n   Команды:")
        self.stdout.write("      all - восстановить все игры")
        self.stdout.write("      ids:1,2,3 - восстановить игры с указанными ID")
        self.stdout.write("      search:текст - поиск по названию")
        self.stdout.write("      quit - выход")

        while True:
            choice = input("\n➤ Ваш выбор: ").strip().lower()

            if choice == 'quit':
                return

            if choice == 'all':
                self._perform_restore(games_list, options.get('backup_first'),
                                      options.get('force'), options.get('dry_run'),
                                      options.get('verbose'), options.get('output_dir'))
                return

            if choice.startswith('ids:'):
                id_list = choice[4:].split(',')
                selected = []
                for game_id in id_list:
                    game_id = game_id.strip()
                    if game_id in backup_data['games']:
                        selected.append(backup_data['games'][game_id])

                if selected:
                    self._perform_restore(selected, options.get('backup_first'),
                                          options.get('force'), options.get('dry_run'),
                                          options.get('verbose'), options.get('output_dir'))
                else:
                    self.stdout.write("❌ Игры не найдены")
                return

            if choice.startswith('search:'):
                search_term = choice[7:].lower()
                selected = []
                for game_data in games_list:
                    if search_term in game_data['name'].lower():
                        selected.append(game_data)

                if selected:
                    self.stdout.write(f"\n🔍 Найдено {len(selected)} игр:")
                    for game_data in selected:
                        self.stdout.write(f"   • {game_data['name']} (ID: {game_data['id']})")

                    confirm = input(f"\nВосстановить найденные игры? (y/N): ").strip().lower()
                    if confirm == 'y':
                        self._perform_restore(selected, options.get('backup_first'),
                                              options.get('force'), options.get('dry_run'),
                                              options.get('verbose'), options.get('output_dir'))
                else:
                    self.stdout.write("❌ Ничего не найдено")
                return

    def _list_backups(self, output_dir, verbose):
        """Показывает все доступные бэкапы"""

        self.stdout.write(f"\n📋 ДОСТУПНЫЕ БЭКАПЫ В ПАПКЕ: {output_dir}")
        self.stdout.write("=" * 60)

        backup_files = glob.glob(os.path.join(output_dir, "games_backup_*.json*"))

        if not backup_files:
            self.stdout.write("📭 Нет бэкапов")
            return

        # Группируем по дате
        backups_info = []
        for f in backup_files:
            mtime = datetime.fromtimestamp(os.path.getmtime(f))
            size = os.path.getsize(f) / (1024 * 1024)  # в MB

            try:
                if f.endswith('.gz'):
                    import gzip
                    with gzip.open(f, 'rt', encoding='utf-8') as gf:
                        data = json.load(gf)
                else:
                    with open(f, 'r', encoding='utf-8') as jf:
                        data = json.load(jf)

                metadata = data.get('metadata', {})
                games_count = metadata.get('total_games', len(data.get('games', {})))
                created = metadata.get('created_at', '')

                backups_info.append({
                    'filename': os.path.basename(f),
                    'path': f,
                    'date': mtime,
                    'size': size,
                    'games': games_count,
                    'created': created,
                    'compressed': f.endswith('.gz')
                })
            except Exception as e:
                backups_info.append({
                    'filename': os.path.basename(f),
                    'path': f,
                    'date': mtime,
                    'size': size,
                    'games': '?',
                    'created': '',
                    'compressed': f.endswith('.gz'),
                    'error': str(e)
                })

        # Сортируем по дате (новые сверху)
        backups_info.sort(key=lambda x: x['date'], reverse=True)

        self.stdout.write(f"\n📦 НАЙДЕНО {len(backup_files)} БЭКАПОВ:\n")

        for i, info in enumerate(backups_info, 1):
            marker = "📌" if i == 1 else "📄"
            compressed_mark = " 🗜️" if info['compressed'] else ""

            self.stdout.write(
                f"   {marker} {info['filename']}{compressed_mark}\n"
                f"      📅 {info['date'].strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"      🎮 Игр: {info['games']}\n"
                f"      💾 Размер: {info['size']:.2f} MB"
            )

            if info.get('created'):
                self.stdout.write(f"      📦 Создан в бэкапе: {info['created']}")

            if info.get('error'):
                self.stdout.write(f"      ⚠️ Ошибка: {info['error']}")

            self.stdout.write("")

        self.stdout.write("💡 ИСПОЛЬЗОВАНИЕ:")
        self.stdout.write("   --restore - восстановить из последнего бэкапа")
        self.stdout.write("   --restore --restore-file <имя> - восстановить из конкретного файла")

    def _cleanup_backups(self, output_dir, days, keep, verbose, force, dry_run):
        """Очищает старые бэкапы"""

        self.stdout.write(f"\n🧹 Очистка старых бэкапов в папке: {output_dir}")

        backup_files = glob.glob(os.path.join(output_dir, "games_backup_*.json*"))

        if not backup_files:
            self.stdout.write("📭 Нет файлов для очистки")
            return

        # Получаем информацию о файлах
        files_info = []
        for f in backup_files:
            mtime = datetime.fromtimestamp(os.path.getmtime(f))
            files_info.append({
                'path': f,
                'mtime': mtime,
                'size': os.path.getsize(f)
            })

        # Сортируем по времени (новые сверху)
        files_info.sort(key=lambda x: x['mtime'], reverse=True)

        files_to_delete = []

        # Удаляем по количеству
        if len(files_info) > keep:
            for file_info in files_info[keep:]:
                files_to_delete.append(file_info)

        # Удаляем по возрасту
        if days > 0:
            cutoff_time = datetime.now() - timedelta(days=days)
            for file_info in files_info:
                if file_info['mtime'] < cutoff_time:
                    if file_info not in files_to_delete:
                        files_to_delete.append(file_info)

        if not files_to_delete:
            self.stdout.write("📭 Нет файлов для удаления")
            return

        self.stdout.write(f"\n📊 Найдено файлов: {len(files_info)}")
        self.stdout.write(f"🗑️ Будет удалено: {len(files_to_delete)}")

        if verbose:
            self.stdout.write("\nФайлы для удаления:")
            for file_info in sorted(files_to_delete, key=lambda x: x['mtime']):
                self.stdout.write(
                    f"   • {os.path.basename(file_info['path'])} "
                    f"({file_info['mtime'].strftime('%Y-%m-%d %H:%M')}, "
                    f"{file_info['size'] / (1024 * 1024):.2f} MB)"
                )

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    "\n🔍 РЕЖИМ ПРОСМОТРА - удаление не выполнено"
                )
            )
            return

        if not force and self.stdout.isatty():
            confirm = input("\nПродолжить удаление? (y/N): ").strip().lower()
            if confirm != 'y':
                self.stdout.write("❌ Очистка отменена")
                return

        # Удаляем файлы
        deleted_count = 0
        freed_space = 0

        total_files = len(files_to_delete)
        for idx, file_info in enumerate(files_to_delete, 1):
            try:
                freed_space += file_info['size']
                os.remove(file_info['path'])
                deleted_count += 1

                # Показываем прогресс
                if idx % 10 == 0 or idx == total_files:
                    percent = (idx / total_files) * 100
                    self.stdout.write(
                        f"   🗑️ Удаление: {idx}/{total_files} файлов ({percent:.1f}%)",
                        ending='\r'
                    )

            except Exception as e:
                self.stderr.write(f"\n   ❌ Ошибка удаления {file_info['path']}: {e}")

        self.stdout.write("")  # Новая строка после прогресса

        self.stdout.write(
            self.style.SUCCESS(
                f"\n✅ ОЧИСТКА ЗАВЕРШЕНА:\n"
                f"   ✅ Удалено файлов: {deleted_count}\n"
                f"   💾 Освобождено места: {freed_space / (1024 * 1024):.2f} MB"
            )
        )

    def _clear_game_keywords(self, options):
        """Удаляет связи ключевых слов у игр (БЕЗ ОБНОВЛЕНИЯ КЭША)"""

        game_names = options.get('game_names')
        game_ids = options.get('game_ids')
        all_games = options.get('all_games')
        force = options.get('force')
        verbose = options.get('verbose')
        dry_run = options.get('dry_run')

        self.stdout.write("\n🔥 УДАЛЕНИЕ СВЯЗЕЙ КЛЮЧЕВЫХ СЛОВ У ИГР")
        self.stdout.write("=" * 60)

        start_time = datetime.now()

        # МАКСИМАЛЬНО БЫСТРЫЙ СБОР - только ID игр
        if game_ids:
            id_list = [int(id.strip()) for id in game_ids.split(',')]
            game_ids_to_clear = id_list
            self.stdout.write(f"📌 Будет обработано игр по ID: {len(game_ids_to_clear)}")

        elif game_names:
            name_list = [name.strip() for name in game_names.split(',')]
            self.stdout.write(f"📌 Поиск игр по названиям: {', '.join(name_list)}")

            from django.db.models import Q
            query = Q()
            for name in name_list:
                query |= Q(name__icontains=name)

            game_ids_to_clear = list(
                Game.objects.filter(query)
                .order_by('-rating_count', '-rating')
                .values_list('id', flat=True)
                .distinct()
            )[:len(name_list)]

            self.stdout.write(f"   ✅ Найдено игр: {len(game_ids_to_clear)}")

        elif all_games:
            self.stdout.write("📌 Сбор всех игр...")

            # Получаем ВСЕ ID игр
            from django.db import connection

            with connection.cursor() as cursor:
                # Получаем общее количество игр
                cursor.execute("SELECT COUNT(*) FROM games_game")
                total_games = cursor.fetchone()[0]
                self.stdout.write(f"   📊 Всего игр в базе: {total_games}")

                # Получаем все ID игр
                self.stdout.write("   ⏳ Загрузка ID всех игр...")
                cursor.execute("SELECT id FROM games_game")
                game_ids_to_clear = [row[0] for row in cursor.fetchall()]

        else:
            self.stderr.write("❌ Укажите --game-ids, --game-names или --all-games")
            return

        collection_time = (datetime.now() - start_time).total_seconds()
        self.stdout.write(f"⏱️  Время сбора ID: {collection_time:.3f} сек")

        if not game_ids_to_clear:
            self.stdout.write("ℹ️ Нет игр для обработки")
            return

        # ПОЛУЧАЕМ РЕАЛЬНУЮ СТАТИСТИКУ ИЗ ТАБЛИЦЫ СВЯЗЕЙ
        self.stdout.write("\n📊 Получение статистики ключевых слов...")
        stats_start = datetime.now()

        from django.db import connection

        with connection.cursor() as cursor:
            # Сколько реально связей в базе
            cursor.execute("SELECT COUNT(*) FROM games_game_keywords")
            total_relations = cursor.fetchone()[0]

            # Какие игры реально имеют ключевые слова
            cursor.execute("""
                           SELECT game_id, COUNT(keyword_id)
                           FROM games_game_keywords
                           WHERE game_id = ANY (%s)
                           GROUP BY game_id
                           """, [game_ids_to_clear])

            real_stats = dict(cursor.fetchall())

        games_with_keywords = [(game_id, real_stats.get(game_id, 0)) for game_id in game_ids_to_clear]
        games_with_keywords = [(gid, count) for gid, count in games_with_keywords if count > 0]

        total_keywords = sum(count for _, count in games_with_keywords)

        stats_time = (datetime.now() - stats_start).total_seconds()
        self.stdout.write(f"⏱️  Время получения статистики: {stats_time:.3f} сек")

        # ВЫВОД СТАТИСТИКИ
        self.stdout.write(f"\n📊 СТАТИСТИКА:")
        self.stdout.write(f"   🔗 Всего связей в БД: {total_relations}")
        self.stdout.write(f"   🎮 Игр с ключевыми словами: {len(games_with_keywords)}")
        self.stdout.write(f"   🔑 Всего ключевых слов: {total_keywords}")

        if not games_with_keywords:
            self.stdout.write("ℹ️ Нет ключевых слов для удаления")
            return

        if verbose and games_with_keywords:
            self.stdout.write("\n   Топ-20 игр по количеству ключевых слов:")
            top_20_ids = [gid for gid, _ in games_with_keywords[:20]]
            if top_20_ids:
                names = dict(Game.objects.filter(id__in=top_20_ids).values_list('id', 'name'))
                for game_id, count in games_with_keywords[:20]:
                    name = names.get(game_id, f"ID:{game_id}")
                    self.stdout.write(f"   • {name[:50]}... (ID: {game_id}) - {count:3d} ключ. слов")

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"\n🔍 РЕЖИМ ПРОСМОТРА - ничего не удалено"
                )
            )
            return

        if not force and self.stdout.isatty():
            self.stdout.write(
                self.style.WARNING(
                    f"\n⚠️ ВНИМАНИЕ: Будет УДАЛЕНО {total_keywords} связей ключевых слов у {len(games_with_keywords)} игр!"
                )
            )
            confirm = input("Продолжить удаление? (y/N): ").strip().lower()
            if confirm != 'y':
                self.stdout.write("❌ Операция отменена")
                return

        # МАКСИМАЛЬНО БЫСТРОЕ УДАЛЕНИЕ
        self.stdout.write("\n🔄 Выполняю удаление...")
        delete_start = datetime.now()

        # Получаем ID игр у которых есть ключевые слова
        game_ids_with_keywords = [gid for gid, _ in games_with_keywords]

        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM games_game_keywords WHERE game_id = ANY(%s)",
                [game_ids_with_keywords]
            )
            deleted_count = cursor.rowcount

        delete_time = (datetime.now() - delete_start).total_seconds()
        self.stdout.write(f"   ✅ Удалено {deleted_count} записей за {delete_time:.3f} сек")

        total_time = (datetime.now() - start_time).total_seconds()

        self.stdout.write(
            self.style.SUCCESS(
                f"\n✅ УДАЛЕНИЕ ЗАВЕРШЕНО:\n"
                f"   ✅ Удалено связей: {deleted_count}\n"
                f"   🎮 Обработано игр: {len(games_with_keywords)}\n"
                f"   ⏱️  Время удаления: {delete_time:.3f} сек\n"
                f"   ⏱️  Общее время: {total_time:.3f} сек"
            )
        )