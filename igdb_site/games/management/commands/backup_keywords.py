# games/management/commands/backup_keywords.py
"""
Команда для создания бэкапа таблицы ключевых слов и их восстановления.
Сохраняет все ключевые слова с их категориями и метаданными.
"""

from django.core.management.base import BaseCommand
from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction, connection
from games.models import Keyword, KeywordCategory
import json
import os
import glob
from datetime import datetime, timedelta


class Command(BaseCommand):
    help = """
    Управление таблицей ключевых слов: бэкап, восстановление, очистка

    Примеры использования:
        # СОЗДАНИЕ БЭКАПА ВСЕХ КЛЮЧЕВЫХ СЛОВ
        python manage.py backup_keywords --create-backup
        python manage.py backup_keywords --create-backup --compress
        python manage.py backup_keywords --create-backup --min-usage 5

        # Восстановление из бэкапа
        python manage.py backup_keywords --restore
        python manage.py backup_keywords --restore --restore-file keywords_backup_20240101.json
        python manage.py backup_keywords --restore --merge

        # Просмотр доступных бэкапов
        python manage.py backup_keywords --list
        python manage.py backup_keywords --list --verbose

        # Очистка старых бэкапов
        python manage.py backup_keywords --cleanup --keep 5
        python manage.py backup_keywords --cleanup --days 30
    """

    def add_arguments(self, parser):
        # ОСНОВНЫЕ РЕЖИМЫ
        parser.add_argument('--create-backup', action='store_true',
                            help='СОЗДАТЬ БЭКАП ВСЕХ КЛЮЧЕВЫХ СЛОВ')
        parser.add_argument('--restore', action='store_true',
                            help='Восстановить ключевые слова из бэкапа')
        parser.add_argument('--list', action='store_true',
                            help='Показать все доступные бэкапы')
        parser.add_argument('--cleanup', action='store_true',
                            help='Очистить старые бэкапы')

        # ПАРАМЕТРЫ ДЛЯ БЭКАПА
        parser.add_argument('--output-dir', type=str, default='keywords_backups',
                            help='Папка для сохранения бэкапов (по умолчанию keywords_backups)')
        parser.add_argument('--compress', action='store_true',
                            help='Сжимать бэкап (добавит .gz к имени файла)')
        parser.add_argument('--min-usage', type=int, default=0,
                            help='Минимальное количество использований для включения в бэкап')
        parser.add_argument('--category', type=str,
                            help='Бэкап только ключевые слова из указанной категории')

        # ПАРАМЕТРЫ ДЛЯ ВОССТАНОВЛЕНИЯ
        parser.add_argument('--restore-file', type=str,
                            help='Конкретный файл для восстановления')
        parser.add_argument('--merge', action='store_true',
                            help='Добавить/обновить ключевые слова, не удаляя существующие')
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

        output_dir = options.get('output_dir')
        force = options.get('force')
        verbose = options.get('verbose')
        dry_run = options.get('dry_run')

        # Создаём папку для бэкапов
        os.makedirs(output_dir, exist_ok=True)

        # Проверка: взаимоисключающие режимы
        mode_count = sum([create_backup, restore_mode, cleanup_mode, list_mode])
        if mode_count > 1:
            self.stderr.write(
                "❌ Укажите только один режим: --create-backup, --restore, --cleanup или --list"
            )
            return

        # Режим просмотра бэкапов
        if list_mode:
            self._list_backups(output_dir, verbose)
            return

        # Режим создания бэкапа
        if create_backup:
            self._create_backup(options)
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

        # Если ничего не указано, показываем помощь
        self.print_help('manage.py', 'backup_keywords')

    def _create_backup(self, options):
        """Создаёт бэкап всех ключевых слов"""
        output_dir = options.get('output_dir')
        compress = options.get('compress')
        min_usage = options.get('min_usage')
        category_name = options.get('category')
        force = options.get('force')
        verbose = options.get('verbose')
        dry_run = options.get('dry_run')

        self.stdout.write("\n📦 СОЗДАНИЕ БЭКАПА КЛЮЧЕВЫХ СЛОВ")
        self.stdout.write("=" * 60)

        # Формируем запрос
        keywords_query = Keyword.objects.all()

        if min_usage > 0:
            keywords_query = keywords_query.filter(cached_usage_count__gte=min_usage)

        if category_name:
            keywords_query = keywords_query.filter(category__name=category_name)

        # Получаем все категории для справки
        categories = {cat.id: cat.name for cat in KeywordCategory.objects.all()}

        total_keywords = keywords_query.count()
        total_categories = len(categories)

        if total_keywords == 0:
            self.stdout.write("❌ Нет ключевых слов для бэкапа")
            return

        # Формируем имя файла
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filters = []
        if min_usage > 0:
            filters.append(f"min{min_usage}")
        if category_name:
            filters.append(category_name.replace(' ', '_'))

        filter_suffix = f"_{'_'.join(filters)}" if filters else ""
        base_filename = f"keywords_backup_{timestamp}{filter_suffix}_{total_keywords}kw.json"
        if compress:
            base_filename += '.gz'

        filepath = os.path.join(output_dir, base_filename)

        self.stdout.write(f"\n📊 Информация о бэкапе:")
        self.stdout.write(f"   📁 Папка: {output_dir}")
        self.stdout.write(f"   📄 Файл: {base_filename}")
        self.stdout.write(f"   🔑 Всего ключевых слов: {total_keywords}")
        self.stdout.write(f"   📚 Всего категорий: {total_categories}")
        if min_usage > 0:
            self.stdout.write(f"   📊 Мин. использований: {min_usage}")
        if category_name:
            self.stdout.write(f"   🏷️ Категория: {category_name}")
        self.stdout.write(f"   🗜️ Сжатие: {'да' if compress else 'нет'}")

        if not force and not dry_run and self.stdout.isatty():
            # Оцениваем размер (примерно 0.5 KB на ключевое слово)
            estimated_size = total_keywords * 0.5 / 1024  # в MB
            self.stdout.write(f"\n📊 Примерный размер: ~{estimated_size:.2f} MB")
            confirm = input(f"\nСоздать бэкап для {total_keywords} ключевых слов? (y/N): ").strip().lower()
            if confirm != 'y':
                self.stdout.write("❌ Операция отменена")
                return

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"\n🔍 РЕЖИМ ПРОСМОТРА: будет создан бэкап для {total_keywords} ключевых слов"
                )
            )
            return

        # Собираем данные
        backup_data = {
            'metadata': {
                'version': '1.0',
                'created_at': datetime.now().isoformat(),
                'total_keywords': total_keywords,
                'total_categories': total_categories,
                'min_usage': min_usage,
                'category_filter': category_name,
                'source': 'backup_keywords_command'
            },
            'categories': {},
            'keywords': []
        }

        # Сохраняем категории
        for cat_id, cat_name in categories.items():
            backup_data['categories'][str(cat_id)] = {
                'id': cat_id,
                'name': cat_name
            }

        processed = 0
        self.stdout.write("")

        # Собираем ключевые слова
        for keyword in keywords_query.iterator():
            backup_data['keywords'].append({
                'id': keyword.id,
                'igdb_id': keyword.igdb_id,
                'name': keyword.name,
                'category_id': keyword.category_id,
                'cached_usage_count': keyword.cached_usage_count,
                'created_at': keyword.created_at.isoformat() if keyword.created_at else None
            })

            processed += 1
            if processed % 1000 == 0 or processed == total_keywords:
                percent = (processed / total_keywords) * 100
                bar_length = 30
                filled_length = int(bar_length * processed // total_keywords)
                bar = '█' * filled_length + '░' * (bar_length - filled_length)
                self.stdout.write(
                    f"   🔄 Прогресс: |{bar}| {processed}/{total_keywords} ключ. слов ({percent:.1f}%)",
                    ending='\r'
                )

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
                    f"   🔑 Ключевых слов: {total_keywords}\n"
                    f"   📚 Категорий: {total_categories}"
                )
            )

        except Exception as e:
            self.stderr.write(f"❌ Ошибка при сохранении бэкапа: {e}")
            if os.path.exists(filepath):
                os.remove(filepath)

    def _restore_from_backup(self, options):
        """Восстанавливает ключевые слова из бэкапа"""
        output_dir = options.get('output_dir')
        restore_file = options.get('restore_file')
        merge_mode = options.get('merge')
        backup_first = options.get('backup_first')
        force = options.get('force')
        verbose = options.get('verbose')
        dry_run = options.get('dry_run')

        # Если файл не указан, берём самый свежий
        if not restore_file:
            backup_files = glob.glob(os.path.join(output_dir, "keywords_backup_*.json*"))
            if not backup_files:
                self.stderr.write(f"❌ Нет файлов бэкапов в папке {output_dir}")
                return

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
        if 'keywords' not in backup_data:
            self.stderr.write("❌ Неподдерживаемый формат бэкапа")
            return

        metadata = backup_data.get('metadata', {})
        categories_data = backup_data.get('categories', {})

        total_in_backup = len(backup_data['keywords'])
        total_categories = len(categories_data)

        self.stdout.write(f"\n📊 Информация о бэкапе:")
        self.stdout.write(f"   📅 Создан: {metadata.get('created_at', 'Unknown')}")
        self.stdout.write(f"   🔑 Ключевых слов в бэкапе: {total_in_backup}")
        self.stdout.write(f"   📚 Категорий в бэкапе: {total_categories}")
        self.stdout.write(f"   🔄 Режим: {'добавление/обновление (--merge)' if merge_mode else 'полная замена'}")

        # Текущая статистика
        current_total = Keyword.objects.count()
        self.stdout.write(f"\n📊 Текущее состояние БД:")
        self.stdout.write(f"   🔑 Ключевых слов сейчас: {current_total}")

        if dry_run:
            self.stdout.write(
                self.style.WARNING(
                    f"\n🔍 РЕЖИМ ПРОСМОТРА - восстановление не выполнено"
                )
            )
            return

        if not force and self.stdout.isatty():
            confirm = input(f"\nПродолжить восстановление? (y/N): ").strip().lower()
            if confirm != 'y':
                self.stdout.write("❌ Восстановление отменено")
                return

        # Создаём бэкап текущего состояния если нужно
        if backup_first:
            self.stdout.write("\n📦 Создание резервного бэкапа текущего состояния...")
            self._create_emergency_backup(output_dir)

        # Восстанавливаем в транзакции
        try:
            with transaction.atomic():
                # Если не merge - очищаем всё
                if not merge_mode:
                    self.stdout.write("\n🧹 Очистка текущих ключевых слов...")
                    Keyword.objects.all().delete()
                    KeywordCategory.objects.all().delete()
                    # Сбрасываем последовательности ID
                    with connection.cursor() as cursor:
                        cursor.execute("ALTER SEQUENCE games_keyword_id_seq RESTART WITH 1")
                        cursor.execute("ALTER SEQUENCE games_keywordcategory_id_seq RESTART WITH 1")

                # Восстанавливаем категории
                self.stdout.write("\n📚 Восстановление категорий...")
                category_id_map = {}  # старый ID -> новый ID

                for cat_id_str, cat_data in categories_data.items():
                    old_id = int(cat_id_str)
                    cat_name = cat_data['name']

                    # Проверяем, есть ли уже такая категория
                    existing = KeywordCategory.objects.filter(name=cat_name).first()
                    if existing:
                        category_id_map[old_id] = existing.id
                        if verbose:
                            self.stdout.write(f"   📌 Категория '{cat_name}' уже существует (ID: {existing.id})")
                    else:
                        new_cat = KeywordCategory.objects.create(name=cat_name)
                        category_id_map[old_id] = new_cat.id
                        if verbose:
                            self.stdout.write(f"   ✅ Создана категория '{cat_name}' (ID: {new_cat.id})")

                # Восстанавливаем ключевые слова
                self.stdout.write("\n🔑 Восстановление ключевых слов...")
                restored = 0
                skipped = 0
                total_keywords = len(backup_data['keywords'])

                for i, kw_data in enumerate(backup_data['keywords'], 1):
                    name = kw_data['name']
                    igdb_id = kw_data['igdb_id']
                    old_category_id = kw_data.get('category_id')
                    cached_usage = kw_data.get('cached_usage_count', 0)

                    # Маппим ID категории
                    new_category_id = category_id_map.get(old_category_id) if old_category_id else None

                    # Проверяем, существует ли уже такое ключевое слово
                    existing = Keyword.objects.filter(
                        Q(name__iexact=name) | Q(igdb_id=igdb_id)
                    ).first()

                    if existing:
                        if merge_mode:
                            # Обновляем существующее
                            existing.igdb_id = igdb_id
                            if new_category_id:
                                existing.category_id = new_category_id
                            existing.cached_usage_count = cached_usage
                            existing.save()
                            restored += 1
                            if verbose:
                                self.stdout.write(f"   🔄 Обновлено: {name}")
                        else:
                            skipped += 1
                    else:
                        # Создаём новое
                        Keyword.objects.create(
                            igdb_id=igdb_id,
                            name=name,
                            category_id=new_category_id,
                            cached_usage_count=cached_usage
                        )
                        restored += 1
                        if verbose and i % 100 == 0:
                            self.stdout.write(f"   ✅ Восстановлено {i}/{total_keywords}")

                    # Показываем прогресс
                    if i % 1000 == 0 or i == total_keywords:
                        percent = (i / total_keywords) * 100
                        bar_length = 30
                        filled_length = int(bar_length * i // total_keywords)
                        bar = '█' * filled_length + '░' * (bar_length - filled_length)
                        self.stdout.write(
                            f"   🔄 Прогресс: |{bar}| {i}/{total_keywords} ключ. слов ({percent:.1f}%)",
                            ending='\r'
                        )

                self.stdout.write("")  # Новая строка после прогресса

                self.stdout.write(
                    self.style.SUCCESS(
                        f"\n✅ ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО:\n"
                        f"   ✅ Восстановлено/обновлено: {restored}\n"
                        f"   ⏭️ Пропущено (уже есть): {skipped}\n"
                        f"   📚 Категорий: {len(category_id_map)}"
                    )
                )

        except Exception as e:
            self.stderr.write(f"\n❌ Ошибка при восстановлении: {e}")
            raise

    def _create_emergency_backup(self, output_dir):
        """Создаёт экстренный бэкап перед восстановлением"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = os.path.join(output_dir, f"pre_restore_backup_{timestamp}.json")

        # Собираем текущие данные
        categories = {cat.id: cat.name for cat in KeywordCategory.objects.all()}
        keywords = []

        for kw in Keyword.objects.all():
            keywords.append({
                'id': kw.id,
                'igdb_id': kw.igdb_id,
                'name': kw.name,
                'category_id': kw.category_id,
                'cached_usage_count': kw.cached_usage_count,
                'created_at': kw.created_at.isoformat() if kw.created_at else None
            })

        backup_data = {
            'metadata': {
                'version': '1.0',
                'created_at': datetime.now().isoformat(),
                'total_keywords': len(keywords),
                'total_categories': len(categories),
                'source': 'emergency_backup_before_restore'
            },
            'categories': {str(cid): {'id': cid, 'name': name} for cid, name in categories.items()},
            'keywords': keywords
        }

        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, ensure_ascii=False, indent=2)

        self.stdout.write(f"   ✅ Создан бэкап: {os.path.basename(backup_file)}")

    def _list_backups(self, output_dir, verbose):
        """Показывает все доступные бэкапы"""
        self.stdout.write(f"\n📋 ДОСТУПНЫЕ БЭКАПЫ В ПАПКЕ: {output_dir}")
        self.stdout.write("=" * 60)

        backup_files = glob.glob(os.path.join(output_dir, "keywords_backup_*.json*"))

        if not backup_files:
            self.stdout.write("📭 Нет бэкапов")
            return

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
                keywords_count = metadata.get('total_keywords', len(data.get('keywords', [])))
                categories_count = metadata.get('total_categories', len(data.get('categories', {})))
                created = metadata.get('created_at', '')

                backups_info.append({
                    'filename': os.path.basename(f),
                    'path': f,
                    'date': mtime,
                    'size': size,
                    'keywords': keywords_count,
                    'categories': categories_count,
                    'created': created,
                    'compressed': f.endswith('.gz')
                })
            except Exception as e:
                backups_info.append({
                    'filename': os.path.basename(f),
                    'path': f,
                    'date': mtime,
                    'size': size,
                    'keywords': '?',
                    'categories': '?',
                    'created': '',
                    'compressed': f.endswith('.gz'),
                    'error': str(e)
                })

        backups_info.sort(key=lambda x: x['date'], reverse=True)

        self.stdout.write(f"\n📦 НАЙДЕНО {len(backup_files)} БЭКАПОВ:\n")

        for i, info in enumerate(backups_info, 1):
            marker = "📌" if i == 1 else "📄"
            compressed_mark = " 🗜️" if info['compressed'] else ""

            self.stdout.write(
                f"   {marker} {info['filename']}{compressed_mark}\n"
                f"      📅 {info['date'].strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"      🔑 Ключевых слов: {info['keywords']}\n"
                f"      📚 Категорий: {info['categories']}\n"
                f"      💾 Размер: {info['size']:.2f} MB"
            )

            if info.get('created'):
                self.stdout.write(f"      📦 Создан в бэкапе: {info['created']}")

            if info.get('error'):
                self.stdout.write(f"      ⚠️ Ошибка: {info['error']}")

            self.stdout.write("")

    def _cleanup_backups(self, output_dir, days, keep, verbose, force, dry_run):
        """Очищает старые бэкапы"""
        self.stdout.write(f"\n🧹 Очистка старых бэкапов в папке: {output_dir}")

        backup_files = glob.glob(os.path.join(output_dir, "keywords_backup_*.json*"))

        if not backup_files:
            self.stdout.write("📭 Нет файлов для очистки")
            return

        files_info = []
        for f in backup_files:
            mtime = datetime.fromtimestamp(os.path.getmtime(f))
            files_info.append({
                'path': f,
                'mtime': mtime,
                'size': os.path.getsize(f)
            })

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
                self.style.WARNING("\n🔍 РЕЖИМ ПРОСМОТРА - удаление не выполнено")
            )
            return

        if not force and self.stdout.isatty():
            confirm = input("\nПродолжить удаление? (y/N): ").strip().lower()
            if confirm != 'y':
                self.stdout.write("❌ Очистка отменена")
                return

        deleted_count = 0
        freed_space = 0
        total_files = len(files_to_delete)

        for idx, file_info in enumerate(files_to_delete, 1):
            try:
                freed_space += file_info['size']
                os.remove(file_info['path'])
                deleted_count += 1

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