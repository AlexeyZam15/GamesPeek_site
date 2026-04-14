"""
Django management command for backing up and restoring genres or themes with all relationships.
"""

import json
import os
import glob
from datetime import datetime
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction
from django.db import connection

from games.models_parts.simple_models import Genre, Theme
from games.models_parts.game import Game


class Command(BaseCommand):
    """
    Create backup or restore a genre or theme with all relationships.

    Примеры:
      # Резервное копирование
      python manage.py backup_genre_theme --backup --type genre --name Action
      python manage.py backup_genre_theme --backup-all

      # Восстановление
      python manage.py backup_genre_theme --restore --type genre --name Action
      python manage.py backup_genre_theme --restore --file backups/genre_Action.json
      python manage.py backup_genre_theme --restore-all --force

      # Проверка
      python manage.py backup_genre_theme --check-up --type genre --verbose
    """

    help = 'Create backup or restore genre/theme with all relationships'

    def add_arguments(self, parser):
        operation_group = parser.add_mutually_exclusive_group(required=False)
        operation_group.add_argument(
            '--backup',
            action='store_true',
            help='Создать резервную копию указанного жанра или темы'
        )
        operation_group.add_argument(
            '--backup-all',
            action='store_true',
            help='Создать резервные копии для всех жанров, всех тем или обоих'
        )
        operation_group.add_argument(
            '--restore',
            action='store_true',
            help='Восстановить из одного файла резервной копии'
        )
        operation_group.add_argument(
            '--restore-all',
            action='store_true',
            help='Восстановить все резервные копии из директории'
        )
        operation_group.add_argument(
            '--check-up',
            action='store_true',
            help='Проверить соответствие резервных копий текущей базе данных'
        )

        parser.add_argument(
            '--type',
            choices=['genre', 'theme', 'all'],
            help='Тип сущности: жанр, тема или все'
        )
        parser.add_argument(
            '--name',
            type=str,
            help='Имя жанра или темы для резервного копирования (требуется для --backup)'
        )
        parser.add_argument(
            '--file',
            type=str,
            help='Путь к файлу резервной копии для восстановления (требуется для --restore)'
        )
        parser.add_argument(
            '--backup-dir',
            type=str,
            default='backups',
            help='Директория для файлов резервных копий (по умолчанию: "backups")'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Выполнить проверку без фактических изменений'
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help='Принудительное восстановление с перезаписью существующих данных'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод информации о проверке'
        )

    def handle(self, *args, **options):
        if options['backup']:
            self._perform_single_backup(options)
        elif options['backup_all']:
            self._perform_bulk_backup(options)
        elif options['restore']:
            self._perform_restore(options)
        elif options['restore_all']:
            self._perform_bulk_restore(options)
        elif options['check_up']:
            self._perform_check_up(options)
        else:
            raise CommandError("Необходимо указать --backup, --backup-all, --restore, --restore-all или --check-up")

    def _perform_check_up(self, options):
        """
        Проверяет соответствие резервных копий текущей базе данных.

        Аргументы:
            options: Словарь с параметрами командной строки
        """
        entity_type = options.get('type')
        backup_dir = options.get('backup_dir')
        verbose = options.get('verbose', False)

        # Если type не указан, проверяем все
        if entity_type is None:
            entity_type = 'all'
            self.stdout.write(self.style.WARNING("Не указан --type, проверяю все жанры и темы"))

        if not os.path.exists(backup_dir):
            raise CommandError(f"Директория резервных копий не найдена: {backup_dir}")

        self.stdout.write(f"Директория резервных копий: {os.path.abspath(backup_dir)}")
        self.stdout.write(f"Тип сущности: {entity_type}")

        if verbose:
            self.stdout.write("\n" + self.style.NOTICE("=== РЕЖИМ ПОДРОБНОЙ ПРОВЕРКИ ===\n"))

        total_checked = 0
        total_matches = 0
        total_mismatches = 0
        total_errors = 0

        if entity_type in ['genre', 'all']:
            self.stdout.write("\n" + "=" * 50)
            self.stdout.write("ПРОВЕРКА ЖАНРОВ")
            self.stdout.write("=" * 50)

            genres_result = self._check_entity_backups('genre', backup_dir, verbose)
            total_checked += genres_result['checked']
            total_matches += genres_result['matches']
            total_mismatches += genres_result['mismatches']
            total_errors += genres_result['errors']

        if entity_type in ['theme', 'all']:
            self.stdout.write("\n" + "=" * 50)
            self.stdout.write("ПРОВЕРКА ТЕМ")
            self.stdout.write("=" * 50)

            themes_result = self._check_entity_backups('theme', backup_dir, verbose)
            total_checked += themes_result['checked']
            total_matches += themes_result['matches']
            total_mismatches += themes_result['mismatches']
            total_errors += themes_result['errors']

        self.stdout.write("\n" + "=" * 50)
        self.stdout.write("ИТОГИ ПРОВЕРКИ")
        self.stdout.write("=" * 50)
        self.stdout.write(f"Проверено файлов: {total_checked}")
        self.stdout.write(f"Совпадают с БД: {total_matches}")
        self.stdout.write(f"Не совпадают с БД: {total_mismatches}")
        self.stdout.write(f"Ошибок при проверке: {total_errors}")

        if total_mismatches == 0 and total_errors == 0:
            self.stdout.write(self.style.SUCCESS("\nВсе резервные копии соответствуют текущей базе данных!"))
        else:
            self.stdout.write(
                self.style.WARNING(f"\nОбнаружены несоответствия! Рекомендуется создать свежие резервные копии."))

    def _check_entity_backups(self, entity_type, backup_dir, verbose):
        """
        Проверяет резервные копии сущностей указанного типа.

        Аргументы:
            entity_type: Тип сущности ('genre' или 'theme')
            backup_dir: Директория с резервными копиями
            verbose: Подробный вывод

        Возвращает:
            Словарь с результатами проверки
        """
        pattern = f"{entity_type}_*.json"
        file_pattern = os.path.join(backup_dir, pattern)
        backup_files = glob.glob(file_pattern)

        if not backup_files:
            self.stdout.write(self.style.WARNING(f"Файлы резервных копий {entity_type} не найдены в {backup_dir}"))
            return {'checked': 0, 'matches': 0, 'mismatches': 0, 'errors': 0}

        self.stdout.write(f"Найдено файлов: {len(backup_files)}")

        checked = 0
        matches = 0
        mismatches = 0
        errors = 0

        for idx, file_path in enumerate(sorted(backup_files), 1):
            try:
                filename = os.path.basename(file_path)

                if verbose:
                    self.stdout.write(f"\n[{idx}/{len(backup_files)}] Проверка: {filename}")

                # Загружаем данные из бэкапа
                with open(file_path, 'r', encoding='utf-8') as backup_file:
                    backup_data = json.load(backup_file)

                # Проверяем соответствие
                result = self._check_backup_consistency(backup_data, entity_type, verbose)
                checked += 1

                if result['consistent']:
                    matches += 1
                    if verbose:
                        self.stdout.write(self.style.SUCCESS(f"  ✓ {result['message']}"))
                else:
                    mismatches += 1
                    self.stdout.write(self.style.WARNING(f"  ✗ {result['message']}"))
                    if verbose and result.get('details'):
                        for detail in result['details']:
                            self.stdout.write(f"    {detail}")

            except Exception as e:
                errors += 1
                self.stdout.write(self.style.ERROR(f"  [{idx}] Ошибка при проверке {filename}: {str(e)}"))
                if verbose:
                    import traceback
                    self.stdout.write(traceback.format_exc())

        if not verbose:
            self.stdout.write(f"\nРезультаты проверки {entity_type}:")
            self.stdout.write(f"  Совпадают: {matches}, Не совпадают: {mismatches}, Ошибок: {errors}")

        return {
            'checked': checked,
            'matches': matches,
            'mismatches': mismatches,
            'errors': errors
        }

    def _check_backup_consistency(self, backup_data, entity_type, verbose):
        """
        Проверяет соответствие одного файла резервной копии текущей базе данных.

        Аргументы:
            backup_data: Данные из файла резервной копии
            entity_type: Тип сущности
            verbose: Подробный вывод

        Возвращает:
            Словарь с результатом проверки
        """
        name = backup_data.get('name')
        backup_igdb_id = backup_data.get('igdb_id')
        backup_games_count = len(backup_data.get('games', []))

        # Проверяем существование сущности в БД
        if entity_type == 'genre':
            model = Genre
        else:
            model = Theme

        try:
            entity = model.objects.get(name__iexact=name)
        except model.DoesNotExist:
            return {
                'consistent': False,
                'message': f"Сущность '{name}' не найдена в базе данных"
            }

        # Проверяем соответствие IGDB ID
        if entity.igdb_id != backup_igdb_id:
            return {
                'consistent': False,
                'message': f"Сущность '{name}': IGDB ID не совпадает (БД: {entity.igdb_id}, бэкап: {backup_igdb_id})"
            }

        # Получаем текущие игры из БД
        if entity_type == 'genre':
            current_games = list(
                Game.objects.filter(genres=entity).values_list('igdb_id', flat=True).order_by('igdb_id'))
        else:
            current_games = list(
                Game.objects.filter(themes=entity).values_list('igdb_id', flat=True).order_by('igdb_id'))

        backup_games = [game['igdb_id'] for game in backup_data.get('games', [])]
        backup_games.sort()

        # Сравниваем количество игр
        if len(current_games) != backup_games_count:
            details = []
            if verbose:
                details.append(f"Количество игр: БД - {len(current_games)}, бэкап - {backup_games_count}")

            # Находим добавленные и удаленные игры
            added_games = set(current_games) - set(backup_games)
            removed_games = set(backup_games) - set(current_games)

            if added_games:
                details.append(f"Добавлено игр в БД: {len(added_games)}")
                if verbose and len(added_games) <= 10:
                    details.append(f"  IGDB ID добавленных игр: {list(added_games)[:10]}")

            if removed_games:
                details.append(f"Удалено игр из БД: {len(removed_games)}")
                if verbose and len(removed_games) <= 10:
                    details.append(f"  IGDB ID удаленных игр: {list(removed_games)[:10]}")

            return {
                'consistent': False,
                'message': f"Сущность '{name}': количество игр не совпадает",
                'details': details
            }

        # Проверяем полное соответствие списка игр
        if current_games != backup_games:
            details = []
            if verbose:
                added_games = set(current_games) - set(backup_games)
                removed_games = set(backup_games) - set(current_games)

                if added_games:
                    details.append(f"Добавлено игр: {len(added_games)}")
                    if len(added_games) <= 10:
                        details.append(f"  IGDB ID: {list(added_games)[:10]}")

                if removed_games:
                    details.append(f"Удалено игр: {len(removed_games)}")
                    if len(removed_games) <= 10:
                        details.append(f"  IGDB ID: {list(removed_games)[:10]}")

            return {
                'consistent': False,
                'message': f"Сущность '{name}': список игр не совпадает",
                'details': details
            }

        return {
            'consistent': True,
            'message': f"Сущность '{name}': полностью соответствует БД (игр: {backup_games_count})"
        }

    def _perform_single_backup(self, options):
        entity_type = options.get('type')
        name = options.get('name')
        backup_dir = options.get('backup_dir')

        if not entity_type or not name:
            raise CommandError("--backup requires --type and --name parameters")

        if entity_type == 'genre':
            try:
                entity = Genre.objects.get(name__iexact=name)
            except Genre.DoesNotExist:
                raise CommandError(f"Genre '{name}' not found")
        else:
            try:
                entity = Theme.objects.get(name__iexact=name)
            except Theme.DoesNotExist:
                raise CommandError(f"Theme '{name}' not found")

        self._create_backup(entity, entity_type, backup_dir)

    def _perform_bulk_backup(self, options):
        entity_type = options.get('type')
        backup_dir = options.get('backup_dir')

        # Если type не указан, делаем бэкап всего
        if entity_type is None:
            entity_type = 'all'
            self.stdout.write(self.style.WARNING("Не указан --type, создаю резервные копии всех жанров и тем"))

        os.makedirs(backup_dir, exist_ok=True)

        self.stdout.write(f"Директория для резервных копий: {os.path.abspath(backup_dir)}")
        self.stdout.write(f"Тип сущности: {entity_type}")

        if entity_type in ['genre', 'all']:
            self.stdout.write("\n" + "=" * 50)
            self.stdout.write("СОЗДАНИЕ РЕЗЕРВНЫХ КОПИЙ ВСЕХ ЖАНРОВ")
            self.stdout.write("=" * 50)

            genres = Genre.objects.all().order_by('name')
            total = genres.count()
            self.stdout.write(f"Найдено жанров в базе данных: {total}\n")

            if total == 0:
                self.stdout.write(self.style.WARNING("Жанры в базе данных не найдены!"))
            else:
                success_count = 0
                error_count = 0

                for idx, genre in enumerate(genres, 1):
                    try:
                        self.stdout.write(f"[{idx}/{total}] Создание резервной копии жанра: {genre.name}")
                        self._create_backup(genre, backup_dir)
                        success_count += 1
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"  Ошибка: {str(e)}"))
                        error_count += 1
                        import traceback
                        self.stdout.write(self.style.ERROR(f"  Подробности: {traceback.format_exc()}"))

                self.stdout.write(f"\nЖанры: успешно - {success_count}, с ошибками - {error_count}")

        if entity_type in ['theme', 'all']:
            self.stdout.write("\n" + "=" * 50)
            self.stdout.write("СОЗДАНИЕ РЕЗЕРВНЫХ КОПИЙ ВСЕХ ТЕМ")
            self.stdout.write("=" * 50)

            themes = Theme.objects.all().order_by('name')
            total = themes.count()
            self.stdout.write(f"Найдено тем в базе данных: {total}\n")

            if total == 0:
                self.stdout.write(self.style.WARNING("Темы в базе данных не найдены!"))
            else:
                success_count = 0
                error_count = 0

                for idx, theme in enumerate(themes, 1):
                    try:
                        self.stdout.write(f"[{idx}/{total}] Создание резервной копии темы: {theme.name}")
                        self._create_backup(theme, backup_dir)
                        success_count += 1
                    except Exception as e:
                        self.stdout.write(self.style.ERROR(f"  Ошибка: {str(e)}"))
                        error_count += 1
                        import traceback
                        self.stdout.write(self.style.ERROR(f"  Подробности: {traceback.format_exc()}"))

                self.stdout.write(f"\nТемы: успешно - {success_count}, с ошибками - {error_count}")

        self.stdout.write("\n" + self.style.SUCCESS("Создание резервных копий успешно завершено!"))

    def _create_backup(self, entity, backup_dir):
        """
        Создает резервную копию сущности (жанра или темы) со всеми связанными играми.

        Аргументы:
            entity: Объект Genre или Theme
            backup_dir: Директория для сохранения бэкапа
        """
        # Определяем тип сущности автоматически
        if isinstance(entity, Genre):
            entity_type = 'genre'
            entity_type_ru = 'жанра'
        elif isinstance(entity, Theme):
            entity_type = 'theme'
            entity_type_ru = 'темы'
        else:
            raise ValueError(f"Неизвестный тип сущности: {type(entity)}")

        self.stdout.write(f"    Сериализация {entity_type_ru}: {entity.name}")
        backup_data = self._serialize_entity_raw_sql(entity, entity_type)

        # Очищаем имя файла от недопустимых символов
        safe_name = "".join(c for c in entity.name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        filename = f"{entity_type}_{safe_name}.json"
        file_path = os.path.join(backup_dir, filename)

        self.stdout.write(f"    Запись в файл: {file_path}")
        with open(file_path, 'w', encoding='utf-8') as backup_file:
            json.dump(backup_data, backup_file, ensure_ascii=False, indent=2)

        file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
        self.stdout.write(
            self.style.SUCCESS(
                f"    Создан: {filename} (игр: {len(backup_data['games'])}, размер: {file_size_mb:.1f} МБ)"
            )
        )

    def _serialize_entity_raw_sql(self, entity, entity_type):
        """Максимально быстрая сериализация через raw SQL."""

        if entity_type == 'genre':
            join_table = 'games_game_genres'
            entity_id_field = 'genre_id'
        else:
            join_table = 'games_game_themes'
            entity_id_field = 'theme_id'

        with connection.cursor() as cursor:
            cursor.execute(f"""
                SELECT 
                    g.id,
                    g.igdb_id,
                    g.name,
                    g.summary,
                    g.storyline,
                    g.rating,
                    g.rating_count,
                    g.first_release_date,
                    g.cover_url,
                    g.game_type,
                    g.version_title
                FROM games_game g
                INNER JOIN {join_table} j ON g.id = j.game_id
                WHERE j.{entity_id_field} = %s
                ORDER BY g.id
            """, [entity.id])

            rows = cursor.fetchall()
            total_games = len(rows)

            if total_games == 0:
                return {
                    'type': entity_type,
                    'igdb_id': entity.igdb_id,
                    'name': entity.name,
                    'created_at': datetime.now().isoformat(),
                    'games': [],
                    'total_games': 0
                }

            games_data = []
            for row in rows:
                first_release_date = None
                if row[7]:
                    first_release_date = row[7].isoformat() if hasattr(row[7], 'isoformat') else str(row[7])

                games_data.append({
                    'igdb_id': row[1],
                    'name': row[2],
                    'summary': row[3],
                    'storyline': row[4],
                    'rating': float(row[5]) if row[5] is not None else None,
                    'rating_count': row[6] or 0,
                    'first_release_date': first_release_date,
                    'cover_url': row[8],
                    'game_type': row[9],
                    'version_title': row[10],
                })

            return {
                'type': entity_type,
                'igdb_id': entity.igdb_id,
                'name': entity.name,
                'created_at': datetime.now().isoformat(),
                'games': games_data,
                'total_games': len(games_data)
            }

    def _perform_restore(self, options):
        file_path = options.get('file')
        dry_run = options.get('dry_run')
        force = options.get('force')
        name = options.get('name')  # Добавлена поддержка поиска по имени

        # Если не указан file, но указан name и type - ищем файл автоматически
        if not file_path and name and options.get('type'):
            entity_type = options.get('type')
            backup_dir = options.get('backup_dir')
            safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()
            auto_file_path = os.path.join(backup_dir, f"{entity_type}_{safe_name}.json")

            if os.path.exists(auto_file_path):
                file_path = auto_file_path
                self.stdout.write(f"Автоматически найден файл бэкапа: {file_path}")
            else:
                # Пробуем найти файл без учета регистра
                pattern = f"{entity_type}_*.json"
                search_pattern = os.path.join(backup_dir, pattern)
                backup_files = glob.glob(search_pattern)

                found_file = None
                for backup_file in backup_files:
                    filename = os.path.basename(backup_file)
                    # Извлекаем имя из формата "type_Имя.json"
                    if '_' in filename:
                        file_name_part = filename[filename.find('_') + 1:filename.rfind('.')]
                        if file_name_part.lower() == safe_name.lower():
                            found_file = backup_file
                            break

                if found_file:
                    file_path = found_file
                    self.stdout.write(f"Найден файл бэкапа (без учета регистра): {file_path}")
                else:
                    raise CommandError(
                        f"Файл бэкапа не найден для {entity_type} '{name}'. "
                        f"Используйте --file для указания конкретного файла"
                    )

        if not file_path:
            raise CommandError(
                "--restore требует --file параметр, или --type и --name для автоматического поиска"
            )

        if not os.path.exists(file_path):
            raise CommandError(f"Файл не найден: {file_path}")

        self.stdout.write(f"Загрузка файла бэкапа...")

        with open(file_path, 'r', encoding='utf-8') as backup_file:
            backup_data = json.load(backup_file)

        # Проверяем соответствие типа и имени, если указаны
        if name and options.get('type'):
            entity_type = options.get('type')
            if backup_data.get('type') != entity_type:
                raise CommandError(
                    f"Несоответствие типа: ожидается {entity_type}, "
                    f"в бэкапе {backup_data.get('type')}"
                )
            if backup_data.get('name').lower() != name.lower():
                raise CommandError(
                    f"Несоответствие имени: ожидается '{name}', "
                    f"в бэкапе '{backup_data.get('name')}'"
                )

        self._restore_from_data(backup_data, dry_run, force)

    def _perform_bulk_restore(self, options):
        entity_type = options.get('type', 'all')
        backup_dir = options.get('backup_dir')
        dry_run = options.get('dry_run')
        force = options.get('force')

        if not os.path.exists(backup_dir):
            raise CommandError(f"Backup directory not found: {backup_dir}")

        self.stdout.write(f"Scanning backup directory: {backup_dir}")

        if dry_run:
            self.stdout.write(self.style.WARNING("=== DRY-RUN MODE: NO CHANGES WILL BE MADE ==="))

        if entity_type in ['genre', 'all']:
            self._restore_all_from_type('genre', backup_dir, dry_run, force)

        if entity_type in ['theme', 'all']:
            self._restore_all_from_type('theme', backup_dir, dry_run, force)

        self.stdout.write(self.style.SUCCESS("Bulk restore completed successfully!"))

    def _restore_all_from_type(self, entity_type, backup_dir, dry_run, force):
        pattern = f"{entity_type}_*.json"
        file_pattern = os.path.join(backup_dir, pattern)
        backup_files = glob.glob(file_pattern)

        if not backup_files:
            self.stdout.write(self.style.WARNING(f"No {entity_type} backup files found in {backup_dir}"))
            return

        self.stdout.write(f"\nFound {len(backup_files)} {entity_type} backup files")

        success_count = 0
        error_count = 0

        for idx, file_path in enumerate(sorted(backup_files), 1):
            try:
                filename = os.path.basename(file_path)
                self.stdout.write(f"  [{idx}/{len(backup_files)}] Restoring {filename}")

                with open(file_path, 'r', encoding='utf-8') as backup_file:
                    backup_data = json.load(backup_file)

                self._restore_from_data(backup_data, dry_run, force)
                success_count += 1

            except Exception as e:
                self.stdout.write(self.style.ERROR(f"    Failed: {str(e)}"))
                error_count += 1

        self.stdout.write(f"  {entity_type.capitalize()}s: {success_count} succeeded, {error_count} failed")

    def _restore_from_data(self, backup_data, dry_run, force):
        entity_type = backup_data['type']
        name = backup_data['name']

        if dry_run:
            self.stdout.write(f"  [DRY-RUN] Will restore {entity_type}: {name}")
            return

        self.stdout.write(f"  Restoring {entity_type}: {name}")

        with transaction.atomic():
            restored_entity = self._restore_entity(backup_data, entity_type, dry_run, force)

            if restored_entity:
                games_count = self._restore_games(backup_data, restored_entity, entity_type)
                self.stdout.write(f"    Restored {games_count} games")

    def _restore_entity(self, backup_data, entity_type, dry_run, force):
        if entity_type == 'genre':
            model = Genre
        else:
            model = Theme

        igdb_id = backup_data['igdb_id']
        name = backup_data['name']

        if dry_run:
            return None

        existing = model.objects.filter(igdb_id=igdb_id).first()

        if existing and not force:
            raise CommandError(
                f"{entity_type.capitalize()} '{name}' already exists. "
                f"Use --force to overwrite"
            )

        if existing and force:
            existing.name = name
            existing.save()
            return existing
        else:
            return model.objects.create(igdb_id=igdb_id, name=name)

    def _restore_games(self, backup_data, restored_entity, entity_type):
        """Восстановление игр с их связями."""

        games_data = backup_data['games']
        total_games = len(games_data)

        if total_games == 0:
            return 0

        restored_count = 0

        for game_data in games_data:
            first_release_date = None
            if game_data.get('first_release_date'):
                try:
                    first_release_date = datetime.fromisoformat(game_data['first_release_date'])
                except (ValueError, TypeError):
                    pass

            game, created = Game.objects.get_or_create(
                igdb_id=game_data['igdb_id'],
                defaults={
                    'name': game_data['name'],
                    'summary': game_data.get('summary'),
                    'storyline': game_data.get('storyline'),
                    'rating': game_data.get('rating'),
                    'rating_count': game_data.get('rating_count', 0),
                    'first_release_date': first_release_date,
                    'cover_url': game_data.get('cover_url'),
                    'game_type': game_data.get('game_type'),
                    'version_title': game_data.get('version_title'),
                }
            )

            if entity_type == 'genre':
                game.genres.add(restored_entity)
            else:
                game.themes.add(restored_entity)

            restored_count += 1

        return restored_count
