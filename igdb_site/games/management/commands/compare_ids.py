# compare_ids.py
# Создайте файл: your_app/management/commands/compare_ids.py

import json
import os
import re
from django.core.management.base import BaseCommand, CommandError
from django.conf import settings
from pathlib import Path


class Command(BaseCommand):
    help = 'Сравнивает все файлы в папке с их оригиналами по цифрам в имени'

    def add_arguments(self, parser):
        parser.add_argument(
            'target_folder',
            type=str,
            help='Путь к папке с файлами для сравнения'
        )
        parser.add_argument(
            '--source-folder',
            type=str,
            default=None,
            help='Путь к папке с исходными файлами'
        )
        parser.add_argument(
            '--source-dir',
            type=str,
            default='games_export',
            help='Директория с исходными файлами относительно BASE_DIR'
        )
        parser.add_argument(
            '--source-prefix',
            type=str,
            default='games_part_',
            help='Префикс исходных файлов'
        )
        parser.add_argument(
            '--target-pattern',
            type=str,
            default=r'.*\.json$',  # Изменено с .txt на .json
            help='Регулярное выражение для фильтрации файлов (по умолчанию: .*\.json$)'
        )
        parser.add_argument(
            '--show-names',
            action='store_true',
            help='Показывать названия игр в выводе'
        )
        parser.add_argument(
            '--summary-only',
            action='store_true',
            help='Показывать только сводку по всем файлам'
        )
        parser.add_argument(
            '--export-report',
            type=str,
            default=None,
            help='Экспортировать отчет в указанный файл (JSON)'
        )
        parser.add_argument(
            '--fix-json',
            action='store_true',
            help='Попытаться автоматически исправить проблемы с JSON'
        )

    def handle(self, *args, **options):
        target_folder = options['target_folder']

        if not os.path.exists(target_folder):
            raise CommandError(f'Папка не найдена: {target_folder}')

        if not os.path.isdir(target_folder):
            raise CommandError(f'Путь не является папкой: {target_folder}')

        if options['source_folder']:
            source_folder = options['source_folder']
        else:
            base_dir = getattr(settings, 'BASE_DIR', None)
            if not base_dir:
                base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            source_folder = os.path.join(base_dir, options['source_dir'])

        if not os.path.exists(source_folder):
            raise CommandError(f'Папка с исходными файлами не найдена: {source_folder}')

        self.stdout.write(self.style.SUCCESS(f'Папка для сравнения: {target_folder}'))
        self.stdout.write(self.style.SUCCESS(f'Папка с оригиналами: {source_folder}'))
        self.stdout.write('=' * 80)

        target_files = self.get_target_files(target_folder, options['target_pattern'])

        if not target_files:
            self.stdout.write(self.style.WARNING('Не найдено файлов для сравнения'))
            self.stdout.write(f'Проверьте паттерн: {options["target_pattern"]}')
            return

        self.stdout.write(f'Найдено файлов для сравнения: {len(target_files)}')
        for f in target_files:
            self.stdout.write(f'  - {os.path.basename(f)}')

        results = []
        total_missing = 0
        total_extra = 0
        total_source_ids = 0
        total_target_ids = 0
        files_with_issues = 0

        for target_file in target_files:
            result = self.compare_file(
                target_file,
                source_folder,
                options,
                target_folder
            )

            if result:
                results.append(result)
                total_missing += result['missing_count']
                total_extra += result['extra_count']
                total_source_ids += result['source_count']
                total_target_ids += result['target_count']

                if result['missing_count'] > 0 or result['extra_count'] > 0:
                    files_with_issues += 1

                if not options['summary_only']:
                    self.print_file_result(result, options['show_names'])

        self.print_summary(results, files_with_issues, total_source_ids, total_target_ids,
                           total_missing, total_extra, len(target_files))

        if options['export_report']:
            self.export_report(results, options['export_report'])

    def load_source_ids(self, filepath):
        """Загружает ID и полные строки из исходного файла (JSONL)"""
        ids = set()
        names = {}
        source_lines = {}  # Словарь: id -> полная строка JSON

        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

                # Пробуем как JSONL (каждая строка - JSON)
                lines = content.strip().split('\n')
                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        item = json.loads(line)
                        if isinstance(item, dict) and 'id' in item:
                            ids.add(item['id'])
                            if 'name' in item:
                                names[item['id']] = item['name']
                            # Сохраняем полную исходную строку
                            source_lines[item['id']] = line
                    except:
                        # Если строка не парсится, пропускаем
                        pass

                # Если ничего не нашли, пробуем как обычный JSON
                if not ids:
                    try:
                        data = json.loads(content)
                        if isinstance(data, list):
                            for item in data:
                                if isinstance(item, dict) and 'id' in item:
                                    ids.add(item['id'])
                                    if 'name' in item:
                                        names[item['id']] = item['name']
                                    # Для JSON массива сериализуем каждый объект
                                    source_lines[item['id']] = json.dumps(item, ensure_ascii=False)
                        elif isinstance(data, dict) and 'id' in data:
                            ids.add(data['id'])
                            if 'name' in data:
                                names[data['id']] = data['name']
                            source_lines[data['id']] = json.dumps(data, ensure_ascii=False)
                    except:
                        pass

        except Exception as e:
            self.stdout.write(self.style.ERROR(
                f'Ошибка чтения {os.path.basename(filepath)}: {e}'
            ))

        return ids, names, source_lines

    def compare_file(self, target_file, source_folder, options, target_folder):
        """Сравнивает один файл и возвращает полные строки для недостающих ID"""
        source_file = self.get_source_file(
            target_file,
            source_folder,
            options['source_prefix'],
            target_folder
        )

        if not source_file:
            return {
                'target_file': target_file,
                'source_file': None,
                'error': 'Исходный файл не найден',
                'missing_count': 0,
                'extra_count': 0,
                'source_count': 0,
                'target_count': 0,
                'missing_ids': [],
                'extra_ids': [],
                'source_names': {},
                'target_names': {},
                'missing_source_lines': {}  # Полные строки для недостающих ID
            }

        # Загружаем ID из целевого файла (теперь .json)
        target_data, target_error = self.load_json_with_fix(target_file, options['fix_json'])

        if target_error:
            return {
                'target_file': target_file,
                'source_file': source_file,
                'error': target_error,
                'missing_count': 0,
                'extra_count': 0,
                'source_count': 0,
                'target_count': 0,
                'missing_ids': [],
                'extra_ids': [],
                'source_names': {},
                'target_names': {},
                'missing_source_lines': {}
            }

        target_ids = set()
        target_names = {}

        if isinstance(target_data, list):
            for item in target_data:
                if isinstance(item, dict) and 'id' in item:
                    target_ids.add(item['id'])
                    if 'name' in item:
                        target_names[item['id']] = item['name']
        elif isinstance(target_data, dict):
            # Если файл содержит один объект, а не массив
            if 'id' in target_data:
                target_ids.add(target_data['id'])
                if 'name' in target_data:
                    target_names[target_data['id']] = target_data['name']

        # Загружаем ID и полные строки из исходного файла
        source_ids, source_names, source_lines = self.load_source_ids(source_file)

        missing_ids = source_ids - target_ids
        extra_ids = target_ids - source_ids

        # Собираем полные строки для недостающих ID
        missing_source_lines = {}
        for missing_id in missing_ids:
            if missing_id in source_lines:
                missing_source_lines[missing_id] = source_lines[missing_id]

        return {
            'target_file': target_file,
            'source_file': source_file,
            'error': None,
            'missing_count': len(missing_ids),
            'extra_count': len(extra_ids),
            'source_count': len(source_ids),
            'target_count': len(target_ids),
            'missing_ids': missing_ids,
            'extra_ids': extra_ids,
            'source_names': source_names,
            'target_names': target_names,
            'missing_source_lines': missing_source_lines,  # Полные строки для недостающих ID
            'target_filename': os.path.basename(target_file),
            'source_filename': os.path.basename(source_file)
        }

    def print_file_result(self, result, show_names):
        """Выводит результат для одного файла"""
        if result['error']:
            self.stdout.write(self.style.ERROR(f'\n❌ {result["target_filename"]}: {result["error"]}'))
            return

        filename = result['target_filename']

        self.stdout.write('\n' + '=' * 80)

        if result['missing_count'] == 0 and result['extra_count'] == 0:
            self.stdout.write(self.style.SUCCESS(f'✅ {filename} - ПОЛНОСТЬЮ СОВПАДАЕТ'))
        else:
            self.stdout.write(self.style.WARNING(f'⚠️  {filename} - ТРЕБУЕТ ИСПРАВЛЕНИЯ'))

        self.stdout.write(f'   Исходный файл: {result["source_filename"]}')
        self.stdout.write(f'   ID в исходном: {result["source_count"]}, ID в целевом: {result["target_count"]}')

        if result['missing_count'] > 0:
            self.stdout.write(self.style.WARNING(f'\n   ❌ НЕ ХВАТАЕТ ID ({result["missing_count"]}):'))
            # Выводим все строки подряд без ограничений
            for id_val in sorted(result['missing_ids']):
                if id_val in result.get('missing_source_lines', {}):
                    self.stdout.write(f'      {result["missing_source_lines"][id_val]}')
                else:
                    self.stdout.write(f'      {id_val}')

        if result['extra_count'] > 0:
            self.stdout.write(self.style.ERROR(f'\n   ➕ ЛИШНИЕ ID ({result["extra_count"]}):'))
            for id_val in sorted(result['extra_ids'])[:20]:
                if show_names and id_val in result['target_names']:
                    self.stdout.write(f'      {id_val} - {result["target_names"][id_val]}')
                else:
                    self.stdout.write(f'      {id_val}')
            if result['extra_count'] > 20:
                self.stdout.write(f'      ... и еще {result["extra_count"] - 20} ID')

    def export_report(self, results, export_file):
        """Экспортирует отчет, включая полные строки для недостающих ID"""
        report = {
            'total_files': len(results),
            'files_with_issues': sum(
                1 for r in results if r.get('missing_count', 0) > 0 or r.get('extra_count', 0) > 0),
            'files_with_errors': sum(1 for r in results if r.get('error')),
            'total_missing': sum(r.get('missing_count', 0) for r in results),
            'total_extra': sum(r.get('extra_count', 0) for r in results),
            'files': []
        }

        for r in results:
            file_report = {
                'target_file': r.get('target_file'),
                'source_file': r.get('source_file'),
                'error': r.get('error'),
                'missing_count': r.get('missing_count', 0),
                'extra_count': r.get('extra_count', 0),
                'missing_ids': list(r.get('missing_ids', [])),
                'extra_ids': list(r.get('extra_ids', [])),
                'missing_source_lines': {}  # Словарь id -> полная строка JSON
            }

            # Добавляем полные строки для недостающих ID
            if 'missing_source_lines' in r:
                for missing_id, source_line in r['missing_source_lines'].items():
                    file_report['missing_source_lines'][str(missing_id)] = source_line

            report['files'].append(file_report)

        try:
            with open(export_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            self.stdout.write(self.style.SUCCESS(f'\n📄 Отчет экспортирован в: {export_file}'))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'\nОшибка экспорта: {e}'))

    def get_target_files(self, folder, pattern):
        """Получает список файлов в папке"""
        target_files = []
        pattern_re = re.compile(pattern)

        for file in os.listdir(folder):
            file_path = os.path.join(folder, file)
            if os.path.isfile(file_path) and pattern_re.search(file):
                target_files.append(file_path)

        target_files.sort(key=self.extract_number_from_filename)
        return target_files

    def extract_number_from_filename(self, filepath):
        """Извлекает число из имени файла"""
        filename = os.path.basename(filepath)
        numbers = re.findall(r'\d+', filename)
        if numbers:
            return int(numbers[-1])
        return 0

    def get_source_file(self, target_file, source_folder, source_prefix, target_folder):
        """Определяет исходный файл"""
        target_filename = os.path.basename(target_file)
        numbers = re.findall(r'\d+', target_filename)

        if not numbers:
            rel_path = os.path.relpath(target_file, target_folder)
            numbers = re.findall(r'\d+', rel_path)
            if not numbers:
                return None

        number = numbers[-1]
        number_zfill = number.zfill(4)

        source_file = os.path.join(source_folder, f"{source_prefix}{number_zfill}.json")

        if not os.path.exists(source_file):
            source_file = os.path.join(source_folder, f"{source_prefix}{number}.json")

        if not os.path.exists(source_file):
            for ext in ['.json', '.jsonl', '.txt']:
                test_file = os.path.join(source_folder, f"{source_prefix}{number_zfill}{ext}")
                if os.path.exists(test_file):
                    return test_file
                test_file = os.path.join(source_folder, f"{source_prefix}{number}{ext}")
                if os.path.exists(test_file):
                    return test_file

        return source_file if os.path.exists(source_file) else None

    def load_json_with_fix(self, filepath, fix=False):
        """Загружает JSON с возможностью исправления ошибок"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()

            # Пробуем стандартную загрузку
            try:
                data = json.loads(content)
                return data, None
            except json.JSONDecodeError as e:
                if not fix:
                    return None, f"Ошибка парсинга: {e}"

                self.stdout.write(self.style.WARNING(f'Попытка исправить JSON в {os.path.basename(filepath)}...'))

                # Фикс 1: убираем завершающие запятые
                content_fixed = re.sub(r',\s*}', '}', content)
                content_fixed = re.sub(r',\s*]', ']', content_fixed)

                try:
                    data = json.loads(content_fixed)
                    self.stdout.write(self.style.SUCCESS(f'  ✓ Успешно исправлено (убраны завершающие запятые)'))
                    return data, None
                except:
                    pass

                # Фикс 2: пытаемся найти и исправить пропущенные запятые
                lines = content.split('\n')
                fixed_lines = []
                for i, line in enumerate(lines):
                    if i > 0 and line.strip().startswith('{') and not lines[i - 1].rstrip().endswith(','):
                        if fixed_lines:
                            fixed_lines[-1] = fixed_lines[-1].rstrip() + ','
                    fixed_lines.append(line)

                content_fixed = '\n'.join(fixed_lines)

                try:
                    data = json.loads(content_fixed)
                    self.stdout.write(self.style.SUCCESS(f'  ✓ Успешно исправлено (добавлены пропущенные запятые)'))
                    return data, None
                except:
                    pass

                # Фикс 3: пробуем загружать построчно (как JSONL)
                try:
                    data = []
                    for line in content.split('\n'):
                        line = line.strip()
                        if line:
                            if line.startswith('{') and line.endswith('}'):
                                obj = json.loads(line)
                                data.append(obj)
                    if data:
                        self.stdout.write(self.style.SUCCESS(f'  ✓ Успешно загружено как JSONL (построчно)'))
                        return data, None
                except:
                    pass

                return None, f"Не удалось исправить JSON: {e}"

        except Exception as e:
            return None, f"Ошибка чтения файла: {e}"

    def print_summary(self, results, files_with_issues, total_source_ids, total_target_ids,
                      total_missing, total_extra, total_files):
        """Выводит общую статистику"""
        self.stdout.write('\n' + '=' * 80)
        self.stdout.write(self.style.SUCCESS('ОБЩАЯ СТАТИСТИКА'))
        self.stdout.write('=' * 80)

        error_files = sum(1 for r in results if r.get('error'))
        perfect_files = total_files - files_with_issues - error_files

        self.stdout.write(f'Всего файлов обработано: {total_files}')
        self.stdout.write(f'✅ Полностью совпадают: {perfect_files}')
        self.stdout.write(f'⚠️  Требуют исправления: {files_with_issues}')
        self.stdout.write(f'❌ С ошибками парсинга: {error_files}')

        self.stdout.write(f'\n📊 Общее количество ID:')
        self.stdout.write(f'   В исходных файлах: {total_source_ids}')
        self.stdout.write(f'   В целевых файлах: {total_target_ids}')

        if total_missing > 0 or total_extra > 0:
            self.stdout.write(f'\n📈 Различия:')
            self.stdout.write(self.style.WARNING(f'   Не хватает ID: {total_missing}'))
            self.stdout.write(self.style.ERROR(f'   Лишние ID: {total_extra}'))
