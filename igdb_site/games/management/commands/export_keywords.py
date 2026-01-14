# games/management/commands/export_keywords.py
import os
import json
from django.core.management.base import BaseCommand
from games.models import Keyword


class Command(BaseCommand):
    help = 'Экспортирует все ключевые слова в файл(ы)'

    def add_arguments(self, parser):
        parser.add_argument(
            '--output',
            type=str,
            default='keywords_export.txt',
            help='Базовое имя выходных файлов (по умолчанию: keywords_export.txt)'
        )
        parser.add_argument(
            '--format',
            type=str,
            choices=['txt-comma', 'txt-lines', 'csv', 'json'],
            default='txt-comma',
            help='Формат вывода: txt-comma (через запятую), txt-lines (по строкам), csv, json'
        )
        parser.add_argument(
            '--split',
            type=int,
            default=0,
            help='Разделить на несколько файлов по N ключевых слов (0 = не разделять)'
        )
        parser.add_argument(
            '--max-per-line',
            type=int,
            default=50,
            help='Максимум ключевых слов на строку (только для txt-comma)'
        )

    def handle(self, *args, **options):
        base_output = options['output']
        format_type = options['format']
        split_size = options['split']
        max_per_line = options['max_per_line']

        # Получаем ключевые слова
        keywords = list(Keyword.objects.order_by('name').values_list('name', flat=True))
        total_keywords = len(keywords)

        self.stdout.write(f'Найдено ключевых слов: {total_keywords}')

        # Определяем на сколько частей делить
        if split_size > 0 and total_keywords > split_size:
            num_files = (total_keywords + split_size - 1) // split_size
            self.stdout.write(f'Разделяем на {num_files} файлов по {split_size} ключевых слов')

            for i in range(num_files):
                start_idx = i * split_size
                end_idx = min((i + 1) * split_size, total_keywords)
                chunk = keywords[start_idx:end_idx]

                # Генерируем имя файла
                if num_files > 1:
                    if '.' in base_output:
                        name, ext = base_output.rsplit('.', 1)
                        filename = f'{name}_part_{i + 1:03d}.{ext}'
                    else:
                        filename = f'{base_output}_part_{i + 1:03d}.txt'
                else:
                    filename = base_output

                # Сохраняем часть
                self._save_keywords(chunk, filename, format_type, max_per_line)
                self.stdout.write(f'  Сохранено в {filename}: {len(chunk)} ключевых слов')

        else:
            # Сохраняем в один файл
            filename = base_output
            self._save_keywords(keywords, filename, format_type, max_per_line)
            self.stdout.write(f'Сохранено в {filename}: {total_keywords} ключевых слов')

    def _save_keywords(self, keywords, filename, format_type, max_per_line):
        """Сохраняет ключевые слова в файл"""
        os.makedirs(os.path.dirname(filename) if os.path.dirname(filename) else '.', exist_ok=True)

        with open(filename, 'w', encoding='utf-8') as f:
            if format_type == 'json':
                json.dump(keywords, f, ensure_ascii=False, indent=2)

            elif format_type == 'csv':
                f.write('keyword\n')
                for name in keywords:
                    f.write(f'{name}\n')

            elif format_type == 'txt-lines':
                for name in keywords:
                    f.write(f'{name}\n')

            elif format_type == 'txt-comma':
                # Через запятую с переносами
                for i in range(0, len(keywords), max_per_line):
                    chunk = keywords[i:i + max_per_line]
                    line = ', '.join(chunk)
                    f.write(f'{line}\n')