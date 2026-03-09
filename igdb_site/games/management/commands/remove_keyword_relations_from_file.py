# games/management/commands/remove_keyword_relations_from_file.py
"""
Команда для удаления связей ключевых слов с играми на основе файла результатов анализа.
Читает файл analysis_keywords_results.txt и удаляет все связи ключевых слов,
которые были найдены в этом файле.
"""

import re
import sys
import os
from typing import Set, Tuple
from django.core.management.base import BaseCommand
from django.db import transaction
from games.models import Game, Keyword


class Command(BaseCommand):
    help = 'Удаляет связи ключевых слов с играми на основе файла результатов анализа'

    def add_arguments(self, parser):
        parser.add_argument(
            '--file',
            type=str,
            default='P:\\Users\\Alexey\\Desktop\\igdb_site\\igdb_site\\analysis_keywords_results.txt',
            help='Путь к файлу с результатами анализа (по умолчанию: analysis_keywords_results.txt)'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Режим пробного запуска (без реального удаления)'
        )
        parser.add_argument(
            '--verbose',
            action='store_true',
            help='Подробный вывод'
        )

    def handle(self, *args, **options):
        file_path = options['file']
        dry_run = options['dry_run']
        verbose = options['verbose']

        if dry_run:
            self.stdout.write("🔍 РЕЖИМ ПРОБНОГО ЗАПУСКА (без реального удаления)")
        else:
            self.stdout.write("⚠️  РЕЖИМ РЕАЛЬНОГО УДАЛЕНИЯ")

        # Проверяем существование файла
        if not os.path.exists(file_path):
            self.stderr.write(f"❌ Файл не найден: {file_path}")
            return

        self.stdout.write(f"📁 Читаем файл: {file_path}")

        # Парсим файл и собираем данные
        games_data = self._parse_file(file_path, verbose)

        if not games_data:
            self.stdout.write("❌ Не удалось найти данные в файле")
            return

        self.stdout.write(f"\n📊 НАЙДЕНО В ФАЙЛЕ:")
        self.stdout.write(f"   Всего игр: {len(games_data)}")

        total_keywords = sum(len(data['keywords']) for data in games_data.values())
        self.stdout.write(f"   Всего ключевых слов: {total_keywords}")

        if verbose:
            self._print_summary(games_data)

        # Подтверждение
        if not dry_run:
            self.stdout.write("\n⚠️  ВНИМАНИЕ: Это удалит все указанные связи из базы данных!")
            confirm = input("Вы уверены? (введите 'yes' для подтверждения): ")
            if confirm.lower() != 'yes':
                self.stdout.write("❌ Операция отменена")
                return

        # Удаляем связи
        results = self._remove_relations(games_data, dry_run, verbose)

        # Выводим результаты
        self._print_results(results, dry_run)

    def _parse_file(self, file_path: str, verbose: bool) -> dict:
        """
        Парсит файл и возвращает словарь:
        {
            game_id: {
                'name': game_name,
                'keywords': [keyword_name1, keyword_name2, ...]
            }
        }
        """
        games_data = {}
        current_game_id = None
        current_game_name = None
        current_keywords = []

        keyword_pattern = re.compile(r'^\s*•\s+([^(]+)(?:\s*\(.*\))?$')
        game_header_pattern = re.compile(r'^(\d+)\.\s+🎮\s+(.+?)\s+\(ID:\s*(\d+)\)$')

        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        for line in lines:
            line = line.rstrip()

            # Пропускаем пустые строки и разделители
            if not line or line.startswith('=') or line.startswith('📊') or line.startswith('🔍'):
                continue

            # Ищем заголовок игры
            game_match = game_header_pattern.match(line)
            if game_match:
                # Сохраняем предыдущую игру если есть
                if current_game_id and current_keywords:
                    games_data[current_game_id] = {
                        'name': current_game_name,
                        'keywords': list(set(current_keywords))  # Уникальные ключевые слова
                    }
                    if verbose:
                        self.stdout.write(
                            f"   Найдено {len(current_keywords)} ключевых слов для игры {current_game_id}: {current_game_name}")

                # Начинаем новую игру
                game_number = game_match.group(1)
                current_game_name = game_match.group(2)
                current_game_id = int(game_match.group(3))
                current_keywords = []
                continue

            # Ищем ключевые слова
            keyword_match = keyword_pattern.match(line)
            if keyword_match and current_game_id:
                keyword = keyword_match.group(1).strip()
                if keyword and keyword not in ['Ключевые слова']:
                    current_keywords.append(keyword)

        # Добавляем последнюю игру
        if current_game_id and current_keywords:
            games_data[current_game_id] = {
                'name': current_game_name,
                'keywords': list(set(current_keywords))
            }
            if verbose:
                self.stdout.write(
                    f"   Найдено {len(current_keywords)} ключевых слов для игры {current_game_id}: {current_game_name}")

        return games_data

    def _print_summary(self, games_data: dict):
        """Выводит подробную сводку по играм"""
        self.stdout.write("\n📋 ДЕТАЛЬНАЯ ИНФОРМАЦИЯ:")
        for game_id, data in games_data.items():
            self.stdout.write(f"\n  🎮 {data['name']} (ID: {game_id})")
            for keyword in sorted(data['keywords']):
                self.stdout.write(f"     • {keyword}")

    def _remove_relations(self, games_data: dict, dry_run: bool, verbose: bool) -> dict:
        """
        Удаляет связи между играми и ключевыми словами

        Returns:
            {
                'total_games': общее количество обработанных игр,
                'total_relations': общее количество удаленных связей,
                'games_processed': список обработанных игр с деталями
            }
        """
        results = {
            'total_games': 0,
            'total_relations': 0,
            'games_processed': [],
            'errors': []
        }

        for game_id, data in games_data.items():
            try:
                game = Game.objects.get(id=game_id)

                # Получаем ключевые слова по именам
                keywords_to_remove = []
                not_found_keywords = []

                for keyword_name in data['keywords']:
                    try:
                        keyword = Keyword.objects.get(name__iexact=keyword_name)
                        keywords_to_remove.append(keyword)
                    except Keyword.DoesNotExist:
                        not_found_keywords.append(keyword_name)

                if keywords_to_remove:
                    # Получаем текущие ключевые слова игры для статистики
                    current_keywords = set(game.keywords.all())

                    # Определяем, какие из запрошенных ключевых слов действительно есть у игры
                    existing_keywords = [k for k in keywords_to_remove if k in current_keywords]

                    if dry_run:
                        # В режиме dry-run только подсчитываем
                        results['total_relations'] += len(existing_keywords)
                        results['games_processed'].append({
                            'game_id': game_id,
                            'game_name': data['name'],
                            'removed_count': len(existing_keywords),
                            'total_before': len(current_keywords),
                            'total_after': len(current_keywords) - len(existing_keywords),
                            'not_found': not_found_keywords
                        })
                        if verbose:
                            self.stdout.write(
                                f"   [DRY RUN] Будет удалено {len(existing_keywords)} связей у игры {game_id}")
                    else:
                        # Реальное удаление
                        with transaction.atomic():
                            # Удаляем связи (remove работает с many-to-many)
                            for keyword in existing_keywords:
                                game.keywords.remove(keyword)

                            results['total_relations'] += len(existing_keywords)

                            # Получаем обновленное количество
                            new_count = game.keywords.count()

                            results['games_processed'].append({
                                'game_id': game_id,
                                'game_name': data['name'],
                                'removed_count': len(existing_keywords),
                                'total_before': len(current_keywords),
                                'total_after': new_count,
                                'not_found': not_found_keywords
                            })

                            if verbose:
                                self.stdout.write(f"   ✅ Удалено {len(existing_keywords)} связей у игры {game_id}")
                else:
                    results['games_processed'].append({
                        'game_id': game_id,
                        'game_name': data['name'],
                        'removed_count': 0,
                        'total_before': game.keywords.count(),
                        'total_after': game.keywords.count(),
                        'not_found': data['keywords']
                    })

                results['total_games'] += 1

            except Game.DoesNotExist:
                error_msg = f"❌ Игра с ID {game_id} не найдена"
                results['errors'].append(error_msg)
                if verbose:
                    self.stderr.write(error_msg)
            except Exception as e:
                error_msg = f"❌ Ошибка при обработке игры {game_id}: {e}"
                results['errors'].append(error_msg)
                if verbose:
                    self.stderr.write(error_msg)

        return results

    def _print_results(self, results: dict, dry_run: bool):
        """Выводит результаты операции"""
        mode = "ПРОБНЫЙ ЗАПУСК" if dry_run else "РЕЗУЛЬТАТЫ"

        self.stdout.write("\n" + "=" * 60)
        self.stdout.write(f"📊 {mode}")
        self.stdout.write("=" * 60)

        self.stdout.write(f"📊 Обработано игр: {results['total_games']}")
        self.stdout.write(f"📊 Удалено связей: {results['total_relations']}")

        if results['errors']:
            self.stdout.write(f"\n❌ ОШИБКИ ({len(results['errors'])}):")
            for error in results['errors']:
                self.stdout.write(f"   {error}")

        if results['games_processed']:
            self.stdout.write(f"\n📋 ДЕТАЛИ ПО ИГРАМ:")
            total_removed = 0
            for game in results['games_processed']:
                total_removed += game['removed_count']
                status = "✅" if game['removed_count'] > 0 else "ℹ️"
                self.stdout.write(f"   {status} {game['game_name']} (ID: {game['game_id']}): "
                                  f"удалено {game['removed_count']} связей")
                if game['not_found']:
                    self.stdout.write(f"      ⚠️ Не найдены в БД: {', '.join(game['not_found'])}")

        self.stdout.write("=" * 60)

        if dry_run:
            self.stdout.write("\n💡 Для реального удаления запустите без --dry-run")
        else:
            self.stdout.write("✅ Операция завершена")