import time
import requests
import re
import json
from django.core.management.base import BaseCommand
from django.db.models import Q
from games.models import Game


class Command(BaseCommand):
    help = 'Импорт описаний игр из RAWG API'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Только проверка без сохранения в БД'
        )
        parser.add_argument(
            '--limit',
            type=int,
            default=0,
            help='Ограничить количество обрабатываемых игр (0 = все)'
        )
        parser.add_argument(
            '--offset',
            type=int,
            default=0,
            help='Пропустить указанное количество игр с начала'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Включить подробный вывод'
        )
        parser.add_argument(
            '--delay',
            type=float,
            default=0.5,
            help='Задержка между запросами (секунды)'
        )
        parser.add_argument(
            '--overwrite',
            action='store_true',
            help='Перезаписать описания, даже если они уже существуют'
        )
        parser.add_argument(
            '--api-key',
            type=str,
            default='4d916c5755a8471b9d55ad3c35f40b1c',
            help='RAWG API ключ (по умолчанию ваш)'
        )
        parser.add_argument(
            '--game-ids',
            type=str,
            help='ID конкретных игр для обработки (через запятую)'
        )
        parser.add_argument(
            '--log-dir',
            type=str,
            default='logs',
            help='Директория для сохранения логов (по умолчанию: logs/)'
        )
        parser.add_argument(
            '--order-by',
            type=str,
            default='id',
            choices=['id', 'name', '-rating', '-rating_count', '-first_release_date'],
            help='Поле для сортировки игр'
        )

    def handle(self, *args, **options):
        # Настройки
        dry_run = options['dry_run']
        limit = options['limit']
        offset = options['offset']
        debug = options['debug']
        delay = options['delay']
        overwrite = options['overwrite']
        api_key = options['api_key']
        game_ids_str = options['game_ids']
        log_dir = options['log_dir']
        order_by = options['order_by']

        # Подготовка логов
        import os
        os.makedirs(log_dir, exist_ok=True)

        timestamp = time.strftime('%Y%m%d_%H%M%S')
        failed_log_file = os.path.join(log_dir, f'failed_games_{timestamp}.json')
        not_found_log_file = os.path.join(log_dir, f'not_found_games_{timestamp}.json')

        failed_games = []
        not_found_games = []

        # Статистика
        stats = {
            'total': 0,
            'processed': 0,
            'updated': 0,
            'skipped_no_overwrite': 0,
            'skipped_not_found': 0,
            'errors': 0,
            'offset_skipped': offset
        }

        # Получаем игры для обработки
        games_query = Game.objects.all()

        # Фильтр по конкретным ID игр
        if game_ids_str:
            try:
                game_ids = [int(id.strip()) for id in game_ids_str.split(',')]
                games_query = games_query.filter(id__in=game_ids)
                if debug:
                    self.stdout.write(f'🔍 Фильтр по ID игр: {game_ids}')
            except ValueError:
                self.stdout.write(self.style.ERROR('❌ Некорректный формат game-ids'))
                return

        # Если НЕ overwrite - берем только игры без описания
        if not overwrite:
            games_query = games_query.filter(
                Q(rawg_description__isnull=True) |
                Q(rawg_description__exact='')
            )
            if debug:
                self.stdout.write('ℹ️  Режим: пропускать игры с существующими описаниями')

        # Применяем сортировку
        games_query = games_query.order_by(order_by)

        # Получаем общее количество до применения offset и limit
        total_in_db = games_query.count()

        # Применяем offset
        if offset > 0:
            games_query = games_query[offset:]
            if debug:
                self.stdout.write(f'ℹ️  Пропущено первых {offset} игр')

        # Применяем лимит
        if limit > 0:
            games_query = games_query[:limit]

        games = list(games_query)
        stats['total'] = len(games)

        if stats['total'] == 0:
            self.stdout.write(self.style.WARNING('⚠️  Не найдено игр для обработки'))
            if offset > 0 and total_in_db > 0:
                self.stdout.write(f'   Offset пропустил {offset} игр, всего в БД: {total_in_db}')
            if not overwrite:
                self.stdout.write('   Используйте --overwrite для обновления всех игр')
            return

        # Вывод информации
        self.stdout.write(self.style.SUCCESS(
            f'🚀 Начинаем импорт из RAWG для {stats["total"]} игр...'
        ))
        self.stdout.write(f'📊 Всего в выборке: {total_in_db} игр')
        if offset > 0:
            self.stdout.write(f'📊 Пропущено (offset): {offset} игр')
        self.stdout.write(f'📊 Будет обработано: {stats["total"]} игр')
        self.stdout.write(f'📊 Режим: {"OVERWRITE" if overwrite else "SKIP EXISTING"}')
        self.stdout.write(f'📊 Dry Run: {"ДА" if dry_run else "НЕТ"}')
        self.stdout.write(f'📊 Сортировка: {order_by}')
        self.stdout.write(f'⏰ Задержка: {delay} секунд')
        self.stdout.write(f'📁 Логи ошибок: {failed_log_file}')
        self.stdout.write(f'📁 Логи не найденных: {not_found_log_file}')
        self.stdout.write('─' * 60)

        # Обрабатываем игры
        for i, game in enumerate(games, 1):
            stats['processed'] += 1
            global_position = offset + i  # Позиция в общей выборке

            has_existing_desc = bool(game.rawg_description and len(game.rawg_description) > 0)

            # Улучшенный вывод прогресса
            progress_bar = self.get_progress_bar(i, stats['total'])
            self.stdout.write(f'\n{progress_bar} {i}/{stats["total"]}')
            if offset > 0:
                self.stdout.write(f'   📍 Глобальная позиция: {global_position}/{total_in_db}')
            self.stdout.write(f'   🎮 "{game.name}" (ID: {game.id})')

            if overwrite and has_existing_desc:
                self.stdout.write('   ℹ️  Уже имеет описание (будет перезаписано)')

            try:
                # 1. Проверка на существующее описание (без overwrite)
                if not overwrite and has_existing_desc:
                    self.stdout.write(self.style.NOTICE('   ⏭️  Пропущено (уже есть описание)'))
                    stats['skipped_no_overwrite'] += 1
                    continue

                # 2. Ищем игру в RAWG
                search_params = {
                    'key': api_key,
                    'search': game.name,
                    'page_size': 3,
                    'search_precise': 'true'
                }

                if debug:
                    self.stdout.write(f'   🔍 Поиск в RAWG: {game.name}')

                search_response = requests.get(
                    'https://api.rawg.io/api/games',
                    params=search_params,
                    timeout=10
                )

                # Обработка ошибок API
                if search_response.status_code != 200:
                    error_msg = f'Ошибка API: {search_response.status_code}'
                    self.stdout.write(self.style.ERROR(f'   ❌ {error_msg}'))

                    failed_games.append({
                        'id': game.id,
                        'name': game.name,
                        'position': global_position,
                        'error': error_msg,
                        'status_code': search_response.status_code,
                        'response': search_response.text[:500] if search_response.text else None,
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                    })
                    stats['errors'] += 1
                    continue

                search_data = search_response.json()
                results = search_data.get('results', [])

                # Если игра не найдена в RAWG
                if not results:
                    self.stdout.write(self.style.WARNING('   ⚠️  Не найдено в RAWG'))
                    stats['skipped_not_found'] += 1

                    not_found_games.append({
                        'id': game.id,
                        'name': game.name,
                        'position': global_position,
                        'reason': 'not_found_in_rawg',
                        'search_query': game.name,
                        'search_url': f'https://api.rawg.io/api/games?search={requests.utils.quote(game.name)}',
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                    })
                    continue

                # 3. Ищем наиболее подходящую игру
                best_match = self.find_best_match(game.name, results)
                if not best_match:
                    best_match = results[0]

                game_id = best_match.get('id')
                matched_name = best_match.get('name', 'N/A')

                if debug:
                    self.stdout.write(f'   ✅ Найдено в RAWG: "{matched_name}" (ID: {game_id})')

                # 4. Получаем детальную информацию
                detail_response = requests.get(
                    f'https://api.rawg.io/api/games/{game_id}',
                    params={'key': api_key},
                    timeout=10
                )

                if detail_response.status_code != 200:
                    error_msg = f'Ошибка деталей: {detail_response.status_code}'
                    self.stdout.write(self.style.ERROR(f'   ❌ {error_msg}'))

                    failed_games.append({
                        'id': game.id,
                        'name': game.name,
                        'position': global_position,
                        'error': error_msg,
                        'status_code': detail_response.status_code,
                        'rawg_game_id': game_id,
                        'rawg_game_name': matched_name,
                        'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                    })
                    stats['errors'] += 1
                    continue

                detail_data = detail_response.json()

                # 5. Извлекаем описание (даже если короткое)
                description = self.extract_description(detail_data, best_match)

                # 6. Показываем разницу
                if overwrite and has_existing_desc and not dry_run:
                    old_len = len(game.rawg_description) if game.rawg_description else 0
                    new_len = len(description) if description else 0
                    self.stdout.write(f'   📝 Замена: {old_len} → {new_len} символов')

                # 7. Сохраняем (даже если description пустое или короткое)
                if dry_run:
                    if description:
                        preview = description[:200] + '...' if len(description) > 200 else description
                        self.stdout.write(self.style.SUCCESS('   [DRY RUN] Найдено описание'))
                        self.stdout.write(f'   📏 Длина: {len(description)} символов')
                        if debug and len(description) < 500:
                            self.stdout.write(f'   📄 Текст: {preview}')
                    else:
                        self.stdout.write(self.style.WARNING('   [DRY RUN] Описание не найдено'))
                    stats['updated'] += 1
                else:
                    # Сохраняем даже пустое описание
                    game.rawg_description = description[:15000] if description else ''
                    game.save(update_fields=['rawg_description'])

                    if description:
                        action = "Обновлено" if has_existing_desc else "Добавлено"
                        self.stdout.write(self.style.SUCCESS(
                            f'   ✅ {action} {len(description)} символов'
                        ))
                    else:
                        self.stdout.write(self.style.WARNING('   ⚠️  Сохранено пустое описание'))
                    stats['updated'] += 1

                # 8. Задержка между запросами
                if i < stats['total'] and delay > 0:
                    if debug:
                        self.stdout.write(f'   ⏳ Задержка {delay} секунд...')
                    time.sleep(delay)

            except requests.exceptions.Timeout:
                error_msg = 'Таймаут запроса'
                self.stdout.write(self.style.ERROR(f'   ❌ {error_msg}'))

                failed_games.append({
                    'id': game.id,
                    'name': game.name,
                    'position': global_position,
                    'error': error_msg,
                    'type': 'timeout',
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                })
                stats['errors'] += 1

            except Exception as e:
                error_msg = str(e)[:200]
                self.stdout.write(self.style.ERROR(f'   ❌ Ошибка: {error_msg}'))

                failed_games.append({
                    'id': game.id,
                    'name': game.name,
                    'position': global_position,
                    'error': error_msg,
                    'type': 'exception',
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                })
                stats['errors'] += 1

        # Сохраняем логи
        if failed_games:
            self.save_log_file(failed_log_file, failed_games, 'failed')

        if not_found_games:
            self.save_log_file(not_found_log_file, not_found_games, 'not_found')

        # Вывод статистики
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write(self.style.SUCCESS('📊 ИМПОРТ ЗАВЕРШЕН'))
        self.stdout.write(f'📈 Всего игр в выборке: {total_in_db}')
        if offset > 0:
            self.stdout.write(f'📈 Пропущено (offset): {offset}')
        self.stdout.write(f'📈 Обработано игр: {stats["processed"]}')
        self.stdout.write(f'✅ Обновлено/добавлено: {stats["updated"]}')

        if not overwrite:
            self.stdout.write(f'⏭️  Пропущено (уже есть описание): {stats["skipped_no_overwrite"]}')

        self.stdout.write(f'⏭️  Пропущено (не найдено в RAWG): {stats["skipped_not_found"]}')
        self.stdout.write(f'❌ Ошибок: {stats["errors"]}')

        # Прогресс для продолжения (только если использовался offset и есть ещё игры)
        next_offset = offset + stats['processed']
        if next_offset < total_in_db:
            self.stdout.write(self.style.NOTICE(f'\n🎯 Для продолжения используйте:'))
            self.stdout.write(f'   python manage.py import_rawg_descriptions --offset={next_offset}')
            if limit > 0:
                self.stdout.write(f'   # или с тем же лимитом: --offset={next_offset} --limit={limit}')

        # Информация о логах
        if failed_games:
            self.stdout.write(self.style.WARNING(
                f'\n📁 Логи ошибок сохранены: {failed_log_file}'
            ))
            self.stdout.write(f'   Количество ошибок: {len(failed_games)}')

            # Показываем список ID для повторной обработки
            failed_ids = [str(g['id']) for g in failed_games]
            self.stdout.write(self.style.NOTICE(
                '\n💡 Для повторной обработки ошибок:'
            ))
            self.stdout.write(
                f'   python manage.py import_rawg_descriptions --game-ids="{",".join(failed_ids)}" --overwrite')

        if not_found_games:
            self.stdout.write(self.style.NOTICE(
                f'\n📁 Логи не найденных игр: {not_found_log_file}'
            ))
            self.stdout.write(f'   Не найдено в RAWG: {len(not_found_games)}')

            # Показываем список не найденных игр
            not_found_names = [f'{g["id"]}: {g["name"]}' for g in not_found_games[:5]]
            self.stdout.write(f'   Примеры: {", ".join(not_found_names)}')
            if len(not_found_games) > 5:
                self.stdout.write(f'   ... и ещё {len(not_found_games) - 5} игр')

        if dry_run:
            self.stdout.write(self.style.WARNING('\n⚠️  DRY RUN: данные НЕ сохранены'))

    def get_progress_bar(self, current, total, width=20):
        """Создает текстовый прогресс-бар"""
        if total == 0:
            return "[                    ]"

        progress = int((current / total) * width)
        bar = "[" + "=" * progress + " " * (width - progress) + "]"
        percent = int((current / total) * 100)
        return f"{bar} {percent}%"

    def find_best_match(self, game_name, results):
        """Находит наилучшее совпадение по названию игры"""
        game_name_lower = game_name.lower().strip()
        best_match = None
        best_score = -1

        for result in results:
            result_name = result.get('name', '').lower().strip()
            score = 0

            if result_name == game_name_lower:
                return result

            if game_name_lower in result_name:
                score += 3
            elif result_name in game_name_lower:
                score += 2

            if 'released' in result:
                score += 1

            if score > best_score:
                best_score = score
                best_match = result

        return best_match

    def extract_description(self, detail_data, search_result):
        """Извлекает и очищает описание из данных RAWG"""
        description = (
                detail_data.get('description') or
                detail_data.get('description_raw') or
                search_result.get('description') or
                ''
        )

        if description:
            description = re.sub(r'<[^>]+>', '', description)
            description = re.sub(r'\s+', ' ', description).strip()

        return description

    def save_log_file(self, filename, data, log_type):
        """Сохраняет логи в JSON файл"""
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump({
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
                    'type': log_type,
                    'count': len(data),
                    'games': data
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.stdout.write(self.style.ERROR(f'   ❌ Ошибка сохранения логов: {e}'))