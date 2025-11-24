# management/commands/assign_rpg_tactical_genres.py

from django.core.management.base import BaseCommand
from games.models import Game, Genre
from games.igdb_api import make_igdb_request, set_debug_mode


class Command(BaseCommand):
    help = 'Присвоить жанры Role-playing (RPG) и Tactical играм из файла'

    def add_arguments(self, parser):
        parser.add_argument(
            '--input-file',
            type=str,
            required=True,
            help='Файл со списком игр (название на каждой строке)'
        )
        parser.add_argument(
            '--debug',
            action='store_true',
            help='Включить режим отладки'
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Показать что будет сделано без реального сохранения'
        )

    def handle(self, *args, **options):
        input_file = options['input_file']
        debug = options['debug']
        dry_run = options['dry_run']

        set_debug_mode(debug)

        self.stdout.write('🎮 ПРИСВОЕНИЕ ЖАНРОВ RPG И TACTICAL ИГРАМ ИЗ ФАЙЛА')
        self.stdout.write('=' * 60)
        self.stdout.write(f'📁 Файл: {input_file}')
        if dry_run:
            self.stdout.write('💡 РЕЖИМ ПРОСМОТРА (без сохранения)')
        self.stdout.write('=' * 60)

        # Получаем ID жанров
        self.stdout.write('🔍 Поиск ID жанров...')

        # Ищем ID жанра "Role-playing (RPG)"
        rpg_query = 'fields id,name; where name = "Role-playing (RPG)";'
        rpg_genres = make_igdb_request('genres', rpg_query, debug=debug)
        rpg_genre_id = rpg_genres[0]['id'] if rpg_genres else None

        # Ищем ID жанра "Tactical"
        tactical_query = 'fields id,name; where name = "Tactical";'
        tactical_genres = make_igdb_request('genres', tactical_query, debug=debug)
        tactical_genre_id = tactical_genres[0]['id'] if tactical_genres else None

        if not rpg_genre_id or not tactical_genre_id:
            self.stderr.write('❌ Не найдены ID жанров RPG или Tactical')
            return

        self.stdout.write(f'✅ RPG Genre ID: {rpg_genre_id}')
        self.stdout.write(f'✅ Tactical Genre ID: {tactical_genre_id}')

        # Создаем или получаем объекты жанров
        rpg_genre, _ = Genre.objects.get_or_create(
            igdb_id=rpg_genre_id,
            defaults={'name': 'Role-playing (RPG)'}
        )
        tactical_genre, _ = Genre.objects.get_or_create(
            igdb_id=tactical_genre_id,
            defaults={'name': 'Tactical'}
        )

        # Читаем игры из файла
        try:
            with open(input_file, 'r', encoding='utf-8') as f:
                game_names = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            self.stderr.write(f'❌ Файл {input_file} не найден!')
            return

        if not game_names:
            self.stderr.write('❌ Файл пуст!')
            return

        self.stdout.write(f'📋 Игр для обработки: {len(game_names)}')

        # Обрабатываем игры
        processed_count = 0
        not_found_count = 0
        updated_count = 0

        for i, game_name in enumerate(game_names, 1):
            if debug:
                self.stdout.write(f'🔍 [{i}/{len(game_names)}] Поиск: {game_name}')

            try:
                # Ищем игру в базе по точному названию
                games = Game.objects.filter(name__iexact=game_name)

                if not games.exists():
                    # Пробуем поиск по частичному совпадению
                    games = Game.objects.filter(name__icontains=game_name)

                if games.exists():
                    game = games.first()

                    # Получаем текущие жанры игры
                    current_genres = list(game.genres.all())
                    current_genre_ids = [g.igdb_id for g in current_genres]

                    # Проверяем, нужно ли добавлять жанры
                    needs_rpg = rpg_genre_id not in current_genre_ids
                    needs_tactical = tactical_genre_id not in current_genre_ids

                    if needs_rpg or needs_tactical:
                        # Добавляем недостающие жанры
                        if needs_rpg:
                            current_genres.append(rpg_genre)
                            if debug:
                                self.stdout.write(f'   ➕ Добавлен жанр RPG')

                        if needs_tactical:
                            current_genres.append(tactical_genre)
                            if debug:
                                self.stdout.write(f'   ➕ Добавлен жанр Tactical')

                        if not dry_run:
                            game.genres.set(current_genres)
                            game.save()

                        updated_count += 1
                        self.stdout.write(f'   ✅ Обновлена: {game.name}')
                    else:
                        if debug:
                            self.stdout.write(f'   ⏭️  Уже имеет оба жанра: {game.name}')

                else:
                    not_found_count += 1
                    self.stdout.write(f'   ❌ Не найдена в базе: {game_name}')

                processed_count += 1

            except Exception as e:
                self.stderr.write(f'   ❌ Ошибка обработки {game_name}: {e}')

        # Итоги
        self.stdout.write('\n' + '=' * 60)
        self.stdout.write('✅ ОБРАБОТКА ЗАВЕРШЕНА!')
        self.stdout.write(f'• Обработано игр: {processed_count}')
        self.stdout.write(f'• Обновлено жанров: {updated_count}')
        self.stdout.write(f'• Не найдено в базе: {not_found_count}')

        if dry_run:
            self.stdout.write('💡 РЕЖИМ ПРОСМОТРА - изменения не сохранены')
        else:
            self.stdout.write('💾 Изменения сохранены в базу')

        # Показываем примеры обновленных игр
        if updated_count > 0 and not dry_run:
            self.stdout.write(f'\n🎮 Примеры обновленных игр:')
            recent_updated = Game.objects.filter(genres__in=[rpg_genre, tactical_genre]).distinct()[:5]
            for game in recent_updated:
                genres = [g.name for g in game.genres.all()]
                self.stdout.write(f'   • {game.name} - {", ".join(genres)}')