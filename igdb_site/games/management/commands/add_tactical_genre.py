from django.core.management.base import BaseCommand
from games.models import Game, Genre, Keyword


class Command(BaseCommand):
    help = 'Добавляет жанр Tactical играм с ключевым словом tactical turn-based combat, у которых нет этого жанра'

    def add_arguments(self, parser):
        parser.add_argument(
            '--preview',
            action='store_true',
            help='РЕЖИМ ПРЕВЬЮ: показать какие игры будут обновлены без фактического сохранения'
        )

    def handle(self, *args, **options):
        preview_mode = options['preview']

        self.stdout.write('🎮 ДОБАВЛЕНИЕ ЖАНРА TACTICAL ИГРАМ С КЛЮЧЕВЫМ СЛОВОМ')
        if preview_mode:
            self.stdout.write('👀 РЕЖИМ ПРЕВЬЮ - изменения не сохраняются!')
        self.stdout.write('=' * 60)

        try:
            # 1. Найдем ключевое слово "tactical turn-based combat" в базе
            self.stdout.write('🔍 Поиск ключевого слова "tactical turn-based combat"...')

            try:
                tactical_keyword = Keyword.objects.get(name='tactical turn-based combat')
                self.stdout.write(f'✅ Найдено ключевое слово: {tactical_keyword.name} (ID: {tactical_keyword.igdb_id})')
            except Keyword.DoesNotExist:
                self.stdout.write('❌ Ключевое слово "tactical turn-based combat" не найдено в базе')
                return

            # 2. Найдем жанр "Tactical" в базе
            self.stdout.write('🔍 Поиск жанра "Tactical"...')

            try:
                tactical_genre = Genre.objects.get(name='Tactical')
                self.stdout.write(f'✅ Найден жанр: {tactical_genre.name} (ID: {tactical_genre.igdb_id})')
            except Genre.DoesNotExist:
                self.stdout.write('❌ Жанр "Tactical" не найден в базе')
                return

            # 3. Найдем игры с ключевым словом, но без жанра Tactical
            self.stdout.write('\n🔍 Поиск игр для обновления...')

            games_to_update = Game.objects.filter(
                keywords=tactical_keyword
            ).exclude(
                genres=tactical_genre
            ).distinct()

            self.stdout.write(f'📊 Найдено игр для обновления: {games_to_update.count()}')

            if not games_to_update:
                self.stdout.write('✅ Все игры уже имеют правильные жанры!')
                return

            # 4. Покажем список игр, которые будут обновлены
            self.stdout.write('\n📝 Игры для обновления:')
            for i, game in enumerate(games_to_update, 1):
                current_genres = ', '.join([genre.name for genre in game.genres.all()])
                self.stdout.write(f'   {i}. {game.name}')
                self.stdout.write(f'      Текущие жанры: {current_genres or "Нет жанров"}')
                self.stdout.write(f'      Будет добавлен: Tactical')

            # 5. Если это режим превью - останавливаемся
            if preview_mode:
                self.stdout.write('\n' + '=' * 60)
                self.stdout.write('👀 ПРЕВЬЮ ЗАВЕРШЕНО')
                self.stdout.write('ℹ️  Для фактического обновления запустите команду БЕЗ --preview')
                return

            # 6. Запрашиваем подтверждение для реального выполнения
            self.stdout.write('\n⚠️  ВНИМАНИЕ: Это приведет к фактическому изменению базы данных!')
            self.stdout.write('❓ Вы уверены, что хотите добавить жанр "Tactical" к этим играм?')
            confirmation = input('   📝 Введите "ДА" для подтверждения: ')

            if confirmation.upper() != 'ДА':
                self.stdout.write('❌ Операция отменена')
                return

            # 7. Выполняем обновление
            self.stdout.write('\n🔄 Добавление жанра Tactical...')
            updated_count = 0

            for game in games_to_update:
                try:
                    # Добавляем жанр Tactical к существующим жанрам
                    game.genres.add(tactical_genre)
                    updated_count += 1
                    self.stdout.write(f'   ✅ Обновлена: {game.name}')

                except Exception as e:
                    self.stderr.write(f'   ❌ Ошибка обновления {game.name}: {e}')

            # 8. Итоги
            self.stdout.write('\n' + '=' * 60)
            self.stdout.write(self.style.SUCCESS('✅ ОБНОВЛЕНИЕ ЗАВЕРШЕНО!'))
            self.stdout.write(f'• Обновлено игр: {updated_count}')
            self.stdout.write(f'• Добавлен жанр: {tactical_genre.name}')
            self.stdout.write(f'• По ключевому слову: {tactical_keyword.name}')

            # 9. Статистика по жанру
            tactical_games_count = Game.objects.filter(genres=tactical_genre).count()
            self.stdout.write(f'• Всего игр с жанром Tactical в базе: {tactical_games_count}')

        except Exception as e:
            self.stderr.write(f'❌ Ошибка: {e}')
            import traceback
            self.stderr.write(traceback.format_exc())